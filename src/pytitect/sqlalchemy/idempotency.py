"""Explicit PostgreSQL idempotency, receipt and local HTTP transaction composition."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.core import Clock, OpaqueId, SystemClock
from pytitect.idempotency import (
    AbandonReservationResult,
    CompleteReservationResult,
    Conflict,
    Execute,
    IdempotencyDecision,
    IdempotencyPolicy,
    IdempotencyScope,
    InProgress,
    MarkUncertainResult,
    RenewReservationResult,
    Replay,
    RequestFingerprint,
    ReservationAbandoned,
    ReservationCompleted,
    ReservationMarkedUncertain,
    ReservationRenewed,
    ReservationToken,
    StaleReservation,
    Uncertain,
    _positive,
    _utc,
)
from pytitect.receipts import (
    CommandReceipt,
    MutationReceipt,
    OperationReceipt,
    Receipt,
    ReceiptKind,
    ReceiptState,
    ReceiptTransitioned,
    RunReceipt,
    TerminalBoundaryReceipt,
    _valid_reconciliation,
)
from pytitect.sqlalchemy.stores import PayloadSerializer
from pytitect.sqlalchemy.uow import SessionFactory


class SQLAlchemyIdempotencyStore[T]:
    def __init__(
        self,
        session: AsyncSession,
        model: type[Any],
        serializer: PayloadSerializer[T],
    ) -> None:
        self.session = session
        self.model = model
        self.serializer = serializer

    def _identity(self, scope: IdempotencyScope, key: str) -> tuple[Any, ...]:
        if not key:
            raise ValueError("idempotency key must not be empty")
        return (
            self.model.namespace == scope.namespace,
            self.model.subject == scope.subject,
            self.model.operation == scope.operation,
            self.model.key == key,
        )

    async def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> IdempotencyDecision[T]:
        _utc(now)
        _positive(lease_ttl, "lease_ttl")
        identity = self._identity(scope, key)
        token = uuid.uuid4().hex
        statement = (
            insert(self.model)
            .values(
                namespace=scope.namespace,
                subject=scope.subject,
                operation=scope.operation,
                key=key,
                fingerprint=str(fingerprint.value),
                token=token,
                state="reserved",
                expires_at=now + lease_ttl,
            )
            .on_conflict_do_nothing(index_elements=["namespace", "subject", "operation", "key"])
            .returning(self.model.token)
        )
        if (await self.session.execute(statement)).scalar_one_or_none() is not None:
            return Execute(ReservationToken(token))
        row = (
            await self.session.execute(
                select(self.model)
                .where(*identity)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one()
        if row.expires_at <= now and row.state != "uncertain":
            row.token, row.fingerprint = token, str(fingerprint.value)
            row.state, row.expires_at = "reserved", now + lease_ttl
            row.result, row.reason = None, None
            await self.session.flush()
            return Execute(ReservationToken(token))
        return self._decision(row, fingerprint)

    def _decision(self, row: Any, fingerprint: RequestFingerprint) -> IdempotencyDecision[T]:
        if row.fingerprint != str(fingerprint.value):
            return Conflict()
        if row.state == "completed":
            return Replay(self.serializer.decode(row.result))
        if row.state == "uncertain":
            return Uncertain(row.reason or "outcome unknown")
        return InProgress(row.expires_at)

    async def lookup(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
    ) -> IdempotencyDecision[T] | None:
        _utc(now)
        row = await self.session.execute(select(self.model).where(*self._identity(scope, key)))
        current = row.scalar_one_or_none()
        if current is None or (current.expires_at <= now and current.state != "uncertain"):
            return None
        return self._decision(current, fingerprint)

    def _authority(self, token: ReservationToken, now: datetime) -> tuple[Any, ...]:
        _utc(now)
        return (
            self.model.token == token.value,
            self.model.state == "reserved",
            self.model.expires_at > func.greatest(now, func.clock_timestamp()),
        )

    async def _change(self, token: ReservationToken, now: datetime, **values: Any) -> bool:
        await self.session.execute(
            select(self.model.token).where(self.model.token == token.value).with_for_update()
        )
        statement = (
            update(self.model)
            .where(*self._authority(token, now))
            .values(**values)
            .returning(self.model.token)
            .execution_options(synchronize_session=False)
        )
        return (await self.session.execute(statement)).scalar_one_or_none() is not None

    async def renew(
        self,
        token: ReservationToken,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> RenewReservationResult:
        _positive(lease_ttl, "lease_ttl")
        if await self._change(token, now, expires_at=now + lease_ttl):
            return ReservationRenewed(now + lease_ttl)
        return StaleReservation()

    async def complete(
        self,
        token: ReservationToken,
        value: T,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> CompleteReservationResult:
        _positive(retention_ttl, "retention_ttl")
        if await self._change(
            token,
            now,
            state="completed",
            result=self.serializer.encode(value),
            expires_at=now + retention_ttl,
        ):
            return ReservationCompleted(now + retention_ttl)
        return StaleReservation()

    async def mark_uncertain(
        self,
        token: ReservationToken,
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MarkUncertainResult:
        _positive(retention_ttl, "retention_ttl")
        if not reason:
            raise ValueError("uncertainty reason must not be empty")
        if await self._change(
            token, now, state="uncertain", reason=reason, expires_at=now + retention_ttl
        ):
            return ReservationMarkedUncertain(now + retention_ttl)
        return StaleReservation()

    async def abandon(self, token: ReservationToken, *, now: datetime) -> AbandonReservationResult:
        statement = (
            delete(self.model)
            .where(*self._authority(token, now))
            .returning(self.model.token)
            .execution_options(synchronize_session=False)
        )
        if (await self.session.execute(statement)).scalar_one_or_none() is not None:
            return ReservationAbandoned()
        return StaleReservation()


class SQLAlchemyReceiptStore[T]:
    def __init__(
        self,
        session: AsyncSession,
        model: type[Any],
        serializer: PayloadSerializer[T],
    ) -> None:
        self.session, self.model, self.serializer = session, model, serializer

    def _values(self, receipt: Receipt[T]) -> dict[str, Any]:
        return dict(
            receipt_id=str(receipt.receipt_id),
            kind=receipt.kind.value,
            state=receipt.state.value,
            created_at=receipt.created_at,
            updated_at=receipt.updated_at,
            receipt_metadata=dict(receipt.metadata),
            result=None if receipt.result is None else self.serializer.encode(receipt.result),
        )

    async def get(self, receipt_id: OpaqueId[object]) -> Receipt[T] | None:
        row = (
            await self.session.execute(
                select(self.model).where(self.model.receipt_id == str(receipt_id))
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        kind = ReceiptKind(row.kind)
        receipt_type = {
            ReceiptKind.MUTATION: MutationReceipt,
            ReceiptKind.COMMAND: CommandReceipt,
            ReceiptKind.RUN: RunReceipt,
            ReceiptKind.OPERATION: OperationReceipt,
            ReceiptKind.TERMINAL_BOUNDARY: TerminalBoundaryReceipt,
        }[kind]
        return receipt_type(
            OpaqueId(row.receipt_id),
            ReceiptState(row.state),
            row.created_at,
            row.updated_at,
            None if row.result is None else self.serializer.decode(row.result),
            row.receipt_metadata,
        )

    async def add(self, receipt: Receipt[T]) -> bool:
        statement = (
            insert(self.model)
            .values(**self._values(receipt))
            .on_conflict_do_nothing(index_elements=["receipt_id"])
            .returning(self.model.receipt_id)
        )
        return (await self.session.execute(statement)).scalar_one_or_none() is not None

    async def transition(self, receipt: Receipt[T], target: Receipt[T]) -> bool:
        proposed = receipt.transition(target.state, at=target.updated_at, result=target.result)
        if not isinstance(proposed, ReceiptTransitioned) or proposed.receipt != target:
            return False
        return await self._cas(receipt, target)

    async def reconcile_uncertain(self, receipt: Receipt[T], target: Receipt[T]) -> bool:
        if not _valid_reconciliation(receipt, target):
            return False
        return await self._cas(receipt, target)

    async def _cas(self, receipt: Receipt[T], target: Receipt[T]) -> bool:
        # Lock before comparing the complete record, including caller-serialized data.
        row = (
            await self.session.execute(
                select(self.model)
                .where(self.model.receipt_id == str(receipt.receipt_id))
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if row is None or any(
            getattr(row, key) != value for key, value in self._values(receipt).items()
        ):
            return False
        for key, value in self._values(target).items():
            setattr(row, key, value)
        await self.session.flush()
        return True


@dataclass(frozen=True, slots=True)
class RequestCommitted[T]:
    value: T
    receipt: Receipt[T]


class SQLAlchemyIdempotentRequest[T]:
    """One local transaction, with explicit callbacks and independent reconciliation.

    The callback must only make effects in the supplied session (including outbox).
    No automatic execution follows an ambiguous commit. Missing reconciliation
    evidence is uncertainty, even if the connection used to query also failed.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        idempotency_model: type[Any],
        receipt_model: type[Any],
        serializer: PayloadSerializer[T],
        policy: IdempotencyPolicy,
        clock: Clock | None = None,
    ) -> None:
        self._sessions, self._idempotency, self._receipt = (
            session_factory,
            idempotency_model,
            receipt_model,
        )
        self._serializer, self._policy = serializer, policy
        self._clock = clock or SystemClock()

    async def execute(
        self,
        *,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        receipt_id: OpaqueId[object],
        mutate: Callable[[AsyncSession], Awaitable[T]],
    ) -> RequestCommitted[T] | IdempotencyDecision[T]:
        committing = False
        try:
            async with self._sessions() as session:
                async with session.begin():
                    store = SQLAlchemyIdempotencyStore(session, self._idempotency, self._serializer)
                    decision = await store.reserve(
                        scope,
                        key,
                        fingerprint,
                        now=self._clock.now(),
                        lease_ttl=self._policy.execution_lease_ttl,
                    )
                    if not isinstance(decision, Execute):
                        return decision
                    created_at = self._clock.now()
                    value = await mutate(session)
                    now = self._clock.now()
                    completed = await store.complete(
                        decision.token,
                        value,
                        now=now,
                        retention_ttl=self._policy.result_retention_ttl,
                    )
                    if not isinstance(completed, ReservationCompleted):
                        raise RuntimeError("idempotency execution authority expired")
                    receipt = OperationReceipt(
                        receipt_id,
                        ReceiptState.COMPLETED,
                        created_at,
                        max(created_at, now),
                        value,
                    )
                    if not await SQLAlchemyReceiptStore(
                        session, self._receipt, self._serializer
                    ).add(receipt):
                        raise ValueError("receipt identity already exists")
                    await session.flush()
                    committing = True
                return RequestCommitted(value, receipt)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not committing:
                raise
        return await self.reconcile(scope=scope, key=key, fingerprint=fingerprint)

    async def reconcile(
        self,
        *,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
    ) -> IdempotencyDecision[T]:
        try:
            async with self._sessions() as session, session.begin():
                result = await SQLAlchemyIdempotencyStore(
                    session, self._idempotency, self._serializer
                ).lookup(
                    scope,
                    key,
                    fingerprint,
                    now=self._clock.now(),
                )
                return result if result is not None else Uncertain("no confirmed commit evidence")
        except asyncio.CancelledError:
            raise
        except Exception:
            return Uncertain("reconciliation unavailable")
