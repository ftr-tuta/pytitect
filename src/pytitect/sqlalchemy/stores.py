"""Async PostgreSQL stores using an explicit consumer-owned AsyncSession."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol, cast

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.aio.quarantine import RejectedDelivery
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.inbox import (
    InboxAccepted,
    InboxDecision,
    InboxDuplicate,
    InboxInProgress,
    InboxScope,
)
from pytitect.outbox import (
    OutboxAdded,
    OutboxAddResult,
    OutboxClaim,
    OutboxDuplicate,
    OutboxEnvelope,
)


class PayloadSerializer[PayloadT](Protocol):
    def encode(self, value: PayloadT) -> bytes: ...

    def decode(self, value: bytes) -> PayloadT: ...


@dataclass(frozen=True, slots=True)
class ModelBundle:
    """Consumer-provided concrete models; Pytitect does not own names or migrations."""

    inbox: type[Any]
    outbox: type[Any]
    checkpoint: type[Any]
    rejected_delivery: type[Any]
    idempotency: type[Any] | None = None
    receipt: type[Any] | None = None
    lease: type[Any] | None = None
    generation: type[Any] | None = None
    mutation_batch: type[Any] | None = None
    process_manager: type[Any] | None = None
    timer: type[Any] | None = None
    job: type[Any] | None = None
    projection: type[Any] | None = None
    event: type[Any] | None = None
    snapshot: type[Any] | None = None


class SQLAlchemyInboxStore:
    def __init__(
        self, session: AsyncSession, model: type[Any], *, capacity: int | None = None
    ) -> None:
        if capacity is not None and capacity <= 0:
            raise ValueError("capacity must be positive when specified")
        self.session = session
        self.model = model
        self.capacity = capacity

    async def begin(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision:
        _utc(now)
        if not token or ttl <= timedelta(0):
            raise ValueError("token and a positive ttl are required")
        values = {
            "namespace": scope.namespace,
            "source": scope.source,
            "consumer": scope.consumer,
            "message_id": str(message_id),
            "token": token,
            "expires_at": now + ttl,
            "completed_at": None,
        }
        statement = (
            insert(self.model)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=["namespace", "source", "consumer", "message_id"]
            )
            .returning(self.model.message_id)
        )
        inserted = (await self.session.execute(statement)).scalar_one_or_none()
        if inserted is not None:
            return InboxAccepted(token)
        row = (
            await self.session.execute(
                select(self.model)
                .where(
                    self.model.namespace == scope.namespace,
                    self.model.source == scope.source,
                    self.model.consumer == scope.consumer,
                    self.model.message_id == str(message_id),
                )
                .with_for_update()
            )
        ).scalar_one()
        if row.completed_at is not None:
            return InboxDuplicate(cast(datetime, row.completed_at))
        if row.expires_at > now:
            return InboxInProgress(cast(datetime, row.expires_at))
        row.token = token
        row.expires_at = now + ttl
        return InboxAccepted(token)

    async def complete(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool:
        _utc(now)
        row = await self._locked(scope, message_id)
        if (
            row is None
            or row.token != token
            or row.completed_at is not None
            or row.expires_at <= now
        ):
            return False
        row.completed_at = now
        return True

    async def abandon(self, scope: InboxScope, message_id: OpaqueId[object], *, token: str) -> bool:
        row = await self._locked(scope, message_id)
        if row is None or row.token != token or row.completed_at is not None:
            return False
        await self.session.delete(row)
        return True

    async def _locked(self, scope: InboxScope, message_id: OpaqueId[object]) -> Any | None:
        return (
            await self.session.execute(
                select(self.model)
                .where(
                    self.model.namespace == scope.namespace,
                    self.model.source == scope.source,
                    self.model.consumer == scope.consumer,
                    self.model.message_id == str(message_id),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()


class SQLAlchemyOutboxStore[PayloadT]:
    def __init__(
        self, session: AsyncSession, model: type[Any], serializer: PayloadSerializer[PayloadT]
    ) -> None:
        self.session = session
        self.model = model
        self.serializer = serializer

    async def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        statement = (
            insert(self.model)
            .values(
                message_id=str(envelope.message_id),
                topic=envelope.topic,
                payload=self.serializer.encode(envelope.payload),
                occurred_at=envelope.occurred_at,
                available_at=envelope.available_at,
                attempt=envelope.attempt,
            )
            .on_conflict_do_nothing(index_elements=["message_id"])
            .returning(self.model.message_id)
        )
        added = (await self.session.execute(statement)).scalar_one_or_none()
        return OutboxAdded() if added is not None else OutboxDuplicate()

    async def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[OutboxClaim[PayloadT]]:
        _utc(now)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("claim limit must be a positive integer")
        if claim_ttl <= timedelta(0):
            raise ValueError("claim ttl must be positive")
        rows = (
            await self.session.execute(outbox_claim_statement(self.model, now=now, limit=limit))
        ).scalars()
        claims: list[OutboxClaim[PayloadT]] = []
        for row in rows:
            claim_id = uuid.uuid4().hex
            row.claim_id = claim_id
            row.claimed_until = now + claim_ttl
            envelope = self._envelope(row)
            claims.append(OutboxClaim(claim_id, envelope, row.claimed_until))
        return claims

    async def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> bool:
        _utc(at)
        row = await self._claimed(claim)
        if row is None:
            return False
        row.delivered_at = at
        row.claim_id = None
        row.claimed_until = None
        return True

    async def retry(self, claim: OutboxClaim[PayloadT], *, available_at: datetime) -> bool:
        _utc(available_at)
        row = await self._claimed(claim)
        if row is None:
            return False
        row.attempt += 1
        row.available_at = available_at
        row.claim_id = None
        row.claimed_until = None
        return True

    async def failed(self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime) -> bool:
        _utc(at)
        if not reason:
            raise ValueError("outbox failure reason must not be empty")
        row = await self._claimed(claim)
        if row is None:
            return False
        row.failure_reason = reason
        row.failed_at = at
        row.claim_id = None
        row.claimed_until = None
        return True

    async def _claimed(self, claim: OutboxClaim[PayloadT]) -> Any | None:
        return (
            await self.session.execute(
                select(self.model)
                .where(
                    self.model.message_id == str(claim.envelope.message_id),
                    self.model.claim_id == claim.claim_id,
                    self.model.delivered_at.is_(None),
                    self.model.failure_reason.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()

    def _envelope(self, row: Any) -> OutboxEnvelope[PayloadT]:
        return OutboxEnvelope(
            OpaqueId(row.message_id),
            row.topic,
            self.serializer.decode(row.payload),
            row.occurred_at,
            row.available_at,
            row.attempt,
        )


class SQLAlchemyCheckpointStore:
    def __init__(self, session: AsyncSession, model: type[Any]) -> None:
        self.session = session
        self.model = model

    async def load(self, stream: str) -> Checkpoint | None:
        _stream(stream)
        value = (
            await self.session.execute(
                select(self.model.checkpoint).where(self.model.stream == stream)
            )
        ).scalar_one_or_none()
        return None if value is None else Checkpoint(value)

    async def load_for_update(self, stream: str) -> Checkpoint | None:
        _stream(stream)
        value = (
            await self.session.execute(
                select(self.model.checkpoint).where(self.model.stream == stream).with_for_update()
            )
        ).scalar_one_or_none()
        return None if value is None else Checkpoint(value)

    async def advance(
        self,
        stream: str,
        *,
        expected: Checkpoint | None,
        checkpoint: Checkpoint,
    ) -> bool:
        current = await self.load_for_update(stream)
        if current != expected:
            return False
        if current is None:
            self.session.add(self.model(stream=stream, checkpoint=checkpoint.value))
        else:
            row = (
                await self.session.execute(
                    select(self.model).where(self.model.stream == stream).with_for_update()
                )
            ).scalar_one()
            row.checkpoint = checkpoint.value
        return True


class SQLAlchemyRejectedDeliveryStore:
    def __init__(self, session: AsyncSession, model: type[Any]) -> None:
        self.session = session
        self.model = model

    async def add(self, delivery: RejectedDelivery) -> bool:
        statement = (
            insert(self.model)
            .values(
                quarantine_id=delivery.quarantine_id,
                message_id=delivery.message_id,
                source=delivery.source,
                consumer=delivery.consumer,
                failed_at=delivery.failed_at,
                payload_sha256=delivery.payload_sha256,
                reason=delivery.reason,
                quarantine_metadata=dict(delivery.metadata),
                payload=delivery.payload,
            )
            .on_conflict_do_nothing(index_elements=["quarantine_id"])
            .returning(self.model.quarantine_id)
        )
        return (await self.session.execute(statement)).scalar_one_or_none() is not None


def outbox_claim_statement(model: type[Any], *, now: datetime, limit: int) -> Select[tuple[Any]]:
    """Build the PostgreSQL finite claim query, exposed for adapter QA."""

    _utc(now)
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("claim limit must be a positive integer")
    return (
        select(model)
        .where(
            model.available_at <= now,
            model.delivered_at.is_(None),
            model.failure_reason.is_(None),
            (model.claimed_until.is_(None) | (model.claimed_until <= now)),
        )
        .order_by(model.available_at, model.message_id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("store timestamps must be timezone-aware UTC")


def _stream(stream: str) -> None:
    if not stream:
        raise ValueError("checkpoint stream must not be empty")
