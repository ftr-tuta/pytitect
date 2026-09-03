"""Consumer-selected Django transaction boundary."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, cast

from pytitect.core import Clock, SystemClock
from pytitect.idempotency import (
    Conflict,
    Execute,
    IdempotencyDecision,
    IdempotencyPolicy,
    IdempotencyScope,
    InProgress,
    Replay,
    RequestFingerprint,
    ReservationCompleted,
    Uncertain,
)
from pytitect.outbox import OutboxAdded, OutboxEnvelope
from pytitect.receipts import Receipt, ReceiptState


@dataclass(frozen=True, slots=True)
class DjangoTransactionBoundary:
    using: str

    def __post_init__(self) -> None:
        if not self.using:
            raise ValueError("a Django database alias is required")

    def atomic(self) -> AbstractContextManager[None]:
        from django.db import transaction

        return cast(AbstractContextManager[None], transaction.atomic(using=self.using))

    def on_commit(self, callback: Callable[[], None]) -> None:
        from django.db import transaction

        transaction.on_commit(callback, using=self.using)


@dataclass(frozen=True, slots=True)
class TransactionalOperationCommitted[ResultT]:
    value: ResultT
    receipt: Receipt[ResultT]
    outbox_messages: int


@dataclass(frozen=True, slots=True)
class TransactionalOperationRolledBack:
    reason: str


type TransactionalOperationResult[ResultT] = (
    TransactionalOperationCommitted[ResultT]
    | TransactionalOperationRolledBack
    | Replay[ResultT]
    | Conflict
    | InProgress
    | Uncertain
)


class _ExpectedRollback(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason


class DjangoTransactionalOperation[ResultT, OutboxPayloadT]:
    """One-alias operation for domain, receipt, outbox, and idempotency writes."""

    def __init__(
        self,
        *,
        using: str,
        domain_using: str,
        idempotency: object,
        receipts: object,
        outbox: object,
        idempotency_policy: IdempotencyPolicy,
        clock: Clock | None = None,
    ) -> None:
        if not using or not domain_using:
            raise ValueError("one database alias is required")
        aliases = {
            using,
            domain_using,
            getattr(idempotency, "using", None),
            getattr(receipts, "using", None),
            getattr(outbox, "using", None),
        }
        if aliases != {using}:
            raise ValueError("domain, idempotency, receipts, and outbox must use exactly one alias")
        self.using = using
        self._idempotency = cast(Any, idempotency)
        self._receipts = cast(Any, receipts)
        self._outbox = cast(Any, outbox)
        self._policy = idempotency_policy
        self._clock = clock or SystemClock()

    def execute(
        self,
        *,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        mutate: Callable[[str], ResultT],
        make_receipt: Callable[[ResultT], Receipt[ResultT]],
        make_outbox: Callable[[ResultT], tuple[OutboxEnvelope[OutboxPayloadT], ...]] = (
            lambda result: ()
        ),
    ) -> TransactionalOperationResult[ResultT]:
        from django.db import transaction

        now = self._clock.now()
        try:
            with transaction.atomic(using=self.using):
                decision = cast(
                    IdempotencyDecision[ResultT],
                    self._idempotency.reserve(
                        scope,
                        key,
                        fingerprint,
                        now=now,
                        lease_ttl=self._policy.execution_lease_ttl,
                    ),
                )
                if not isinstance(decision, Execute):
                    return decision
                value = mutate(self.using)
                receipt = make_receipt(value)
                if receipt.result != value or receipt.state not in {
                    ReceiptState.COMPLETED,
                    ReceiptState.REJECTED,
                    ReceiptState.CONFLICTED,
                    ReceiptState.UNCERTAIN,
                }:
                    raise ValueError("transactional operations require a matching terminal receipt")
                if not self._receipts.add(receipt):
                    raise _ExpectedRollback("receipt compare-and-set failed")
                envelopes = make_outbox(value)
                for envelope in envelopes:
                    if not isinstance(self._outbox.add(envelope), OutboxAdded):
                        raise _ExpectedRollback("outbox compare-and-set failed")
                completed = self._idempotency.complete(
                    decision.token,
                    value,
                    now=now,
                    retention_ttl=self._policy.result_retention_ttl,
                )
                if not isinstance(completed, ReservationCompleted):
                    raise _ExpectedRollback("idempotency compare-and-set failed")
                return TransactionalOperationCommitted(value, receipt, len(envelopes))
        except _ExpectedRollback as failure:
            return TransactionalOperationRolledBack(failure.reason)
