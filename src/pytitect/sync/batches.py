"""Bounded mutation-batch coordination over explicit idempotency ports."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, TypeVar, cast

from pytitect.core import JsonValue
from pytitect.idempotency import (
    Conflict,
    Execute,
    IdempotencyPolicy,
    IdempotencyScope,
    IdempotencyStore,
    InProgress,
    Replay,
    RequestFingerprint,
    ReservationCompleted,
    Uncertain,
)
from pytitect.security.canonical import canonical_json

PayloadT = TypeVar("PayloadT")
ResultT = TypeVar("ResultT")


class BatchPolicy(StrEnum):
    ALL_OR_NOTHING = "all_or_nothing"
    PER_ITEM = "per_item"


ALL_OR_NOTHING = BatchPolicy.ALL_OR_NOTHING
PER_ITEM = BatchPolicy.PER_ITEM


@dataclass(frozen=True, slots=True)
class BatchLimits:
    max_items: int = 1_000
    max_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (self.max_items, self.max_bytes)
        ):
            raise ValueError("batch limits must be positive")


@dataclass(frozen=True, slots=True)
class BatchItem[PayloadT]:
    item_id: str
    payload: PayloadT

    def __post_init__(self) -> None:
        if not self.item_id or self.item_id != self.item_id.strip():
            raise ValueError("batch item IDs must be non-empty and trimmed")


@dataclass(frozen=True, slots=True)
class BatchItemReceipt[ResultT]:
    item_id: str
    result: ResultT
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class BatchCommitted[ResultT]:
    receipts: tuple[BatchItemReceipt[ResultT], ...]


@dataclass(frozen=True, slots=True)
class BatchReplay[ResultT]:
    receipts: tuple[BatchItemReceipt[ResultT], ...]


@dataclass(frozen=True, slots=True)
class BatchConflict:
    item_id: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class BatchInProgress:
    item_id: str | None
    retry_after: datetime


@dataclass(frozen=True, slots=True)
class BatchUncertain:
    item_id: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class BatchItemsCommittedEnvelopeUnconfirmed[ResultT]:
    receipts: tuple[BatchItemReceipt[ResultT], ...]


type BatchResult[ResultT] = (
    BatchCommitted[ResultT]
    | BatchReplay[ResultT]
    | BatchConflict
    | BatchInProgress
    | BatchUncertain
    | BatchItemsCommittedEnvelopeUnconfirmed[ResultT]
)


class BatchTransaction(Protocol):
    def atomic(self) -> AbstractContextManager[None]: ...


class _RollbackBatch(Exception):
    def __init__(self, result: BatchConflict | BatchInProgress | BatchUncertain) -> None:
        self.result = result


class MutationBatchCoordinator[PayloadT, ResultT]:
    """Coordinate envelope and per-item idempotency with stable receipt ordering."""

    def __init__(
        self,
        envelope_store: IdempotencyStore[tuple[BatchItemReceipt[ResultT], ...]],
        item_store: IdempotencyStore[BatchItemReceipt[ResultT]],
        transaction: BatchTransaction,
        *,
        using: str,
        limits: BatchLimits | None = None,
    ) -> None:
        if not using:
            raise ValueError("a database alias is required")
        for store in (envelope_store, item_store):
            alias = getattr(store, "using", using)
            if alias != using:
                raise ValueError("batch stores and mutations must use exactly one database alias")
        if getattr(transaction, "using", using) != using:
            raise ValueError("batch transaction and mutations must use exactly one database alias")
        self._envelopes = envelope_store
        self._items = item_store
        self._transaction = transaction
        self._using = using
        self._limits = limits or BatchLimits()

    def execute(
        self,
        *,
        batch_id: str,
        items: Sequence[BatchItem[PayloadT]],
        policy: BatchPolicy,
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        now: datetime,
        idempotency_policy: IdempotencyPolicy,
        namespace: str = "sync",
    ) -> BatchResult[ResultT]:
        if not batch_id or not namespace:
            raise ValueError("batch_id and namespace are required")
        _validate_items(items, self._limits)
        envelope_scope = IdempotencyScope(namespace, batch_id, "mutation-batch")
        envelope_fp = _fingerprint(
            {
                "items": [
                    {"id": item.item_id, "payload": cast(JsonValue, item.payload)} for item in items
                ]
            }
        )
        if policy is BatchPolicy.ALL_OR_NOTHING:
            try:
                with self._transaction.atomic():
                    envelope = self._envelopes.reserve(
                        envelope_scope,
                        batch_id,
                        envelope_fp,
                        now=now,
                        lease_ttl=idempotency_policy.execution_lease_ttl,
                    )
                    if isinstance(envelope, Replay):
                        return BatchReplay(envelope.value)
                    early = _early(envelope, item_id=None)
                    if early is not None:
                        raise _RollbackBatch(early)
                    assert isinstance(envelope, Execute)
                    return self._all_or_nothing(
                        envelope,
                        batch_id,
                        items,
                        mutate,
                        now=now,
                        idempotency_policy=idempotency_policy,
                        namespace=namespace,
                    )
            except _RollbackBatch as failure:
                return failure.result
        if policy is BatchPolicy.PER_ITEM:
            envelope = self._envelopes.reserve(
                envelope_scope,
                batch_id,
                envelope_fp,
                now=now,
                lease_ttl=idempotency_policy.execution_lease_ttl,
            )
            if isinstance(envelope, Replay):
                return BatchReplay(envelope.value)
            early = _early(envelope, item_id=None)
            if early is not None:
                return early
            assert isinstance(envelope, Execute)
            return self._per_item(
                envelope,
                batch_id,
                items,
                mutate,
                now=now,
                idempotency_policy=idempotency_policy,
                namespace=namespace,
            )
        raise ValueError("unsupported batch policy")

    def _all_or_nothing(
        self,
        envelope: Execute,
        batch_id: str,
        items: Sequence[BatchItem[PayloadT]],
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        *,
        now: datetime,
        idempotency_policy: IdempotencyPolicy,
        namespace: str,
    ) -> BatchResult[ResultT]:
        reservations: list[
            tuple[BatchItem[PayloadT], Execute | Replay[BatchItemReceipt[ResultT]]]
        ] = []
        for item in items:
            decision = self._reserve_item(
                batch_id,
                item,
                now=now,
                idempotency_policy=idempotency_policy,
                namespace=namespace,
            )
            early = _early(decision, item_id=item.item_id)
            if early is not None:
                raise _RollbackBatch(early)
            assert isinstance(decision, (Execute, Replay))
            reservations.append((item, decision))
        receipts: list[BatchItemReceipt[ResultT]] = []
        for item, reservation in reservations:
            if isinstance(reservation, Replay):
                receipts.append(
                    BatchItemReceipt(item.item_id, reservation.value.result, replayed=True)
                )
                continue
            receipt = BatchItemReceipt(item.item_id, mutate(item, self._using))
            if not isinstance(
                self._items.complete(
                    reservation.token,
                    receipt,
                    now=now,
                    retention_ttl=idempotency_policy.result_retention_ttl,
                ),
                ReservationCompleted,
            ):
                raise _RollbackBatch(BatchUncertain(item.item_id, "item CAS failed"))
            receipts.append(receipt)
        ordered = tuple(receipts)
        if not isinstance(
            self._envelopes.complete(
                envelope.token,
                ordered,
                now=now,
                retention_ttl=idempotency_policy.result_retention_ttl,
            ),
            ReservationCompleted,
        ):
            raise _RollbackBatch(BatchUncertain(None, "envelope CAS failed"))
        return BatchCommitted(ordered)

    def _per_item(
        self,
        envelope: Execute,
        batch_id: str,
        items: Sequence[BatchItem[PayloadT]],
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        *,
        now: datetime,
        idempotency_policy: IdempotencyPolicy,
        namespace: str,
    ) -> BatchResult[ResultT]:
        receipts: list[BatchItemReceipt[ResultT]] = []
        for item in items:
            try:
                with self._transaction.atomic():
                    decision = self._reserve_item(
                        batch_id,
                        item,
                        now=now,
                        idempotency_policy=idempotency_policy,
                        namespace=namespace,
                    )
                    if isinstance(decision, Replay):
                        receipts.append(
                            BatchItemReceipt(item.item_id, decision.value.result, replayed=True)
                        )
                        continue
                    early = _early(decision, item_id=item.item_id)
                    if early is not None:
                        raise _RollbackBatch(early)
                    assert isinstance(decision, Execute)
                    receipt = BatchItemReceipt(item.item_id, mutate(item, self._using))
                    if not isinstance(
                        self._items.complete(
                            decision.token,
                            receipt,
                            now=now,
                            retention_ttl=idempotency_policy.result_retention_ttl,
                        ),
                        ReservationCompleted,
                    ):
                        raise _RollbackBatch(
                            BatchUncertain(item.item_id, "item CAS failed; mutation rolled back")
                        )
                    receipts.append(receipt)
            except _RollbackBatch as failure:
                return failure.result
        ordered = tuple(receipts)
        if not isinstance(
            self._envelopes.complete(
                envelope.token,
                ordered,
                now=now,
                retention_ttl=idempotency_policy.result_retention_ttl,
            ),
            ReservationCompleted,
        ):
            return BatchItemsCommittedEnvelopeUnconfirmed(ordered)
        return BatchCommitted(ordered)

    def _reserve_item(
        self,
        batch_id: str,
        item: BatchItem[PayloadT],
        *,
        now: datetime,
        idempotency_policy: IdempotencyPolicy,
        namespace: str,
    ) -> Execute | Replay[BatchItemReceipt[ResultT]] | Conflict | InProgress | Uncertain:
        scope = IdempotencyScope(namespace, batch_id, f"mutation-item:{item.item_id}")
        return self._items.reserve(
            scope,
            item.item_id,
            _fingerprint(cast(JsonValue, item.payload)),
            now=now,
            lease_ttl=idempotency_policy.execution_lease_ttl,
        )


def _validate_items(items: Sequence[BatchItem[Any]], limits: BatchLimits) -> None:
    if len(items) > limits.max_items:
        raise ValueError("batch exceeds max_items")
    identifiers = [item.item_id for item in items]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("batch item IDs must be unique")
    payload = canonical_json(
        {
            "items": [
                {"id": item.item_id, "payload": cast(JsonValue, item.payload)} for item in items
            ]
        }
    )
    if len(payload) > limits.max_bytes:
        raise ValueError("batch exceeds max_bytes")


def _fingerprint(value: JsonValue) -> RequestFingerprint:
    return RequestFingerprint.from_json(value, canonicalizer=canonical_json)


def _early(
    decision: object, *, item_id: str | None
) -> BatchConflict | BatchInProgress | BatchUncertain | None:
    if isinstance(decision, Conflict):
        return BatchConflict(item_id, decision.reason)
    if isinstance(decision, InProgress):
        return BatchInProgress(item_id, decision.retry_after)
    if isinstance(decision, Uncertain):
        return BatchUncertain(item_id, decision.reason)
    return None
