"""Safely resumable mutation batches over explicit consumer-owned stores."""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, TypeVar, cast

from pytitect.core import Clock, JsonValue, SystemClock
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
    ReservationRenewed,
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


class MutationBatchState(StrEnum):
    PROCESSING = "processing"
    PARTIALLY_COMMITTED = "partially_committed"
    COMPLETED = "completed"
    UNCERTAIN = "uncertain"


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
    """Committed item receipts whose final batch transition was not confirmed.

    A retry after the batch lease expires can prove the retained item receipts and
    complete the envelope without repeating those mutations.
    """

    receipts: tuple[BatchItemReceipt[ResultT], ...]


type BatchResult[ResultT] = (
    BatchCommitted[ResultT]
    | BatchReplay[ResultT]
    | BatchConflict
    | BatchInProgress
    | BatchUncertain
    | BatchItemsCommittedEnvelopeUnconfirmed[ResultT]
)


@dataclass(frozen=True, slots=True)
class MutationBatchLease[ResultT]:
    namespace: str
    batch_id: str
    token: str
    state: MutationBatchState
    next_index: int
    total_items: int
    receipts: tuple[BatchItemReceipt[ResultT], ...]
    expires_at: datetime
    resumed: bool = False

    def __post_init__(self) -> None:
        _utc(self.expires_at)
        if not self.namespace or not self.batch_id or not self.token:
            raise ValueError("batch lease identity must not be empty")
        if self.state not in {
            MutationBatchState.PROCESSING,
            MutationBatchState.PARTIALLY_COMMITTED,
        }:
            raise ValueError("a batch lease must be in an executing state")
        if not 0 <= self.next_index <= self.total_items:
            raise ValueError("batch lease progress is invalid")
        if self.next_index != len(self.receipts):
            raise ValueError("batch progress and receipts must agree")


@dataclass(frozen=True, slots=True)
class MutationBatchLeaseRenewed[ResultT]:
    lease: MutationBatchLease[ResultT]


@dataclass(frozen=True, slots=True)
class MutationBatchProgressed[ResultT]:
    lease: MutationBatchLease[ResultT]


@dataclass(frozen=True, slots=True)
class MutationBatchCompleted:
    retained_until: datetime


@dataclass(frozen=True, slots=True)
class MutationBatchMarkedUncertain:
    retained_until: datetime


@dataclass(frozen=True, slots=True)
class StaleMutationBatchLease:
    reason: str = "batch lease is absent, expired, or no longer authoritative"


type MutationBatchBeginResult[ResultT] = (
    MutationBatchLease[ResultT]
    | BatchReplay[ResultT]
    | BatchConflict
    | BatchInProgress
    | BatchUncertain
)
type MutationBatchRenewResult[ResultT] = (
    MutationBatchLeaseRenewed[ResultT] | StaleMutationBatchLease
)
type MutationBatchAdvanceResult[ResultT] = (
    MutationBatchProgressed[ResultT] | StaleMutationBatchLease
)
type MutationBatchCompleteResult = MutationBatchCompleted | StaleMutationBatchLease
type MutationBatchUncertainResult = MutationBatchMarkedUncertain | StaleMutationBatchLease


class MutationBatchStore(Protocol[ResultT]):
    """Persistence port for a retained, resumable batch state machine."""

    def begin(
        self,
        namespace: str,
        batch_id: str,
        fingerprint: RequestFingerprint,
        *,
        total_items: int,
        now: datetime,
        lease_ttl: timedelta,
    ) -> MutationBatchBeginResult[ResultT]: ...

    def renew(
        self,
        lease: MutationBatchLease[ResultT],
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> MutationBatchRenewResult[ResultT]: ...

    def advance(
        self,
        lease: MutationBatchLease[ResultT],
        receipt: BatchItemReceipt[ResultT],
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> MutationBatchAdvanceResult[ResultT]: ...

    def complete(
        self,
        lease: MutationBatchLease[ResultT],
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MutationBatchCompleteResult: ...

    def mark_uncertain(
        self,
        lease: MutationBatchLease[ResultT],
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MutationBatchUncertainResult: ...


@dataclass(slots=True)
class _StoredBatch[ResultT]:
    fingerprint: RequestFingerprint
    total_items: int
    token: str
    state: MutationBatchState
    next_index: int
    receipts: tuple[BatchItemReceipt[ResultT], ...]
    lease_expires_at: datetime
    retention_expires_at: datetime | None = None
    uncertainty_reason: str | None = None


class InMemoryMutationBatchStore[ResultT]:
    """Thread-safe, finite, process-local reference batch store."""

    def __init__(self, *, capacity: int = 1_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._entries: OrderedDict[tuple[str, str], _StoredBatch[ResultT]] = OrderedDict()
        self._tokens: dict[str, tuple[str, str]] = {}
        self._lock = threading.RLock()

    def begin(
        self,
        namespace: str,
        batch_id: str,
        fingerprint: RequestFingerprint,
        *,
        total_items: int,
        now: datetime,
        lease_ttl: timedelta,
    ) -> MutationBatchBeginResult[ResultT]:
        _validate_begin(namespace, batch_id, total_items, now, lease_ttl)
        identity = (namespace, batch_id)
        with self._lock:
            self._purge_terminal(now)
            current = self._entries.get(identity)
            if current is not None:
                self._entries.move_to_end(identity)
                if current.fingerprint != fingerprint or current.total_items != total_items:
                    return BatchConflict(None, "the batch ID was used with different items")
                if current.state is MutationBatchState.COMPLETED:
                    return BatchReplay(current.receipts)
                if current.state is MutationBatchState.UNCERTAIN:
                    return BatchUncertain(
                        None, current.uncertainty_reason or "the prior batch outcome is unknown"
                    )
                if current.lease_expires_at > now:
                    return BatchInProgress(None, current.lease_expires_at)
                self._tokens.pop(current.token, None)
                current.token = uuid.uuid4().hex
                current.lease_expires_at = now + lease_ttl
                self._tokens[current.token] = identity
                return self._lease(identity, current, resumed=True)
            if len(self._entries) >= self._capacity:
                return BatchUncertain(None, "mutation batch capacity exceeded; no lease was issued")
            token = uuid.uuid4().hex
            stored: _StoredBatch[ResultT] = _StoredBatch(
                fingerprint=fingerprint,
                total_items=total_items,
                token=token,
                state=MutationBatchState.PROCESSING,
                next_index=0,
                receipts=(),
                lease_expires_at=now + lease_ttl,
            )
            self._entries[identity] = stored
            self._tokens[token] = identity
            return self._lease(identity, stored, resumed=False)

    def renew(
        self,
        lease: MutationBatchLease[ResultT],
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> MutationBatchRenewResult[ResultT]:
        _utc(now)
        _positive(lease_ttl, "lease_ttl")
        with self._lock:
            current = self._active(lease, now)
            if current is None:
                return StaleMutationBatchLease()
            current.lease_expires_at = now + lease_ttl
            return MutationBatchLeaseRenewed(
                self._lease((lease.namespace, lease.batch_id), current, resumed=lease.resumed)
            )

    def advance(
        self,
        lease: MutationBatchLease[ResultT],
        receipt: BatchItemReceipt[ResultT],
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> MutationBatchAdvanceResult[ResultT]:
        _utc(now)
        _positive(lease_ttl, "lease_ttl")
        with self._lock:
            current = self._active(lease, now)
            if current is None or current.next_index >= current.total_items:
                return StaleMutationBatchLease("batch progress is stale or already complete")
            current.receipts += (receipt,)
            current.next_index += 1
            current.state = MutationBatchState.PARTIALLY_COMMITTED
            current.lease_expires_at = now + lease_ttl
            return MutationBatchProgressed(
                self._lease((lease.namespace, lease.batch_id), current, resumed=lease.resumed)
            )

    def complete(
        self,
        lease: MutationBatchLease[ResultT],
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MutationBatchCompleteResult:
        _utc(now)
        _positive(retention_ttl, "retention_ttl")
        with self._lock:
            current = self._active(lease, now)
            if current is None or current.next_index != current.total_items:
                return StaleMutationBatchLease("batch is stale or has uncommitted items")
            current.state = MutationBatchState.COMPLETED
            current.retention_expires_at = now + retention_ttl
            self._tokens.pop(current.token, None)
            return MutationBatchCompleted(current.retention_expires_at)

    def mark_uncertain(
        self,
        lease: MutationBatchLease[ResultT],
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MutationBatchUncertainResult:
        _utc(now)
        _positive(retention_ttl, "retention_ttl")
        if not reason:
            raise ValueError("an uncertainty reason is required")
        with self._lock:
            current = self._active(lease, now)
            if current is None:
                return StaleMutationBatchLease()
            current.state = MutationBatchState.UNCERTAIN
            current.uncertainty_reason = reason
            current.retention_expires_at = now + retention_ttl
            self._tokens.pop(current.token, None)
            return MutationBatchMarkedUncertain(current.retention_expires_at)

    def _active(
        self, lease: MutationBatchLease[ResultT], now: datetime
    ) -> _StoredBatch[ResultT] | None:
        identity = self._tokens.get(lease.token)
        if identity != (lease.namespace, lease.batch_id):
            return None
        current = self._entries.get(identity)
        if (
            current is None
            or current.token != lease.token
            or current.state
            not in {MutationBatchState.PROCESSING, MutationBatchState.PARTIALLY_COMMITTED}
            or current.lease_expires_at <= now
            or current.lease_expires_at != lease.expires_at
            or current.next_index != lease.next_index
            or current.receipts != lease.receipts
        ):
            return None
        return current

    def _lease(
        self,
        identity: tuple[str, str],
        stored: _StoredBatch[ResultT],
        *,
        resumed: bool,
    ) -> MutationBatchLease[ResultT]:
        return MutationBatchLease(
            namespace=identity[0],
            batch_id=identity[1],
            token=stored.token,
            state=stored.state,
            next_index=stored.next_index,
            total_items=stored.total_items,
            receipts=stored.receipts,
            expires_at=stored.lease_expires_at,
            resumed=resumed,
        )

    def _purge_terminal(self, now: datetime) -> None:
        expired = [
            identity
            for identity, entry in self._entries.items()
            if entry.state in {MutationBatchState.COMPLETED, MutationBatchState.UNCERTAIN}
            and entry.retention_expires_at is not None
            and entry.retention_expires_at <= now
        ]
        for identity in expired:
            self._entries.pop(identity)


class BatchTransaction(Protocol):
    def atomic(self) -> AbstractContextManager[None]: ...


class _RollbackBatch(Exception):
    def __init__(self, result: BatchConflict | BatchInProgress | BatchUncertain) -> None:
        self.result = result


class MutationBatchCoordinator[PayloadT, ResultT]:
    """Commit each item receipt and batch progress under one selected transaction alias."""

    def __init__(
        self,
        batch_store: MutationBatchStore[ResultT],
        item_store: IdempotencyStore[BatchItemReceipt[ResultT]],
        transaction: BatchTransaction,
        *,
        using: str,
        limits: BatchLimits | None = None,
        clock: Clock | None = None,
    ) -> None:
        if not using:
            raise ValueError("a database alias is required")
        for store in (batch_store, item_store):
            alias = getattr(store, "using", using)
            if alias != using:
                raise ValueError("batch stores and mutations must use exactly one database alias")
        if getattr(transaction, "using", using) != using:
            raise ValueError("batch transaction and mutations must use exactly one database alias")
        self._batches = batch_store
        self._items = item_store
        self._transaction = transaction
        self._using = using
        self._limits = limits or BatchLimits()
        self._clock = clock or SystemClock()

    def execute(
        self,
        *,
        batch_id: str,
        items: Sequence[BatchItem[PayloadT]],
        policy: BatchPolicy,
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        idempotency_policy: IdempotencyPolicy,
        namespace: str = "sync",
    ) -> BatchResult[ResultT]:
        if not batch_id or not namespace:
            raise ValueError("batch_id and namespace are required")
        _validate_items(items, self._limits)
        fingerprint = _fingerprint(
            {
                "policy": policy.value,
                "items": [
                    {"id": item.item_id, "payload": cast(JsonValue, item.payload)} for item in items
                ],
            }
        )
        if policy is BatchPolicy.ALL_OR_NOTHING:
            return self._all_or_nothing(
                namespace,
                batch_id,
                fingerprint,
                items,
                mutate,
                idempotency_policy,
            )
        if policy is BatchPolicy.PER_ITEM:
            return self._per_item(
                namespace,
                batch_id,
                fingerprint,
                items,
                mutate,
                idempotency_policy,
            )
        raise ValueError("unsupported batch policy")

    def _begin(
        self,
        namespace: str,
        batch_id: str,
        fingerprint: RequestFingerprint,
        total_items: int,
        policy: IdempotencyPolicy,
    ) -> MutationBatchBeginResult[ResultT]:
        return self._batches.begin(
            namespace,
            batch_id,
            fingerprint,
            total_items=total_items,
            now=self._clock.now(),
            lease_ttl=policy.execution_lease_ttl,
        )

    def _all_or_nothing(
        self,
        namespace: str,
        batch_id: str,
        fingerprint: RequestFingerprint,
        items: Sequence[BatchItem[PayloadT]],
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        policy: IdempotencyPolicy,
    ) -> BatchResult[ResultT]:
        try:
            with self._transaction.atomic():
                decision = self._begin(namespace, batch_id, fingerprint, len(items), policy)
                if not isinstance(decision, MutationBatchLease):
                    early = _batch_decision(decision)
                    if isinstance(early, BatchReplay):
                        return early
                    raise _RollbackBatch(early)
                lease, receipts = self._prove_prefix(decision, items, policy)
                reservations: list[
                    tuple[
                        BatchItem[PayloadT],
                        Execute | Replay[BatchItemReceipt[ResultT]],
                    ]
                ] = []
                for item in items[lease.next_index :]:
                    lease = self._renew_batch(lease, item.item_id, policy)
                    item_decision = self._reserve_item(batch_id, item, policy, namespace)
                    item_early = _item_decision(item_decision, item.item_id)
                    if item_early is not None:
                        raise _RollbackBatch(item_early)
                    assert isinstance(item_decision, (Execute, Replay))
                    reservations.append((item, item_decision))
                for item, item_decision in reservations:
                    lease, receipt = self._apply_reserved_item(
                        lease, item, item_decision, mutate, policy
                    )
                    receipts.append(receipt)
                if not self._finish(lease, policy):
                    raise _RollbackBatch(BatchUncertain(None, "final batch CAS failed"))
                return BatchCommitted(tuple(receipts))
        except _RollbackBatch as failure:
            return failure.result

    def _per_item(
        self,
        namespace: str,
        batch_id: str,
        fingerprint: RequestFingerprint,
        items: Sequence[BatchItem[PayloadT]],
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        policy: IdempotencyPolicy,
    ) -> BatchResult[ResultT]:
        decision = self._begin(namespace, batch_id, fingerprint, len(items), policy)
        if not isinstance(decision, MutationBatchLease):
            return _batch_decision(decision)
        try:
            with self._transaction.atomic():
                lease, receipts = self._prove_prefix(decision, items, policy)
        except _RollbackBatch as failure:
            return failure.result
        for item in items[lease.next_index :]:
            try:
                with self._transaction.atomic():
                    lease, receipt = self._apply_item(
                        lease, batch_id, item, mutate, policy, namespace
                    )
                    receipts.append(receipt)
            except _RollbackBatch as failure:
                return failure.result
        with self._transaction.atomic():
            if not self._finish(lease, policy):
                return BatchItemsCommittedEnvelopeUnconfirmed(tuple(receipts))
        return BatchCommitted(tuple(receipts))

    def _prove_prefix(
        self,
        lease: MutationBatchLease[ResultT],
        items: Sequence[BatchItem[PayloadT]],
        policy: IdempotencyPolicy,
    ) -> tuple[MutationBatchLease[ResultT], list[BatchItemReceipt[ResultT]]]:
        if not lease.resumed:
            return lease, list(lease.receipts)
        proven: list[BatchItemReceipt[ResultT]] = []
        for item, stored in zip(items[: lease.next_index], lease.receipts, strict=True):
            lease = self._renew_batch(lease, item.item_id, policy)
            decision = self._reserve_item(lease.batch_id, item, policy, lease.namespace)
            if isinstance(decision, Replay) and decision.value == stored:
                proven.append(replace(stored, replayed=True))
                continue
            if isinstance(decision, Execute):
                self._items.abandon(decision.token, now=self._clock.now())
            reason = "a committed item receipt is no longer provable within retention"
            self._batches.mark_uncertain(
                lease,
                reason,
                now=self._clock.now(),
                retention_ttl=policy.uncertainty_retention_ttl,
            )
            raise _RollbackBatch(BatchUncertain(item.item_id, reason))
        return lease, proven

    def _apply_item(
        self,
        lease: MutationBatchLease[ResultT],
        batch_id: str,
        item: BatchItem[PayloadT],
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        policy: IdempotencyPolicy,
        namespace: str,
    ) -> tuple[MutationBatchLease[ResultT], BatchItemReceipt[ResultT]]:
        lease = self._renew_batch(lease, item.item_id, policy)
        decision = self._reserve_item(batch_id, item, policy, namespace)
        early = _item_decision(decision, item.item_id)
        if early is not None:
            raise _RollbackBatch(early)
        assert isinstance(decision, (Execute, Replay))
        return self._apply_reserved_item(lease, item, decision, mutate, policy)

    def _apply_reserved_item(
        self,
        lease: MutationBatchLease[ResultT],
        item: BatchItem[PayloadT],
        decision: Execute | Replay[BatchItemReceipt[ResultT]],
        mutate: Callable[[BatchItem[PayloadT], str], ResultT],
        policy: IdempotencyPolicy,
    ) -> tuple[MutationBatchLease[ResultT], BatchItemReceipt[ResultT]]:
        lease = self._renew_batch(lease, item.item_id, policy)
        if isinstance(decision, Replay):
            if decision.value.item_id != item.item_id:
                raise _RollbackBatch(
                    BatchUncertain(item.item_id, "stored item receipt identity does not match")
                )
            receipt = replace(decision.value, replayed=True)
        else:
            item_renewal = self._items.renew(
                decision.token,
                now=self._clock.now(),
                lease_ttl=policy.execution_lease_ttl,
            )
            if not isinstance(item_renewal, ReservationRenewed):
                raise _RollbackBatch(
                    BatchUncertain(item.item_id, "item lease renewal failed before mutation")
                )
            receipt = BatchItemReceipt(item.item_id, mutate(item, self._using))
            completed = self._items.complete(
                decision.token,
                receipt,
                now=self._clock.now(),
                retention_ttl=policy.result_retention_ttl,
            )
            if not isinstance(completed, ReservationCompleted):
                raise _RollbackBatch(
                    BatchUncertain(item.item_id, "item receipt CAS failed; mutation rolled back")
                )
        progressed = self._batches.advance(
            lease,
            replace(receipt, replayed=False),
            now=self._clock.now(),
            lease_ttl=policy.execution_lease_ttl,
        )
        if not isinstance(progressed, MutationBatchProgressed):
            raise _RollbackBatch(
                BatchUncertain(item.item_id, "batch progress CAS failed; item rolled back")
            )
        return progressed.lease, receipt

    def _renew_batch(
        self,
        lease: MutationBatchLease[ResultT],
        item_id: str | None,
        policy: IdempotencyPolicy,
    ) -> MutationBatchLease[ResultT]:
        renewal = self._batches.renew(
            lease,
            now=self._clock.now(),
            lease_ttl=policy.execution_lease_ttl,
        )
        if not isinstance(renewal, MutationBatchLeaseRenewed):
            raise _RollbackBatch(BatchUncertain(item_id, renewal.reason))
        return renewal.lease

    def _finish(self, lease: MutationBatchLease[ResultT], policy: IdempotencyPolicy) -> bool:
        renewal = self._batches.renew(
            lease,
            now=self._clock.now(),
            lease_ttl=policy.execution_lease_ttl,
        )
        if not isinstance(renewal, MutationBatchLeaseRenewed):
            return False
        completed = self._batches.complete(
            renewal.lease,
            now=self._clock.now(),
            retention_ttl=policy.result_retention_ttl,
        )
        return isinstance(completed, MutationBatchCompleted)

    def _reserve_item(
        self,
        batch_id: str,
        item: BatchItem[PayloadT],
        policy: IdempotencyPolicy,
        namespace: str,
    ) -> Execute | Replay[BatchItemReceipt[ResultT]] | Conflict | InProgress | Uncertain:
        scope = IdempotencyScope(namespace, batch_id, f"mutation-item:{item.item_id}")
        return self._items.reserve(
            scope,
            item.item_id,
            _fingerprint(cast(JsonValue, item.payload)),
            now=self._clock.now(),
            lease_ttl=policy.execution_lease_ttl,
        )


def _batch_decision[ResultT](
    decision: MutationBatchBeginResult[ResultT],
) -> BatchReplay[ResultT] | BatchConflict | BatchInProgress | BatchUncertain:
    if isinstance(decision, MutationBatchLease):
        raise AssertionError("an executing lease is not an early decision")
    return decision


def _item_decision(
    decision: object, item_id: str
) -> BatchConflict | BatchInProgress | BatchUncertain | None:
    if isinstance(decision, Conflict):
        return BatchConflict(item_id, decision.reason)
    if isinstance(decision, InProgress):
        return BatchInProgress(item_id, decision.retry_after)
    if isinstance(decision, Uncertain):
        return BatchUncertain(item_id, decision.reason)
    return None


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


def _validate_begin(
    namespace: str,
    batch_id: str,
    total_items: int,
    now: datetime,
    lease_ttl: timedelta,
) -> None:
    _utc(now)
    _positive(lease_ttl, "lease_ttl")
    if not namespace or not batch_id:
        raise ValueError("batch namespace and ID must not be empty")
    if isinstance(total_items, bool) or not isinstance(total_items, int) or total_items < 0:
        raise ValueError("total_items must be a non-negative integer")


def _positive(value: timedelta, name: str) -> None:
    if value <= timedelta(0):
        raise ValueError(f"{name} must be positive")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("mutation batch timestamps must be timezone-aware UTC")
