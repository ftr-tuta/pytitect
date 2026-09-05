"""Outbox ports, bounded reference store, and a one-round dispatcher."""

from __future__ import annotations

import math
import threading
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Protocol, TypeVar, cast

from pytitect.core import Clock, OpaqueId, SystemClock
from pytitect.maintenance import (
    ArchiveFailedOutboxPlan,
    MaintenanceSummary,
    PurgeDeliveredOutboxPlan,
)

PayloadT = TypeVar("PayloadT")
PayloadT_co = TypeVar("PayloadT_co", covariant=True)


@dataclass(frozen=True, slots=True)
class OutboxEnvelope[PayloadT]:
    message_id: OpaqueId[object]
    topic: str
    payload: PayloadT
    occurred_at: datetime
    available_at: datetime
    attempt: int = 0

    def __post_init__(self) -> None:
        _utc(self.occurred_at)
        _utc(self.available_at)
        if not self.topic:
            raise ValueError("outbox topic must not be empty")
        if self.attempt < 0:
            raise ValueError("attempt must not be negative")


@dataclass(frozen=True, slots=True)
class OutboxClaim[PayloadT]:
    claim_id: str
    envelope: OutboxEnvelope[PayloadT]
    claimed_until: datetime

    def __post_init__(self) -> None:
        _utc(self.claimed_until)


@dataclass(frozen=True, slots=True)
class FailedOutboxEnvelope[PayloadT]:
    envelope: OutboxEnvelope[PayloadT]
    reason: str
    failed_at: datetime

    def __post_init__(self) -> None:
        _utc(self.failed_at)
        if not self.reason:
            raise ValueError("outbox failure reason must not be empty")


@dataclass(frozen=True, slots=True)
class Delivered:
    pass


@dataclass(frozen=True, slots=True)
class Retryable:
    reason: str

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("retry reason must not be empty")


@dataclass(frozen=True, slots=True)
class PermanentFailure:
    reason: str

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("permanent failure reason must not be empty")


type DeliveryResult = Delivered | Retryable | PermanentFailure


@dataclass(frozen=True, slots=True)
class OutboxAdded:
    pass


@dataclass(frozen=True, slots=True)
class OutboxDuplicate:
    pass


type OutboxAddResult = OutboxAdded | OutboxDuplicate


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    initial_delay: timedelta = timedelta(seconds=1)
    multiplier: float = 2.0
    maximum_delay: timedelta = timedelta(minutes=15)
    max_attempts: int = 10

    def __post_init__(self) -> None:
        if self.initial_delay <= timedelta(0) or self.maximum_delay <= timedelta(0):
            raise ValueError("retry delays must be positive")
        if (
            not math.isfinite(self.multiplier)
            or self.multiplier < 1
            or isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts <= 0
        ):
            raise ValueError("retry multiplier and max_attempts are invalid")

    def delay(self, attempt: int) -> timedelta:
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError("attempt must be at least one")
        if self.initial_delay >= self.maximum_delay:
            return self.maximum_delay
        if self.multiplier == 1:
            return self.initial_delay
        # Compare exponents before evaluating either the power or timedelta product.
        ceiling = math.log(self.maximum_delay / self.initial_delay) / math.log(self.multiplier)
        if attempt - 1 >= ceiling:
            return self.maximum_delay
        return min(self.initial_delay * self.multiplier ** (attempt - 1), self.maximum_delay)


class OutboxStore(Protocol[PayloadT]):
    def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult: ...

    def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[OutboxClaim[PayloadT]]: ...

    def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> bool: ...

    def retry(self, claim: OutboxClaim[PayloadT], *, available_at: datetime) -> bool: ...

    def failed(self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime) -> bool: ...


class OutboxMaintenanceStore(Protocol[PayloadT_co]):
    def purge_delivered(self, plan: PurgeDeliveredOutboxPlan) -> MaintenanceSummary: ...

    def archive_failed(
        self,
        plan: ArchiveFailedOutboxPlan,
        archive: Callable[[Sequence[FailedOutboxEnvelope[PayloadT_co]]], None],
    ) -> MaintenanceSummary: ...


@dataclass(slots=True)
class _Stored[PayloadT]:
    envelope: OutboxEnvelope[PayloadT]
    claim_id: str | None = None
    claimed_until: datetime | None = None
    delivered_at: datetime | None = None
    failure_reason: str | None = None
    failed_at: datetime | None = None


class InMemoryOutboxStore[PayloadT]:
    """Finite process-local reference store with no cross-process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._items: dict[OpaqueId[object], _Stored[PayloadT]] = {}
        self._lock = threading.RLock()

    def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        with self._lock:
            if envelope.message_id in self._items:
                return OutboxDuplicate()
            if len(self._items) >= self._capacity:
                raise OverflowError("outbox capacity exceeded")
            self._items[envelope.message_id] = _Stored(envelope)
            return OutboxAdded()

    def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[OutboxClaim[PayloadT]]:
        _utc(now)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("claim limit must be a positive integer")
        if claim_ttl <= timedelta(0):
            raise ValueError("claim limit and ttl must be positive")
        claimed: list[OutboxClaim[PayloadT]] = []
        with self._lock:
            eligible = sorted(
                self._items.values(),
                key=lambda item: (item.envelope.available_at, str(item.envelope.message_id)),
            )
            for item in eligible:
                if len(claimed) >= limit:
                    break
                if item.envelope.available_at > now:
                    continue
                if item.delivered_at is not None or item.failure_reason is not None:
                    continue
                if item.claimed_until is not None and item.claimed_until > now:
                    continue
                item.claim_id = uuid.uuid4().hex
                item.claimed_until = now + claim_ttl
                claimed.append(OutboxClaim(item.claim_id, item.envelope, item.claimed_until))
        return claimed

    def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> bool:
        _utc(at)
        with self._lock:
            item = self._valid(claim)
            if item is None:
                return False
            item.delivered_at = at
            item.claim_id = None
            item.claimed_until = None
            return True

    def retry(self, claim: OutboxClaim[PayloadT], *, available_at: datetime) -> bool:
        _utc(available_at)
        with self._lock:
            item = self._valid(claim)
            if item is None:
                return False
            item.envelope = replace(
                item.envelope,
                attempt=item.envelope.attempt + 1,
                available_at=available_at,
            )
            item.claim_id = None
            item.claimed_until = None
            return True

    def failed(self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime) -> bool:
        _utc(at)
        if not reason:
            raise ValueError("outbox failure reason must not be empty")
        with self._lock:
            item = self._valid(claim)
            if item is None:
                return False
            item.failure_reason = reason
            item.failed_at = at
            item.claim_id = None
            item.claimed_until = None
            return True

    def purge_delivered(self, plan: PurgeDeliveredOutboxPlan) -> MaintenanceSummary:
        with self._lock:
            selected = sorted(
                (
                    item
                    for item in self._items.values()
                    if item.delivered_at is not None and item.delivered_at <= plan.cutoff
                ),
                key=lambda item: (item.delivered_at, str(item.envelope.message_id)),
            )[: plan.batch_size]
            if not plan.dry_run:
                for item in selected:
                    self._items.pop(item.envelope.message_id)
            return MaintenanceSummary(
                len(selected), 0 if plan.dry_run else len(selected), plan.dry_run
            )

    def archive_failed(
        self,
        plan: ArchiveFailedOutboxPlan,
        archive: Callable[[Sequence[FailedOutboxEnvelope[PayloadT]]], None],
    ) -> MaintenanceSummary:
        with self._lock:
            selected = sorted(
                (
                    item
                    for item in self._items.values()
                    if item.failure_reason is not None
                    and item.failed_at is not None
                    and item.failed_at <= plan.cutoff
                ),
                key=lambda item: (item.failed_at, str(item.envelope.message_id)),
            )[: plan.batch_size]
            if not plan.dry_run and selected:
                archive(
                    tuple(
                        FailedOutboxEnvelope(
                            item.envelope,
                            cast(str, item.failure_reason),
                            cast(datetime, item.failed_at),
                        )
                        for item in selected
                    )
                )
                for item in selected:
                    self._items.pop(item.envelope.message_id)
            return MaintenanceSummary(
                len(selected), 0 if plan.dry_run else len(selected), plan.dry_run
            )

    def _valid(self, claim: OutboxClaim[PayloadT]) -> _Stored[PayloadT] | None:
        item = self._items.get(claim.envelope.message_id)
        if (
            item is None
            or item.claim_id != claim.claim_id
            or item.delivered_at is not None
            or item.failure_reason is not None
        ):
            return None
        return item


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    claimed: int
    delivered: int
    retried: int
    failed: int


class OneRoundDispatcher[PayloadT]:
    def __init__(
        self,
        store: OutboxStore[PayloadT],
        handler: Callable[[OutboxEnvelope[PayloadT]], DeliveryResult],
        *,
        retry_policy: RetryPolicy | None = None,
        clock: Clock | None = None,
        claim_ttl: timedelta = timedelta(minutes=1),
    ) -> None:
        self._store = store
        self._handler = handler
        self._retry = retry_policy or RetryPolicy()
        self._clock = clock or SystemClock()
        self._claim_ttl = claim_ttl

    def dispatch(self, *, limit: int) -> DispatchSummary:
        now = self._clock.now()
        claims = self._store.claim(now=now, limit=limit, claim_ttl=self._claim_ttl)
        delivered = retried = failed = 0
        for claim in claims:
            result = self._handler(claim.envelope)
            if isinstance(result, Delivered):
                delivered += int(self._store.delivered(claim, at=now))
            elif (
                isinstance(result, Retryable)
                and claim.envelope.attempt + 1 < self._retry.max_attempts
            ):
                next_attempt = claim.envelope.attempt + 1
                available_at = now + self._retry.delay(next_attempt)
                retried += int(self._store.retry(claim, available_at=available_at))
            else:
                reason = (
                    result.reason if isinstance(result, (Retryable, PermanentFailure)) else "failed"
                )
                failed += int(self._store.failed(claim, reason=reason, at=now))
        return DispatchSummary(len(claims), delivered, retried, failed)


class OutboxStoreHarness[PayloadT]:
    """Reusable behavioral contract for outbox stores."""

    def __init__(self, factory: Callable[[], OutboxStore[PayloadT]]) -> None:
        self._factory = factory

    def exercise(self, *, payload: PayloadT, now: datetime) -> None:
        store = self._factory()
        early = OutboxEnvelope(OpaqueId("early"), "events", payload, now, now)
        late = OutboxEnvelope(OpaqueId("late"), "events", payload, now, now + timedelta(minutes=1))
        if not isinstance(store.add(late), OutboxAdded):
            raise AssertionError("a new outbox identity must be added")
        if not isinstance(store.add(early), OutboxAdded):
            raise AssertionError("a second outbox identity must be added")
        if not isinstance(store.add(early), OutboxDuplicate):
            raise AssertionError("an existing outbox identity must be rejected as duplicate")
        claims = store.claim(now=now, limit=1, claim_ttl=timedelta(seconds=30))
        if len(claims) != 1 or claims[0].envelope != early:
            raise AssertionError("outbox claims must be finite, eligible, and ordered")
        claim = claims[0]
        try:
            store.failed(claim, reason="", at=now)
        except ValueError:
            pass
        else:
            raise AssertionError("an empty terminal failure reason must be rejected")
        retry_at = now + timedelta(seconds=10)
        if not store.retry(claim, available_at=retry_at):
            raise AssertionError("an authoritative claim must be retryable")
        if store.retry(claim, available_at=retry_at):
            raise AssertionError("a released claim must become stale")
        if store.claim(now=now, limit=1, claim_ttl=timedelta(seconds=30)):
            raise AssertionError("a delayed outbox item must not be claimed early")
        retried = store.claim(now=retry_at, limit=1, claim_ttl=timedelta(seconds=30))
        if len(retried) != 1 or retried[0].envelope.attempt != 1:
            raise AssertionError("a retried outbox item must increment its attempt")
        if not store.failed(retried[0], reason="terminal", at=retry_at):
            raise AssertionError("an authoritative claim must accept terminal failure")
        if not isinstance(store.add(early), OutboxDuplicate):
            raise AssertionError("a terminal failure must keep its identity reserved")
        delivered = store.claim(
            now=now + timedelta(minutes=1), limit=1, claim_ttl=timedelta(seconds=30)
        )
        if len(delivered) != 1 or not store.delivered(delivered[0], at=now + timedelta(minutes=1)):
            raise AssertionError("an authoritative eligible claim must be deliverable")
        if store.delivered(delivered[0], at=now + timedelta(minutes=1)):
            raise AssertionError("a delivered claim must become stale")
        if not isinstance(store.add(late), OutboxDuplicate):
            raise AssertionError("a delivered outbox identity must remain reserved")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("outbox timestamps must be timezone-aware UTC")
