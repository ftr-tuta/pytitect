"""Explicit idempotency coordination with bounded reference storage."""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

from pytitect.core import (
    Canonicalizer,
    Clock,
    Fingerprint,
    JsonValue,
    SystemClock,
    canonical_json_bytes,
    sha256_fingerprint,
)
from pytitect.maintenance import MaintenanceSummary, PurgeIdempotencyPlan

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class IdempotencyScope:
    namespace: str
    subject: str
    operation: str

    def __post_init__(self) -> None:
        if not self.namespace or not self.subject or not self.operation:
            raise ValueError("idempotency scope parts must not be empty")


@dataclass(frozen=True, slots=True)
class ReservationToken:
    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("reservation tokens must not be empty")


@dataclass(frozen=True, slots=True)
class IdempotencyPolicy:
    """Independent execution and terminal-state retention periods."""

    execution_lease_ttl: timedelta
    result_retention_ttl: timedelta
    uncertainty_retention_ttl: timedelta

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            if getattr(self, name) <= timedelta(0):
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class RequestFingerprint:
    value: Fingerprint

    @classmethod
    def from_json(
        cls,
        value: JsonValue,
        *,
        canonicalizer: Canonicalizer = canonical_json_bytes,
    ) -> RequestFingerprint:
        return cls(sha256_fingerprint(value, canonicalizer=canonicalizer))


@dataclass(frozen=True, slots=True)
class Execute:
    token: ReservationToken


@dataclass(frozen=True, slots=True)
class Replay[T]:
    value: T


@dataclass(frozen=True, slots=True)
class Conflict:
    reason: str = "the idempotency key was used with a different request"


@dataclass(frozen=True, slots=True)
class InProgress:
    retry_after: datetime


@dataclass(frozen=True, slots=True)
class Uncertain:
    reason: str


type IdempotencyDecision[T] = Execute | Replay[T] | Conflict | InProgress | Uncertain


@dataclass(frozen=True, slots=True)
class ReservationRenewed:
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class ReservationCompleted:
    retained_until: datetime


@dataclass(frozen=True, slots=True)
class ReservationMarkedUncertain:
    retained_until: datetime


@dataclass(frozen=True, slots=True)
class ReservationAbandoned:
    pass


@dataclass(frozen=True, slots=True)
class StaleReservation:
    reason: str = "reservation is absent, expired, or no longer executing"


type RenewReservationResult = ReservationRenewed | StaleReservation
type CompleteReservationResult = ReservationCompleted | StaleReservation
type MarkUncertainResult = ReservationMarkedUncertain | StaleReservation
type AbandonReservationResult = ReservationAbandoned | StaleReservation


class IdempotencyStore(Protocol[T]):
    def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> IdempotencyDecision[T]: ...

    def renew(
        self,
        token: ReservationToken,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> RenewReservationResult: ...

    def complete(
        self,
        token: ReservationToken,
        value: T,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> CompleteReservationResult: ...

    def mark_uncertain(
        self,
        token: ReservationToken,
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MarkUncertainResult: ...

    def abandon(
        self,
        token: ReservationToken,
        *,
        now: datetime,
    ) -> AbandonReservationResult: ...


@dataclass(slots=True)
class _Entry[T]:
    token: ReservationToken
    fingerprint: RequestFingerprint
    expires_at: datetime
    state: str = "reserved"
    value: T | None = None
    reason: str | None = None


class InMemoryIdempotencyStore[T]:
    """Thread-safe, process-local reference store with finite capacity."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._entries: OrderedDict[tuple[IdempotencyScope, str], _Entry[T]] = OrderedDict()
        self._tokens: dict[ReservationToken, tuple[IdempotencyScope, str]] = {}
        self._lock = threading.RLock()

    def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> IdempotencyDecision[T]:
        _utc(now)
        if not key:
            raise ValueError("idempotency key must be supplied by the consumer")
        _positive(lease_ttl, "lease_ttl")
        identity = (scope, key)
        with self._lock:
            self._purge(now)
            current = self._entries.get(identity)
            if current is not None:
                self._entries.move_to_end(identity)
                if current.fingerprint != fingerprint:
                    return Conflict()
                if current.state == "completed":
                    return Replay(current.value)  # type: ignore[arg-type]
                if current.state == "uncertain":
                    return Uncertain(current.reason or "the prior outcome is unknown")
                return InProgress(current.expires_at)
            if len(self._entries) >= self._capacity:
                return Uncertain("idempotency capacity exceeded; no reservation was made")
            token = ReservationToken(uuid.uuid4().hex)
            self._entries[identity] = _Entry(token, fingerprint, now + lease_ttl)
            self._tokens[token] = identity
            return Execute(token)

    def renew(
        self,
        token: ReservationToken,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> RenewReservationResult:
        _utc(now)
        _positive(lease_ttl, "lease_ttl")
        with self._lock:
            entry = self._executing(token, now)
            if entry is None:
                return StaleReservation()
            entry.expires_at = now + lease_ttl
            return ReservationRenewed(entry.expires_at)

    def complete(
        self,
        token: ReservationToken,
        value: T,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> CompleteReservationResult:
        _utc(now)
        _positive(retention_ttl, "retention_ttl")
        with self._lock:
            entry = self._executing(token, now)
            if entry is None:
                return StaleReservation()
            entry.state = "completed"
            entry.value = value
            entry.expires_at = now + retention_ttl
            return ReservationCompleted(entry.expires_at)

    def mark_uncertain(
        self,
        token: ReservationToken,
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MarkUncertainResult:
        _utc(now)
        if not reason:
            raise ValueError("an uncertainty reason is required")
        _positive(retention_ttl, "retention_ttl")
        with self._lock:
            entry = self._executing(token, now)
            if entry is None:
                return StaleReservation()
            entry.state = "uncertain"
            entry.reason = reason
            entry.expires_at = now + retention_ttl
            return ReservationMarkedUncertain(entry.expires_at)

    def abandon(
        self,
        token: ReservationToken,
        *,
        now: datetime,
    ) -> AbandonReservationResult:
        _utc(now)
        with self._lock:
            identity = self._tokens.get(token)
            entry = self._executing(token, now)
            if identity is None or entry is None:
                return StaleReservation()
            self._entries.pop(identity)
            self._tokens.pop(token)
            return ReservationAbandoned()

    def purge(self, plan: PurgeIdempotencyPlan) -> MaintenanceSummary:
        with self._lock:
            selected = sorted(
                (
                    (identity, entry)
                    for identity, entry in self._entries.items()
                    if entry.expires_at <= plan.cutoff
                    and (entry.state != "uncertain" or plan.include_uncertain)
                ),
                key=lambda item: (
                    item[1].expires_at,
                    item[0][0].namespace,
                    item[0][0].subject,
                    item[0][0].operation,
                    item[0][1],
                ),
            )[: plan.batch_size]
            if not plan.dry_run:
                for identity, entry in selected:
                    self._entries.pop(identity)
                    self._tokens.pop(entry.token, None)
            return MaintenanceSummary(
                len(selected), 0 if plan.dry_run else len(selected), plan.dry_run
            )

    def _executing(self, token: ReservationToken, now: datetime) -> _Entry[T] | None:
        identity = self._tokens.get(token)
        if identity is None:
            return None
        entry = self._entries.get(identity)
        if (
            entry is None
            or entry.token != token
            or entry.state != "reserved"
            or entry.expires_at <= now
        ):
            return None
        return entry

    def _purge(self, now: datetime) -> None:
        expired = [
            identity
            for identity, entry in self._entries.items()
            if entry.expires_at <= now and entry.state != "uncertain"
        ]
        for identity in expired:
            entry = self._entries.pop(identity)
            self._tokens.pop(entry.token, None)


@dataclass(frozen=True, slots=True)
class IdempotencyCoordinator[T]:
    store: IdempotencyStore[T]
    policy: IdempotencyPolicy
    clock: Clock = field(default_factory=SystemClock)

    def begin(
        self,
        *,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
    ) -> IdempotencyDecision[T]:
        return self.store.reserve(
            scope,
            key,
            fingerprint,
            now=self.clock.now(),
            lease_ttl=self.policy.execution_lease_ttl,
        )

    def renew(self, token: ReservationToken) -> RenewReservationResult:
        return self.store.renew(
            token,
            now=self.clock.now(),
            lease_ttl=self.policy.execution_lease_ttl,
        )

    def complete(self, token: ReservationToken, value: T) -> CompleteReservationResult:
        return self.store.complete(
            token,
            value,
            now=self.clock.now(),
            retention_ttl=self.policy.result_retention_ttl,
        )

    def uncertain(self, token: ReservationToken, reason: str) -> MarkUncertainResult:
        return self.store.mark_uncertain(
            token,
            reason,
            now=self.clock.now(),
            retention_ttl=self.policy.uncertainty_retention_ttl,
        )

    def abandon(self, token: ReservationToken) -> AbandonReservationResult:
        return self.store.abandon(token, now=self.clock.now())


class IdempotencyStoreHarness[T]:
    """Minimal reusable conformance harness for consumer store implementations."""

    def __init__(self, factory: Callable[[], IdempotencyStore[T]]) -> None:
        self._factory = factory

    def exercise(self, *, value: T, now: datetime) -> None:
        store = self._factory()
        lease_ttl = timedelta(minutes=1)
        retention_ttl = timedelta(hours=1)
        scope = IdempotencyScope("test", "subject", "operation")
        first = RequestFingerprint.from_json({"value": 1})
        different = RequestFingerprint.from_json({"value": 2})
        decision = store.reserve(scope, "consumer-key", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(decision, Execute):
            raise AssertionError("first reservation must execute")
        if not isinstance(
            store.reserve(scope, "consumer-key", first, now=now, lease_ttl=lease_ttl),
            InProgress,
        ):
            raise AssertionError("concurrent reservation must be in progress")
        if not isinstance(
            store.reserve(scope, "consumer-key", different, now=now, lease_ttl=lease_ttl),
            Conflict,
        ):
            raise AssertionError("different fingerprint must conflict")
        renewed_at = now + timedelta(seconds=1)
        if not isinstance(
            store.renew(decision.token, now=renewed_at, lease_ttl=lease_ttl),
            ReservationRenewed,
        ):
            raise AssertionError("an authoritative reservation must renew")
        if not isinstance(
            store.complete(
                decision.token,
                value,
                now=renewed_at,
                retention_ttl=retention_ttl,
            ),
            ReservationCompleted,
        ):
            raise AssertionError("valid reservation must complete")
        replay = store.reserve(
            scope,
            "consumer-key",
            first,
            now=now + lease_ttl,
            lease_ttl=lease_ttl,
        )
        if not isinstance(replay, Replay) or replay.value != value:
            raise AssertionError("completed reservation must replay")

        uncertain_decision = store.reserve(scope, "uncertain", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(uncertain_decision, Execute):
            raise AssertionError("a new uncertainty identity must execute")
        if not isinstance(
            store.mark_uncertain(
                uncertain_decision.token,
                "outcome unknown",
                now=now,
                retention_ttl=retention_ttl,
            ),
            ReservationMarkedUncertain,
        ):
            raise AssertionError("an authoritative reservation must become uncertain")
        if not isinstance(
            store.reserve(scope, "uncertain", first, now=now, lease_ttl=lease_ttl),
            Uncertain,
        ):
            raise AssertionError("an uncertain result must remain retained")

        abandoned = store.reserve(scope, "abandoned", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(abandoned, Execute):
            raise AssertionError("a new abandon identity must execute")
        if not isinstance(store.abandon(abandoned.token, now=now), ReservationAbandoned):
            raise AssertionError("an authoritative reservation must be abandonable")
        if not isinstance(
            store.reserve(scope, "abandoned", first, now=now, lease_ttl=lease_ttl), Execute
        ):
            raise AssertionError("an abandoned identity must be reusable")

        expiring = store.reserve(scope, "expiring", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(expiring, Execute):
            raise AssertionError("a new expiring identity must execute")
        replacement = store.reserve(
            scope, "expiring", first, now=now + lease_ttl, lease_ttl=lease_ttl
        )
        if not isinstance(replacement, Execute) or replacement.token == expiring.token:
            raise AssertionError("an expired execution reservation must be replaced")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("idempotency timestamps must be timezone-aware UTC")


def _positive(value: timedelta, name: str) -> None:
    if value <= timedelta(0):
        raise ValueError(f"{name} must be positive")
