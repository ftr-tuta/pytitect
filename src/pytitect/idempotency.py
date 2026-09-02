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


class IdempotencyStore(Protocol[T]):
    def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> IdempotencyDecision[T]: ...

    def complete(self, token: ReservationToken, value: T, *, now: datetime) -> bool: ...

    def mark_uncertain(self, token: ReservationToken, reason: str, *, now: datetime) -> bool: ...


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
        ttl: timedelta,
    ) -> IdempotencyDecision[T]:
        _utc(now)
        if not key:
            raise ValueError("idempotency key must be supplied by the consumer")
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
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
            self._entries[identity] = _Entry(token, fingerprint, now + ttl)
            self._tokens[token] = identity
            return Execute(token)

    def complete(self, token: ReservationToken, value: T, *, now: datetime) -> bool:
        _utc(now)
        with self._lock:
            identity = self._tokens.get(token)
            if identity is None:
                return False
            entry = self._entries.get(identity)
            if entry is None or entry.token != token or entry.expires_at <= now:
                return False
            entry.state = "completed"
            entry.value = value
            return True

    def mark_uncertain(self, token: ReservationToken, reason: str, *, now: datetime) -> bool:
        _utc(now)
        if not reason:
            raise ValueError("an uncertainty reason is required")
        with self._lock:
            identity = self._tokens.get(token)
            if identity is None:
                return False
            entry = self._entries.get(identity)
            if entry is None or entry.token != token or entry.expires_at <= now:
                return False
            entry.state = "uncertain"
            entry.reason = reason
            return True

    def _purge(self, now: datetime) -> None:
        expired = [identity for identity, entry in self._entries.items() if entry.expires_at <= now]
        for identity in expired:
            entry = self._entries.pop(identity)
            self._tokens.pop(entry.token, None)


@dataclass(frozen=True, slots=True)
class IdempotencyCoordinator[T]:
    store: IdempotencyStore[T]
    ttl: timedelta
    clock: Clock = field(default_factory=SystemClock)

    def __post_init__(self) -> None:
        if self.ttl <= timedelta(0):
            raise ValueError("ttl must be positive")

    def begin(
        self,
        *,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
    ) -> IdempotencyDecision[T]:
        return self.store.reserve(scope, key, fingerprint, now=self.clock.now(), ttl=self.ttl)

    def complete(self, token: ReservationToken, value: T) -> bool:
        return self.store.complete(token, value, now=self.clock.now())

    def uncertain(self, token: ReservationToken, reason: str) -> bool:
        return self.store.mark_uncertain(token, reason, now=self.clock.now())


class IdempotencyStoreHarness[T]:
    """Minimal reusable conformance harness for consumer store implementations."""

    def __init__(self, factory: Callable[[], IdempotencyStore[T]]) -> None:
        self._factory = factory

    def exercise(self, *, value: T, now: datetime) -> None:
        store = self._factory()
        scope = IdempotencyScope("test", "subject", "operation")
        first = RequestFingerprint.from_json({"value": 1})
        different = RequestFingerprint.from_json({"value": 2})
        decision = store.reserve(scope, "consumer-key", first, now=now, ttl=timedelta(minutes=1))
        if not isinstance(decision, Execute):
            raise AssertionError("first reservation must execute")
        if not isinstance(
            store.reserve(scope, "consumer-key", first, now=now, ttl=timedelta(minutes=1)),
            InProgress,
        ):
            raise AssertionError("concurrent reservation must be in progress")
        if not isinstance(
            store.reserve(scope, "consumer-key", different, now=now, ttl=timedelta(minutes=1)),
            Conflict,
        ):
            raise AssertionError("different fingerprint must conflict")
        if not store.complete(decision.token, value, now=now):
            raise AssertionError("valid reservation must complete")
        replay = store.reserve(scope, "consumer-key", first, now=now, ttl=timedelta(minutes=1))
        if not isinstance(replay, Replay) or replay.value != value:
            raise AssertionError("completed reservation must replay")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("idempotency timestamps must be timezone-aware UTC")
