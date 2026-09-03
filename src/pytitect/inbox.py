"""Inbox de-duplication ports and a bounded process-local reference implementation."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.core import OpaqueId


@dataclass(frozen=True, slots=True)
class InboxScope:
    namespace: str
    source: str
    consumer: str

    def __post_init__(self) -> None:
        if not self.namespace or not self.source or not self.consumer:
            raise ValueError("inbox scope parts must not be empty")


@dataclass(frozen=True, slots=True)
class InboxEnvelope[PayloadT]:
    scope: InboxScope
    message_id: OpaqueId[object]
    payload: PayloadT
    received_at: datetime

    def __post_init__(self) -> None:
        _utc(self.received_at)


@dataclass(frozen=True, slots=True)
class InboxAccepted:
    token: str


@dataclass(frozen=True, slots=True)
class InboxDuplicate:
    completed_at: datetime


@dataclass(frozen=True, slots=True)
class InboxInProgress:
    retry_after: datetime


@dataclass(frozen=True, slots=True)
class InboxCapacityExceeded:
    capacity: int


type InboxDecision = InboxAccepted | InboxDuplicate | InboxInProgress | InboxCapacityExceeded


class InboxStore(Protocol):
    def begin(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision: ...

    def complete(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool: ...

    def abandon(self, scope: InboxScope, message_id: OpaqueId[object], *, token: str) -> bool: ...


@dataclass(slots=True)
class _InboxEntry:
    token: str
    expires_at: datetime
    completed_at: datetime | None = None


class InMemoryInboxStore:
    """Finite process-local reference store with no cross-process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._entries: OrderedDict[tuple[InboxScope, OpaqueId[object]], _InboxEntry] = OrderedDict()
        self._lock = threading.RLock()

    def begin(
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
        identity = (scope, message_id)
        with self._lock:
            entry = self._entries.get(identity)
            if entry is not None:
                if entry.completed_at is not None:
                    return InboxDuplicate(entry.completed_at)
                if entry.expires_at > now:
                    return InboxInProgress(entry.expires_at)
                self._entries.pop(identity)
            if len(self._entries) >= self._capacity:
                return InboxCapacityExceeded(self._capacity)
            self._entries[identity] = _InboxEntry(token, now + ttl)
            return InboxAccepted(token)

    def complete(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool:
        _utc(now)
        with self._lock:
            entry = self._entries.get((scope, message_id))
            if entry is None or entry.token != token or entry.expires_at <= now:
                return False
            entry.completed_at = now
            return True

    def abandon(self, scope: InboxScope, message_id: OpaqueId[object], *, token: str) -> bool:
        with self._lock:
            identity = (scope, message_id)
            entry = self._entries.get(identity)
            if entry is None or entry.token != token or entry.completed_at is not None:
                return False
            self._entries.pop(identity)
            return True


class InboxStoreHarness:
    """Reusable behavioral contract for scoped inbox stores."""

    def __init__(self, factory: Callable[[], InboxStore]) -> None:
        self._factory = factory

    def exercise(self, *, now: datetime) -> None:
        store = self._factory()
        ttl = timedelta(minutes=1)
        message_id: OpaqueId[object] = OpaqueId("message")
        scope = InboxScope("test", "source", "consumer-a")
        other_scope = InboxScope("test", "source", "consumer-b")
        accepted = store.begin(scope, message_id, token="one", now=now, ttl=ttl)
        if not isinstance(accepted, InboxAccepted):
            raise AssertionError("a new scoped inbox identity must be accepted")
        if not isinstance(
            store.begin(scope, message_id, token="two", now=now, ttl=ttl),
            InboxInProgress,
        ):
            raise AssertionError("an active scoped inbox identity must be in progress")
        if not isinstance(
            store.begin(other_scope, message_id, token="other", now=now, ttl=ttl),
            InboxAccepted,
        ):
            raise AssertionError("the same message ID must be isolated by inbox scope")
        if store.complete(scope, message_id, token="wrong", now=now):
            raise AssertionError("a non-authoritative inbox token must not complete")
        if not store.complete(scope, message_id, token="one", now=now):
            raise AssertionError("the authoritative inbox token must complete")
        if not isinstance(
            store.begin(scope, message_id, token="three", now=now, ttl=ttl),
            InboxDuplicate,
        ):
            raise AssertionError("a completed scoped inbox identity must remain duplicate")
        if store.abandon(scope, message_id, token="one"):
            raise AssertionError("a completed inbox identity must not be abandoned")

        expiring: OpaqueId[object] = OpaqueId("expiring")
        first = store.begin(scope, expiring, token="old", now=now, ttl=ttl)
        if not isinstance(first, InboxAccepted):
            raise AssertionError("a second inbox identity must be accepted")
        resumed = store.begin(scope, expiring, token="new", now=now + ttl, ttl=ttl)
        if not isinstance(resumed, InboxAccepted):
            raise AssertionError("an expired inbox reservation must be replaceable")
        if store.abandon(scope, expiring, token="old"):
            raise AssertionError("a replaced inbox token must be stale")
        if not store.abandon(scope, expiring, token="new"):
            raise AssertionError("the current inbox reservation must be abandonable")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("inbox timestamps must be timezone-aware UTC")
