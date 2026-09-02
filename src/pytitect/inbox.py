"""Inbox de-duplication ports and a bounded process-local reference implementation."""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.core import OpaqueId


@dataclass(frozen=True, slots=True)
class InboxEnvelope[PayloadT]:
    message_id: OpaqueId[object]
    source: str
    payload: PayloadT
    received_at: datetime

    def __post_init__(self) -> None:
        _utc(self.received_at)
        if not self.source:
            raise ValueError("inbox source must not be empty")


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
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision: ...

    def complete(self, message_id: OpaqueId[object], *, token: str, now: datetime) -> bool: ...

    def abandon(self, message_id: OpaqueId[object], *, token: str) -> bool: ...


@dataclass(slots=True)
class _InboxEntry:
    token: str
    expires_at: datetime
    completed_at: datetime | None = None


class InMemoryInboxStore:
    def __init__(self, *, capacity: int = 10_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._entries: OrderedDict[OpaqueId[object], _InboxEntry] = OrderedDict()
        self._lock = threading.RLock()

    def begin(
        self,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision:
        _utc(now)
        if not token or ttl <= timedelta(0):
            raise ValueError("token and a positive ttl are required")
        with self._lock:
            entry = self._entries.get(message_id)
            if entry is not None:
                if entry.completed_at is not None:
                    return InboxDuplicate(entry.completed_at)
                if entry.expires_at > now:
                    return InboxInProgress(entry.expires_at)
                self._entries.pop(message_id)
            if len(self._entries) >= self._capacity:
                return InboxCapacityExceeded(self._capacity)
            self._entries[message_id] = _InboxEntry(token, now + ttl)
            return InboxAccepted(token)

    def complete(self, message_id: OpaqueId[object], *, token: str, now: datetime) -> bool:
        _utc(now)
        with self._lock:
            entry = self._entries.get(message_id)
            if entry is None or entry.token != token or entry.expires_at <= now:
                return False
            entry.completed_at = now
            return True

    def abandon(self, message_id: OpaqueId[object], *, token: str) -> bool:
        with self._lock:
            entry = self._entries.get(message_id)
            if entry is None or entry.token != token or entry.completed_at is not None:
                return False
            self._entries.pop(message_id)
            return True


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("inbox timestamps must be timezone-aware UTC")
