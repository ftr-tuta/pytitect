"""Atomic replay decisions and bounded in-memory storage."""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ReplayAccepted:
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class ReplayDetected:
    first_seen_until: datetime


@dataclass(frozen=True, slots=True)
class ReplayCapacityExceeded:
    capacity: int


type ReplayDecision = ReplayAccepted | ReplayDetected | ReplayCapacityExceeded


class ReplayStore(Protocol):
    def reserve(
        self, namespace: str, value: str, *, now: datetime, ttl: timedelta
    ) -> ReplayDecision: ...


class InMemoryReplayStore:
    def __init__(self, *, capacity: int = 10_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._entries: OrderedDict[tuple[str, str], datetime] = OrderedDict()
        self._lock = threading.RLock()

    def reserve(
        self, namespace: str, value: str, *, now: datetime, ttl: timedelta
    ) -> ReplayDecision:
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise ValueError("replay timestamps must be timezone-aware UTC")
        if not namespace or not value or ttl <= timedelta(0):
            raise ValueError("namespace, value, and positive ttl are required")
        identity = (namespace, value)
        with self._lock:
            for expired in [key for key, until in self._entries.items() if until <= now]:
                self._entries.pop(expired)
            existing = self._entries.get(identity)
            if existing is not None:
                return ReplayDetected(existing)
            if len(self._entries) >= self._capacity:
                return ReplayCapacityExceeded(self._capacity)
            expires_at = now + ttl
            self._entries[identity] = expires_at
            return ReplayAccepted(expires_at)
