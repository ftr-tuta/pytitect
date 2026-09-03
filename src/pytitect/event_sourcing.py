"""Preview optimistic event streams, bounded pages, and optional snapshots."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Protocol

from pytitect.core import JsonScalar, JsonValue, validate_json


@dataclass(frozen=True, slots=True)
class StreamId:
    category: str
    stream_id: str

    def __post_init__(self) -> None:
        if not self.category or not self.stream_id:
            raise ValueError("stream category and ID must not be empty")


@dataclass(frozen=True, slots=True)
class NewEvent:
    event_id: str
    event_type: str
    payload: JsonValue
    occurred_at: datetime
    metadata: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.event_id or not self.event_type:
            raise ValueError("event identity and type must not be empty")
        validate_json(self.payload)
        _utc(self.occurred_at)
        if len(self.metadata) > 32:
            raise ValueError("event metadata exceeds 32 items")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class StoredEvent:
    stream: StreamId
    stream_version: int
    global_position: int
    event: NewEvent

    def __post_init__(self) -> None:
        if self.stream_version <= 0 or self.global_position <= 0:
            raise ValueError("stored event positions must be positive")


@dataclass(frozen=True, slots=True)
class AppendCommitted:
    first_version: int
    last_version: int
    events: tuple[StoredEvent, ...]


@dataclass(frozen=True, slots=True)
class WrongExpectedVersion:
    expected: int
    actual: int


@dataclass(frozen=True, slots=True)
class DuplicateEventId:
    event_id: str


type AppendResult = AppendCommitted | WrongExpectedVersion | DuplicateEventId


@dataclass(frozen=True, slots=True)
class EventPage:
    events: tuple[StoredEvent, ...]
    next_position: int
    complete: bool


@dataclass(frozen=True, slots=True)
class Snapshot:
    stream: StreamId
    version: int
    state: JsonValue
    created_at: datetime

    def __post_init__(self) -> None:
        if self.version <= 0:
            raise ValueError("snapshot version must be positive")
        validate_json(self.state)
        _utc(self.created_at)


class EventStore(Protocol):
    def append(
        self, stream: StreamId, *, expected_version: int, events: Sequence[NewEvent]
    ) -> AppendResult: ...

    def read_stream(self, stream: StreamId, *, after_version: int, limit: int) -> EventPage: ...

    def read_all(self, *, after_position: int, limit: int) -> EventPage: ...

    def load_snapshot(self, stream: StreamId) -> Snapshot | None: ...

    def save_snapshot(self, snapshot: Snapshot, *, expected_version: int | None) -> bool: ...


class InMemoryEventStore:
    """Finite process-local event store with no durability or cross-process coordination."""

    def __init__(self, *, capacity: int = 100_000, max_page_size: int = 1_000) -> None:
        if capacity <= 0 or max_page_size <= 0:
            raise ValueError("event capacity and max_page_size must be positive")
        self._capacity = capacity
        self._max_page_size = max_page_size
        self._streams: dict[StreamId, list[StoredEvent]] = {}
        self._all: list[StoredEvent] = []
        self._event_ids: set[str] = set()
        self._snapshots: dict[StreamId, Snapshot] = {}
        self._lock = threading.RLock()

    def append(
        self, stream: StreamId, *, expected_version: int, events: Sequence[NewEvent]
    ) -> AppendResult:
        if expected_version < 0:
            raise ValueError("expected stream version must not be negative")
        if not events:
            raise ValueError("an append requires at least one event")
        identifiers = [event.event_id for event in events]
        if len(identifiers) != len(set(identifiers)):
            return DuplicateEventId(
                next(identifier for identifier in identifiers if identifiers.count(identifier) > 1)
            )
        with self._lock:
            current = self._streams.get(stream, [])
            actual = len(current)
            if actual != expected_version:
                return WrongExpectedVersion(expected_version, actual)
            duplicate = next(
                (identifier for identifier in identifiers if identifier in self._event_ids), None
            )
            if duplicate is not None:
                return DuplicateEventId(duplicate)
            if len(self._all) + len(events) > self._capacity:
                raise OverflowError("event store capacity exceeded")
            stored = tuple(
                StoredEvent(
                    stream,
                    actual + index,
                    len(self._all) + index,
                    event,
                )
                for index, event in enumerate(events, start=1)
            )
            self._streams.setdefault(stream, []).extend(stored)
            self._all.extend(stored)
            self._event_ids.update(identifiers)
            return AppendCommitted(stored[0].stream_version, stored[-1].stream_version, stored)

    def read_stream(self, stream: StreamId, *, after_version: int = 0, limit: int) -> EventPage:
        self._page_arguments(after_version, limit)
        with self._lock:
            available = [
                event
                for event in self._streams.get(stream, [])
                if event.stream_version > after_version
            ]
            selected = tuple(available[:limit])
            next_position = after_version if not selected else selected[-1].stream_version
            return EventPage(selected, next_position, len(available) <= limit)

    def read_all(self, *, after_position: int = 0, limit: int) -> EventPage:
        self._page_arguments(after_position, limit)
        with self._lock:
            available = [event for event in self._all if event.global_position > after_position]
            selected = tuple(available[:limit])
            next_position = after_position if not selected else selected[-1].global_position
            return EventPage(selected, next_position, len(available) <= limit)

    def load_snapshot(self, stream: StreamId) -> Snapshot | None:
        with self._lock:
            return self._snapshots.get(stream)

    def save_snapshot(self, snapshot: Snapshot, *, expected_version: int | None) -> bool:
        with self._lock:
            current = self._snapshots.get(snapshot.stream)
            actual = None if current is None else current.version
            if actual != expected_version:
                return False
            stream_version = len(self._streams.get(snapshot.stream, []))
            if snapshot.version > stream_version:
                raise ValueError("snapshot cannot be ahead of its event stream")
            self._snapshots[snapshot.stream] = snapshot
            return True

    def _page_arguments(self, position: int, limit: int) -> None:
        if position < 0:
            raise ValueError("event position must not be negative")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= self._max_page_size
        ):
            raise ValueError("event page limit is outside the configured finite range")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("event timestamps must be timezone-aware UTC")


__all__ = [
    "AppendCommitted",
    "DuplicateEventId",
    "EventPage",
    "EventStore",
    "InMemoryEventStore",
    "NewEvent",
    "Snapshot",
    "StoredEvent",
    "StreamId",
    "WrongExpectedVersion",
]
