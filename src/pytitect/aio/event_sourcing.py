"""Preview async workflow ports and finite process-local reference adapters."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from pytitect.event_sourcing import (
    AppendResult,
    EventPage,
    InMemoryEventStore,
    NewEvent,
    Snapshot,
    StreamId,
)


class AsyncEventStore(Protocol):
    async def append(
        self, stream: StreamId, *, expected_version: int, events: Sequence[NewEvent]
    ) -> AppendResult: ...

    async def read_stream(
        self, stream: StreamId, *, after_version: int, limit: int
    ) -> EventPage: ...

    async def read_all(self, *, after_position: int, limit: int) -> EventPage: ...

    async def load_snapshot(self, stream: StreamId) -> Snapshot | None: ...

    async def save_snapshot(self, snapshot: Snapshot, *, expected_version: int | None) -> bool: ...

    async def watermark(self) -> int: ...


class InMemoryAsyncEventStore:
    """Finite process-local reference; no durability or cross-process fencing."""

    def __init__(self, *, capacity: int = 10_000) -> None:

        self._store = InMemoryEventStore(capacity=capacity)

    async def append(
        self, stream: StreamId, *, expected_version: int, events: Sequence[NewEvent]
    ) -> AppendResult:

        return self._store.append(stream, expected_version=expected_version, events=events)

    async def read_stream(self, stream: StreamId, *, after_version: int, limit: int) -> EventPage:

        return self._store.read_stream(stream, after_version=after_version, limit=limit)

    async def read_all(self, *, after_position: int, limit: int) -> EventPage:

        return self._store.read_all(after_position=after_position, limit=limit)

    async def load_snapshot(self, stream: StreamId) -> Snapshot | None:

        return self._store.load_snapshot(stream)

    async def save_snapshot(self, snapshot: Snapshot, *, expected_version: int | None) -> bool:

        return self._store.save_snapshot(snapshot, expected_version=expected_version)

    async def watermark(self) -> int:

        return len(self._store._all)
