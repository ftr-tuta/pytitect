"""Preview async workflow ports and finite process-local reference adapters."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from pytitect.aio.event_sourcing import AsyncEventStore
from pytitect.core import JsonValue, validate_json
from pytitect.event_sourcing import StoredEvent
from pytitect.projections import (
    InMemoryProjectionStore,
    ProjectionApplyResult,
    ProjectionDefinition,
    ProjectionKey,
    ProjectionState,
    ProjectionVersionMismatch,
    RebuildRun,
    RebuildStatus,
)


class AsyncProjectionStore(Protocol):
    async def load(self, key: ProjectionKey) -> ProjectionState | None: ...

    async def apply(
        self,
        key: ProjectionKey,
        *,
        expected_checkpoint: int,
        projection_version: int,
        state: JsonValue,
        events: Sequence[StoredEvent],
    ) -> ProjectionApplyResult: ...

    async def begin_rebuild(self, run: RebuildRun) -> bool: ...

    async def load_rebuild(self, run_id: str) -> RebuildRun | None: ...

    async def advance_rebuild(
        self,
        run_id: str,
        *,
        expected_position: int,
        state: JsonValue,
        next_position: int,
        complete: bool,
    ) -> RebuildRun | None: ...


class InMemoryAsyncProjectionStore:
    """Finite process-local reference; no durability or cross-process fencing."""

    def __init__(self, *, capacity: int = 10_000) -> None:

        self._store = InMemoryProjectionStore(capacity=capacity)

    async def load(self, key: ProjectionKey) -> ProjectionState | None:

        return self._store.load(key)

    async def apply(
        self,
        key: ProjectionKey,
        *,
        expected_checkpoint: int,
        projection_version: int,
        state: JsonValue,
        events: Sequence[StoredEvent],
    ) -> ProjectionApplyResult:

        return self._store.apply(
            key,
            expected_checkpoint=expected_checkpoint,
            projection_version=projection_version,
            state=state,
            events=events,
        )

    async def begin_rebuild(self, run: RebuildRun) -> bool:

        return self._store.begin_rebuild(run)

    async def load_rebuild(self, run_id: str) -> RebuildRun | None:

        return self._store.load_rebuild(run_id)

    async def advance_rebuild(
        self,
        run_id: str,
        *,
        expected_position: int,
        state: JsonValue,
        next_position: int,
        complete: bool,
    ) -> RebuildRun | None:

        return self._store.advance_rebuild(
            run_id,
            expected_position=expected_position,
            state=state,
            next_position=next_position,
            complete=complete,
        )


class AsyncProjectionRuntime:
    def __init__(self, store: AsyncProjectionStore, events: AsyncEventStore) -> None:
        self._store = store
        self._events = events

    async def project_once(
        self, key: ProjectionKey, definition: ProjectionDefinition, *, limit: int
    ) -> ProjectionApplyResult:
        current = await self._store.load(key)
        checkpoint = 0 if current is None else current.checkpoint
        if current is not None and current.version != definition.version:
            return ProjectionVersionMismatch(definition.version, current.version)
        page = await self._events.read_all(after_position=checkpoint, limit=limit)
        state = definition.initial_state if current is None else current.state
        for event in page.events:
            state = definition.reduce(state, event)
            validate_json(state)
        return await self._store.apply(
            key,
            expected_checkpoint=checkpoint,
            projection_version=definition.version,
            state=state,
            events=page.events,
        )

    async def resume_rebuild(self, run_id: str, definition: ProjectionDefinition) -> RebuildRun:
        run = await self._store.load_rebuild(run_id)
        if run is None:
            raise LookupError(f"unknown rebuild run: {run_id}")
        if run.projection_version != definition.version:
            raise ValueError("rebuild definition version changed")
        if run.status is RebuildStatus.COMPLETED:
            return run
        page = await self._events.read_all(after_position=run.next_position, limit=run.batch_size)
        selected = tuple(
            event for event in page.events if event.global_position <= run.through_position
        )
        state = run.state
        for event in selected:
            state = definition.reduce(state, event)
            validate_json(state)
        next_position = run.next_position if not selected else selected[-1].global_position
        complete = next_position == run.through_position
        if not selected and not complete:
            raise RuntimeError("rebuild watermark has missing durable coverage")
        advanced = await self._store.advance_rebuild(
            run_id,
            expected_position=run.next_position,
            state=state,
            next_position=next_position,
            complete=complete,
        )
        if advanced is None:
            raise RuntimeError("rebuild progress compare-and-set failed")
        return advanced
