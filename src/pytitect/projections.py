"""Preview versioned projections and finite resumable rebuilds."""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import pairwise
from typing import Protocol

from pytitect.core import JsonValue, validate_json
from pytitect.event_sourcing import EventStore, StoredEvent


@dataclass(frozen=True, slots=True)
class ProjectionKey:
    name: str
    partition: str

    def __post_init__(self) -> None:
        if not self.name or not self.partition:
            raise ValueError("projection name and partition must not be empty")


@dataclass(frozen=True, slots=True)
class ProjectionState:
    key: ProjectionKey
    version: int
    checkpoint: int
    state: JsonValue

    def __post_init__(self) -> None:
        if self.version <= 0 or self.checkpoint < 0:
            raise ValueError("projection version and checkpoint are invalid")
        validate_json(self.state)


@dataclass(frozen=True, slots=True)
class ProjectionApplied:
    state: ProjectionState
    events: int


@dataclass(frozen=True, slots=True)
class StaleProjectionCheckpoint:
    expected: int
    actual: int


@dataclass(frozen=True, slots=True)
class ProjectionVersionMismatch:
    expected: int
    actual: int


type ProjectionApplyResult = (
    ProjectionApplied | StaleProjectionCheckpoint | ProjectionVersionMismatch
)


class RebuildStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class RebuildRun:
    run_id: str
    key: ProjectionKey
    projection_version: int
    through_position: int
    batch_size: int
    next_position: int
    state: JsonValue
    status: RebuildStatus = RebuildStatus.RUNNING

    def __post_init__(self) -> None:
        if not self.run_id or self.projection_version <= 0:
            raise ValueError("rebuild identity and projection version are required")
        if (
            self.through_position < 0
            or not 0 <= self.next_position <= self.through_position
            or self.batch_size <= 0
        ):
            raise ValueError("rebuild positions and batch size are invalid")
        validate_json(self.state)


class ProjectionStore(Protocol):
    def load(self, key: ProjectionKey) -> ProjectionState | None: ...

    def apply(
        self,
        key: ProjectionKey,
        *,
        expected_checkpoint: int,
        projection_version: int,
        state: JsonValue,
        events: Sequence[StoredEvent],
    ) -> ProjectionApplyResult: ...


class InMemoryProjectionStore:
    """Finite process-local projection store with no durability or process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if capacity <= 0:
            raise ValueError("projection capacity must be positive")
        self._capacity = capacity
        self._states: dict[ProjectionKey, ProjectionState] = {}
        self._rebuilds: dict[str, RebuildRun] = {}
        self._lock = threading.RLock()

    def load(self, key: ProjectionKey) -> ProjectionState | None:
        with self._lock:
            return self._states.get(key)

    def apply(
        self,
        key: ProjectionKey,
        *,
        expected_checkpoint: int,
        projection_version: int,
        state: JsonValue,
        events: Sequence[StoredEvent],
    ) -> ProjectionApplyResult:
        validate_json(state)
        if projection_version <= 0:
            raise ValueError("projection version must be positive")
        if any(
            later.global_position <= earlier.global_position for earlier, later in pairwise(events)
        ):
            raise ValueError("projection events must be strictly ordered")
        if events and events[0].global_position <= expected_checkpoint:
            raise ValueError("projection checkpoint must not regress")
        with self._lock:
            current = self._states.get(key)
            actual_checkpoint = 0 if current is None else current.checkpoint
            if actual_checkpoint != expected_checkpoint:
                return StaleProjectionCheckpoint(expected_checkpoint, actual_checkpoint)
            if current is not None and current.version != projection_version:
                return ProjectionVersionMismatch(projection_version, current.version)
            if current is None and len(self._states) >= self._capacity:
                raise OverflowError("projection capacity exceeded")
            checkpoint = expected_checkpoint if not events else events[-1].global_position
            projected = ProjectionState(key, projection_version, checkpoint, state)
            self._states[key] = projected
            return ProjectionApplied(projected, len(events))

    def begin_rebuild(self, run: RebuildRun) -> bool:
        with self._lock:
            if run.run_id in self._rebuilds:
                return False
            if len(self._states) + len(self._rebuilds) >= self._capacity:
                raise OverflowError("projection capacity exceeded")
            self._rebuilds[run.run_id] = run
            return True

    def load_rebuild(self, run_id: str) -> RebuildRun | None:
        with self._lock:
            return self._rebuilds.get(run_id)

    def advance_rebuild(
        self,
        run_id: str,
        *,
        expected_position: int,
        state: JsonValue,
        next_position: int,
        complete: bool,
    ) -> RebuildRun | None:
        validate_json(state)
        with self._lock:
            current = self._rebuilds.get(run_id)
            if (
                current is None
                or current.status is not RebuildStatus.RUNNING
                or current.next_position != expected_position
            ):
                return None
            if (
                not current.next_position <= next_position <= current.through_position
                or complete != (next_position == current.through_position)
            ):
                raise ValueError("rebuild progress must respect its fixed watermark")
            active = self._states.get(current.key)
            if (
                complete
                and active is not None
                and (
                    active.checkpoint > next_position or active.version > current.projection_version
                )
            ):
                return None
            status = RebuildStatus.COMPLETED if complete else RebuildStatus.RUNNING
            advanced = replace(current, state=state, next_position=next_position, status=status)
            self._rebuilds[run_id] = advanced
            if complete:
                self._states[current.key] = ProjectionState(
                    current.key,
                    current.projection_version,
                    next_position,
                    state,
                )
            return advanced


type ProjectionReducer = Callable[[JsonValue, StoredEvent], JsonValue]


@dataclass(frozen=True, slots=True)
class ProjectionDefinition:
    version: int
    initial_state: JsonValue
    reduce: ProjectionReducer

    def __post_init__(self) -> None:
        if self.version <= 0:
            raise ValueError("projection version must be positive")
        validate_json(self.initial_state)


class ProjectionRuntime:
    def __init__(self, store: InMemoryProjectionStore, events: EventStore) -> None:
        self._store = store
        self._events = events

    def project_once(
        self, key: ProjectionKey, definition: ProjectionDefinition, *, limit: int
    ) -> ProjectionApplyResult:
        current = self._store.load(key)
        checkpoint = 0 if current is None else current.checkpoint
        if current is not None and current.version != definition.version:
            return ProjectionVersionMismatch(definition.version, current.version)
        page = self._events.read_all(after_position=checkpoint, limit=limit)
        state = definition.initial_state if current is None else current.state
        for event in page.events:
            state = definition.reduce(state, event)
            validate_json(state)
        return self._store.apply(
            key,
            expected_checkpoint=checkpoint,
            projection_version=definition.version,
            state=state,
            events=page.events,
        )

    def resume_rebuild(self, run_id: str, definition: ProjectionDefinition) -> RebuildRun:
        run = self._store.load_rebuild(run_id)
        if run is None:
            raise LookupError(f"unknown rebuild run: {run_id}")
        if run.projection_version != definition.version:
            raise ValueError("rebuild definition version changed")
        if run.status is RebuildStatus.COMPLETED:
            return run
        page = self._events.read_all(after_position=run.next_position, limit=run.batch_size)
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
        advanced = self._store.advance_rebuild(
            run_id,
            expected_position=run.next_position,
            state=state,
            next_position=next_position,
            complete=complete,
        )
        if advanced is None:
            raise RuntimeError("rebuild progress compare-and-set failed")
        return advanced


__all__ = [
    "InMemoryProjectionStore",
    "ProjectionApplied",
    "ProjectionDefinition",
    "ProjectionKey",
    "ProjectionRuntime",
    "ProjectionState",
    "ProjectionStore",
    "ProjectionVersionMismatch",
    "RebuildRun",
    "RebuildStatus",
    "StaleProjectionCheckpoint",
]
