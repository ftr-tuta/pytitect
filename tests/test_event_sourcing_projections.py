from datetime import UTC, datetime

from pytitect.event_sourcing import (
    AppendCommitted,
    InMemoryEventStore,
    NewEvent,
    Snapshot,
    StreamId,
    WrongExpectedVersion,
)
from pytitect.projections import (
    InMemoryProjectionStore,
    ProjectionApplied,
    ProjectionDefinition,
    ProjectionKey,
    ProjectionRuntime,
    ProjectionVersionMismatch,
    RebuildRun,
    RebuildStatus,
)

NOW = datetime(2026, 9, 3, tzinfo=UTC)


def new_event(identifier: str, amount: int) -> NewEvent:
    return NewEvent(identifier, "example.incremented.v1", {"amount": amount}, NOW)


def test_optimistic_streams_are_bounded_and_snapshots_are_optional() -> None:
    store = InMemoryEventStore(max_page_size=2)
    stream = StreamId("example", "one")
    appended = store.append(
        stream,
        expected_version=0,
        events=[new_event("one", 1), new_event("two", 2), new_event("three", 3)],
    )
    assert isinstance(appended, AppendCommitted)
    assert isinstance(
        store.append(stream, expected_version=0, events=[new_event("four", 4)]),
        WrongExpectedVersion,
    )
    first = store.read_stream(stream, after_version=0, limit=2)
    assert [event.stream_version for event in first.events] == [1, 2]
    assert not first.complete
    snapshot = Snapshot(stream, 2, {"total": 3}, NOW)
    assert store.save_snapshot(snapshot, expected_version=None)
    assert store.load_snapshot(stream) == snapshot


def test_projection_checkpoint_and_version_guard_are_atomic() -> None:
    events = InMemoryEventStore()
    stream = StreamId("example", "one")
    events.append(stream, expected_version=0, events=[new_event("one", 2)])
    projections = InMemoryProjectionStore()
    runtime = ProjectionRuntime(projections, events)

    def reduce(state: object, event: object) -> object:
        return {"count": state["count"] + event.event.payload["amount"]}  # type: ignore[index,union-attr]

    definition = ProjectionDefinition(1, {"count": 0}, reduce)
    applied = runtime.project_once(ProjectionKey("totals", "all"), definition, limit=10)
    assert isinstance(applied, ProjectionApplied)
    assert applied.state.state == {"count": 2}
    mismatch = runtime.project_once(
        ProjectionKey("totals", "all"), ProjectionDefinition(2, {}, reduce), limit=10
    )
    assert isinstance(mismatch, ProjectionVersionMismatch)


def test_rebuild_is_finite_and_resumable() -> None:
    events = InMemoryEventStore()
    stream = StreamId("example", "one")
    events.append(
        stream,
        expected_version=0,
        events=[new_event("one", 1), new_event("two", 1), new_event("three", 1)],
    )
    projections = InMemoryProjectionStore()
    projections.begin_rebuild(
        RebuildRun(
            "rebuild-1",
            ProjectionKey("counts", "all"),
            1,
            3,
            2,
            0,
            {"count": 0},
        )
    )
    definition = ProjectionDefinition(
        1,
        {"count": 0},
        lambda state, event: {"count": state["count"] + 1},  # type: ignore[index]
    )
    runtime = ProjectionRuntime(projections, events)
    first = runtime.resume_rebuild("rebuild-1", definition)
    assert first.status is RebuildStatus.RUNNING
    second = runtime.resume_rebuild("rebuild-1", definition)
    assert second.status is RebuildStatus.COMPLETED
    assert second.state == {"count": 3}
