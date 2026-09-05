from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pytitect import OpaqueId
from pytitect.checkpoints import (
    AtomicCheckpointConfirmed,
    AtomicCheckpointCoordinator,
    Checkpoint,
    CheckpointItem,
    DeferredCheckpointCoordinator,
    StaleCheckpoint,
    StateCommittedCheckpointUnconfirmed,
)
from pytitect.inbox import (
    InboxAccepted,
    InboxDuplicate,
    InboxEnvelope,
    InboxInProgress,
    InboxScope,
    InMemoryInboxStore,
)
from pytitect.leases import (
    FencedCommit,
    FencedCommitted,
    InMemoryLeaseStore,
    LeaseAcquired,
    LeaseAuthority,
    LeaseBusy,
    LeaseReleased,
    StaleLease,
)
from pytitect.outbox import (
    Delivered,
    DeliveryResult,
    InMemoryOutboxStore,
    OneRoundDispatcher,
    OutboxEnvelope,
    PermanentFailure,
    Retryable,
    RetryPolicy,
)


def test_inbox_duplicate_in_progress_abandon_and_expiry() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = InMemoryInboxStore(capacity=2)
    scope = InboxScope("events", "upstream", "projection")
    message: OpaqueId[object] = OpaqueId("message-1")
    assert isinstance(
        store.begin(scope, message, token="worker-1", now=now, ttl=timedelta(seconds=5)),
        InboxAccepted,
    )
    assert isinstance(
        store.begin(scope, message, token="worker-2", now=now, ttl=timedelta(seconds=5)),
        InboxInProgress,
    )
    assert store.abandon(scope, message, token="worker-1")
    assert isinstance(
        store.begin(scope, message, token="worker-2", now=now, ttl=timedelta(seconds=5)),
        InboxAccepted,
    )
    assert store.complete(scope, message, token="worker-2", now=now)
    assert isinstance(
        store.begin(scope, message, token="worker-3", now=now, ttl=timedelta(seconds=5)),
        InboxDuplicate,
    )
    other = InboxScope("events", "upstream", "another-projection")
    assert isinstance(
        store.begin(other, message, token="worker-4", now=now, ttl=timedelta(seconds=5)),
        InboxAccepted,
    )
    envelope = InboxEnvelope(scope, message, {"value": 1}, now)
    assert envelope.scope == scope
    with pytest.raises(ValueError, match="scope parts"):
        InboxScope("", "upstream", "projection")
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        InboxEnvelope(scope, message, None, datetime(2026, 1, 1))


def test_outbox_one_round_delivered_retry_and_permanent() -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    store = InMemoryOutboxStore[str]()
    for index, payload in enumerate(("ok", "retry", "bad")):
        store.add(
            OutboxEnvelope(
                OpaqueId(f"m-{index}"),
                "events",
                payload,
                clock.now(),
                clock.now(),
            )
        )

    def handler(envelope: OutboxEnvelope[str]) -> DeliveryResult:
        if envelope.payload == "ok":
            return Delivered()
        if envelope.payload == "retry":
            return Retryable("temporary")
        return PermanentFailure("invalid")

    dispatcher = OneRoundDispatcher(
        store,
        handler,
        retry_policy=RetryPolicy(initial_delay=timedelta(seconds=1), max_attempts=2),
        clock=clock,
    )
    first = dispatcher.dispatch(limit=10)
    assert (first.claimed, first.delivered, first.retried, first.failed) == (3, 1, 1, 1)
    assert dispatcher.dispatch(limit=10).claimed == 0
    clock.advance(timedelta(seconds=1))
    second = dispatcher.dispatch(limit=10)
    assert second.claimed == 1 and second.failed == 1


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self.values: dict[str, Checkpoint] = {}

    def load(self, stream: str) -> Checkpoint | None:
        return self.values.get(stream)

    def load_for_update(self, stream: str) -> Checkpoint | None:
        return self.load(stream)

    def advance(self, stream: str, *, expected: Checkpoint | None, checkpoint: Checkpoint) -> bool:
        if self.values.get(stream) != expected:
            return False
        self.values[stream] = checkpoint
        return True


class FakeTransaction:
    def __init__(self, *, commit: bool = True) -> None:
        self.commit = commit
        self.callbacks: list[Callable[[], None]] = []

    @contextmanager
    def atomic(self):  # type: ignore[no-untyped-def]
        try:
            yield
        except Exception:
            self.callbacks.clear()
            raise
        else:
            if self.commit:
                callbacks, self.callbacks = self.callbacks, []
                for callback in callbacks:
                    callback()

    def on_commit(self, callback: Callable[[], None]) -> None:
        self.callbacks.append(callback)


def test_atomic_checkpoint_advances_inside_transaction() -> None:
    store = MemoryCheckpointStore()
    transaction = FakeTransaction()
    state: list[int] = []
    coordinator = AtomicCheckpointCoordinator[int](store, transaction)
    result = coordinator.apply(
        stream="orders",
        items=iter([CheckpointItem(Checkpoint(b"1"), 10)]),
        apply_state=state.append,
    )
    assert isinstance(result, AtomicCheckpointConfirmed)
    assert result.batch.applied == 1 and state == [10]
    assert store.load("orders") == Checkpoint(b"1")
    assert transaction.callbacks == []


def test_atomic_stale_and_deferred_uncertainty_are_typed() -> None:
    class StaleStore(MemoryCheckpointStore):
        def advance(
            self,
            stream: str,
            *,
            expected: Checkpoint | None,
            checkpoint: Checkpoint,
        ) -> bool:
            return False

    atomic = AtomicCheckpointCoordinator[int](StaleStore(), FakeTransaction())
    stale = atomic.apply(
        stream="orders",
        items=iter([CheckpointItem(Checkpoint(b"1"), 10)]),
        apply_state=lambda payload: None,
    )
    assert isinstance(stale, StaleCheckpoint)

    state: list[int] = []
    deferred = DeferredCheckpointCoordinator[int](StaleStore(), FakeTransaction())
    uncertain = deferred.apply(
        stream="orders",
        items=iter([CheckpointItem(Checkpoint(b"1"), 10)]),
        apply_state=state.append,
    )
    assert isinstance(uncertain, StateCommittedCheckpointUnconfirmed)
    assert state == [10]


def test_leases_takeover_monotonic_fencing_and_atomic_stale_rejection() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = InMemoryLeaseStore[str]()
    first = store.acquire("job", owner="a", now=now, ttl=timedelta(seconds=5))
    assert isinstance(first, LeaseAcquired) and first.lease.fencing_token == 1
    assert isinstance(store.acquire("job", owner="b", now=now, ttl=timedelta(seconds=5)), LeaseBusy)
    takeover = store.acquire(
        "job", owner="b", now=now + timedelta(seconds=5), ttl=timedelta(seconds=5)
    )
    assert isinstance(takeover, LeaseAcquired) and takeover.lease.fencing_token == 2
    assert isinstance(
        store.renew(first.lease, now=now + timedelta(seconds=1), ttl=timedelta(seconds=5)),
        StaleLease,
    )
    state: list[str] = []

    def locked(resource: str, compare: Callable[[LeaseAuthority | None], Any]) -> Any:
        def compare_current() -> Any:
            current = store.current(resource)
            authority = (
                LeaseAuthority(current.owner, current.fencing_token, current.expires_at)
                if current is not None
                else None
            )
            return compare(authority)

        return store.locked_authority(resource, compare_current)

    fenced = FencedCommit[str, None](locked, clock=lambda: now + timedelta(seconds=6))
    assert isinstance(fenced.commit(first.lease, lambda: state.append("stale")), StaleLease)
    assert isinstance(fenced.commit(takeover.lease, lambda: state.append("fresh")), FencedCommitted)
    assert state == ["fresh"]
    assert isinstance(store.release(takeover.lease, now=now + timedelta(seconds=6)), LeaseReleased)


def test_reference_lease_store_retains_fencing_authority_with_finite_capacity() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = InMemoryLeaseStore[str](capacity=1)
    acquired = store.acquire("first", owner="worker", now=now, ttl=timedelta(minutes=1))
    assert isinstance(acquired, LeaseAcquired)
    assert isinstance(store.release(acquired.lease, now=now), LeaseReleased)
    with pytest.raises(OverflowError, match="lease authority capacity exceeded"):
        store.acquire("second", owner="worker", now=now, ttl=timedelta(minutes=1))
    reacquired = store.acquire("first", owner="worker", now=now, ttl=timedelta(minutes=1))
    assert isinstance(reacquired, LeaseAcquired)
    assert reacquired.lease.fencing_token == 2
