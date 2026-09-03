from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule

from pytitect import OpaqueId
from pytitect.idempotency import (
    Execute,
    IdempotencyScope,
    InMemoryIdempotencyStore,
    InProgress,
    RequestFingerprint,
    ReservationAbandoned,
    ReservationCompleted,
    ReservationRenewed,
    StaleReservation,
)
from pytitect.leases import (
    InMemoryLeaseStore,
    Lease,
    LeaseAcquired,
    LeaseBusy,
    LeaseReleased,
    LeaseRenewed,
    StaleLease,
)
from pytitect.receipts import (
    InMemoryReceiptStore,
    MutationReceipt,
    ReceiptState,
    ReceiptTransitioned,
)
from pytitect.sync import (
    BatchItemReceipt,
    InMemoryMutationBatchStore,
    MutationBatchLease,
)
from pytitect.sync.batches import (
    MutationBatchCompleted,
    MutationBatchLeaseRenewed,
    MutationBatchProgressed,
    StaleMutationBatchLease,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
LEASE_TTL = timedelta(seconds=2)
RETENTION_TTL = timedelta(minutes=1)


class IdempotencyStateMachine(RuleBasedStateMachine):
    @initialize()
    def start(self) -> None:
        self.store = InMemoryIdempotencyStore[int](capacity=20)
        self.now = NOW
        self.index = 0
        self.token = None
        self.scope = IdempotencyScope("machine", "subject", "operation")
        self.fingerprint = RequestFingerprint.from_json({"value": 1})

    @precondition(lambda self: self.token is None)
    @rule()
    def reserve(self) -> None:
        self.index += 1
        decision = self.store.reserve(
            self.scope,
            f"key-{self.index}",
            self.fingerprint,
            now=self.now,
            lease_ttl=LEASE_TTL,
        )
        assert isinstance(decision, Execute)
        self.token = decision.token

    @precondition(lambda self: self.token is not None)
    @rule()
    def duplicate_is_busy(self) -> None:
        decision = self.store.reserve(
            self.scope,
            f"key-{self.index}",
            self.fingerprint,
            now=self.now,
            lease_ttl=LEASE_TTL,
        )
        assert isinstance(decision, InProgress)

    @precondition(lambda self: self.token is not None)
    @rule()
    def renew(self) -> None:
        assert isinstance(
            self.store.renew(self.token, now=self.now, lease_ttl=LEASE_TTL),
            ReservationRenewed,
        )

    @precondition(lambda self: self.token is not None)
    @rule()
    def complete(self) -> None:
        assert isinstance(
            self.store.complete(self.token, self.index, now=self.now, retention_ttl=RETENTION_TTL),
            ReservationCompleted,
        )
        self.token = None

    @precondition(lambda self: self.token is not None)
    @rule()
    def abandon(self) -> None:
        assert isinstance(self.store.abandon(self.token, now=self.now), ReservationAbandoned)
        self.token = None

    @precondition(lambda self: self.token is not None)
    @rule()
    def expire(self) -> None:
        self.now += LEASE_TTL
        assert isinstance(
            self.store.renew(self.token, now=self.now, lease_ttl=LEASE_TTL), StaleReservation
        )
        self.token = None


class ReceiptStateMachine(RuleBasedStateMachine):
    @initialize()
    def start(self) -> None:
        self.store = InMemoryReceiptStore[int](capacity=100)
        self.now = NOW
        self.index = 0
        self.current = None

    @precondition(lambda self: self.current is None)
    @rule()
    def add(self) -> None:
        self.index += 1
        self.current = MutationReceipt[int](
            OpaqueId(f"receipt-{self.index}"), ReceiptState.ACCEPTED, self.now, self.now
        )
        assert self.store.add(self.current)
        assert not self.store.add(self.current)

    @precondition(
        lambda self: self.current is not None and self.current.state is ReceiptState.ACCEPTED
    )
    @rule()
    def process(self) -> None:
        self.now += timedelta(milliseconds=1)
        outcome = self.current.transition(ReceiptState.PROCESSING, at=self.now)
        assert isinstance(outcome, ReceiptTransitioned)
        assert self.store.transition(self.current, outcome.receipt)
        self.current = outcome.receipt

    @precondition(
        lambda self: self.current is not None and self.current.state is ReceiptState.PROCESSING
    )
    @rule()
    def complete(self) -> None:
        self.now += timedelta(milliseconds=1)
        outcome = self.current.transition(ReceiptState.COMPLETED, at=self.now, result=self.index)
        assert isinstance(outcome, ReceiptTransitioned)
        assert self.store.transition(self.current, outcome.receipt)
        self.current = None

    @invariant()
    def stored_value_matches_model(self) -> None:
        if self.current is not None:
            assert self.store.get(self.current.receipt_id) == self.current


class LeaseStateMachine(RuleBasedStateMachine):
    @initialize()
    def start(self) -> None:
        self.store = InMemoryLeaseStore[str]()
        self.now = NOW
        self.lease: Lease[str] | None = None

    @precondition(lambda self: self.lease is None)
    @rule()
    def acquire(self) -> None:
        outcome = self.store.acquire("resource", owner="worker", now=self.now, ttl=LEASE_TTL)
        assert isinstance(outcome, LeaseAcquired)
        self.lease = outcome.lease

    @precondition(lambda self: self.lease is not None)
    @rule()
    def exclude_other_worker(self) -> None:
        assert isinstance(
            self.store.acquire("resource", owner="other", now=self.now, ttl=LEASE_TTL),
            LeaseBusy,
        )

    @precondition(lambda self: self.lease is not None)
    @rule()
    def renew(self) -> None:
        previous = self.lease
        outcome = self.store.renew(previous, now=self.now, ttl=LEASE_TTL)
        assert isinstance(outcome, LeaseRenewed)
        self.lease = outcome.lease
        if previous != self.lease:
            assert isinstance(self.store.release(previous, now=self.now), StaleLease)

    @precondition(lambda self: self.lease is not None)
    @rule()
    def release(self) -> None:
        assert isinstance(self.store.release(self.lease, now=self.now), LeaseReleased)
        self.lease = None

    @precondition(lambda self: self.lease is not None)
    @rule()
    def expire(self) -> None:
        self.now += LEASE_TTL
        assert isinstance(self.store.release(self.lease, now=self.now), StaleLease)
        self.lease = None

    @invariant()
    def authority_agrees_with_active_model(self) -> None:
        if self.lease is not None:
            assert self.store.current("resource") == self.lease


class MutationBatchStateMachine(RuleBasedStateMachine):
    @initialize()
    def start(self) -> None:
        self.store = InMemoryMutationBatchStore[int](capacity=100)
        self.now = NOW
        self.index = 0
        self.lease: MutationBatchLease[int] | None = None
        self.fingerprint = RequestFingerprint.from_json({"items": [1]})

    @precondition(lambda self: self.lease is None)
    @rule()
    def begin(self) -> None:
        self.index += 1
        outcome = self.store.begin(
            "machine",
            f"batch-{self.index}",
            self.fingerprint,
            total_items=1,
            now=self.now,
            lease_ttl=LEASE_TTL,
        )
        assert isinstance(outcome, MutationBatchLease)
        self.lease = outcome

    @precondition(lambda self: self.lease is not None)
    @rule()
    def renew(self) -> None:
        outcome = self.store.renew(self.lease, now=self.now, lease_ttl=LEASE_TTL)
        assert isinstance(outcome, MutationBatchLeaseRenewed)
        self.lease = outcome.lease

    @precondition(lambda self: self.lease is not None and self.lease.next_index == 0)
    @rule()
    def advance(self) -> None:
        outcome = self.store.advance(
            self.lease,
            BatchItemReceipt("item", self.index),
            now=self.now,
            lease_ttl=LEASE_TTL,
        )
        assert isinstance(outcome, MutationBatchProgressed)
        self.lease = outcome.lease

    @precondition(lambda self: self.lease is not None and self.lease.next_index == 1)
    @rule()
    def complete(self) -> None:
        assert isinstance(
            self.store.complete(self.lease, now=self.now, retention_ttl=RETENTION_TTL),
            MutationBatchCompleted,
        )
        self.lease = None

    @precondition(lambda self: self.lease is not None)
    @rule()
    def expire(self) -> None:
        self.now += LEASE_TTL
        assert isinstance(
            self.store.renew(self.lease, now=self.now, lease_ttl=LEASE_TTL),
            StaleMutationBatchLease,
        )
        self.lease = None


StateMachineSettings = settings(max_examples=20, stateful_step_count=12, deadline=None)

TestIdempotencyStateMachine = IdempotencyStateMachine.TestCase
TestIdempotencyStateMachine.settings = StateMachineSettings
TestReceiptStateMachine = ReceiptStateMachine.TestCase
TestReceiptStateMachine.settings = StateMachineSettings
TestLeaseStateMachine = LeaseStateMachine.TestCase
TestLeaseStateMachine.settings = StateMachineSettings
TestMutationBatchStateMachine = MutationBatchStateMachine.TestCase
TestMutationBatchStateMachine.settings = StateMachineSettings
