from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest

from pytitect import OpaqueId
from pytitect.idempotency import (
    Conflict,
    Execute,
    IdempotencyCoordinator,
    IdempotencyPolicy,
    IdempotencyScope,
    IdempotencyStoreHarness,
    InMemoryIdempotencyStore,
    InProgress,
    Replay,
    RequestFingerprint,
    ReservationAbandoned,
    ReservationCompleted,
    ReservationMarkedUncertain,
    ReservationRenewed,
    Uncertain,
)
from pytitect.receipts import (
    ConfirmedCompleted,
    InvalidTransition,
    MutationReceipt,
    ReceiptReconciler,
    ReceiptState,
    ReceiptTransitioned,
)


def test_idempotency_lifecycle_expiry_and_uncertain() -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    store = InMemoryIdempotencyStore[dict[str, int]](capacity=2)
    policy = IdempotencyPolicy(
        execution_lease_ttl=timedelta(seconds=10),
        result_retention_ttl=timedelta(minutes=5),
        uncertainty_retention_ttl=timedelta(minutes=10),
    )
    coordinator = IdempotencyCoordinator(store, policy, clock)
    scope = IdempotencyScope("api", "tenant-1", "create")
    fingerprint = RequestFingerprint.from_json({"amount": 1})
    first = coordinator.begin(scope=scope, key="client-key", fingerprint=fingerprint)
    assert isinstance(first, Execute)
    assert isinstance(
        coordinator.begin(scope=scope, key="client-key", fingerprint=fingerprint), InProgress
    )
    assert isinstance(
        coordinator.begin(
            scope=scope,
            key="client-key",
            fingerprint=RequestFingerprint.from_json({"amount": 2}),
        ),
        Conflict,
    )
    clock.advance(timedelta(seconds=9))
    assert isinstance(coordinator.renew(first.token), ReservationRenewed)
    assert isinstance(coordinator.complete(first.token, {"id": 7}), ReservationCompleted)
    replay = coordinator.begin(scope=scope, key="client-key", fingerprint=fingerprint)
    assert replay == Replay({"id": 7})

    other = coordinator.begin(scope=scope, key="other", fingerprint=fingerprint)
    assert isinstance(other, Execute)
    assert isinstance(
        coordinator.uncertain(other.token, "effect committed, response lost"),
        ReservationMarkedUncertain,
    )
    assert isinstance(
        coordinator.begin(scope=scope, key="other", fingerprint=fingerprint), Uncertain
    )
    clock.advance(timedelta(minutes=6))
    reopened = coordinator.begin(scope=scope, key="client-key", fingerprint=fingerprint)
    assert isinstance(reopened, Execute)
    assert isinstance(coordinator.abandon(reopened.token), ReservationAbandoned)
    with pytest.raises(ValueError):
        coordinator.begin(scope=scope, key="", fingerprint=fingerprint)


def test_idempotency_first_reservation_is_atomic() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = InMemoryIdempotencyStore[int]()
    scope = IdempotencyScope("test", "one", "race")
    fingerprint = RequestFingerprint.from_json({"same": True})
    outcomes: list[object] = []
    barrier = threading.Barrier(8)

    def reserve() -> None:
        barrier.wait()
        outcomes.append(
            store.reserve(
                scope,
                "key",
                fingerprint,
                now=now,
                lease_ttl=timedelta(minutes=1),
            )
        )

    threads = [threading.Thread(target=reserve) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(isinstance(outcome, Execute) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, InProgress) for outcome in outcomes) == 7
    IdempotencyStoreHarness(lambda: InMemoryIdempotencyStore[int]()).exercise(value=1, now=now)


def test_receipt_transitions_and_metadata_bounds() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    receipt = MutationReceipt[str](
        OpaqueId("receipt-1"), ReceiptState.ACCEPTED, now, now, metadata={"safe": "yes"}
    )
    processing = receipt.transition(ReceiptState.PROCESSING, at=now + timedelta(seconds=1))
    assert isinstance(processing, ReceiptTransitioned)
    completed = processing.receipt.transition(
        ReceiptState.COMPLETED, at=now + timedelta(seconds=2), result="done"
    )
    assert isinstance(completed, ReceiptTransitioned)
    assert completed.receipt.result == "done"
    assert isinstance(
        completed.receipt.transition(ReceiptState.PROCESSING, at=now + timedelta(seconds=3)),
        InvalidTransition,
    )
    assert isinstance(
        processing.receipt.transition(ReceiptState.COMPLETED, at=now + timedelta(seconds=2)),
        InvalidTransition,
    )
    assert isinstance(
        processing.receipt.transition(ReceiptState.REJECTED, at=now), InvalidTransition
    )
    with pytest.raises(ValueError):
        MutationReceipt[str](
            OpaqueId("x"), ReceiptState.ACCEPTED, now, now, metadata={str(i): i for i in range(65)}
        )


def test_uncertain_receipt_requires_explicit_cas_reconciliation() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    uncertain = MutationReceipt[str](
        OpaqueId("receipt-uncertain"), ReceiptState.UNCERTAIN, now, now
    )

    class Store:
        current = uncertain

        def get(self, receipt_id):  # type: ignore[no-untyped-def]
            return self.current if receipt_id == self.current.receipt_id else None

        def add(self, receipt):  # type: ignore[no-untyped-def]
            del receipt
            return False

        def transition(self, receipt, target):  # type: ignore[no-untyped-def]
            del receipt, target
            return False

        def reconcile_uncertain(self, receipt, target):  # type: ignore[no-untyped-def]
            if receipt != self.current:
                return False
            self.current = target
            return True

    outcome = ReceiptReconciler(Store()).reconcile(
        uncertain.receipt_id,
        ReceiptState.COMPLETED,
        at=now + timedelta(seconds=1),
        result="confirmed",
    )
    assert isinstance(outcome, ConfirmedCompleted)
    assert outcome.receipt.result == "confirmed"
