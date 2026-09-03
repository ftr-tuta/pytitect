from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest

from pytitect.idempotency import (
    IdempotencyPolicy,
    InMemoryIdempotencyStore,
    RequestFingerprint,
    StaleReservation,
)
from pytitect.sync import (
    ALL_OR_NOTHING,
    PER_ITEM,
    BatchCommitted,
    BatchConflict,
    BatchInProgress,
    BatchItem,
    BatchItemReceipt,
    BatchItemsCommittedEnvelopeUnconfirmed,
    BatchReplay,
    BatchUncertain,
    CursorAlgorithm,
    CursorDecoded,
    CursorRejected,
    DatasetDependencyGraph,
    DependencyClosure,
    DependencyCycle,
    DependencyOrder,
    GenerationCommitted,
    GenerationGuard,
    InMemoryMutationBatchStore,
    MutationBatchCoordinator,
    MutationBatchLease,
    OpaqueCursorCodec,
    StaleGeneration,
    StaleMutationBatchLease,
)
from pytitect.sync.batches import (
    MutationBatchCompleted,
    MutationBatchLeaseRenewed,
    MutationBatchMarkedUncertain,
    MutationBatchProgressed,
)


def test_opaque_cursor_hmac_aes_context_expiry_rotation_and_tampering() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    codec = OpaqueCursorCodec(
        {"old": b"o" * 32, "new": b"n" * 32},
        nonce_factory=lambda size: b"x" * size,
    )
    for algorithm in CursorAlgorithm:
        token = codec.encode(
            b"page",
            dataset="orders",
            partition="one",
            kid="new",
            algorithm=algorithm,
            expires_at=now + timedelta(minutes=1),
        )
        decoded = codec.decode(token, dataset="orders", partition="one", now=now)
        assert decoded == CursorDecoded(b"page", "new", now + timedelta(minutes=1))
        assert isinstance(
            codec.decode(token, dataset="other", partition="one", now=now), CursorRejected
        )
        assert isinstance(
            codec.decode(
                token[:-1] + ("A" if token[-1] != "A" else "B"),
                dataset="orders",
                partition="one",
                now=now,
            ),
            CursorRejected,
        )
        expired = codec.decode(
            token, dataset="orders", partition="one", now=now + timedelta(minutes=1)
        )
        assert isinstance(expired, CursorRejected) and expired.code == "expired"
    with pytest.raises(ValueError):
        OpaqueCursorCodec({"short": b"x"}).encode(
            b"page", dataset="orders", partition="one", kid="short"
        )


def test_dependency_graph_closure_stable_order_cycle_and_limits() -> None:
    graph = DatasetDependencyGraph({"orders": ["customers"], "customers": []})
    order = graph.validate()
    assert order == DependencyOrder(("customers", "orders"))
    assert graph.closure(["orders"]) == DependencyClosure(("customers", "orders"))
    assert isinstance(DatasetDependencyGraph({"a": ["b"], "b": ["a"]}).validate(), DependencyCycle)


class Transaction:
    @contextmanager
    def atomic(self):  # type: ignore[no-untyped-def]
        yield


class GenerationStore:
    value = 2

    def load_for_update(self, dataset: str, partition: str) -> int | None:
        del dataset, partition
        return self.value

    def compare_and_set(
        self,
        dataset: str,
        partition: str,
        *,
        expected: int | None,
        generation: int,
    ) -> bool:
        del dataset, partition
        if self.value != expected:
            return False
        self.value = generation
        return True


POLICY = IdempotencyPolicy(
    execution_lease_ttl=timedelta(minutes=1),
    result_retention_ttl=timedelta(hours=1),
    uncertainty_retention_ttl=timedelta(days=1),
)


def test_generation_guard() -> None:
    guard = GenerationGuard(GenerationStore(), Transaction())
    assert guard.commit(
        dataset="orders", partition="one", expected=2, mutation=lambda: "done"
    ) == GenerationCommitted("done", 2)
    assert isinstance(
        guard.commit(dataset="orders", partition="one", expected=1, mutation=lambda: "bad"),
        StaleGeneration,
    )


def test_mutation_batch_store_lease_progress_retention_and_uncertainty() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = InMemoryMutationBatchStore[dict[str, int]](capacity=2)
    fingerprint = RequestFingerprint.from_json({"items": [1]})
    lease = store.begin(
        "sync",
        "batch",
        fingerprint,
        total_items=1,
        now=now,
        lease_ttl=timedelta(seconds=5),
    )
    assert isinstance(lease, MutationBatchLease)
    assert isinstance(
        store.begin(
            "sync",
            "batch",
            fingerprint,
            total_items=1,
            now=now,
            lease_ttl=timedelta(seconds=5),
        ),
        BatchInProgress,
    )
    assert isinstance(
        store.begin(
            "sync",
            "batch",
            RequestFingerprint.from_json({"items": [2]}),
            total_items=1,
            now=now,
            lease_ttl=timedelta(seconds=5),
        ),
        BatchConflict,
    )
    renewed = store.renew(
        lease,
        now=now + timedelta(seconds=1),
        lease_ttl=timedelta(seconds=5),
    )
    assert isinstance(renewed, MutationBatchLeaseRenewed)
    assert isinstance(
        store.renew(lease, now=now + timedelta(seconds=1), lease_ttl=timedelta(seconds=5)),
        StaleMutationBatchLease,
    )
    receipt = BatchItemReceipt("one", {"value": 1})
    progressed = store.advance(
        renewed.lease,
        receipt,
        now=now + timedelta(seconds=2),
        lease_ttl=timedelta(seconds=5),
    )
    assert isinstance(progressed, MutationBatchProgressed)
    completed = store.complete(
        progressed.lease,
        now=now + timedelta(seconds=3),
        retention_ttl=timedelta(minutes=1),
    )
    assert isinstance(completed, MutationBatchCompleted)
    assert isinstance(
        store.begin(
            "sync",
            "batch",
            fingerprint,
            total_items=1,
            now=now + timedelta(seconds=6),
            lease_ttl=timedelta(seconds=5),
        ),
        BatchReplay,
    )
    replacement = store.begin(
        "sync",
        "batch",
        fingerprint,
        total_items=1,
        now=now + timedelta(minutes=2),
        lease_ttl=timedelta(seconds=5),
    )
    assert isinstance(replacement, MutationBatchLease)
    marked = store.mark_uncertain(
        replacement,
        "outcome unknown",
        now=now + timedelta(minutes=2),
        retention_ttl=timedelta(minutes=1),
    )
    assert isinstance(marked, MutationBatchMarkedUncertain)
    assert isinstance(
        store.begin(
            "sync",
            "batch",
            fingerprint,
            total_items=1,
            now=now + timedelta(minutes=2),
            lease_ttl=timedelta(seconds=5),
        ),
        BatchUncertain,
    )


def test_mutation_batches_empty_order_replay_and_envelope_uncertainty() -> None:
    from conftest import ManualClock

    clock = ManualClock()
    envelopes = InMemoryMutationBatchStore()
    events: list[str] = []

    class TrackingItemStore(InMemoryIdempotencyStore):
        def reserve(self, scope, key, fingerprint, *, now, lease_ttl):  # type: ignore[no-untyped-def]
            events.append(f"reserve:{key}")
            return super().reserve(scope, key, fingerprint, now=now, lease_ttl=lease_ttl)

    item_store = TrackingItemStore()
    coordinator = MutationBatchCoordinator(
        envelopes, item_store, Transaction(), using="default", clock=clock
    )
    calls: list[str] = []
    items = (BatchItem("b", {"value": 2}), BatchItem("a", {"value": 1}))
    result = coordinator.execute(
        batch_id="batch",
        items=items,
        policy=ALL_OR_NOTHING,
        mutate=lambda item, using: (
            events.append(f"mutate:{item.item_id}"),
            calls.append(f"{using}:{item.item_id}"),
            item.payload,
        )[-1],
        idempotency_policy=POLICY,
    )
    assert isinstance(result, BatchCommitted)
    assert [receipt.item_id for receipt in result.receipts] == ["b", "a"]
    assert calls == ["default:b", "default:a"]
    assert events[:3] == ["reserve:b", "reserve:a", "mutate:b"]
    replay = coordinator.execute(
        batch_id="batch",
        items=items,
        policy=ALL_OR_NOTHING,
        mutate=lambda item, using: calls.append(item.item_id) or item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(replay, BatchReplay)
    assert calls == ["default:b", "default:a"]

    empty = MutationBatchCoordinator(
        InMemoryMutationBatchStore(),
        InMemoryIdempotencyStore(),
        Transaction(),
        using="default",
        clock=clock,
    ).execute(
        batch_id="empty",
        items=(),
        policy=PER_ITEM,
        mutate=lambda item, using: None,
        idempotency_policy=POLICY,
    )
    assert empty == BatchCommitted(())

    class EnvelopeUnconfirmed(InMemoryMutationBatchStore):
        fail_once = True

        def complete(self, lease, *, now, retention_ttl):  # type: ignore[no-untyped-def]
            if self.fail_once:
                self.fail_once = False
                return StaleMutationBatchLease()
            return super().complete(lease, now=now, retention_ttl=retention_ttl)

    resumable = EnvelopeUnconfirmed()
    resumable_items = InMemoryIdempotencyStore()
    mutation_calls: list[str] = []
    uncertain = MutationBatchCoordinator(
        resumable,
        resumable_items,
        Transaction(),
        using="default",
        clock=clock,
    ).execute(
        batch_id="uncertain",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: mutation_calls.append(item.item_id) or item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(uncertain, BatchItemsCommittedEnvelopeUnconfirmed)
    clock.advance(timedelta(minutes=1))
    recovered = MutationBatchCoordinator(
        resumable,
        item_store=resumable_items,
        transaction=Transaction(),
        using="default",
        clock=clock,
    ).execute(
        batch_id="uncertain",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: mutation_calls.append(item.item_id) or item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(recovered, BatchCommitted)
    assert recovered.receipts[0].replayed is True
    assert mutation_calls == ["one"]


def test_per_item_cas_failure_rolls_back_that_item() -> None:
    from conftest import ManualClock

    clock = ManualClock()
    mutations: list[str] = []

    class RollingTransaction:
        @contextmanager
        def atomic(self):  # type: ignore[no-untyped-def]
            before = list(mutations)
            try:
                yield
            except Exception:
                mutations[:] = before
                raise

    class ItemUnconfirmed(InMemoryIdempotencyStore):
        def complete(  # type: ignore[no-untyped-def]
            self, token, value, *, now, retention_ttl
        ):
            del token, value, now, retention_ttl
            return StaleReservation()

    result = MutationBatchCoordinator(
        InMemoryMutationBatchStore(),
        ItemUnconfirmed(),
        RollingTransaction(),
        using="default",
        clock=clock,
    ).execute(
        batch_id="item-cas",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: mutations.append(item.item_id) or item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(result, BatchUncertain)
    assert mutations == []
