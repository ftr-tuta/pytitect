from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest

from pytitect.idempotency import InMemoryIdempotencyStore
from pytitect.sync import (
    ALL_OR_NOTHING,
    PER_ITEM,
    BatchCommitted,
    BatchItem,
    BatchItemsCommittedEnvelopeUnconfirmed,
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
    MutationBatchCoordinator,
    OpaqueCursorCodec,
    StaleGeneration,
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


def test_generation_guard() -> None:
    guard = GenerationGuard(GenerationStore(), Transaction())
    assert guard.commit(
        dataset="orders", partition="one", expected=2, mutation=lambda: "done"
    ) == GenerationCommitted("done", 2)
    assert isinstance(
        guard.commit(dataset="orders", partition="one", expected=1, mutation=lambda: "bad"),
        StaleGeneration,
    )


def test_mutation_batches_empty_order_replay_and_envelope_uncertainty() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    envelopes = InMemoryIdempotencyStore[tuple]()
    item_store = InMemoryIdempotencyStore()
    coordinator = MutationBatchCoordinator(envelopes, item_store, Transaction(), using="default")
    calls: list[str] = []
    items = (BatchItem("b", {"value": 2}), BatchItem("a", {"value": 1}))
    result = coordinator.execute(
        batch_id="batch",
        items=items,
        policy=ALL_OR_NOTHING,
        mutate=lambda item, using: calls.append(f"{using}:{item.item_id}") or item.payload,
        now=now,
        ttl=timedelta(minutes=1),
    )
    assert isinstance(result, BatchCommitted)
    assert [receipt.item_id for receipt in result.receipts] == ["b", "a"]
    assert calls == ["default:b", "default:a"]

    empty = MutationBatchCoordinator(
        InMemoryIdempotencyStore(), InMemoryIdempotencyStore(), Transaction(), using="default"
    ).execute(
        batch_id="empty",
        items=(),
        policy=PER_ITEM,
        mutate=lambda item, using: None,
        now=now,
        ttl=timedelta(minutes=1),
    )
    assert empty == BatchCommitted(())

    class EnvelopeUnconfirmed(InMemoryIdempotencyStore):
        def complete(self, token, value, *, now):  # type: ignore[no-untyped-def]
            del token, value, now
            return False

    uncertain = MutationBatchCoordinator(
        EnvelopeUnconfirmed(), InMemoryIdempotencyStore(), Transaction(), using="default"
    ).execute(
        batch_id="uncertain",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: item.payload,
        now=now,
        ttl=timedelta(minutes=1),
    )
    assert isinstance(uncertain, BatchItemsCommittedEnvelopeUnconfirmed)


def test_per_item_cas_failure_rolls_back_that_item() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
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
        def complete(self, token, value, *, now):  # type: ignore[no-untyped-def]
            del token, value, now
            return False

    result = MutationBatchCoordinator(
        InMemoryIdempotencyStore(), ItemUnconfirmed(), RollingTransaction(), using="default"
    ).execute(
        batch_id="item-cas",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: mutations.append(item.item_id) or item.payload,
        now=now,
        ttl=timedelta(minutes=1),
    )
    assert isinstance(result, BatchUncertain)
    assert mutations == []
