from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime

from pytitect.checkpoints import (
    CheckpointStoreHarness,
    InMemoryCheckpointStore,
    TransactionBoundaryHarness,
)
from pytitect.django import (
    DjangoCheckpointStore,
    DjangoGenerationStore,
    DjangoIdempotencyStore,
    DjangoInboxStore,
    DjangoLeaseStore,
    DjangoMutationBatchStore,
    DjangoOutboxStore,
    DjangoReceiptStore,
    DjangoReplayStore,
)
from pytitect.idempotency import IdempotencyStoreHarness, InMemoryIdempotencyStore
from pytitect.inbox import InboxStoreHarness, InMemoryInboxStore
from pytitect.leases import InMemoryLeaseStore, LeaseAuthority, LeaseStoreHarness
from pytitect.outbox import InMemoryOutboxStore, OutboxStoreHarness
from pytitect.receipts import InMemoryReceiptStore, ReceiptStoreHarness
from pytitect.security import InMemoryReplayStore, ReplayStoreHarness
from pytitect.sync import (
    GenerationStoreHarness,
    InMemoryGenerationStore,
    InMemoryMutationBatchStore,
    MutationBatchStoreHarness,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class SynchronousBoundary:
    def __init__(self) -> None:
        self._callbacks: list[Callable[[], None]] = []

    @contextmanager
    def atomic(self):  # type: ignore[no-untyped-def]
        before = len(self._callbacks)
        try:
            yield
        except Exception:
            del self._callbacks[before:]
            raise
        else:
            callbacks, self._callbacks = self._callbacks[before:], self._callbacks[:before]
            for callback in callbacks:
                callback()

    def on_commit(self, callback: Callable[[], None]) -> None:
        self._callbacks.append(callback)


def test_in_memory_stores_conform_to_public_harnesses() -> None:
    ReplayStoreHarness(InMemoryReplayStore).exercise(now=NOW)
    InboxStoreHarness(InMemoryInboxStore).exercise(now=NOW)
    OutboxStoreHarness(InMemoryOutboxStore).exercise(payload={"value": 1}, now=NOW)
    CheckpointStoreHarness(InMemoryCheckpointStore).exercise()
    ReceiptStoreHarness(InMemoryReceiptStore).exercise(value={"value": 1}, now=NOW)
    IdempotencyStoreHarness(InMemoryIdempotencyStore).exercise(value={"value": 1}, now=NOW)
    LeaseStoreHarness(InMemoryLeaseStore).exercise(now=NOW)
    GenerationStoreHarness(InMemoryGenerationStore).exercise()
    MutationBatchStoreHarness(InMemoryMutationBatchStore).exercise(result={"value": 1}, now=NOW)
    TransactionBoundaryHarness(SynchronousBoundary).exercise()


def test_callback_adapters_conform_to_public_harnesses() -> None:
    alias = "conformance"

    def checked(using: str) -> None:
        if using != alias:
            raise AssertionError("the callback received an unexpected database alias")

    def replay_factory() -> DjangoReplayStore:
        memory = InMemoryReplayStore()
        return DjangoReplayStore.from_callbacks(
            using=alias,
            reserve_digest=lambda namespace, digest, *, now, ttl, using: (
                checked(using),
                memory.reserve(namespace, digest, now=now, ttl=ttl),
            )[1],
        )

    def inbox_factory() -> DjangoInboxStore:
        memory = InMemoryInboxStore()
        return DjangoInboxStore.from_callbacks(
            using=alias,
            begin=lambda scope, message_id, *, token, now, ttl, using: (
                checked(using),
                memory.begin(scope, message_id, token=token, now=now, ttl=ttl),
            )[1],
            complete=lambda scope, message_id, *, token, now, using: (
                checked(using),
                memory.complete(scope, message_id, token=token, now=now),
            )[1],
            abandon=lambda scope, message_id, *, token, using: (
                checked(using),
                memory.abandon(scope, message_id, token=token),
            )[1],
        )

    def outbox_factory() -> DjangoOutboxStore[dict[str, int]]:
        memory = InMemoryOutboxStore[dict[str, int]]()
        return DjangoOutboxStore.from_callbacks(
            using=alias,
            add=lambda envelope, *, using: (checked(using), memory.add(envelope))[1],
            claim=lambda *, now, limit, claim_ttl, using: (
                checked(using),
                memory.claim(now=now, limit=limit, claim_ttl=claim_ttl),
            )[1],
            delivered=lambda claim, *, at, using: (
                checked(using),
                memory.delivered(claim, at=at),
            )[1],
            retry=lambda claim, *, available_at, using: (
                checked(using),
                memory.retry(claim, available_at=available_at),
            )[1],
            failed=lambda claim, *, reason, at, using: (
                checked(using),
                memory.failed(claim, reason=reason, at=at),
            )[1],
        )

    def checkpoint_factory() -> DjangoCheckpointStore:
        memory = InMemoryCheckpointStore()
        return DjangoCheckpointStore.from_callbacks(
            using=alias,
            load=lambda stream, *, using: (checked(using), memory.load(stream))[1],
            load_for_update=lambda stream, *, using: (
                checked(using),
                memory.load_for_update(stream),
            )[1],
            advance=lambda stream, *, expected, checkpoint, using: (
                checked(using),
                memory.advance(stream, expected=expected, checkpoint=checkpoint),
            )[1],
        )

    def receipt_factory() -> DjangoReceiptStore[dict[str, int]]:
        memory = InMemoryReceiptStore[dict[str, int]]()
        return DjangoReceiptStore.from_callbacks(
            using=alias,
            get=lambda receipt_id, *, using: (checked(using), memory.get(receipt_id))[1],
            add=lambda receipt, *, using: (checked(using), memory.add(receipt))[1],
            transition=lambda receipt, target, *, using: (
                checked(using),
                memory.transition(receipt, target),
            )[1],
            reconcile_uncertain=lambda receipt, target, *, using: (
                checked(using),
                memory.reconcile_uncertain(receipt, target),
            )[1],
        )

    def idempotency_factory() -> DjangoIdempotencyStore[dict[str, int]]:
        memory = InMemoryIdempotencyStore[dict[str, int]]()
        return DjangoIdempotencyStore.from_callbacks(
            using=alias,
            reserve=lambda scope, key, fingerprint, *, now, lease_ttl, using: (
                checked(using),
                memory.reserve(scope, key, fingerprint, now=now, lease_ttl=lease_ttl),
            )[1],
            renew=lambda token, *, now, lease_ttl, using: (
                checked(using),
                memory.renew(token, now=now, lease_ttl=lease_ttl),
            )[1],
            complete=lambda token, value, *, now, retention_ttl, using: (
                checked(using),
                memory.complete(token, value, now=now, retention_ttl=retention_ttl),
            )[1],
            mark_uncertain=lambda token, reason, *, now, retention_ttl, using: (
                checked(using),
                memory.mark_uncertain(token, reason, now=now, retention_ttl=retention_ttl),
            )[1],
            abandon=lambda token, *, now, using: (
                checked(using),
                memory.abandon(token, now=now),
            )[1],
        )

    def lease_factory() -> DjangoLeaseStore[str]:
        memory = InMemoryLeaseStore[str]()

        def lock_authority(resource: str, *, using: str) -> LeaseAuthority | None:
            checked(using)
            current = memory.current(resource)
            if current is None:
                return None
            return LeaseAuthority(current.owner, current.fencing_token, current.expires_at)

        return DjangoLeaseStore.from_callbacks(
            using=alias,
            acquire=lambda resource, *, owner, now, ttl, using: (
                checked(using),
                memory.acquire(resource, owner=owner, now=now, ttl=ttl),
            )[1],
            renew=lambda lease, *, now, ttl, using: (
                checked(using),
                memory.renew(lease, now=now, ttl=ttl),
            )[1],
            release=lambda lease, *, now, using: (
                checked(using),
                memory.release(lease, now=now),
            )[1],
            authority=lambda resource, *, using: (
                checked(using),
                memory.authority(resource),
            )[1],
            lock_authority=lock_authority,
        )

    def generation_factory() -> DjangoGenerationStore:
        memory = InMemoryGenerationStore()
        return DjangoGenerationStore.from_callbacks(
            using=alias,
            load_for_update=lambda dataset, partition, *, using: (
                checked(using),
                memory.load_for_update(dataset, partition),
            )[1],
            compare_and_set=lambda dataset, partition, *, expected, generation, using: (
                checked(using),
                memory.compare_and_set(
                    dataset, partition, expected=expected, generation=generation
                ),
            )[1],
        )

    def mutation_batch_factory() -> DjangoMutationBatchStore[dict[str, int]]:
        memory = InMemoryMutationBatchStore[dict[str, int]]()
        return DjangoMutationBatchStore.from_callbacks(
            using=alias,
            begin=lambda namespace, batch_id, fingerprint, *, total_items, now, lease_ttl, using: (
                checked(using),
                memory.begin(
                    namespace,
                    batch_id,
                    fingerprint,
                    total_items=total_items,
                    now=now,
                    lease_ttl=lease_ttl,
                ),
            )[1],
            renew=lambda current, *, now, lease_ttl, using: (
                checked(using),
                memory.renew(current, now=now, lease_ttl=lease_ttl),
            )[1],
            advance=lambda current, receipt, *, now, lease_ttl, using: (
                checked(using),
                memory.advance(current, receipt, now=now, lease_ttl=lease_ttl),
            )[1],
            complete=lambda current, *, now, retention_ttl, using: (
                checked(using),
                memory.complete(current, now=now, retention_ttl=retention_ttl),
            )[1],
            mark_uncertain=lambda current, reason, *, now, retention_ttl, using: (
                checked(using),
                memory.mark_uncertain(current, reason, now=now, retention_ttl=retention_ttl),
            )[1],
        )

    ReplayStoreHarness(replay_factory).exercise(now=NOW)
    InboxStoreHarness(inbox_factory).exercise(now=NOW)
    OutboxStoreHarness(outbox_factory).exercise(payload={"value": 1}, now=NOW)
    CheckpointStoreHarness(checkpoint_factory).exercise()
    ReceiptStoreHarness(receipt_factory).exercise(value={"value": 1}, now=NOW)
    IdempotencyStoreHarness(idempotency_factory).exercise(value={"value": 1}, now=NOW)
    LeaseStoreHarness(lease_factory).exercise(now=NOW)
    GenerationStoreHarness(generation_factory).exercise()
    MutationBatchStoreHarness(mutation_batch_factory).exercise(result={"value": 1}, now=NOW)
