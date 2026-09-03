from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier

import pytest
from django.db import close_old_connections, transaction
from django.utils import timezone

from pytitect import OpaqueId
from pytitect.checkpoints import (
    AtomicCheckpointConfirmed,
    AtomicCheckpointCoordinator,
    Checkpoint,
    CheckpointItem,
)
from pytitect.django import (
    DjangoCheckpointStore,
    DjangoGenerationStore,
    DjangoIdempotencyStore,
    DjangoInboxStore,
    DjangoLeaseStore,
    DjangoOutboxStore,
    DjangoReceiptStore,
    DjangoReplayStore,
    DjangoTransactionBoundary,
    DjangoTransactionalOperation,
    TransactionalOperationCommitted,
    TransactionalOperationRolledBack,
)
from pytitect.idempotency import (
    Execute,
    IdempotencyPolicy,
    IdempotencyScope,
    InProgress,
    Replay,
    RequestFingerprint,
)
from pytitect.inbox import InboxAccepted, InboxDuplicate
from pytitect.leases import LeaseAcquired, LeaseReleased, LeaseRenewed, StaleLease
from pytitect.outbox import OutboxEnvelope
from pytitect.receipts import (
    ConfirmedCompleted,
    ConfirmedRejected,
    MutationReceipt,
    ReceiptReconciler,
    ReceiptState,
    StillUncertain,
)
from pytitect.security import ReplayAccepted, ReplayDetected
from pytitect.sync import GenerationCommitted, GenerationGuard, StaleGeneration
from pytitect_protocol_matrix.mobile_v2.models import (
    CheckpointRecord,
    DomainMutation,
    GenerationRecord,
    IdempotencyRecord,
    InboxRecord,
    LeaseRecord,
    OutboxRecord,
    ReceiptRecord,
    ReplayRecord,
)

pytestmark = pytest.mark.django_db(transaction=True)


def _json(value):  # type: ignore[no-untyped-def]
    return value


def test_replay_inbox_outbox_and_digest_only_storage() -> None:
    now = timezone.now()
    replay = DjangoReplayStore.from_model(ReplayRecord, using="default")
    first = replay.reserve("dpop", "clear-proof", now=now, ttl=timedelta(minutes=1))
    assert isinstance(first, ReplayAccepted)
    assert isinstance(
        replay.reserve("dpop", "clear-proof", now=now, ttl=timedelta(minutes=1)),
        ReplayDetected,
    )
    row = ReplayRecord.objects.get()
    assert row.digest != "clear-proof"
    assert len(row.digest) == 64

    inbox = DjangoInboxStore.from_model(InboxRecord, using="default")
    message_id = OpaqueId("message-1")
    assert isinstance(
        inbox.begin(message_id, token="worker", now=now, ttl=timedelta(minutes=1)),
        InboxAccepted,
    )
    assert inbox.complete(message_id, token="worker", now=now)
    assert isinstance(
        inbox.begin(message_id, token="other", now=now, ttl=timedelta(minutes=1)),
        InboxDuplicate,
    )

    outbox = DjangoOutboxStore.from_model(
        OutboxRecord,
        using="default",
        encode_payload=_json,
        decode_payload=_json,
    )
    for message in ("b", "a"):
        outbox.add(
            OutboxEnvelope(OpaqueId(message), "topic", {"id": message}, now, now)
        )
    claims = outbox.claim(now=now, limit=1, claim_ttl=timedelta(minutes=1))
    assert [str(claim.envelope.message_id) for claim in claims] == ["a"]
    assert outbox.delivered(claims[0])
    second = outbox.claim(now=now, limit=1, claim_ttl=timedelta(minutes=1))
    assert [str(claim.envelope.message_id) for claim in second] == ["b"]


def test_checkpoint_lease_and_generation_guards_share_the_locking_transaction() -> None:
    now = timezone.now()
    boundary = DjangoTransactionBoundary("default")
    checkpoint_store = DjangoCheckpointStore.from_model(
        CheckpointRecord, using="default"
    )
    coordinator = AtomicCheckpointCoordinator(checkpoint_store, boundary)

    def mutate(value):  # type: ignore[no-untyped-def]
        DomainMutation.objects.create(protocol="checkpoint", value=value)

    applied = coordinator.apply(
        stream="orders",
        items=iter([CheckpointItem(Checkpoint(b"1"), 1)]),
        apply_state=mutate,
    )
    assert isinstance(applied, AtomicCheckpointConfirmed)
    assert checkpoint_store.load("orders") == Checkpoint(b"1")

    leases = DjangoLeaseStore.from_model(
        LeaseRecord,
        using="default",
        encode_resource=str,
        decode_resource=str,
    )
    acquired = leases.acquire("job", owner="one", now=now, ttl=timedelta(minutes=1))
    assert isinstance(acquired, LeaseAcquired)
    assert isinstance(leases.release(acquired.lease, now=now), LeaseReleased)
    takeover = leases.acquire("job", owner="two", now=now, ttl=timedelta(minutes=1))
    assert isinstance(takeover, LeaseAcquired)
    assert takeover.lease.fencing_token == acquired.lease.fencing_token + 1
    assert LeaseRecord.objects.count() == 1

    generations = DjangoGenerationStore.from_model(GenerationRecord, using="default")
    with transaction.atomic(using="default"):
        assert generations.compare_and_set(
            "orders", "tenant-1", expected=None, generation=2
        )
    guard = GenerationGuard(generations, boundary)
    committed = guard.commit(
        dataset="orders",
        partition="tenant-1",
        expected=2,
        mutation=lambda: (
            DomainMutation.objects.create(protocol="generation", value=2).pk
        ),
    )
    assert isinstance(committed, GenerationCommitted)
    stale = guard.commit(
        dataset="orders", partition="tenant-1", expected=1, mutation=lambda: None
    )
    assert isinstance(stale, StaleGeneration)


def test_two_workers_serialize_idempotency_and_skip_locked_outbox_claims() -> None:
    now = timezone.now()
    idempotency = DjangoIdempotencyStore.from_model(
        IdempotencyRecord,
        using="default",
        encode_value=_json,
        decode_value=_json,
    )
    scope = IdempotencyScope("canary", "subject", "mutation")
    fingerprint = RequestFingerprint.from_json({"value": 1})
    reservation_barrier = Barrier(2)

    def reserve():  # type: ignore[no-untyped-def]
        close_old_connections()
        try:
            reservation_barrier.wait()
            return idempotency.reserve(
                scope,
                "concurrent-key",
                fingerprint,
                now=now,
                ttl=timedelta(minutes=1),
            )
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as executor:
        decisions = [
            future.result() for future in [executor.submit(reserve) for _ in range(2)]
        ]
    assert sum(isinstance(decision, Execute) for decision in decisions) == 1
    assert sum(isinstance(decision, InProgress) for decision in decisions) == 1

    outbox = DjangoOutboxStore.from_model(
        OutboxRecord,
        using="default",
        encode_payload=_json,
        decode_payload=_json,
    )
    for message in ("concurrent-b", "concurrent-a"):
        outbox.add(OutboxEnvelope(OpaqueId(message), "topic", {}, now, now))
    claim_barrier = Barrier(2)

    def claim():  # type: ignore[no-untyped-def]
        close_old_connections()
        try:
            claim_barrier.wait()
            return outbox.claim(now=now, limit=1, claim_ttl=timedelta(minutes=1))
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = [
            future.result() for future in [executor.submit(claim) for _ in range(2)]
        ]
    claimed_ids = sorted(str(result[0].envelope.message_id) for result in claims)
    assert claimed_ids == ["concurrent-a", "concurrent-b"]


def test_concurrent_renewal_makes_the_old_lease_stale() -> None:
    now = timezone.now()
    leases = DjangoLeaseStore.from_model(
        LeaseRecord,
        using="default",
        encode_resource=str,
        decode_resource=str,
    )
    acquired = leases.acquire(
        "concurrent-job", owner="worker", now=now, ttl=timedelta(minutes=1)
    )
    assert isinstance(acquired, LeaseAcquired)
    renew_at = now + timedelta(seconds=1)
    renew_barrier = Barrier(2)

    def renew():  # type: ignore[no-untyped-def]
        close_old_connections()
        try:
            renew_barrier.wait()
            return leases.renew(
                acquired.lease,
                now=renew_at,
                ttl=timedelta(minutes=2),
            )
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            future.result() for future in [executor.submit(renew) for _ in range(2)]
        ]
    assert sum(isinstance(result, LeaseRenewed) for result in results) == 1
    assert sum(isinstance(result, StaleLease) for result in results) == 1


def test_transactional_operation_rolls_back_every_owned_write_and_replays() -> None:
    now = timezone.now()

    class Clock:
        def now(self):  # type: ignore[no-untyped-def]
            return now

    idempotency = DjangoIdempotencyStore.from_model(
        IdempotencyRecord,
        using="default",
        encode_value=_json,
        decode_value=_json,
    )
    receipts = DjangoReceiptStore.from_model(
        ReceiptRecord,
        using="default",
        encode_result=_json,
        decode_result=_json,
    )
    outbox = DjangoOutboxStore.from_model(
        OutboxRecord,
        using="default",
        encode_payload=_json,
        decode_payload=_json,
    )
    operation = DjangoTransactionalOperation(
        using="default",
        domain_using="default",
        idempotency=idempotency,
        receipts=receipts,
        outbox=outbox,
        idempotency_policy=IdempotencyPolicy(
            timedelta(minutes=1), timedelta(hours=1), timedelta(days=1)
        ),
        clock=Clock(),
    )
    scope = IdempotencyScope("canary", "subject", "transactional")
    fingerprint = RequestFingerprint.from_json({"value": 7})

    outbox.add(OutboxEnvelope(OpaqueId("duplicate"), "topic", {}, now, now))

    def mutate(using):  # type: ignore[no-untyped-def]
        row = DomainMutation.objects.using(using).create(
            protocol="transactional", value=7
        )
        return {"mutation_id": row.pk}

    def receipt(value):  # type: ignore[no-untyped-def]
        return MutationReceipt(
            OpaqueId("transactional-receipt"),
            ReceiptState.COMPLETED,
            now,
            now,
            result=value,
        )

    rolled_back = operation.execute(
        scope=scope,
        key="duplicate-outbox",
        fingerprint=fingerprint,
        mutate=mutate,
        make_receipt=receipt,
        make_outbox=lambda value: (
            OutboxEnvelope(OpaqueId("duplicate"), "topic", value, now, now),
        ),
    )
    assert isinstance(rolled_back, TransactionalOperationRolledBack)
    assert DomainMutation.objects.count() == 0
    assert ReceiptRecord.objects.count() == 0
    assert IdempotencyRecord.objects.count() == 0
    assert OutboxRecord.objects.count() == 1

    def crash(using):  # type: ignore[no-untyped-def]
        DomainMutation.objects.using(using).create(protocol="crash", value=7)
        raise RuntimeError("synthetic crash")

    with pytest.raises(RuntimeError, match="synthetic crash"):
        operation.execute(
            scope=scope,
            key="crash",
            fingerprint=fingerprint,
            mutate=crash,
            make_receipt=receipt,
        )
    assert DomainMutation.objects.count() == 0
    assert IdempotencyRecord.objects.count() == 0

    committed = operation.execute(
        scope=scope,
        key="success",
        fingerprint=fingerprint,
        mutate=mutate,
        make_receipt=receipt,
        make_outbox=lambda value: (
            OutboxEnvelope(OpaqueId("success"), "topic", value, now, now),
        ),
    )
    assert isinstance(committed, TransactionalOperationCommitted)
    replay = operation.execute(
        scope=scope,
        key="success",
        fingerprint=fingerprint,
        mutate=lambda using: pytest.fail("replay repeated the domain mutation"),
        make_receipt=receipt,
    )
    assert isinstance(replay, Replay)
    assert DomainMutation.objects.filter(protocol="transactional").count() == 1


def test_uncertain_receipt_reconciliation_has_one_concurrent_winner() -> None:
    now = timezone.now()
    store = DjangoReceiptStore.from_model(
        ReceiptRecord,
        using="default",
        encode_result=_json,
        decode_result=_json,
    )
    uncertain = MutationReceipt(OpaqueId("uncertain"), ReceiptState.UNCERTAIN, now, now)
    assert store.add(uncertain)
    reconcile_barrier = Barrier(2)

    def reconcile(target):  # type: ignore[no-untyped-def]
        close_old_connections()
        try:
            reconcile_barrier.wait()
            return ReceiptReconciler(store).reconcile(
                uncertain.receipt_id,
                target,
                at=now + timedelta(seconds=1),
                result={"confirmed": True}
                if target is ReceiptState.COMPLETED
                else None,
            )
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            future.result()
            for future in (
                executor.submit(reconcile, ReceiptState.COMPLETED),
                executor.submit(reconcile, ReceiptState.REJECTED),
            )
        ]
    assert (
        sum(
            isinstance(result, (ConfirmedCompleted, ConfirmedRejected))
            for result in results
        )
        == 1
    )
    assert sum(isinstance(result, StillUncertain) for result in results) == 1
