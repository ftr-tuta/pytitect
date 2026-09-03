from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier

import pytest
from django.db import close_old_connections
from django.db import transaction
from django.utils import timezone
from pytitect import OpaqueId
from pytitect.checkpoints import AtomicCheckpointConfirmed
from pytitect.checkpoints import AtomicCheckpointCoordinator
from pytitect.checkpoints import Checkpoint
from pytitect.checkpoints import CheckpointItem
from pytitect.checkpoints import CheckpointStoreHarness
from pytitect.checkpoints import TransactionBoundaryHarness
from pytitect.django import DjangoCheckpointStore
from pytitect.django import DjangoGenerationStore
from pytitect.django import DjangoIdempotencyStore
from pytitect.django import DjangoInboxStore
from pytitect.django import DjangoLeaseStore
from pytitect.django import DjangoMutationBatchStore
from pytitect.django import DjangoOutboxStore
from pytitect.django import DjangoReceiptStore
from pytitect.django import DjangoReplayStore
from pytitect.django import DjangoRetentionMaintenance
from pytitect.django import DjangoTransactionalOperation
from pytitect.django import DjangoTransactionBoundary
from pytitect.django import RetentionIndexModels
from pytitect.django import TransactionalOperationCommitted
from pytitect.django import TransactionalOperationRolledBack
from pytitect.django import build_retention_index_check
from pytitect.idempotency import Execute
from pytitect.idempotency import IdempotencyPolicy
from pytitect.idempotency import IdempotencyScope
from pytitect.idempotency import IdempotencyStoreHarness
from pytitect.idempotency import InProgress
from pytitect.idempotency import Replay
from pytitect.idempotency import RequestFingerprint
from pytitect.inbox import InboxAccepted
from pytitect.inbox import InboxDuplicate
from pytitect.inbox import InboxScope
from pytitect.inbox import InboxStoreHarness
from pytitect.leases import LeaseAcquired
from pytitect.leases import LeaseReleased
from pytitect.leases import LeaseRenewed
from pytitect.leases import LeaseStoreHarness
from pytitect.leases import StaleLease
from pytitect.maintenance import ArchiveFailedOutboxPlan
from pytitect.maintenance import MaintenanceSummary
from pytitect.maintenance import PurgeDeliveredOutboxPlan
from pytitect.maintenance import PurgeIdempotencyPlan
from pytitect.maintenance import PurgeInboxPlan
from pytitect.maintenance import PurgeReceiptsPlan
from pytitect.maintenance import PurgeReplayPlan
from pytitect.outbox import OutboxEnvelope
from pytitect.outbox import OutboxStoreHarness
from pytitect.receipts import ConfirmedCompleted
from pytitect.receipts import ConfirmedRejected
from pytitect.receipts import MutationReceipt
from pytitect.receipts import ReceiptReconciler
from pytitect.receipts import ReceiptState
from pytitect.receipts import ReceiptStoreHarness
from pytitect.receipts import StillUncertain
from pytitect.security import ReplayAccepted
from pytitect.security import ReplayDetected
from pytitect.security import ReplayStoreHarness
from pytitect.sync import PER_ITEM
from pytitect.sync import BatchCommitted
from pytitect.sync import BatchInProgress
from pytitect.sync import BatchItem
from pytitect.sync import BatchItemReceipt
from pytitect.sync import BatchReplay
from pytitect.sync import GenerationCommitted
from pytitect.sync import GenerationGuard
from pytitect.sync import GenerationStoreHarness
from pytitect.sync import MutationBatchCoordinator
from pytitect.sync import MutationBatchState
from pytitect.sync import MutationBatchStoreHarness
from pytitect.sync import StaleGeneration

from pytitect_protocol_matrix.mobile_v2.models import CheckpointRecord
from pytitect_protocol_matrix.mobile_v2.models import DomainMutation
from pytitect_protocol_matrix.mobile_v2.models import GenerationRecord
from pytitect_protocol_matrix.mobile_v2.models import IdempotencyRecord
from pytitect_protocol_matrix.mobile_v2.models import InboxRecord
from pytitect_protocol_matrix.mobile_v2.models import LeaseRecord
from pytitect_protocol_matrix.mobile_v2.models import MutationBatchRecord
from pytitect_protocol_matrix.mobile_v2.models import OutboxArchive
from pytitect_protocol_matrix.mobile_v2.models import OutboxRecord
from pytitect_protocol_matrix.mobile_v2.models import ReceiptRecord
from pytitect_protocol_matrix.mobile_v2.models import ReplayRecord

pytestmark = pytest.mark.django_db(transaction=True)


def _json(value):  # type: ignore[no-untyped-def]
    return value


def test_postgresql_models_conform_to_public_store_harnesses() -> None:
    now = timezone.now()
    ReplayStoreHarness(
        lambda: DjangoReplayStore.from_model(ReplayRecord, using="default"),
    ).exercise(now=now)
    InboxStoreHarness(
        lambda: DjangoInboxStore.from_model(InboxRecord, using="default"),
    ).exercise(now=now)
    OutboxStoreHarness(
        lambda: DjangoOutboxStore.from_model(
            OutboxRecord,
            using="default",
            encode_payload=_json,
            decode_payload=_json,
        ),
    ).exercise(payload={"value": 1}, now=now)
    with transaction.atomic(using="default"):
        CheckpointStoreHarness(
            lambda: DjangoCheckpointStore.from_model(CheckpointRecord, using="default"),
        ).exercise()
    ReceiptStoreHarness(
        lambda: DjangoReceiptStore.from_model(
            ReceiptRecord,
            using="default",
            encode_result=_json,
            decode_result=_json,
        ),
    ).exercise(value={"value": 1}, now=now)
    IdempotencyStoreHarness(
        lambda: DjangoIdempotencyStore.from_model(
            IdempotencyRecord,
            using="default",
            encode_value=_json,
            decode_value=_json,
        ),
    ).exercise(value={"value": 1}, now=now)
    LeaseStoreHarness(
        lambda: DjangoLeaseStore.from_model(
            LeaseRecord,
            using="default",
            encode_resource=str,
        ),
    ).exercise(now=now)
    with transaction.atomic(using="default"):
        GenerationStoreHarness(
            lambda: DjangoGenerationStore.from_model(GenerationRecord, using="default"),
        ).exercise()
    MutationBatchStoreHarness(
        lambda: DjangoMutationBatchStore.from_model(
            MutationBatchRecord,
            using="default",
            encode_result=_json,
            decode_result=_json,
        ),
    ).exercise(result={"value": 1}, now=now)
    TransactionBoundaryHarness(
        lambda: DjangoTransactionBoundary("default"),
    ).exercise()


def test_postgresql_retention_is_bounded_and_archival_is_atomic() -> None:
    now = timezone.now()
    maintenance = DjangoRetentionMaintenance("default")
    IdempotencyRecord.objects.create(
        namespace="maintenance",
        subject="subject",
        operation="operation",
        idempotency_key="completed",
        fingerprint="a" * 64,
        reservation_token="completed-token",
        state="completed",
        expires_at=now,
        value={"value": 1},
        updated_at=now,
    )
    IdempotencyRecord.objects.create(
        namespace="maintenance",
        subject="subject",
        operation="operation",
        idempotency_key="uncertain",
        fingerprint="b" * 64,
        reservation_token="uncertain-token",
        state="uncertain",
        expires_at=now,
        uncertainty_reason="outcome unknown",
        updated_at=now,
    )
    assert maintenance.purge_idempotency(
        IdempotencyRecord,
        PurgeIdempotencyPlan(now, batch_size=1, dry_run=True),
    ) == MaintenanceSummary(1, 0, True)
    assert maintenance.purge_idempotency(
        IdempotencyRecord,
        PurgeIdempotencyPlan(now),
    ) == MaintenanceSummary(1, 1, False)

    MutationBatchRecord.objects.create(
        namespace="maintenance",
        batch_id="uncertain",
        fingerprint="c" * 64,
        reservation_token="batch-token",
        state="uncertain",
        total_items=1,
        next_index=1,
        receipts=[{"item_id": "item", "result": {"value": 1}}],
        lease_expires_at=now,
        retention_expires_at=now,
        uncertainty_reason="outcome unknown",
        updated_at=now,
    )
    assert maintenance.purge_mutation_batches(
        MutationBatchRecord,
        PurgeIdempotencyPlan(now),
    ) == MaintenanceSummary(0, 0, False)
    assert maintenance.purge_mutation_batches(
        MutationBatchRecord,
        PurgeIdempotencyPlan(now, include_uncertain=True),
    ) == MaintenanceSummary(1, 1, False)
    assert (
        IdempotencyRecord.objects.values_list("state", flat=True).get() == "uncertain"
    )
    assert maintenance.purge_idempotency(
        IdempotencyRecord,
        PurgeIdempotencyPlan(now, include_uncertain=True),
    ) == MaintenanceSummary(1, 1, False)

    for digest in ("a" * 64, "b" * 64):
        ReplayRecord.objects.create(
            namespace="maintenance", digest=digest, expires_at=now
        )
    assert maintenance.purge_replay(
        ReplayRecord,
        PurgeReplayPlan(now, batch_size=1),
    ) == MaintenanceSummary(1, 1, False)
    assert ReplayRecord.objects.count() == 1

    InboxRecord.objects.create(
        namespace="maintenance",
        source="source",
        consumer="consumer",
        message_id="completed",
        reservation_token="token",
        expires_at=now + timedelta(minutes=1),
        completed_at=now,
    )
    InboxRecord.objects.create(
        namespace="maintenance",
        source="source",
        consumer="consumer",
        message_id="expired",
        reservation_token="token",
        expires_at=now,
    )
    assert maintenance.purge_inbox(
        InboxRecord,
        PurgeInboxPlan(now, batch_size=2),
    ) == MaintenanceSummary(2, 2, False)

    ReceiptRecord.objects.create(
        receipt_id="completed",
        kind="mutation",
        state="completed",
        created_at=now,
        updated_at=now,
        result={"value": 1},
    )
    ReceiptRecord.objects.create(
        receipt_id="uncertain",
        kind="mutation",
        state="uncertain",
        created_at=now,
        updated_at=now,
    )
    assert maintenance.purge_receipts(
        ReceiptRecord,
        PurgeReceiptsPlan(now),
    ) == MaintenanceSummary(1, 1, False)
    assert ReceiptRecord.objects.values_list("state", flat=True).get() == "uncertain"

    outbox = DjangoOutboxStore.from_model(
        OutboxRecord,
        using="default",
        encode_payload=_json,
        decode_payload=_json,
    )
    for message_id in ("delivered", "failed"):
        outbox.add(
            OutboxEnvelope(
                OpaqueId(message_id),
                "maintenance",
                {"message": message_id},
                now,
                now,
            ),
        )
    delivered, failed = outbox.claim(
        now=now,
        limit=2,
        claim_ttl=timedelta(minutes=1),
    )
    assert outbox.delivered(delivered, at=now)
    assert outbox.failed(failed, reason="terminal", at=now)
    assert OutboxRecord.objects.count() == 2
    assert maintenance.purge_delivered_outbox(
        OutboxRecord,
        PurgeDeliveredOutboxPlan(now),
    ) == MaintenanceSummary(1, 1, False)

    def archive(records, *, using):  # type: ignore[no-untyped-def]
        for record in records:
            OutboxArchive.objects.using(using).create(
                message_id=str(record.envelope.message_id),
                topic=record.envelope.topic,
                payload=record.envelope.payload,
                occurred_at=record.envelope.occurred_at,
                available_at=record.envelope.available_at,
                attempt=record.envelope.attempt,
                failure_reason=record.reason,
                failed_at=record.failed_at,
            )

    assert maintenance.archive_failed_outbox(
        OutboxRecord,
        ArchiveFailedOutboxPlan(now),
        decode_payload=_json,
        archive=archive,
    ) == MaintenanceSummary(1, 1, False)
    assert not OutboxRecord.objects.exists()
    assert OutboxArchive.objects.values_list("message_id", flat=True).get() == "failed"

    rollback_envelope = OutboxEnvelope(
        OpaqueId("archive-rollback"),
        "maintenance",
        {"message": "archive-rollback"},
        now,
        now,
    )
    outbox.add(rollback_envelope)
    rollback_claim = outbox.claim(
        now=now,
        limit=1,
        claim_ttl=timedelta(minutes=1),
    )[0]
    assert outbox.failed(rollback_claim, reason="terminal", at=now)

    class ArchiveRejected(Exception):
        pass

    def reject_archive(records, *, using):  # type: ignore[no-untyped-def]
        record = records[0]
        OutboxArchive.objects.using(using).create(
            message_id=str(record.envelope.message_id),
            topic=record.envelope.topic,
            payload=record.envelope.payload,
            occurred_at=record.envelope.occurred_at,
            available_at=record.envelope.available_at,
            attempt=record.envelope.attempt,
            failure_reason=record.reason,
            failed_at=record.failed_at,
        )
        raise ArchiveRejected

    with pytest.raises(ArchiveRejected):
        maintenance.archive_failed_outbox(
            OutboxRecord,
            ArchiveFailedOutboxPlan(now),
            decode_payload=_json,
            archive=reject_archive,
        )
    assert OutboxRecord.objects.filter(message_id="archive-rollback").exists()
    assert not OutboxArchive.objects.filter(message_id="archive-rollback").exists()

    check = build_retention_index_check(
        RetentionIndexModels(
            idempotency=IdempotencyRecord,
            mutation_batches=MutationBatchRecord,
            replay=ReplayRecord,
            inbox=InboxRecord,
            receipts=ReceiptRecord,
            outbox=OutboxRecord,
        ),
    )
    assert check() == []


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
    inbox_scope = InboxScope("canary", "upstream", "projection")
    message_id = OpaqueId("message-1")
    assert isinstance(
        inbox.begin(
            inbox_scope,
            message_id,
            token="worker",
            now=now,
            ttl=timedelta(minutes=1),
        ),
        InboxAccepted,
    )
    assert inbox.complete(inbox_scope, message_id, token="worker", now=now)
    assert isinstance(
        inbox.begin(
            inbox_scope,
            message_id,
            token="other",
            now=now,
            ttl=timedelta(minutes=1),
        ),
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
            OutboxEnvelope(OpaqueId(message), "topic", {"id": message}, now, now),
        )
    claims = outbox.claim(now=now, limit=1, claim_ttl=timedelta(minutes=1))
    assert [str(claim.envelope.message_id) for claim in claims] == ["a"]
    assert outbox.delivered(claims[0], at=now)
    second = outbox.claim(now=now, limit=1, claim_ttl=timedelta(minutes=1))
    assert [str(claim.envelope.message_id) for claim in second] == ["b"]


def test_checkpoint_lease_and_generation_guards_share_the_locking_transaction() -> None:
    now = timezone.now()
    boundary = DjangoTransactionBoundary("default")
    checkpoint_store = DjangoCheckpointStore.from_model(
        CheckpointRecord,
        using="default",
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
            "orders",
            "tenant-1",
            expected=None,
            generation=2,
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
        dataset="orders",
        partition="tenant-1",
        expected=1,
        mutation=lambda: None,
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
                lease_ttl=timedelta(minutes=1),
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
    )
    acquired = leases.acquire(
        "concurrent-job",
        owner="worker",
        now=now,
        ttl=timedelta(minutes=1),
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


def test_mutation_batch_resumes_one_worker_after_a_committed_prefix() -> None:
    now = timezone.now()

    class Clock:
        def __init__(self, value):  # type: ignore[no-untyped-def]
            self.value = value

        def now(self):  # type: ignore[no-untyped-def]
            return self.value

    clock = Clock(now)
    batches = DjangoMutationBatchStore.from_model(
        MutationBatchRecord,
        using="default",
        encode_result=_json,
        decode_result=_json,
    )
    items = DjangoIdempotencyStore.from_model(
        IdempotencyRecord,
        using="default",
        encode_value=lambda receipt: {
            "item_id": receipt.item_id,
            "result": receipt.result,
            "replayed": receipt.replayed,
        },
        decode_value=lambda value: BatchItemReceipt(
            value["item_id"],
            value["result"],
            value["replayed"],
        ),
    )
    policy = IdempotencyPolicy(
        timedelta(seconds=5),
        timedelta(hours=1),
        timedelta(days=1),
    )
    coordinator = MutationBatchCoordinator(
        batches,
        items,
        DjangoTransactionBoundary("default"),
        using="default",
        clock=clock,
    )
    payloads = (BatchItem("one", 1), BatchItem("two", 2))

    def crash_after_first(item, using):  # type: ignore[no-untyped-def]
        if item.item_id == "two":
            raise RuntimeError("synthetic crash after committed prefix")
        row = DomainMutation.objects.using(using).create(
            protocol="batch",
            value=item.payload,
        )
        return {"row_id": row.pk}

    with pytest.raises(RuntimeError, match="committed prefix"):
        coordinator.execute(
            batch_id="resumable-batch",
            items=payloads,
            policy=PER_ITEM,
            mutate=crash_after_first,
            idempotency_policy=policy,
        )
    record = MutationBatchRecord.objects.get(batch_id="resumable-batch")
    assert record.state == MutationBatchState.PARTIALLY_COMMITTED
    assert record.next_index == 1
    assert DomainMutation.objects.filter(protocol="batch").count() == 1

    clock.value += timedelta(seconds=5)
    resume_barrier = Barrier(2)

    def resume():  # type: ignore[no-untyped-def]
        close_old_connections()
        try:
            resume_barrier.wait()
            local = MutationBatchCoordinator(
                batches,
                items,
                DjangoTransactionBoundary("default"),
                using="default",
                clock=clock,
            )
            return local.execute(
                batch_id="resumable-batch",
                items=payloads,
                policy=PER_ITEM,
                mutate=lambda item, using: {
                    "row_id": DomainMutation.objects.using(using)
                    .create(protocol="batch", value=item.payload)
                    .pk,
                },
                idempotency_policy=policy,
            )
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [
            future.result() for future in [executor.submit(resume) for _ in range(2)]
        ]
    assert sum(isinstance(outcome, BatchCommitted) for outcome in outcomes) == 1
    assert (
        sum(isinstance(outcome, (BatchInProgress, BatchReplay)) for outcome in outcomes)
        == 1
    )
    assert DomainMutation.objects.filter(protocol="batch").count() == 2


def test_mutation_batch_recovers_a_crash_before_final_confirmation() -> None:
    now = timezone.now()

    class Clock:
        def __init__(self, value):  # type: ignore[no-untyped-def]
            self.value = value

        def now(self):  # type: ignore[no-untyped-def]
            return self.value

    clock = Clock(now)
    durable_batches = DjangoMutationBatchStore.from_model(
        MutationBatchRecord,
        using="default",
        encode_result=_json,
        decode_result=_json,
    )

    class FinalConfirmationCrash:
        using = "default"
        fail_once = True

        def begin(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return durable_batches.begin(*args, **kwargs)

        def renew(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return durable_batches.renew(*args, **kwargs)

        def advance(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return durable_batches.advance(*args, **kwargs)

        def complete(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("synthetic crash before final confirmation")
            return durable_batches.complete(*args, **kwargs)

        def mark_uncertain(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return durable_batches.mark_uncertain(*args, **kwargs)

    batches = FinalConfirmationCrash()
    items = DjangoIdempotencyStore.from_model(
        IdempotencyRecord,
        using="default",
        encode_value=lambda receipt: {
            "item_id": receipt.item_id,
            "result": receipt.result,
            "replayed": receipt.replayed,
        },
        decode_value=lambda value: BatchItemReceipt(
            value["item_id"],
            value["result"],
            value["replayed"],
        ),
    )
    policy = IdempotencyPolicy(
        timedelta(seconds=5),
        timedelta(hours=1),
        timedelta(days=1),
    )
    coordinator = MutationBatchCoordinator(
        batches,
        items,
        DjangoTransactionBoundary("default"),
        using="default",
        clock=clock,
    )
    payloads = (BatchItem("final-item", 3),)

    with pytest.raises(RuntimeError, match="before final confirmation"):
        coordinator.execute(
            batch_id="final-confirmation-crash",
            items=payloads,
            policy=PER_ITEM,
            mutate=lambda item, using: {
                "row_id": DomainMutation.objects.using(using)
                .create(protocol="final-crash", value=item.payload)
                .pk,
            },
            idempotency_policy=policy,
        )
    record = MutationBatchRecord.objects.get(batch_id="final-confirmation-crash")
    assert record.state == MutationBatchState.PARTIALLY_COMMITTED
    assert record.next_index == 1
    assert DomainMutation.objects.filter(protocol="final-crash").count() == 1

    before_expiry = coordinator.execute(
        batch_id="final-confirmation-crash",
        items=payloads,
        policy=PER_ITEM,
        mutate=lambda item, using: pytest.fail("retry repeated a retained item"),
        idempotency_policy=policy,
    )
    assert isinstance(before_expiry, BatchInProgress)

    clock.value += timedelta(seconds=5)
    recovered = coordinator.execute(
        batch_id="final-confirmation-crash",
        items=payloads,
        policy=PER_ITEM,
        mutate=lambda item, using: pytest.fail("recovery repeated a retained item"),
        idempotency_policy=policy,
    )
    assert isinstance(recovered, BatchCommitted)
    assert recovered.receipts[0].replayed is True
    assert DomainMutation.objects.filter(protocol="final-crash").count() == 1


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
            timedelta(minutes=1),
            timedelta(hours=1),
            timedelta(days=1),
        ),
        clock=Clock(),
    )
    scope = IdempotencyScope("canary", "subject", "transactional")
    fingerprint = RequestFingerprint.from_json({"value": 7})

    outbox.add(OutboxEnvelope(OpaqueId("duplicate"), "topic", {}, now, now))

    def mutate(using):  # type: ignore[no-untyped-def]
        row = DomainMutation.objects.using(using).create(
            protocol="transactional",
            value=7,
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
