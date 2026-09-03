from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pytitect import OpaqueId
from pytitect.checkpoints import Checkpoint
from pytitect.django import (
    DjangoCheckpointStore,
    DjangoFencedCommit,
    DjangoGenerationStore,
    DjangoIdempotencyStore,
    DjangoInboxStore,
    DjangoLeaseStore,
    DjangoMutationBatchStore,
    DjangoOutboxStore,
    DjangoReceiptStore,
    DjangoReplayStore,
    DjangoTransactionalOperation,
    TransactionalOperationCommitted,
    TransactionalOperationRolledBack,
)
from pytitect.idempotency import (
    Execute,
    IdempotencyPolicy,
    IdempotencyScope,
    RequestFingerprint,
    ReservationAbandoned,
    ReservationCompleted,
    ReservationMarkedUncertain,
    ReservationRenewed,
    ReservationToken,
    StaleReservation,
)
from pytitect.inbox import InboxAccepted, InboxScope
from pytitect.leases import (
    FencedCommitted,
    Lease,
    LeaseAcquired,
    LeaseAuthority,
    LeaseReleased,
    LeaseRenewed,
)
from pytitect.outbox import OutboxAdded, OutboxClaim, OutboxDuplicate, OutboxEnvelope
from pytitect.receipts import MutationReceipt, ReceiptState
from pytitect.security import ReplayAccepted
from pytitect.sync.batches import (
    BatchConflict,
    BatchInProgress,
    BatchItemReceipt,
    BatchReplay,
    BatchUncertain,
    MutationBatchCompleted,
    MutationBatchLease,
    MutationBatchLeaseRenewed,
    MutationBatchMarkedUncertain,
    MutationBatchProgressed,
    MutationBatchState,
    StaleMutationBatchLease,
)


def test_callback_stores_always_receive_the_explicit_alias() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    seen: list[str] = []

    def record(using: str) -> None:
        seen.append(using)

    idempotency = DjangoIdempotencyStore.from_callbacks(
        using="events",
        reserve=lambda *args, using, **kwargs: (record(using), Execute(ReservationToken("t")))[1],
        renew=lambda *args, using, **kwargs: (
            record(using),
            ReservationRenewed(now + timedelta(seconds=1)),
        )[1],
        complete=lambda *args, using, **kwargs: (
            record(using),
            ReservationCompleted(now + timedelta(minutes=1)),
        )[1],
        mark_uncertain=lambda *args, using, **kwargs: (
            record(using),
            ReservationMarkedUncertain(now + timedelta(minutes=1)),
        )[1],
        abandon=lambda *args, using, **kwargs: (record(using), ReservationAbandoned())[1],
    )
    scope = IdempotencyScope("n", "s", "o")
    fingerprint = RequestFingerprint.from_json({"value": 1})
    decision = idempotency.reserve(
        scope, "key", fingerprint, now=now, lease_ttl=timedelta(seconds=1)
    )
    assert isinstance(decision, Execute)
    assert isinstance(
        idempotency.renew(decision.token, now=now, lease_ttl=timedelta(seconds=1)),
        ReservationRenewed,
    )
    assert isinstance(
        idempotency.complete(
            decision.token,
            {"ok": True},
            now=now,
            retention_ttl=timedelta(minutes=1),
        ),
        ReservationCompleted,
    )
    assert isinstance(
        idempotency.mark_uncertain(
            decision.token,
            "unknown",
            now=now,
            retention_ttl=timedelta(minutes=1),
        ),
        ReservationMarkedUncertain,
    )
    assert isinstance(idempotency.abandon(decision.token, now=now), ReservationAbandoned)

    batch_lease = MutationBatchLease(
        "sync",
        "batch",
        "batch-token",
        MutationBatchState.PROCESSING,
        0,
        1,
        (),
        now + timedelta(minutes=1),
    )
    batch_receipt = BatchItemReceipt("item", {"ok": True})
    progressed_lease = replace(
        batch_lease,
        state=MutationBatchState.PARTIALLY_COMMITTED,
        next_index=1,
        receipts=(batch_receipt,),
    )
    batches = DjangoMutationBatchStore.from_callbacks(
        using="events",
        begin=lambda *args, using, **kwargs: (record(using), batch_lease)[1],
        renew=lambda *args, using, **kwargs: (
            record(using),
            MutationBatchLeaseRenewed(batch_lease),
        )[1],
        advance=lambda *args, using, **kwargs: (
            record(using),
            MutationBatchProgressed(progressed_lease),
        )[1],
        complete=lambda *args, using, **kwargs: (
            record(using),
            MutationBatchCompleted(now + timedelta(hours=1)),
        )[1],
        mark_uncertain=lambda *args, using, **kwargs: (
            record(using),
            MutationBatchMarkedUncertain(now + timedelta(days=1)),
        )[1],
    )
    assert (
        batches.begin(
            "sync",
            "batch",
            fingerprint,
            total_items=1,
            now=now,
            lease_ttl=timedelta(minutes=1),
        )
        == batch_lease
    )
    assert isinstance(
        batches.renew(batch_lease, now=now, lease_ttl=timedelta(minutes=1)),
        MutationBatchLeaseRenewed,
    )
    assert isinstance(
        batches.advance(
            batch_lease,
            batch_receipt,
            now=now,
            lease_ttl=timedelta(minutes=1),
        ),
        MutationBatchProgressed,
    )
    assert isinstance(
        batches.complete(progressed_lease, now=now, retention_ttl=timedelta(hours=1)),
        MutationBatchCompleted,
    )
    assert isinstance(
        batches.mark_uncertain(
            batch_lease,
            "unknown",
            now=now,
            retention_ttl=timedelta(days=1),
        ),
        MutationBatchMarkedUncertain,
    )

    replay = DjangoReplayStore.from_callbacks(
        using="events",
        reserve_digest=lambda namespace, digest, *, now, ttl, using: (
            record(using),
            ReplayAccepted(now + ttl),
        )[1],
    )
    assert isinstance(
        replay.reserve("proof", "clear-value", now=now, ttl=timedelta(seconds=1)),
        ReplayAccepted,
    )

    inbox = DjangoInboxStore.from_callbacks(
        using="events",
        begin=lambda scope, message_id, *, token, now, ttl, using: (
            record(using),
            InboxAccepted(token),
        )[1],
        complete=lambda *args, using, **kwargs: (record(using), True)[1],
        abandon=lambda *args, using, **kwargs: (record(using), True)[1],
    )
    inbox_scope = InboxScope("events", "upstream", "projection")
    message_id = OpaqueId("message")
    assert isinstance(
        inbox.begin(
            inbox_scope,
            message_id,
            token="worker",
            now=now,
            ttl=timedelta(seconds=1),
        ),
        InboxAccepted,
    )
    assert inbox.complete(inbox_scope, message_id, token="worker", now=now)
    assert inbox.abandon(inbox_scope, message_id, token="worker")

    envelope = OutboxEnvelope(message_id, "topic", {"value": 1}, now, now)
    claim = OutboxClaim("claim", envelope, now + timedelta(seconds=1))
    outbox = DjangoOutboxStore.from_callbacks(
        using="events",
        add=lambda envelope, *, using: record(using),
        claim=lambda *, now, limit, claim_ttl, using: (record(using), (claim,))[1],
        delivered=lambda claim, *, using: (record(using), True)[1],
        retry=lambda claim, *, available_at, using: (record(using), True)[1],
        failed=lambda claim, *, reason, using: (record(using), True)[1],
    )
    outbox.add(envelope)
    assert outbox.claim(now=now, limit=1, claim_ttl=timedelta(seconds=1)) == (claim,)
    assert outbox.delivered(claim)
    assert outbox.retry(claim, available_at=now)
    assert outbox.failed(claim, reason="terminal")

    checkpoint = DjangoCheckpointStore.from_callbacks(
        using="events",
        load=lambda stream, *, using: (record(using), Checkpoint(b"1"))[1],
        load_for_update=lambda stream, *, using: (record(using), Checkpoint(b"1"))[1],
        advance=lambda stream, *, expected, checkpoint, using: (record(using), True)[1],
    )
    assert checkpoint.load("stream") == Checkpoint(b"1")
    assert checkpoint.load_for_update("stream") == Checkpoint(b"1")
    assert checkpoint.advance("stream", expected=Checkpoint(b"1"), checkpoint=Checkpoint(b"2"))

    receipt_value = MutationReceipt(
        OpaqueId("receipt"), ReceiptState.COMPLETED, now, now, result={"ok": True}
    )
    receipts = DjangoReceiptStore.from_callbacks(
        using="events",
        get=lambda receipt_id, *, using: (record(using), receipt_value)[1],
        add=lambda receipt, *, using: (record(using), True)[1],
        transition=lambda receipt, target, *, using: (record(using), True)[1],
        reconcile_uncertain=lambda receipt, target, *, using: (record(using), True)[1],
    )
    assert receipts.get(receipt_value.receipt_id) == receipt_value
    assert receipts.add(receipt_value)
    assert receipts.transition(receipt_value, receipt_value)
    assert receipts.reconcile_uncertain(receipt_value, receipt_value)

    lease_value = Lease("resource", "worker", 1, now + timedelta(seconds=1))
    leases = DjangoLeaseStore.from_callbacks(
        using="events",
        acquire=lambda resource, *, owner, now, ttl, using: (
            record(using),
            LeaseAcquired(lease_value),
        )[1],
        renew=lambda lease, *, now, ttl, using: (record(using), LeaseRenewed(lease))[1],
        release=lambda lease, *, now, using: (record(using), LeaseReleased())[1],
        authority=lambda resource, *, using: (record(using), 1)[1],
        lock_authority=lambda resource, *, using: (
            record(using),
            LeaseAuthority("worker", 1, now + timedelta(seconds=1)),
        )[1],
    )
    assert isinstance(
        leases.acquire("resource", owner="worker", now=now, ttl=timedelta(seconds=1)),
        LeaseAcquired,
    )
    assert isinstance(leases.renew(lease_value, now=now, ttl=timedelta(seconds=1)), LeaseRenewed)
    assert isinstance(leases.release(lease_value, now=now), LeaseReleased)
    assert leases.authority("resource") == 1
    assert leases.lock_authority("resource") is not None

    generations = DjangoGenerationStore.from_callbacks(
        using="events",
        load_for_update=lambda dataset, partition, *, using: (record(using), 2)[1],
        compare_and_set=lambda dataset, partition, *, expected, generation, using: (
            record(using),
            True,
        )[1],
    )
    assert generations.load_for_update("dataset", "partition") == 2
    assert generations.compare_and_set("dataset", "partition", expected=2, generation=3)
    assert seen and set(seen) == {"events"}


@contextmanager
def _atomic(**kwargs: Any):
    assert kwargs["using"] == "events"
    yield


def test_django_mutation_batch_model_adapter_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from django.db import transaction

    from pytitect.django import stores as django_stores

    monkeypatch.setattr(transaction, "atomic", _atomic)
    monkeypatch.setattr(
        django_stores,
        "_postgresql",
        lambda using: None if using == "events" else pytest.fail("unexpected alias"),
    )

    class Constraint:
        fields = ("namespace", "batch_id")

    class Meta:
        fields: tuple[object, ...] = ()
        unique_together: tuple[tuple[str, ...], ...] = ()
        total_unique_constraints = (Constraint(),)

    class Row:
        def __init__(self, manager: Manager, **values: object) -> None:
            self._manager = manager
            for name, value in values.items():
                setattr(self, name, value)

        def save(self, *, using: str, update_fields: list[str]) -> None:
            assert using == "events"
            assert update_fields

        def delete(self, *, using: str) -> None:
            assert using == "events"
            self._manager.rows.remove(self)

    class Query:
        def __init__(self, manager: Manager, lookup: dict[str, object]) -> None:
            self.manager = manager
            self.lookup = lookup

        def first(self) -> Row | None:
            return next(
                (
                    row
                    for row in self.manager.rows
                    if all(getattr(row, name) == value for name, value in self.lookup.items())
                ),
                None,
            )

    class Manager:
        def __init__(self) -> None:
            self.rows: list[Row] = []

        def using(self, using: str) -> Manager:
            assert using == "events"
            return self

        def select_for_update(self) -> Manager:
            return self

        def filter(self, **lookup: object) -> Query:
            return Query(self, lookup)

        def create(self, **values: object) -> Row:
            row = Row(self, **values)
            self.rows.append(row)
            return row

    class BatchModel:
        _meta = Meta()
        _default_manager = Manager()

    store = DjangoMutationBatchStore.from_model(
        BatchModel,
        using="events",
        encode_result=lambda value: value,
        decode_result=lambda value: value,
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    lease_ttl = timedelta(seconds=5)
    retention_ttl = timedelta(minutes=1)
    fingerprint = RequestFingerprint.from_json({"items": [1]})

    with pytest.raises(ValueError):
        store.begin("", "batch", fingerprint, total_items=1, now=now, lease_ttl=lease_ttl)
    lease = store.begin("sync", "batch", fingerprint, total_items=1, now=now, lease_ttl=lease_ttl)
    assert isinstance(lease, MutationBatchLease)
    assert isinstance(
        store.begin("sync", "batch", fingerprint, total_items=1, now=now, lease_ttl=lease_ttl),
        BatchInProgress,
    )
    assert isinstance(
        store.begin(
            "sync",
            "batch",
            RequestFingerprint.from_json({"items": [2]}),
            total_items=1,
            now=now,
            lease_ttl=lease_ttl,
        ),
        BatchConflict,
    )

    renewed = store.renew(lease, now=now + timedelta(seconds=1), lease_ttl=lease_ttl)
    assert isinstance(renewed, MutationBatchLeaseRenewed)
    assert isinstance(
        store.renew(lease, now=now + timedelta(seconds=1), lease_ttl=lease_ttl),
        StaleMutationBatchLease,
    )
    with pytest.raises(ValueError):
        store.advance(
            renewed.lease,
            BatchItemReceipt("one", {"value": 1}),
            now=now + timedelta(seconds=2),
            lease_ttl=timedelta(0),
        )
    progressed = store.advance(
        renewed.lease,
        BatchItemReceipt("one", {"value": 1}),
        now=now + timedelta(seconds=2),
        lease_ttl=lease_ttl,
    )
    assert isinstance(progressed, MutationBatchProgressed)
    assert isinstance(
        store.complete(
            renewed.lease,
            now=now + timedelta(seconds=3),
            retention_ttl=retention_ttl,
        ),
        StaleMutationBatchLease,
    )
    completed = store.complete(
        progressed.lease,
        now=now + timedelta(seconds=3),
        retention_ttl=retention_ttl,
    )
    assert isinstance(completed, MutationBatchCompleted)
    replay = store.begin(
        "sync",
        "batch",
        fingerprint,
        total_items=1,
        now=now + timedelta(seconds=4),
        lease_ttl=lease_ttl,
    )
    assert isinstance(replay, BatchReplay)
    assert replay.receipts == (BatchItemReceipt("one", {"value": 1}),)

    replacement = store.begin(
        "sync",
        "batch",
        fingerprint,
        total_items=1,
        now=completed.retained_until,
        lease_ttl=lease_ttl,
    )
    assert isinstance(replacement, MutationBatchLease)
    with pytest.raises(ValueError):
        store.mark_uncertain(
            replacement,
            "",
            now=completed.retained_until,
            retention_ttl=retention_ttl,
        )
    marked = store.mark_uncertain(
        replacement,
        "commit outcome unknown",
        now=completed.retained_until,
        retention_ttl=retention_ttl,
    )
    assert isinstance(marked, MutationBatchMarkedUncertain)
    uncertain = store.begin(
        "sync",
        "batch",
        fingerprint,
        total_items=1,
        now=completed.retained_until + timedelta(seconds=1),
        lease_ttl=lease_ttl,
    )
    assert isinstance(uncertain, BatchUncertain)
    assert uncertain.reason == "commit outcome unknown"


def test_django_fenced_and_transactional_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    from django.db import transaction

    monkeypatch.setattr(transaction, "atomic", _atomic)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    lease = Lease("resource", "worker", 1, now + timedelta(seconds=1))
    fenced = DjangoFencedCommit(
        using="events",
        lock_authority=lambda resource, using: LeaseAuthority(
            "worker", 1, now + timedelta(seconds=1)
        ),
        now=lambda: now,
    )
    assert fenced.commit(lease, lambda: "done") == FencedCommitted("done")

    class Idempotency:
        using = "events"
        complete_result: object = ReservationCompleted(now + timedelta(minutes=1))

        def reserve(self, *args: Any, **kwargs: Any) -> Execute:
            return Execute(ReservationToken("token"))

        def complete(self, *args: Any, **kwargs: Any) -> object:
            return self.complete_result

    class Receipts:
        using = "events"

        def add(self, receipt: object) -> bool:
            return True

    class Outbox:
        using = "events"
        add_result = OutboxAdded()

        def add(self, envelope: object) -> object:
            del envelope
            return self.add_result

    idempotency = Idempotency()
    outbox = Outbox()
    operation = DjangoTransactionalOperation(
        using="events",
        domain_using="events",
        idempotency=idempotency,
        receipts=Receipts(),
        outbox=outbox,
        idempotency_policy=IdempotencyPolicy(
            timedelta(minutes=1), timedelta(hours=1), timedelta(days=1)
        ),
    )
    result = {"ok": True}
    receipt = MutationReceipt(OpaqueId("receipt"), ReceiptState.COMPLETED, now, now, result=result)
    committed = operation.execute(
        scope=IdempotencyScope("n", "s", "o"),
        key="key",
        fingerprint=RequestFingerprint.from_json(result),
        mutate=lambda using: result,
        make_receipt=lambda value: receipt,
        make_outbox=lambda value: (OutboxEnvelope(OpaqueId("added"), "topic", value, now, now),),
    )
    assert isinstance(committed, TransactionalOperationCommitted)
    assert committed.outbox_messages == 1
    idempotency.complete_result = StaleReservation()
    rolled_back = operation.execute(
        scope=IdempotencyScope("n", "s", "o"),
        key="other",
        fingerprint=RequestFingerprint.from_json(result),
        mutate=lambda using: result,
        make_receipt=lambda value: receipt,
    )
    assert isinstance(rolled_back, TransactionalOperationRolledBack)
    idempotency.complete_result = ReservationCompleted(now + timedelta(minutes=1))
    outbox.add_result = OutboxDuplicate()
    outbox_rolled_back = operation.execute(
        scope=IdempotencyScope("n", "s", "o"),
        key="outbox-duplicate",
        fingerprint=RequestFingerprint.from_json(result),
        mutate=lambda using: result,
        make_receipt=lambda value: receipt,
        make_outbox=lambda value: (
            OutboxEnvelope(OpaqueId("duplicate"), "topic", value, now, now),
        ),
    )
    assert isinstance(outbox_rolled_back, TransactionalOperationRolledBack)
    with pytest.raises(ValueError):
        DjangoTransactionalOperation(
            using="events",
            domain_using="other",
            idempotency=idempotency,
            receipts=Receipts(),
            outbox=Outbox(),
            idempotency_policy=IdempotencyPolicy(
                timedelta(minutes=1), timedelta(hours=1), timedelta(days=1)
            ),
        )
