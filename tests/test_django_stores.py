from __future__ import annotations

from contextlib import contextmanager
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
    DjangoOutboxStore,
    DjangoReceiptStore,
    DjangoReplayStore,
    DjangoTransactionalOperation,
    TransactionalOperationCommitted,
    TransactionalOperationRolledBack,
)
from pytitect.idempotency import Execute, IdempotencyScope, RequestFingerprint, ReservationToken
from pytitect.inbox import InboxAccepted
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


def test_callback_stores_always_receive_the_explicit_alias() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    seen: list[str] = []

    def record(using: str) -> None:
        seen.append(using)

    idempotency = DjangoIdempotencyStore.from_callbacks(
        using="events",
        reserve=lambda *args, using, **kwargs: (record(using), Execute(ReservationToken("t")))[1],
        complete=lambda *args, using, **kwargs: (record(using), True)[1],
        mark_uncertain=lambda *args, using, **kwargs: (record(using), True)[1],
    )
    scope = IdempotencyScope("n", "s", "o")
    fingerprint = RequestFingerprint.from_json({"value": 1})
    decision = idempotency.reserve(scope, "key", fingerprint, now=now, ttl=timedelta(seconds=1))
    assert isinstance(decision, Execute)
    assert idempotency.complete(decision.token, {"ok": True}, now=now)
    assert idempotency.mark_uncertain(decision.token, "unknown", now=now)

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
        begin=lambda message_id, *, token, now, ttl, using: (
            record(using),
            InboxAccepted(token),
        )[1],
        complete=lambda *args, using, **kwargs: (record(using), True)[1],
        abandon=lambda *args, using, **kwargs: (record(using), True)[1],
    )
    message_id = OpaqueId("message")
    assert isinstance(
        inbox.begin(message_id, token="worker", now=now, ttl=timedelta(seconds=1)),
        InboxAccepted,
    )
    assert inbox.complete(message_id, token="worker", now=now)
    assert inbox.abandon(message_id, token="worker")

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
        complete_result = True

        def reserve(self, *args: Any, **kwargs: Any) -> Execute:
            return Execute(ReservationToken("token"))

        def complete(self, *args: Any, **kwargs: Any) -> bool:
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
        ttl=timedelta(minutes=1),
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
    idempotency.complete_result = False
    rolled_back = operation.execute(
        scope=IdempotencyScope("n", "s", "o"),
        key="other",
        fingerprint=RequestFingerprint.from_json(result),
        mutate=lambda using: result,
        make_receipt=lambda value: receipt,
    )
    assert isinstance(rolled_back, TransactionalOperationRolledBack)
    idempotency.complete_result = True
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
            ttl=timedelta(minutes=1),
        )
