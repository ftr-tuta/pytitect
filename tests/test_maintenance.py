from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from pytitect import OpaqueId
from pytitect.django.maintenance import (
    DjangoRetentionMaintenance,
    RetentionIndexModels,
    build_retention_index_check,
)
from pytitect.idempotency import (
    Execute,
    IdempotencyScope,
    InMemoryIdempotencyStore,
    RequestFingerprint,
    Uncertain,
)
from pytitect.inbox import InboxAccepted, InboxScope, InMemoryInboxStore
from pytitect.maintenance import (
    ArchiveFailedOutboxPlan,
    MaintenanceSummary,
    PurgeDeliveredOutboxPlan,
    PurgeIdempotencyPlan,
    PurgeInboxPlan,
    PurgeReceiptsPlan,
    PurgeReplayPlan,
)
from pytitect.outbox import InMemoryOutboxStore, OutboxAdded, OutboxDuplicate, OutboxEnvelope
from pytitect.receipts import InMemoryReceiptStore, MutationReceipt, ReceiptState
from pytitect.security import InMemoryReplayStore, ReplayAccepted
from pytitect.sync import (
    BatchUncertain,
    InMemoryMutationBatchStore,
    MutationBatchCompleted,
    MutationBatchLease,
    MutationBatchMarkedUncertain,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
CUTOFF = NOW + timedelta(minutes=1)


def test_maintenance_plans_require_utc_cutoffs_and_finite_batches() -> None:
    assert PurgeReplayPlan(NOW, batch_size=2, dry_run=True).batch_size == 2
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        PurgeReplayPlan(datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="positive integer"):
        PurgeReplayPlan(NOW, batch_size=True)
    with pytest.raises(ValueError, match="affected <= selected"):
        MaintenanceSummary(1, 2, False)
    with pytest.raises(ValueError, match="dry-run maintenance"):
        MaintenanceSummary(1, 1, True)


def test_idempotency_maintenance_is_bounded_and_uncertain_is_opt_in() -> None:
    store = InMemoryIdempotencyStore[dict[str, int]]()
    scope = IdempotencyScope("api", "subject", "operation")
    fingerprint = RequestFingerprint.from_json({"value": 1})
    for key in ("one", "two"):
        decision = store.reserve(
            scope,
            key,
            fingerprint,
            now=NOW,
            lease_ttl=timedelta(minutes=1),
        )
        assert isinstance(decision, Execute)
        store.complete(
            decision.token,
            {"value": 1},
            now=NOW,
            retention_ttl=timedelta(minutes=1),
        )
    uncertain = store.reserve(
        scope,
        "uncertain",
        fingerprint,
        now=NOW,
        lease_ttl=timedelta(minutes=1),
    )
    assert isinstance(uncertain, Execute)
    store.mark_uncertain(
        uncertain.token,
        "outcome unknown",
        now=NOW,
        retention_ttl=timedelta(minutes=1),
    )

    dry_run = store.purge(PurgeIdempotencyPlan(CUTOFF, batch_size=1, dry_run=True))
    assert dry_run == MaintenanceSummary(1, 0, True)
    assert store.purge(PurgeIdempotencyPlan(CUTOFF, batch_size=1)) == MaintenanceSummary(
        1, 1, False
    )
    assert store.purge(PurgeIdempotencyPlan(CUTOFF, batch_size=10)) == MaintenanceSummary(
        1, 1, False
    )
    assert store.purge(PurgeIdempotencyPlan(CUTOFF)) == MaintenanceSummary(0, 0, False)
    assert isinstance(
        store.reserve(
            scope,
            "uncertain",
            fingerprint,
            now=CUTOFF + timedelta(seconds=1),
            lease_ttl=timedelta(minutes=1),
        ),
        Uncertain,
    )
    assert store.purge(PurgeIdempotencyPlan(CUTOFF, include_uncertain=True)) == MaintenanceSummary(
        1, 1, False
    )


def test_replay_inbox_and_receipt_maintenance_use_terminal_cutoffs() -> None:
    replay = InMemoryReplayStore()
    assert isinstance(
        replay.reserve("proof", "one", now=NOW, ttl=timedelta(minutes=1)), ReplayAccepted
    )
    assert replay.purge(PurgeReplayPlan(CUTOFF, dry_run=True)) == MaintenanceSummary(1, 0, True)
    assert replay.purge(PurgeReplayPlan(CUTOFF)) == MaintenanceSummary(1, 1, False)

    inbox = InMemoryInboxStore()
    scope = InboxScope("events", "source", "consumer")
    completed_id: OpaqueId[object] = OpaqueId("completed")
    expired_id: OpaqueId[object] = OpaqueId("expired")
    assert isinstance(
        inbox.begin(
            scope,
            completed_id,
            token="complete",
            now=NOW,
            ttl=timedelta(minutes=2),
        ),
        InboxAccepted,
    )
    assert inbox.complete(scope, completed_id, token="complete", now=CUTOFF)
    assert isinstance(
        inbox.begin(
            scope,
            expired_id,
            token="expire",
            now=NOW,
            ttl=timedelta(minutes=1),
        ),
        InboxAccepted,
    )
    assert inbox.purge(PurgeInboxPlan(CUTOFF, batch_size=1)) == MaintenanceSummary(1, 1, False)
    assert inbox.purge(PurgeInboxPlan(CUTOFF, batch_size=1)) == MaintenanceSummary(1, 1, False)

    receipts = InMemoryReceiptStore[str]()
    completed = MutationReceipt(
        OpaqueId("completed"), ReceiptState.COMPLETED, NOW, CUTOFF, result="done"
    )
    uncertain = replace(completed, receipt_id=OpaqueId("uncertain"), state=ReceiptState.UNCERTAIN)
    processing = replace(
        completed, receipt_id=OpaqueId("processing"), state=ReceiptState.PROCESSING
    )
    assert receipts.add(completed) and receipts.add(uncertain) and receipts.add(processing)
    assert receipts.purge(PurgeReceiptsPlan(CUTOFF)) == MaintenanceSummary(1, 1, False)
    assert receipts.get(uncertain.receipt_id) == uncertain
    assert receipts.get(processing.receipt_id) == processing
    assert receipts.purge(PurgeReceiptsPlan(CUTOFF, include_uncertain=True)) == MaintenanceSummary(
        1, 1, False
    )


def test_outbox_retains_terminal_rows_until_explicit_purge_or_archive() -> None:
    store = InMemoryOutboxStore[dict[str, int]]()
    delivered = OutboxEnvelope(OpaqueId("delivered"), "events", {"value": 1}, NOW, NOW)
    failed = OutboxEnvelope(OpaqueId("failed"), "events", {"value": 2}, NOW, NOW)
    assert isinstance(store.add(delivered), OutboxAdded)
    assert isinstance(store.add(failed), OutboxAdded)
    first = store.claim(now=NOW, limit=1, claim_ttl=timedelta(minutes=1))[0]
    assert first.envelope == delivered and store.delivered(first, at=CUTOFF)
    second = store.claim(now=NOW, limit=1, claim_ttl=timedelta(minutes=1))[0]
    assert second.envelope == failed and store.failed(second, reason="terminal", at=CUTOFF)
    assert isinstance(store.add(delivered), OutboxDuplicate)
    assert isinstance(store.add(failed), OutboxDuplicate)
    assert store.claim(now=CUTOFF, limit=10, claim_ttl=timedelta(minutes=1)) == []

    assert store.purge_delivered(
        PurgeDeliveredOutboxPlan(CUTOFF, dry_run=True)
    ) == MaintenanceSummary(1, 0, True)
    assert isinstance(store.add(delivered), OutboxDuplicate)
    assert store.purge_delivered(PurgeDeliveredOutboxPlan(CUTOFF)) == MaintenanceSummary(
        1, 1, False
    )
    assert isinstance(store.add(delivered), OutboxAdded)

    archived = []
    assert store.archive_failed(
        ArchiveFailedOutboxPlan(CUTOFF, dry_run=True), archived.extend
    ) == MaintenanceSummary(1, 0, True)
    assert archived == []
    assert store.archive_failed(
        ArchiveFailedOutboxPlan(CUTOFF), archived.extend
    ) == MaintenanceSummary(1, 1, False)
    assert len(archived) == 1
    assert archived[0].envelope == failed
    assert archived[0].reason == "terminal"
    assert archived[0].failed_at == CUTOFF
    assert isinstance(store.add(failed), OutboxAdded)


def test_mutation_batch_uncertainty_requires_explicit_purge_opt_in() -> None:
    store = InMemoryMutationBatchStore[dict[str, int]]()
    fingerprint = RequestFingerprint.from_json({"items": []})
    completed = store.begin(
        "sync",
        "completed",
        fingerprint,
        total_items=0,
        now=NOW,
        lease_ttl=timedelta(minutes=1),
    )
    assert isinstance(completed, MutationBatchLease)
    assert isinstance(
        store.complete(
            completed,
            now=NOW,
            retention_ttl=timedelta(minutes=1),
        ),
        MutationBatchCompleted,
    )
    uncertain = store.begin(
        "sync",
        "uncertain",
        fingerprint,
        total_items=0,
        now=NOW,
        lease_ttl=timedelta(minutes=1),
    )
    assert isinstance(uncertain, MutationBatchLease)
    assert isinstance(
        store.mark_uncertain(
            uncertain,
            "outcome unknown",
            now=NOW,
            retention_ttl=timedelta(minutes=1),
        ),
        MutationBatchMarkedUncertain,
    )
    assert store.purge(PurgeIdempotencyPlan(CUTOFF)) == MaintenanceSummary(1, 1, False)
    assert isinstance(
        store.begin(
            "sync",
            "uncertain",
            fingerprint,
            total_items=0,
            now=CUTOFF + timedelta(seconds=1),
            lease_ttl=timedelta(minutes=1),
        ),
        BatchUncertain,
    )
    assert store.purge(PurgeIdempotencyPlan(CUTOFF, include_uncertain=True)) == MaintenanceSummary(
        1, 1, False
    )


class FakeQuerySet:
    def __init__(self, rows: list[Any], calls: list[tuple[str, object]]) -> None:
        self.rows = rows
        self.calls = calls

    def select_for_update(self, *, skip_locked: bool):
        self.calls.append(("lock", skip_locked))
        return self

    def filter(self, *conditions: object, **filters: object):
        self.calls.append(("filter", (conditions, filters)))
        return self

    def order_by(self, *fields: str):
        self.calls.append(("order", fields))
        return self

    def __getitem__(self, item: slice):
        self.calls.append(("slice", item.stop))
        return self.rows[: item.stop]

    def delete(self) -> tuple[int, dict[str, int]]:
        self.calls.append(("delete", len(self.rows)))
        affected = len(self.rows)
        self.rows.clear()
        return affected, {}


class FakeManager(FakeQuerySet):
    def using(self, alias: str):
        self.calls.append(("using", alias))
        return self


def fake_model(rows: list[Any]):
    calls: list[tuple[str, object]] = []
    meta = SimpleNamespace(label="tests.Record", fields=(), indexes=())
    return SimpleNamespace(_default_manager=FakeManager(rows, calls), _meta=meta), calls


def test_django_maintenance_uses_one_alias_locks_and_deterministic_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from django.db import transaction

    import pytitect.django.maintenance as django_maintenance

    aliases: list[str] = []
    monkeypatch.setattr(django_maintenance, "_postgresql", aliases.append)

    @contextmanager
    def atomic(*, using: str):
        aliases.append(using)
        yield

    monkeypatch.setattr(transaction, "atomic", atomic)
    maintenance = DjangoRetentionMaintenance("events")

    model, calls = fake_model([SimpleNamespace(pk=1)])
    assert maintenance.purge_idempotency(
        model,
        PurgeIdempotencyPlan(NOW, include_uncertain=True),
    ) == MaintenanceSummary(1, 1, False)
    assert (
        "filter",
        ((), {"state__in": ["reserved", "completed", "uncertain"], "expires_at__lte": NOW}),
    ) in calls
    assert ("lock", True) in calls and ("order", ("expires_at", "pk")) in calls

    model, calls = fake_model([SimpleNamespace(pk=7)])
    assert maintenance.purge_mutation_batches(
        model,
        PurgeIdempotencyPlan(NOW, include_uncertain=True),
    ) == MaintenanceSummary(1, 1, False)
    assert ("order", ("retention_expires_at", "pk")) in calls

    model, calls = fake_model([SimpleNamespace(pk=2)])
    assert maintenance.purge_replay(
        model,
        PurgeReplayPlan(NOW, dry_run=True),
    ) == MaintenanceSummary(1, 0, True)
    assert not any(name == "delete" for name, _ in calls)

    model, calls = fake_model([SimpleNamespace(pk=3)])
    assert maintenance.purge_inbox(model, PurgeInboxPlan(NOW)) == MaintenanceSummary(1, 1, False)
    assert ("order", ("completed_at", "expires_at", "pk")) in calls

    model, calls = fake_model([SimpleNamespace(pk=4)])
    assert maintenance.purge_receipts(
        model,
        PurgeReceiptsPlan(NOW, include_uncertain=True),
    ) == MaintenanceSummary(1, 1, False)
    assert (
        "filter",
        (
            (),
            {
                "state__in": ["completed", "rejected", "conflicted", "uncertain"],
                "updated_at__lte": NOW,
            },
        ),
    ) in calls

    model, calls = fake_model([SimpleNamespace(pk=5)])
    assert maintenance.purge_delivered_outbox(
        model,
        PurgeDeliveredOutboxPlan(NOW),
    ) == MaintenanceSummary(1, 1, False)
    assert ("order", ("delivered_at", "pk")) in calls

    failed_row = SimpleNamespace(
        pk=6,
        message_id="failed",
        topic="events",
        payload={"value": 1},
        occurred_at=NOW,
        available_at=NOW,
        attempt=2,
        failure_reason="terminal",
        failed_at=NOW,
    )
    model, calls = fake_model([failed_row])
    archived = []

    def archive(records, *, using):  # type: ignore[no-untyped-def]
        archived.extend(records)
        aliases.append(using)

    assert maintenance.archive_failed_outbox(
        model,
        ArchiveFailedOutboxPlan(NOW),
        decode_payload=lambda value: value,
        archive=archive,
    ) == MaintenanceSummary(1, 1, False)
    assert archived[0].reason == "terminal" and archived[0].envelope.attempt == 2
    assert aliases and all(alias == "events" for alias in aliases)
    assert ("order", ("failed_at", "pk")) in calls
    with pytest.raises(ValueError, match="explicit database alias"):
        DjangoRetentionMaintenance("")


def test_retention_index_check_is_explicit_and_reports_missing_prefixes() -> None:
    indexed = SimpleNamespace(
        _meta=SimpleNamespace(
            label="tests.Indexed",
            fields=(SimpleNamespace(name="identity", db_index=True, unique=False),),
            indexes=(SimpleNamespace(fields=("expires_at", "identity")),),
        )
    )
    missing = SimpleNamespace(
        _meta=SimpleNamespace(
            label="tests.Missing",
            fields=(SimpleNamespace(name="identity", db_index=False, unique=False),),
            indexes=(),
        )
    )
    check = build_retention_index_check(RetentionIndexModels(replay=indexed, outbox=missing))
    warnings = check()
    assert len(warnings) == 2
    assert {warning.id for warning in warnings} == {"pytitect.W001"}
    assert all("consumer-owned model" in warning.hint for warning in warnings)
