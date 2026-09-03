from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from django.test import Client
from reference_app.models import (
    FailedOutboxArchive,
    IdempotencyRecord,
    OutboxRecord,
    ReceiptRecord,
    SyntheticMutation,
)
from reference_app.service import (
    archive_failed,
    dispatch_one_round,
    execute_mutation,
    purge_delivered,
)
from reference_project.openapi import build_openapi

from pytitect.core import Clock
from pytitect.django.transactions import TransactionalOperationCommitted
from pytitect.maintenance import ArchiveFailedOutboxPlan, PurgeDeliveredOutboxPlan
from pytitect.outbox import Delivered, PermanentFailure

pytestmark = pytest.mark.django_db(transaction=True)


class FixedClock(Clock):
    def __init__(self, current: datetime) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


def _post(client: Client, path: str, payload: object):
    return client.post(path, data=json.dumps(payload), content_type="application/json")


def test_legacy_and_versioned_routes_share_one_atomic_service() -> None:
    client = Client()
    payload = {"idempotency_key": "key-1", "value": 7}

    applied = _post(client, "/reference/legacy/mutations/item-1", payload)
    replayed = _post(client, "/reference/sync/1/mutations/item-1", payload)
    conflict = _post(
        client,
        "/reference/sync/1/mutations/item-1",
        {"idempotency_key": "key-1", "value": 8},
    )

    assert applied.status_code == 200
    assert applied.json() == {
        "state": "applied",
        "value": {"mutation_id": "item-1", "value": 7},
    }
    assert replayed.status_code == 200
    assert replayed.json()["state"] == "replayed"
    assert conflict.status_code == 409
    assert conflict.json()["state"] == "conflict"
    assert SyntheticMutation.objects.count() == 1
    assert IdempotencyRecord.objects.count() == 1
    assert ReceiptRecord.objects.count() == 1
    assert OutboxRecord.objects.count() == 1


def test_crash_rolls_back_all_writes_and_retry_commits() -> None:
    with pytest.raises(RuntimeError, match="synthetic crash"):
        execute_mutation(
            mutation_id="crash-1",
            idempotency_key="key-crash",
            value=11,
            crash_after_domain_write=True,
        )

    assert SyntheticMutation.objects.count() == 0
    assert IdempotencyRecord.objects.count() == 0
    assert ReceiptRecord.objects.count() == 0
    assert OutboxRecord.objects.count() == 0

    retried = execute_mutation(mutation_id="crash-1", idempotency_key="key-crash", value=11)
    assert isinstance(retried, TransactionalOperationCommitted)
    assert SyntheticMutation.objects.count() == 1
    assert ReceiptRecord.objects.count() == 1
    assert OutboxRecord.objects.count() == 1


def test_one_round_dispatch_retains_terminals_until_bounded_maintenance() -> None:
    execute_mutation(mutation_id="deliver-1", idempotency_key="key-d", value=1)
    execute_mutation(mutation_id="fail-1", idempotency_key="key-f", value=2)
    now = datetime.now(UTC) + timedelta(seconds=1)

    def handler(envelope):
        if str(envelope.message_id) == "mutation:deliver-1":
            return Delivered()
        return PermanentFailure("synthetic terminal failure")

    summary = dispatch_one_round(handler, clock=FixedClock(now))
    assert (summary.claimed, summary.delivered, summary.failed) == (2, 1, 1)
    assert OutboxRecord.objects.count() == 2

    purged = purge_delivered(PurgeDeliveredOutboxPlan(now, batch_size=1))
    archived = archive_failed(ArchiveFailedOutboxPlan(now, batch_size=1))

    assert (purged.selected, purged.affected) == (1, 1)
    assert (archived.selected, archived.affected) == (1, 1)
    assert OutboxRecord.objects.count() == 0
    archive = FailedOutboxArchive.objects.get()
    assert archive.failure_reason == "synthetic terminal failure"
    assert archive.message_id == "mutation:fail-1"


def test_boundary_rejects_unknown_fields_and_non_post_requests() -> None:
    client = Client()
    invalid = _post(
        client,
        "/reference/sync/1/mutations/item-2",
        {"idempotency_key": "key-2", "value": 1, "unknown": True},
    )
    get = client.get("/reference/sync/1/mutations/item-2")

    assert invalid.status_code == 400
    assert get.status_code == 405
    assert SyntheticMutation.objects.count() == 0


def test_committed_openapi_matches_the_deterministic_builder() -> None:
    path = __file__.replace("tests/test_reference.py", "openapi.json")
    with open(path, encoding="utf-8") as handle:
        committed = json.load(handle)
    assert committed == build_openapi()
