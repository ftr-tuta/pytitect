from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import cast

from django.utils import timezone

from pytitect.core import Clock, JsonValue, OpaqueId
from pytitect.django.maintenance import DjangoRetentionMaintenance
from pytitect.django.stores import (
    DjangoIdempotencyStore,
    DjangoOutboxStore,
    DjangoReceiptStore,
)
from pytitect.django.transactions import (
    DjangoTransactionalOperation,
    TransactionalOperationResult,
)
from pytitect.idempotency import IdempotencyPolicy, IdempotencyScope, RequestFingerprint
from pytitect.maintenance import ArchiveFailedOutboxPlan, PurgeDeliveredOutboxPlan
from pytitect.outbox import (
    DeliveryResult,
    DispatchSummary,
    FailedOutboxEnvelope,
    OneRoundDispatcher,
    OutboxEnvelope,
)
from pytitect.receipts import MutationReceipt, ReceiptState
from reference_app.models import (
    FailedOutboxArchive,
    IdempotencyRecord,
    OutboxRecord,
    ReceiptRecord,
    SyntheticMutation,
)

DATABASE_ALIAS = "default"
IDEMPOTENCY_POLICY = IdempotencyPolicy(
    execution_lease_ttl=timedelta(minutes=1),
    result_retention_ttl=timedelta(days=7),
    uncertainty_retention_ttl=timedelta(days=30),
)


def _encode_json(value: JsonValue) -> JsonValue:
    return value


def _decode_json(value: JsonValue) -> JsonValue:
    return value


def _idempotency_store() -> DjangoIdempotencyStore[JsonValue]:
    return DjangoIdempotencyStore.from_model(
        IdempotencyRecord,
        using=DATABASE_ALIAS,
        encode_value=_encode_json,
        decode_value=_decode_json,
    )


def _receipt_store() -> DjangoReceiptStore[JsonValue]:
    return DjangoReceiptStore.from_model(
        ReceiptRecord,
        using=DATABASE_ALIAS,
        encode_result=_encode_json,
        decode_result=_decode_json,
    )


def _outbox_store() -> DjangoOutboxStore[JsonValue]:
    return DjangoOutboxStore.from_model(
        OutboxRecord,
        using=DATABASE_ALIAS,
        encode_payload=_encode_json,
        decode_payload=_decode_json,
    )


def execute_mutation(
    *,
    mutation_id: str,
    idempotency_key: str,
    value: int,
    crash_after_domain_write: bool = False,
) -> TransactionalOperationResult[JsonValue]:
    """Apply one synthetic mutation through a consumer-owned transaction boundary."""

    operation = DjangoTransactionalOperation[JsonValue, JsonValue](
        using=DATABASE_ALIAS,
        domain_using=DATABASE_ALIAS,
        idempotency=_idempotency_store(),
        receipts=_receipt_store(),
        outbox=_outbox_store(),
        idempotency_policy=IDEMPOTENCY_POLICY,
    )
    fingerprint = RequestFingerprint.from_json({"mutation_id": mutation_id, "value": value})

    def mutate(using: str) -> JsonValue:
        now = timezone.now()
        SyntheticMutation.objects.using(using).create(
            mutation_id=mutation_id,
            value=value,
            created_at=now,
        )
        if crash_after_domain_write:
            raise RuntimeError("synthetic crash after the domain write")
        return {"mutation_id": mutation_id, "value": value}

    def make_receipt(result: JsonValue) -> MutationReceipt[JsonValue]:
        now = timezone.now()
        return MutationReceipt(
            OpaqueId(f"mutation:{mutation_id}"),
            ReceiptState.COMPLETED,
            now,
            now,
            result=result,
        )

    def make_outbox(result: JsonValue) -> tuple[OutboxEnvelope[JsonValue], ...]:
        now = timezone.now()
        return (
            OutboxEnvelope(
                OpaqueId(f"mutation:{mutation_id}"),
                "reference.mutation.applied",
                result,
                now,
                now,
            ),
        )

    return operation.execute(
        scope=IdempotencyScope("reference", mutation_id, "set-value"),
        key=idempotency_key,
        fingerprint=fingerprint,
        mutate=mutate,
        make_receipt=make_receipt,
        make_outbox=make_outbox,
    )


def dispatch_one_round(
    handler: Callable[[OutboxEnvelope[JsonValue]], DeliveryResult],
    *,
    limit: int = 100,
    clock: Clock | None = None,
) -> DispatchSummary:
    """Claim and handle at most ``limit`` eligible messages."""

    dispatcher = OneRoundDispatcher(
        _outbox_store(),
        handler,
        clock=clock,
    )
    return dispatcher.dispatch(limit=limit)


def purge_delivered(plan: PurgeDeliveredOutboxPlan):
    return DjangoRetentionMaintenance(DATABASE_ALIAS).purge_delivered_outbox(OutboxRecord, plan)


def archive_failed(plan: ArchiveFailedOutboxPlan):
    def persist(records: Sequence[FailedOutboxEnvelope[JsonValue]], *, using: str) -> None:
        rows = [
            FailedOutboxArchive(
                message_id=str(record.envelope.message_id),
                topic=record.envelope.topic,
                payload=cast(dict[str, object], record.envelope.payload),
                occurred_at=record.envelope.occurred_at,
                available_at=record.envelope.available_at,
                attempt=record.envelope.attempt,
                failure_reason=record.reason,
                failed_at=record.failed_at,
            )
            for record in records
        ]
        FailedOutboxArchive.objects.using(using).bulk_create(rows)

    return DjangoRetentionMaintenance(DATABASE_ALIAS).archive_failed_outbox(
        OutboxRecord,
        plan,
        decode_payload=_decode_json,
        archive=persist,
    )
