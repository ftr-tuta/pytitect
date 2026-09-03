"""Explicit PostgreSQL retention maintenance for consumer-owned models."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, cast

from pytitect.core import JsonValue, OpaqueId
from pytitect.maintenance import (
    ArchiveFailedOutboxPlan,
    MaintenanceSummary,
    PurgeDeliveredOutboxPlan,
    PurgeIdempotencyPlan,
    PurgeInboxPlan,
    PurgeReceiptsPlan,
    PurgeReplayPlan,
)
from pytitect.outbox import FailedOutboxEnvelope, OutboxEnvelope

PayloadT = TypeVar("PayloadT")


class DurableOutboxArchive[PayloadT](Protocol):
    """Archive rows durably on the supplied alias; do not perform external effects."""

    def __call__(
        self,
        records: Sequence[FailedOutboxEnvelope[PayloadT]],
        *,
        using: str,
    ) -> None: ...


class _DecodePayload[PayloadT](Protocol):
    def __call__(self, value: JsonValue) -> PayloadT: ...


class DjangoRetentionMaintenance:
    """Run one bounded PostgreSQL maintenance transaction at a time."""

    def __init__(self, using: str) -> None:
        if not using:
            raise ValueError("an explicit database alias is required")
        self.using = using

    def purge_idempotency(
        self,
        model: Any,
        plan: PurgeIdempotencyPlan,
    ) -> MaintenanceSummary:
        states = ["reserved", "completed"]
        if plan.include_uncertain:
            states.append("uncertain")
        return self._delete(
            model,
            filters={"state__in": states, "expires_at__lte": plan.cutoff},
            ordering=("expires_at", "pk"),
            batch_size=plan.batch_size,
            dry_run=plan.dry_run,
        )

    def purge_replay(self, model: Any, plan: PurgeReplayPlan) -> MaintenanceSummary:
        return self._delete(
            model,
            filters={"expires_at__lte": plan.cutoff},
            ordering=("expires_at", "pk"),
            batch_size=plan.batch_size,
            dry_run=plan.dry_run,
        )

    def purge_mutation_batches(
        self,
        model: Any,
        plan: PurgeIdempotencyPlan,
    ) -> MaintenanceSummary:
        states = ["completed"]
        if plan.include_uncertain:
            states.append("uncertain")
        return self._delete(
            model,
            filters={
                "state__in": states,
                "retention_expires_at__isnull": False,
                "retention_expires_at__lte": plan.cutoff,
            },
            ordering=("retention_expires_at", "pk"),
            batch_size=plan.batch_size,
            dry_run=plan.dry_run,
        )

    def purge_inbox(self, model: Any, plan: PurgeInboxPlan) -> MaintenanceSummary:
        _postgresql(self.using)
        from django.db import transaction
        from django.db.models import Q

        with transaction.atomic(using=self.using):
            rows = list(
                _manager(model, self.using)
                .select_for_update(skip_locked=True)
                .filter(
                    Q(completed_at__isnull=False, completed_at__lte=plan.cutoff)
                    | Q(completed_at__isnull=True, expires_at__lte=plan.cutoff)
                )
                .order_by("completed_at", "expires_at", "pk")[: plan.batch_size]
            )
            return _delete_selected(model, self.using, rows, plan.dry_run)

    def purge_receipts(self, model: Any, plan: PurgeReceiptsPlan) -> MaintenanceSummary:
        states = ["completed", "rejected", "conflicted"]
        if plan.include_uncertain:
            states.append("uncertain")
        return self._delete(
            model,
            filters={"state__in": states, "updated_at__lte": plan.cutoff},
            ordering=("updated_at", "pk"),
            batch_size=plan.batch_size,
            dry_run=plan.dry_run,
        )

    def purge_delivered_outbox(
        self,
        model: Any,
        plan: PurgeDeliveredOutboxPlan,
    ) -> MaintenanceSummary:
        return self._delete(
            model,
            filters={"delivered_at__isnull": False, "delivered_at__lte": plan.cutoff},
            ordering=("delivered_at", "pk"),
            batch_size=plan.batch_size,
            dry_run=plan.dry_run,
        )

    def archive_failed_outbox(
        self,
        model: Any,
        plan: ArchiveFailedOutboxPlan,
        *,
        decode_payload: _DecodePayload[PayloadT],
        archive: DurableOutboxArchive[PayloadT],
    ) -> MaintenanceSummary:
        _postgresql(self.using)
        from django.db import transaction

        with transaction.atomic(using=self.using):
            rows = list(
                _manager(model, self.using)
                .select_for_update(skip_locked=True)
                .filter(
                    failure_reason__isnull=False,
                    failed_at__isnull=False,
                    failed_at__lte=plan.cutoff,
                )
                .order_by("failed_at", "pk")[: plan.batch_size]
            )
            if not plan.dry_run and rows:
                records = tuple(
                    FailedOutboxEnvelope(
                        OutboxEnvelope(
                            OpaqueId(str(row.message_id)),
                            str(row.topic),
                            decode_payload(cast(JsonValue, row.payload)),
                            row.occurred_at,
                            row.available_at,
                            int(row.attempt),
                        ),
                        str(row.failure_reason),
                        row.failed_at,
                    )
                    for row in rows
                )
                archive(records, using=self.using)
            return _delete_selected(model, self.using, rows, plan.dry_run)

    def _delete(
        self,
        model: Any,
        *,
        filters: Mapping[str, object],
        ordering: tuple[str, ...],
        batch_size: int,
        dry_run: bool,
    ) -> MaintenanceSummary:
        _postgresql(self.using)
        from django.db import transaction

        with transaction.atomic(using=self.using):
            rows = list(
                _manager(model, self.using)
                .select_for_update(skip_locked=True)
                .filter(**filters)
                .order_by(*ordering)[:batch_size]
            )
            return _delete_selected(model, self.using, rows, dry_run)


@dataclass(frozen=True, slots=True)
class RetentionIndexModels:
    idempotency: Any | None = None
    mutation_batches: Any | None = None
    replay: Any | None = None
    inbox: Any | None = None
    receipts: Any | None = None
    outbox: Any | None = None


def build_retention_index_check(
    models: RetentionIndexModels,
) -> Callable[..., list[Any]]:
    """Build an opt-in Django system check for the documented retention indexes."""

    requirements = (
        (models.idempotency, (("state", "expires_at"),)),
        (models.mutation_batches, (("state", "retention_expires_at"),)),
        (models.replay, (("expires_at",),)),
        (models.inbox, (("completed_at",), ("expires_at",))),
        (models.receipts, (("state", "updated_at"),)),
        (models.outbox, (("delivered_at",), ("failed_at",))),
    )

    def check(
        app_configs: object = None,
        databases: object = None,
        **kwargs: object,
    ) -> list[Any]:
        del app_configs, databases, kwargs
        from django.core.checks import Warning

        warnings: list[Any] = []
        for model, expected_indexes in requirements:
            if model is None:
                continue
            available = _index_prefixes(model)
            for fields in expected_indexes:
                if fields in available:
                    continue
                joined = ", ".join(fields)
                warnings.append(
                    Warning(
                        f"{model._meta.label} has no retention index beginning with ({joined}).",
                        hint="Add the index in the consumer-owned model and migration.",
                        obj=model,
                        id="pytitect.W001",
                    )
                )
        return warnings

    return check


def _manager(model: Any, using: str) -> Any:
    return model._default_manager.using(using)


def _delete_selected(
    model: Any, using: str, rows: Sequence[Any], dry_run: bool
) -> MaintenanceSummary:
    selected = len(rows)
    if dry_run or not rows:
        return MaintenanceSummary(selected, 0, dry_run)
    _manager(model, using).filter(pk__in=[row.pk for row in rows]).delete()
    return MaintenanceSummary(selected, selected, False)


def _postgresql(using: str) -> None:
    from django.db import connections

    if connections[using].vendor != "postgresql":
        raise ValueError("Django maintenance requires an explicit PostgreSQL database alias")


def _index_prefixes(model: Any) -> set[tuple[str, ...]]:
    prefixes: set[tuple[str, ...]] = set()
    for field in model._meta.fields:
        if getattr(field, "db_index", False) or getattr(field, "unique", False):
            prefixes.add((str(field.name),))
    for index in model._meta.indexes:
        fields = tuple(index.fields)
        for length in range(1, len(fields) + 1):
            prefixes.add(fields[:length])
    return prefixes
