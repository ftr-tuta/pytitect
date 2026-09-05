"""Finite terminal-state retention; unresolved work and authority rows are retained."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.maintenance import (
    MaintenanceSummary,
    PurgeDeliveredOutboxPlan,
    PurgeIdempotencyPlan,
    PurgeReceiptsPlan,
)


class SQLAlchemyRetention:
    """Caller-owned transaction; no implicit job/timer/lease authority deletion."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _purge(
        self, model: type[Any], criteria: tuple[Any, ...], *, limit: int, dry_run: bool
    ) -> MaintenanceSummary:
        primary = inspect(model).primary_key
        rows = (
            await self.session.execute(
                select(*primary)
                .where(*criteria)
                .order_by(*primary)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
        if not dry_run:
            for row in rows:
                await self.session.execute(
                    delete(model)
                    .where(*(column == value for column, value in zip(primary, row, strict=True)))
                    .execution_options(synchronize_session=False)
                )
        return MaintenanceSummary(len(rows), 0 if dry_run else len(rows), dry_run)

    async def purge_delivered(
        self, model: type[Any], plan: PurgeDeliveredOutboxPlan
    ) -> MaintenanceSummary:
        return await self._purge(
            model,
            (model.delivered_at <= plan.cutoff, model.uncertain_at.is_(None)),
            limit=plan.batch_size,
            dry_run=plan.dry_run,
        )

    async def purge_idempotency(
        self, model: type[Any], plan: PurgeIdempotencyPlan
    ) -> MaintenanceSummary:
        return await self._purge(
            model,
            (model.state == "completed", model.expires_at <= plan.cutoff),
            limit=plan.batch_size,
            dry_run=plan.dry_run,
        )

    async def purge_receipts(self, model: type[Any], plan: PurgeReceiptsPlan) -> MaintenanceSummary:
        if plan.include_uncertain:
            raise ValueError("uncertain receipts require reconciliation before retention")
        return await self._purge(
            model,
            (
                model.state.in_(["completed", "rejected", "conflicted"]),
                model.updated_at <= plan.cutoff,
            ),
            limit=plan.batch_size,
            dry_run=plan.dry_run,
        )
