"""Projection state/checkpoint atomicity and durable fixed-watermark rebuilds."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.core import JsonValue, validate_json
from pytitect.event_sourcing import StoredEvent
from pytitect.projections import (
    ProjectionApplied,
    ProjectionApplyResult,
    ProjectionKey,
    ProjectionState,
    ProjectionVersionMismatch,
    RebuildRun,
    RebuildStatus,
    StaleProjectionCheckpoint,
)
from pytitect.sqlalchemy.events import SQLAlchemyEventStore


class SQLAlchemyProjectionStore:
    def __init__(
        self,
        session: AsyncSession,
        *,
        projection_model: type[Any],
        rebuild_model: type[Any],
        events: SQLAlchemyEventStore,
    ) -> None:
        if events.session is not session:
            raise ValueError("projection and event stores must use the same transaction session")
        self.session, self.model, self.rebuild_model, self.events = (
            session,
            projection_model,
            rebuild_model,
            events,
        )

    def _key(self, key: ProjectionKey) -> tuple[Any, ...]:
        return self.model.projection_name == key.name, self.model.partition == key.partition

    def _state(self, row: Any) -> ProjectionState:
        return ProjectionState(
            ProjectionKey(row.projection_name, row.partition),
            row.version,
            row.checkpoint,
            row.state,
        )

    def _run(self, row: Any) -> RebuildRun:
        return RebuildRun(
            row.run_id,
            ProjectionKey(row.projection_name, row.partition),
            row.projection_version,
            row.through_position,
            row.batch_size,
            row.next_position,
            row.state,
            RebuildStatus(row.status),
        )

    async def load(self, key: ProjectionKey) -> ProjectionState | None:
        row = (
            await self.session.execute(select(self.model).where(*self._key(key)))
        ).scalar_one_or_none()
        return None if row is None else self._state(row)

    async def _locked(self, key: ProjectionKey, version: int, state: JsonValue) -> Any:
        await self.session.execute(
            insert(self.model)
            .values(
                projection_name=key.name,
                partition=key.partition,
                version=version,
                checkpoint=0,
                state=state,
            )
            .on_conflict_do_nothing(index_elements=["projection_name", "partition"])
        )
        return (
            await self.session.execute(
                select(self.model)
                .where(*self._key(key))
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one()

    async def apply(
        self,
        key: ProjectionKey,
        *,
        expected_checkpoint: int,
        projection_version: int,
        state: JsonValue,
        events: Sequence[StoredEvent],
    ) -> ProjectionApplyResult:
        validate_json(state)
        if expected_checkpoint < 0 or projection_version <= 0:
            raise ValueError("projection checkpoint or version is invalid")
        async with self.session.begin_nested() as transaction:
            row = await self._locked(key, projection_version, state)
            if row.checkpoint != expected_checkpoint:
                actual = row.checkpoint
                await transaction.rollback()
                return StaleProjectionCheckpoint(expected_checkpoint, actual)
            if row.version != projection_version:
                actual = row.version
                await transaction.rollback()
                return ProjectionVersionMismatch(projection_version, actual)
            if events:
                page = await self.events.read_all(
                    after_position=expected_checkpoint, limit=len(events)
                )
                if tuple(events) != page.events:
                    raise ValueError("projection must cover the next authoritative event page")
                row.checkpoint = events[-1].global_position
            row.state = state
            await self.session.flush()
            return ProjectionApplied(self._state(row), len(events))

    async def begin_rebuild(self, run: RebuildRun) -> bool:
        if (
            run.next_position != 0
            or run.status is not RebuildStatus.RUNNING
            or run.through_position > await self.events.watermark()
            or run.batch_size > self.events.max_page_size
        ):
            raise ValueError("rebuild must start at zero with a durable watermark and bounded page")
        statement = insert(self.rebuild_model).values(
            run_id=run.run_id,
            projection_name=run.key.name,
            partition=run.key.partition,
            projection_version=run.projection_version,
            through_position=run.through_position,
            batch_size=run.batch_size,
            next_position=0,
            state=run.state,
            status=run.status.value,
        )
        changed = (
            await self.session.execute(
                statement.on_conflict_do_nothing(index_elements=["run_id"]).returning(
                    self.rebuild_model.run_id
                )
            )
        ).scalar_one_or_none()
        return changed is not None

    async def load_rebuild(self, run_id: str) -> RebuildRun | None:
        row = (
            await self.session.execute(
                select(self.rebuild_model).where(self.rebuild_model.run_id == run_id)
            )
        ).scalar_one_or_none()
        return None if row is None else self._run(row)

    async def advance_rebuild(
        self,
        run_id: str,
        *,
        expected_position: int,
        state: JsonValue,
        next_position: int,
        complete: bool,
    ) -> RebuildRun | None:
        validate_json(state)
        async with self.session.begin_nested() as transaction:
            row = (
                await self.session.execute(
                    select(self.rebuild_model)
                    .where(self.rebuild_model.run_id == run_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                row is None
                or row.status != RebuildStatus.RUNNING.value
                or row.next_position != expected_position
            ):
                return None
            run = self._run(row)
            page = await self.events.read_all(
                after_position=expected_position, limit=run.batch_size
            )
            covered = [
                event for event in page.events if event.global_position <= run.through_position
            ]
            required = expected_position if not covered else covered[-1].global_position
            if (
                next_position != required
                or next_position > run.through_position
                or complete != (next_position == run.through_position)
                or (not covered and not complete)
            ):
                raise ValueError(
                    "rebuild progress must cover a bounded page up to its fixed watermark"
                )
            if complete:
                active = await self._locked(run.key, run.projection_version, state)
                if active.checkpoint > next_position or active.version > run.projection_version:
                    await transaction.rollback()
                    return None
                active.checkpoint, active.version, active.state = (
                    next_position,
                    run.projection_version,
                    state,
                )
            row.next_position, row.state = next_position, state
            row.status = RebuildStatus.COMPLETED.value if complete else RebuildStatus.RUNNING.value
            await self.session.flush()
            return replace(
                run, next_position=next_position, state=state, status=RebuildStatus(row.status)
            )
