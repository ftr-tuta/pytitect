"""Optional event log and snapshot storage with transactionally ordered positions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.event_sourcing import (
    AppendCommitted,
    AppendResult,
    DuplicateEventId,
    EventPage,
    NewEvent,
    Snapshot,
    StoredEvent,
    StreamId,
    WrongExpectedVersion,
)


class SQLAlchemyEventStore:
    """One explicit log/partition. Its position row stays locked until caller commit.

    A sequence alone cannot order commits. Every append and snapshot writer must
    use this log lock; consumers own models, uniqueness constraints and transactions.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        event_model: type[Any],
        log_model: type[Any],
        snapshot_model: type[Any],
        log_id: str,
        max_page_size: int = 1000,
    ) -> None:
        if not log_id or isinstance(max_page_size, bool) or max_page_size <= 0:
            raise ValueError("log identity and positive page size are required")
        self.session, self.model, self.log_model = session, event_model, log_model
        self.snapshot_model, self.log_id, self.max_page_size = snapshot_model, log_id, max_page_size

    async def _lock(self) -> Any:
        await self.session.execute(
            insert(self.log_model)
            .values(log_id=self.log_id, position=0)
            .on_conflict_do_nothing(index_elements=["log_id"])
        )
        return (
            await self.session.execute(
                select(self.log_model)
                .where(self.log_model.log_id == self.log_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one()

    async def watermark(self) -> int:
        value = (
            await self.session.execute(
                select(self.log_model.position).where(self.log_model.log_id == self.log_id)
            )
        ).scalar_one_or_none()
        return 0 if value is None else int(value)

    def _stream(self, model: type[Any], stream: StreamId) -> tuple[Any, ...]:
        return (
            model.log_id == self.log_id,
            model.category == stream.category,
            model.stream_id == stream.stream_id,
        )

    async def append(
        self,
        stream: StreamId,
        *,
        expected_version: int,
        events: Sequence[NewEvent],
    ) -> AppendResult:
        if expected_version < 0 or not events or len(events) > self.max_page_size:
            raise ValueError("append requires a bounded nonempty batch and nonnegative version")
        ids = [event.event_id for event in events]
        if len(ids) != len(set(ids)):
            return DuplicateEventId(next(identity for identity in ids if ids.count(identity) > 1))
        log = await self._lock()
        actual = int(
            (
                await self.session.execute(
                    select(func.coalesce(func.max(self.model.stream_version), 0)).where(
                        *self._stream(self.model, stream)
                    )
                )
            ).scalar_one()
        )
        if actual != expected_version:
            return WrongExpectedVersion(expected_version, actual)
        duplicate = (
            await self.session.execute(
                select(self.model.event_id)
                .where(self.model.log_id == self.log_id, self.model.event_id.in_(ids))
                .limit(1)
            )
        ).scalar_one_or_none()
        if duplicate is not None:
            return DuplicateEventId(duplicate)
        stored = tuple(
            StoredEvent(stream, actual + index, log.position + index, event)
            for index, event in enumerate(events, 1)
        )
        for item in stored:
            self.session.add(
                self.model(
                    log_id=self.log_id,
                    category=stream.category,
                    stream_id=stream.stream_id,
                    stream_version=item.stream_version,
                    global_position=item.global_position,
                    event_id=item.event.event_id,
                    event_type=item.event.event_type,
                    payload=item.event.payload,
                    occurred_at=item.event.occurred_at,
                    event_metadata=dict(item.event.metadata),
                )
            )
        log.position += len(stored)
        await self.session.flush()
        return AppendCommitted(stored[0].stream_version, stored[-1].stream_version, stored)

    def _arguments(self, position: int, limit: int) -> None:
        if position < 0 or isinstance(limit, bool) or not 1 <= limit <= self.max_page_size:
            raise ValueError("page position or bounded limit is invalid")

    def _event(self, row: Any) -> StoredEvent:
        return StoredEvent(
            StreamId(row.category, row.stream_id),
            row.stream_version,
            row.global_position,
            NewEvent(
                row.event_id, row.event_type, row.payload, row.occurred_at, row.event_metadata
            ),
        )

    async def read_stream(
        self, stream: StreamId, *, after_version: int = 0, limit: int
    ) -> EventPage:
        self._arguments(after_version, limit)
        rows = (
            (
                await self.session.execute(
                    select(self.model)
                    .where(
                        *self._stream(self.model, stream), self.model.stream_version > after_version
                    )
                    .order_by(self.model.stream_version)
                    .limit(limit + 1)
                )
            )
            .scalars()
            .all()
        )
        events = tuple(self._event(row) for row in rows[:limit])
        return EventPage(
            events, after_version if not events else events[-1].stream_version, len(rows) <= limit
        )

    async def read_all(self, *, after_position: int = 0, limit: int) -> EventPage:
        self._arguments(after_position, limit)
        rows = (
            (
                await self.session.execute(
                    select(self.model)
                    .where(
                        self.model.log_id == self.log_id,
                        self.model.global_position > after_position,
                    )
                    .order_by(self.model.global_position)
                    .limit(limit + 1)
                )
            )
            .scalars()
            .all()
        )
        events = tuple(self._event(row) for row in rows[:limit])
        return EventPage(
            events, after_position if not events else events[-1].global_position, len(rows) <= limit
        )

    async def load_snapshot(self, stream: StreamId) -> Snapshot | None:
        row = (
            await self.session.execute(
                select(self.snapshot_model).where(*self._stream(self.snapshot_model, stream))
            )
        ).scalar_one_or_none()
        return None if row is None else Snapshot(stream, row.version, row.state, row.created_at)

    async def save_snapshot(self, snapshot: Snapshot, *, expected_version: int | None) -> bool:
        await self._lock()
        current = await self.load_snapshot(snapshot.stream)
        if (None if current is None else current.version) != expected_version:
            return False
        actual = (
            await self.session.execute(
                select(func.coalesce(func.max(self.model.stream_version), 0)).where(
                    *self._stream(self.model, snapshot.stream)
                )
            )
        ).scalar_one()
        if snapshot.version > actual or (
            current is not None and snapshot.version < current.version
        ):
            raise ValueError("snapshot must not exceed its stream or regress")
        statement = insert(self.snapshot_model).values(
            log_id=self.log_id,
            category=snapshot.stream.category,
            stream_id=snapshot.stream.stream_id,
            version=snapshot.version,
            state=snapshot.state,
            created_at=snapshot.created_at,
        )
        await self.session.execute(
            statement.on_conflict_do_update(
                index_elements=["log_id", "category", "stream_id"],
                set_={
                    "version": snapshot.version,
                    "state": snapshot.state,
                    "created_at": snapshot.created_at,
                },
            )
        )
        return True
