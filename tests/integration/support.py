"""Synthetic test-owned models and isolated PostgreSQL namespaces."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from typing import Any, ClassVar

from sqlalchemy import JSON, Integer, String, UniqueConstraint
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateSchema, DropSchema

from pytitect.messaging import JsonMessageCodec, Message
from pytitect.sqlalchemy.models import (
    CheckpointModelMixin,
    EventLogModelMixin,
    EventModelMixin,
    IdempotencyModelMixin,
    InboxModelMixin,
    JobModelMixin,
    OutboxModelMixin,
    ProcessManagerModelMixin,
    ProcessTimerModelMixin,
    ProjectionModelMixin,
    ProjectionRebuildModelMixin,
    ReceiptModelMixin,
    RejectedDeliveryModelMixin,
    SnapshotModelMixin,
)


class Base(DeclarativeBase):
    type_annotation_map: ClassVar = {dict[str, Any]: JSON}


class Inbox(InboxModelMixin, Base):
    __tablename__ = "inbox"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (UniqueConstraint("namespace", "source", "consumer", "message_id"),)


class Outbox(OutboxModelMixin, Base):
    __tablename__ = "outbox"
    message_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class CheckpointRow(CheckpointModelMixin, Base):
    __tablename__ = "checkpoint"
    stream: Mapped[str] = mapped_column(String(512), primary_key=True)


class Rejected(RejectedDeliveryModelMixin, Base):
    __tablename__ = "rejected"
    quarantine_id: Mapped[str] = mapped_column(String(1024), primary_key=True)


class Idempotency(IdempotencyModelMixin, Base):
    __tablename__ = "idempotency"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (
        UniqueConstraint("namespace", "subject", "operation", "key"),
        UniqueConstraint("token"),
    )


class ReceiptRow(ReceiptModelMixin, Base):
    __tablename__ = "receipt"
    receipt_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class Process(ProcessManagerModelMixin, Base):
    __tablename__ = "process"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (UniqueConstraint("process_name", "instance_id"),)


class Timer(ProcessTimerModelMixin, Base):
    __tablename__ = "timer"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (UniqueConstraint("process_name", "instance_id", "timer_id"),)


class JobRow(JobModelMixin, Base):
    __tablename__ = "job"
    job_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class Log(EventLogModelMixin, Base):
    __tablename__ = "log"
    log_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class Event(EventModelMixin, Base):
    __tablename__ = "event"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (
        UniqueConstraint("log_id", "event_id"),
        UniqueConstraint("log_id", "global_position"),
        UniqueConstraint("log_id", "category", "stream_id", "stream_version"),
    )


class SnapshotRow(SnapshotModelMixin, Base):
    __tablename__ = "snapshot"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (UniqueConstraint("log_id", "category", "stream_id"),)


class Projection(ProjectionModelMixin, Base):
    __tablename__ = "projection"
    id: Mapped[int] = mapped_column(primary_key=True)
    __table_args__ = (UniqueConstraint("projection_name", "partition"),)


class Rebuild(ProjectionRebuildModelMixin, Base):
    __tablename__ = "rebuild"
    run_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class Effect(Base):
    __tablename__ = "effect"
    identity: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[int] = mapped_column(Integer, nullable=False)


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"selected integration requires {name}; provision tool/integration_environment.py"
        )
    return value


def now() -> datetime:
    return datetime.now(UTC)


def message(identity: str = "synthetic-1") -> Message:
    stamp = now()
    stamp = stamp.replace(microsecond=stamp.microsecond // 1000 * 1000)
    return Message(
        id=identity,
        source="urn:example:reliability",
        type="example.changed.v1",
        subject="synthetic",
        time=stamp,
        dataschema="urn:example:synthetic:1",
        data={"value": 1},
    )


CODEC = JsonMessageCodec()


class Database:
    def __init__(self, *, schema: str | None = None) -> None:
        self.schema = schema or "pytitect_" + uuid.uuid4().hex
        self.engine = create_async_engine(
            required_environment("TEST_POSTGRES_DSN").replace(
                "postgresql://", "postgresql+psycopg://"
            ),
            pool_size=8,
            max_overflow=0,
            pool_timeout=5,
            connect_args={
                "connect_timeout": 5,
                "application_name": self.schema,
                "options": "-c statement_timeout=10000 -c lock_timeout=7000",
            },
            execution_options={"schema_translate_map": {None: self.schema}},
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def __aenter__(self) -> Database:
        async with self.engine.begin() as connection:
            await connection.execute(CreateSchema(self.schema))
            await connection.run_sync(Base.metadata.create_all)
        return self

    async def __aexit__(self, *args: object) -> None:
        try:
            async with self.engine.begin() as connection:
                await connection.execute(DropSchema(self.schema, cascade=True))
        finally:
            await self.engine.dispose()
