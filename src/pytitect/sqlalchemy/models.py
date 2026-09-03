"""Abstract SQLAlchemy column mixins for consumer-owned PostgreSQL models."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, LargeBinary, String, Text
from sqlalchemy.orm import Mapped, declarative_mixin, mapped_column


@declarative_mixin
class InboxModelMixin:
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(512), nullable=False)
    consumer: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    token: Mapped[str] = mapped_column(String(64), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


@declarative_mixin
class OutboxModelMixin:
    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(512), nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attempt: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    claim_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


@declarative_mixin
class CheckpointModelMixin:
    stream: Mapped[str] = mapped_column(String(512), nullable=False)
    checkpoint: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


@declarative_mixin
class RejectedDeliveryModelMixin:
    quarantine_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(512), nullable=False)
    consumer: Mapped[str] = mapped_column(String(255), nullable=False)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    quarantine_metadata: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    payload: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)


@declarative_mixin
class LeaseColumnsMixin:
    claim_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)


@declarative_mixin
class VersionColumnsMixin:
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)


@declarative_mixin
class TerminalStateColumnsMixin:
    terminal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
