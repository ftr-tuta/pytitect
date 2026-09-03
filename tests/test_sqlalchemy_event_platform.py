from datetime import UTC, datetime

import pytest
from sqlalchemy import LargeBinary, String
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pytitect.sqlalchemy import OutboxModelMixin, outbox_claim_statement


class Base(DeclarativeBase):
    pass


class ConsumerOutbox(OutboxModelMixin, Base):
    __tablename__ = "synthetic_outbox"

    message_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


def test_outbox_claim_is_finite_and_uses_skip_locked() -> None:
    statement = outbox_claim_statement(
        ConsumerOutbox,
        now=datetime(2026, 9, 3, tzinfo=UTC),
        limit=25,
    )
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "LIMIT" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "ORDER BY" in sql


def test_outbox_claim_rejects_unbounded_limits() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        outbox_claim_statement(
            ConsumerOutbox,
            now=datetime(2026, 9, 3, tzinfo=UTC),
            limit=0,
        )
