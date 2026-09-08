"""Borrowed session factory with independent, short relay transactions."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from pytitect.aio.resilience import SettlementResult
from pytitect.core import OpaqueId
from pytitect.outbox import OutboxAddResult, OutboxClaim, OutboxEnvelope
from pytitect.sqlalchemy.stores import PayloadSerializer, SQLAlchemyOutboxStore
from pytitect.sqlalchemy.uow import SessionFactory


class SQLAlchemyRelayStore[PayloadT]:
    """One new session per operation; no session or transaction spans publication."""

    def __init__(
        self,
        session_factory: SessionFactory,
        model: type[Any],
        serializer: PayloadSerializer[PayloadT],
    ) -> None:
        self._sessions = session_factory
        self._model = model
        self._serializer = serializer

    async def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).add(envelope)

    async def claim(
        self,
        *,
        now: datetime,
        limit: int,
        claim_ttl: timedelta,
        max_bytes: int | None = None,
    ) -> Sequence[OutboxClaim[PayloadT]]:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).claim(
                now=now,
                limit=limit,
                claim_ttl=claim_ttl,
                max_bytes=max_bytes,
            )

    async def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> SettlementResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).delivered(
                claim,
                at=at,
            )

    async def retry(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).retry(
                claim,
                at=at,
                available_at=available_at,
            )

    async def defer(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).defer(
                claim,
                at=at,
                available_at=available_at,
            )

    async def uncertain(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        reason: str,
        at: datetime,
    ) -> SettlementResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).uncertain(
                claim,
                reason=reason,
                at=at,
            )

    async def resolve_uncertain(
        self,
        message_id: OpaqueId[object],
        *,
        expected_at: datetime,
        delivered: bool,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(
                session, self._model, self._serializer
            ).resolve_uncertain(
                message_id,
                expected_at=expected_at,
                delivered=delivered,
                available_at=available_at,
                at=at,
            )

    async def failed(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        reason: str,
        at: datetime,
    ) -> SettlementResult:
        async with self._sessions() as session, session.begin():
            return await SQLAlchemyOutboxStore(session, self._model, self._serializer).failed(
                claim,
                at=at,
                reason=reason,
            )
