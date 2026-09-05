"""AsyncSession-per-unit-of-work integration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.aio.uow import AsyncUnitOfWork
from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.inbox import InboxDecision, InboxScope
from pytitect.sqlalchemy.stores import SQLAlchemyInboxStore

type SessionFactory = Callable[[], AsyncSession]
type DecisionSaver = Callable[[AsyncSession, Decision], Awaitable[None]]


class SQLAlchemyUnitOfWorkFactory:
    """Creates one explicit AsyncSession for each unit of work."""

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        inbox_model: type[object],
        save_decision: DecisionSaver,
    ) -> None:
        self._session_factory = session_factory
        self._inbox_model = inbox_model
        self._save_decision = save_decision

    def __call__(self) -> AsyncUnitOfWork:
        return _SQLAlchemyUnitOfWork(
            self._session_factory(),
            inbox_model=self._inbox_model,
            save_decision=self._save_decision,
        )


class _SQLAlchemyUnitOfWork:
    def __init__(
        self,
        session: AsyncSession,
        *,
        inbox_model: type[object],
        save_decision: DecisionSaver,
    ) -> None:
        self._session = session
        self._inbox = SQLAlchemyInboxStore(session, inbox_model)
        self._save_decision = save_decision
        self._finished = False

    async def __aenter__(self) -> _SQLAlchemyUnitOfWork:
        try:
            await self._session.begin()
        except BaseException:
            await self._session.close()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        try:
            if not self._finished:
                await self._session.rollback()
        finally:
            await self._session.close()

    async def reserve_message(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision:
        return await self._inbox.begin(scope, message_id, token=token, now=now, ttl=ttl)

    async def complete_message(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool:
        return await self._inbox.complete(scope, message_id, token=token, now=now)

    async def save_decision(self, decision: Decision) -> None:
        await self._save_decision(self._session, decision)

    async def commit(self) -> None:
        await self._session.commit()
        self._finished = True

    async def rollback(self) -> None:
        if not self._finished:
            await self._session.rollback()
            self._finished = True
