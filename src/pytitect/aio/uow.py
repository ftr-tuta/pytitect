"""Explicit async unit-of-work ports and a finite process-local test implementation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.inbox import InboxAccepted, InboxDecision, InboxScope, InMemoryInboxStore


class AsyncUnitOfWork(Protocol):
    async def __aenter__(self) -> AsyncUnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None: ...

    async def reserve_message(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision: ...

    async def complete_message(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool: ...

    async def save_decision(self, decision: Decision) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class AsyncUnitOfWorkFactory(Protocol):
    def __call__(self) -> AsyncUnitOfWork: ...


class InMemoryAsyncUnitOfWorkFactory:
    """Finite process-local atomicity harness with no durability or process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("unit-of-work capacity must be a positive integer")
        self._capacity = capacity
        self._inbox = InMemoryInboxStore(capacity=capacity)
        self._decisions: list[Decision] = []
        self._lock = asyncio.Lock()

    @property
    def decisions(self) -> tuple[Decision, ...]:
        return tuple(self._decisions)

    def __call__(self) -> AsyncUnitOfWork:
        return _InMemoryAsyncUnitOfWork(
            inbox=self._inbox,
            decisions=self._decisions,
            lock=self._lock,
            capacity=self._capacity,
        )


class _InMemoryAsyncUnitOfWork:
    def __init__(
        self,
        *,
        inbox: InMemoryInboxStore,
        decisions: list[Decision],
        lock: asyncio.Lock,
        capacity: int,
    ) -> None:
        self._inbox = inbox
        self._decisions = decisions
        self._lock = lock
        self._capacity = capacity
        self._staged: list[Decision] = []
        self._reservations: list[tuple[InboxScope, OpaqueId[object], str]] = []
        self._completions: list[tuple[InboxScope, OpaqueId[object], str, datetime]] = []
        self._entered = False
        self._finished = False

    async def __aenter__(self) -> _InMemoryAsyncUnitOfWork:
        if self._entered:
            raise RuntimeError("unit of work is single-use")
        await self._lock.acquire()
        self._entered = True
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
                await self.rollback()
        finally:
            self._lock.release()

    async def reserve_message(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision:
        self._active()
        decision = self._inbox.begin(scope, message_id, token=token, now=now, ttl=ttl)
        if isinstance(decision, InboxAccepted):
            self._reservations.append((scope, message_id, token))
        return decision

    async def complete_message(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool:
        self._active()
        if (scope, message_id, token) not in self._reservations:
            return False
        self._completions.append((scope, message_id, token, now))
        return True

    async def save_decision(self, decision: Decision) -> None:
        self._active()
        if len(self._decisions) + len(self._staged) >= self._capacity:
            raise OverflowError("unit-of-work decision capacity exceeded")
        self._staged.append(decision)

    async def commit(self) -> None:
        self._active()
        for scope, message_id, token, now in self._completions:
            if not self._inbox.complete(scope, message_id, token=token, now=now):
                await self.rollback()
                raise RuntimeError("inbox completion compare-and-set failed")
        self._decisions.extend(self._staged)
        self._finished = True

    async def rollback(self) -> None:
        if self._finished:
            return
        for scope, message_id, token in self._reservations:
            self._inbox.abandon(scope, message_id, token=token)
        self._staged.clear()
        self._completions.clear()
        self._finished = True

    def _active(self) -> None:
        if not self._entered or self._finished:
            raise RuntimeError("unit of work is not active")
