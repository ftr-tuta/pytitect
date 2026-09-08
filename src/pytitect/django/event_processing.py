"""Explicit bounded bridges between async runtimes and synchronous Django transactions."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

from pytitect.aio import AsyncDelivery
from pytitect.aio.resilience import SettlementResult
from pytitect.application import HandlingContext
from pytitect.core import OpaqueId
from pytitect.django.relay import DjangoRelayStore
from pytitect.messaging import MessageValue
from pytitect.outbox import OutboxAddResult, OutboxClaim, OutboxEnvelope

ResultT = TypeVar("ResultT")


class AsyncSyncBridge(Protocol):
    async def run(self, operation: Callable[[], ResultT]) -> ResultT: ...


class SyncTransactionRunner(Protocol):
    def run(self, operation: Callable[[], ResultT]) -> ResultT: ...


class DjangoAsyncBridge:
    """Bounded sync-to-async calls; no executor or process is started at construction."""

    def __init__(self, *, concurrency: int = 8, thread_sensitive: bool = True) -> None:
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency <= 0:
            raise ValueError("bridge concurrency must be a positive integer")
        self._semaphore = asyncio.Semaphore(concurrency)
        self._thread_sensitive = thread_sensitive

    async def run(self, operation: Callable[[], ResultT]) -> ResultT:
        from asgiref.sync import sync_to_async

        async with self._semaphore:
            adapted = sync_to_async(operation, thread_sensitive=self._thread_sensitive)
            return await adapted()


@dataclass(frozen=True, slots=True)
class DjangoTransactionRunner:
    using: str

    def __post_init__(self) -> None:
        if not self.using:
            raise ValueError("a Django database alias is required")

    def run(self, operation: Callable[[], ResultT]) -> ResultT:
        from django.db import transaction

        with transaction.atomic(using=self.using):
            return operation()


@dataclass(frozen=True, slots=True)
class DjangoDeliveryCommitted:
    pass


@dataclass(frozen=True, slots=True)
class DjangoDeliveryRetryable:
    delay: timedelta | None = None


@dataclass(frozen=True, slots=True)
class DjangoDeliveryQuarantined:
    quarantine_id: str

    def __post_init__(self) -> None:
        if not self.quarantine_id:
            raise ValueError("quarantined delivery requires a durable quarantine ID")


type DjangoDeliveryResult = (
    DjangoDeliveryCommitted | DjangoDeliveryRetryable | DjangoDeliveryQuarantined
)
type DjangoMessageHandler = Callable[[MessageValue, HandlingContext], DjangoDeliveryResult]


class DjangoTransactionalConsumer:
    """Runs all local effects in one sync transaction and settles afterward."""

    def __init__(
        self,
        handler: DjangoMessageHandler,
        *,
        transaction: SyncTransactionRunner,
        bridge: AsyncSyncBridge,
    ) -> None:
        self._handler = handler
        self._transaction = transaction
        self._bridge = bridge

    async def process(self, delivery: AsyncDelivery) -> DjangoDeliveryResult:
        message = delivery.message
        context = HandlingContext(
            message_id=message.id,
            correlation_id=message.correlationid,
            causation_id=message.causationid,
        )
        result = await self._bridge.run(
            lambda: self._transaction.run(lambda: self._handler(message, context))
        )
        if isinstance(result, DjangoDeliveryCommitted):
            await delivery.ack()
        elif isinstance(result, DjangoDeliveryRetryable):
            await delivery.retry(delay=result.delay)
        else:
            await delivery.terminate()
        return result


class DjangoAsyncOutboxStore[PayloadT]:
    """Adapts a sync Django outbox store for AsyncRelay in short transaction blocks."""

    def __init__(
        self,
        store: DjangoRelayStore[PayloadT],
        *,
        transaction: SyncTransactionRunner,
        bridge: AsyncSyncBridge,
    ) -> None:
        self._store = store
        self._transaction = transaction
        self._bridge = bridge

    async def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        return await self._run(lambda: self._store.add(envelope))

    async def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta, max_bytes: int | None = None
    ) -> Sequence[OutboxClaim[PayloadT]]:
        return await self._run(
            lambda: self._store.claim(
                now=now, limit=limit, claim_ttl=claim_ttl, max_bytes=max_bytes
            )
        )

    async def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> SettlementResult:
        return await self._run(lambda: self._store.delivered(claim, at=at))

    async def retry(
        self, claim: OutboxClaim[PayloadT], *, available_at: datetime, at: datetime
    ) -> SettlementResult:
        return await self._run(lambda: self._store.retry(claim, available_at=available_at, at=at))

    async def defer(
        self, claim: OutboxClaim[PayloadT], *, available_at: datetime, at: datetime
    ) -> SettlementResult:
        return await self._run(lambda: self._store.defer(claim, available_at=available_at, at=at))

    async def uncertain(
        self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime
    ) -> SettlementResult:
        return await self._run(lambda: self._store.uncertain(claim, reason=reason, at=at))

    async def resolve_uncertain(
        self,
        message_id: OpaqueId[object],
        *,
        expected_at: datetime,
        delivered: bool,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        return await self._run(
            lambda: self._store.resolve_uncertain(
                message_id,
                expected_at=expected_at,
                delivered=delivered,
                available_at=available_at,
                at=at,
            )
        )

    async def failed(
        self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime
    ) -> SettlementResult:
        return await self._run(lambda: self._store.failed(claim, reason=reason, at=at))

    async def _run(self, operation: Callable[[], ResultT]) -> ResultT:
        return await self._bridge.run(lambda: self._transaction.run(operation))
