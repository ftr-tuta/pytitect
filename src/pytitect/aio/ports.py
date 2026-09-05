"""Async reliability ports, intentionally separate from synchronous ports."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.aio.resilience import SettlementResult
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.inbox import InboxDecision, InboxScope
from pytitect.messaging import Message, PublicationResult
from pytitect.outbox import OutboxAddResult, OutboxClaim, OutboxEnvelope


class AsyncInboxStore(Protocol):
    async def begin(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision: ...

    async def complete(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool: ...

    async def abandon(
        self, scope: InboxScope, message_id: OpaqueId[object], *, token: str
    ) -> bool: ...


class AsyncOutboxStore[PayloadT](Protocol):
    async def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult: ...

    async def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta, max_bytes: int | None = None
    ) -> Sequence[OutboxClaim[PayloadT]]: ...

    async def delivered(
        self, claim: OutboxClaim[PayloadT], *, at: datetime
    ) -> SettlementResult: ...

    async def retry(
        self, claim: OutboxClaim[PayloadT], *, available_at: datetime, at: datetime
    ) -> SettlementResult: ...

    async def defer(
        self, claim: OutboxClaim[PayloadT], *, available_at: datetime, at: datetime
    ) -> SettlementResult: ...

    async def uncertain(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        reason: str,
        at: datetime,
    ) -> SettlementResult: ...

    async def resolve_uncertain(
        self,
        message_id: OpaqueId[object],
        *,
        expected_at: datetime,
        delivered: bool,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult: ...

    async def failed(
        self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime
    ) -> SettlementResult: ...


class AsyncCheckpointStore(Protocol):
    async def load(self, stream: str) -> Checkpoint | None: ...

    async def load_for_update(self, stream: str) -> Checkpoint | None: ...

    async def advance(
        self,
        stream: str,
        *,
        expected: Checkpoint | None,
        checkpoint: Checkpoint,
    ) -> bool: ...


class AsyncPublisher(Protocol):
    async def publish(self, *, destination: str, message: Message) -> PublicationResult: ...


class AsyncDelivery(Protocol):
    @property
    def message(self) -> Message: ...

    async def ack(self) -> None: ...

    async def retry(self, *, delay: timedelta | None = None) -> None: ...

    async def terminate(self) -> None: ...


class AsyncDeliverySource(Protocol):
    def deliveries(self, *, batch_size: int) -> AsyncIterator[AsyncDelivery]: ...
