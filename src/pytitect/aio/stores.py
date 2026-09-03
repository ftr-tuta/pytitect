"""Finite process-local async reference stores."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timedelta

from pytitect.checkpoints import Checkpoint, InMemoryCheckpointStore
from pytitect.core import OpaqueId
from pytitect.inbox import InboxDecision, InboxScope, InMemoryInboxStore
from pytitect.outbox import (
    InMemoryOutboxStore,
    OutboxAddResult,
    OutboxClaim,
    OutboxEnvelope,
)


class InMemoryAsyncInboxStore:
    """Finite process-local reference store with no cross-process coordination or durability."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        self._store = InMemoryInboxStore(capacity=capacity)
        self._lock = asyncio.Lock()

    async def begin(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision:
        async with self._lock:
            return self._store.begin(scope, message_id, token=token, now=now, ttl=ttl)

    async def complete(
        self,
        scope: InboxScope,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
    ) -> bool:
        async with self._lock:
            return self._store.complete(scope, message_id, token=token, now=now)

    async def abandon(self, scope: InboxScope, message_id: OpaqueId[object], *, token: str) -> bool:
        async with self._lock:
            return self._store.abandon(scope, message_id, token=token)


class InMemoryAsyncOutboxStore[PayloadT]:
    """Finite process-local reference store with no cross-process coordination or durability."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        self._store: InMemoryOutboxStore[PayloadT] = InMemoryOutboxStore(capacity=capacity)
        self._lock = asyncio.Lock()

    async def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        async with self._lock:
            return self._store.add(envelope)

    async def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[OutboxClaim[PayloadT]]:
        async with self._lock:
            return self._store.claim(now=now, limit=limit, claim_ttl=claim_ttl)

    async def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> bool:
        async with self._lock:
            return self._store.delivered(claim, at=at)

    async def retry(self, claim: OutboxClaim[PayloadT], *, available_at: datetime) -> bool:
        async with self._lock:
            return self._store.retry(claim, available_at=available_at)

    async def failed(self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime) -> bool:
        async with self._lock:
            return self._store.failed(claim, reason=reason, at=at)


class InMemoryAsyncCheckpointStore:
    """Finite process-local reference store with no cross-process coordination or durability."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        self._store = InMemoryCheckpointStore(capacity=capacity)
        self._lock = asyncio.Lock()

    async def load(self, stream: str) -> Checkpoint | None:
        async with self._lock:
            return self._store.load(stream)

    async def load_for_update(self, stream: str) -> Checkpoint | None:
        async with self._lock:
            return self._store.load_for_update(stream)

    async def advance(
        self,
        stream: str,
        *,
        expected: Checkpoint | None,
        checkpoint: Checkpoint,
    ) -> bool:
        async with self._lock:
            return self._store.advance(stream, expected=expected, checkpoint=checkpoint)
