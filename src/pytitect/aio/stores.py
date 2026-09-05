"""Finite process-local async reference stores."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from pytitect.aio.resilience import SettlementResult
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
    """Finite process-local reference; no durability or cross-process authority.

    ``payload_size`` defines retained payload bytes for bounded claims. Message
    envelopes use their encoded size by default; other payloads require a callback.
    """

    def __init__(
        self,
        *,
        capacity: int = 10_000,
        payload_size: Callable[[PayloadT], int] | None = None,
    ) -> None:
        self._store: InMemoryOutboxStore[PayloadT] = InMemoryOutboxStore(capacity=capacity)
        self._lock = asyncio.Lock()
        self._payload_size = payload_size
        self._uncertain: dict[OpaqueId[object], datetime] = {}

    async def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        async with self._lock:
            return self._store.add(envelope)

    async def claim(
        self,
        *,
        now: datetime,
        limit: int,
        claim_ttl: timedelta,
        max_bytes: int | None = None,
    ) -> Sequence[OutboxClaim[PayloadT]]:
        if max_bytes is not None and (isinstance(max_bytes, bool) or max_bytes <= 0):
            raise ValueError("max_bytes must be positive")
        async with self._lock:
            held = {identity: self._store._items.pop(identity) for identity in self._uncertain}
            try:
                claims = self._store.claim(now=now, limit=limit, claim_ttl=claim_ttl)
            finally:
                self._store._items.update(held)
            if max_bytes is None:
                return claims
            retained = 0
            selected = []
            for claim in claims:
                size = self._size(claim.envelope.payload)
                if size < 0:
                    raise ValueError("payload size must not be negative")
                if retained + size <= max_bytes:
                    retained += size
                    selected.append(claim)
                else:
                    item = self._store._valid(claim)
                    assert item is not None
                    item.claim_id = None
                    item.claimed_until = None
            return selected

    def _size(self, payload: PayloadT) -> int:
        from pytitect.messaging import JsonMessageCodec, Message

        if self._payload_size is not None:
            return self._payload_size(payload)
        if isinstance(payload, Message):
            return len(JsonMessageCodec().encode(payload))
        if isinstance(payload, bytes):
            return len(payload)
        raise ValueError("bounded claims require a payload_size callback")

    def _current(self, claim: OutboxClaim[PayloadT], at: datetime) -> bool:
        from pytitect.outbox import _utc

        _utc(at)
        item = self._store._valid(claim)
        return bool(
            item is not None
            and item.claimed_until == claim.claimed_until
            and claim.claimed_until > at
        )

    async def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> SettlementResult:
        async with self._lock:
            if not self._current(claim, at):
                return SettlementResult.STALE
            self._store.delivered(claim, at=at)
            return SettlementResult.APPLIED

    async def retry(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        async with self._lock:
            if not self._current(claim, at):
                return SettlementResult.STALE
            self._store.retry(claim, available_at=available_at)
            return SettlementResult.APPLIED

    async def defer(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        from dataclasses import replace

        from pytitect.outbox import _utc

        _utc(available_at)
        async with self._lock:
            if not self._current(claim, at):
                return SettlementResult.STALE
            item = self._store._valid(claim)
            assert item is not None
            item.envelope = replace(item.envelope, available_at=available_at)
            item.claim_id = None
            item.claimed_until = None
            return SettlementResult.DEFERRED

    async def uncertain(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        reason: str,
        at: datetime,
    ) -> SettlementResult:
        if not reason:
            raise ValueError("uncertainty reason must not be empty")
        async with self._lock:
            if not self._current(claim, at):
                return SettlementResult.STALE
            item = self._store._valid(claim)
            assert item is not None
            item.claim_id = None
            item.claimed_until = None
            self._uncertain[claim.envelope.message_id] = at
            return SettlementResult.APPLIED

    async def resolve_uncertain(
        self,
        message_id: OpaqueId[object],
        *,
        expected_at: datetime,
        delivered: bool,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        from dataclasses import replace

        from pytitect.outbox import _utc

        for stamp in (expected_at, available_at, at):
            _utc(stamp)
        async with self._lock:
            if self._uncertain.get(message_id) != expected_at:
                return SettlementResult.STALE
            item = self._store._items[message_id]
            item.delivered_at = at if delivered else None
            item.envelope = replace(item.envelope, available_at=available_at)
            del self._uncertain[message_id]
            return SettlementResult.APPLIED

    async def failed(
        self,
        claim: OutboxClaim[PayloadT],
        *,
        reason: str,
        at: datetime,
    ) -> SettlementResult:
        async with self._lock:
            if not self._current(claim, at):
                return SettlementResult.STALE
            self._store.failed(claim, reason=reason, at=at)
            return SettlementResult.APPLIED


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
