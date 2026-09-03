"""Testing contracts for async stores."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from pytitect.aio.ports import AsyncCheckpointStore, AsyncInboxStore, AsyncOutboxStore
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.inbox import InboxAccepted, InboxDuplicate, InboxInProgress, InboxScope
from pytitect.outbox import OutboxAdded, OutboxDuplicate, OutboxEnvelope


class AsyncInboxStoreHarness:
    def __init__(self, factory: Callable[[], AsyncInboxStore]) -> None:
        self._factory = factory

    async def exercise(self, *, now: datetime) -> None:
        store = self._factory()
        scope = InboxScope("test", "source", "consumer")
        message_id: OpaqueId[object] = OpaqueId("message")
        ttl = timedelta(minutes=1)
        if not isinstance(
            await store.begin(scope, message_id, token="one", now=now, ttl=ttl), InboxAccepted
        ):
            raise AssertionError("a new async inbox identity must be accepted")
        if not isinstance(
            await store.begin(scope, message_id, token="two", now=now, ttl=ttl),
            InboxInProgress,
        ):
            raise AssertionError("an active async inbox identity must be in progress")
        if not await store.complete(scope, message_id, token="one", now=now):
            raise AssertionError("the current async inbox token must complete")
        if not isinstance(
            await store.begin(scope, message_id, token="three", now=now, ttl=ttl),
            InboxDuplicate,
        ):
            raise AssertionError("a completed async inbox identity must be duplicate")


class AsyncOutboxStoreHarness[PayloadT]:
    def __init__(self, factory: Callable[[], AsyncOutboxStore[PayloadT]]) -> None:
        self._factory = factory

    async def exercise(self, *, payload: PayloadT, now: datetime) -> None:
        store = self._factory()
        envelope = OutboxEnvelope(OpaqueId("message"), "events", payload, now, now)
        if not isinstance(await store.add(envelope), OutboxAdded):
            raise AssertionError("a new async outbox identity must be added")
        if not isinstance(await store.add(envelope), OutboxDuplicate):
            raise AssertionError("an async outbox duplicate must be rejected")
        claims = await store.claim(now=now, limit=1, claim_ttl=timedelta(minutes=1))
        if len(claims) != 1:
            raise AssertionError("an eligible async outbox row must be claimed")
        if not await store.delivered(claims[0], at=now):
            raise AssertionError("the current async outbox claim must be deliverable")
        if await store.delivered(claims[0], at=now):
            raise AssertionError("a completed async outbox claim must become stale")


class AsyncCheckpointStoreHarness:
    def __init__(self, factory: Callable[[], AsyncCheckpointStore]) -> None:
        self._factory = factory

    async def exercise(self) -> None:
        store = self._factory()
        first = Checkpoint(b"one")
        second = Checkpoint(b"two")
        if await store.load("stream") is not None:
            raise AssertionError("a new async checkpoint must be absent")
        if not await store.advance("stream", expected=None, checkpoint=first):
            raise AssertionError("an absent async checkpoint must accept a None CAS")
        if await store.advance("stream", expected=None, checkpoint=second):
            raise AssertionError("a stale async checkpoint CAS must fail")
        if not await store.advance("stream", expected=first, checkpoint=second):
            raise AssertionError("the current async checkpoint CAS must succeed")
