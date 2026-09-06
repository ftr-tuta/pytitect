"""Testing contracts for async stores."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta

from pytitect.aio.idempotency import AsyncIdempotencyStore
from pytitect.aio.ports import AsyncCheckpointStore, AsyncInboxStore, AsyncOutboxStore
from pytitect.aio.receipts import AsyncReceiptStore
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.idempotency import (
    Conflict,
    Execute,
    IdempotencyScope,
    InProgress,
    Replay,
    RequestFingerprint,
    ReservationAbandoned,
    ReservationCompleted,
    ReservationMarkedUncertain,
    ReservationRenewed,
    Uncertain,
)
from pytitect.inbox import InboxAccepted, InboxDuplicate, InboxInProgress, InboxScope
from pytitect.outbox import OutboxAdded, OutboxDuplicate, OutboxEnvelope
from pytitect.receipts import (
    MutationReceipt,
    ReceiptState,
    ReceiptTransitioned,
)


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


class AsyncIdempotencyStoreHarness[T]:
    """Minimal reusable conformance harness for consumer store implementations."""

    def __init__(self, factory: Callable[[], AsyncIdempotencyStore[T]]) -> None:
        self._factory = factory

    async def exercise(self, *, value: T, now: datetime) -> None:
        store = self._factory()
        lease_ttl = timedelta(minutes=1)
        retention_ttl = timedelta(hours=1)
        scope = IdempotencyScope("test", "subject", "operation")
        first = RequestFingerprint.from_json({"value": 1})
        different = RequestFingerprint.from_json({"value": 2})
        decision = await store.reserve(scope, "consumer-key", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(decision, Execute):
            raise AssertionError("first reservation must execute")
        if not isinstance(
            await store.reserve(scope, "consumer-key", first, now=now, lease_ttl=lease_ttl),
            InProgress,
        ):
            raise AssertionError("concurrent reservation must be in progress")
        if not isinstance(
            await store.reserve(scope, "consumer-key", different, now=now, lease_ttl=lease_ttl),
            Conflict,
        ):
            raise AssertionError("different fingerprint must conflict")
        renewed_at = now + timedelta(seconds=1)
        if not isinstance(
            await store.renew(decision.token, now=renewed_at, lease_ttl=lease_ttl),
            ReservationRenewed,
        ):
            raise AssertionError("an authoritative reservation must renew")
        if not isinstance(
            await store.complete(
                decision.token,
                value,
                now=renewed_at,
                retention_ttl=retention_ttl,
            ),
            ReservationCompleted,
        ):
            raise AssertionError("valid reservation must complete")
        replay = await store.reserve(
            scope,
            "consumer-key",
            first,
            now=now + lease_ttl,
            lease_ttl=lease_ttl,
        )
        if not isinstance(replay, Replay) or replay.value != value:
            raise AssertionError("completed reservation must replay")

        uncertain_decision = await store.reserve(
            scope, "uncertain", first, now=now, lease_ttl=lease_ttl
        )
        if not isinstance(uncertain_decision, Execute):
            raise AssertionError("a new uncertainty identity must execute")
        if not isinstance(
            await store.mark_uncertain(
                uncertain_decision.token,
                "outcome unknown",
                now=now,
                retention_ttl=retention_ttl,
            ),
            ReservationMarkedUncertain,
        ):
            raise AssertionError("an authoritative reservation must become uncertain")
        if not isinstance(
            await store.reserve(scope, "uncertain", first, now=now, lease_ttl=lease_ttl),
            Uncertain,
        ):
            raise AssertionError("an uncertain result must remain retained")

        abandoned = await store.reserve(scope, "abandoned", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(abandoned, Execute):
            raise AssertionError("a new abandon identity must execute")
        if not isinstance(await store.abandon(abandoned.token, now=now), ReservationAbandoned):
            raise AssertionError("an authoritative reservation must be abandonable")
        if not isinstance(
            await store.reserve(scope, "abandoned", first, now=now, lease_ttl=lease_ttl), Execute
        ):
            raise AssertionError("an abandoned identity must be reusable")

        expiring = await store.reserve(scope, "expiring", first, now=now, lease_ttl=lease_ttl)
        if not isinstance(expiring, Execute):
            raise AssertionError("a new expiring identity must execute")
        replacement = await store.reserve(
            scope, "expiring", first, now=now + lease_ttl, lease_ttl=lease_ttl
        )
        if not isinstance(replacement, Execute) or replacement.token == expiring.token:
            raise AssertionError("an expired execution reservation must be replaced")


class AsyncReceiptStoreHarness[ResultT]:
    """Reusable behavioral contract for receipt stores."""

    def __init__(self, factory: Callable[[], AsyncReceiptStore[ResultT]]) -> None:
        self._factory = factory

    async def exercise(self, *, value: ResultT, now: datetime) -> None:
        store = self._factory()
        accepted = MutationReceipt[ResultT](OpaqueId("accepted"), ReceiptState.ACCEPTED, now, now)
        if not await store.add(accepted) or await store.add(accepted):
            raise AssertionError("receipt insertion must be unique")
        if await store.get(accepted.receipt_id) != accepted:
            raise AssertionError("an inserted receipt must be loadable")
        transitioned = accepted.transition(ReceiptState.PROCESSING, at=now + timedelta(seconds=1))
        if not isinstance(transitioned, ReceiptTransitioned):
            raise AssertionError("the harness processing transition must be valid")
        if not await store.transition(accepted, transitioned.receipt):
            raise AssertionError("a current receipt transition must succeed")
        if await store.transition(accepted, transitioned.receipt):
            raise AssertionError("a stale receipt transition must fail")

        uncertain = MutationReceipt[ResultT](
            OpaqueId("uncertain"), ReceiptState.UNCERTAIN, now, now
        )
        if not await store.add(uncertain):
            raise AssertionError("an uncertain receipt must be insertable")
        completed = replace(
            uncertain,
            state=ReceiptState.COMPLETED,
            updated_at=now + timedelta(seconds=1),
            result=value,
        )
        if not await store.reconcile_uncertain(uncertain, completed):
            raise AssertionError("a current uncertain receipt must reconcile")
        if await store.reconcile_uncertain(uncertain, completed):
            raise AssertionError("a stale uncertain receipt must not reconcile")
        if await store.get(uncertain.receipt_id) != completed:
            raise AssertionError("a reconciled receipt must be durable")
