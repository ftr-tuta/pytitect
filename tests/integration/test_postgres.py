"""Durable observations always use an independent session after commit."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import func, select, update

from pytitect.aio import AsyncRelay
from pytitect.aio.resilience import SettlementResult
from pytitect.application import Decision
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.idempotency import (
    Conflict,
    Execute,
    IdempotencyPolicy,
    IdempotencyScope,
    Replay,
    RequestFingerprint,
    ReservationCompleted,
    StaleReservation,
    Uncertain,
)
from pytitect.inbox import InboxAccepted, InboxDuplicate, InboxInProgress, InboxScope
from pytitect.messaging import PublicationConfirmed, Route, RoutingTable
from pytitect.outbox import OutboxEnvelope
from pytitect.sqlalchemy.idempotency import (
    RequestCommitted,
    SQLAlchemyIdempotencyStore,
    SQLAlchemyIdempotentRequest,
    SQLAlchemyReceiptStore,
)
from pytitect.sqlalchemy.relay import SQLAlchemyRelayStore
from pytitect.sqlalchemy.stores import (
    SQLAlchemyCheckpointStore,
    SQLAlchemyInboxStore,
    SQLAlchemyOutboxStore,
)
from tests.integration.support import (
    CODEC,
    CheckpointRow,
    Database,
    Effect,
    Idempotency,
    Inbox,
    Outbox,
    ReceiptRow,
    Rejected,
    message,
    now,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]
TTL = timedelta(seconds=30)


def test_real_checkpoint_first_writers_and_compare_and_set() -> None:
    async def run():
        async with Database() as db:
            ready = asyncio.Barrier(2)

            async def advance(expected, value):
                async with db.sessions() as session, session.begin():
                    await ready.wait()
                    return await SQLAlchemyCheckpointStore(session, CheckpointRow).advance(
                        "stream", expected=expected, checkpoint=value
                    )

            assert sorted(
                await asyncio.gather(
                    advance(None, Checkpoint(b"1")), advance(None, Checkpoint(b"1"))
                )
            ) == [False, True]
            assert sorted(
                await asyncio.gather(
                    advance(Checkpoint(b"1"), Checkpoint(b"2")),
                    advance(Checkpoint(b"1"), Checkpoint(b"3")),
                )
            ) == [False, True]
            async with db.sessions() as session:
                assert await SQLAlchemyCheckpointStore(session, CheckpointRow).load("stream") in (
                    Checkpoint(b"2"),
                    Checkpoint(b"3"),
                )
            assert db.engine.pool.checkedout() == 0

    asyncio.run(run())


def test_inbox_concurrency_takeover_and_expiry_after_lock_wait() -> None:
    async def run():
        async with Database() as db:
            scope, identity = InboxScope("test", "source", "consumer"), OpaqueId("message")
            barrier = asyncio.Barrier(2)

            async def reserve(token):
                async with db.sessions() as session, session.begin():
                    await barrier.wait()
                    return await SQLAlchemyInboxStore(session, Inbox).begin(
                        scope, identity, token=token, now=now(), ttl=TTL
                    )

            results = await asyncio.gather(reserve("a"), reserve("b"))
            assert sum(isinstance(x, InboxAccepted) for x in results) == 1
            assert sum(isinstance(x, InboxInProgress) for x in results) == 1
            winner = next(x.token for x in results if isinstance(x, InboxAccepted))
            async with db.sessions() as session, session.begin():
                await session.execute(update(Inbox).values(expires_at=now() - TTL))
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyInboxStore(session, Inbox)
                assert isinstance(
                    await store.begin(scope, identity, token="replacement", now=now(), ttl=TTL),
                    InboxAccepted,
                )
                assert not await store.complete(scope, identity, token=winner, now=now())
                assert await store.complete(scope, identity, token="replacement", now=now())
            async with db.sessions() as session:
                assert isinstance(
                    await SQLAlchemyInboxStore(session, Inbox).begin(
                        scope, identity, token="third", now=now(), ttl=TTL
                    ),
                    InboxDuplicate,
                )
            # A stale supplied UTC sample cannot authorize settlement after database expiry.
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyInboxStore(session, Inbox)
                stamp = now()
                assert isinstance(
                    await store.begin(
                        scope,
                        OpaqueId("expired"),
                        token="old",
                        now=stamp - TTL,
                        ttl=timedelta(seconds=1),
                    ),
                    InboxAccepted,
                )
                assert not await store.complete(
                    scope, OpaqueId("expired"), token="old", now=stamp - TTL
                )

    asyncio.run(run())


def test_relay_distinct_sessions_no_transaction_during_publish_and_byte_bounds() -> None:
    async def run():
        async with Database() as db:
            store = SQLAlchemyRelayStore(db.sessions, Outbox, CODEC)
            sizes = []
            for index in range(12):
                msg = message(str(index))
                sizes.append(len(CODEC.encode(msg)))
                await store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time))
            seen = set()

            class Publisher:
                async def publish(self, *, destination, message):
                    # Claim transactions are closed before the first transport call.
                    assert db.engine.pool.checkedout() == 0
                    assert message.id not in seen
                    seen.add(message.id)
                    await asyncio.sleep(0)
                    return PublicationConfirmed(message.id)

            routes = RoutingTable([Route("example.changed.v1", "events")])
            relays = [
                AsyncRelay(
                    store,
                    Publisher(),
                    routes,
                    concurrency=1,
                    max_admitted=2,
                    max_retained_bytes=max(sizes) * 2,
                )
                for _ in range(2)
            ]
            # Synchronize claims before publications so this assertion isolates their lifecycle.
            first = await relays[0].run_once(limit=100000)
            second = await relays[1].run_once(limit=100000)
            assert first.claimed == second.claimed == 2
            async with db.sessions() as session:
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(Outbox)
                        .where(Outbox.delivered_at.is_not(None))
                    )
                    == 4
                )
            claims = await asyncio.gather(
                *(store.claim(now=now(), limit=2, claim_ttl=TTL) for _ in range(2))
            )
            assert not (
                {str(x.envelope.message_id) for x in claims[0]}
                & {str(x.envelope.message_id) for x in claims[1]}
            )
            results = await asyncio.gather(
                *(store.delivered(claim, at=now()) for group in claims for claim in group)
            )
            assert results == [SettlementResult.APPLIED] * 4
            assert await store.delivered(claims[0][0], at=now()) is SettlementResult.STALE
            assert db.engine.pool.checkedout() == 0

    asyncio.run(run())


def test_idempotent_requests_atomic_replay_scope_conflict_and_rollback() -> None:
    async def run():
        async with Database() as db:
            request = SQLAlchemyIdempotentRequest(
                db.sessions,
                idempotency_model=Idempotency,
                receipt_model=ReceiptRow,
                serializer=CODEC,
                policy=IdempotencyPolicy(TTL, TTL * 100, TTL * 100),
            )
            scope, fingerprint = (
                IdempotencyScope("test", "subject", "operation"),
                RequestFingerprint.from_json({"v": 1}),
            )
            calls = 0

            async def mutate(session):
                nonlocal calls
                calls += 1
                msg = message("one")
                session.add(Effect(identity="one", value=1))
                await SQLAlchemyOutboxStore(session, Outbox, CODEC).add(
                    OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time)
                )
                return msg

            async def execute(**kwargs):
                return await request.execute(
                    scope=scope,
                    key="key",
                    fingerprint=fingerprint,
                    receipt_id=OpaqueId("receipt"),
                    mutate=mutate,
                    **kwargs,
                )

            results = await asyncio.gather(execute(), execute())
            assert calls == 1
            assert sum(isinstance(x, RequestCommitted) for x in results) == 1
            assert sum(isinstance(x, Replay) for x in results) == 1
            assert isinstance(
                await request.reconcile(scope=scope, key="key", fingerprint=fingerprint), Replay
            )
            assert isinstance(
                await request.execute(
                    scope=scope,
                    key="key",
                    fingerprint=RequestFingerprint.from_json({"v": 2}),
                    receipt_id=OpaqueId("other"),
                    mutate=mutate,
                ),
                Conflict,
            )
            async with db.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Effect)) == 1
                assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
                assert (
                    await SQLAlchemyReceiptStore(session, ReceiptRow, CODEC).get(
                        OpaqueId("receipt")
                    )
                ).result.id == "one"

            async def fail(session):
                session.add(Effect(identity="rollback", value=1))
                await session.flush()
                raise ValueError("proven before commit")

            with pytest.raises(ValueError, match="before commit"):
                await request.execute(
                    scope=scope,
                    key="rollback",
                    fingerprint=fingerprint,
                    receipt_id=OpaqueId("rollback"),
                    mutate=fail,
                )
            async with db.sessions() as session:
                assert await session.get(Effect, "rollback") is None
                assert (
                    await session.execute(select(Idempotency).where(Idempotency.key == "rollback"))
                ).scalar_one_or_none() is None
            assert isinstance(
                await request.reconcile(scope=scope, key="missing", fingerprint=fingerprint),
                Uncertain,
            )
            assert db.engine.pool.checkedout() == 0

    asyncio.run(run())


def test_expired_idempotency_token_and_result_retention() -> None:
    async def run():
        async with Database() as db:
            scope, fingerprint, stamp = (
                IdempotencyScope("test", "subject", "op"),
                RequestFingerprint.from_json(None),
                now(),
            )
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyIdempotencyStore(session, Idempotency, CODEC)
                first = await store.reserve(
                    scope, "key", fingerprint, now=stamp - TTL, lease_ttl=timedelta(seconds=1)
                )
                assert isinstance(first, Execute)
                assert isinstance(
                    await store.complete(
                        first.token, message(), now=stamp - TTL, retention_ttl=TTL
                    ),
                    StaleReservation,
                )
                second = await store.reserve(scope, "key", fingerprint, now=stamp, lease_ttl=TTL)
                assert isinstance(second, Execute) and first.token != second.token
                assert isinstance(
                    await store.complete(first.token, message(), now=stamp, retention_ttl=TTL),
                    StaleReservation,
                )
                assert isinstance(
                    await store.complete(second.token, message(), now=stamp, retention_ttl=TTL),
                    ReservationCompleted,
                )
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyIdempotencyStore(session, Idempotency, CODEC)
                assert isinstance(
                    await store.reserve(
                        scope, "key", fingerprint, now=stamp + TTL * 2, lease_ttl=TTL
                    ),
                    Execute,
                )
                assert isinstance(
                    await store.reserve(
                        replace(scope, subject="other"),
                        "key",
                        fingerprint,
                        now=now(),
                        lease_ttl=TTL,
                    ),
                    Execute,
                )

    asyncio.run(run())


def test_full_idempotency_receipt_conformance_and_ambiguous_commit_reconciliation():
    from sqlalchemy import event as sa_event

    from pytitect.aio import AsyncIdempotencyStoreHarness, AsyncReceiptStoreHarness
    from pytitect.receipts import MutationReceipt, ReceiptState

    async def run():
        async with Database() as db:
            async with db.sessions() as session, session.begin():
                await AsyncIdempotencyStoreHarness(
                    lambda: SQLAlchemyIdempotencyStore(session, Idempotency, CODEC)
                ).exercise(value=message(), now=now())
                await AsyncReceiptStoreHarness(
                    lambda: SQLAlchemyReceiptStore(session, ReceiptRow, CODEC)
                ).exercise(value=message(), now=now())
                store = SQLAlchemyReceiptStore(session, ReceiptRow, CODEC)
                assert await store.get(OpaqueId("absent")) is None
                stamp = now()
                initial = MutationReceipt(OpaqueId("invalid"), ReceiptState.ACCEPTED, stamp, stamp)
                assert not await store.transition(
                    initial, replace(initial, state=ReceiptState.COMPLETED, result=message())
                )
                assert not await store.reconcile_uncertain(initial, initial)
            scope, fingerprint = (
                IdempotencyScope("test", "subject", "ambiguous"),
                RequestFingerprint.from_json(None),
            )
            request = SQLAlchemyIdempotentRequest(
                db.sessions,
                idempotency_model=Idempotency,
                receipt_model=ReceiptRow,
                serializer=CODEC,
                policy=IdempotencyPolicy(TTL, TTL * 100, TTL * 100),
            )

            async def mutate(session):
                session.add(Effect(identity="ambiguous", value=1))

                def lost_response(session):
                    raise ConnectionError("synthetic response loss after PostgreSQL COMMIT")

                sa_event.listen(session.sync_session, "after_commit", lost_response, once=True)
                return message("ambiguous")

            result = await request.execute(
                scope=scope,
                key="key",
                fingerprint=fingerprint,
                receipt_id=OpaqueId("ambiguous"),
                mutate=mutate,
            )
            assert isinstance(result, Replay)
            async with db.sessions() as session:
                assert await session.get(Effect, "ambiguous") is not None

            def unavailable():
                raise ConnectionError("independent reconciliation connection unavailable")

            offline = SQLAlchemyIdempotentRequest(
                unavailable,
                idempotency_model=Idempotency,
                receipt_model=ReceiptRow,
                serializer=CODEC,
                policy=IdempotencyPolicy(TTL, TTL * 100, TTL * 100),
            )
            assert isinstance(
                await offline.reconcile(scope=scope, key="key", fingerprint=fingerprint), Uncertain
            )

    asyncio.run(run())


def test_outbox_expiry_uncertainty_reconciliation_and_interrupted_retention():
    from pytitect.maintenance import PurgeDeliveredOutboxPlan
    from pytitect.sqlalchemy.maintenance import SQLAlchemyRetention

    async def run():
        async with Database() as db:
            store = SQLAlchemyRelayStore(db.sessions, Outbox, CODEC)
            msg = message("uncertain")
            await store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time))
            claim = (await store.claim(now=now(), limit=1, claim_ttl=TTL))[0]
            assert (
                await store.delivered(replace(claim, claim_id="wrong"), at=now())
                is SettlementResult.STALE
            )
            stamp = now()
            assert await store.uncertain(claim, reason="confirmation lost", at=stamp)
            assert not await store.claim(now=stamp + TTL * 2, limit=10, claim_ttl=TTL)
            async with db.sessions() as session, session.begin():
                retention = SQLAlchemyRetention(session)
                assert (
                    await retention.purge_delivered(
                        Outbox, PurgeDeliveredOutboxPlan(stamp + TTL * 2)
                    )
                ).affected == 0
            assert await store.resolve_uncertain(
                OpaqueId(msg.id), expected_at=stamp, delivered=False, available_at=now(), at=now()
            )
            assert not await store.resolve_uncertain(
                OpaqueId(msg.id), expected_at=stamp, delivered=True, available_at=now(), at=now()
            )
            claim = (await store.claim(now=now(), limit=1, claim_ttl=TTL))[0]
            assert await store.retry(claim, at=now(), available_at=now())
            claim = (await store.claim(now=now(), limit=1, claim_ttl=TTL))[0]
            assert (
                await store.defer(claim, at=now(), available_at=now()) is SettlementResult.DEFERRED
            )
            claim = (await store.claim(now=now(), limit=1, claim_ttl=TTL))[0]
            assert await store.delivered(claim, at=now())
            async with db.sessions() as session:
                await session.begin()
                retention = SQLAlchemyRetention(session)
                assert (
                    await retention.purge_delivered(
                        Outbox, PurgeDeliveredOutboxPlan(now(), dry_run=True)
                    )
                ).selected == 1
                assert (
                    await retention.purge_delivered(
                        Outbox, PurgeDeliveredOutboxPlan(now(), dry_run=False)
                    )
                ).affected == 1
                await session.rollback()
            async with db.sessions() as session:
                assert (await session.get(Outbox, msg.id)).delivered_at is not None
            msg = message("expired")
            await store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time))
            claim = (await store.claim(now=now(), limit=1, claim_ttl=TTL))[0]
            async with db.sessions() as session, session.begin():
                await session.execute(
                    update(Outbox)
                    .where(Outbox.message_id == msg.id)
                    .values(claimed_until=now() - TTL)
                )
            assert not await store.retry(claim, at=now(), available_at=now())
            replacement = (await store.claim(now=now(), limit=1, claim_ttl=TTL))[0]
            assert await store.failed(replacement, reason="explicit rejection", at=now())
            assert not await store.claim(now=now() + TTL * 3, limit=10, claim_ttl=TTL)

    asyncio.run(run())


def test_outbox_expiry_during_unchanged_row_lock_wait():
    import time

    from sqlalchemy import text

    async def run():
        async with Database() as db:
            store = SQLAlchemyRelayStore(db.sessions, Outbox, CODEC)
            msg = message("lock-wait")
            await store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time))
            claim = (await store.claim(now=now(), limit=1, claim_ttl=timedelta(seconds=1)))[0]
            stamp = now()
            async with db.sessions() as holder:
                await holder.begin()
                await holder.execute(
                    select(Outbox).where(Outbox.message_id == msg.id).with_for_update()
                )
                task = asyncio.create_task(store.delivered(claim, at=stamp))
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    async with db.sessions() as observer:
                        waiting = await observer.scalar(
                            text(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE wait_event_type='Lock' AND application_name=:name"
                            ),
                            {"name": db.schema},
                        )
                    if waiting:
                        break
                    await asyncio.sleep(0.01)
                else:
                    task.cancel()
                    raise AssertionError("settlement did not reach the database lock barrier")
                await asyncio.sleep(max(0, (claim.claimed_until - now()).total_seconds()) + 0.01)
                await holder.commit()
                assert await asyncio.wait_for(task, 5) is SettlementResult.STALE

    asyncio.run(run())


def test_store_transition_boundaries_and_session_cleanup():
    from sqlalchemy.ext.asyncio import AsyncSession

    from pytitect.aio import AsyncCheckpointStoreHarness
    from pytitect.aio.quarantine import QuarantinePolicy, rejected_delivery
    from pytitect.idempotency import ReservationToken
    from pytitect.maintenance import PurgeIdempotencyPlan, PurgeReceiptsPlan
    from pytitect.receipts import MutationReceipt, ReceiptState
    from pytitect.sqlalchemy.maintenance import SQLAlchemyRetention
    from pytitect.sqlalchemy.stores import SQLAlchemyRejectedDeliveryStore
    from pytitect.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory

    async def run():
        async with Database() as db:
            async with db.sessions() as session, session.begin():
                await AsyncCheckpointStoreHarness(
                    lambda: SQLAlchemyCheckpointStore(session, CheckpointRow)
                ).exercise()
                checkpoints = SQLAlchemyCheckpointStore(session, CheckpointRow)
                assert await checkpoints.load_for_update("stream") == Checkpoint(b"two")
                inbox = SQLAlchemyInboxStore(session, Inbox)
                scope = InboxScope("test", "source", "consumer")
                assert not await inbox.abandon(scope, OpaqueId("absent"), token="one")
                await inbox.begin(scope, OpaqueId("abandon"), token="one", now=now(), ttl=TTL)
                assert not await inbox.abandon(scope, OpaqueId("abandon"), token="wrong")
                assert await inbox.abandon(scope, OpaqueId("abandon"), token="one")
                with pytest.raises(ValueError):
                    SQLAlchemyInboxStore(session, Inbox, capacity=0)
                with pytest.raises(ValueError):
                    await inbox.begin(scope, OpaqueId("bad"), token="", now=now(), ttl=TTL)
                with pytest.raises(ValueError):
                    await checkpoints.load("")
                outbox = SQLAlchemyOutboxStore(session, Outbox, CODEC)
                for args in (
                    {"limit": 0, "claim_ttl": TTL},
                    {"limit": 1, "claim_ttl": timedelta(0)},
                    {"limit": 1, "claim_ttl": TTL, "max_bytes": 0},
                ):
                    with pytest.raises(ValueError):
                        await outbox.claim(now=now(), **args)
                with pytest.raises(ValueError):
                    await outbox.claim(now=now().replace(tzinfo=None), limit=1, claim_ttl=TTL)
                msg = message("bytes")
                await outbox.add(
                    OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time)
                )
                assert not await outbox.claim(now=now(), limit=1, claim_ttl=TTL, max_bytes=1)
                claim = (await outbox.claim(now=now(), limit=1, claim_ttl=TTL))[0]
                for method in (outbox.failed, outbox.uncertain):
                    with pytest.raises(ValueError):
                        await method(claim, reason="", at=now())
                quarantine = SQLAlchemyRejectedDeliveryStore(session, Rejected)
                record = rejected_delivery(
                    quarantine_id="q",
                    message_id="one",
                    source="source",
                    consumer="consumer",
                    failed_at=now(),
                    reason="bad",
                    encoded_payload=b"{}",
                    policy=QuarantinePolicy(),
                )
                assert await quarantine.add(record)
                assert not await quarantine.add(record)
                idem = SQLAlchemyIdempotencyStore(session, Idempotency, CODEC)
                missing = ReservationToken("absent")
                assert isinstance(
                    await idem.renew(missing, now=now(), lease_ttl=TTL), StaleReservation
                )
                assert isinstance(
                    await idem.mark_uncertain(missing, "unknown", now=now(), retention_ttl=TTL),
                    StaleReservation,
                )
                assert isinstance(await idem.abandon(missing, now=now()), StaleReservation)
                with pytest.raises(ValueError):
                    await idem.mark_uncertain(missing, "", now=now(), retention_ttl=TTL)
                with pytest.raises(ValueError):
                    await idem.lookup(
                        IdempotencyScope("n", "s", "o"),
                        "",
                        RequestFingerprint.from_json(None),
                        now=now(),
                    )
                receipts = SQLAlchemyReceiptStore(session, ReceiptRow, CODEC)
                stamp = now()
                receipt = MutationReceipt(
                    OpaqueId("uncertain"), ReceiptState.UNCERTAIN, stamp, stamp
                )
                assert not await receipts.reconcile_uncertain(
                    receipt, replace(receipt, state=ReceiptState.REJECTED)
                )
                assert await receipts.add(receipt)
                retention = SQLAlchemyRetention(session)
                assert (
                    await retention.purge_receipts(ReceiptRow, PurgeReceiptsPlan(now()))
                ).selected == 0
                assert (
                    await retention.purge_idempotency(Idempotency, PurgeIdempotencyPlan(now()))
                ).selected == 0
                with pytest.raises(ValueError):
                    await retention.purge_receipts(
                        ReceiptRow, PurgeReceiptsPlan(now(), include_uncertain=True)
                    )
            closed = []

            class BrokenSession(AsyncSession):
                def begin(self):
                    raise OSError("begin failed")

                async def close(self):
                    closed.append(True)
                    await super().close()

            async def save(session, decision):
                session.add(Effect(identity="uow", value=1))

            factory = SQLAlchemyUnitOfWorkFactory(
                lambda: BrokenSession(db.engine), inbox_model=Inbox, save_decision=save
            )
            with pytest.raises(OSError):
                async with factory():
                    pass
            assert closed == [True]
            factory = SQLAlchemyUnitOfWorkFactory(
                db.sessions, inbox_model=Inbox, save_decision=save
            )
            async with factory() as unit:
                await unit.save_decision(Decision())
                await unit.rollback()
                await unit.rollback()
            async with db.sessions() as session:
                assert await session.get(Effect, "uow") is None

    asyncio.run(run())
