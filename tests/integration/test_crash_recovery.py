import asyncio
import sys
import uuid
from datetime import timedelta

import pytest
from nats.js.api import ConsumerConfig
from sqlalchemy import func, select

from pytitect.aio import AsyncConsumer, AsyncRelay, InMemoryRejectedDeliveryStore
from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.idempotency import (
    IdempotencyPolicy,
    IdempotencyScope,
    Replay,
    RequestFingerprint,
    Uncertain,
)
from pytitect.messaging import DeliveryAck, PublicationConfirmed, Route, RoutingTable
from pytitect.nats import NatsDelivery, NatsJetStreamPublisher
from pytitect.outbox import OutboxEnvelope
from pytitect.sqlalchemy.idempotency import RequestCommitted, SQLAlchemyIdempotentRequest
from pytitect.sqlalchemy.relay import SQLAlchemyRelayStore
from pytitect.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from tests.integration.support import (
    CODEC,
    Database,
    Effect,
    Idempotency,
    Inbox,
    Outbox,
    ReceiptRow,
    message,
    now,
)
from tests.integration.test_brokers import connect_nats

pytestmark = [pytest.mark.integration, pytest.mark.postgres, pytest.mark.nats]


async def kill_at_barrier(mode, schema, subject):
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.integration.crash_actor",
        mode,
        schema,
        subject,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 15)
        if line != b"BARRIER\n":
            _, error = await child.communicate()
            raise AssertionError(error.decode())
        child.kill()
        assert await asyncio.wait_for(child.wait(), 5) != 0
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


@pytest.mark.parametrize("mode,committed", [("before_commit", False), ("commit_before_ack", True)])
def test_process_death_at_commit_and_ack_recovers_durable_inbox(mode, committed):
    async def run():
        async with Database() as db:
            client = await connect_nats()
            js, identity = client.jetstream(), "pytitect_" + uuid.uuid4().hex
            created = False
            try:
                await js.add_stream(name=identity, subjects=[identity], max_msgs=20)
                created = True
                subscription = await js.pull_subscribe(
                    identity, durable="test", config=ConsumerConfig(ack_wait=0.3, max_deliver=5)
                )
                msg = message("crash")
                assert isinstance(
                    await NatsJetStreamPublisher(js).publish(destination=identity, message=msg),
                    PublicationConfirmed,
                )
                await kill_at_barrier(mode, db.schema, identity)
                async with db.sessions() as session:
                    assert bool(await session.get(Effect, "crash")) is committed
                raw = (await subscription.fetch(1, timeout=5))[0]
                assert raw.metadata.num_delivered >= 2
                calls = 0

                async def save(session, decision):
                    nonlocal calls
                    calls += 1
                    session.add(Effect(identity=decision.result, value=1))

                consumer = AsyncConsumer(
                    consumer="test",
                    namespace="test",
                    handler=lambda msg, ctx: Decision(result=msg.id),
                    unit_of_work=SQLAlchemyUnitOfWorkFactory(
                        db.sessions, inbox_model=Inbox, save_decision=save
                    ),
                    quarantine=InMemoryRejectedDeliveryStore(),
                )
                assert isinstance(await consumer.process(NatsDelivery(raw)), DeliveryAck)
                assert calls == (0 if committed else 1)
                async with db.sessions() as session:
                    assert await session.scalar(select(func.count()).select_from(Effect)) == 1
                    assert (
                        await session.scalar(
                            select(func.count())
                            .select_from(Inbox)
                            .where(Inbox.completed_at.is_not(None))
                        )
                        == 1
                    )
            finally:
                if created:
                    await js.delete_stream(identity)
                await client.close()

    asyncio.run(run())


def test_publish_confirm_then_process_death_leaves_claim_recoverable():
    async def run():
        async with Database() as db:
            client = await connect_nats()
            js, identity = client.jetstream(), "pytitect_" + uuid.uuid4().hex
            created = False
            try:
                # Disable broker deduplication after its tiny window to exercise inbox suppression.
                await js.add_stream(
                    name=identity, subjects=[identity], max_msgs=20, duplicate_window=0.1
                )
                created = True
                subscription = await js.pull_subscribe(
                    identity, durable="test", config=ConsumerConfig(ack_wait=1, max_deliver=5)
                )
                msg, store = (
                    message("publication"),
                    SQLAlchemyRelayStore(db.sessions, Outbox, CODEC),
                )
                await store.add(OutboxEnvelope(OpaqueId(msg.id), identity, msg, msg.time, msg.time))
                await kill_at_barrier("publish_before_settlement", db.schema, identity)
                first = (await subscription.fetch(1, timeout=5))[0]
                async with db.sessions() as session, session.begin():
                    row = await session.get(Outbox, msg.id)
                    assert row.delivered_at is None
                    await asyncio.sleep(
                        max(0.0, (row.claimed_until - now()).total_seconds()) + 0.01
                    )
                summary = await AsyncRelay(
                    store,
                    NatsJetStreamPublisher(js),
                    RoutingTable([Route("example.changed.v1", identity)]),
                ).run_once(limit=1)
                assert summary.delivered == 1
                second = (await subscription.fetch(1, timeout=5))[0]

                async def save(session, decision):
                    session.add(Effect(identity=decision.result, value=1))

                consumer = AsyncConsumer(
                    consumer="test",
                    namespace="test",
                    handler=lambda msg, ctx: Decision(result=msg.id),
                    unit_of_work=SQLAlchemyUnitOfWorkFactory(
                        db.sessions, inbox_model=Inbox, save_decision=save
                    ),
                    quarantine=InMemoryRejectedDeliveryStore(),
                )
                await consumer.process(NatsDelivery(first))
                await consumer.process(NatsDelivery(second))
                async with db.sessions() as session:
                    assert await session.scalar(select(func.count()).select_from(Effect)) == 1
                    assert (await session.get(Outbox, msg.id)).delivered_at is not None
            finally:
                if created:
                    await js.delete_stream(identity)
                await client.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode,committed", [("request_before_commit", False), ("request_after_commit", True)]
)
def test_request_process_death_preserves_identity_across_response_loss(mode, committed):
    async def run():
        async with Database() as db:
            await kill_at_barrier(mode, db.schema, "events")
            request = SQLAlchemyIdempotentRequest(
                db.sessions,
                idempotency_model=Idempotency,
                receipt_model=ReceiptRow,
                serializer=CODEC,
                policy=IdempotencyPolicy(
                    timedelta(seconds=30), timedelta(days=1), timedelta(days=1)
                ),
            )
            scope, fingerprint = (
                IdempotencyScope("test", "subject", "request"),
                RequestFingerprint.from_json(None),
            )
            result = await request.reconcile(scope=scope, key="key", fingerprint=fingerprint)
            assert isinstance(result, Replay if committed else Uncertain)
            calls = 0

            async def mutate(session):
                nonlocal calls
                calls += 1
                session.add(Effect(identity="request", value=1))
                return message("request")

            result = await request.execute(
                scope=scope,
                key="key",
                fingerprint=fingerprint,
                receipt_id=OpaqueId("request"),
                mutate=mutate,
            )
            assert isinstance(result, Replay if committed else RequestCommitted)
            assert calls == (0 if committed else 1)
            async with db.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Effect)) == 1
                assert await session.scalar(select(func.count()).select_from(ReceiptRow)) == 1

    asyncio.run(run())


def test_workflow_claim_process_death_preserves_monotonic_fences():
    from dataclasses import replace

    from pytitect.jobs import Job, StaleJobClaim
    from pytitect.processes import (
        ProcessDecision,
        ProcessEffect,
        ProcessEffectKind,
        ProcessKey,
        TimerSchedule,
    )
    from pytitect.sqlalchemy.workflows import SQLAlchemyJobStore
    from tests.integration.support import JobRow, Timer
    from tests.integration.test_workflows import process_store, save_decision

    async def run():
        async with Database() as db:
            async with db.sessions() as session, session.begin():
                await SQLAlchemyJobStore(session, JobRow, save_decision=save_decision).schedule(
                    Job("job", "test", {}, now())
                )
                await process_store(session).apply(
                    ProcessKey("test", "one"),
                    expected_version=0,
                    decision=ProcessDecision(
                        {},
                        schedule=(
                            TimerSchedule(
                                "timer",
                                now(),
                                ProcessEffect(
                                    "timer", ProcessEffectKind.TASK, "task", {"value": 1}
                                ),
                            ),
                        ),
                    ),
                    at=now(),
                )
            await kill_at_barrier("workflow_claim", db.schema, "unused")
            async with db.sessions() as session, session.begin():
                old_job = await session.get(JobRow, "job")
                old_timer = (await session.execute(select(Timer))).scalar_one()
                job_authority = (old_job.claim_id, old_job.claimed_until, old_job.fencing_token)
                timer_authority = (
                    old_timer.claim_id,
                    old_timer.claimed_until,
                    old_timer.fencing_token,
                )
                assert not await SQLAlchemyJobStore(
                    session, JobRow, save_decision=save_decision
                ).claim(now=now(), limit=1, claim_ttl=timedelta(seconds=10))
                until = max(old_job.claimed_until, old_timer.claimed_until)
            await asyncio.sleep(max(0, (until - now()).total_seconds()) + 0.01)
            async with db.sessions() as session, session.begin():
                jobs = SQLAlchemyJobStore(session, JobRow, save_decision=save_decision)
                current = (await jobs.claim(now=now(), limit=1, claim_ttl=timedelta(seconds=10)))[0]
                assert current.fencing_token > job_authority[2]
                old = replace(
                    current,
                    claim_id=job_authority[0],
                    claimed_until=job_authority[1],
                    fencing_token=job_authority[2],
                )
                assert isinstance(
                    await jobs.succeed(
                        old, decision=Decision(result={"identity": "stale"}), at=now()
                    ),
                    StaleJobClaim,
                )
                await jobs.succeed(
                    current, decision=Decision(result={"identity": "current"}), at=now()
                )
                processes = process_store(session)
                current = (
                    await processes.claim_timers(
                        now=now(), limit=1, claim_ttl=timedelta(seconds=10)
                    )
                )[0]
                assert current.timer.fencing_token > timer_authority[2]
                old = replace(
                    current,
                    claim_id=timer_authority[0],
                    claimed_until=timer_authority[1],
                    timer=replace(current.timer, fencing_token=timer_authority[2]),
                )
                assert not await processes.complete_timer(old, at=now())
                assert await processes.complete_timer(current, at=now())
            async with db.sessions() as session:
                assert await session.get(Effect, "stale") is None
                assert await session.scalar(select(func.count()).select_from(Effect)) == 2

    asyncio.run(run())


def test_interrupted_maintenance_and_event_append_rollback_after_process_death():
    from pytitect.event_sourcing import NewEvent, StreamId
    from pytitect.maintenance import PurgeDeliveredOutboxPlan
    from pytitect.sqlalchemy.maintenance import SQLAlchemyRetention
    from tests.integration.test_workflows import event_store

    async def run():
        async with Database() as db:
            store = SQLAlchemyRelayStore(db.sessions, Outbox, CODEC)
            msg = message("retention")
            envelope = OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time)
            await store.add(envelope)
            old = (await store.claim(now=now(), limit=1, claim_ttl=timedelta(seconds=30)))[0]
            assert await store.delivered(old, at=now())
            await kill_at_barrier("maintenance_before_commit", db.schema, "unused")
            async with db.sessions() as session, session.begin():
                assert (await session.get(Outbox, msg.id)).delivered_at is not None
                assert (
                    await SQLAlchemyRetention(session).purge_delivered(
                        Outbox, PurgeDeliveredOutboxPlan(now(), dry_run=False)
                    )
                ).affected == 1
            await store.add(envelope)
            current = (await store.claim(now=now(), limit=1, claim_ttl=timedelta(seconds=30)))[0]
            assert current.claim_id != old.claim_id
            assert not await store.delivered(old, at=now())
            assert await store.delivered(current, at=now())
            await kill_at_barrier("event_before_commit", db.schema, "unused")
            async with db.sessions() as session, session.begin():
                events = event_store(session)
                assert await events.watermark() == 0
                result = await events.append(
                    StreamId("test", "one"),
                    expected_version=0,
                    events=[NewEvent("accepted", "changed", {}, now())],
                )
                assert result.events[0].global_position == 1

    asyncio.run(run())
