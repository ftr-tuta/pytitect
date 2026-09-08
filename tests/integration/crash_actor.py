"""Disposable process controlled by a parent pipe barrier, never by a timing guess."""

import asyncio
import sys
from datetime import timedelta

from pytitect.aio import AsyncConsumer, AsyncRelay, InMemoryRejectedDeliveryStore
from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.idempotency import IdempotencyPolicy, IdempotencyScope, RequestFingerprint
from pytitect.messaging import Route, RoutingTable
from pytitect.nats import NatsDelivery, NatsJetStreamPublisher
from pytitect.outbox import OutboxEnvelope
from pytitect.sqlalchemy.idempotency import SQLAlchemyIdempotentRequest
from pytitect.sqlalchemy.relay import SQLAlchemyRelayStore
from pytitect.sqlalchemy.stores import SQLAlchemyOutboxStore
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
)
from tests.integration.test_brokers import connect_nats


async def barrier():
    print("BARRIER", flush=True)
    await asyncio.Event().wait()


async def main():
    mode, schema, subject = sys.argv[1:]
    db = Database(schema=schema)
    if mode == "workflow_claim":
        from pytitect.sqlalchemy.workflows import SQLAlchemyJobStore
        from tests.integration.support import JobRow, now
        from tests.integration.test_workflows import process_store, save_decision

        async with db.sessions() as session, session.begin():
            await SQLAlchemyJobStore(session, JobRow, save_decision=save_decision).claim(
                now=now(), limit=1, claim_ttl=timedelta(seconds=1)
            )
            await process_store(session).claim_timers(
                now=now(), limit=1, claim_ttl=timedelta(seconds=1)
            )
        await barrier()
    if mode == "maintenance_before_commit":
        from pytitect.maintenance import PurgeDeliveredOutboxPlan
        from pytitect.sqlalchemy.maintenance import SQLAlchemyRetention
        from tests.integration.support import now

        async with db.sessions() as session, session.begin():
            await SQLAlchemyRetention(session).purge_delivered(
                Outbox, PurgeDeliveredOutboxPlan(now(), dry_run=False)
            )
            await barrier()
    if mode == "event_before_commit":
        from pytitect.event_sourcing import NewEvent, StreamId
        from tests.integration.support import now
        from tests.integration.test_workflows import event_store

        async with db.sessions() as session, session.begin():
            await event_store(session).append(
                StreamId("test", "one"),
                expected_version=0,
                events=[NewEvent("interrupted", "changed", {}, now())],
            )
            await barrier()
    if mode.startswith("request"):

        async def mutate(session):
            session.add(Effect(identity="request", value=1))
            msg = message("request")
            await SQLAlchemyOutboxStore(session, Outbox, CODEC).add(
                OutboxEnvelope(OpaqueId(msg.id), subject, msg, msg.time, msg.time)
            )
            if mode == "request_before_commit":
                await session.flush()
                await barrier()
            return msg

        request = SQLAlchemyIdempotentRequest(
            db.sessions,
            idempotency_model=Idempotency,
            receipt_model=ReceiptRow,
            serializer=CODEC,
            policy=IdempotencyPolicy(timedelta(seconds=30), timedelta(days=1), timedelta(days=1)),
        )
        await request.execute(
            scope=IdempotencyScope("test", "subject", "request"),
            key="key",
            fingerprint=RequestFingerprint.from_json(None),
            receipt_id=OpaqueId("request"),
            mutate=mutate,
        )
        await barrier()
    client = await connect_nats()
    js = client.jetstream()
    if mode == "publish_before_settlement":

        class Store(SQLAlchemyRelayStore):
            async def delivered(self, claim, *, at):
                await barrier()
                return await super().delivered(claim, at=at)

        await AsyncRelay(
            Store(db.sessions, Outbox, CODEC),
            NatsJetStreamPublisher(js),
            RoutingTable([Route("example.changed.v1", subject)]),
            claim_ttl=timedelta(milliseconds=300),
        ).run_once(limit=1)
    else:
        subscription = await js.pull_subscribe(subject, durable="test")
        raw = (await subscription.fetch(1, timeout=5))[0]

        class Delivery(NatsDelivery):
            async def ack(self):
                if mode == "commit_before_ack":
                    await barrier()
                await super().ack()

        async def save(session, decision):
            session.add(Effect(identity=decision.result, value=1))
            await session.flush()
            if mode == "before_commit":
                await barrier()

        consumer = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=lambda msg, ctx: Decision(result=msg.id),
            unit_of_work=SQLAlchemyUnitOfWorkFactory(
                db.sessions, inbox_model=Inbox, save_decision=save
            ),
            quarantine=InMemoryRejectedDeliveryStore(),
        )
        await consumer.process(Delivery(raw))


if __name__ == "__main__":
    asyncio.run(main())
