"""Exact tokens survive real byte stores, JetStream and explicit consumer admission."""

import asyncio
import uuid
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from nats.js.api import ConsumerConfig
from sqlalchemy import func, select

from pytitect.aio import AsyncConsumer, AsyncRelay, InMemoryRejectedDeliveryStore
from pytitect.application import Decision
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.messaging import DeliveryAck, ExactJsonMessageCodec, Route, RoutingTable
from pytitect.nats import NatsDelivery, NatsJetStreamPublisher
from pytitect.outbox import OutboxEnvelope
from pytitect.sqlalchemy import SQLAlchemyUnitOfWorkFactory
from pytitect.sqlalchemy.relay import SQLAlchemyRelayStore
from pytitect.sqlalchemy.stores import SQLAlchemyCheckpointStore, SQLAlchemyOutboxStore
from pytitect.sync import (
    EXACT_JSON_INTEGRITY,
    ExactJsonSha256Integrity,
    SyncIntegritySelection,
    decode_sync_raw,
)
from pytitect.wire import WireIntegrityError, decode_wire
from tests.integration.support import CheckpointRow, Database, Effect, Inbox, Outbox, message, now
from tests.integration.test_brokers import connect_nats


def exact_message(token):
    from pytitect.messaging import JsonMessageCodec

    raw = JsonMessageCodec().encode(replace(message(), data=None))
    return ExactJsonMessageCodec().decode(
        raw.replace(b'"data":null', b'"data":' + token.encode()).replace(
            b"titect-message/1", b"titect-message/2"
        )
    )


def page():
    return decode_wire(
        (
            Path(__file__).resolve().parents[2]
            / "interop/titect-sync/1/fixtures/positive/verified-delta.json"
        ).read_bytes()
    )


@pytest.mark.integration
@pytest.mark.postgres
def test_postgres_exact_persistence_and_integrity_failure_leave_checkpoint_unchanged():
    async def run():
        codec = ExactJsonMessageCodec()
        value = exact_message("[1.00000000000000001,1E+000,-0,1e9999," + "9" * 4301 + "]")
        async with Database() as db:
            async with db.sessions() as session, session.begin():
                await SQLAlchemyOutboxStore(session, Outbox, codec).add(
                    OutboxEnvelope(OpaqueId(value.id), "exact", value, now(), now())
                )
                session.add(Effect(identity="before", value=1))
                assert await SQLAlchemyCheckpointStore(session, CheckpointRow).advance(
                    "sync", expected=None, checkpoint=Checkpoint(b"before")
                )
            async with db.sessions() as session:
                stored = (await session.execute(select(Outbox.payload))).scalar_one()
                assert bytes(stored) == codec.encode(value)
            policy = ExactJsonSha256Integrity()
            sealed = policy.seal(page()).encode()
            for raw, acknowledgement in [
                (sealed.replace(b'"n":1.0', b'"n":1e0'), EXACT_JSON_INTEGRITY),
                (sealed, None),
                (sealed, "changed"),
            ]:
                with pytest.raises(WireIntegrityError):
                    async with db.sessions() as session, session.begin():
                        decode_sync_raw(
                            raw,
                            integrity=SyncIntegritySelection(policy),
                            acknowledgement=acknowledgement,
                        )
                        session.add(Effect(identity="after", value=2))
                        await SQLAlchemyCheckpointStore(session, CheckpointRow).advance(
                            "sync", expected=Checkpoint(b"before"), checkpoint=Checkpoint(b"after")
                        )
                async with db.sessions() as session:
                    assert (
                        await session.execute(select(func.count()).select_from(Effect))
                    ).scalar_one() == 1
                    assert await SQLAlchemyCheckpointStore(session, CheckpointRow).load(
                        "sync"
                    ) == Checkpoint(b"before")
            relay = SQLAlchemyRelayStore(db.sessions, Outbox, codec)
            claims = await relay.claim(
                now=now(), limit=1, claim_ttl=timedelta(seconds=30), max_bytes=10000
            )
            assert codec.encode(claims[0].envelope.payload) == codec.encode(value)
            assert db.engine.pool.checkedout() == 0

    asyncio.run(run())


@pytest.mark.integration
def test_exact_postgres_relay_jetstream_and_atomic_consumer_admission():
    async def run():
        client = await connect_nats()
        js = client.jetstream()
        identity = "pytitect_" + uuid.uuid4().hex
        codec = ExactJsonMessageCodec()
        value = replace(
            exact_message("[1,1.0,1e0,1E+000,-0,1.00000000000000001,1e9999," + "9" * 4301 + "]"),
            id=identity,
        )
        created = False
        try:
            await js.add_stream(name=identity, subjects=[identity], max_msgs=10, max_bytes=100000)
            created = True
            subscription = await js.pull_subscribe(
                identity,
                durable="exact",
                config=ConsumerConfig(ack_wait=2, max_deliver=3, max_ack_pending=1),
            )
            async with Database() as db:
                relay_store = SQLAlchemyRelayStore(db.sessions, Outbox, codec)
                await relay_store.add(
                    OutboxEnvelope(OpaqueId(identity), "exact", value, now(), now())
                )
                relay = AsyncRelay(
                    relay_store,
                    NatsJetStreamPublisher(js, codec=codec),
                    RoutingTable([Route(value.type, identity)]),
                )
                assert (await relay.run_once(limit=1)).delivered == 1
                raw = (await subscription.fetch(1, timeout=3))[0]
                assert raw.data == codec.encode(value)
                assert raw.headers["Titect-Profile"] == "titect-message/2"
                delivery = NatsDelivery(raw, codec=codec)
                observed = []

                def handle(message, context):
                    observed.append(codec.encode(message))
                    return Decision()

                async def save(session, decision):
                    session.add(Effect(identity=identity, value=1))

                consumer = AsyncConsumer(
                    consumer="exact",
                    namespace="test",
                    handler=handle,
                    unit_of_work=SQLAlchemyUnitOfWorkFactory(
                        db.sessions, inbox_model=Inbox, save_decision=save
                    ),
                    quarantine=InMemoryRejectedDeliveryStore(),
                    codec=codec,
                )
                assert await consumer.process(delivery) == DeliveryAck()
                await client.flush(timeout=3)
                assert observed == [raw.data]
                async with db.sessions() as session:
                    assert (await session.execute(select(Effect.value))).scalar_one() == 1
                    assert (
                        await session.execute(select(Inbox.completed_at))
                    ).scalar_one() is not None
                    assert (
                        await session.execute(select(Outbox.delivered_at))
                    ).scalar_one() is not None
                assert db.engine.pool.checkedout() == 0
        finally:
            if created:
                await js.delete_stream(identity)
            await client.close()

    asyncio.run(run())
