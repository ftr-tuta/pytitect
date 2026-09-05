"""Loopback-only synthetic FastAPI/PostgreSQL/JetStream application."""

from __future__ import annotations

import argparse
import asyncio
import resource
from contextlib import asynccontextmanager
from datetime import timedelta

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from nats.js.api import ConsumerConfig
from sqlalchemy import func, select, text
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

from pytitect.aio import AsyncConsumer, AsyncRelay, InMemoryRejectedDeliveryStore
from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.fastapi import idempotency_key_from_headers
from pytitect.idempotency import (
    Conflict,
    IdempotencyPolicy,
    IdempotencyScope,
    Replay,
    RequestFingerprint,
)
from pytitect.messaging import Route, RoutingTable
from pytitect.nats import NatsDelivery, NatsJetStreamPublisher
from pytitect.outbox import OutboxEnvelope
from pytitect.sqlalchemy.idempotency import RequestCommitted, SQLAlchemyIdempotentRequest
from pytitect.sqlalchemy.relay import SQLAlchemyRelayStore
from pytitect.sqlalchemy.stores import SQLAlchemyOutboxStore
from pytitect.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory


def build_app(schema: str, subject: str, *, admission: int = 8, db_delay: float = 0) -> FastAPI:
    db = Database(schema=schema)
    active = 0
    peaks = {"tasks": 0, "connections": 0, "rss_kib": 0}
    counts = {"retried": 0, "uncertain": 0, "useful": 0, "accepted": 0, "rejected": 0}
    scope = IdempotencyScope("benchmark", "synthetic", "operation")
    requests = SQLAlchemyIdempotentRequest(
        db.sessions,
        idempotency_model=Idempotency,
        receipt_model=ReceiptRow,
        serializer=CODEC,
        policy=IdempotencyPolicy(timedelta(seconds=10), timedelta(days=1), timedelta(days=1)),
    )
    tasks = []

    @asynccontextmanager
    async def lifespan(app):
        client = await connect_nats()
        js = client.jetstream()
        subscription = await js.pull_subscribe(
            subject,
            durable="benchmark",
            config=ConsumerConfig(ack_wait=1, max_deliver=50, max_ack_pending=16),
        )
        relay = AsyncRelay(
            SQLAlchemyRelayStore(db.sessions, Outbox, CODEC),
            NatsJetStreamPublisher(js),
            RoutingTable([Route("example.changed.v1", subject)]),
            concurrency=4,
            max_admitted=8,
            max_retained_bytes=64 * 1024,
            claim_ttl=timedelta(seconds=1),
        )

        async def save(session, decision):
            session.add(Effect(identity="received:" + decision.result, value=1))

        consumer = AsyncConsumer(
            consumer="benchmark",
            namespace="benchmark",
            handler=lambda msg, ctx: Decision(result=msg.id),
            unit_of_work=SQLAlchemyUnitOfWorkFactory(
                db.sessions, inbox_model=Inbox, save_decision=save
            ),
            quarantine=InMemoryRejectedDeliveryStore(),
            concurrency=4,
            queue_capacity=4,
            max_message_bytes=8192,
            max_retained_bytes=65536,
        )

        async def publish():
            while True:
                summary = await relay.run_once(limit=8)
                counts["retried"] += summary.retried
                counts["uncertain"] += summary.uncertain
                await asyncio.sleep(0.01)

        async def consume():
            while True:
                try:
                    raw = await subscription.fetch(8, timeout=0.2)
                except TimeoutError:
                    continue

                async def source(raw=raw):
                    for item in raw:
                        yield NatsDelivery(item)

                summary = await consumer.run(source())
                counts["useful"] += summary.acknowledged

        async with asyncio.TaskGroup() as group:
            tasks.extend([group.create_task(publish()), group.create_task(consume())])
            try:
                yield
            finally:
                for task in tasks:
                    task.cancel()
        await client.close()
        await db.engine.dispose()

    app = FastAPI(title="Synthetic reliability capacity fixture", lifespan=lifespan)

    @app.post("/operations")
    async def operation(request: Request):
        nonlocal active
        if active >= admission:
            counts["rejected"] += 1
            return JSONResponse({"status": "busy"}, status_code=503)
        active += 1
        try:
            key = idempotency_key_from_headers(request.headers).value
            if int(request.headers.get("content-length", "0")) > 8192:
                return JSONResponse({"status": "too_large"}, status_code=413)
            payload = await request.json()
            fingerprint = RequestFingerprint.from_json(payload)

            async def mutate(session):
                if db_delay:
                    await session.execute(text("SELECT pg_sleep(:delay)"), {"delay": db_delay})
                msg = message(key)
                session.add(Effect(identity="accepted:" + key, value=1))
                await SQLAlchemyOutboxStore(session, Outbox, CODEC).add(
                    OutboxEnvelope(OpaqueId(key), subject, msg, msg.time, msg.time)
                )
                return msg

            result = await requests.execute(
                scope=scope,
                key=key,
                fingerprint=fingerprint,
                receipt_id=OpaqueId(key),
                mutate=mutate,
            )
            if isinstance(result, RequestCommitted):
                counts["accepted"] += 1
                return JSONResponse({"id": result.value.id}, status_code=201)
            if isinstance(result, Replay):
                return JSONResponse({"id": result.value.id}, status_code=200)
            return JSONResponse(
                {"status": "conflict" if isinstance(result, Conflict) else "pending"},
                status_code=409 if isinstance(result, Conflict) else 202,
            )
        finally:
            active -= 1

    @app.get("/reconciliation/{key}")
    async def reconcile(key: str):
        result = await requests.reconcile(
            scope=scope, key=key, fingerprint=RequestFingerprint.from_json({"value": 1})
        )
        return JSONResponse(
            {"status": "completed" if isinstance(result, Replay) else "uncertain"},
            status_code=200 if isinstance(result, Replay) else 202,
        )

    @app.get("/metrics")
    async def metrics():
        peaks["tasks"] = max(peaks["tasks"], len(asyncio.all_tasks()))
        peaks["connections"] = max(peaks["connections"], db.engine.pool.checkedout())
        peaks["rss_kib"] = max(peaks["rss_kib"], resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        async with db.sessions() as session:
            pending, oldest = (
                await session.execute(
                    select(func.count(), func.min(Outbox.occurred_at)).where(
                        Outbox.delivered_at.is_(None)
                    )
                )
            ).one()
            waits = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type='Lock' AND datname=current_database() "
                    "AND application_name=:application"
                ),
                {"application": db.schema},
            )
        return {
            **counts,
            **peaks,
            "pending": pending,
            "backlog_age_seconds": 0
            if oldest is None
            else max(0, (now() - oldest).total_seconds()),
            "database_lock_waiters": waits,
            "active_requests": active,
            "background_ok": not any(task.done() for task in tasks),
        }

    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--db-delay", type=float, default=0)
    args = parser.parse_args()
    uvicorn.run(
        build_app(args.schema, args.subject, db_delay=args.db_delay),
        host="127.0.0.1",
        port=args.port,
        log_level="error",
        access_log=False,
        limit_concurrency=64,
        timeout_keep_alive=2,
    )


if __name__ == "__main__":
    main()
