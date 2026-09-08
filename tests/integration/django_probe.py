"""Configure Django only inside an isolated test subprocess."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse


def main() -> None:
    import django
    import psycopg
    from django.conf import settings
    from django.db import connections, models, transaction
    from psycopg import sql

    dsn = os.environ["TEST_POSTGRES_DSN"]
    parsed, schema = urlparse(dsn), "pytitect_django_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as setup:
        setup.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        settings.configure(
            INSTALLED_APPS=[],
            USE_TZ=True,
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.postgresql",
                    "NAME": parsed.path.lstrip("/"),
                    "USER": parsed.username,
                    "PASSWORD": parsed.password,
                    "HOST": parsed.hostname,
                    "PORT": parsed.port,
                    "OPTIONS": {
                        "options": (
                            f"-c search_path={schema} "
                            "-c statement_timeout=10000 -c lock_timeout=7000"
                        ),
                        "connect_timeout": 5,
                    },
                }
            },
        )
        django.setup()
        from pytitect.aio import AsyncRelay
        from pytitect.checkpoints import Checkpoint
        from pytitect.core import OpaqueId
        from pytitect.django import (
            DjangoAsyncBridge,
            DjangoAsyncOutboxStore,
            DjangoCheckpointStore,
            DjangoFencedCommit,
            DjangoInboxStore,
            DjangoLeaseStore,
            DjangoRelayStore,
            DjangoTransactionRunner,
        )
        from pytitect.django.abstract_models import (
            AbstractCheckpointModel,
            AbstractInboxModel,
            AbstractLeaseAuthorityModel,
            AbstractRelayOutboxModel,
        )
        from pytitect.inbox import InboxAccepted, InboxDuplicate, InboxScope
        from pytitect.leases import FencedCommitted, LeaseAcquired, StaleLease
        from pytitect.messaging import (
            JsonMessageCodec,
            Message,
            PublicationConfirmed,
            Route,
            RoutingTable,
        )
        from pytitect.outbox import OutboxEnvelope

        def model(name, base, unique):
            meta = type(
                "Meta",
                (),
                {
                    "app_label": "synthetic",
                    "db_table": name.lower(),
                    "constraints": [
                        models.UniqueConstraint(fields=unique, name=name.lower() + "_identity")
                    ],
                },
            )
            return type(name, (base,), {"__module__": __name__, "Meta": meta})

        checkpoint_model = model("Checkpoint", AbstractCheckpointModel, ["stream"])
        inbox_model = model(
            "Inbox", AbstractInboxModel, ["namespace", "source", "consumer", "message_id"]
        )
        lease_model = model("Lease", AbstractLeaseAuthorityModel, ["resource_key"])
        outbox_model = model("Outbox", AbstractRelayOutboxModel, ["message_id"])
        with connections["default"].schema_editor() as editor:
            for concrete in (checkpoint_model, inbox_model, lease_model, outbox_model):
                editor.create_model(concrete)

        def now():
            return datetime.now(UTC)

        checkpoint = DjangoCheckpointStore.from_model(checkpoint_model, using="default")
        barrier = threading.Barrier(2, timeout=5)

        def advance():
            try:
                barrier.wait()
                with transaction.atomic():
                    return checkpoint.advance(
                        "stream", expected=None, checkpoint=Checkpoint(b"one")
                    )
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: advance(), range(2)))
        assert sorted(results) == [False, True]
        assert checkpoint.load("stream") == Checkpoint(b"one")
        inbox = DjangoInboxStore.from_model(inbox_model, using="default")
        scope = InboxScope("test", "source", "consumer")

        def reserve(token):
            try:
                barrier.wait()
                with transaction.atomic():
                    decision = inbox.begin(
                        scope,
                        OpaqueId("message"),
                        token=token,
                        now=now(),
                        ttl=timedelta(seconds=30),
                    )
                    if isinstance(decision, InboxAccepted):
                        assert inbox.complete(scope, OpaqueId("message"), token=token, now=now())
                    return decision
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(reserve, ("one", "two")))
        assert sum(isinstance(result, InboxAccepted) for result in results) == 1
        assert sum(isinstance(result, InboxDuplicate) for result in results) == 1
        leases = DjangoLeaseStore.from_model(lease_model, using="default")
        first = leases.acquire("resource", owner="one", now=now(), ttl=timedelta(seconds=30))
        assert isinstance(first, LeaseAcquired)
        lease_model.objects.filter(resource_key="resource").update(
            expires_at=now() - timedelta(seconds=1)
        )
        replacement = leases.acquire("resource", owner="two", now=now(), ttl=timedelta(seconds=30))
        assert replacement.lease.fencing_token > first.lease.fencing_token
        fence = DjangoFencedCommit.from_store(leases, now=now)
        assert isinstance(fence.commit(first.lease, lambda: False), StaleLease)
        assert isinstance(fence.commit(replacement.lease, lambda: True), FencedCommitted)
        codec = JsonMessageCodec()
        store = DjangoRelayStore(
            outbox_model,
            using="default",
            encode_payload=lambda msg: json.loads(codec.encode(msg)),
            decode_payload=lambda value: codec.decode(json.dumps(value).encode()),
        )
        stamp = now().replace(microsecond=0)
        msg = Message(
            id="one",
            source="urn:example:reliability",
            type="example.changed.v1",
            subject="test",
            time=stamp,
            dataschema="urn:example:test:1",
            data={},
        )
        with transaction.atomic():
            store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, stamp, stamp))

        async def exercise():
            class Publisher:
                async def publish(self, **kwargs):
                    # Query on an independent connection through the explicit bridge.
                    return PublicationConfirmed("one")

            bridge = DjangoAsyncBridge(concurrency=2, thread_sensitive=False)
            adapter = DjangoAsyncOutboxStore(
                store, transaction=DjangoTransactionRunner("default"), bridge=bridge
            )
            summary = await AsyncRelay(
                adapter, Publisher(), RoutingTable([Route(msg.type, "events")])
            ).run_once(limit=100)
            assert summary.delivered == 1
            await bridge.run(lambda: connections.close_all())

        asyncio.run(exercise())
        connections.close_all()
        assert outbox_model.objects.get(message_id="one").delivered_at is not None
        print("Django PostgreSQL adapter and async bridge conformance passed.")
    finally:
        connections.close_all()
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as cleanup:
            cleanup.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()
