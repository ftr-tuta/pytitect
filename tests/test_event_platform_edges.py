import asyncio
import json
from collections.abc import AsyncIterator
from concurrent.futures import Executor, Future
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from pytitect.aio import (
    AsyncConsumer,
    AsyncQueryRuntime,
    AsyncRelay,
    InMemoryAsyncCheckpointStore,
    InMemoryAsyncInboxStore,
    InMemoryAsyncOutboxStore,
    InMemoryAsyncUnitOfWorkFactory,
    InMemoryRejectedDeliveryStore,
    PermanentProcessingError,
    QuarantinePolicy,
    RejectedDelivery,
    RetryableProcessingError,
    rejected_delivery,
)
from pytitect.application import (
    Command,
    CommandBinding,
    CommandRegistry,
    Decision,
    DomainEvent,
    HandlingContext,
    Query,
    QueryBinding,
    QueryRegistry,
    Task,
)
from pytitect.aws import (
    AwsConsumerSpec,
    AwsTopology,
    AwsTopologyAction,
    EventBridgePublisher,
    SqsDelivery,
    SqsDeliverySource,
    apply_aws_topology,
    classify_aws_error,
    plan_aws_topology,
)
from pytitect.checkpoints import Checkpoint
from pytitect.core import OpaqueId
from pytitect.event_sourcing import (
    AppendCommitted,
    DuplicateEventId,
    InMemoryEventStore,
    NewEvent,
    Snapshot,
    StoredEvent,
    StreamId,
    WrongExpectedVersion,
)
from pytitect.fastapi import IdempotencyKey, idempotency_key_from_headers
from pytitect.faststream_nats import FastStreamNatsDelivery
from pytitect.inbox import InboxScope
from pytitect.jobs import (
    InMemoryJobStore,
    Job,
    JobClaim,
    JobDuplicate,
    JobRetried,
    JobRunner,
    JobSchedule,
    JobState,
    JobTerminated,
    PermanentJobError,
    ScheduleKind,
    StaleJobClaim,
)
from pytitect.messaging import (
    CapabilitiesRejected,
    CapabilityRequirements,
    CodecRegistry,
    DeliveryTerminated,
    JsonMessageCodec,
    Message,
    MessageType,
    MessageTypeRegistry,
    PublicationConfirmed,
    PublicationRejected,
    PublicationRetryable,
    Route,
    RoutingTable,
    TransportCapabilities,
    negotiate_capabilities,
)
from pytitect.nats import (
    ConsumerSpec,
    NatsDelivery,
    NatsJetStreamPublisher,
    NatsPullDeliverySource,
    NatsTopology,
    StreamSpec,
    classify_nats_publication_error,
    plan_nats_topology,
)
from pytitect.operations import (
    Metric,
    OperationalEvent,
    ProbeResult,
    ReadinessPolicy,
    RuntimeRole,
    evaluate_readiness,
    safe_failure_reason,
    trace_from_transport_headers,
    trace_transport_headers,
)
from pytitect.outbox import OutboxEnvelope
from pytitect.processes import (
    InMemoryProcessManagerStore,
    ProcessApplied,
    ProcessDecision,
    ProcessEffect,
    ProcessEffectKind,
    ProcessKey,
    ProcessManagerBinding,
    ProcessManagerRegistry,
    ProcessManagerRuntime,
    ProcessState,
    ProcessStatus,
    ProcessTimer,
    ProcessTimerClaim,
    StaleProcessVersion,
    TimerSchedule,
)
from pytitect.projections import (
    InMemoryProjectionStore,
    ProjectionApplied,
    ProjectionDefinition,
    ProjectionKey,
    ProjectionRuntime,
    ProjectionState,
    ProjectionVersionMismatch,
    RebuildRun,
    RebuildStatus,
    StaleProjectionCheckpoint,
)
from pytitect.trace import TraceContext

NOW = datetime(2026, 9, 3, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return NOW


def message(identifier: str = "message-1", **changes: object) -> Message:
    values: dict[str, object] = {
        "id": identifier,
        "source": "urn:example:test",
        "type": "example.changed.v1",
        "subject": "example/1",
        "time": NOW,
        "dataschema": "urn:example:changed:1",
        "data": {"value": 1},
    }
    values.update(changes)
    return Message(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("changes", "pattern"),
    [
        ({"id": ""}, "id"),
        ({"source": " source "}, "source"),
        ({"type": "1invalid"}, "type"),
        ({"correlationid": ""}, "correlationid"),
        ({"causationid": " bad "}, "causationid"),
        ({"specversion": "0.3"}, "specversion"),
        ({"datacontenttype": "text/plain"}, "datacontenttype"),
        ({"profile": "other"}, "profile"),
        ({"time": datetime(2026, 9, 3)}, "timezone-aware"),
        ({"time": NOW.replace(microsecond=1)}, "millisecond"),
    ],
)
def test_message_validation_edges(changes: dict[str, object], pattern: str) -> None:
    with pytest.raises(ValueError, match=pattern):
        message(**changes)


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        b"[]",
        b'{"id":"only"}',
        b'{"id":"x","source":"s","specversion":"1.0","type":"x","subject":"s","time":"2026-09-03T00:00:00.000Z","dataschema":"d","datacontenttype":"application/json","profile":"titect-message/1","data":null,"correlationid":1}',
        b'{"id":"x","source":"s","specversion":"1.0","type":"x","subject":"s","time":"2026-09-03T00:00:00.000Z","dataschema":"d","datacontenttype":"application/json","profile":"titect-message/1","data":NaN}',
    ],
)
def test_message_codec_rejects_malformed_documents(payload: bytes) -> None:
    with pytest.raises(ValueError):
        JsonMessageCodec().decode(payload)


def test_message_registry_and_result_validation_edges() -> None:
    with pytest.raises(ValueError):
        CodecRegistry([])
    codec = JsonMessageCodec()
    with pytest.raises(ValueError, match="unique"):
        CodecRegistry([codec, codec])
    with pytest.raises(LookupError):
        CodecRegistry([codec]).require("missing")
    with pytest.raises(ValueError):
        MessageType("", "schema")
    registry = MessageTypeRegistry([MessageType("example.changed.v1", "schema")])
    with pytest.raises(ValueError, match="schema"):
        registry.validate("example.changed.v1", "wrong")
    with pytest.raises(ValueError):
        Route("", "destination")
    with pytest.raises(LookupError):
        RoutingTable([]).destination_for("missing")
    with pytest.raises(ValueError):
        DeliveryTerminated("")
    for factory in (
        lambda: PublicationConfirmed(""),
        lambda: PublicationRetryable(""),
        lambda: PublicationRejected(""),
    ):
        with pytest.raises(ValueError):
            factory()


def test_capability_validation_and_full_acceptance_edges() -> None:
    with pytest.raises(ValueError):
        TransportCapabilities(False, False, False, 0)
    with pytest.raises(ValueError):
        CapabilityRequirements(max_message_bytes=0)
    rejected = negotiate_capabilities(
        TransportCapabilities(False, False, False, 10),
        CapabilityRequirements(topology_management=True),
    )
    assert isinstance(rejected, CapabilitiesRejected)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Command("", None),
        lambda: Query(" bad ", None),
        lambda: DomainEvent("", None),
        lambda: Task("", None),
        lambda: HandlingContext(""),
        lambda: CommandBinding("", lambda command, context: Decision()),
        lambda: QueryBinding("", lambda query, context: None),
    ],
)
def test_application_validation_edges(factory: Any) -> None:
    with pytest.raises(ValueError):
        factory()


def test_application_lookup_and_query_result_validation() -> None:
    commands = CommandRegistry([])
    with pytest.raises(LookupError):
        commands.handler_for("missing")
    queries = QueryRegistry(
        [QueryBinding("bad", lambda query, context: {1: "not-json"})]  # type: ignore[dict-item]
    )
    with pytest.raises(ValueError):
        queries.dispatch(Query("bad", None), HandlingContext("query"))


def test_async_reference_store_transition_edges() -> None:
    async def exercise() -> None:
        inbox = InMemoryAsyncInboxStore()
        scope = InboxScope("tests", "source", "consumer")
        identifier: OpaqueId[object] = OpaqueId("message")
        accepted = await inbox.begin(
            scope, identifier, token="one", now=NOW, ttl=timedelta(seconds=1)
        )
        assert accepted
        assert not await inbox.complete(scope, identifier, token="wrong", now=NOW)
        assert await inbox.abandon(scope, identifier, token="one")
        checkpoints = InMemoryAsyncCheckpointStore()
        assert await checkpoints.load_for_update("stream") is None
        assert await checkpoints.advance("stream", expected=None, checkpoint=Checkpoint(b"one"))
        outbox = InMemoryAsyncOutboxStore[str]()
        envelope = OutboxEnvelope(OpaqueId("outbox"), "events", "value", NOW, NOW)
        await outbox.add(envelope)
        claim = (await outbox.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=1)))[0]
        assert await outbox.retry(claim, available_at=NOW)
        claim = (await outbox.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=1)))[0]
        assert await outbox.failed(claim, reason="terminal", at=NOW)

    asyncio.run(exercise())


def test_quarantine_policy_retention_duplicates_and_bounds() -> None:
    with pytest.raises(ValueError):
        QuarantinePolicy(max_reason_chars=0)
    policy = QuarantinePolicy(retain_payload=True, max_payload_bytes=4, max_metadata_items=1)
    item = rejected_delivery(
        quarantine_id="q",
        message_id="m",
        source="s",
        consumer="c",
        failed_at=NOW,
        reason="",
        encoded_payload=b"data",
        policy=policy,
    )
    assert item.payload == b"data" and item.reason == "rejected"

    async def exercise() -> None:
        store = InMemoryRejectedDeliveryStore(capacity=1)
        assert await store.add(item)
        assert not await store.add(item)
        other = rejected_delivery(
            quarantine_id="other",
            message_id="m",
            source="s",
            consumer="c",
            failed_at=NOW,
            reason="bad",
            encoded_payload=b"data",
            policy=policy,
        )
        with pytest.raises(OverflowError):
            await store.add(other)

    asyncio.run(exercise())
    with pytest.raises(ValueError, match="payload"):
        rejected_delivery(
            quarantine_id="q",
            message_id="m",
            source="s",
            consumer="c",
            failed_at=NOW,
            reason="bad",
            encoded_payload=b"large",
            policy=policy,
        )
    with pytest.raises(ValueError, match="metadata"):
        rejected_delivery(
            quarantine_id="q",
            message_id="m",
            source="s",
            consumer="c",
            failed_at=NOW,
            reason="bad",
            encoded_payload=b"data",
            policy=policy,
            metadata={"one": 1, "two": 2},
        )
    with pytest.raises(ValueError):
        InMemoryRejectedDeliveryStore(capacity=0)
    with pytest.raises(ValueError, match="identity"):
        RejectedDelivery("", "m", "s", "c", NOW, "0" * 64, "bad")
    with pytest.raises(ValueError, match="SHA-256"):
        RejectedDelivery("q", "m", "s", "c", NOW, "short", "bad")
    with pytest.raises(ValueError, match="timezone"):
        RejectedDelivery("q", "m", "s", "c", datetime(2026, 1, 1), "0" * 64, "bad")


def test_in_memory_unit_of_work_lifecycle_and_capacity_edges() -> None:
    with pytest.raises(ValueError):
        InMemoryAsyncUnitOfWorkFactory(capacity=0)

    async def exercise() -> None:
        factory = InMemoryAsyncUnitOfWorkFactory(capacity=1)
        transaction = factory()
        with pytest.raises(RuntimeError):
            await transaction.save_decision(Decision())
        async with transaction:
            with pytest.raises(RuntimeError, match="single-use"):
                await transaction.__aenter__()
            scope = InboxScope("tests", "source", "consumer")
            identifier: OpaqueId[object] = OpaqueId("message")
            assert not await transaction.complete_message(
                scope, identifier, token="missing", now=NOW
            )
            await transaction.save_decision(Decision())
            with pytest.raises(OverflowError):
                await transaction.save_decision(Decision())
            await transaction.rollback()
            await transaction.rollback()
            with pytest.raises(RuntimeError):
                await transaction.commit()

    asyncio.run(exercise())


class RuntimeDelivery:
    def __init__(self, value: Message) -> None:
        self._message = value
        self.actions: list[str] = []

    @property
    def message(self) -> Message:
        return self._message

    async def ack(self) -> None:
        self.actions.append("ack")

    async def retry(self, *, delay: timedelta | None = None) -> None:
        self.actions.append("retry")

    async def terminate(self) -> None:
        self.actions.append("term")


@pytest.mark.parametrize("failure", [RetryableProcessingError(), ConnectionError(), OSError()])
def test_consumer_retry_classification(failure: Exception) -> None:
    def handler(value: Message, context: HandlingContext) -> Decision:
        raise failure

    delivery = RuntimeDelivery(message())
    consumer = AsyncConsumer(
        consumer="consumer",
        namespace="tests",
        handler=handler,
        unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
        quarantine=InMemoryRejectedDeliveryStore(),
    )
    assert asyncio.run(consumer.process(delivery)) == "retried"
    assert delivery.actions == ["retry"]


def test_consumer_bounded_run_and_failed_quarantine_retry() -> None:
    class BrokenQuarantine:
        async def add(self, item: object) -> bool:
            raise OSError("unavailable")

    consumer = AsyncConsumer(
        consumer="consumer",
        namespace="tests",
        handler=lambda value, context: (_ for _ in ()).throw(PermanentProcessingError("bad")),
        unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
        quarantine=BrokenQuarantine(),
        concurrency=2,
        queue_capacity=1,
    )
    deliveries = [RuntimeDelivery(message("one")), RuntimeDelivery(message("two"))]

    async def source() -> AsyncIterator[RuntimeDelivery]:
        for delivery in deliveries:
            yield delivery

    summary = asyncio.run(consumer.run(source()))
    assert summary.retried == 2


def test_runtime_validation_nonaccepted_completion_and_cancellation_edges() -> None:
    with pytest.raises(ValueError):
        AsyncConsumer(
            consumer="",
            namespace="tests",
            handler=lambda value, context: Decision(),
            unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
            quarantine=InMemoryRejectedDeliveryStore(),
        )
    with pytest.raises(ValueError):
        AsyncRelay(
            InMemoryAsyncOutboxStore(),
            ResultPublisher([]),
            RoutingTable([]),
            concurrency=0,
        )

    class RefusingTransaction:
        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def reserve_message(self, *args: object, **kwargs: object) -> object:
            from pytitect.inbox import InboxCapacityExceeded

            return InboxCapacityExceeded(1)

        async def complete_message(self, *args: object, **kwargs: object) -> bool:
            return False

        async def save_decision(self, decision: Decision) -> None:
            return None

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

    def refusing() -> RefusingTransaction:
        return RefusingTransaction()

    delivery = RuntimeDelivery(message())
    consumer = AsyncConsumer(
        consumer="consumer",
        namespace="tests",
        handler=lambda value, context: Decision(),
        unit_of_work=refusing,
        quarantine=InMemoryRejectedDeliveryStore(),
    )
    assert asyncio.run(consumer.process(delivery)) == "retried"

    class CancelPublisher:
        async def publish(self, *, destination: str, message: Message) -> object:
            raise asyncio.CancelledError

    async def cancelled_relay() -> None:
        store = InMemoryAsyncOutboxStore[Message]()
        await store.add(OutboxEnvelope(OpaqueId("cancel"), "events", message(), NOW, NOW))
        with pytest.raises(asyncio.CancelledError):
            await AsyncRelay(
                store,
                CancelPublisher(),
                RoutingTable([Route("example.changed.v1", "events")]),
                clock=FixedClock(),
            ).run_once(limit=1)

    asyncio.run(cancelled_relay())


class ResultPublisher:
    def __init__(self, results: list[object]) -> None:
        self.results = results

    async def publish(self, *, destination: str, message: Message) -> object:
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_query_runtime_and_relay_retry_failure_branches() -> None:
    query = QueryRegistry([QueryBinding("query", lambda value, context: {"ok": True})])
    assert asyncio.run(
        AsyncQueryRuntime(query).execute(Query("query", None), HandlingContext("q"))
    ).result == {"ok": True}

    async def exercise() -> tuple[object, object, object]:
        routes = RoutingTable([Route("example.changed.v1", "events")])
        store = InMemoryAsyncOutboxStore[Message]()
        await store.add(OutboxEnvelope(OpaqueId("one"), "events", message("one"), NOW, NOW))
        retry = await AsyncRelay(
            store,
            ResultPublisher([PublicationRetryable("later")]),
            routes,
            clock=FixedClock(),
        ).run_once(limit=1)
        later = NOW + timedelta(seconds=1)
        retry_claim = await store.claim(now=later, limit=1, claim_ttl=timedelta(seconds=1))
        assert retry_claim
        await store.retry(retry_claim[0], available_at=NOW)
        rejected = await AsyncRelay(
            store,
            ResultPublisher([PublicationRejected("invalid")]),
            routes,
            clock=FixedClock(),
        ).run_once(limit=1)
        second = InMemoryAsyncOutboxStore[Message]()
        await second.add(OutboxEnvelope(OpaqueId("two"), "events", message("two"), NOW, NOW))
        unavailable = await AsyncRelay(
            second, ResultPublisher([OSError()]), routes, clock=FixedClock()
        ).run_once(limit=1)
        return retry, rejected, unavailable

    retry, rejected, unavailable = asyncio.run(exercise())
    assert retry.retried == 1
    assert rejected.failed == 1
    assert unavailable.retried == 1


class ImmediateExecutor(Executor):
    def submit(self, fn: Any, /, *args: object, **kwargs: object) -> Future[object]:
        future: Future[object] = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return future


class AwsBackend:
    def __init__(self, current: AwsConsumerSpec | None = None) -> None:
        self.current = current
        self.operations: list[object] = []

    async def bus_exists(self, name: str) -> bool:
        return False

    async def queue_exists(self, name: str) -> bool:
        return False

    async def binding(self, bus: str, consumer: str) -> AwsConsumerSpec | None:
        return self.current

    async def apply(self, operation: object) -> None:
        self.operations.append(operation)


def test_aws_topology_validation_plan_and_apply() -> None:
    with pytest.raises(ValueError):
        AwsConsumerSpec("", "queue", ("event",))
    spec = AwsConsumerSpec("consumer", "queue", ("example.changed.v1",))
    with pytest.raises(ValueError):
        AwsTopology("bus", (spec, spec))
    topology = AwsTopology("bus", (spec,))

    async def exercise() -> object:
        backend = AwsBackend(AwsConsumerSpec("consumer", "queue", ("other",)))
        plan = await plan_aws_topology(topology, backend)
        assert plan.operations[-1].action is AwsTopologyAction.UPDATE_RULE_TARGET
        assert await apply_aws_topology(plan, backend) == 3
        return backend

    assert len(asyncio.run(exercise()).operations) == 3


def test_aws_publication_and_delivery_failure_edges() -> None:
    with pytest.raises(ValueError):
        EventBridgePublisher(object(), event_bus_name="", executor=ImmediateExecutor())
    with pytest.raises(ValueError):
        EventBridgePublisher(
            object(), event_bus_name="bus", executor=ImmediateExecutor(), max_concurrency=0
        )
    publisher = EventBridgePublisher(
        SimpleNamespace(put_events=lambda **kwargs: {"FailedEntryCount": 0, "Entries": [{}]}),
        event_bus_name="bus",
        executor=ImmediateExecutor(),
    )
    assert isinstance(
        asyncio.run(publisher.publish(destination="wrong", message=message())), PublicationRejected
    )
    assert isinstance(
        asyncio.run(publisher.publish(destination="bus", message=message())), PublicationRetryable
    )
    assert isinstance(classify_aws_error(OSError()), PublicationRetryable)
    assert isinstance(classify_aws_error(ValueError()), PublicationRejected)
    broken = EventBridgePublisher(
        SimpleNamespace(put_events=lambda **kwargs: (_ for _ in ()).throw(OSError("down"))),
        event_bus_name="bus",
        executor=ImmediateExecutor(),
    )
    assert isinstance(
        asyncio.run(broken.publish(destination="bus", message=message())), PublicationRetryable
    )
    rejected = EventBridgePublisher(
        SimpleNamespace(
            put_events=lambda **kwargs: {
                "FailedEntryCount": 1,
                "Entries": [{"ErrorCode": "InvalidEventPattern", "ErrorMessage": "bad"}],
            }
        ),
        event_bus_name="bus",
        executor=ImmediateExecutor(),
    )
    assert isinstance(
        asyncio.run(rejected.publish(destination="bus", message=message())), PublicationRejected
    )

    body = json.dumps({"detail": JsonMessageCodec().encode(message()).decode()})
    raw = {"Body": body, "ReceiptHandle": "receipt"}
    client = SimpleNamespace(
        delete_message=lambda **kwargs: None,
        change_message_visibility=lambda **kwargs: None,
    )

    async def delivery_edges() -> None:
        delivery = SqsDelivery(
            client,
            queue_url="queue",
            raw_message=raw,
            executor=ImmediateExecutor(),
            semaphore=asyncio.Semaphore(1),
        )
        with pytest.raises(ValueError):
            await delivery.retry(delay=timedelta(hours=13))
        await delivery.retry()
        await delivery.terminate()
        with pytest.raises(RuntimeError):
            await delivery.ack()

    asyncio.run(delivery_edges())
    with pytest.raises(ValueError):
        SqsDelivery(
            client,
            queue_url="queue",
            raw_message={"Body": "bad", "ReceiptHandle": "receipt"},
            executor=ImmediateExecutor(),
            semaphore=asyncio.Semaphore(1),
        )
    with pytest.raises(ValueError):
        SqsDelivery(
            client,
            queue_url="",
            raw_message=raw,
            executor=ImmediateExecutor(),
            semaphore=asyncio.Semaphore(1),
        )


def test_sqs_source_bounds_and_receive() -> None:
    body = JsonMessageCodec().encode(message()).decode()
    client = SimpleNamespace(
        receive_message=lambda **kwargs: {"Messages": [{"Body": body, "ReceiptHandle": "receipt"}]}
    )

    async def exercise() -> None:
        source = SqsDeliverySource(
            client,
            queue_url="queue",
            executor=ImmediateExecutor(),
            wait_time=timedelta(0),
        )
        deliveries = [delivery async for delivery in source.deliveries(batch_size=1)]
        assert deliveries[0].message == message()
        with pytest.raises(ValueError):
            _ = [delivery async for delivery in source.deliveries(batch_size=11)]

    asyncio.run(exercise())
    with pytest.raises(ValueError):
        SqsDeliverySource(client, queue_url="", executor=ImmediateExecutor())
    with pytest.raises(ValueError):
        SqsDeliverySource(
            client, queue_url="queue", executor=ImmediateExecutor(), max_concurrency=0
        )
    with pytest.raises(ValueError):
        SqsDeliverySource(
            client,
            queue_url="queue",
            executor=ImmediateExecutor(),
            wait_time=timedelta(seconds=21),
        )
    with pytest.raises(ValueError):
        SqsDeliverySource(
            client,
            queue_url="queue",
            executor=ImmediateExecutor(),
            visibility_timeout=timedelta(hours=13),
        )

    async def visibility() -> None:
        source = SqsDeliverySource(
            client,
            queue_url="queue",
            executor=ImmediateExecutor(),
            wait_time=timedelta(0),
            visibility_timeout=timedelta(seconds=10),
        )
        assert len([item async for item in source.deliveries(batch_size=1)]) == 1

    asyncio.run(visibility())


class NatsRaw:
    def __init__(self) -> None:
        self.data = JsonMessageCodec().encode(message())
        self.actions: list[str] = []

    async def ack(self) -> None:
        self.actions.append("ack")

    async def nak(self, *, delay: float | None = None) -> None:
        self.actions.append("nak")

    async def term(self) -> None:
        self.actions.append("term")


def test_nats_delivery_pull_and_publication_edges() -> None:
    async def delivery_edges() -> None:
        raw = NatsRaw()
        delivery = NatsDelivery(raw)
        await delivery.ack()
        with pytest.raises(RuntimeError):
            await delivery.terminate()
        raw = NatsRaw()
        await NatsDelivery(raw).terminate()
        raw = NatsRaw()
        await NatsDelivery(raw).retry()
        raw = NatsRaw()
        with pytest.raises(ValueError):
            await NatsDelivery(raw).retry(delay=timedelta(seconds=-1))

        class Subscription:
            async def fetch(self, batch: int, *, timeout: float) -> list[NatsRaw]:
                return [NatsRaw()]

        source = NatsPullDeliverySource(Subscription())
        assert len([item async for item in source.deliveries(batch_size=1)]) == 1
        with pytest.raises(ValueError):
            _ = [item async for item in source.deliveries(batch_size=0)]

    asyncio.run(delivery_edges())
    with pytest.raises(ValueError):
        NatsPullDeliverySource(object(), fetch_timeout=timedelta(0))

    class NoAck:
        async def publish(self, subject: str, payload: bytes, *, headers: object) -> object:
            return object()

    result = asyncio.run(
        NatsJetStreamPublisher(NoAck()).publish(destination="events", message=message())
    )
    assert isinstance(result, PublicationRetryable)
    assert isinstance(
        asyncio.run(NatsJetStreamPublisher(NoAck()).publish(destination="", message=message())),
        PublicationRejected,
    )
    assert isinstance(classify_nats_publication_error(TimeoutError()), PublicationRetryable)
    assert isinstance(classify_nats_publication_error(ValueError()), PublicationRejected)

    class BrokenJetStream:
        async def publish(self, subject: str, payload: bytes, *, headers: object) -> object:
            raise OSError("down")

    assert isinstance(
        asyncio.run(
            NatsJetStreamPublisher(BrokenJetStream()).publish(
                destination="events", message=message()
            )
        ),
        PublicationRetryable,
    )


def test_nats_topology_validation_and_update_plan() -> None:
    with pytest.raises(ValueError):
        StreamSpec("", ("events",), timedelta(seconds=1))
    stream = StreamSpec("EVENTS", ("events.>",), timedelta(hours=1))
    consumer = ConsumerSpec("EVENTS", "consumer", "events.one", timedelta(seconds=1), 2)
    with pytest.raises(ValueError):
        NatsTopology((stream, stream), ())
    with pytest.raises(ValueError):
        NatsTopology((), (consumer,))

    class Backend:
        async def stream(self, name: str) -> StreamSpec:
            return StreamSpec("EVENTS", ("old.>",), timedelta(hours=1))

        async def consumer(self, stream: str, durable: str) -> ConsumerSpec:
            return ConsumerSpec("EVENTS", "consumer", "old.one", timedelta(seconds=1), 2)

        async def apply(self, operation: object) -> None:
            raise AssertionError

    plan = asyncio.run(plan_nats_topology(NatsTopology((stream,), (consumer,)), Backend()))
    assert len(plan.operations) == 2


def test_operations_validation_failure_probe_and_trace_helpers() -> None:
    with pytest.raises(ValueError):
        ProbeResult("BAD", True)
    with pytest.raises(ValueError):
        Metric("bad name", 1)
    with pytest.raises(ValueError):
        OperationalEvent("event", datetime(2026, 1, 1), {})
    with pytest.raises(ValueError):
        ReadinessPolicy(RuntimeRole.API, (), timedelta(0))

    class Broken:
        name = "broken"

        async def check(self) -> ProbeResult:
            raise OSError("private\nreason")

    report = asyncio.run(evaluate_readiness(ReadinessPolicy(RuntimeRole.CONSUMER, (Broken(),))))
    assert not report.ready and report.probes[0].detail == "OSError: private reason"
    with pytest.raises(ValueError):
        safe_failure_reason(ValueError(), max_chars=0)
    trace = TraceContext("1" * 32, "2" * 16, 1)
    headers = trace_transport_headers(trace)
    assert trace_from_transport_headers(headers) == trace
    assert trace_transport_headers(None) == {}


def test_process_validation_capacity_and_lookup_edges() -> None:
    with pytest.raises(ValueError):
        ProcessKey("", "id")
    effect = ProcessEffect("effect", ProcessEffectKind.COMMAND, "command", {})
    with pytest.raises(ValueError):
        ProcessDecision(
            {}, schedule=(TimerSchedule("timer", NOW, effect),), cancel_timers=("timer",)
        )
    with pytest.raises(ValueError):
        ProcessManagerRegistry(
            [ProcessManagerBinding("same", lambda state, value: ProcessDecision({}))] * 2
        )
    with pytest.raises(LookupError):
        ProcessManagerRegistry([]).require("missing")
    store = InMemoryProcessManagerStore(capacity=1)
    assert store.apply(
        ProcessKey("process", "one"),
        expected_version=0,
        decision=ProcessDecision({}, ProcessStatus.RUNNING),
        at=NOW,
    )
    with pytest.raises(OverflowError):
        store.apply(
            ProcessKey("process", "two"),
            expected_version=0,
            decision=ProcessDecision({}),
            at=NOW,
        )


def test_job_validation_terminal_and_schedule_policy_edges() -> None:
    with pytest.raises(ValueError):
        Job("", "task", {}, NOW)
    with pytest.raises(ValueError):
        JobSchedule("one", "task", {}, NOW, ScheduleKind.FIXED_INTERVAL)
    with pytest.raises(ValueError):
        JobSchedule("one", "task", {}, NOW, ScheduleKind.CONSUMER_POLICY)
    store = InMemoryJobStore()
    job = Job("one", "task", {}, NOW, max_attempts=1)
    assert store.schedule(job)
    assert isinstance(store.schedule(job), JobDuplicate)
    claim = store.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=1))[0]
    transition = store.retry(claim, reason=" failed\nnow ", run_at=NOW)
    assert transition.reason == "failed now"  # type: ignore[union-attr]
    assert store.get("one").state is JobState.TERMINAL  # type: ignore[union-attr]

    policy_store = InMemoryJobStore()
    schedule = JobSchedule(
        "policy",
        "task",
        {},
        NOW,
        ScheduleKind.CONSUMER_POLICY,
        policy="next",
    )
    policy_store.add_schedule(schedule)
    with pytest.raises(LookupError):
        policy_store.materialize(now=NOW, limit=1)
    policy_store = InMemoryJobStore()
    policy_store.add_schedule(schedule)
    assert policy_store.materialize(now=NOW, limit=1, policies={"next": lambda current: None}) == 1


def test_event_store_and_projection_edge_contracts() -> None:
    with pytest.raises(ValueError):
        StreamId("", "id")
    with pytest.raises(ValueError):
        NewEvent("", "type", {}, NOW)
    events = InMemoryEventStore(capacity=2, max_page_size=1)
    stream = StreamId("example", "one")
    duplicate = NewEvent("same", "event", {}, NOW)
    assert isinstance(
        events.append(stream, expected_version=0, events=[duplicate, duplicate]), DuplicateEventId
    )
    events.append(stream, expected_version=0, events=[duplicate])
    assert isinstance(
        events.append(StreamId("example", "two"), expected_version=0, events=[duplicate]),
        DuplicateEventId,
    )
    with pytest.raises(ValueError):
        events.read_all(after_position=0, limit=2)
    with pytest.raises(ValueError):
        events.save_snapshot(Snapshot(stream, 2, {}, NOW), expected_version=None)

    projections = InMemoryProjectionStore(capacity=2)
    key = ProjectionKey("projection", "all")
    first = projections.apply(
        key,
        expected_checkpoint=0,
        projection_version=1,
        state={},
        events=(),
    )
    assert first
    assert isinstance(
        projections.apply(
            key,
            expected_checkpoint=1,
            projection_version=1,
            state={},
            events=(),
        ),
        object,
    )
    assert isinstance(
        projections.apply(
            key,
            expected_checkpoint=0,
            projection_version=2,
            state={},
            events=(),
        ),
        ProjectionVersionMismatch,
    )
    with pytest.raises(ValueError):
        ProjectionDefinition(0, {}, lambda state, event: state)
    with pytest.raises(ValueError):
        RebuildRun("", key, 1, 0, 1, 0, {})


def test_process_timer_conflict_fencing_and_runtime_edges() -> None:
    effect = ProcessEffect("effect", ProcessEffectKind.COMMAND, "command", {})
    with pytest.raises(ValueError):
        ProcessEffect("", ProcessEffectKind.COMMAND, "command", {})
    with pytest.raises(ValueError):
        TimerSchedule("", NOW, effect)
    with pytest.raises(ValueError):
        TimerSchedule("timer", datetime(2026, 1, 1), effect)
    with pytest.raises(ValueError):
        ProcessState(ProcessKey("process", "one"), 0, ProcessStatus.RUNNING, {}, NOW)
    with pytest.raises(ValueError):
        ProcessDecision({}, effects=(effect, effect))
    timer = TimerSchedule("timer", NOW + timedelta(seconds=5), effect)
    with pytest.raises(ValueError):
        ProcessDecision({}, schedule=(timer, timer))
    with pytest.raises(ValueError):
        ProcessManagerBinding("", lambda state, value: ProcessDecision({}))
    with pytest.raises(ValueError):
        InMemoryProcessManagerStore(capacity=0)

    key = ProcessKey("process", "one")
    store = InMemoryProcessManagerStore(capacity=10)
    first = store.apply(
        key,
        expected_version=0,
        decision=ProcessDecision({}, effects=(effect,), schedule=(timer,)),
        at=NOW,
    )
    assert first
    with pytest.raises(ValueError, match="timer"):
        store.apply(
            key,
            expected_version=1,
            decision=ProcessDecision({}, schedule=(timer,)),
            at=NOW,
        )
    with pytest.raises(ValueError, match="effect"):
        store.apply(
            key,
            expected_version=1,
            decision=ProcessDecision({}, effects=(effect,)),
            at=NOW,
        )
    with pytest.raises(ValueError, match="expected"):
        store.apply(key, expected_version=-1, decision=ProcessDecision({}), at=NOW)
    assert store.claim_timers(now=NOW, limit=1, claim_ttl=timedelta(seconds=1)) == []
    claim = store.claim_timers(
        now=NOW + timedelta(seconds=5), limit=1, claim_ttl=timedelta(seconds=1)
    )[0]
    assert (
        store.claim_timers(now=NOW + timedelta(seconds=5), limit=1, claim_ttl=timedelta(seconds=1))
        == []
    )
    forged = ProcessTimerClaim(
        ProcessTimer(key, "timer", timer.due_at, effect, claim.timer.fencing_token),
        "wrong",
        claim.claimed_until,
    )
    assert not store.complete_timer(forged)
    with pytest.raises(ValueError):
        store.claim_timers(now=NOW, limit=0, claim_ttl=timedelta(seconds=1))
    with pytest.raises(ValueError):
        store.claim_timers(now=NOW, limit=1, claim_ttl=timedelta(0))
    with pytest.raises(ValueError):
        store.claim_timers(now=datetime(2026, 1, 1), limit=1, claim_ttl=timedelta(seconds=1))

    registry = ProcessManagerRegistry(
        [ProcessManagerBinding("process", lambda state, value: ProcessDecision(value))]
    )
    result = ProcessManagerRuntime(store, registry, now=lambda: NOW).handle(key, {"step": 2})
    assert isinstance(result, ProcessApplied)
    assert isinstance(
        store.apply(key, expected_version=0, decision=ProcessDecision({}), at=NOW),
        StaleProcessVersion,
    )


def test_job_claim_schedule_and_runner_failure_edges() -> None:
    with pytest.raises(ValueError):
        Job("job", "task", {}, NOW, max_attempts=0)
    with pytest.raises(ValueError):
        Job("job", "task", {}, datetime(2026, 1, 1))
    base = Job("job", "task", {}, NOW)
    with pytest.raises(ValueError):
        JobClaim(base, "", NOW, 1)
    with pytest.raises(ValueError):
        JobSchedule(
            "schedule",
            "task",
            {},
            NOW,
            ScheduleKind.ONE_SHOT,
            interval=timedelta(seconds=1),
        )
    with pytest.raises(ValueError):
        InMemoryJobStore(capacity=0)

    store = InMemoryJobStore(capacity=2)
    assert store.get("missing") is None
    store.schedule(base)
    claim = store.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=1))[0]
    assert store.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=1)) == []
    assert store.succeed(claim, decision=Decision(), at=NOW)
    assert isinstance(store.retry(claim, reason="late", run_at=NOW), StaleJobClaim)
    assert isinstance(store.terminate(claim, reason="late", at=NOW), StaleJobClaim)
    with pytest.raises(OverflowError):
        store.schedule(Job("other", "task", {}, NOW))
    with pytest.raises(ValueError):
        store.claim(now=NOW, limit=0, claim_ttl=timedelta(seconds=1))
    with pytest.raises(ValueError):
        store.claim(now=NOW, limit=1, claim_ttl=timedelta(0))

    terminal_store = InMemoryJobStore()
    terminal_store.schedule(Job("terminal", "task", {}, NOW))
    terminal_claim = terminal_store.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=1))[0]
    assert isinstance(
        terminal_store.terminate(terminal_claim, reason=" permanent ", at=NOW),
        JobTerminated,
    )
    with pytest.raises(ValueError, match="reason"):
        terminal_store.retry(replace(terminal_claim, claim_id="stale"), reason=" ", run_at=NOW)

    schedules = InMemoryJobStore()
    one_shot = JobSchedule("once", "task", {}, NOW, ScheduleKind.ONE_SHOT)
    assert schedules.add_schedule(one_shot)
    assert not schedules.add_schedule(one_shot)
    with pytest.raises(ValueError):
        schedules.materialize(now=NOW, limit=0)
    assert schedules.materialize(now=NOW, limit=1) == 1
    assert schedules.materialize(now=NOW + timedelta(days=1), limit=1) == 0

    policy = JobSchedule(
        "policy-advance",
        "task",
        {},
        NOW,
        ScheduleKind.CONSUMER_POLICY,
        policy="next",
    )
    policy_store = InMemoryJobStore()
    policy_store.add_schedule(policy)
    with pytest.raises(ValueError, match="advance"):
        policy_store.materialize(now=NOW, limit=1, policies={"next": lambda item: NOW})
    policy_store = InMemoryJobStore()
    policy_store.add_schedule(policy)
    with pytest.raises(ValueError, match="UTC"):
        policy_store.materialize(
            now=NOW,
            limit=1,
            policies={"next": lambda item: datetime(2026, 1, 2)},
        )

    runner_store = InMemoryJobStore()
    runner_store.schedule(Job("permanent", "permanent", {}, NOW))
    runner_store.schedule(Job("missing", "missing", {}, NOW, max_attempts=1))

    def permanent(job: Job) -> Decision:
        raise PermanentJobError("invalid")

    summary = JobRunner(runner_store, {"permanent": permanent}, clock=FixedClock()).run_once(
        limit=2, claim_ttl=timedelta(seconds=1)
    )
    assert summary.terminated == 2
    assert isinstance(runner_store.get("permanent"), Job)
    assert isinstance(
        runner_store.retry(JobClaim(base, "missing", NOW, 1), reason="retry", run_at=NOW),
        StaleJobClaim,
    )
    assert isinstance(JobRetried(NOW), JobRetried)


def test_event_stream_snapshot_and_projection_rebuild_edges() -> None:
    with pytest.raises(ValueError):
        NewEvent("event", "type", {}, datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        NewEvent("event", "type", {}, NOW, {str(index): index for index in range(33)})
    stream = StreamId("example", "one")
    event = NewEvent("one", "event", {}, NOW)
    with pytest.raises(ValueError):
        StoredEvent(stream, 0, 1, event)
    with pytest.raises(ValueError):
        Snapshot(stream, 0, {}, NOW)
    with pytest.raises(ValueError):
        InMemoryEventStore(capacity=0)

    events = InMemoryEventStore(capacity=2, max_page_size=2)
    with pytest.raises(ValueError):
        events.append(stream, expected_version=-1, events=(event,))
    with pytest.raises(ValueError):
        events.append(stream, expected_version=0, events=())
    committed = events.append(stream, expected_version=0, events=(event,))
    assert isinstance(committed, AppendCommitted)
    assert isinstance(
        events.append(stream, expected_version=0, events=(NewEvent("two", "event", {}, NOW),)),
        WrongExpectedVersion,
    )
    events.append(stream, expected_version=1, events=(NewEvent("two", "event", {}, NOW),))
    with pytest.raises(OverflowError):
        events.append(stream, expected_version=2, events=(NewEvent("three", "event", {}, NOW),))
    assert events.read_stream(StreamId("example", "empty"), after_version=0, limit=1).complete
    with pytest.raises(ValueError):
        events.read_stream(stream, after_version=-1, limit=1)
    with pytest.raises(ValueError):
        events.read_all(after_position=0, limit=True)
    snapshot = Snapshot(stream, 1, {}, NOW)
    assert events.save_snapshot(snapshot, expected_version=None)
    assert not events.save_snapshot(snapshot, expected_version=None)

    with pytest.raises(ValueError):
        ProjectionKey("", "all")
    with pytest.raises(ValueError):
        ProjectionState(ProjectionKey("projection", "all"), 0, 0, {})
    with pytest.raises(ValueError):
        InMemoryProjectionStore(capacity=0)
    projections = InMemoryProjectionStore(capacity=2)
    key = ProjectionKey("projection", "all")
    with pytest.raises(ValueError):
        projections.apply(key, expected_checkpoint=0, projection_version=0, state={}, events=())
    stored = committed.events[0]
    with pytest.raises(ValueError, match="ordered"):
        projections.apply(
            key,
            expected_checkpoint=0,
            projection_version=1,
            state={},
            events=(replace(stored, global_position=2), stored),
        )
    assert isinstance(
        projections.apply(
            key,
            expected_checkpoint=1,
            projection_version=1,
            state={},
            events=(),
        ),
        StaleProjectionCheckpoint,
    )
    applied = projections.apply(
        key,
        expected_checkpoint=0,
        projection_version=1,
        state={},
        events=(stored,),
    )
    assert isinstance(applied, ProjectionApplied)

    run = RebuildRun("run", key, 1, 2, 1, 0, {})
    assert projections.begin_rebuild(run)
    assert not projections.begin_rebuild(run)
    assert projections.load_rebuild("missing") is None
    assert (
        projections.advance_rebuild(
            "missing", expected_position=0, state={}, next_position=0, complete=False
        )
        is None
    )
    assert (
        projections.advance_rebuild(
            "run", expected_position=1, state={}, next_position=1, complete=False
        )
        is None
    )
    advanced = projections.advance_rebuild(
        "run", expected_position=0, state={}, next_position=2, complete=True
    )
    assert advanced is not None and advanced.status is RebuildStatus.COMPLETED
    assert (
        projections.advance_rebuild(
            "run", expected_position=2, state={}, next_position=2, complete=True
        )
        is None
    )

    runtime = ProjectionRuntime(projections, events)
    definition = ProjectionDefinition(1, {}, lambda state, item: state)
    assert runtime.resume_rebuild("run", definition) is advanced
    with pytest.raises(LookupError):
        runtime.resume_rebuild("unknown", definition)
    with pytest.raises(ValueError, match="version"):
        runtime.resume_rebuild("run", ProjectionDefinition(2, {}, lambda state, item: state))


def test_fastapi_and_faststream_edge_contracts() -> None:
    with pytest.raises(ValueError):
        IdempotencyKey("")
    with pytest.raises(ValueError):
        IdempotencyKey("x" * 256)
    with pytest.raises(ValueError):
        idempotency_key_from_headers({})

    actions: list[str] = []

    async def acknowledge() -> None:
        actions.append("ack")

    async def retry(delay: timedelta | None) -> None:
        actions.append("retry")

    async def terminate() -> None:
        actions.append("term")

    async def exercise() -> None:
        delivery = FastStreamNatsDelivery(
            message(),
            acknowledge=acknowledge,
            retry_delivery=retry,
            terminate_delivery=terminate,
        )
        await delivery.terminate()
        with pytest.raises(RuntimeError):
            await delivery.ack()

    asyncio.run(exercise())
    assert actions == ["term"]
