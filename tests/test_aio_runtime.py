import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pytitect.aio import (
    AsyncCommandRuntime,
    AsyncConsumer,
    AsyncRelay,
    InMemoryAsyncOutboxStore,
    InMemoryAsyncUnitOfWorkFactory,
    InMemoryRejectedDeliveryStore,
    PermanentProcessingError,
    QuarantinePolicy,
)
from pytitect.application import (
    Command,
    CommandBinding,
    CommandRegistry,
    Decision,
    HandlingContext,
)
from pytitect.core import OpaqueId
from pytitect.messaging import (
    DeliveryAck,
    DeliveryTerminated,
    Message,
    PublicationConfirmed,
    Route,
    RoutingTable,
)
from pytitect.outbox import OutboxEnvelope

NOW = datetime(2026, 9, 3, tzinfo=UTC)


def event(message_id: str = "message-1") -> Message:
    return Message(
        id=message_id,
        source="urn:example:source",
        type="example.changed.v1",
        subject="example/1",
        time=NOW,
        dataschema="urn:example:changed:1",
        data={"value": 1},
    )


class Delivery:
    def __init__(self, message: Message, log: list[str]) -> None:
        self._message = message
        self.log = log

    @property
    def message(self) -> Message:
        return self._message

    async def ack(self) -> None:
        self.log.append("ack")

    async def retry(self, *, delay: timedelta | None = None) -> None:
        self.log.append(f"retry:{delay}")

    async def terminate(self) -> None:
        self.log.append("term")


class Publisher:
    def __init__(self) -> None:
        self.destinations: list[str] = []

    async def publish(self, *, destination: str, message: Message) -> PublicationConfirmed:
        self.destinations.append(destination)
        return PublicationConfirmed(f"transport:{message.id}")


def test_command_runtime_commits_decisions() -> None:
    factory = InMemoryAsyncUnitOfWorkFactory()
    registry = CommandRegistry(
        [CommandBinding("execute", lambda command, context: Decision(result=command.payload))]
    )
    result = asyncio.run(
        AsyncCommandRuntime(registry, factory).execute(
            Command("execute", {"ok": True}), HandlingContext("command-1")
        )
    )
    assert result.decision.result == {"ok": True}
    assert factory.decisions == (result.decision,)


def test_consumer_acknowledges_only_after_atomic_commit_and_suppresses_duplicate() -> None:
    factory = InMemoryAsyncUnitOfWorkFactory()
    quarantine = InMemoryRejectedDeliveryStore()
    log: list[str] = []

    async def handle(message: Message, context: HandlingContext) -> Decision:
        log.append("handled")
        return Decision(result={"id": context.message_id})

    consumer = AsyncConsumer(
        consumer="projection-a",
        namespace="tests",
        handler=handle,
        unit_of_work=factory,
        quarantine=quarantine,
    )

    async def exercise() -> None:
        assert await consumer.process(Delivery(event(), log)) == DeliveryAck()
        log.append("between")
        assert await consumer.process(Delivery(event(), log)) == DeliveryAck()

    asyncio.run(exercise())
    assert log == ["handled", "ack", "between", "ack"]
    assert len(factory.decisions) == 1


def test_permanent_failure_terminates_only_after_quarantine() -> None:
    quarantine = InMemoryRejectedDeliveryStore()

    def reject(message: Message, context: HandlingContext) -> Decision:
        raise PermanentProcessingError("private\n  failure details")

    consumer = AsyncConsumer(
        consumer="consumer-a",
        namespace="tests",
        handler=reject,
        unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
        quarantine=quarantine,
        quarantine_policy=QuarantinePolicy(retain_payload=False),
    )
    log: list[str] = []
    assert isinstance(asyncio.run(consumer.process(Delivery(event(), log))), DeliveryTerminated)
    assert log == ["term"]
    assert quarantine.items[0].payload is None
    assert quarantine.items[0].reason == "private failure details"
    assert len(quarantine.items[0].payload_sha256) == 64


def test_relay_marks_confirmed_publications_delivered() -> None:
    store = InMemoryAsyncOutboxStore[Message]()
    publisher = Publisher()

    async def exercise() -> object:
        await store.add(OutboxEnvelope(OpaqueId("one"), "ignored", event(), NOW, NOW))
        return await AsyncRelay(
            store,
            publisher,
            RoutingTable([Route("example.changed.v1", "changes")]),
        ).run_once(limit=1)

    summary = asyncio.run(exercise())
    assert summary.delivered == 1
    assert publisher.destinations == ["changes"]


def test_consumer_propagates_cancellation() -> None:
    async def cancel(message: Message, context: HandlingContext) -> Decision:
        raise asyncio.CancelledError

    consumer = AsyncConsumer(
        consumer="consumer-a",
        namespace="tests",
        handler=cancel,
        unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
        quarantine=InMemoryRejectedDeliveryStore(),
    )
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(consumer.process(Delivery(event(), [])))
