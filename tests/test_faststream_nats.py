import asyncio
from datetime import UTC, datetime

from pytitect.aio import (
    AsyncConsumer,
    InMemoryAsyncUnitOfWorkFactory,
    InMemoryRejectedDeliveryStore,
)
from pytitect.application import Decision
from pytitect.faststream_nats import FastStreamNatsAdapter
from pytitect.messaging import DeliveryAck, JsonMessageCodec, Message


class RawMessage:
    def __init__(self) -> None:
        self.actions: list[str] = []

    async def ack(self) -> None:
        self.actions.append("ack")

    async def nak(self, *, delay: float | None = None) -> None:
        self.actions.append(f"nak:{delay}")

    async def term(self) -> None:
        self.actions.append("term")


def test_adapter_returns_unregistered_handler_and_delegates_to_runtime() -> None:
    message = Message(
        id="message-1",
        source="urn:example:test",
        type="example.changed.v1",
        subject="example/1",
        time=datetime(2026, 9, 3, tzinfo=UTC),
        dataschema="urn:example:changed:1",
        data=None,
    )
    consumer = AsyncConsumer(
        consumer="consumer-a",
        namespace="tests",
        handler=lambda message, context: Decision(result={"handled": message.id}),
        unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
        quarantine=InMemoryRejectedDeliveryStore(),
    )
    adapter = FastStreamNatsAdapter(consumer)
    raw = RawMessage()
    result = asyncio.run(adapter.subscriber_handler()(JsonMessageCodec().encode(message), raw))
    assert result == DeliveryAck()
    assert raw.actions == ["ack"]
