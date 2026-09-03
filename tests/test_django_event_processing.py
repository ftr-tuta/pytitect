import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from pytitect.application import HandlingContext
from pytitect.django.event_processing import (
    DjangoDeliveryCommitted,
    DjangoDeliveryRetryable,
    DjangoTransactionalConsumer,
)
from pytitect.messaging import Message


class DirectBridge:
    async def run[ResultT](self, operation: Callable[[], ResultT]) -> ResultT:
        return operation()


class DirectTransaction:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def run[ResultT](self, operation: Callable[[], ResultT]) -> ResultT:
        self.events.append("transaction:begin")
        result = operation()
        self.events.append("transaction:commit")
        return result


class Delivery:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self._message = Message(
            id="message-1",
            source="urn:example:test",
            type="example.changed.v1",
            subject="example/1",
            time=datetime(2026, 9, 3, tzinfo=UTC),
            dataschema="urn:example:changed:1",
            data=None,
        )

    @property
    def message(self) -> Message:
        return self._message

    async def ack(self) -> None:
        self.events.append("ack")

    async def retry(self, *, delay: timedelta | None = None) -> None:
        self.events.append(f"retry:{delay}")

    async def terminate(self) -> None:
        self.events.append("term")


def test_django_consumer_settles_only_after_transaction_commit() -> None:
    events: list[str] = []

    def handler(message: Message, context: HandlingContext) -> DjangoDeliveryCommitted:
        events.append("handle")
        return DjangoDeliveryCommitted()

    consumer = DjangoTransactionalConsumer(
        handler, transaction=DirectTransaction(events), bridge=DirectBridge()
    )
    asyncio.run(consumer.process(Delivery(events)))
    assert events == ["transaction:begin", "handle", "transaction:commit", "ack"]


def test_django_consumer_maps_retry_outside_transaction() -> None:
    events: list[str] = []
    consumer = DjangoTransactionalConsumer(
        lambda message, context: DjangoDeliveryRetryable(timedelta(seconds=3)),
        transaction=DirectTransaction(events),
        bridge=DirectBridge(),
    )
    asyncio.run(consumer.process(Delivery(events)))
    assert events[-2:] == ["transaction:commit", "retry:0:00:03"]
