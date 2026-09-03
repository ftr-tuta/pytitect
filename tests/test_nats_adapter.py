import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pytitect.messaging import Message, PublicationConfirmed
from pytitect.nats import (
    ConsumerSpec,
    NatsDelivery,
    NatsJetStreamPublisher,
    NatsTopology,
    StreamSpec,
    TopologyAction,
    apply_nats_topology,
    plan_nats_topology,
)


def event() -> Message:
    return Message(
        id="message-1",
        source="urn:example:test",
        type="example.changed.v1",
        subject="example/1",
        time=datetime(2026, 9, 3, tzinfo=UTC),
        dataschema="urn:example:changed:1",
        data={"ok": True},
    )


@dataclass
class Ack:
    stream: str = "EVENTS"
    seq: int = 3


class JetStream:
    def __init__(self) -> None:
        self.call: tuple[str, bytes, dict[str, str]] | None = None

    async def publish(self, subject: str, payload: bytes, *, headers: Any) -> Ack:
        self.call = (subject, payload, dict(headers))
        return Ack()


def test_publisher_sets_stable_deduplication_id_and_requires_ack() -> None:
    jetstream = JetStream()
    result = asyncio.run(
        NatsJetStreamPublisher(jetstream).publish(destination="events.changed", message=event())
    )
    assert isinstance(result, PublicationConfirmed)
    assert result.transport_id == "EVENTS:3"
    assert jetstream.call is not None
    assert jetstream.call[2]["Nats-Msg-Id"] == "message-1"


class RawMessage:
    def __init__(self, payload: bytes) -> None:
        self.data = payload
        self.actions: list[str] = []

    async def ack(self) -> None:
        self.actions.append("ack")

    async def nak(self, *, delay: float | None = None) -> None:
        self.actions.append(f"nak:{delay}")

    async def term(self) -> None:
        self.actions.append("term")


def test_delivery_maps_ack_nak_and_term_once() -> None:
    from pytitect.messaging import JsonMessageCodec

    raw = RawMessage(JsonMessageCodec().encode(event()))
    delivery = NatsDelivery(raw)
    asyncio.run(delivery.retry(delay=timedelta(seconds=2)))
    assert raw.actions == ["nak:2.0"]


class Backend:
    def __init__(self) -> None:
        self.operations: list[object] = []

    async def stream(self, name: str) -> None:
        return None

    async def consumer(self, stream: str, durable: str) -> None:
        return None

    async def apply(self, operation: object) -> None:
        self.operations.append(operation)


def test_topology_is_planned_then_explicitly_applied() -> None:
    backend = Backend()
    topology = NatsTopology(
        streams=(StreamSpec("EVENTS", ("events.>",), timedelta(days=1)),),
        consumers=(
            ConsumerSpec("EVENTS", "consumer-a", "events.changed", timedelta(seconds=30), 10),
        ),
    )

    async def exercise() -> object:
        plan = await plan_nats_topology(topology, backend)
        assert [operation.action for operation in plan.operations] == [
            TopologyAction.ADD_STREAM,
            TopologyAction.ADD_CONSUMER,
        ]
        assert backend.operations == []
        await apply_nats_topology(plan, backend)
        return plan

    asyncio.run(exercise())
    assert len(backend.operations) == 2
