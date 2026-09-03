"""Inert NATS topology validation, planning, and explicit application."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Protocol


@dataclass(frozen=True, slots=True)
class StreamSpec:
    name: str
    subjects: tuple[str, ...]
    max_age: timedelta
    replicas: int = 1

    def __post_init__(self) -> None:
        if not self.name or not self.subjects or any(not subject for subject in self.subjects):
            raise ValueError("stream name and subjects must not be empty")
        if self.max_age <= timedelta(0) or self.replicas <= 0:
            raise ValueError("stream retention and replicas must be positive")


@dataclass(frozen=True, slots=True)
class ConsumerSpec:
    stream: str
    durable: str
    filter_subject: str
    ack_wait: timedelta
    max_deliver: int

    def __post_init__(self) -> None:
        if not self.stream or not self.durable or not self.filter_subject:
            raise ValueError("consumer identity and filter must not be empty")
        if self.ack_wait <= timedelta(0) or self.max_deliver <= 0:
            raise ValueError("consumer acknowledgement limits must be positive")


@dataclass(frozen=True, slots=True)
class NatsTopology:
    streams: tuple[StreamSpec, ...] = ()
    consumers: tuple[ConsumerSpec, ...] = ()

    def __post_init__(self) -> None:
        stream_names = [stream.name for stream in self.streams]
        if len(stream_names) != len(set(stream_names)):
            raise ValueError("stream names must be unique")
        consumer_names = [(consumer.stream, consumer.durable) for consumer in self.consumers]
        if len(consumer_names) != len(set(consumer_names)):
            raise ValueError("consumer identities must be unique")
        known = set(stream_names)
        if any(consumer.stream not in known for consumer in self.consumers):
            raise ValueError("every consumer must reference a declared stream")


class TopologyAction(StrEnum):
    ADD_STREAM = "add_stream"
    UPDATE_STREAM = "update_stream"
    ADD_CONSUMER = "add_consumer"
    UPDATE_CONSUMER = "update_consumer"


@dataclass(frozen=True, slots=True)
class TopologyOperation:
    action: TopologyAction
    identity: str
    specification: StreamSpec | ConsumerSpec


@dataclass(frozen=True, slots=True)
class NatsTopologyPlan:
    operations: tuple[TopologyOperation, ...]


class NatsTopologyBackend(Protocol):
    async def stream(self, name: str) -> StreamSpec | None: ...

    async def consumer(self, stream: str, durable: str) -> ConsumerSpec | None: ...

    async def apply(self, operation: TopologyOperation) -> None: ...


async def plan_nats_topology(
    topology: NatsTopology, backend: NatsTopologyBackend
) -> NatsTopologyPlan:
    operations: list[TopologyOperation] = []
    for stream in topology.streams:
        current_stream = await backend.stream(stream.name)
        if current_stream is None:
            operations.append(TopologyOperation(TopologyAction.ADD_STREAM, stream.name, stream))
        elif current_stream != stream:
            operations.append(TopologyOperation(TopologyAction.UPDATE_STREAM, stream.name, stream))
    for consumer in topology.consumers:
        current_consumer = await backend.consumer(consumer.stream, consumer.durable)
        identity = f"{consumer.stream}/{consumer.durable}"
        if current_consumer is None:
            operations.append(TopologyOperation(TopologyAction.ADD_CONSUMER, identity, consumer))
        elif current_consumer != consumer:
            operations.append(TopologyOperation(TopologyAction.UPDATE_CONSUMER, identity, consumer))
    return NatsTopologyPlan(tuple(operations))


async def apply_nats_topology(plan: NatsTopologyPlan, backend: NatsTopologyBackend) -> int:
    for operation in plan.operations:
        await backend.apply(operation)
    return len(plan.operations)
