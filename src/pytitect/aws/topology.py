"""Explicit EventBridge-to-SQS topology validation, planning, and application."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AwsConsumerSpec:
    consumer: str
    queue_name: str
    event_types: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.consumer or not self.queue_name or not self.event_types:
            raise ValueError("AWS consumer identity, queue, and event types are required")
        if any(not event_type for event_type in self.event_types):
            raise ValueError("AWS consumer event types must not be empty")


@dataclass(frozen=True, slots=True)
class AwsTopology:
    event_bus_name: str
    consumers: tuple[AwsConsumerSpec, ...]

    def __post_init__(self) -> None:
        if not self.event_bus_name or not self.consumers:
            raise ValueError("a custom bus and at least one logical consumer are required")
        identities = [consumer.consumer for consumer in self.consumers]
        queues = [consumer.queue_name for consumer in self.consumers]
        if len(identities) != len(set(identities)) or len(queues) != len(set(queues)):
            raise ValueError("each logical consumer must have one unique SQS Standard queue")


class AwsTopologyAction(StrEnum):
    ADD_BUS = "add_bus"
    ADD_QUEUE = "add_queue"
    ADD_RULE_TARGET = "add_rule_target"
    UPDATE_RULE_TARGET = "update_rule_target"


@dataclass(frozen=True, slots=True)
class AwsTopologyOperation:
    action: AwsTopologyAction
    identity: str
    specification: str | AwsConsumerSpec


@dataclass(frozen=True, slots=True)
class AwsTopologyPlan:
    operations: tuple[AwsTopologyOperation, ...]


class AwsTopologyBackend(Protocol):
    async def bus_exists(self, name: str) -> bool: ...

    async def queue_exists(self, name: str) -> bool: ...

    async def binding(self, bus: str, consumer: str) -> AwsConsumerSpec | None: ...

    async def apply(self, operation: AwsTopologyOperation) -> None: ...


async def plan_aws_topology(topology: AwsTopology, backend: AwsTopologyBackend) -> AwsTopologyPlan:
    operations: list[AwsTopologyOperation] = []
    if not await backend.bus_exists(topology.event_bus_name):
        operations.append(
            AwsTopologyOperation(
                AwsTopologyAction.ADD_BUS,
                topology.event_bus_name,
                topology.event_bus_name,
            )
        )
    for consumer in topology.consumers:
        if not await backend.queue_exists(consumer.queue_name):
            operations.append(
                AwsTopologyOperation(AwsTopologyAction.ADD_QUEUE, consumer.queue_name, consumer)
            )
        current = await backend.binding(topology.event_bus_name, consumer.consumer)
        action = (
            AwsTopologyAction.ADD_RULE_TARGET
            if current is None
            else AwsTopologyAction.UPDATE_RULE_TARGET
        )
        if current != consumer:
            operations.append(AwsTopologyOperation(action, consumer.consumer, consumer))
    return AwsTopologyPlan(tuple(operations))


async def apply_aws_topology(plan: AwsTopologyPlan, backend: AwsTopologyBackend) -> int:
    for operation in plan.operations:
        await backend.apply(operation)
    return len(plan.operations)
