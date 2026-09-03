"""Explicit framework-neutral command, query, and pure decision contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from pytitect.core import JsonValue, validate_json
from pytitect.messaging import Message


@dataclass(frozen=True, slots=True)
class Command:
    name: str
    payload: JsonValue

    def __post_init__(self) -> None:
        _named_json(self.name, self.payload, "command")


@dataclass(frozen=True, slots=True)
class Query:
    name: str
    payload: JsonValue

    def __post_init__(self) -> None:
        _named_json(self.name, self.payload, "query")


@dataclass(frozen=True, slots=True)
class DomainEvent:
    name: str
    payload: JsonValue

    def __post_init__(self) -> None:
        _named_json(self.name, self.payload, "domain event")


@dataclass(frozen=True, slots=True)
class IntegrationEvent:
    message: Message


@dataclass(frozen=True, slots=True)
class Task:
    name: str
    payload: JsonValue

    def __post_init__(self) -> None:
        _named_json(self.name, self.payload, "task")


@dataclass(frozen=True, slots=True)
class Decision:
    """A pure result; runtimes decide how and where its effects are persisted."""

    result: JsonValue = None
    domain_events: tuple[DomainEvent, ...] = ()
    integration_events: tuple[IntegrationEvent, ...] = ()
    commands: tuple[Command, ...] = ()
    tasks: tuple[Task, ...] = ()

    def __post_init__(self) -> None:
        validate_json(self.result)


@dataclass(frozen=True, slots=True)
class HandlingContext:
    message_id: str
    correlation_id: str | None = None
    causation_id: str | None = None
    attributes: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.message_id:
            raise ValueError("handling context requires a message ID")
        copied = dict(self.attributes)
        validate_json(copied)
        object.__setattr__(self, "attributes", MappingProxyType(copied))


class CommandHandler(Protocol):
    def __call__(self, command: Command, context: HandlingContext) -> Decision: ...


class QueryHandler(Protocol):
    def __call__(self, query: Query, context: HandlingContext) -> JsonValue: ...


@dataclass(frozen=True, slots=True)
class CommandBinding:
    name: str
    handler: CommandHandler

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("command binding name must not be empty")


@dataclass(frozen=True, slots=True)
class QueryBinding:
    name: str
    handler: QueryHandler

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("query binding name must not be empty")


class CommandRegistry:
    """Immutable consumer-owned command handler registry."""

    def __init__(self, bindings: Iterable[CommandBinding]) -> None:
        handlers: dict[str, CommandHandler] = {}
        for binding in bindings:
            if binding.name in handlers:
                raise ValueError(f"duplicate command binding: {binding.name}")
            handlers[binding.name] = binding.handler
        self._handlers: Mapping[str, CommandHandler] = MappingProxyType(handlers)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    def handler_for(self, name: str) -> CommandHandler:
        try:
            return self._handlers[name]
        except KeyError as exc:
            raise LookupError(f"no command handler for {name!r}") from exc

    def dispatch(self, command: Command, context: HandlingContext) -> Decision:
        return self.handler_for(command.name)(command, context)


class QueryRegistry:
    """Immutable consumer-owned query handler registry."""

    def __init__(self, bindings: Iterable[QueryBinding]) -> None:
        handlers: dict[str, QueryHandler] = {}
        for binding in bindings:
            if binding.name in handlers:
                raise ValueError(f"duplicate query binding: {binding.name}")
            handlers[binding.name] = binding.handler
        self._handlers: Mapping[str, QueryHandler] = MappingProxyType(handlers)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    def handler_for(self, name: str) -> QueryHandler:
        try:
            return self._handlers[name]
        except KeyError as exc:
            raise LookupError(f"no query handler for {name!r}") from exc

    def dispatch(self, query: Query, context: HandlingContext) -> JsonValue:
        result = self.handler_for(query.name)(query, context)
        validate_json(result)
        return result


def _named_json(name: str, payload: JsonValue, kind: str) -> None:
    if not name or name != name.strip():
        raise ValueError(f"{kind} name must be non-empty and trimmed")
    validate_json(payload)


__all__ = [
    "Command",
    "CommandBinding",
    "CommandHandler",
    "CommandRegistry",
    "Decision",
    "DomainEvent",
    "HandlingContext",
    "IntegrationEvent",
    "Query",
    "QueryBinding",
    "QueryHandler",
    "QueryRegistry",
    "Task",
]
