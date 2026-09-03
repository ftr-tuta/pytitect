"""Immutable event-type declarations independent from transport routing."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class MessageType:
    event_type: str
    schema: str
    version: int = 1

    def __post_init__(self) -> None:
        if not self.event_type or not self.schema:
            raise ValueError("event type and schema must not be empty")
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0:
            raise ValueError("message type version must be a positive integer")


class MessageTypeRegistry:
    """A closed declaration set created and owned by the consumer."""

    def __init__(self, declarations: Iterable[MessageType]) -> None:
        values: dict[str, MessageType] = {}
        for declaration in declarations:
            if declaration.event_type in values:
                raise ValueError(f"duplicate event type: {declaration.event_type}")
            values[declaration.event_type] = declaration
        self._values: Mapping[str, MessageType] = MappingProxyType(values)

    @property
    def event_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._values))

    def resolve(self, event_type: str) -> MessageType:
        try:
            return self._values[event_type]
        except KeyError as exc:
            raise LookupError(f"unknown event type: {event_type}") from exc

    def validate(self, message_type: str, dataschema: str) -> MessageType:
        declaration = self.resolve(message_type)
        if declaration.schema != dataschema:
            raise ValueError("message schema does not match its registered event type")
        return declaration
