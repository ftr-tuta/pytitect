"""Explicit logical routing kept separate from event-type identity."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class Route:
    event_type: str
    destination: str

    def __post_init__(self) -> None:
        if not self.event_type or not self.destination:
            raise ValueError("route event type and destination must not be empty")


class RoutingTable:
    """Immutable event-type-to-logical-destination mapping."""

    def __init__(self, routes: Iterable[Route]) -> None:
        values: dict[str, str] = {}
        for route in routes:
            if route.event_type in values:
                raise ValueError(f"duplicate route for event type: {route.event_type}")
            values[route.event_type] = route.destination
        self._values: Mapping[str, str] = MappingProxyType(values)

    def destination_for(self, event_type: str) -> str:
        try:
            return self._values[event_type]
        except KeyError as exc:
            raise LookupError(f"no route for event type: {event_type}") from exc
