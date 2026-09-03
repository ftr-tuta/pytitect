"""Testing-only deterministic event-platform fault injection."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import StrEnum


class FaultPoint(StrEnum):
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"
    AFTER_PUBLISH_CONFIRM = "after_publish_confirm"
    BEFORE_ACK = "before_ack"
    BROKER_UNAVAILABLE = "broker_unavailable"
    DURING_SHUTDOWN = "during_shutdown"
    STALE_CLAIM = "stale_claim"


class InjectedCrash(BaseException):
    """A process-like crash signal deliberately not caught by Exception handlers."""

    def __init__(self, point: FaultPoint) -> None:
        super().__init__(point.value)
        self.point = point


@dataclass(frozen=True, slots=True)
class FaultPlan:
    points: frozenset[FaultPoint]
    repeat: bool = False

    @classmethod
    def at(cls, *points: FaultPoint, repeat: bool = False) -> FaultPlan:
        if not points:
            raise ValueError("fault plan requires at least one point")
        return cls(frozenset(points), repeat)


class FaultInjector:
    """Thread-safe, instance-local injector; it never changes global runtime state."""

    def __init__(self, plan: FaultPlan) -> None:
        self._plan = plan
        self._triggered: set[FaultPoint] = set()
        self._lock = threading.Lock()

    @property
    def triggered(self) -> frozenset[FaultPoint]:
        with self._lock:
            return frozenset(self._triggered)

    def hit(self, point: FaultPoint) -> None:
        with self._lock:
            enabled = point in self._plan.points and (
                self._plan.repeat or point not in self._triggered
            )
            if enabled:
                self._triggered.add(point)
        if enabled:
            raise InjectedCrash(point)


__all__ = ["FaultInjector", "FaultPlan", "FaultPoint", "InjectedCrash"]
