"""Consumer-triggered protocol canaries with no scheduler or external effects of their own."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from pytitect.core import Clock, JsonScalar, Observer, SystemClock


@dataclass(frozen=True, slots=True)
class CanaryPassed:
    name: str
    started_at: datetime
    finished_at: datetime
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CanaryFailed:
    name: str
    started_at: datetime
    finished_at: datetime
    reason: str
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)


type CanaryResult = CanaryPassed | CanaryFailed
type CanaryProbe = Callable[[], tuple[bool, str | None, Mapping[str, JsonScalar]]]


@dataclass(frozen=True, slots=True)
class Canary:
    name: str
    probe: CanaryProbe

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("canary name must not be empty")


@dataclass(frozen=True, slots=True)
class CanarySuite:
    canaries: Sequence[Canary]
    observer: Observer
    clock: Clock = field(default_factory=SystemClock)

    def run_once(self) -> tuple[CanaryResult, ...]:
        """Run one explicit round. Scheduling and exception policy remain consumer-owned."""

        outcomes: list[CanaryResult] = []
        for canary in self.canaries:
            started = self.clock.now()
            passed, reason, attributes = canary.probe()
            finished = self.clock.now()
            if passed:
                outcome: CanaryResult = CanaryPassed(
                    canary.name, started, finished, dict(attributes)
                )
                event_name = "canary.passed"
            else:
                outcome = CanaryFailed(
                    canary.name,
                    started,
                    finished,
                    reason or "canary reported failure",
                    dict(attributes),
                )
                event_name = "canary.failed"
            self.observer.observe(
                event_name,
                {
                    "canary": canary.name,
                    "duration_ms": max(
                        0,
                        int((finished - started) / timedelta(milliseconds=1)),
                    ),
                    **attributes,
                },
            )
            outcomes.append(outcome)
        return tuple(outcomes)
