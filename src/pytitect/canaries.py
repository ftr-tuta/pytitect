"""Consumer-triggered protocol canaries with no scheduler or external effects."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
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


@dataclass(frozen=True, slots=True)
class CanaryCrashed:
    name: str
    started_at: datetime
    finished_at: datetime
    exception_type: str
    reason: str
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CanaryTimedOut:
    name: str
    started_at: datetime
    finished_at: datetime
    reason: str
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CanarySkipped:
    name: str
    started_at: datetime
    finished_at: datetime
    reason: str
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)


type CanaryResult = CanaryPassed | CanaryFailed | CanaryCrashed | CanaryTimedOut | CanarySkipped
type CanaryProbeResult = tuple[bool, str | None, Mapping[str, JsonScalar]] | CanarySkipped
type CanaryProbe = Callable[[], CanaryProbeResult]


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
        """Run every probe once; I/O timeout enforcement remains probe-owned."""

        outcomes: list[CanaryResult] = []
        for canary in self.canaries:
            started = self.clock.now()
            attributes: Mapping[str, JsonScalar] = {}
            try:
                result = canary.probe()
                finished = self.clock.now()
                if isinstance(result, CanarySkipped):
                    attributes = dict(result.attributes)
                    outcome: CanaryResult = CanarySkipped(
                        canary.name, started, finished, result.reason, attributes
                    )
                    event_name = "canary.skipped"
                else:
                    passed, reason, attributes = result
                    copied = dict(attributes)
                    if passed:
                        outcome = CanaryPassed(canary.name, started, finished, copied)
                        event_name = "canary.passed"
                    else:
                        outcome = CanaryFailed(
                            canary.name,
                            started,
                            finished,
                            reason or "canary reported failure",
                            copied,
                        )
                        event_name = "canary.failed"
            except TimeoutError as error:
                finished = self.clock.now()
                outcome = CanaryTimedOut(
                    canary.name, started, finished, _safe_reason(error, "canary timed out")
                )
                event_name = "canary.timed_out"
            except Exception as error:
                finished = self.clock.now()
                outcome = CanaryCrashed(
                    canary.name,
                    started,
                    finished,
                    type(error).__name__,
                    _safe_reason(error, "canary crashed"),
                )
                event_name = "canary.crashed"
            with suppress(Exception):
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


def _safe_reason(error: Exception, default: str) -> str:
    return (" ".join(str(error).split()) or default)[:1_024]
