"""Role-specific readiness, bounded metrics, and safe operational observations."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from pytitect.core import JsonScalar
from pytitect.trace import TraceContext, trace_context_from_headers

_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")


class RuntimeRole(StrEnum):
    API = "api"
    RELAY = "relay"
    CONSUMER = "consumer"
    SCHEDULER = "scheduler"
    PROJECTION = "projection"


@dataclass(frozen=True, slots=True)
class ProbeResult:
    name: str
    ready: bool
    detail: str | None = None

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError("probe name is invalid")
        if self.detail is not None and len(self.detail) > 256:
            raise ValueError("probe detail exceeds 256 characters")


class ReadinessProbe(Protocol):
    @property
    def name(self) -> str: ...

    async def check(self) -> ProbeResult: ...


@dataclass(frozen=True, slots=True)
class ReadinessPolicy:
    role: RuntimeRole
    required: tuple[ReadinessProbe, ...]
    timeout: timedelta = timedelta(seconds=5)

    def __post_init__(self) -> None:
        if self.timeout <= timedelta(0):
            raise ValueError("readiness timeout must be positive")
        names = [probe.name for probe in self.required]
        if len(names) != len(set(names)):
            raise ValueError("readiness probe names must be unique")


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    role: RuntimeRole
    ready: bool
    probes: tuple[ProbeResult, ...]


async def evaluate_readiness(policy: ReadinessPolicy) -> ReadinessReport:
    async def check(probe: ReadinessProbe) -> ProbeResult:
        try:
            async with asyncio.timeout(policy.timeout.total_seconds()):
                result = await probe.check()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return ProbeResult(probe.name, False, safe_failure_reason(exc))
        if result.name != probe.name:
            return ProbeResult(probe.name, False, "probe identity mismatch")
        return result

    results = await asyncio.gather(*(check(probe) for probe in policy.required))
    ordered = tuple(sorted(results, key=lambda result: result.name))
    return ReadinessReport(policy.role, all(result.ready for result in ordered), ordered)


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    value: int | float
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError("metric name is invalid")
        if len(self.attributes) > 32:
            raise ValueError("metric attributes exceed 32 items")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


class MetricSink(Protocol):
    def record(self, metric: Metric) -> None: ...


@dataclass(frozen=True, slots=True)
class NullMetricSink:
    def record(self, metric: Metric) -> None:
        del metric


@dataclass(frozen=True, slots=True)
class OperationalEvent:
    name: str
    occurred_at: datetime
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError("operational event name is invalid")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() != timedelta(0):
            raise ValueError("operational event time must be timezone-aware UTC")
        if len(self.attributes) > 32:
            raise ValueError("operational event attributes exceed 32 items")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


class OperationalSink(Protocol):
    def emit(self, event: OperationalEvent) -> None: ...


def safe_failure_reason(error: BaseException, *, max_chars: int = 256) -> str:
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    text = " ".join(str(error).split()) or type(error).__name__
    return f"{type(error).__name__}: {text}"[:max_chars]


def trace_transport_headers(trace: TraceContext | None) -> Mapping[str, str]:
    """Render transport metadata without changing the closed message envelope."""

    return MappingProxyType({} if trace is None else dict(trace.to_headers()))


def trace_from_transport_headers(headers: Mapping[str, str]) -> TraceContext | None:
    return trace_context_from_headers(headers)


__all__ = [
    "Metric",
    "MetricSink",
    "NullMetricSink",
    "OperationalEvent",
    "OperationalSink",
    "ProbeResult",
    "ReadinessPolicy",
    "ReadinessProbe",
    "ReadinessReport",
    "RuntimeRole",
    "evaluate_readiness",
    "safe_failure_reason",
    "trace_from_transport_headers",
    "trace_transport_headers",
]
