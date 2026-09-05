"""Optional synchronous, nonblocking observation of finite runtime facts."""

from __future__ import annotations

from contextlib import suppress
from datetime import datetime
from enum import StrEnum

from pytitect.operations import Metric, MetricSink, OperationalEvent, OperationalSink, RuntimeRole


class RuntimeFact(StrEnum):
    ADMITTED = "admitted"
    BUSY = "busy"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    DELIVERED = "delivered"
    RETRIED = "retried"
    DEFERRED = "deferred"
    STALE = "stale"
    FAILED = "failed"
    UNCERTAIN = "uncertain"
    ACKNOWLEDGED = "acknowledged"
    TERMINATED = "terminated"
    CANCELLED = "cancelled"
    STOPPED = "stopped"


class RuntimeObservation:
    """No buffering. Sinks must return promptly; exceptions cannot affect business work."""

    def __init__(
        self,
        role: RuntimeRole,
        sink: OperationalSink | None = None,
        metrics: MetricSink | None = None,
    ) -> None:
        self._role = role
        self._sink = sink
        self._metrics = metrics

    def lag(self, occurred_at: datetime, at: datetime) -> None:
        """Observe wall-clock message age; skew is clamped and never grants authority."""
        if self._metrics is None:
            return
        with suppress(Exception):
            self._metrics.record(
                Metric(
                    "runtime.message_age_seconds",
                    max(0.0, (at - occurred_at).total_seconds()),
                    {"role": self._role.value},
                )
            )

    def emit(self, fact: RuntimeFact, at: datetime) -> None:
        if self._sink is None:
            return
        with suppress(Exception):
            self._sink.emit(
                OperationalEvent(
                    "runtime.transition",
                    at,
                    {"role": self._role.value, "outcome": fact.value},
                )
            )
