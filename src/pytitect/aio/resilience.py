"""Preview monotonic deadlines and finite, instance-owned retry allowances.

Budgets coordinate one event loop in one process. They are neither distributed
limits nor automatically replenished quotas. Callers own the budget lifetime.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from pytitect.outbox import RetryPolicy


class SettlementResult(StrEnum):
    APPLIED = "applied"
    STALE = "stale"
    DEFERRED = "deferred"

    def __bool__(self) -> bool:
        return self is not SettlementResult.STALE


@dataclass(frozen=True, slots=True)
class Deadline:
    expires_at: float
    monotonic: Callable[[], float] = time.monotonic

    @classmethod
    def after(
        cls, duration: timedelta, *, monotonic: Callable[[], float] = time.monotonic
    ) -> Deadline:
        if duration <= timedelta(0):
            raise ValueError("deadline duration must be positive")
        return cls(monotonic() + duration.total_seconds(), monotonic)

    @property
    def remaining(self) -> float:
        return max(0.0, self.expires_at - self.monotonic())


class RetryBudget:
    """Finite retry count shared explicitly by compositions on one event loop."""

    def __init__(self, allowance: int) -> None:
        if isinstance(allowance, bool) or not isinstance(allowance, int) or allowance < 0:
            raise ValueError("retry allowance must be a nonnegative integer")
        self._remaining = allowance

    @property
    def remaining(self) -> int:
        return self._remaining

    def take(self) -> bool:
        if not self._remaining:
            return False
        self._remaining -= 1
        return True


@dataclass(frozen=True, slots=True)
class RetryScheduled:
    delay: timedelta


@dataclass(frozen=True, slots=True)
class RetryDeferred:
    delay: timedelta
    reason: str


@dataclass(frozen=True, slots=True)
class RetryComposition:
    policy: RetryPolicy
    budget: RetryBudget
    jitter: Callable[[], float] = lambda: 1.0

    def schedule(
        self,
        attempt: int,
        *,
        now: datetime,
        deadline: Deadline,
        retry_after: datetime | None = None,
    ) -> RetryScheduled | RetryDeferred:
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise ValueError("retry timestamps must be UTC")
        fraction = self.jitter()
        if not math.isfinite(fraction) or not 0 <= fraction <= 1:
            raise ValueError("jitter must return a finite value in [0, 1]")
        delay = self.policy.delay(attempt) * fraction
        if retry_after is not None:
            if retry_after.tzinfo is None or retry_after.utcoffset() != timedelta(0):
                raise ValueError("retry_after must be UTC")
            delay = max(delay, retry_after - now)
        if attempt >= self.policy.max_attempts:
            return RetryDeferred(delay, "attempts")
        if delay.total_seconds() >= deadline.remaining:
            return RetryDeferred(delay, "deadline")
        if not self.budget.take():
            return RetryDeferred(delay, "budget")
        return RetryScheduled(delay)
