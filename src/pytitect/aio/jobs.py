"""Preview async workflow ports and finite process-local reference adapters."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.application import Decision
from pytitect.jobs import (
    InMemoryJobStore,
    Job,
    JobClaim,
    JobScheduleResult,
    JobTransition,
    StaleJobClaim,
)


class AsyncJobStore(Protocol):
    async def schedule(self, job: Job) -> JobScheduleResult: ...

    async def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[JobClaim]: ...

    async def succeed(
        self, claim: JobClaim, *, decision: Decision, at: datetime
    ) -> JobTransition: ...

    async def retry(
        self, claim: JobClaim, *, reason: str, run_at: datetime, at: datetime
    ) -> JobTransition: ...

    async def terminate(self, claim: JobClaim, *, reason: str, at: datetime) -> JobTransition: ...


class InMemoryAsyncJobStore:
    """Finite process-local reference; no durability or cross-process fencing."""

    def __init__(self, *, capacity: int = 10_000) -> None:

        self._store = InMemoryJobStore(capacity=capacity)

    async def schedule(self, job: Job) -> JobScheduleResult:

        return self._store.schedule(job)

    async def claim(self, *, now: datetime, limit: int, claim_ttl: timedelta) -> Sequence[JobClaim]:

        return self._store.claim(now=now, limit=limit, claim_ttl=claim_ttl)

    async def succeed(self, claim: JobClaim, *, decision: Decision, at: datetime) -> JobTransition:

        if at.tzinfo is None or at.utcoffset() != timedelta(0):
            raise ValueError("settlement time must be UTC")

        if claim.claimed_until <= at:
            return StaleJobClaim()

        return self._store.succeed(claim, decision=decision, at=at)

    async def retry(
        self, claim: JobClaim, *, reason: str, run_at: datetime, at: datetime
    ) -> JobTransition:

        if at.tzinfo is None or at.utcoffset() != timedelta(0):
            raise ValueError("settlement time must be UTC")

        if claim.claimed_until <= at:
            return StaleJobClaim()

        return self._store.retry(claim, reason=reason, run_at=run_at)

    async def terminate(self, claim: JobClaim, *, reason: str, at: datetime) -> JobTransition:

        if at.tzinfo is None or at.utcoffset() != timedelta(0):
            raise ValueError("settlement time must be UTC")

        if claim.claimed_until <= at:
            return StaleJobClaim()

        return self._store.terminate(claim, reason=reason, at=at)
