"""Preview async workflow ports and finite process-local reference adapters."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.processes import (
    InMemoryProcessManagerStore,
    ProcessApplyResult,
    ProcessDecision,
    ProcessKey,
    ProcessState,
    ProcessTimerClaim,
)


class AsyncProcessManagerStore(Protocol):
    async def load(self, key: ProcessKey) -> ProcessState | None: ...

    async def apply(
        self, key: ProcessKey, *, expected_version: int, decision: ProcessDecision, at: datetime
    ) -> ProcessApplyResult: ...

    async def claim_timers(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[ProcessTimerClaim]: ...

    async def complete_timer(self, claim: ProcessTimerClaim, *, at: datetime) -> bool: ...


class InMemoryAsyncProcessManagerStore:
    """Finite process-local reference; no durability or cross-process fencing."""

    def __init__(self, *, capacity: int = 10_000) -> None:

        self._store = InMemoryProcessManagerStore(capacity=capacity)

    async def load(self, key: ProcessKey) -> ProcessState | None:

        return self._store.load(key)

    async def apply(
        self, key: ProcessKey, *, expected_version: int, decision: ProcessDecision, at: datetime
    ) -> ProcessApplyResult:

        return self._store.apply(key, expected_version=expected_version, decision=decision, at=at)

    async def claim_timers(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[ProcessTimerClaim]:

        return self._store.claim_timers(now=now, limit=limit, claim_ttl=claim_ttl)

    async def complete_timer(self, claim: ProcessTimerClaim, *, at: datetime) -> bool:

        if at.tzinfo is None or at.utcoffset() != timedelta(0):
            raise ValueError("settlement time must be UTC")

        if claim.claimed_until <= at:
            return False

        return self._store.complete_timer(claim)
