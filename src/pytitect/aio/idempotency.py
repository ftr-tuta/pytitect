"""Preview async ports and finite process-local reference stores."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol

from pytitect.core import Clock, SystemClock
from pytitect.idempotency import (
    AbandonReservationResult,
    CompleteReservationResult,
    IdempotencyDecision,
    IdempotencyPolicy,
    IdempotencyScope,
    InMemoryIdempotencyStore,
    MarkUncertainResult,
    RenewReservationResult,
    RequestFingerprint,
    ReservationToken,
)


class AsyncIdempotencyStore[T](Protocol):
    async def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> IdempotencyDecision[T]: ...

    async def renew(
        self,
        token: ReservationToken,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> RenewReservationResult: ...

    async def complete(
        self,
        token: ReservationToken,
        value: T,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> CompleteReservationResult: ...

    async def mark_uncertain(
        self,
        token: ReservationToken,
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MarkUncertainResult: ...

    async def abandon(
        self,
        token: ReservationToken,
        *,
        now: datetime,
    ) -> AbandonReservationResult: ...


class InMemoryAsyncIdempotencyStore[T]:
    """Finite process-local reference; no cross-process coordination or durability."""

    def __init__(self, *, capacity: int = 10_000) -> None:

        self._store = InMemoryIdempotencyStore[T](capacity=capacity)

    async def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> IdempotencyDecision[T]:

        return self._store.reserve(scope, key, fingerprint, now=now, lease_ttl=lease_ttl)

    async def renew(
        self, token: ReservationToken, *, now: datetime, lease_ttl: timedelta
    ) -> RenewReservationResult:

        return self._store.renew(token, now=now, lease_ttl=lease_ttl)

    async def complete(
        self, token: ReservationToken, value: T, *, now: datetime, retention_ttl: timedelta
    ) -> CompleteReservationResult:

        return self._store.complete(token, value, now=now, retention_ttl=retention_ttl)

    async def mark_uncertain(
        self, token: ReservationToken, reason: str, *, now: datetime, retention_ttl: timedelta
    ) -> MarkUncertainResult:

        return self._store.mark_uncertain(token, reason, now=now, retention_ttl=retention_ttl)

    async def abandon(self, token: ReservationToken, *, now: datetime) -> AbandonReservationResult:

        return self._store.abandon(token, now=now)


@dataclass(frozen=True, slots=True)
class AsyncIdempotencyCoordinator[T]:
    store: AsyncIdempotencyStore[T]
    policy: IdempotencyPolicy
    clock: Clock = field(default_factory=SystemClock)

    async def begin(
        self,
        *,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
    ) -> IdempotencyDecision[T]:
        return await self.store.reserve(
            scope,
            key,
            fingerprint,
            now=self.clock.now(),
            lease_ttl=self.policy.execution_lease_ttl,
        )

    async def renew(self, token: ReservationToken) -> RenewReservationResult:
        return await self.store.renew(
            token,
            now=self.clock.now(),
            lease_ttl=self.policy.execution_lease_ttl,
        )

    async def complete(self, token: ReservationToken, value: T) -> CompleteReservationResult:
        return await self.store.complete(
            token,
            value,
            now=self.clock.now(),
            retention_ttl=self.policy.result_retention_ttl,
        )

    async def uncertain(self, token: ReservationToken, reason: str) -> MarkUncertainResult:
        return await self.store.mark_uncertain(
            token,
            reason,
            now=self.clock.now(),
            retention_ttl=self.policy.uncertainty_retention_ttl,
        )

    async def abandon(self, token: ReservationToken) -> AbandonReservationResult:
        return await self.store.abandon(token, now=self.clock.now())
