"""Lease ownership, monotonic fencing, and fenced commits."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

ResourceT = TypeVar("ResourceT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True, slots=True)
class Lease[ResourceT]:
    resource: ResourceT
    owner: str
    fencing_token: int
    expires_at: datetime

    def __post_init__(self) -> None:
        _utc(self.expires_at)
        if not self.owner or self.fencing_token <= 0:
            raise ValueError("lease owner and a positive fencing token are required")


@dataclass(frozen=True, slots=True)
class LeaseAuthority:
    owner: str
    fencing_token: int
    expires_at: datetime

    def __post_init__(self) -> None:
        _utc(self.expires_at)
        if not self.owner or self.fencing_token <= 0:
            raise ValueError("authority owner and a positive fencing token are required")


@dataclass(frozen=True, slots=True)
class LeaseAcquired[ResourceT]:
    lease: Lease[ResourceT]


@dataclass(frozen=True, slots=True)
class LeaseBusy:
    owner: str
    retry_after: datetime


@dataclass(frozen=True, slots=True)
class LeaseRenewed[ResourceT]:
    lease: Lease[ResourceT]


@dataclass(frozen=True, slots=True)
class LeaseReleased:
    pass


@dataclass(frozen=True, slots=True)
class StaleLease:
    reason: str = "lease is absent, expired, or no longer owned by this token"


type AcquireResult[ResourceT] = LeaseAcquired[ResourceT] | LeaseBusy
type RenewResult[ResourceT] = LeaseRenewed[ResourceT] | StaleLease
type ReleaseResult = LeaseReleased | StaleLease


class LeaseStore(Protocol[ResourceT]):
    def acquire(
        self, resource: ResourceT, *, owner: str, now: datetime, ttl: timedelta
    ) -> AcquireResult[ResourceT]: ...

    def renew(
        self, lease: Lease[ResourceT], *, now: datetime, ttl: timedelta
    ) -> RenewResult[ResourceT]: ...

    def release(self, lease: Lease[ResourceT], *, now: datetime) -> ReleaseResult: ...

    def authority(self, resource: ResourceT) -> int | None: ...


class InMemoryLeaseStore[ResourceT]:
    def __init__(self) -> None:
        self._leases: dict[ResourceT, Lease[ResourceT]] = {}
        self._tokens: dict[ResourceT, int] = {}
        self._lock = threading.RLock()

    def acquire(
        self, resource: ResourceT, *, owner: str, now: datetime, ttl: timedelta
    ) -> AcquireResult[ResourceT]:
        _utc(now)
        if not owner or ttl <= timedelta(0):
            raise ValueError("owner and positive ttl are required")
        with self._lock:
            current = self._leases.get(resource)
            if current is not None and current.expires_at > now:
                return LeaseBusy(current.owner, current.expires_at)
            token = self._tokens.get(resource, 0) + 1
            lease = Lease(resource, owner, token, now + ttl)
            self._tokens[resource] = token
            self._leases[resource] = lease
            return LeaseAcquired(lease)

    def renew(
        self, lease: Lease[ResourceT], *, now: datetime, ttl: timedelta
    ) -> RenewResult[ResourceT]:
        _utc(now)
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        with self._lock:
            current = self._leases.get(lease.resource)
            if current != lease or current.expires_at <= now:
                return StaleLease()
            renewed = Lease(lease.resource, lease.owner, lease.fencing_token, now + ttl)
            self._leases[lease.resource] = renewed
            return LeaseRenewed(renewed)

    def release(self, lease: Lease[ResourceT], *, now: datetime) -> ReleaseResult:
        _utc(now)
        with self._lock:
            current = self._leases.get(lease.resource)
            if current != lease or current.expires_at <= now:
                return StaleLease()
            self._leases.pop(lease.resource)
            return LeaseReleased()

    def authority(self, resource: ResourceT) -> int | None:
        with self._lock:
            lease = self._leases.get(resource)
            return lease.fencing_token if lease is not None else None

    def current(self, resource: ResourceT) -> Lease[ResourceT] | None:
        with self._lock:
            return self._leases.get(resource)

    def locked_authority(self, resource: ResourceT, mutation: Callable[[], ResultT]) -> ResultT:
        """Reference-only helper to hold the authority lock across a mutation."""

        with self._lock:
            return mutation()


@dataclass(frozen=True, slots=True)
class FencedCommitted[ResultT]:
    value: ResultT


type FencedResult[ResultT] = FencedCommitted[ResultT] | StaleLease


class FencedCommit[ResourceT, ResultT]:
    """Compare authority under the consumer's lock and mutate in that same callback."""

    def __init__(
        self,
        locked_authority: Callable[
            [ResourceT, Callable[[LeaseAuthority | None], FencedResult[ResultT]]],
            FencedResult[ResultT],
        ],
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._locked_authority = locked_authority
        self._clock = clock

    def commit(
        self,
        lease: Lease[ResourceT],
        mutation: Callable[[], ResultT],
    ) -> FencedResult[ResultT]:
        def compare(authority: LeaseAuthority | None) -> FencedResult[ResultT]:
            if (
                authority is None
                or authority.fencing_token != lease.fencing_token
                or authority.owner != lease.owner
                or authority.expires_at <= self._clock()
            ):
                return StaleLease("lease is expired or no longer authoritative")
            return FencedCommitted(mutation())

        return self._locked_authority(lease.resource, compare)


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("lease timestamps must be timezone-aware UTC")
