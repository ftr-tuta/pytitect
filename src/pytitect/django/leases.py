"""Django fenced commits that lock authority and mutate in one transaction."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

from pytitect.leases import (
    FencedCommitted,
    FencedResult,
    Lease,
    LeaseAuthority,
    StaleLease,
)

ResourceT = TypeVar("ResourceT")
ResultT = TypeVar("ResultT")


class DjangoFencedCommit[ResourceT]:
    def __init__(
        self,
        *,
        using: str,
        lock_authority: Callable[[ResourceT, str], LeaseAuthority | None],
        now: Callable[[], datetime],
    ) -> None:
        if not using:
            raise ValueError("a Django database alias is required")
        self.using = using
        self._lock_authority = lock_authority
        self._now = now

    @classmethod
    def from_store(
        cls,
        store: object,
        *,
        now: Callable[[], datetime],
    ) -> DjangoFencedCommit[ResourceT]:
        using = getattr(store, "using", None)
        locked = getattr(store, "lock_authority", None)
        if not isinstance(using, str) or not using or not callable(locked):
            raise ValueError("a Django lease store with an explicit alias is required")
        return cls(
            using=using,
            lock_authority=lambda resource, alias: locked(resource),
            now=now,
        )

    def commit(
        self,
        lease: Lease[ResourceT],
        mutation: Callable[[], ResultT],
    ) -> FencedResult[ResultT]:
        from django.db import transaction

        with transaction.atomic(using=self.using):
            authority = self._lock_authority(lease.resource, self.using)
            if (
                authority is None
                or authority.owner != lease.owner
                or authority.fencing_token != lease.fencing_token
                or authority.expires_at <= self._now()
            ):
                return StaleLease("lease is expired or no longer authoritative")
            return FencedCommitted(mutation())
