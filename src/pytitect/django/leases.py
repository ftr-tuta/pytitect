"""Django fenced-commit adapter built entirely from consumer-owned callbacks."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar

from pytitect.leases import FencedCommit, FencedResult, LeaseAuthority

ResourceT = TypeVar("ResourceT")
ResultT = TypeVar("ResultT")
AuthorityRowT = TypeVar("AuthorityRowT")


@dataclass(frozen=True, slots=True)
class DjangoFencedCommitFactory[ResourceT, AuthorityRowT, ResultT]:
    """Bind row lookup/locking and token extraction owned by the consuming project."""

    using: str
    select_for_update: Callable[[ResourceT, str], AuthorityRowT]
    authority_from_row: Callable[[AuthorityRowT], LeaseAuthority]
    now: Callable[[], datetime]

    def build(self) -> FencedCommit[ResourceT, ResultT]:
        if not self.using:
            raise ValueError("a Django database alias is required")

        def locked(
            resource: ResourceT,
            compare: Callable[[LeaseAuthority | None], FencedResult[ResultT]],
        ) -> FencedResult[ResultT]:
            from django.db import transaction

            with transaction.atomic(using=self.using):
                row = self.select_for_update(resource, self.using)
                return compare(self.authority_from_row(row))

        return FencedCommit(locked, clock=self.now)
