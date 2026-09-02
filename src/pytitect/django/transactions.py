"""Consumer-selected Django transaction boundary."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import cast


@dataclass(frozen=True, slots=True)
class DjangoTransactionBoundary:
    using: str

    def __post_init__(self) -> None:
        if not self.using:
            raise ValueError("a Django database alias is required")

    def atomic(self) -> AbstractContextManager[None]:
        from django.db import transaction

        return cast(AbstractContextManager[None], transaction.atomic(using=self.using))

    def on_commit(self, callback: Callable[[], None]) -> None:
        from django.db import transaction

        transaction.on_commit(callback, using=self.using)
