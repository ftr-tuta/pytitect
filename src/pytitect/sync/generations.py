"""Generation guards that compare and mutate inside one transaction."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol, TypeVar

ResultT = TypeVar("ResultT")


class GenerationStore(Protocol):
    def load_for_update(self, dataset: str, partition: str) -> int | None: ...


class GenerationTransaction(Protocol):
    def atomic(self) -> AbstractContextManager[None]: ...


@dataclass(frozen=True, slots=True)
class GenerationCommitted[ResultT]:
    value: ResultT
    generation: int


@dataclass(frozen=True, slots=True)
class StaleGeneration:
    expected: int
    actual: int | None


type GenerationResult[ResultT] = GenerationCommitted[ResultT] | StaleGeneration


class GenerationGuard:
    def __init__(self, store: GenerationStore, transaction: GenerationTransaction) -> None:
        store_alias = getattr(store, "using", None)
        transaction_alias = getattr(transaction, "using", None)
        if (
            store_alias is not None
            and transaction_alias is not None
            and store_alias != transaction_alias
        ):
            raise ValueError("generation store and transaction must use exactly one alias")
        self._store = store
        self._transaction = transaction

    def commit(
        self,
        *,
        dataset: str,
        partition: str,
        expected: int,
        mutation: Callable[[], ResultT],
    ) -> GenerationResult[ResultT]:
        if not dataset or not partition or expected < 0:
            raise ValueError("dataset, partition, and a non-negative generation are required")
        with self._transaction.atomic():
            actual = self._store.load_for_update(dataset, partition)
            if actual != expected:
                return StaleGeneration(expected, actual)
            return GenerationCommitted(mutation(), actual)
