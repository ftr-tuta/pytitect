"""Generation guards that compare and mutate inside one transaction."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol, TypeVar

ResultT = TypeVar("ResultT")


class GenerationStore(Protocol):
    def load_for_update(self, dataset: str, partition: str) -> int | None: ...

    def compare_and_set(
        self,
        dataset: str,
        partition: str,
        *,
        expected: int | None,
        generation: int,
    ) -> bool: ...


class InMemoryGenerationStore:
    """Finite process-local generation store with atomic compare-and-set."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._generations: dict[tuple[str, str], int] = {}
        self._lock = threading.RLock()

    def load_for_update(self, dataset: str, partition: str) -> int | None:
        _validate(dataset, partition)
        with self._lock:
            return self._generations.get((dataset, partition))

    def compare_and_set(
        self,
        dataset: str,
        partition: str,
        *,
        expected: int | None,
        generation: int,
    ) -> bool:
        _validate(dataset, partition)
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("generation must be a non-negative integer")
        if expected is not None and (
            isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
        ):
            raise ValueError("expected generation must be a non-negative integer or None")
        identity = (dataset, partition)
        with self._lock:
            if self._generations.get(identity) != expected:
                return False
            if identity not in self._generations and len(self._generations) >= self._capacity:
                raise OverflowError("generation capacity exceeded")
            self._generations[identity] = generation
            return True


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


class GenerationStoreHarness:
    """Reusable behavioral contract for generation stores."""

    def __init__(self, factory: Callable[[], GenerationStore]) -> None:
        self._factory = factory

    def exercise(self) -> None:
        store = self._factory()
        if store.load_for_update("dataset", "partition") is not None:
            raise AssertionError("a new generation identity must be absent")
        if not store.compare_and_set("dataset", "partition", expected=None, generation=1):
            raise AssertionError("an absent generation must accept a None CAS")
        if store.compare_and_set("dataset", "partition", expected=None, generation=2):
            raise AssertionError("a stale generation CAS must fail")
        if store.load_for_update("dataset", "partition") != 1:
            raise AssertionError("a committed generation must be loadable")
        if not store.compare_and_set("dataset", "partition", expected=1, generation=2):
            raise AssertionError("the current generation CAS must succeed")
        if store.load_for_update("dataset", "partition") != 2:
            raise AssertionError("generation advancement must be durable")


def _validate(dataset: str, partition: str) -> None:
    if not dataset or not partition:
        raise ValueError("dataset and partition must not be empty")
