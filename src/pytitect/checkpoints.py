"""Atomic and explicitly deferred checkpoint coordination."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol, TypeVar

PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True, slots=True)
class Checkpoint:
    value: bytes

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("checkpoint must be non-empty")


@dataclass(frozen=True, slots=True)
class CheckpointItem[PayloadT]:
    checkpoint: Checkpoint
    payload: PayloadT


class CheckpointStore(Protocol):
    def load(self, stream: str) -> Checkpoint | None: ...

    def load_for_update(self, stream: str) -> Checkpoint | None: ...

    def advance(
        self,
        stream: str,
        *,
        expected: Checkpoint | None,
        checkpoint: Checkpoint,
    ) -> bool: ...


class TransactionBoundary(Protocol):
    def atomic(self) -> AbstractContextManager[None]: ...

    def on_commit(self, callback: Callable[[], None]) -> None: ...


@dataclass(frozen=True, slots=True)
class CheckpointBatchResult:
    applied: int
    previous: Checkpoint | None
    pending: Checkpoint | None


@dataclass(frozen=True, slots=True)
class AtomicCheckpointConfirmed:
    batch: CheckpointBatchResult


@dataclass(frozen=True, slots=True)
class StaleCheckpoint:
    previous: Checkpoint | None
    attempted: Checkpoint | None


@dataclass(frozen=True, slots=True)
class StateCommittedCheckpointConfirmed:
    batch: CheckpointBatchResult


@dataclass(frozen=True, slots=True)
class StateCommittedCheckpointUnconfirmed:
    batch: CheckpointBatchResult


type AtomicCheckpointResult = AtomicCheckpointConfirmed | StaleCheckpoint
type DeferredCheckpointResult = (
    StateCommittedCheckpointConfirmed | StateCommittedCheckpointUnconfirmed
)


class _RollbackStaleCheckpoint(Exception):
    def __init__(self, result: StaleCheckpoint) -> None:
        self.result = result


class AtomicCheckpointCoordinator[PayloadT]:
    """Apply state and advance its checkpoint under one transaction and row lock."""

    def __init__(self, store: CheckpointStore, transaction: TransactionBoundary) -> None:
        _matching_aliases(store, transaction, "atomic checkpoint")
        self._store = store
        self._transaction = transaction

    def apply(
        self,
        *,
        stream: str,
        items: Iterator[CheckpointItem[PayloadT]],
        apply_state: Callable[[PayloadT], None],
    ) -> AtomicCheckpointResult:
        _validate_stream(stream)
        previous: Checkpoint | None = None
        last: Checkpoint | None = None
        applied = 0
        try:
            with self._transaction.atomic():
                previous = self._store.load_for_update(stream)
                for item in items:
                    apply_state(item.payload)
                    last = item.checkpoint
                    applied += 1
                if last is not None and not self._store.advance(
                    stream, expected=previous, checkpoint=last
                ):
                    raise _RollbackStaleCheckpoint(StaleCheckpoint(previous, last))
        except _RollbackStaleCheckpoint as failure:
            return failure.result
        return AtomicCheckpointConfirmed(CheckpointBatchResult(applied, previous, last))


class DeferredCheckpointCoordinator[PayloadT]:
    """Commit state first and report checkpoint confirmation truthfully afterward."""

    def __init__(self, store: CheckpointStore, transaction: TransactionBoundary) -> None:
        self._store = store
        self._transaction = transaction

    def apply(
        self,
        *,
        stream: str,
        items: Iterator[CheckpointItem[PayloadT]],
        apply_state: Callable[[PayloadT], None],
    ) -> DeferredCheckpointResult:
        _validate_stream(stream)
        previous = self._store.load(stream)
        last: Checkpoint | None = None
        applied = 0
        with self._transaction.atomic():
            for item in items:
                apply_state(item.payload)
                last = item.checkpoint
                applied += 1
        batch = CheckpointBatchResult(applied, previous, last)
        if last is None or self._store.advance(stream, expected=previous, checkpoint=last):
            return StateCommittedCheckpointConfirmed(batch)
        return StateCommittedCheckpointUnconfirmed(batch)


def _validate_stream(stream: str) -> None:
    if not stream:
        raise ValueError("stream must not be empty")


def _matching_aliases(store: object, transaction: object, operation: str) -> None:
    store_alias = getattr(store, "using", None)
    transaction_alias = getattr(transaction, "using", None)
    if (
        store_alias is not None
        and transaction_alias is not None
        and store_alias != transaction_alias
    ):
        raise ValueError(f"{operation} store and transaction must use exactly one alias")
