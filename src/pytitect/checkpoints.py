"""Opaque checkpoints advanced only through a transaction's on-commit hook."""

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


class CheckpointCoordinator[PayloadT]:
    def __init__(self, store: CheckpointStore, transaction: TransactionBoundary) -> None:
        self._store = store
        self._transaction = transaction

    def apply(
        self,
        *,
        stream: str,
        items: Iterator[CheckpointItem[PayloadT]],
        apply_state: Callable[[PayloadT], None],
    ) -> CheckpointBatchResult:
        if not stream:
            raise ValueError("stream must not be empty")
        previous = self._store.load(stream)
        last: Checkpoint | None = None
        applied = 0
        with self._transaction.atomic():
            for item in items:
                apply_state(item.payload)
                last = item.checkpoint
                applied += 1
            if last is not None:
                final = last

                def advance() -> None:
                    if not self._store.advance(stream, expected=previous, checkpoint=final):
                        raise RuntimeError("checkpoint compare-and-set failed after commit")

                self._transaction.on_commit(advance)
        return CheckpointBatchResult(applied, previous, last)
