"""Typed operation receipts and validated state transitions."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, TypeVar

from pytitect.core import JsonScalar, OpaqueId

ResultT = TypeVar("ResultT")


class ReceiptState(StrEnum):
    ACCEPTED = "accepted"
    PROCESSING = "processing"
    COMPLETED = "completed"
    REJECTED = "rejected"
    CONFLICTED = "conflicted"
    UNCERTAIN = "uncertain"


class ReceiptKind(StrEnum):
    MUTATION = "mutation"
    COMMAND = "command"
    RUN = "run"
    OPERATION = "operation"
    TERMINAL_BOUNDARY = "terminal_boundary"


_TRANSITIONS = {
    ReceiptState.ACCEPTED: frozenset(
        {
            ReceiptState.PROCESSING,
            ReceiptState.REJECTED,
            ReceiptState.CONFLICTED,
            ReceiptState.UNCERTAIN,
        }
    ),
    ReceiptState.PROCESSING: frozenset(
        {
            ReceiptState.COMPLETED,
            ReceiptState.REJECTED,
            ReceiptState.CONFLICTED,
            ReceiptState.UNCERTAIN,
        }
    ),
    ReceiptState.COMPLETED: frozenset(),
    ReceiptState.REJECTED: frozenset(),
    ReceiptState.CONFLICTED: frozenset(),
    ReceiptState.UNCERTAIN: frozenset(),
}


@dataclass(frozen=True, slots=True)
class Receipt[ResultT]:
    receipt_id: OpaqueId[object]
    kind: ReceiptKind
    state: ReceiptState
    created_at: datetime
    updated_at: datetime
    result: ResultT | None = None
    metadata: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _utc(self.created_at)
        _utc(self.updated_at)
        if self.updated_at < self.created_at:
            raise ValueError("updated_at cannot precede created_at")
        if len(self.metadata) > 64:
            raise ValueError("receipt metadata is limited to 64 items")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def transition(
        self,
        target: ReceiptState,
        *,
        at: datetime,
        result: ResultT | None = None,
    ) -> TransitionResult[ResultT]:
        _utc(at)
        if at < self.updated_at:
            return InvalidTransition(self.state, target, "transition timestamp moved backwards")
        if target not in _TRANSITIONS[self.state]:
            return InvalidTransition(self.state, target, "state transition is not allowed")
        if target is ReceiptState.COMPLETED and result is None:
            return InvalidTransition(self.state, target, "completed receipts require a result")
        return ReceiptTransitioned(replace(self, state=target, updated_at=at, result=result))


@dataclass(frozen=True, slots=True)
class MutationReceipt(Receipt[ResultT]):
    kind: ReceiptKind = field(default=ReceiptKind.MUTATION, init=False)


@dataclass(frozen=True, slots=True)
class CommandReceipt(Receipt[ResultT]):
    kind: ReceiptKind = field(default=ReceiptKind.COMMAND, init=False)


@dataclass(frozen=True, slots=True)
class RunReceipt(Receipt[ResultT]):
    kind: ReceiptKind = field(default=ReceiptKind.RUN, init=False)


@dataclass(frozen=True, slots=True)
class OperationReceipt(Receipt[ResultT]):
    kind: ReceiptKind = field(default=ReceiptKind.OPERATION, init=False)


@dataclass(frozen=True, slots=True)
class TerminalBoundaryReceipt(Receipt[ResultT]):
    kind: ReceiptKind = field(default=ReceiptKind.TERMINAL_BOUNDARY, init=False)


@dataclass(frozen=True, slots=True)
class ReceiptTransitioned[ResultT]:
    receipt: Receipt[ResultT]


@dataclass(frozen=True, slots=True)
class InvalidTransition:
    source: ReceiptState
    target: ReceiptState
    reason: str


type TransitionResult[ResultT] = ReceiptTransitioned[ResultT] | InvalidTransition


class ReceiptStore(Protocol[ResultT]):
    """Persistence port; resolving uncertainty requires the explicit CAS method."""

    def get(self, receipt_id: OpaqueId[object]) -> Receipt[ResultT] | None: ...

    def add(self, receipt: Receipt[ResultT]) -> bool: ...

    def transition(self, receipt: Receipt[ResultT], target: Receipt[ResultT]) -> bool: ...

    def reconcile_uncertain(
        self,
        receipt: Receipt[ResultT],
        target: Receipt[ResultT],
    ) -> bool: ...


class InMemoryReceiptStore[ResultT]:
    """Finite process-local reference receipt store with compare-and-set transitions."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._receipts: dict[OpaqueId[object], Receipt[ResultT]] = {}
        self._lock = threading.RLock()

    def get(self, receipt_id: OpaqueId[object]) -> Receipt[ResultT] | None:
        with self._lock:
            return self._receipts.get(receipt_id)

    def add(self, receipt: Receipt[ResultT]) -> bool:
        with self._lock:
            if receipt.receipt_id in self._receipts:
                return False
            if len(self._receipts) >= self._capacity:
                raise OverflowError("receipt capacity exceeded")
            self._receipts[receipt.receipt_id] = receipt
            return True

    def transition(self, receipt: Receipt[ResultT], target: Receipt[ResultT]) -> bool:
        proposed = receipt.transition(
            target.state,
            at=target.updated_at,
            result=target.result,
        )
        if not isinstance(proposed, ReceiptTransitioned) or proposed.receipt != target:
            return False
        with self._lock:
            if self._receipts.get(receipt.receipt_id) != receipt:
                return False
            self._receipts[receipt.receipt_id] = target
            return True

    def reconcile_uncertain(
        self,
        receipt: Receipt[ResultT],
        target: Receipt[ResultT],
    ) -> bool:
        if not _valid_reconciliation(receipt, target):
            return False
        with self._lock:
            if self._receipts.get(receipt.receipt_id) != receipt:
                return False
            self._receipts[receipt.receipt_id] = target
            return True


@dataclass(frozen=True, slots=True)
class ConfirmedCompleted[ResultT]:
    receipt: Receipt[ResultT]


@dataclass(frozen=True, slots=True)
class ConfirmedRejected[ResultT]:
    receipt: Receipt[ResultT]


@dataclass(frozen=True, slots=True)
class ConfirmedConflicted[ResultT]:
    receipt: Receipt[ResultT]


@dataclass(frozen=True, slots=True)
class StillUncertain[ResultT]:
    receipt: Receipt[ResultT] | None
    reason: str


type ReconciliationResult[ResultT] = (
    ConfirmedCompleted[ResultT]
    | ConfirmedRejected[ResultT]
    | ConfirmedConflicted[ResultT]
    | StillUncertain[ResultT]
)


@dataclass(frozen=True, slots=True)
class ReceiptReconciler[ResultT]:
    store: ReceiptStore[ResultT]

    def reconcile(
        self,
        receipt_id: OpaqueId[object],
        target: ReceiptState,
        *,
        at: datetime,
        result: ResultT | None = None,
    ) -> ReconciliationResult[ResultT]:
        _utc(at)
        if target not in {
            ReceiptState.COMPLETED,
            ReceiptState.REJECTED,
            ReceiptState.CONFLICTED,
        }:
            raise ValueError("uncertain receipts may only reconcile to a confirmed terminal state")
        current = self.store.get(receipt_id)
        if current is None or current.state is not ReceiptState.UNCERTAIN:
            return StillUncertain(current, "receipt is absent or no longer uncertain")
        if at < current.updated_at:
            return StillUncertain(current, "reconciliation timestamp moved backwards")
        if target is ReceiptState.COMPLETED and result is None:
            raise ValueError("completed reconciliation requires a result")
        reconciled = replace(current, state=target, updated_at=at, result=result)
        if not self.store.reconcile_uncertain(current, reconciled):
            return StillUncertain(self.store.get(receipt_id), "receipt changed concurrently")
        if target is ReceiptState.COMPLETED:
            return ConfirmedCompleted(reconciled)
        if target is ReceiptState.REJECTED:
            return ConfirmedRejected(reconciled)
        return ConfirmedConflicted(reconciled)


class ReceiptStoreHarness[ResultT]:
    """Reusable behavioral contract for receipt stores."""

    def __init__(self, factory: Callable[[], ReceiptStore[ResultT]]) -> None:
        self._factory = factory

    def exercise(self, *, value: ResultT, now: datetime) -> None:
        store = self._factory()
        accepted = MutationReceipt[ResultT](OpaqueId("accepted"), ReceiptState.ACCEPTED, now, now)
        if not store.add(accepted) or store.add(accepted):
            raise AssertionError("receipt insertion must be unique")
        if store.get(accepted.receipt_id) != accepted:
            raise AssertionError("an inserted receipt must be loadable")
        transitioned = accepted.transition(ReceiptState.PROCESSING, at=now + timedelta(seconds=1))
        if not isinstance(transitioned, ReceiptTransitioned):
            raise AssertionError("the harness processing transition must be valid")
        if not store.transition(accepted, transitioned.receipt):
            raise AssertionError("a current receipt transition must succeed")
        if store.transition(accepted, transitioned.receipt):
            raise AssertionError("a stale receipt transition must fail")

        uncertain = MutationReceipt[ResultT](
            OpaqueId("uncertain"), ReceiptState.UNCERTAIN, now, now
        )
        if not store.add(uncertain):
            raise AssertionError("an uncertain receipt must be insertable")
        completed = replace(
            uncertain,
            state=ReceiptState.COMPLETED,
            updated_at=now + timedelta(seconds=1),
            result=value,
        )
        if not store.reconcile_uncertain(uncertain, completed):
            raise AssertionError("a current uncertain receipt must reconcile")
        if store.reconcile_uncertain(uncertain, completed):
            raise AssertionError("a stale uncertain receipt must not reconcile")
        if store.get(uncertain.receipt_id) != completed:
            raise AssertionError("a reconciled receipt must be durable")


def _valid_reconciliation[ResultT](receipt: Receipt[ResultT], target: Receipt[ResultT]) -> bool:
    return bool(
        receipt.state is ReceiptState.UNCERTAIN
        and target.state
        in {
            ReceiptState.COMPLETED,
            ReceiptState.REJECTED,
            ReceiptState.CONFLICTED,
        }
        and target.receipt_id == receipt.receipt_id
        and target.kind is receipt.kind
        and target.created_at == receipt.created_at
        and target.updated_at >= receipt.updated_at
        and target.metadata == receipt.metadata
        and (target.state is not ReceiptState.COMPLETED or target.result is not None)
    )


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("receipt timestamps must be timezone-aware UTC")
