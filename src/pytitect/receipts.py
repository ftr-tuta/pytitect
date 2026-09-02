"""Typed operation receipts and validated state transitions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import TypeVar

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


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("receipt timestamps must be timezone-aware UTC")
