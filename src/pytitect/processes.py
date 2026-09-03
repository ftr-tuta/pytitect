"""Preview durable process-manager and saga contracts."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from pytitect.core import JsonValue, validate_json


@dataclass(frozen=True, slots=True)
class ProcessKey:
    name: str
    instance_id: str

    def __post_init__(self) -> None:
        if not self.name or not self.instance_id:
            raise ValueError("process name and instance ID must not be empty")


class ProcessStatus(StrEnum):
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    COMPENSATED = "compensated"
    FAILED = "failed"


class ProcessEffectKind(StrEnum):
    INTEGRATION_EVENT = "integration_event"
    COMMAND = "command"
    TASK = "task"
    COMPENSATION = "compensation"


@dataclass(frozen=True, slots=True)
class ProcessEffect:
    effect_id: str
    kind: ProcessEffectKind
    name: str
    payload: JsonValue

    def __post_init__(self) -> None:
        if not self.effect_id or not self.name:
            raise ValueError("process effect identity and name must not be empty")
        validate_json(self.payload)


@dataclass(frozen=True, slots=True)
class TimerSchedule:
    timer_id: str
    due_at: datetime
    effect: ProcessEffect

    def __post_init__(self) -> None:
        if not self.timer_id:
            raise ValueError("timer ID must not be empty")
        _utc(self.due_at)


@dataclass(frozen=True, slots=True)
class ProcessState:
    key: ProcessKey
    version: int
    status: ProcessStatus
    state: JsonValue
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.version <= 0:
            raise ValueError("process version must be positive")
        validate_json(self.state)
        _utc(self.updated_at)


@dataclass(frozen=True, slots=True)
class ProcessDecision:
    state: JsonValue
    status: ProcessStatus = ProcessStatus.RUNNING
    effects: tuple[ProcessEffect, ...] = ()
    schedule: tuple[TimerSchedule, ...] = ()
    cancel_timers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_json(self.state)
        effect_ids = [effect.effect_id for effect in self.effects]
        timer_ids = [timer.timer_id for timer in self.schedule]
        if len(effect_ids) != len(set(effect_ids)) or len(timer_ids) != len(set(timer_ids)):
            raise ValueError("process effect and timer IDs must be unique per decision")
        if set(timer_ids).intersection(self.cancel_timers):
            raise ValueError("a timer cannot be scheduled and cancelled in one decision")


@dataclass(frozen=True, slots=True)
class ProcessApplied:
    state: ProcessState
    effects: int
    timers_scheduled: int
    timers_cancelled: int


@dataclass(frozen=True, slots=True)
class StaleProcessVersion:
    expected: int
    actual: int


type ProcessApplyResult = ProcessApplied | StaleProcessVersion


@dataclass(frozen=True, slots=True)
class ProcessTimer:
    process: ProcessKey
    timer_id: str
    due_at: datetime
    effect: ProcessEffect
    fencing_token: int = 0


@dataclass(frozen=True, slots=True)
class ProcessTimerClaim:
    timer: ProcessTimer
    claim_id: str
    claimed_until: datetime

    def __post_init__(self) -> None:
        if not self.claim_id:
            raise ValueError("timer claim ID must not be empty")
        _utc(self.claimed_until)


class ProcessManagerStore(Protocol):
    def load(self, key: ProcessKey) -> ProcessState | None: ...

    def apply(
        self,
        key: ProcessKey,
        *,
        expected_version: int,
        decision: ProcessDecision,
        at: datetime,
    ) -> ProcessApplyResult: ...

    def claim_timers(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[ProcessTimerClaim]: ...

    def complete_timer(self, claim: ProcessTimerClaim) -> bool: ...


@dataclass(slots=True)
class _Timer:
    value: ProcessTimer
    claim_id: str | None = None
    claimed_until: datetime | None = None


class InMemoryProcessManagerStore:
    """Finite process-local store with no durability or cross-process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("process store capacity must be a positive integer")
        self._capacity = capacity
        self._states: dict[ProcessKey, ProcessState] = {}
        self._timers: dict[tuple[ProcessKey, str], _Timer] = {}
        self._effects: dict[str, tuple[ProcessKey, ProcessEffect]] = {}
        self._lock = threading.RLock()

    @property
    def pending_effects(self) -> tuple[tuple[ProcessKey, ProcessEffect], ...]:
        with self._lock:
            return tuple(self._effects[key] for key in sorted(self._effects))

    def load(self, key: ProcessKey) -> ProcessState | None:
        with self._lock:
            return self._states.get(key)

    def apply(
        self,
        key: ProcessKey,
        *,
        expected_version: int,
        decision: ProcessDecision,
        at: datetime,
    ) -> ProcessApplyResult:
        _utc(at)
        if expected_version < 0:
            raise ValueError("expected process version must not be negative")
        with self._lock:
            current = self._states.get(key)
            actual = 0 if current is None else current.version
            if actual != expected_version:
                return StaleProcessVersion(expected_version, actual)
            timer_keys = [(key, timer.timer_id) for timer in decision.schedule]
            if any(timer_key in self._timers for timer_key in timer_keys):
                raise ValueError("scheduled timer ID already exists")
            if any(effect.effect_id in self._effects for effect in decision.effects):
                raise ValueError("process effect ID already exists")
            new_items = int(current is None) + len(timer_keys) + len(decision.effects)
            total = len(self._states) + len(self._timers) + len(self._effects)
            if total + new_items > self._capacity:
                raise OverflowError("process store capacity exceeded")
            state = ProcessState(key, actual + 1, decision.status, decision.state, at)
            cancelled = 0
            for timer_id in decision.cancel_timers:
                cancelled += int(self._timers.pop((key, timer_id), None) is not None)
            for schedule in decision.schedule:
                timer = ProcessTimer(key, schedule.timer_id, schedule.due_at, schedule.effect)
                self._timers[(key, schedule.timer_id)] = _Timer(timer)
            for effect in decision.effects:
                self._effects[effect.effect_id] = (key, effect)
            self._states[key] = state
            return ProcessApplied(state, len(decision.effects), len(decision.schedule), cancelled)

    def claim_timers(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[ProcessTimerClaim]:
        _utc(now)
        _claim_arguments(limit, claim_ttl)
        claims: list[ProcessTimerClaim] = []
        with self._lock:
            eligible = sorted(
                self._timers.values(),
                key=lambda stored: (
                    stored.value.due_at,
                    stored.value.process.name,
                    stored.value.process.instance_id,
                    stored.value.timer_id,
                ),
            )
            for stored in eligible:
                if len(claims) >= limit:
                    break
                if stored.value.due_at > now:
                    continue
                if stored.claimed_until is not None and stored.claimed_until > now:
                    continue
                token = stored.value.fencing_token + 1
                stored.value = ProcessTimer(
                    stored.value.process,
                    stored.value.timer_id,
                    stored.value.due_at,
                    stored.value.effect,
                    token,
                )
                stored.claim_id = uuid.uuid4().hex
                stored.claimed_until = now + claim_ttl
                claims.append(
                    ProcessTimerClaim(stored.value, stored.claim_id, stored.claimed_until)
                )
        return claims

    def complete_timer(self, claim: ProcessTimerClaim) -> bool:
        identity = (claim.timer.process, claim.timer.timer_id)
        with self._lock:
            stored = self._timers.get(identity)
            if (
                stored is None
                or stored.claim_id != claim.claim_id
                or stored.value.fencing_token != claim.timer.fencing_token
            ):
                return False
            self._timers.pop(identity)
            return True


type ProcessDecider = Callable[[ProcessState | None, JsonValue], ProcessDecision]


@dataclass(frozen=True, slots=True)
class ProcessManagerBinding:
    name: str
    decide: ProcessDecider

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("process-manager binding name must not be empty")


class ProcessManagerRegistry:
    def __init__(self, bindings: Iterable[ProcessManagerBinding]) -> None:
        values: dict[str, ProcessDecider] = {}
        for binding in bindings:
            if binding.name in values:
                raise ValueError(f"duplicate process-manager binding: {binding.name}")
            values[binding.name] = binding.decide
        self._values = MappingProxyType(values)

    def require(self, name: str) -> ProcessDecider:
        try:
            return self._values[name]
        except KeyError as exc:
            raise LookupError(f"unknown process manager: {name}") from exc


class ProcessManagerRuntime:
    def __init__(
        self,
        store: ProcessManagerStore,
        registry: ProcessManagerRegistry,
        *,
        now: Callable[[], datetime],
    ) -> None:
        self._store = store
        self._registry = registry
        self._now = now

    def handle(self, key: ProcessKey, input_value: JsonValue) -> ProcessApplyResult:
        validate_json(input_value)
        current = self._store.load(key)
        expected = 0 if current is None else current.version
        decision = self._registry.require(key.name)(current, input_value)
        return self._store.apply(
            key,
            expected_version=expected,
            decision=decision,
            at=self._now(),
        )


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("process timestamps must be timezone-aware UTC")


def _claim_arguments(limit: int, claim_ttl: timedelta) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("claim limit must be a positive integer")
    if claim_ttl <= timedelta(0):
        raise ValueError("claim ttl must be positive")


__all__ = [
    "InMemoryProcessManagerStore",
    "ProcessApplied",
    "ProcessDecision",
    "ProcessEffect",
    "ProcessEffectKind",
    "ProcessKey",
    "ProcessManagerBinding",
    "ProcessManagerRegistry",
    "ProcessManagerRuntime",
    "ProcessManagerStore",
    "ProcessState",
    "ProcessStatus",
    "ProcessTimer",
    "ProcessTimerClaim",
    "StaleProcessVersion",
    "TimerSchedule",
]
