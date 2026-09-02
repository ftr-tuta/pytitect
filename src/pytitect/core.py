"""Dependency-free primitives shared by every Pytitect component."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import NewType, Protocol, TypeVar, runtime_checkable

type JsonScalar = bool | int | float | str | None
type JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
Fingerprint = NewType("Fingerprint", str)


@runtime_checkable
class Clock(Protocol):
    """Source of timezone-aware UTC time."""

    def now(self) -> datetime: ...


@dataclass(frozen=True, slots=True)
class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Deadline:
    at: datetime

    def __post_init__(self) -> None:
        _require_aware_utc(self.at, "deadline")

    @classmethod
    def after(cls, timeout: timedelta, *, clock: Clock) -> Deadline:
        if timeout < timedelta(0):
            raise ValueError("timeout must not be negative")
        return cls(clock.now() + timeout)

    def remaining(self, *, clock: Clock) -> timedelta:
        return max(self.at - clock.now(), timedelta(0))

    def expired(self, *, clock: Clock) -> bool:
        return clock.now() >= self.at


@dataclass(frozen=True, slots=True)
class Limits:
    """Finite, explicit resource limits used by boundary components."""

    max_body_bytes: int = 1_048_576
    max_json_depth: int = 32
    max_json_items: int = 10_000
    max_metadata_items: int = 64
    max_string_length: int = 16_384

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive finite integer")


T_co = TypeVar("T_co", covariant=True)


@dataclass(frozen=True, slots=True)
class OpaqueId[T_co]:
    """An identifier whose representation has no domain semantics."""

    value: str

    def __post_init__(self) -> None:
        if not self.value or self.value != self.value.strip():
            raise ValueError("opaque identifiers must be non-empty and trimmed")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class RequestContext:
    request_id: OpaqueId[object]
    correlation_id: OpaqueId[object] | None = None
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


class Observer(Protocol):
    def observe(self, name: str, attributes: Mapping[str, JsonScalar]) -> None: ...


@dataclass(frozen=True, slots=True)
class NullObserver:
    def observe(self, name: str, attributes: Mapping[str, JsonScalar]) -> None:
        del name, attributes


@dataclass(frozen=True, slots=True)
class PytitectRuntime:
    """Explicit immutable composition root. Pytitect never creates a global runtime."""

    clock: Clock = field(default_factory=SystemClock)
    limits: Limits = field(default_factory=Limits)
    observer: Observer = field(default_factory=NullObserver)


type Canonicalizer = Callable[[JsonValue], bytes]


def canonical_json_bytes(value: JsonValue) -> bytes:
    """Stable dependency-free JSON encoding suitable for non-RFC-specific fingerprints."""

    validate_json(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def sha256_fingerprint(
    value: JsonValue | bytes,
    *,
    canonicalizer: Canonicalizer = canonical_json_bytes,
) -> Fingerprint:
    payload = value if isinstance(value, bytes) else canonicalizer(value)
    return Fingerprint(hashlib.sha256(payload).hexdigest())


def hmac_sha256_fingerprint(
    value: JsonValue | bytes,
    *,
    key: bytes,
    canonicalizer: Canonicalizer = canonical_json_bytes,
) -> Fingerprint:
    if not key:
        raise ValueError("HMAC key must not be empty")
    payload = value if isinstance(value, bytes) else canonicalizer(value)
    return Fingerprint(hmac.digest(key, payload, "sha256").hex())


def validate_json(value: JsonValue, *, limits: Limits | None = None) -> None:
    """Validate JSON shape, finite numbers, strings, depth, and aggregate item count."""

    selected = limits or Limits()
    count = 0

    def visit(item: JsonValue, depth: int) -> None:
        nonlocal count
        if depth > selected.max_json_depth:
            raise ValueError("JSON nesting exceeds max_json_depth")
        count += 1
        if count > selected.max_json_items:
            raise ValueError("JSON item count exceeds max_json_items")
        if item is None or isinstance(item, bool):
            return
        if isinstance(item, int):
            return
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("JSON numbers must be finite")
            return
        if isinstance(item, str):
            if len(item) > selected.max_string_length:
                raise ValueError("JSON string exceeds max_string_length")
            return
        if isinstance(item, list):
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON object keys must be strings")
                if len(key) > selected.max_string_length:
                    raise ValueError("JSON key exceeds max_string_length")
                visit(child, depth + 1)
            return
        raise ValueError(f"unsupported JSON value: {type(item).__name__}")

    visit(value, 0)


def _require_aware_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
