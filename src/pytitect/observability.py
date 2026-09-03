"""Privacy-first structured events with an explicit allowlist."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType

from pytitect.core import Clock, JsonScalar, Limits, Observer, SystemClock

_FORBIDDEN_FRAGMENTS = frozenset(
    {
        "body",
        "header",
        "token",
        "cookie",
        "idempotency",
        "password",
        "secret",
        "authorization",
        "dsn",
        "email",
        "phone",
        "address",
        "filepath",
        "path",
    }
)

# Values are intentionally free of raw identifiers and transport details.
OBSERVATION_VOCABULARY = frozenset(
    {
        "operation",
        "outcome",
        "protocol",
        "sync_kind",
        "dataset_hash",
        "trace_sampled",
        "item_count",
        "duration_ms",
    }
)


class AttributeMode(StrEnum):
    PLAIN = "plain"
    HASH = "hash"
    REDACT = "redact"


@dataclass(frozen=True, slots=True)
class AttributeRule:
    mode: AttributeMode = AttributeMode.PLAIN


@dataclass(frozen=True, slots=True)
class ObservationPolicy:
    allowlist: Mapping[str, AttributeRule]
    hash_key: bytes = b""

    def __post_init__(self) -> None:
        copied = dict(self.allowlist)
        for name, rule in copied.items():
            normalized = name.casefold().replace("-", "_")
            if any(fragment in normalized for fragment in _FORBIDDEN_FRAGMENTS):
                raise ValueError(f"sensitive attribute cannot be allowlisted: {name}")
            if rule.mode is AttributeMode.HASH and not self.hash_key:
                raise ValueError("hash_key is required for hashed attributes")
        object.__setattr__(self, "allowlist", MappingProxyType(copied))

    def filter(self, attributes: Mapping[str, JsonScalar]) -> Mapping[str, JsonScalar]:
        output: dict[str, JsonScalar] = {}
        for name, value in attributes.items():
            rule = self.allowlist.get(name)
            if rule is None:
                continue
            if rule.mode is AttributeMode.REDACT:
                output[name] = "[REDACTED]"
            elif rule.mode is AttributeMode.HASH:
                output[name] = pseudonymous_attribute(value, key=self.hash_key)
            else:
                output[name] = value
        return MappingProxyType(output)


@dataclass(frozen=True, slots=True)
class Event:
    name: str
    occurred_at: datetime
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)


class StructuredObserver:
    def __init__(
        self,
        sink: Callable[[Event], None],
        *,
        policy: ObservationPolicy,
        clock: Clock | None = None,
    ) -> None:
        self._sink = sink
        self._policy = policy
        self._clock = clock or SystemClock()

    def observe(self, name: str, attributes: Mapping[str, JsonScalar]) -> None:
        if not name:
            raise ValueError("event name must not be empty")
        self._sink(Event(name, self._clock.now(), self._policy.filter(attributes)))


@dataclass(frozen=True, slots=True)
class ObserverFailure:
    observer: str
    exception_type: str
    message: str


class FailureIsolatedObserver:
    """Contain observer failures and report only bounded, sanitized diagnostics."""

    def __init__(
        self,
        observer: Observer,
        fallback: Callable[[ObserverFailure], None],
        *,
        limits: Limits | None = None,
    ) -> None:
        self._observer = observer
        self._fallback = fallback
        self._limits = limits or Limits()

    def observe(self, name: str, attributes: Mapping[str, JsonScalar]) -> None:
        try:
            self._observer.observe(name, attributes)
        except Exception as error:
            failure = ObserverFailure(
                observer=type(self._observer).__name__[: self._limits.max_string_length],
                exception_type=type(error).__name__[: self._limits.max_string_length],
                message=_sanitized_message(error, self._limits.max_string_length),
            )
            with suppress(Exception):
                self._fallback(failure)


def _sanitized_message(error: Exception, limit: int) -> str:
    message = " ".join(str(error).split())
    if not message:
        message = "observer failed"
    return message[:limit]


def pseudonymous_attribute(value: JsonScalar, *, key: bytes) -> str:
    """Produce the documented keyed BLAKE2b-128 observability pseudonym."""

    if not key:
        raise ValueError("a non-empty key is required for pseudonymous attributes")
    return hashlib.blake2b(str(value).encode(), key=key, digest_size=16).hexdigest()
