"""Privacy-first structured events with an explicit allowlist."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType

from pytitect.core import Clock, JsonScalar, SystemClock

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
                digest = hashlib.blake2b(
                    str(value).encode(), key=self.hash_key, digest_size=16
                ).hexdigest()
                output[name] = digest
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
