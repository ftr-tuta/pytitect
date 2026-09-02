"""Shared typed verification outcomes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType

from pytitect.core import JsonScalar


@dataclass(frozen=True, slots=True)
class VerifiedProof:
    mechanism: str
    key_id: str
    proof_id: str
    issued_at: datetime
    expires_at: datetime | None = None
    attributes: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if len(self.attributes) > 32:
            raise ValueError("verified proof attributes are limited to 32 items")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


@dataclass(frozen=True, slots=True)
class RejectedProof:
    code: str
    detail: str
