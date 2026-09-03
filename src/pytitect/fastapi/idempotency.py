"""Bounded idempotency-key parsing for explicit FastAPI dependencies."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    value: str

    def __post_init__(self) -> None:
        if not self.value or self.value != self.value.strip():
            raise ValueError("idempotency key must be non-empty and trimmed")
        if len(self.value) > 255:
            raise ValueError("idempotency key exceeds 255 characters")


def idempotency_key_from_headers(
    headers: Mapping[str, str], *, header: str = "idempotency-key"
) -> IdempotencyKey:
    normalized = {key.lower(): value for key, value in headers.items()}
    try:
        return IdempotencyKey(normalized[header.lower()])
    except KeyError as exc:
        raise ValueError("idempotency key header is required") from exc
