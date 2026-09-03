"""Bounded rejected-delivery quarantine contracts."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Protocol

from pytitect.core import JsonScalar


@dataclass(frozen=True, slots=True)
class QuarantinePolicy:
    retain_payload: bool = False
    max_payload_bytes: int = 1_048_576
    max_reason_chars: int = 512
    max_metadata_items: int = 32

    def __post_init__(self) -> None:
        for name in ("max_payload_bytes", "max_reason_chars", "max_metadata_items"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class RejectedDelivery:
    quarantine_id: str
    message_id: str
    source: str
    consumer: str
    failed_at: datetime
    payload_sha256: str
    reason: str
    metadata: Mapping[str, JsonScalar] = field(default_factory=dict)
    payload: bytes | None = None

    def __post_init__(self) -> None:
        if not all((self.quarantine_id, self.message_id, self.source, self.consumer, self.reason)):
            raise ValueError("rejected delivery identity and reason must not be empty")
        if len(self.payload_sha256) != 64:
            raise ValueError("rejected delivery requires a SHA-256 digest")
        if self.failed_at.tzinfo is None or self.failed_at.utcoffset() != timedelta(0):
            raise ValueError("rejected delivery time must be timezone-aware UTC")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


class RejectedDeliveryStore(Protocol):
    async def add(self, delivery: RejectedDelivery) -> bool: ...


class InMemoryRejectedDeliveryStore:
    """Finite process-local quarantine with no durability or process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("quarantine capacity must be a positive integer")
        self._capacity = capacity
        self._items: dict[str, RejectedDelivery] = {}
        self._lock = asyncio.Lock()

    @property
    def items(self) -> tuple[RejectedDelivery, ...]:
        return tuple(self._items[key] for key in sorted(self._items))

    async def add(self, delivery: RejectedDelivery) -> bool:
        async with self._lock:
            if delivery.quarantine_id in self._items:
                return False
            if len(self._items) >= self._capacity:
                raise OverflowError("quarantine capacity exceeded")
            self._items[delivery.quarantine_id] = delivery
            return True


def rejected_delivery(
    *,
    quarantine_id: str,
    message_id: str,
    source: str,
    consumer: str,
    failed_at: datetime,
    reason: str,
    encoded_payload: bytes,
    policy: QuarantinePolicy,
    metadata: Mapping[str, JsonScalar] | None = None,
) -> RejectedDelivery:
    sanitized = " ".join(reason.split())[: policy.max_reason_chars]
    if not sanitized:
        sanitized = "rejected"
    selected_metadata = dict(metadata or {})
    if len(selected_metadata) > policy.max_metadata_items:
        raise ValueError("quarantine metadata exceeds max_metadata_items")
    if len(encoded_payload) > policy.max_payload_bytes:
        raise ValueError("quarantine payload exceeds max_payload_bytes")
    return RejectedDelivery(
        quarantine_id=quarantine_id,
        message_id=message_id,
        source=source,
        consumer=consumer,
        failed_at=failed_at,
        payload_sha256=hashlib.sha256(encoded_payload).hexdigest(),
        reason=sanitized,
        metadata=selected_metadata,
        payload=encoded_payload if policy.retain_payload else None,
    )
