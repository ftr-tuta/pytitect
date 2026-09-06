"""Preview raw sync boundaries and explicitly negotiated page integrity."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from typing import Protocol

from pytitect.core import JsonValue, Limits
from pytitect.sync.contracts import (
    SYNC_PROTOCOL,
    SyncDocument,
    SyncLimits,
    _decode_bounded_document,
)
from pytitect.wire import (
    ExactNumber,
    ExactValue,
    WireDocument,
    WireIntegrityError,
    WirePrecisionError,
    WireProfileError,
    WireShapeError,
    decode_wire_stream,
)

EXACT_JSON_INTEGRITY = "integrity-sha-256-exact-json-v1"
SYNC_INTEGRITY_HEADER = "Titect-Sync-Integrity"
_INTEGRITY_PREFIX = b"titect-sync/1\0integrity-sha-256-exact-json-v1\0"


class SyncIntegrityPolicy(Protocol):
    """Consumer-injected policy; owns no session, transport, persistence, or effects."""

    @property
    def capability(self) -> str: ...

    def verify(self, document: WireDocument) -> None: ...


def _object(value: ExactValue) -> Mapping[str, ExactValue]:
    if not isinstance(value, Mapping):
        raise WireShapeError()
    return value


def _page(document: WireDocument) -> tuple[Mapping[str, ExactValue], Mapping[str, ExactValue], int]:
    envelope = _object(document.value)
    if envelope.get("protocol") != SYNC_PROTOCOL or envelope.get("kind") not in (
        "snapshot",
        "delta",
    ):
        raise WireShapeError()
    payload = _object(envelope.get("payload"))
    upserts = payload.get("upserts")
    tombstones = payload.get("tombstones", ())
    if not isinstance(upserts, tuple) or not isinstance(tombstones, tuple):
        raise WireShapeError()
    return envelope, payload, len(upserts) + len(tombstones)


class ExactJsonSha256Integrity:
    """SHA-256 of the domain prefix and complete envelope minus payload.integrity."""

    capability = EXACT_JSON_INTEGRITY

    def digest(self, document: WireDocument) -> str:
        envelope, payload, _ = _page(document)
        preimage = dict(envelope)
        preimage["payload"] = {key: value for key, value in payload.items() if key != "integrity"}
        encoded = WireDocument(preimage, limits=document.limits).encode()
        return hashlib.sha256(_INTEGRITY_PREFIX + encoded).hexdigest()

    def seal(self, document: WireDocument) -> WireDocument:
        """Produce a sealed page; the caller owns capability negotiation and publication."""

        envelope, payload, count = _page(document)
        sealed = dict(envelope)
        sealed["payload"] = {
            **payload,
            "integrity": {
                "algorithm": "sha-256",
                "digest": self.digest(document),
                "item_count": ExactNumber(str(count)),
            },
        }
        result = WireDocument(sealed, limits=document.limits)
        _validate_shape(result, SyncLimits(max_document_bytes=document.limits.max_body_bytes))
        return result

    def verify(self, document: WireDocument) -> None:
        _, payload, count = _page(document)
        integrity = payload.get("integrity")
        if not isinstance(integrity, Mapping) or set(integrity) != {
            "algorithm",
            "digest",
            "item_count",
        }:
            raise WireIntegrityError()
        digest = integrity["digest"]
        item_count = integrity["item_count"]
        count_matches = False
        if isinstance(item_count, ExactNumber):
            with suppress(WirePrecisionError):
                count_matches = item_count.to_int() == count
        if (
            integrity["algorithm"] != "sha-256"
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not count_matches
            or not hmac.compare_digest(digest, self.digest(document))
        ):
            raise WireIntegrityError()


@dataclass(frozen=True, slots=True)
class SyncIntegritySelection:
    """Explicit session context; consumers persist the selected capability themselves."""

    policy: SyncIntegrityPolicy | None = None
    capability: str | None = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "capability", None if self.policy is None else self.policy.capability
        )
        if self.policy is not None and not self.capability:
            raise WireProfileError()

    def acknowledge(self, response_header: str | None) -> None:
        current = None if self.policy is None else self.policy.capability
        if response_header != self.capability or current != self.capability:
            raise WireIntegrityError()


def select_sync_integrity(
    requested: tuple[str, ...],
    acknowledgement: str | None,
    *,
    policies: Iterable[SyncIntegrityPolicy],
) -> SyncIntegritySelection:
    """Require exactly the requested acknowledgement; never downgrade or select a fallback."""

    registry: dict[str, SyncIntegrityPolicy] = {}
    for policy in policies:
        if not policy.capability or policy.capability in registry:
            raise WireProfileError()
        registry[policy.capability] = policy
    requested_integrity = [
        name for name in requested if name.startswith("integrity-") and name != "integrity-sha-256"
    ]
    if any(name not in registry for name in requested_integrity):
        raise WireProfileError()
    if not requested_integrity:
        selection = SyncIntegritySelection()
    else:
        if len(requested_integrity) != 1:
            raise WireProfileError()
        selection = SyncIntegritySelection(registry[requested_integrity[0]])
    selection.acknowledge(acknowledgement)
    return selection


def _shape_value(value: ExactValue) -> JsonValue:
    if isinstance(value, ExactNumber):
        return value.to_int()
    if isinstance(value, tuple):
        return [_shape_value(item) for item in value]
    if isinstance(value, Mapping):
        return {key: _shape_value(item) for key, item in value.items()}
    return value


def _validate_shape(document: WireDocument, limits: SyncLimits) -> None:
    envelope = _object(document.value)
    if not isinstance(envelope.get("protocol"), str):
        raise WireShapeError()
    if envelope["protocol"] != SYNC_PROTOCOL:
        raise WireProfileError()
    # Arbitrary upsert data stays exact; validate only the closed contract fields
    # through the existing mapping validator. Nothing is applied or checkpointed.
    projected = dict(envelope)
    payload = envelope.get("payload")
    if isinstance(payload, Mapping) and envelope.get("kind") in ("snapshot", "delta"):
        projected_payload = dict(payload)
        upserts = payload.get("upserts")
        if isinstance(upserts, tuple):
            for item in upserts:
                if isinstance(item, Mapping) and "value" in item:
                    WireDocument(item["value"])
            projected_payload["upserts"] = tuple(
                {**item, "value": None} if isinstance(item, Mapping) and "value" in item else item
                for item in upserts
            )
        projected["payload"] = projected_payload
    try:
        _decode_bounded_document(_shape_value(projected), limits)
    except (ValueError, TypeError, OverflowError):
        pass
    else:
        return
    raise WireShapeError()


@dataclass(frozen=True, slots=True)
class SyncWireDocument:
    """Validated exact sync envelope, optionally verified before caller-owned application."""

    document: WireDocument
    limits: SyncLimits

    def __post_init__(self) -> None:
        _validate_shape(self.document, self.limits)

    def encode(self) -> bytes:
        return self.document.encode()

    def to_contract(self) -> SyncDocument:
        """Explicit checked conversion to the existing mapping contracts."""

        return _decode_bounded_document(self.document.to_json(), self.limits)


def decode_sync_raw(
    payload: bytes,
    *,
    limits: SyncLimits | None = None,
    wire_limits: Limits | None = None,
    integrity: SyncIntegritySelection | None = None,
    acknowledgement: str | None = None,
) -> SyncWireDocument:
    return decode_sync_stream(
        (payload,),
        limits=limits,
        wire_limits=wire_limits,
        integrity=integrity,
        acknowledgement=acknowledgement,
    )


def decode_sync_stream(
    chunks: Iterable[bytes],
    *,
    limits: SyncLimits | None = None,
    wire_limits: Limits | None = None,
    integrity: SyncIntegritySelection | None = None,
    acknowledgement: str | None = None,
) -> SyncWireDocument:
    selected = limits or SyncLimits()
    allocation = wire_limits or Limits(
        max_body_bytes=selected.max_document_bytes,
        max_json_items=max(10_000, selected.max_items_per_page * 8),
        max_string_length=selected.max_document_bytes,
    )
    allocation = replace(
        allocation, max_body_bytes=min(allocation.max_body_bytes, selected.max_document_bytes)
    )
    selection = integrity or SyncIntegritySelection()
    selection.acknowledge(acknowledgement)
    document = decode_wire_stream(chunks, limits=allocation)
    envelope = _object(document.value)
    if selection.policy is not None and envelope.get("kind") in ("snapshot", "delta"):
        selection.policy.verify(document)
    return SyncWireDocument(document, selected)
