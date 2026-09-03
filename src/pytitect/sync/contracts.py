"""Closed, serializable contracts for the versioned Pytitect sync bundle."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import cast

from pytitect.core import JsonValue, Limits, validate_json

SYNC_PROTOCOL = "titect-sync/1"

_TIMESTAMP = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")


@dataclass(frozen=True, slots=True)
class SyncLimits:
    """Finite decoding and negotiated page limits for ``titect-sync/1``."""

    max_document_bytes: int = 1_048_576
    max_datasets: int = 128
    max_items_per_page: int = 1_000
    max_mutations: int = 1_000
    max_opaque_id_bytes: int = 255
    max_capabilities: int = 32

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive finite integer")


DEFAULT_SYNC_LIMITS = SyncLimits()


class SyncMode(StrEnum):
    SNAPSHOT = "snapshot"
    DELTA = "delta"


class MutationOutcomeState(StrEnum):
    APPLIED = "applied"
    REJECTED = "rejected"
    CONFLICT = "conflict"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True, slots=True)
class SyncSession:
    session_id: str
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        _opaque_id(self.session_id, "session_id")
        _wire_time(self.created_at, "created_at")
        _wire_time(self.expires_at, "expires_at")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be later than created_at")


@dataclass(frozen=True, slots=True)
class DatasetDescriptor:
    dataset_id: str
    generation: int
    modes: tuple[SyncMode, ...]

    def __post_init__(self) -> None:
        _opaque_id(self.dataset_id, "dataset_id")
        _non_negative_int(self.generation, "generation")
        if (
            not self.modes
            or len(self.modes) > len(SyncMode)
            or len(set(self.modes)) != len(self.modes)
        ):
            raise ValueError("modes must contain unique supported sync modes")
        if any(not isinstance(mode, SyncMode) for mode in self.modes):
            raise ValueError("modes must contain SyncMode values")


@dataclass(frozen=True, slots=True)
class BootstrapRequest:
    client_id: str
    dataset_ids: tuple[str, ...]
    capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _opaque_id(self.client_id, "client_id")
        _bounded_ids(self.dataset_ids, "dataset_ids", DEFAULT_SYNC_LIMITS.max_datasets)
        _bounded_capabilities(self.capabilities)


@dataclass(frozen=True, slots=True)
class BootstrapResponse:
    session: SyncSession
    datasets: tuple[DatasetDescriptor, ...]
    limits: SyncLimits

    def __post_init__(self) -> None:
        if not isinstance(self.session, SyncSession) or not isinstance(self.limits, SyncLimits):
            raise ValueError("session and limits must use their sync contract types")
        if not self.datasets or len(self.datasets) > DEFAULT_SYNC_LIMITS.max_datasets:
            raise ValueError("datasets must be non-empty and finite")
        if any(not isinstance(dataset, DatasetDescriptor) for dataset in self.datasets):
            raise ValueError("datasets must contain DatasetDescriptor values")
        identifiers = [dataset.dataset_id for dataset in self.datasets]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("dataset identifiers must be unique")


@dataclass(frozen=True, slots=True)
class Upsert:
    item_id: str
    revision: int
    value: JsonValue

    def __post_init__(self) -> None:
        _opaque_id(self.item_id, "item_id")
        _non_negative_int(self.revision, "revision")
        validate_json(self.value)


@dataclass(frozen=True, slots=True)
class Tombstone:
    item_id: str
    revision: int
    deleted_at: datetime

    def __post_init__(self) -> None:
        _opaque_id(self.item_id, "item_id")
        _non_negative_int(self.revision, "revision")
        _wire_time(self.deleted_at, "deleted_at")


@dataclass(frozen=True, slots=True)
class Integrity:
    algorithm: str
    digest: str
    item_count: int

    def __post_init__(self) -> None:
        if self.algorithm != "sha-256":
            raise ValueError("algorithm must be sha-256")
        if not re.fullmatch(r"[0-9a-f]{64}", self.digest):
            raise ValueError("digest must be a lowercase SHA-256 hexadecimal value")
        _non_negative_int(self.item_count, "item_count")


@dataclass(frozen=True, slots=True)
class SnapshotPage:
    dataset_id: str
    generation: int
    upserts: tuple[Upsert, ...]
    next_cursor: str | None
    integrity: Integrity

    def __post_init__(self) -> None:
        _opaque_id(self.dataset_id, "dataset_id")
        _non_negative_int(self.generation, "generation")
        _bounded_items(self.upserts)
        if any(not isinstance(item, Upsert) for item in self.upserts):
            raise ValueError("upserts must contain Upsert values")
        _optional_id(self.next_cursor, "next_cursor")
        if not isinstance(self.integrity, Integrity):
            raise ValueError("integrity must use the Integrity contract")
        if self.integrity.item_count != len(self.upserts):
            raise ValueError("integrity item_count must match the page")


@dataclass(frozen=True, slots=True)
class DeltaPage:
    dataset_id: str
    generation: int
    upserts: tuple[Upsert, ...]
    tombstones: tuple[Tombstone, ...]
    next_cursor: str | None
    integrity: Integrity

    def __post_init__(self) -> None:
        _opaque_id(self.dataset_id, "dataset_id")
        _non_negative_int(self.generation, "generation")
        _bounded_items(self.upserts + self.tombstones)
        if any(not isinstance(item, Upsert) for item in self.upserts) or any(
            not isinstance(item, Tombstone) for item in self.tombstones
        ):
            raise ValueError("delta items must use Upsert and Tombstone contracts")
        _optional_id(self.next_cursor, "next_cursor")
        if not isinstance(self.integrity, Integrity):
            raise ValueError("integrity must use the Integrity contract")
        if self.integrity.item_count != len(self.upserts) + len(self.tombstones):
            raise ValueError("integrity item_count must match the page")


@dataclass(frozen=True, slots=True)
class ResetRequired:
    dataset_id: str
    generation: int
    reason: str

    def __post_init__(self) -> None:
        _opaque_id(self.dataset_id, "dataset_id")
        _non_negative_int(self.generation, "generation")
        _reason(self.reason)


@dataclass(frozen=True, slots=True)
class GenerationMismatch:
    dataset_id: str
    expected: int
    actual: int

    def __post_init__(self) -> None:
        _opaque_id(self.dataset_id, "dataset_id")
        _non_negative_int(self.expected, "expected")
        _non_negative_int(self.actual, "actual")


@dataclass(frozen=True, slots=True)
class Readiness:
    ready: bool
    checked_at: datetime
    reason: str | None = None
    retry_after_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.ready, bool):
            raise ValueError("ready must be a boolean")
        _wire_time(self.checked_at, "checked_at")
        if self.ready and (self.reason is not None or self.retry_after_ms is not None):
            raise ValueError("ready responses cannot contain retry details")
        if not self.ready:
            if self.reason is None:
                raise ValueError("unready responses require a reason")
            _reason(self.reason)
            if self.retry_after_ms is not None:
                _non_negative_int(self.retry_after_ms, "retry_after_ms")


@dataclass(frozen=True, slots=True)
class MutationOutcome:
    mutation_id: str
    state: MutationOutcomeState
    revision: int | None = None
    receipt_id: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        _opaque_id(self.mutation_id, "mutation_id")
        if not isinstance(self.state, MutationOutcomeState):
            raise ValueError("state must be a MutationOutcomeState")
        _optional_id(self.receipt_id, "receipt_id")
        if self.state is MutationOutcomeState.APPLIED:
            if self.revision is None or self.reason is not None:
                raise ValueError("applied outcomes require revision and forbid reason")
            _non_negative_int(self.revision, "revision")
        else:
            if self.revision is not None or self.reason is None:
                raise ValueError("non-applied outcomes require reason and forbid revision")
            _reason(self.reason)


@dataclass(frozen=True, slots=True)
class MutationOutcomes:
    dataset_id: str
    generation: int
    outcomes: tuple[MutationOutcome, ...]

    def __post_init__(self) -> None:
        _opaque_id(self.dataset_id, "dataset_id")
        _non_negative_int(self.generation, "generation")
        if not self.outcomes or len(self.outcomes) > DEFAULT_SYNC_LIMITS.max_mutations:
            raise ValueError("outcomes must be non-empty and within max_mutations")
        if any(not isinstance(outcome, MutationOutcome) for outcome in self.outcomes):
            raise ValueError("outcomes must contain MutationOutcome values")
        identifiers = [outcome.mutation_id for outcome in self.outcomes]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("mutation outcome identifiers must be unique")


type SyncDocument = (
    SyncSession
    | DatasetDescriptor
    | BootstrapRequest
    | BootstrapResponse
    | SnapshotPage
    | DeltaPage
    | ResetRequired
    | GenerationMismatch
    | Readiness
    | MutationOutcome
    | MutationOutcomes
)


def parse_sync_timestamp(value: str) -> datetime:
    """Parse the protocol's exact RFC 3339 UTC millisecond representation."""

    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        raise ValueError("timestamp must be RFC 3339 UTC with exactly three fractional digits")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError("timestamp is not a valid calendar instant") from error
    _wire_time(parsed, "timestamp")
    return parsed


def format_sync_timestamp(value: datetime) -> str:
    """Render a UTC datetime without silently discarding sub-millisecond precision."""

    _wire_time(value, "timestamp")
    milliseconds = value.microsecond // 1_000
    return (
        f"{value.year:04d}-{value.month:02d}-{value.day:02d}T"
        f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{milliseconds:03d}Z"
    )


def encode_sync_document(document: SyncDocument, *, limits: SyncLimits | None = None) -> JsonValue:
    """Encode one closed sync contract into its versioned wire envelope."""

    selected = limits or DEFAULT_SYNC_LIMITS
    kind, payload = _encode_payload(document)
    value: JsonValue = {"protocol": SYNC_PROTOCOL, "kind": kind, "payload": payload}
    _document_bounds(value, selected)
    return value


def decode_sync_document(value: JsonValue, *, limits: SyncLimits | None = None) -> SyncDocument:
    """Decode a closed sync envelope, rejecting unknown fields at every boundary."""

    selected = limits or DEFAULT_SYNC_LIMITS
    _document_bounds(value, selected)
    envelope = _object(value, "document")
    _fields(envelope, {"protocol", "kind", "payload"}, "document")
    if _string(envelope["protocol"], "protocol") != SYNC_PROTOCOL:
        raise ValueError(f"protocol must be {SYNC_PROTOCOL}")
    kind = _string(envelope["kind"], "kind")
    payload = _object(envelope["payload"], "payload")
    if kind == "session":
        return _decode_session(payload)
    if kind == "dataset":
        return _decode_dataset(payload)
    if kind == "bootstrap_request":
        return _decode_bootstrap_request(payload, selected)
    if kind == "bootstrap_response":
        return _decode_bootstrap_response(payload, selected)
    if kind == "snapshot":
        return _decode_snapshot(payload, selected)
    if kind == "delta":
        return _decode_delta(payload, selected)
    if kind == "reset_required":
        return _decode_reset(payload)
    if kind == "generation_mismatch":
        return _decode_generation_mismatch(payload)
    if kind == "readiness":
        return _decode_readiness(payload)
    if kind == "mutation_outcome":
        return _decode_mutation_outcome(payload)
    if kind == "mutation_outcomes":
        return _decode_mutation_outcomes(payload, selected)
    raise ValueError(f"unsupported sync document kind: {kind}")


def _encode_payload(document: SyncDocument) -> tuple[str, JsonValue]:
    if isinstance(document, SyncSession):
        return "session", _session_json(document)
    if isinstance(document, DatasetDescriptor):
        return "dataset", _dataset_json(document)
    if isinstance(document, BootstrapRequest):
        return "bootstrap_request", {
            "client_id": document.client_id,
            "dataset_ids": list(document.dataset_ids),
            "capabilities": list(document.capabilities),
        }
    if isinstance(document, BootstrapResponse):
        return "bootstrap_response", {
            "session": _session_json(document.session),
            "datasets": [_dataset_json(dataset) for dataset in document.datasets],
            "limits": _limits_json(document.limits),
        }
    if isinstance(document, SnapshotPage):
        return "snapshot", {
            "dataset_id": document.dataset_id,
            "generation": document.generation,
            "upserts": [_upsert_json(item) for item in document.upserts],
            "next_cursor": document.next_cursor,
            "integrity": _integrity_json(document.integrity),
        }
    if isinstance(document, DeltaPage):
        return "delta", {
            "dataset_id": document.dataset_id,
            "generation": document.generation,
            "upserts": [_upsert_json(item) for item in document.upserts],
            "tombstones": [_tombstone_json(item) for item in document.tombstones],
            "next_cursor": document.next_cursor,
            "integrity": _integrity_json(document.integrity),
        }
    if isinstance(document, ResetRequired):
        return "reset_required", {
            "dataset_id": document.dataset_id,
            "generation": document.generation,
            "reason": document.reason,
        }
    if isinstance(document, GenerationMismatch):
        return "generation_mismatch", {
            "dataset_id": document.dataset_id,
            "expected": document.expected,
            "actual": document.actual,
        }
    if isinstance(document, Readiness):
        return "readiness", {
            "ready": document.ready,
            "checked_at": format_sync_timestamp(document.checked_at),
            "reason": document.reason,
            "retry_after_ms": document.retry_after_ms,
        }
    if isinstance(document, MutationOutcome):
        return "mutation_outcome", {
            "mutation_id": document.mutation_id,
            "state": document.state.value,
            "revision": document.revision,
            "receipt_id": document.receipt_id,
            "reason": document.reason,
        }
    if isinstance(document, MutationOutcomes):
        return "mutation_outcomes", {
            "dataset_id": document.dataset_id,
            "generation": document.generation,
            "outcomes": [_mutation_outcome_json(outcome) for outcome in document.outcomes],
        }
    raise TypeError(f"unsupported sync document: {type(document).__name__}")


def _session_json(value: SyncSession) -> JsonValue:
    return {
        "session_id": value.session_id,
        "created_at": format_sync_timestamp(value.created_at),
        "expires_at": format_sync_timestamp(value.expires_at),
    }


def _dataset_json(value: DatasetDescriptor) -> JsonValue:
    return {
        "dataset_id": value.dataset_id,
        "generation": value.generation,
        "modes": [mode.value for mode in value.modes],
    }


def _limits_json(value: SyncLimits) -> JsonValue:
    return {name: getattr(value, name) for name in value.__dataclass_fields__}


def _upsert_json(value: Upsert) -> JsonValue:
    return {"item_id": value.item_id, "revision": value.revision, "value": value.value}


def _tombstone_json(value: Tombstone) -> JsonValue:
    return {
        "item_id": value.item_id,
        "revision": value.revision,
        "deleted_at": format_sync_timestamp(value.deleted_at),
    }


def _integrity_json(value: Integrity) -> JsonValue:
    return {
        "algorithm": value.algorithm,
        "digest": value.digest,
        "item_count": value.item_count,
    }


def _mutation_outcome_json(value: MutationOutcome) -> JsonValue:
    return {
        "mutation_id": value.mutation_id,
        "state": value.state.value,
        "revision": value.revision,
        "receipt_id": value.receipt_id,
        "reason": value.reason,
    }


def _decode_session(value: dict[str, object]) -> SyncSession:
    _fields(value, {"session_id", "created_at", "expires_at"}, "session")
    return SyncSession(
        _string(value["session_id"], "session_id"),
        parse_sync_timestamp(_string(value["created_at"], "created_at")),
        parse_sync_timestamp(_string(value["expires_at"], "expires_at")),
    )


def _decode_dataset(value: dict[str, object]) -> DatasetDescriptor:
    _fields(value, {"dataset_id", "generation", "modes"}, "dataset")
    modes = tuple(SyncMode(_string(item, "mode")) for item in _array(value["modes"], "modes"))
    return DatasetDescriptor(
        _string(value["dataset_id"], "dataset_id"),
        _integer(value["generation"], "generation"),
        modes,
    )


def _decode_bootstrap_request(value: dict[str, object], limits: SyncLimits) -> BootstrapRequest:
    _fields(value, {"client_id", "dataset_ids", "capabilities"}, "bootstrap_request")
    datasets = _strings(value["dataset_ids"], "dataset_ids", limits.max_datasets)
    capabilities = _strings(value["capabilities"], "capabilities", limits.max_capabilities)
    _ids_with_limit(datasets, "dataset_ids", limits)
    _capabilities_with_limit(capabilities, limits)
    client_id = _string(value["client_id"], "client_id")
    _opaque_id(client_id, "client_id", limits.max_opaque_id_bytes)
    return BootstrapRequest(client_id, datasets, capabilities)


def _decode_bootstrap_response(value: dict[str, object], limits: SyncLimits) -> BootstrapResponse:
    _fields(value, {"session", "datasets", "limits"}, "bootstrap_response")
    datasets_raw = _array(value["datasets"], "datasets")
    if not datasets_raw or len(datasets_raw) > limits.max_datasets:
        raise ValueError("datasets must be non-empty and within max_datasets")
    datasets = tuple(_decode_dataset(_object(item, "dataset")) for item in datasets_raw)
    return BootstrapResponse(
        _decode_session(_object(value["session"], "session")),
        datasets,
        _decode_limits(_object(value["limits"], "limits")),
    )


def _decode_limits(value: dict[str, object]) -> SyncLimits:
    names = set(SyncLimits.__dataclass_fields__)
    _fields(value, names, "limits")
    return SyncLimits(**{name: _integer(value[name], name) for name in names})


def _decode_upsert(value: dict[str, object], limits: SyncLimits) -> Upsert:
    _fields(value, {"item_id", "revision", "value"}, "upsert")
    item_id = _string(value["item_id"], "item_id")
    _opaque_id(item_id, "item_id", limits.max_opaque_id_bytes)
    payload = cast(JsonValue, value["value"])
    validate_json(payload)
    return Upsert(item_id, _integer(value["revision"], "revision"), payload)


def _decode_tombstone(value: dict[str, object], limits: SyncLimits) -> Tombstone:
    _fields(value, {"item_id", "revision", "deleted_at"}, "tombstone")
    item_id = _string(value["item_id"], "item_id")
    _opaque_id(item_id, "item_id", limits.max_opaque_id_bytes)
    return Tombstone(
        item_id,
        _integer(value["revision"], "revision"),
        parse_sync_timestamp(_string(value["deleted_at"], "deleted_at")),
    )


def _decode_integrity(value: dict[str, object]) -> Integrity:
    _fields(value, {"algorithm", "digest", "item_count"}, "integrity")
    return Integrity(
        _string(value["algorithm"], "algorithm"),
        _string(value["digest"], "digest"),
        _integer(value["item_count"], "item_count"),
    )


def _decode_snapshot(value: dict[str, object], limits: SyncLimits) -> SnapshotPage:
    _fields(value, {"dataset_id", "generation", "upserts", "next_cursor", "integrity"}, "snapshot")
    dataset_id = _string(value["dataset_id"], "dataset_id")
    _opaque_id(dataset_id, "dataset_id", limits.max_opaque_id_bytes)
    upserts_raw = _array(value["upserts"], "upserts")
    if len(upserts_raw) > limits.max_items_per_page:
        raise ValueError("upserts exceed max_items_per_page")
    return SnapshotPage(
        dataset_id,
        _integer(value["generation"], "generation"),
        tuple(_decode_upsert(_object(item, "upsert"), limits) for item in upserts_raw),
        _nullable_string(value["next_cursor"], "next_cursor"),
        _decode_integrity(_object(value["integrity"], "integrity")),
    )


def _decode_delta(value: dict[str, object], limits: SyncLimits) -> DeltaPage:
    _fields(
        value,
        {"dataset_id", "generation", "upserts", "tombstones", "next_cursor", "integrity"},
        "delta",
    )
    dataset_id = _string(value["dataset_id"], "dataset_id")
    _opaque_id(dataset_id, "dataset_id", limits.max_opaque_id_bytes)
    upserts_raw = _array(value["upserts"], "upserts")
    tombstones_raw = _array(value["tombstones"], "tombstones")
    if len(upserts_raw) + len(tombstones_raw) > limits.max_items_per_page:
        raise ValueError("delta items exceed max_items_per_page")
    return DeltaPage(
        dataset_id,
        _integer(value["generation"], "generation"),
        tuple(_decode_upsert(_object(item, "upsert"), limits) for item in upserts_raw),
        tuple(_decode_tombstone(_object(item, "tombstone"), limits) for item in tombstones_raw),
        _nullable_string(value["next_cursor"], "next_cursor"),
        _decode_integrity(_object(value["integrity"], "integrity")),
    )


def _decode_reset(value: dict[str, object]) -> ResetRequired:
    _fields(value, {"dataset_id", "generation", "reason"}, "reset_required")
    return ResetRequired(
        _string(value["dataset_id"], "dataset_id"),
        _integer(value["generation"], "generation"),
        _string(value["reason"], "reason"),
    )


def _decode_generation_mismatch(value: dict[str, object]) -> GenerationMismatch:
    _fields(value, {"dataset_id", "expected", "actual"}, "generation_mismatch")
    return GenerationMismatch(
        _string(value["dataset_id"], "dataset_id"),
        _integer(value["expected"], "expected"),
        _integer(value["actual"], "actual"),
    )


def _decode_readiness(value: dict[str, object]) -> Readiness:
    _fields(value, {"ready", "checked_at", "reason", "retry_after_ms"}, "readiness")
    ready = value["ready"]
    if not isinstance(ready, bool):
        raise ValueError("ready must be a boolean")
    retry_raw = value["retry_after_ms"]
    return Readiness(
        ready,
        parse_sync_timestamp(_string(value["checked_at"], "checked_at")),
        _nullable_string(value["reason"], "reason"),
        None if retry_raw is None else _integer(retry_raw, "retry_after_ms"),
    )


def _decode_mutation_outcome(value: dict[str, object]) -> MutationOutcome:
    _fields(
        value,
        {"mutation_id", "state", "revision", "receipt_id", "reason"},
        "mutation_outcome",
    )
    revision_raw = value["revision"]
    return MutationOutcome(
        _string(value["mutation_id"], "mutation_id"),
        MutationOutcomeState(_string(value["state"], "state")),
        None if revision_raw is None else _integer(revision_raw, "revision"),
        _nullable_string(value["receipt_id"], "receipt_id"),
        _nullable_string(value["reason"], "reason"),
    )


def _decode_mutation_outcomes(value: dict[str, object], limits: SyncLimits) -> MutationOutcomes:
    _fields(value, {"dataset_id", "generation", "outcomes"}, "mutation_outcomes")
    dataset_id = _string(value["dataset_id"], "dataset_id")
    _opaque_id(dataset_id, "dataset_id", limits.max_opaque_id_bytes)
    outcomes_raw = _array(value["outcomes"], "outcomes")
    if not outcomes_raw or len(outcomes_raw) > limits.max_mutations:
        raise ValueError("outcomes must be non-empty and within max_mutations")
    return MutationOutcomes(
        dataset_id,
        _integer(value["generation"], "generation"),
        tuple(_decode_mutation_outcome(_object(item, "mutation_outcome")) for item in outcomes_raw),
    )


def _document_bounds(value: JsonValue, limits: SyncLimits) -> None:
    validate_json(
        value,
        limits=Limits(
            max_body_bytes=limits.max_document_bytes,
            max_json_depth=32,
            max_json_items=max(10_000, limits.max_items_per_page * 8),
            max_metadata_items=64,
            max_string_length=limits.max_document_bytes,
        ),
    )
    size = len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())
    if size > limits.max_document_bytes:
        raise ValueError("sync document exceeds max_document_bytes")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be a JSON object")
    return cast(dict[str, object], value)


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a JSON array")
    return cast(list[object], value)


def _fields(value: dict[str, object], expected: set[str], name: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise ValueError(f"{name} fields are closed; missing={missing}, unknown={unknown}")


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _nullable_string(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _string(value, name)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _strings(value: object, name: str, maximum: int) -> tuple[str, ...]:
    values = _array(value, name)
    if len(values) > maximum:
        raise ValueError(f"{name} exceeds its finite limit")
    return tuple(_string(item, name) for item in values)


def _non_negative_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _wire_time(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.microsecond % 1_000:
        raise ValueError(f"{name} must have millisecond precision")


def _opaque_id(value: str, name: str, maximum: int | None = None) -> None:
    selected = maximum or DEFAULT_SYNC_LIMITS.max_opaque_id_bytes
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 0x20 for character in value)
    ):
        raise ValueError(f"{name} must be a non-empty trimmed opaque identifier")
    if len(value.encode()) > selected:
        raise ValueError(f"{name} exceeds max_opaque_id_bytes")


def _optional_id(value: str | None, name: str) -> None:
    if value is not None:
        _opaque_id(value, name)


def _bounded_ids(values: tuple[str, ...], name: str, maximum: int) -> None:
    if not values or len(values) > maximum or len(set(values)) != len(values):
        raise ValueError(f"{name} must be non-empty, unique, and finite")
    for value in values:
        _opaque_id(value, name)


def _ids_with_limit(values: tuple[str, ...], name: str, limits: SyncLimits) -> None:
    if not values or len(set(values)) != len(values):
        raise ValueError(f"{name} must be non-empty and unique")
    for value in values:
        _opaque_id(value, name, limits.max_opaque_id_bytes)


def _bounded_capabilities(values: tuple[str, ...]) -> None:
    _capabilities_with_limit(values, DEFAULT_SYNC_LIMITS)


def _capabilities_with_limit(values: tuple[str, ...], limits: SyncLimits) -> None:
    if len(values) > limits.max_capabilities or len(set(values)) != len(values):
        raise ValueError("capabilities must be unique and finite")
    for value in values:
        _opaque_id(value, "capability", min(64, limits.max_opaque_id_bytes))


def _bounded_items(values: tuple[object, ...]) -> None:
    if len(values) > DEFAULT_SYNC_LIMITS.max_items_per_page:
        raise ValueError("page exceeds max_items_per_page")


def _reason(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode()) > 1_024
    ):
        raise ValueError("reason must be non-empty, trimmed, and at most 1024 bytes")
