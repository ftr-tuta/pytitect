from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pytitect.sync import (
    SYNC_PROTOCOL,
    BootstrapRequest,
    BootstrapResponse,
    DatasetDescriptor,
    DeltaPage,
    GenerationMismatch,
    Integrity,
    MutationOutcome,
    MutationOutcomes,
    MutationOutcomeState,
    Readiness,
    ResetRequired,
    SnapshotPage,
    SyncLimits,
    SyncMode,
    SyncSession,
    Tombstone,
    Upsert,
    decode_sync_document,
    encode_sync_document,
    format_sync_timestamp,
    parse_sync_timestamp,
)

NOW = datetime(2026, 1, 1, 0, 0, 0, 123_000, tzinfo=UTC)
DIGEST = "a" * 64


@pytest.mark.parametrize(
    "document",
    [
        SyncSession("session-1", NOW, NOW + timedelta(hours=1)),
        DatasetDescriptor("dataset-1", 3, (SyncMode.SNAPSHOT, SyncMode.DELTA)),
        BootstrapRequest("client-1", ("dataset-1",), ("delta",)),
        BootstrapResponse(
            SyncSession("session-1", NOW, NOW + timedelta(hours=1)),
            (DatasetDescriptor("dataset-1", 3, (SyncMode.SNAPSHOT,)),),
            SyncLimits(),
        ),
        SnapshotPage(
            "dataset-1",
            3,
            (Upsert("item-1", 4, {"value": True}),),
            "cursor-2",
            Integrity("sha-256", DIGEST, 1),
        ),
        DeltaPage(
            "dataset-1",
            3,
            (Upsert("item-1", 4, {"value": True}),),
            (Tombstone("item-2", 5, NOW),),
            None,
            Integrity("sha-256", DIGEST, 2),
        ),
        ResetRequired("dataset-1", 4, "retention window elapsed"),
        GenerationMismatch("dataset-1", 3, 4),
        Readiness(True, NOW),
        Readiness(False, NOW, "temporarily unavailable", 500),
        MutationOutcome("mutation-1", MutationOutcomeState.APPLIED, 4, "receipt-1"),
        MutationOutcome(
            "mutation-2", MutationOutcomeState.UNCERTAIN, reason="outcome is not proven"
        ),
        MutationOutcomes(
            "dataset-1",
            3,
            (MutationOutcome("mutation-1", MutationOutcomeState.APPLIED, 4),),
        ),
    ],
)
def test_sync_contracts_round_trip(document: object) -> None:
    encoded = encode_sync_document(document)  # type: ignore[arg-type]
    assert isinstance(encoded, dict)
    assert encoded["protocol"] == SYNC_PROTOCOL
    assert decode_sync_document(encoded) == document


def test_timestamps_require_fixed_utc_milliseconds() -> None:
    assert format_sync_timestamp(NOW) == "2026-01-01T00:00:00.123Z"
    assert parse_sync_timestamp("2026-01-01T00:00:00.123Z") == NOW
    for value in (
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00.1234Z",
        "2026-01-01T00:00:00.123+00:00",
        "2026-13-01T00:00:00.123Z",
    ):
        with pytest.raises(ValueError):
            parse_sync_timestamp(value)
    with pytest.raises(ValueError):
        format_sync_timestamp(NOW.replace(microsecond=123_456))
    with pytest.raises(ValueError):
        format_sync_timestamp(NOW.replace(tzinfo=None))


def test_sync_decoder_rejects_unknown_fields_versions_types_and_limits() -> None:
    valid = encode_sync_document(BootstrapRequest("client", ("dataset",)))
    assert isinstance(valid, dict)
    valid["unknown"] = True
    with pytest.raises(ValueError, match="unknown"):
        decode_sync_document(valid)

    valid = encode_sync_document(BootstrapRequest("client", ("dataset",)))
    assert isinstance(valid, dict) and isinstance(valid["payload"], dict)
    valid["payload"]["unknown"] = True
    with pytest.raises(ValueError, match="unknown"):
        decode_sync_document(valid)

    wrong_protocol = encode_sync_document(Readiness(True, NOW))
    assert isinstance(wrong_protocol, dict)
    wrong_protocol["protocol"] = "titect-sync/2"
    with pytest.raises(ValueError, match="protocol"):
        decode_sync_document(wrong_protocol)

    oversized = encode_sync_document(BootstrapRequest("client", ("dataset",)))
    with pytest.raises(ValueError, match="max_opaque_id_bytes"):
        decode_sync_document(oversized, limits=SyncLimits(max_opaque_id_bytes=3))

    boolean_generation = encode_sync_document(GenerationMismatch("dataset", 1, 2))
    assert isinstance(boolean_generation, dict) and isinstance(boolean_generation["payload"], dict)
    boolean_generation["payload"]["expected"] = True
    with pytest.raises(ValueError, match="integer"):
        decode_sync_document(boolean_generation)


def test_sync_contract_invariants_are_closed() -> None:
    with pytest.raises(ValueError):
        BootstrapRequest("client", ())
    with pytest.raises(ValueError):
        DatasetDescriptor("dataset", 1, (SyncMode.DELTA, SyncMode.DELTA))
    with pytest.raises(ValueError):
        SnapshotPage("dataset", 1, (), None, Integrity("sha-256", DIGEST, 1))
    with pytest.raises(ValueError):
        MutationOutcome("mutation", MutationOutcomeState.APPLIED, reason="bad")
    with pytest.raises(ValueError):
        MutationOutcome("mutation", MutationOutcomeState.REJECTED)
    with pytest.raises(ValueError):
        MutationOutcomes("dataset", 1, ())
    with pytest.raises(ValueError):
        Readiness(True, NOW, "not ready")
    with pytest.raises(ValueError):
        SyncLimits(max_datasets=0)
