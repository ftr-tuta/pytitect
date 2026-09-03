from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pytitect.idempotency import (
    Conflict,
    Execute,
    IdempotencyPolicy,
    IdempotencyScope,
    InMemoryIdempotencyStore,
    InProgress,
    Replay,
    RequestFingerprint,
    ReservationToken,
    StaleReservation,
    Uncertain,
)
from pytitect.leases import (
    FencedCommit,
    InMemoryLeaseStore,
    Lease,
    LeaseAcquired,
    LeaseAuthority,
    LeaseReleased,
    StaleLease,
)
from pytitect.maintenance import PurgeIdempotencyPlan
from pytitect.security import (
    ContentDigestVerifier,
    InMemoryReplayStore,
    access_token_hash,
    base64url_decode,
    base64url_encode,
    canonical_json,
    parse_ijson,
    validate_ijson,
)
from pytitect.security.encoding import jwk_thumbprint
from pytitect.sync import (
    ALL_OR_NOTHING,
    PER_ITEM,
    BatchConflict,
    BatchInProgress,
    BatchItem,
    BatchItemReceipt,
    BatchItemsCommittedEnvelopeUnconfirmed,
    BatchLimits,
    BatchUncertain,
    BootstrapRequest,
    BootstrapResponse,
    CursorAlgorithm,
    CursorDecoded,
    CursorLimits,
    CursorRejected,
    DatasetDependencyGraph,
    DatasetDescriptor,
    DeltaPage,
    DependencyGraphLimits,
    DependencyLimitExceeded,
    DependencyOrder,
    GenerationGuard,
    InMemoryGenerationStore,
    InMemoryMutationBatchStore,
    Integrity,
    MutationBatchCoordinator,
    MutationBatchLease,
    MutationBatchState,
    MutationOutcome,
    MutationOutcomes,
    MutationOutcomeState,
    OpaqueCursorCodec,
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
)
from pytitect.sync.batches import StaleMutationBatchLease, _batch_decision
from pytitect.trace import TraceContext, parse_trace_context, parse_tracestate

NOW = datetime(2026, 1, 1, tzinfo=UTC)
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"
DIGEST = "a" * 64
POLICY = IdempotencyPolicy(timedelta(seconds=5), timedelta(minutes=1), timedelta(hours=1))


@contextmanager
def atomic():
    yield


class Transaction:
    def atomic(self):
        return atomic()


def test_idempotency_validation_capacity_stale_and_purge_edges() -> None:
    for parts in (("", "subject", "operation"), ("ns", "", "operation"), ("ns", "s", "")):
        with pytest.raises(ValueError):
            IdempotencyScope(*parts)
    with pytest.raises(ValueError):
        ReservationToken("")
    for values in (
        (timedelta(0), timedelta(1), timedelta(1)),
        (timedelta(1), timedelta(0), timedelta(1)),
        (timedelta(1), timedelta(1), timedelta(0)),
    ):
        with pytest.raises(ValueError):
            IdempotencyPolicy(*values)
    for capacity in (True, 0, "1"):
        with pytest.raises(ValueError):
            InMemoryIdempotencyStore(capacity=capacity)  # type: ignore[arg-type]

    scope = IdempotencyScope("ns", "subject", "operation")
    fingerprint = RequestFingerprint.from_json({"value": 1})
    store = InMemoryIdempotencyStore[int](capacity=1)
    with pytest.raises(ValueError):
        store.reserve(
            scope, "key", fingerprint, now=NOW.replace(tzinfo=None), lease_ttl=timedelta(1)
        )
    with pytest.raises(ValueError):
        store.reserve(scope, "", fingerprint, now=NOW, lease_ttl=timedelta(1))
    with pytest.raises(ValueError):
        store.reserve(scope, "key", fingerprint, now=NOW, lease_ttl=timedelta(0))

    decision = store.reserve(scope, "key", fingerprint, now=NOW, lease_ttl=timedelta(1))
    assert isinstance(decision, Execute)
    assert isinstance(
        store.reserve(scope, "full", fingerprint, now=NOW, lease_ttl=timedelta(1)), Uncertain
    )
    unknown = ReservationToken("unknown")
    assert isinstance(store.renew(unknown, now=NOW, lease_ttl=timedelta(1)), StaleReservation)
    assert isinstance(
        store.complete(unknown, 1, now=NOW, retention_ttl=timedelta(1)), StaleReservation
    )
    assert isinstance(
        store.mark_uncertain(unknown, "unknown", now=NOW, retention_ttl=timedelta(1)),
        StaleReservation,
    )
    assert isinstance(store.abandon(unknown, now=NOW), StaleReservation)
    with pytest.raises(ValueError):
        store.mark_uncertain(decision.token, "", now=NOW, retention_ttl=timedelta(1))
    assert isinstance(
        store.renew(decision.token, now=NOW + timedelta(1), lease_ttl=timedelta(1)),
        StaleReservation,
    )

    replacement = store.reserve(
        scope, "key", fingerprint, now=NOW + timedelta(1), lease_ttl=timedelta(1)
    )
    assert isinstance(replacement, Execute)
    store.mark_uncertain(
        replacement.token,
        "unresolved",
        now=NOW + timedelta(1),
        retention_ttl=timedelta(1),
    )
    dry = store.purge(
        PurgeIdempotencyPlan(NOW + timedelta(2), include_uncertain=True, dry_run=True)
    )
    assert (dry.selected, dry.affected) == (1, 0)
    removed = store.purge(PurgeIdempotencyPlan(NOW + timedelta(2), include_uncertain=True))
    assert (removed.selected, removed.affected) == (1, 1)


def test_lease_validation_and_every_fence_rejection() -> None:
    for lease in (
        lambda: Lease("r", "", 1, NOW),
        lambda: Lease("r", "owner", 0, NOW),
        lambda: LeaseAuthority("", 1, NOW),
        lambda: LeaseAuthority("owner", 0, NOW),
    ):
        with pytest.raises(ValueError):
            lease()
    for capacity in (True, 0, "1"):
        with pytest.raises(ValueError):
            InMemoryLeaseStore(capacity=capacity)  # type: ignore[arg-type]

    store = InMemoryLeaseStore[str]()
    with pytest.raises(ValueError):
        store.acquire("r", owner="", now=NOW, ttl=timedelta(1))
    with pytest.raises(ValueError):
        store.acquire("r", owner="owner", now=NOW, ttl=timedelta(0))
    with pytest.raises(ValueError):
        store.acquire("r", owner="owner", now=NOW.replace(tzinfo=None), ttl=timedelta(1))
    acquired = store.acquire("r", owner="owner", now=NOW, ttl=timedelta(1))
    assert isinstance(acquired, LeaseAcquired)
    with pytest.raises(ValueError):
        store.renew(acquired.lease, now=NOW, ttl=timedelta(0))
    assert isinstance(store.release(acquired.lease, now=NOW + timedelta(1)), StaleLease)
    assert store.authority("r") == 1
    assert store.locked_authority("r", lambda: "locked") == "locked"

    lease = acquired.lease
    authorities = (
        None,
        LeaseAuthority("owner", 2, NOW + timedelta(2)),
        LeaseAuthority("other", 1, NOW + timedelta(2)),
        LeaseAuthority("owner", 1, NOW),
    )
    for authority in authorities:
        fenced = FencedCommit(
            lambda resource, compare, authority=authority: compare(authority),
            clock=lambda: NOW,
        )
        assert isinstance(fenced.commit(lease, lambda: "must not run"), StaleLease)

    active = store.acquire("r", owner="next", now=NOW + timedelta(1), ttl=timedelta(1))
    assert isinstance(active, LeaseAcquired)
    assert isinstance(store.release(active.lease, now=NOW + timedelta(1)), LeaseReleased)
    assert store.authority("r") is None


def _cursor_token(header: object, body: bytes = b"x", auth: bytes = b"x") -> str:
    return ".".join(
        (
            base64url_encode(canonical_json(cast(Any, header))),
            base64url_encode(body),
            base64url_encode(auth),
        )
    )


def test_cursor_codec_rejects_every_bounded_envelope_edge() -> None:
    key = b"k" * 32
    codec = OpaqueCursorCodec(lambda kid, algorithm: key if kid == "known" else None)
    for limits in (
        lambda: CursorLimits(max_token_bytes=0),
        lambda: CursorLimits(max_payload_bytes=True),
        lambda: CursorLimits(max_context_length="1"),
    ):
        with pytest.raises(ValueError):
            limits()
    with pytest.raises(ValueError):
        codec.encode(b"x", dataset="d", partition="p", kid="missing")
    with pytest.raises(ValueError, match="empty"):
        codec.encode(b"", dataset="d", partition="p", kid="known")
    with pytest.raises(ValueError):
        codec.encode(
            b"xx", dataset="d", partition="p", kid="known", expires_at=NOW, algorithm="bad"
        )
    with pytest.raises(ValueError):
        OpaqueCursorCodec({"known": key}, limits=CursorLimits(max_payload_bytes=1)).encode(
            b"xx", dataset="d", partition="p", kid="known"
        )
    for name, values in (
        ("dataset", ("", " d")),
        ("partition", ("", "p ")),
        ("kid", ("", " k")),
    ):
        for invalid in values:
            arguments = {"dataset": "d", "partition": "p", "kid": "known"}
            arguments[name] = invalid
            with pytest.raises(ValueError):
                codec.encode(b"x", **arguments)
    with pytest.raises(ValueError):
        codec.encode(
            b"x",
            dataset="d",
            partition="p",
            kid="known",
            expires_at=NOW.replace(tzinfo=None),
        )
    with pytest.raises(ValueError):
        OpaqueCursorCodec({"known": b"short"}).encode(b"x", dataset="d", partition="p", kid="known")
    with pytest.raises(ValueError):
        OpaqueCursorCodec({"known": b"x" * 31}).encode(
            b"x", dataset="d", partition="p", kid="known", algorithm=CursorAlgorithm.A256GCM
        )
    with pytest.raises(ValueError):
        OpaqueCursorCodec({"known": key}, nonce_factory=lambda size: b"x").encode(
            b"x", dataset="d", partition="p", kid="known", algorithm=CursorAlgorithm.A256GCM
        )
    with pytest.raises(ValueError):
        OpaqueCursorCodec({"known": key}, limits=CursorLimits(max_token_bytes=1)).encode(
            b"x", dataset="d", partition="p", kid="known"
        )

    tiny = OpaqueCursorCodec({"known": key}, limits=CursorLimits(max_token_bytes=2))
    assert tiny.decode("abc", dataset="d", partition="p").code == "cursor_too_large"
    assert codec.decode("bad", dataset="d", partition="p").code == "malformed"
    assert (
        codec.decode(_cursor_token([], auth=b"x"), dataset="d", partition="p").code
        == "invalid_header"
    )
    noncanonical = base64url_encode(b'{"v": 1}') + ".eA.eA"
    assert codec.decode(noncanonical, dataset="d", partition="p").code == "noncanonical_header"

    base: dict[str, Any] = {
        "alg": "HS256",
        "dataset": "d",
        "kid": "known",
        "partition": "p",
        "v": 1,
    }
    mutations = (
        ({**base, "unknown": True}, "invalid_header"),
        ({**base, "v": 2}, "unsupported_version"),
        ({**base, "alg": "bad"}, "unsupported_algorithm"),
        ({**base, "kid": ""}, "invalid_header"),
        ({**base, "dataset": " d"}, "invalid_header"),
        ({**base, "dataset": "other"}, "context_mismatch"),
        ({**base, "exp": True}, "invalid_header"),
        ({**base, "exp": 9_000_000_000_000_000}, "invalid_header"),
        ({**base, "nonce": "eA"}, "invalid_header"),
        ({**base, "kid": "missing"}, "unknown_key"),
    )
    for header, code in mutations:
        rejected = codec.decode(_cursor_token(header), dataset="d", partition="p", now=NOW)
        assert isinstance(rejected, CursorRejected) and rejected.code == code
    assert (
        codec.decode(_cursor_token(base), dataset="d", partition="p", now=NOW).code
        == "invalid_auth"
    )

    aes = {**base, "alg": "A256GCM"}
    assert (
        codec.decode(_cursor_token(aes), dataset="d", partition="p", now=NOW).code
        == "invalid_nonce"
    )
    aes["nonce"] = base64url_encode(b"short")
    assert (
        codec.decode(_cursor_token(aes), dataset="d", partition="p", now=NOW).code
        == "invalid_nonce"
    )
    aes["nonce"] = base64url_encode(b"n" * 12)
    assert (
        codec.decode(_cursor_token(aes), dataset="d", partition="p", now=NOW).code
        == "invalid_nonce"
    )
    assert (
        codec.decode(_cursor_token(aes, auth=b"x" * 16), dataset="d", partition="p", now=NOW).code
        == "invalid_auth"
    )

    valid = codec.encode(b"long", dataset="d", partition="p", kid="known")
    bounded = OpaqueCursorCodec({"known": key}, limits=CursorLimits(max_payload_bytes=1))
    assert bounded.decode(valid, dataset="d", partition="p", now=NOW).code == "payload_too_large"
    assert (
        codec.decode(valid, dataset="d", partition="p", now=NOW.replace(tzinfo=None)).code
        == "malformed"
    )


@given(st.binary(min_size=1, max_size=128))
def test_cursor_hypothesis_round_trip(payload: bytes) -> None:
    codec = OpaqueCursorCodec({"key": b"k" * 32})
    decoded = codec.decode(
        codec.encode(payload, dataset="dataset", partition="partition", kid="key"),
        dataset="dataset",
        partition="partition",
        now=NOW,
    )
    assert decoded == CursorDecoded(payload, "key", None)


def test_dependency_graph_and_generation_edges() -> None:
    for limits in (
        lambda: DependencyGraphLimits(max_datasets=0),
        lambda: DependencyGraphLimits(max_partitions=True),
    ):
        with pytest.raises(ValueError):
            limits()
    for invalid in ("", " spaced"):
        with pytest.raises(ValueError):
            DatasetDependencyGraph({invalid: ()})
        with pytest.raises(ValueError):
            DatasetDependencyGraph({"valid": (invalid,)})

    graph = DatasetDependencyGraph(
        {"c": ("b",), "b": ("a",), "a": ()},
        limits=DependencyGraphLimits(max_datasets=2, max_partitions=1),
    )
    assert isinstance(graph.validate(), DependencyLimitExceeded)
    assert isinstance(graph.topological_order(("a", "b", "c")), DependencyLimitExceeded)
    with pytest.raises(ValueError):
        graph.topological_order(("unknown",))
    assert graph.topological_order(("a",)) == DependencyOrder(("a",))
    with pytest.raises(ValueError):
        graph.closure(("unknown",))
    assert isinstance(graph.closure(("a",), partitions=("1", "2")), DependencyLimitExceeded)
    assert isinstance(graph.closure(("c",)), DependencyLimitExceeded)
    cycle = DatasetDependencyGraph({"a": ("b",), "b": ("a",)})
    assert not isinstance(cycle.closure(("a",)), DependencyOrder)

    for capacity in (True, 0, "1"):
        with pytest.raises(ValueError):
            InMemoryGenerationStore(capacity=capacity)  # type: ignore[arg-type]
    generations = InMemoryGenerationStore(capacity=1)
    for dataset, partition in (("", "p"), ("d", "")):
        with pytest.raises(ValueError):
            generations.load_for_update(dataset, partition)
    for generation in (True, -1, "1"):
        with pytest.raises(ValueError):
            generations.compare_and_set("d", "p", expected=None, generation=generation)  # type: ignore[arg-type]
    for expected in (True, -1, "1"):
        with pytest.raises(ValueError):
            generations.compare_and_set("d", "p", expected=expected, generation=1)  # type: ignore[arg-type]
    assert generations.compare_and_set("d", "p", expected=None, generation=1)
    assert not generations.compare_and_set("d", "p", expected=None, generation=2)
    with pytest.raises(OverflowError):
        generations.compare_and_set("other", "p", expected=None, generation=1)

    class Alias:
        using = "one"

        def atomic(self):
            return atomic()

    class OtherStore:
        using = "other"

    with pytest.raises(ValueError):
        GenerationGuard(OtherStore(), Alias())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        GenerationGuard(generations, Transaction()).commit(
            dataset="", partition="p", expected=0, mutation=lambda: None
        )


@given(st.integers(min_value=1, max_value=20))
def test_dependency_dag_hypothesis_has_stable_order(size: int) -> None:
    dependencies = {
        f"d{index}": (() if index == 0 else (f"d{index - 1}",)) for index in range(size)
    }
    order = DatasetDependencyGraph(dependencies).validate()
    assert order == DependencyOrder(tuple(f"d{index}" for index in range(size)))


def test_sync_contract_constructor_and_decoder_edges() -> None:
    with pytest.raises(ValueError):
        SyncSession("session", NOW, NOW)
    with pytest.raises(ValueError):
        DatasetDescriptor("dataset", 1, (cast(Any, "snapshot"),))
    session = SyncSession("session", NOW, NOW + timedelta(1))
    descriptor = DatasetDescriptor("dataset", 1, (SyncMode.SNAPSHOT,))
    for args in (
        (cast(Any, "session"), (descriptor,), SyncLimits()),
        (session, (), SyncLimits()),
        (session, (cast(Any, "dataset"),), SyncLimits()),
        (session, (descriptor, descriptor), SyncLimits()),
    ):
        with pytest.raises(ValueError):
            BootstrapResponse(*args)
    for args in (("md5", DIGEST, 0), ("sha-256", "A" * 64, 0)):
        with pytest.raises(ValueError):
            Integrity(*args)
    integrity = Integrity("sha-256", DIGEST, 0)
    with pytest.raises(ValueError):
        SnapshotPage("dataset", 1, (cast(Any, "bad"),), None, Integrity("sha-256", DIGEST, 1))
    with pytest.raises(ValueError):
        SnapshotPage("dataset", 1, (), None, cast(Any, "bad"))
    with pytest.raises(ValueError):
        DeltaPage("dataset", 1, (cast(Any, "bad"),), (), None, Integrity("sha-256", DIGEST, 1))
    with pytest.raises(ValueError):
        DeltaPage("dataset", 1, (), (), None, cast(Any, "bad"))
    with pytest.raises(ValueError):
        DeltaPage("dataset", 1, (), (), None, Integrity("sha-256", DIGEST, 1))
    with pytest.raises(ValueError):
        Readiness(cast(Any, 1), NOW)
    with pytest.raises(ValueError):
        Readiness(False, NOW)
    with pytest.raises(ValueError):
        MutationOutcome("mutation", cast(Any, "applied"), 1)
    with pytest.raises(ValueError):
        MutationOutcomes(
            "dataset",
            1,
            (cast(Any, "bad"),),
        )
    outcome = MutationOutcome("mutation", MutationOutcomeState.APPLIED, 1)
    with pytest.raises(ValueError):
        MutationOutcomes("dataset", 1, (outcome, outcome))
    with pytest.raises(ValueError):
        BootstrapRequest("client", ("dataset",), ("same", "same"))
    page_item = Upsert("item", 1, {})
    with pytest.raises(ValueError, match="max_items"):
        SnapshotPage(
            "dataset",
            1,
            (page_item,) * 1_001,
            None,
            Integrity("sha-256", DIGEST, 1_001),
        )
    for invalid in ("", " spaced", "control\n"):
        with pytest.raises(ValueError):
            ResetRequired(invalid, 1, "reason")
    with pytest.raises(ValueError):
        ResetRequired("dataset", 1, " bad")
    with pytest.raises(ValueError):
        Upsert("item", -1, {})
    with pytest.raises(ValueError):
        Tombstone("item", 1, NOW.replace(microsecond=1))

    with pytest.raises(TypeError):
        encode_sync_document(cast(Any, object()))
    with pytest.raises(ValueError, match="max_document_bytes"):
        encode_sync_document(
            BootstrapRequest("client", tuple(f"dataset-{index}" for index in range(8))),
            limits=SyncLimits(max_document_bytes=100),
        )
    with pytest.raises(ValueError, match="unsupported"):
        decode_sync_document({"protocol": "titect-sync/1", "kind": "unknown", "payload": {}})
    with pytest.raises(ValueError, match="JSON object"):
        decode_sync_document(cast(Any, []))

    bootstrap = cast(dict[str, Any], encode_sync_document(BootstrapRequest("client", ("dataset",))))
    bootstrap["payload"]["dataset_ids"] = ["dataset", "dataset"]
    with pytest.raises(ValueError, match="unique"):
        decode_sync_document(bootstrap)

    wrong_array = cast(
        dict[str, Any], encode_sync_document(BootstrapRequest("client", ("dataset",)))
    )
    wrong_array["payload"]["dataset_ids"] = "dataset"
    with pytest.raises(ValueError, match="array"):
        decode_sync_document(wrong_array)

    wrong_protocol_type = cast(
        dict[str, Any], encode_sync_document(BootstrapRequest("client", ("dataset",)))
    )
    wrong_protocol_type["protocol"] = 1
    with pytest.raises(ValueError, match="string"):
        decode_sync_document(wrong_protocol_type)

    too_many_capabilities = cast(
        dict[str, Any],
        encode_sync_document(BootstrapRequest("client", ("dataset",), ("one", "two"))),
    )
    with pytest.raises(ValueError, match="finite limit"):
        decode_sync_document(too_many_capabilities, limits=SyncLimits(max_capabilities=1))

    response = cast(
        dict[str, Any],
        encode_sync_document(BootstrapResponse(session, (descriptor,), SyncLimits())),
    )
    response["payload"]["datasets"] = []
    with pytest.raises(ValueError, match="datasets"):
        decode_sync_document(response)

    snapshot = cast(
        dict[str, Any], encode_sync_document(SnapshotPage("dataset", 1, (), None, integrity))
    )
    snapshot["payload"]["upserts"] = [{"item_id": "i", "revision": 1, "value": {}}] * 2
    with pytest.raises(ValueError, match="max_items"):
        decode_sync_document(snapshot, limits=SyncLimits(max_items_per_page=1))

    delta = cast(
        dict[str, Any],
        encode_sync_document(DeltaPage("dataset", 1, (), (), None, integrity)),
    )
    delta["payload"]["tombstones"] = [
        {"item_id": "i", "revision": 1, "deleted_at": "2026-01-01T00:00:00.000Z"}
    ] * 2
    with pytest.raises(ValueError, match="max_items"):
        decode_sync_document(delta, limits=SyncLimits(max_items_per_page=1))

    readiness = cast(dict[str, Any], encode_sync_document(Readiness(True, NOW)))
    readiness["payload"]["ready"] = 1
    with pytest.raises(ValueError, match="boolean"):
        decode_sync_document(readiness)

    outcomes = cast(
        dict[str, Any],
        encode_sync_document(MutationOutcomes("dataset", 1, (outcome,))),
    )
    outcomes["payload"]["outcomes"] = []
    with pytest.raises(ValueError, match="outcomes"):
        decode_sync_document(outcomes)


@given(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**53) + 1, max_value=2**53 - 1),
        st.text(max_size=30),
        st.lists(st.integers(min_value=-(2**53) + 1, max_value=2**53 - 1)),
    )
)
def test_ijson_parser_hypothesis_accepts_its_supported_values(value: Any) -> None:
    validate_ijson(value)
    assert parse_ijson(canonical_json(value)) == value


def test_security_parser_rejects_noncanonical_and_malformed_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validate_ijson(1.5)
    with pytest.raises(ValueError, match="keys"):
        validate_ijson(cast(Any, {1: "value"}))
    with pytest.raises(ValueError, match="unsupported"):
        validate_ijson(cast(Any, (1, 2)))
    with pytest.raises(ValueError, match="non-finite"):
        parse_ijson("NaN")

    import rfc8785

    monkeypatch.setattr(rfc8785, "dumps", lambda value: (_ for _ in ()).throw(TypeError("bad")))
    with pytest.raises(ValueError, match="canonicalized"):
        canonical_json({"value": 1})
    monkeypatch.undo()

    for invalid in ("", "%", "A"):
        with pytest.raises(ValueError):
            base64url_decode(invalid)
    with pytest.raises(ValueError, match="canonical"):
        base64url_decode("Zh")
    with pytest.raises(ValueError, match="key type"):
        jwk_thumbprint({})
    with pytest.raises(ValueError, match="unsupported"):
        jwk_thumbprint({"kty": "unsupported"})
    with pytest.raises(ValueError, match="required"):
        jwk_thumbprint({"kty": "EC"})
    with pytest.raises(ValueError, match="empty"):
        access_token_hash("")

    with pytest.raises(ValueError, match="positive"):
        ContentDigestVerifier(0)
    digest = ContentDigestVerifier()
    for header, code in (
        ("not-a-digest", "malformed_digest"),
        ("sha-256=:AA==:,sha-256=:AA==:", "malformed_digest"),
        ("sha-512=:AA==:", "unsupported_digest"),
        ("sha-256=:%%%%:", "malformed_digest"),
        ("sha-256=:AA==:", "malformed_digest"),
    ):
        assert digest.verify(b"body", header).code == code

    for capacity in (True, 0, "1"):
        with pytest.raises(ValueError):
            InMemoryReplayStore(capacity=capacity)  # type: ignore[arg-type]
    replay = InMemoryReplayStore()
    for namespace, value, now, ttl in (
        ("n", "v", NOW.replace(tzinfo=None), timedelta(1)),
        ("", "v", NOW, timedelta(1)),
        ("n", "", NOW, timedelta(1)),
        ("n", "v", NOW, timedelta(0)),
    ):
        with pytest.raises(ValueError):
            replay.reserve(namespace, value, now=now, ttl=ttl)


def test_trace_value_validation_edges() -> None:
    for flags in (True, -1, 4, "1"):
        with pytest.raises(ValueError):
            TraceContext(TRACE_ID, PARENT_ID, cast(Any, flags))
    assert TraceContext(TRACE_ID, PARENT_ID).to_headers() == {
        "traceparent": f"00-{TRACE_ID}-{PARENT_ID}-00"
    }
    invalid_states = (
        cast(Any, []),
        (cast(Any, ("only",)),),
        (("UPPER", "value"),),
        (("same", "one"), ("same", "two")),
        (("a" * 256, "x" * 256), ("b" * 256, "y" * 256)),
        (("vendor", cast(Any, 1)),),
        (("vendor", ""),),
        (("vendor", "value "),),
        (("vendor", "bad\nvalue"),),
        (("vendor", "bad,value"),),
        (("vendor", "bad=value"),),
    )
    for state in invalid_states:
        with pytest.raises(ValueError):
            TraceContext(TRACE_ID, PARENT_ID, tracestate=state)
    with pytest.raises(ValueError, match="printable"):
        parse_trace_context(f"00-{TRACE_ID}-{PARENT_ID}-00\n")
    with pytest.raises(ValueError, match="delimiters"):
        parse_trace_context(f"00_{TRACE_ID}-{PARENT_ID}-00")
    with pytest.raises(ValueError, match="future"):
        parse_trace_context(f"01-{TRACE_ID}-{PARENT_ID}-00x")
    with pytest.raises(ValueError, match="512"):
        parse_tracestate(cast(Any, None))
    with pytest.raises(ValueError, match="equals"):
        parse_tracestate("vendor")


def test_mutation_batch_store_edges_and_unprovable_resume() -> None:
    for capacity in (True, 0, "1"):
        with pytest.raises(ValueError):
            InMemoryMutationBatchStore(capacity=capacity)  # type: ignore[arg-type]
    for limits in (
        lambda: BatchLimits(max_items=0),
        lambda: BatchLimits(max_bytes=True),
    ):
        with pytest.raises(ValueError):
            limits()
    for factory in (
        lambda: BatchItem("", {}),
        lambda: BatchItem(" item", {}),
        lambda: MutationBatchLease("", "b", "t", MutationBatchState.PROCESSING, 0, 0, (), NOW),
        lambda: MutationBatchLease("n", "b", "t", MutationBatchState.COMPLETED, 0, 0, (), NOW),
        lambda: MutationBatchLease("n", "b", "t", MutationBatchState.PROCESSING, 2, 1, (), NOW),
        lambda: MutationBatchLease("n", "b", "t", MutationBatchState.PROCESSING, 1, 1, (), NOW),
    ):
        with pytest.raises(ValueError):
            factory()

    fingerprint = RequestFingerprint.from_json({"items": [1]})
    store = InMemoryMutationBatchStore[dict[str, int]](capacity=1)
    with pytest.raises(ValueError):
        store.begin("", "batch", fingerprint, total_items=1, now=NOW, lease_ttl=timedelta(1))
    with pytest.raises(ValueError):
        store.begin("n", "batch", fingerprint, total_items=True, now=NOW, lease_ttl=timedelta(1))
    lease = store.begin("n", "batch", fingerprint, total_items=1, now=NOW, lease_ttl=timedelta(1))
    assert isinstance(lease, MutationBatchLease)
    assert isinstance(
        store.begin("n", "full", fingerprint, total_items=1, now=NOW, lease_ttl=timedelta(1)),
        BatchUncertain,
    )
    assert isinstance(
        store.advance(
            lease, BatchItemReceipt("i", {}), now=NOW + timedelta(1), lease_ttl=timedelta(1)
        ),
        StaleMutationBatchLease,
    )
    resumed = store.begin(
        "n", "batch", fingerprint, total_items=1, now=NOW + timedelta(1), lease_ttl=timedelta(1)
    )
    assert isinstance(resumed, MutationBatchLease) and resumed.resumed
    assert isinstance(
        store.complete(resumed, now=NOW + timedelta(1), retention_ttl=timedelta(1)),
        StaleMutationBatchLease,
    )
    with pytest.raises(ValueError):
        store.mark_uncertain(resumed, "", now=NOW + timedelta(1), retention_ttl=timedelta(1))
    stale = replace(resumed, expires_at=resumed.expires_at + timedelta(1))
    assert isinstance(
        store.mark_uncertain(stale, "unknown", now=NOW + timedelta(1), retention_ttl=timedelta(1)),
        StaleMutationBatchLease,
    )
    unknown = replace(resumed, token="unknown")
    assert isinstance(
        store.renew(unknown, now=NOW + timedelta(1), lease_ttl=timedelta(1)),
        StaleMutationBatchLease,
    )
    with pytest.raises(ValueError):
        store.renew(resumed, now=NOW.replace(tzinfo=None), lease_ttl=timedelta(1))
    with pytest.raises(ValueError):
        store.renew(resumed, now=NOW + timedelta(1), lease_ttl=timedelta(0))

    terminal = InMemoryMutationBatchStore[int]()
    empty = terminal.begin(
        "n", "empty", fingerprint, total_items=0, now=NOW, lease_ttl=timedelta(1)
    )
    assert isinstance(empty, MutationBatchLease)
    terminal.complete(empty, now=NOW, retention_ttl=timedelta(1))
    dry = terminal.purge(PurgeIdempotencyPlan(NOW + timedelta(1), dry_run=True))
    assert (dry.selected, dry.affected) == (1, 0)

    from conftest import ManualClock

    clock = ManualClock()

    class FinalUnconfirmed(InMemoryMutationBatchStore):
        def complete(self, lease, *, now, retention_ttl):
            del lease, now, retention_ttl
            return StaleMutationBatchLease()

    batches = FinalUnconfirmed()
    items = InMemoryIdempotencyStore()
    coordinator = MutationBatchCoordinator(
        batches, items, Transaction(), using="default", clock=clock
    )
    first = coordinator.execute(
        batch_id="resume",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(first, BatchItemsCommittedEnvelopeUnconfirmed)
    clock.advance(POLICY.result_retention_ttl)
    resumed_result = coordinator.execute(
        batch_id="resume",
        items=(BatchItem("one", {"value": 1}),),
        policy=PER_ITEM,
        mutate=lambda item, using: item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(resumed_result, BatchUncertain)
    assert "no longer provable" in resumed_result.reason


def test_mutation_batch_coordinator_rejects_alias_limits_and_early_decisions() -> None:
    class AliasStore(InMemoryMutationBatchStore):
        using = "other"

    with pytest.raises(ValueError):
        MutationBatchCoordinator(
            AliasStore(), InMemoryIdempotencyStore(), Transaction(), using="default"
        )
    with pytest.raises(ValueError):
        MutationBatchCoordinator(
            InMemoryMutationBatchStore(),
            InMemoryIdempotencyStore(),
            Transaction(),
            using="",
        )

    class AliasedTransaction(Transaction):
        using = "other"

    with pytest.raises(ValueError):
        MutationBatchCoordinator(
            InMemoryMutationBatchStore(),
            InMemoryIdempotencyStore(),
            AliasedTransaction(),
            using="default",
        )

    coordinator = MutationBatchCoordinator(
        InMemoryMutationBatchStore(),
        InMemoryIdempotencyStore(),
        Transaction(),
        using="default",
        limits=BatchLimits(max_items=2, max_bytes=80),
    )
    with pytest.raises(ValueError):
        coordinator.execute(
            batch_id="",
            items=(),
            policy=PER_ITEM,
            mutate=lambda item, using: None,
            idempotency_policy=POLICY,
        )
    with pytest.raises(ValueError, match="max_items"):
        coordinator.execute(
            batch_id="many",
            items=(BatchItem("a", {}), BatchItem("b", {}), BatchItem("c", {})),
            policy=PER_ITEM,
            mutate=lambda item, using: None,
            idempotency_policy=POLICY,
        )
    with pytest.raises(ValueError, match="unique"):
        coordinator.execute(
            batch_id="duplicate",
            items=(BatchItem("a", {}), BatchItem("a", {})),
            policy=PER_ITEM,
            mutate=lambda item, using: None,
            idempotency_policy=POLICY,
        )

    class UnsupportedPolicy:
        value = "unsupported"

    with pytest.raises(ValueError, match="max_bytes"):
        coordinator.execute(
            batch_id="large",
            items=(BatchItem("a", {"value": "x" * 100}),),
            policy=PER_ITEM,
            mutate=lambda item, using: None,
            idempotency_policy=POLICY,
        )
    with pytest.raises(ValueError, match="unsupported"):
        coordinator.execute(
            batch_id="policy",
            items=(),
            policy=cast(Any, UnsupportedPolicy()),
            mutate=lambda item, using: None,
            idempotency_policy=POLICY,
        )

    class EarlyBatch(InMemoryMutationBatchStore):
        decision: object

        def begin(self, *args, **kwargs):
            return self.decision

    early = EarlyBatch()
    early_coordinator = MutationBatchCoordinator(
        early, InMemoryIdempotencyStore(), Transaction(), using="default"
    )
    for decision in (
        BatchConflict(None, "conflict"),
        BatchInProgress(None, NOW),
        BatchUncertain(None, "uncertain"),
    ):
        early.decision = decision
        outcome = early_coordinator.execute(
            batch_id="early",
            items=(),
            policy=ALL_OR_NOTHING,
            mutate=lambda item, using: None,
            idempotency_policy=POLICY,
        )
        assert outcome == decision
        assert (
            early_coordinator.execute(
                batch_id="early",
                items=(),
                policy=PER_ITEM,
                mutate=lambda item, using: None,
                idempotency_policy=POLICY,
            )
            == decision
        )

    executing = MutationBatchLease(
        "n", "b", "t", MutationBatchState.PROCESSING, 0, 0, (), NOW + timedelta(1)
    )
    with pytest.raises(AssertionError):
        _batch_decision(executing)


def test_mutation_batch_coordinator_types_every_item_and_cas_failure() -> None:
    item = BatchItem("item", {"value": 1})

    class FixedItems(InMemoryIdempotencyStore):
        decision: object
        renewal: object = StaleReservation()

        def reserve(self, *args, **kwargs):
            return self.decision

        def renew(self, *args, **kwargs):
            return self.renewal

    for decision, expected in (
        (Conflict(), BatchConflict),
        (InProgress(NOW), BatchInProgress),
        (Uncertain("unknown"), BatchUncertain),
        (Replay(BatchItemReceipt("wrong", {"value": 1})), BatchUncertain),
        (Execute(ReservationToken("item-token")), BatchUncertain),
    ):
        item_store = FixedItems()
        item_store.decision = decision
        result = MutationBatchCoordinator(
            InMemoryMutationBatchStore(),
            item_store,
            Transaction(),
            using="default",
        ).execute(
            batch_id=f"failure-{type(decision).__name__}",
            items=(item,),
            policy=PER_ITEM,
            mutate=lambda item, using: item.payload,
            idempotency_policy=POLICY,
        )
        assert isinstance(result, expected)

    class AdvanceFails(InMemoryMutationBatchStore):
        def advance(self, *args, **kwargs):
            return StaleMutationBatchLease()

    advanced = MutationBatchCoordinator(
        AdvanceFails(),
        InMemoryIdempotencyStore(),
        Transaction(),
        using="default",
    ).execute(
        batch_id="advance-fails",
        items=(item,),
        policy=PER_ITEM,
        mutate=lambda item, using: item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(advanced, BatchUncertain)
    assert "progress CAS" in advanced.reason

    class RenewFails(InMemoryMutationBatchStore):
        def renew(self, *args, **kwargs):
            return StaleMutationBatchLease("renew failed")

    renewed = MutationBatchCoordinator(
        RenewFails(),
        InMemoryIdempotencyStore(),
        Transaction(),
        using="default",
    ).execute(
        batch_id="renew-fails",
        items=(item,),
        policy=PER_ITEM,
        mutate=lambda item, using: item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(renewed, BatchUncertain)
    assert renewed.reason == "renew failed"

    final = MutationBatchCoordinator(
        RenewFails(),
        InMemoryIdempotencyStore(),
        Transaction(),
        using="default",
    ).execute(
        batch_id="final-fails",
        items=(),
        policy=ALL_OR_NOTHING,
        mutate=lambda item, using: item.payload,
        idempotency_policy=POLICY,
    )
    assert isinstance(final, BatchUncertain)
    assert final.reason == "final batch CAS failed"


def test_optional_django_abstract_models_are_importable_only_after_configuration() -> None:
    import django
    from django.conf import settings

    if not settings.configured:
        settings.configure(SECRET_KEY="test-only", USE_TZ=True)
    django.setup()

    from pytitect.django import abstract_models

    classes = (
        abstract_models.AbstractCheckpointModel,
        abstract_models.AbstractGenerationModel,
        abstract_models.AbstractIdempotencyModel,
        abstract_models.AbstractInboxModel,
        abstract_models.AbstractLeaseAuthorityModel,
        abstract_models.AbstractMutationBatchModel,
        abstract_models.AbstractOutboxModel,
        abstract_models.AbstractReceiptModel,
        abstract_models.AbstractReplayModel,
    )
    assert all(model._meta.abstract for model in classes)
