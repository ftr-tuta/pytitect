"""Boundary contracts, allocation attacks and exact-token regression evidence."""

import asyncio
import hashlib
import json
import math
import sys
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pytitect.core import Limits
from pytitect.messaging import (
    ExactJsonMessageCodec,
    JsonMessageCodec,
    parse_message_time,
)
from pytitect.sync import (
    EXACT_JSON_INTEGRITY,
    ExactJsonSha256Integrity,
    SyncIntegritySelection,
    SyncLimits,
    decode_sync_raw,
    decode_sync_stream,
    parse_sync_timestamp,
    select_sync_integrity,
)
from pytitect.wire import (
    ExactNumber,
    WireDocument,
    WireIntegrityError,
    WireLimitError,
    WirePrecisionError,
    WireProfileError,
    WireShapeError,
    WireSyntaxError,
    decode_wire,
    decode_wire_stream,
)
from tests.test_messaging import message


def exact_message(token="1.00000000000000001"):
    raw = JsonMessageCodec().encode(message(data=None))
    return ExactJsonMessageCodec().decode(
        raw.replace(b'"data":null', b'"data":' + token.encode()).replace(
            b"titect-message/1", b"titect-message/2"
        )
    )


def page(kind="delta"):
    document = {
        "protocol": "titect-sync/1",
        "kind": kind,
        "payload": {
            "dataset_id": "d",
            "generation": 7,
            "upserts": [{"item_id": "a", "revision": 1, "value": {"n": 1.0}}],
            "next_cursor": "next",
            "integrity": {"algorithm": "sha-256", "digest": "0" * 64, "item_count": 1},
        },
    }
    if kind == "delta":
        document["payload"]["tombstones"] = []
    return decode_wire(json.dumps(document).encode())


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b" ",
        b"01",
        b"-",
        b"+1",
        b"1.",
        b"1e",
        b"1e+",
        b"--1",
        b"NaN",
        b"Infinity",
        b"null false",
        b"[1,]",
        b'{"a":1,}',
        b"{a:1}",
        b'{"a" 1}',
        b"[",
        b'"',
        b'"\x01"',
        b'"\\q"',
        b'"\\u123"',
        b'"\\uxxxx"',
        b'"\\ud800"',
        b'"\\ud800\\u0000"',
        b'"\\udc00"',
        b"\xef\xbb\xbf{}",
        b"\xff",
        b'"\xc0\xaf"',
        b'"\xed\xa0\x80"',
        b'"\xe2',
        b'{"secret":"private",}',
    ],
)
def test_payload_free_syntax_failures(raw):
    with pytest.raises(WireSyntaxError) as caught:
        decode_wire(raw)
    assert caught.value.args == ("syntax",)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_stream_limits_charge_actual_bytes_and_stop_before_next_chunk():
    consumed = []

    def chunks():
        yield b'{"a":1}'
        consumed.append("oversize")
        yield b" " * 20
        consumed.append("unreachable")
        yield b"null"

    with pytest.raises(WireLimitError):
        decode_wire_stream(chunks(), limits=Limits(max_body_bytes=10))
    assert consumed == ["oversize"]
    assert decode_wire(b"{}" + b" " * 8, limits=Limits(max_body_bytes=10)).encode() == b"{}"
    with pytest.raises(WireShapeError):
        decode_wire("{}")
    with pytest.raises(WireLimitError):
        decode_wire(b'"\xc3\xa9"', limits=Limits(max_body_bytes=3))


@pytest.mark.parametrize(
    "raw,limits",
    [
        (b'{"a":1,"a":2,"a":3}', Limits(max_json_items=3)),
        (b'{"a":[0,0],"a":0}', Limits(max_json_items=4)),
        (b"[[[0]]]", Limits(max_json_depth=2)),
        (b'"abc"', Limits(max_string_length=2)),
        (b'{"abc":0}', Limits(max_string_length=2)),
    ],
)
def test_allocation_limits_before_retention(raw, limits):
    with pytest.raises(WireLimitError):
        decode_wire(raw, limits=limits)


def test_duplicates_scalars_escaping_and_ordering():
    raw = b'{"z":1,"z":2,"a":[null,true,false,"\\b\\f\\n\\r\\t\\/\\\\\\"", "\\ud83d\\ude00"]}'
    result = decode_wire_stream(bytes([byte]) for byte in raw)
    assert result.value["z"] == ExactNumber("2")
    assert result.value["a"][-1] == "😀"
    assert result.encode().startswith(b'{"a":')
    assert b'"z":2}' in result.encode()
    value = decode_wire('{"😀":0,"\ue000":1}'.encode())
    assert value.encode() == '{"\ue000":1,"😀":0}'.encode()


@pytest.mark.parametrize(
    "token",
    [
        "1",
        "1.0",
        "1e0",
        "1E+000",
        "-0",
        "-0.0",
        "-0e-1",
        "1e99999999999999999999999999999",
        "1e-9999",
        "9" * 4301,
    ],
)
def test_numeric_tokens_never_expand_or_narrow(token):
    document = decode_wire(token.encode())
    assert document.value == ExactNumber(token)
    assert document.encode() == token.encode()
    codec = ExactJsonMessageCodec()
    value = exact_message(token)
    assert codec.decode(codec.encode(value)).data.encode() == token.encode()
    with pytest.raises(WireProfileError):
        JsonMessageCodec().encode(value)
    with pytest.raises(WireProfileError):
        codec.encode(message())
    with pytest.raises(WireProfileError):
        codec.decode(JsonMessageCodec().encode(message()))
    with pytest.raises(WireProfileError):
        JsonMessageCodec().decode_raw(codec.encode(value))


def test_explicit_conversions_preserve_precision_and_process_settings():
    setting = sys.get_int_max_str_digits()
    token = "9" * 4301
    integer = ExactNumber(token).to_int()
    assert integer == 10**4301 - 1
    assert ExactNumber("-" + token).to_int() == -integer
    assert sys.get_int_max_str_digits() == setting
    for value in ["0.1", "9007199254740993.0", "1e-9999", "1e9999"]:
        with pytest.raises(WirePrecisionError):
            ExactNumber(value).to_float()
    for value in ["1.0", "1e0"]:
        with pytest.raises(WirePrecisionError):
            ExactNumber(value).to_int()
    assert ExactNumber("0.5").to_float() == 0.5
    assert math.copysign(1, ExactNumber("-0.0").to_float()) == -1
    assert ExactNumber("1.00000000000000001").to_decimal() == Decimal("1.00000000000000001")
    with pytest.raises(WirePrecisionError):
        ExactNumber("1e999999999999999999999999999999").to_decimal()
    assert decode_wire(b'{"a":[0,0.5,true,null]}').to_json() == {"a": [0, 0.5, True, None]}
    with pytest.raises(WirePrecisionError):
        decode_wire(b"0.1").to_json()


def test_immutable_wire_documents_and_construction_limits():
    source = {"a": (ExactNumber("1"),)}
    document = WireDocument(source)
    source["a"] = ()
    assert document.encode() == b'{"a":[1]}'
    with pytest.raises(TypeError):
        document.value["a"] = ()
    with pytest.raises(FrozenInstanceError):
        document.value = None
    for value in [1, 0.1, [], {1: None}]:
        with pytest.raises(WireShapeError):
            WireDocument(value)
    with pytest.raises(WireSyntaxError):
        WireDocument("\ud800")
    for value, limits in [
        ("abcd", Limits(max_string_length=2)),
        ((None, None), Limits(max_json_items=2)),
        (((None,),), Limits(max_json_depth=1)),
        (ExactNumber("1234"), Limits(max_body_bytes=3)),
        ("é", Limits(max_body_bytes=3)),
    ]:
        with pytest.raises(WireLimitError):
            WireDocument(value, limits=limits)


@pytest.mark.parametrize(
    "token,expected",
    [
        ("1.00000000000000001", "1.0"),
        ("1e-7", "1e-07"),
        ("1e20", "1e+20"),
        ("-0.0", "-0.0"),
        ("1e-9999", "0.0"),
        ("-0", "0"),
        ("9" * 4301, "9" * 4301),
    ],
)
def test_legacy_binary64_encoding_is_unchanged(token, expected):
    codec = JsonMessageCodec()
    raw = codec.encode(message(data=None)).replace(b'"data":null', b'"data":' + token.encode())
    assert decode_wire(codec.encode(codec.decode_raw(raw))).value["data"].token == expected
    assert codec.decode(raw) == codec.decode_stream(bytes([byte]) for byte in raw)


@pytest.mark.parametrize("parser", [parse_sync_timestamp, parse_message_time])
@pytest.mark.parametrize(
    "value",
    [
        "2026-01-01T24:00:00.000Z",
        "2026-02-30T00:00:00.000Z",
        "0000-01-01T00:00:00.000Z",
        "2026-01-01T00:60:00.000Z",
        "2026-01-01T00:00:60.000Z",
        "2026-01-01T00:00:00.000+00:00",
    ],
)
def test_timestamp_normalization_is_rejected(parser, value):
    with pytest.raises(ValueError):
        parser(value)


def test_stream_cancellation_is_not_translated():
    def chunks():
        yield b"["
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        decode_wire_stream(chunks())


@given(
    st.recursive(
        st.one_of(
            st.none(),
            st.booleans(),
            st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=20),
            st.integers().map(lambda n: ExactNumber(str(n))),
        ),
        lambda children: st.one_of(
            st.lists(children, max_size=4).map(tuple),
            st.dictionaries(
                st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=10),
                children,
                max_size=4,
            ),
        ),
        max_leaves=25,
    )
)
def test_exact_encoding_round_trips(value):
    document = WireDocument(value)
    assert decode_wire(document.encode()).value == document.value


def test_integrity_preimage_and_round_trip():
    policy = ExactJsonSha256Integrity()
    for kind in ("snapshot", "delta"):
        source = page(kind)
        expected = json.loads(source.encode())
        del expected["payload"]["integrity"]
        preimage = (
            b"titect-sync/1\0integrity-sha-256-exact-json-v1\0"
            + json.dumps(
                expected, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode()
        )
        assert policy.digest(source) == hashlib.sha256(preimage).hexdigest()
        sealed = policy.seal(source)
        selection = select_sync_integrity(
            (EXACT_JSON_INTEGRITY,), EXACT_JSON_INTEGRITY, policies=[policy]
        )
        result = decode_sync_raw(
            sealed.encode(), integrity=selection, acknowledgement=EXACT_JSON_INTEGRITY
        )
        assert result.encode() == sealed.encode()
        assert result.to_contract().generation == 7
        assert decode_sync_stream([sealed.encode()]).encode() == sealed.encode()
        assert decode_sync_raw(source.encode()).to_contract().integrity.digest == "0" * 64


@pytest.mark.parametrize(
    "before,after",
    [
        (b'"dataset_id":"d"', b'"dataset_id":"e"'),
        (b'"generation":7', b'"generation":8'),
        (b'"revision":1', b'"revision":2'),
        (b'"n":1.0', b'"n":1e0'),
        (b'"next_cursor":"next"', b'"next_cursor":null'),
        (b'"upserts":[', b'"upserts":[{"item_id":"extra","revision":2,"value":null},'),
        (
            b'"tombstones":[]',
            b'"tombstones":[{"item_id":"gone","revision":2,"deleted_at":"2026-01-01T00:00:00.000Z"}]',
        ),
        (b'"item_count":1', b'"item_count":0'),
        (b'"algorithm":"sha-256"', b'"algorithm":"sha-512"'),
    ],
)
def test_tampering_fails_before_caller_can_change_state_or_checkpoint(before, after):
    policy = ExactJsonSha256Integrity()
    sealed = policy.seal(page()).encode()
    assert before in sealed
    state, checkpoints = [], []
    with pytest.raises(WireIntegrityError):
        result = decode_sync_raw(
            sealed.replace(before, after),
            integrity=SyncIntegritySelection(policy),
            acknowledgement=EXACT_JSON_INTEGRITY,
        )
        state.append(result)
        checkpoints.append("next")
    assert state == checkpoints == []


def test_integrity_negotiation_has_no_downgrade():
    policy = ExactJsonSha256Integrity()
    for acknowledgement in (None, "", "other"):
        with pytest.raises(WireIntegrityError):
            select_sync_integrity((EXACT_JSON_INTEGRITY,), acknowledgement, policies=[policy])
        with pytest.raises(WireIntegrityError):
            decode_sync_raw(
                policy.seal(page()).encode(),
                integrity=SyncIntegritySelection(policy),
                acknowledgement=acknowledgement,
            )
    with pytest.raises(WireIntegrityError):
        select_sync_integrity((), EXACT_JSON_INTEGRITY, policies=[policy])
    for requested, policies in [
        (("integrity-unknown",), [policy]),
        ((EXACT_JSON_INTEGRITY,), []),
        ((EXACT_JSON_INTEGRITY, EXACT_JSON_INTEGRITY), [policy]),
        ((), [policy, policy]),
    ]:
        with pytest.raises(WireProfileError):
            select_sync_integrity(requested, None, policies=policies)
    assert select_sync_integrity(("other-capability",), None, policies=[]).capability is None


def test_sync_raw_large_integer_and_precision_conversion():
    token = "9" * 4301
    raw = (
        '{"protocol":"titect-sync/1","kind":"dataset","payload":{"dataset_id":"d","generation":'
        + token
        + ',"modes":["snapshot"]}}'
    ).encode()
    result = decode_sync_raw(raw)
    assert result.to_contract().generation == 10**4301 - 1
    assert token.encode() in result.encode()
    decimal = page().encode().replace(b"1.0", b"1.00000000000000001")
    assert b"1.00000000000000001" in decode_sync_raw(decimal).encode()
    with pytest.raises(WirePrecisionError):
        decode_sync_raw(decimal).to_contract()
    with pytest.raises(WireLimitError):
        decode_sync_raw(raw, limits=SyncLimits(max_document_bytes=100))
    with pytest.raises(WireLimitError):
        decode_sync_raw(raw, wire_limits=Limits(max_body_bytes=100))
    with pytest.raises(WireProfileError):
        decode_sync_raw(raw.replace(b"titect-sync/1", b"titect-sync/2"))
    with pytest.raises(WireShapeError):
        decode_sync_raw(raw.replace(b'"generation":' + token.encode(), b'"generation":1.0'))


@pytest.mark.parametrize(
    "raw",
    [b"[]", b"{}", b'{"protocol":1}', b'{"protocol":"titect-sync/1","kind":"other","payload":{}}'],
)
def test_raw_sync_shape_failures(raw):
    with pytest.raises(WireShapeError):
        decode_sync_raw(raw)


def test_codec_and_envelope_failure_categories():
    codec = ExactJsonMessageCodec()
    for maximum in (True, 0, -1, 1.0):
        with pytest.raises(ValueError):
            ExactJsonMessageCodec(max_envelope_bytes=maximum)
    raw = codec.encode(exact_message())
    for encoded in [
        b"[]",
        b"{}",
        raw.replace(b'"profile":"titect-message/2"', b'"profile":1'),
        raw.replace(b'"subject":"aggregate/example"', b'"subject":1'),
        raw.replace(b'"type":"example.changed.v1"', b'"type":"?"'),
        raw.replace(b'"time":"2026-09-03T12:30:00.123Z"', b'"time":"2026-09-03T24:00:00.123Z"'),
    ]:
        with pytest.raises(WireShapeError):
            codec.decode(encoded)
    for change in [{"profile": "other"}, {"data": None}, {"type": "?"}]:
        with pytest.raises((WireShapeError, WireProfileError)):
            replace(exact_message(), **change)
    for payload in [b"[]", b"NaN", b"{}", b"x" * 1048577]:
        with pytest.raises(ValueError):
            JsonMessageCodec().decode(payload)
    raw = JsonMessageCodec().encode(message(data=None))
    with pytest.raises(WirePrecisionError):
        JsonMessageCodec().decode_raw(raw.replace(b'"data":null', b'"data":1e9999'))
    with pytest.raises(WireShapeError):
        JsonMessageCodec().decode_raw(raw.replace(b'"type":"example.changed.v1"', b'"type":"?"'))


def test_injected_policy_identity_cannot_change_during_a_session():
    class Policy:
        capability = EXACT_JSON_INTEGRITY

        def verify(self, document):
            pass

    policy = Policy()
    selection = SyncIntegritySelection(policy)
    policy.capability = "integrity-other"
    with pytest.raises(WireIntegrityError):
        selection.acknowledge(policy.capability)
    with pytest.raises(WireIntegrityError):
        selection.acknowledge(EXACT_JSON_INTEGRITY)
    policy.capability = ""
    with pytest.raises(WireProfileError):
        SyncIntegritySelection(policy)
    with pytest.raises(WireProfileError):
        select_sync_integrity((), None, policies=[policy])


def test_integrity_rejects_invalid_page_and_declaration_shapes():
    policy = ExactJsonSha256Integrity()
    for value in [
        None,
        {},
        {"protocol": "titect-sync/1", "kind": "delta", "payload": {}},
        {
            "protocol": "titect-sync/1",
            "kind": "delta",
            "payload": {"upserts": (), "tombstones": True},
        },
    ]:
        with pytest.raises(WireShapeError):
            policy.digest(WireDocument(value))
    raw = page().encode()
    for changed in [
        raw.replace(b'"algorithm":"sha-256"', b'"extra":null,"algorithm":"sha-256"'),
        raw.replace(b'"item_count":1', b'"item_count":1.0'),
        raw.replace(b'"item_count":1', b'"item_count":true'),
    ]:
        with pytest.raises(WireIntegrityError):
            policy.verify(decode_wire(changed))
    with pytest.raises(WireLimitError):
        decode_wire(b"[" * 2000 + b"0" + b"]" * 2000, limits=Limits(max_json_depth=3000))


def test_exact_admission_and_unsupported_json_transforming_transports():
    from concurrent.futures import ThreadPoolExecutor
    from types import SimpleNamespace

    from pytitect.aio import (
        AsyncConsumer,
        InMemoryAsyncUnitOfWorkFactory,
        InMemoryRejectedDeliveryStore,
    )
    from pytitect.application import Decision
    from pytitect.aws import EventBridgePublisher, SqsDelivery
    from pytitect.faststream_nats import FastStreamNatsAdapter
    from pytitect.messaging import DeliveryAck, PublicationRejected
    from tests.test_faststream_nats import RawMessage

    codec = ExactJsonMessageCodec()
    value = exact_message()
    with ThreadPoolExecutor(max_workers=1) as executor:
        publisher = EventBridgePublisher(
            SimpleNamespace(), event_bus_name="test", executor=executor
        )
        assert isinstance(
            asyncio.run(publisher.publish(destination="test", message=value)), PublicationRejected
        )
        with pytest.raises(WireProfileError):
            EventBridgePublisher(
                SimpleNamespace(), event_bus_name="test", executor=executor, codec=codec
            )
        with pytest.raises(WireProfileError):
            SqsDelivery(
                SimpleNamespace(),
                raw_message={},
                queue_url="test",
                executor=executor,
                semaphore=asyncio.Semaphore(1),
                codec=codec,
            )
    observed = []

    def handle(message, context):
        observed.append(message.data.encode())
        return Decision()

    consumer = AsyncConsumer(
        consumer="exact",
        namespace="test",
        handler=handle,
        unit_of_work=InMemoryAsyncUnitOfWorkFactory(),
        quarantine=InMemoryRejectedDeliveryStore(),
        codec=codec,
    )
    assert (
        asyncio.run(
            FastStreamNatsAdapter(consumer, codec=codec).handle(codec.encode(value), RawMessage())
        )
        == DeliveryAck()
    )
    assert observed == [value.data.encode()]


def test_checked_decimal_conversion_is_independent_of_consumer_traps():
    from decimal import InvalidOperation, localcontext

    with localcontext() as context:
        context.traps[InvalidOperation] = False
        with pytest.raises(WirePrecisionError):
            ExactNumber("1e9999999999999999999999999999").to_decimal()
        assert ExactNumber("1.00000000000000001").to_decimal() == Decimal("1.00000000000000001")
