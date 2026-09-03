from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pytitect.messaging import (
    CapabilitiesAccepted,
    CapabilitiesRejected,
    CapabilityRequirements,
    CodecRegistry,
    JsonMessageCodec,
    Message,
    MessageType,
    MessageTypeRegistry,
    Route,
    RoutingTable,
    TransportCapabilities,
    negotiate_capabilities,
    parse_message_time,
)


def message(**changes: object) -> Message:
    values: dict[str, object] = {
        "id": "01JTESTMESSAGE00000000000000",
        "source": "urn:example:component",
        "type": "example.changed.v1",
        "subject": "aggregate/example",
        "time": datetime(2026, 9, 3, 12, 30, 0, 123000, tzinfo=UTC),
        "dataschema": "urn:example:schema:changed:1",
        "data": {"active": True, "count": 2},
        "correlationid": "correlation-1",
        "causationid": "command-1",
    }
    values.update(changes)
    return Message(**values)  # type: ignore[arg-type]


def test_canonical_message_round_trip_matches_normative_fixture() -> None:
    codec = JsonMessageCodec()
    encoded = codec.encode(message())
    assert codec.encode(codec.decode(encoded)) == encoded
    assert b'"profile":"titect-message/1"' in encoded
    assert b'"time":"2026-09-03T12:30:00.123Z"' in encoded


@given(st.dictionaries(st.text(max_size=12), st.integers(), max_size=8))
def test_json_payloads_have_stable_canonical_bytes(data: dict[str, int]) -> None:
    codec = JsonMessageCodec()
    assert codec.encode(message(data=data)) == codec.encode(
        message(data=dict(reversed(data.items())))
    )


@pytest.mark.parametrize(
    "value",
    ["2026-09-03T12:30:00Z", "2026-09-03T12:30:00.123456Z", "2026-09-03 12:30:00.123Z"],
)
def test_timestamp_profile_is_closed(value: str) -> None:
    with pytest.raises(ValueError, match="milliseconds"):
        parse_message_time(value)


def test_codec_rejects_unknown_fields_and_oversize_documents() -> None:
    encoded = JsonMessageCodec().encode(message())
    with pytest.raises(ValueError, match="closed profile"):
        JsonMessageCodec().decode(encoded[:-1] + b',"unknown":true}')
    with pytest.raises(ValueError, match="max_envelope_bytes"):
        JsonMessageCodec(max_envelope_bytes=10).encode(message())


def test_registries_are_explicit_closed_values() -> None:
    declarations = MessageTypeRegistry(
        [MessageType("example.changed.v1", "urn:example:schema:changed:1")]
    )
    assert declarations.validate(message().type, message().dataschema).version == 1
    with pytest.raises(LookupError):
        declarations.resolve("missing")
    with pytest.raises(ValueError, match="duplicate"):
        MessageTypeRegistry(
            [
                MessageType("example.changed.v1", "one"),
                MessageType("example.changed.v1", "two"),
            ]
        )
    codecs = CodecRegistry([JsonMessageCodec()])
    assert codecs.require("application/json").decode(
        codecs.require("application/json").encode(message())
    )


def test_routing_does_not_change_event_identity() -> None:
    routes = RoutingTable([Route("example.changed.v1", "logical-changes")])
    assert routes.destination_for(message().type) == "logical-changes"
    assert message().type == "example.changed.v1"


def test_capability_negotiation_reports_every_missing_requirement() -> None:
    offered = TransportCapabilities(False, False, True, 128)
    rejected = negotiate_capabilities(
        offered,
        CapabilityRequirements(True, True, True, 256, "ordered"),
    )
    assert isinstance(rejected, CapabilitiesRejected)
    assert rejected.missing == (
        "ordered_delivery",
        "broker_deduplication",
        "max_message_bytes",
        "profile:ordered",
    )
    assert isinstance(
        negotiate_capabilities(offered, CapabilityRequirements(max_message_bytes=128)),
        CapabilitiesAccepted,
    )
