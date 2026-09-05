"""Preview titect-message/2 values and explicitly selected exact-token codec."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import cast

from pytitect.core import Limits
from pytitect.messaging.model import (
    CLOUD_EVENTS_SPEC_VERSION,
    JSON_CONTENT_TYPE,
    Message,
    format_message_time,
    parse_message_time,
)
from pytitect.wire import (
    ExactValue,
    WireDocument,
    WireProfileError,
    WireShapeError,
    decode_wire_stream,
)

EXACT_MESSAGE_PROFILE = "titect-message/2"


@dataclass(frozen=True, slots=True)
class ExactMessage:
    """An immutable /2 envelope; data has no implicit ordinary-number view."""

    id: str
    source: str
    type: str
    subject: str
    time: datetime
    dataschema: str
    data: WireDocument
    correlationid: str | None = None
    causationid: str | None = None
    specversion: str = CLOUD_EVENTS_SPEC_VERSION
    datacontenttype: str = JSON_CONTENT_TYPE
    profile: str = EXACT_MESSAGE_PROFILE

    def __post_init__(self) -> None:
        if self.profile != EXACT_MESSAGE_PROFILE:
            raise WireProfileError()
        if not isinstance(self.data, WireDocument):
            raise WireShapeError()
        try:
            Message(
                id=self.id,
                source=self.source,
                type=self.type,
                subject=self.subject,
                time=self.time,
                dataschema=self.dataschema,
                data=None,
                correlationid=self.correlationid,
                causationid=self.causationid,
                specversion=self.specversion,
                datacontenttype=self.datacontenttype,
            )
        except (ValueError, TypeError, AttributeError):
            pass
        else:
            return
        raise WireShapeError()


type MessageValue = Message | ExactMessage


def _envelope(message: MessageValue, data: ExactValue) -> dict[str, ExactValue]:
    document: dict[str, ExactValue] = {
        "id": message.id,
        "source": message.source,
        "specversion": message.specversion,
        "type": message.type,
        "subject": message.subject,
        "time": format_message_time(message.time),
        "dataschema": message.dataschema,
        "datacontenttype": message.datacontenttype,
        "profile": message.profile,
        "data": data,
    }
    if message.correlationid is not None:
        document["correlationid"] = message.correlationid
    if message.causationid is not None:
        document["causationid"] = message.causationid
    return document


def _metadata(document: WireDocument, profile: str) -> dict[str, str]:
    from pytitect.messaging.codecs import MESSAGE_FIELDS, REQUIRED_MESSAGE_FIELDS

    value = document.value
    if not isinstance(value, Mapping):
        raise WireShapeError()
    if not set(value) >= REQUIRED_MESSAGE_FIELDS or not set(value) <= MESSAGE_FIELDS:
        raise WireShapeError()
    if not isinstance(value["profile"], str):
        raise WireShapeError()
    if value["profile"] != profile:
        raise WireProfileError()
    for key, item in value.items():
        if key != "data" and not isinstance(item, str):
            raise WireShapeError()
    return {key: cast(str, item) for key, item in value.items() if key != "data"}


def _message_arguments(metadata: dict[str, str]) -> dict[str, str]:
    return {key: item for key, item in metadata.items() if key != "time"}


class ExactJsonMessageCodec:
    """Explicit /2 codec; never selects another profile or converts numeric tokens."""

    media_type = 'application/json;profile="titect-message/2"'

    def __init__(
        self, *, limits: Limits | None = None, max_envelope_bytes: int = 1_048_576
    ) -> None:
        if (
            isinstance(max_envelope_bytes, bool)
            or not isinstance(max_envelope_bytes, int)
            or max_envelope_bytes <= 0
        ):
            raise ValueError("max_envelope_bytes must be a positive integer")
        selected = limits or Limits()
        self._limits = replace(
            selected, max_body_bytes=min(selected.max_body_bytes, max_envelope_bytes)
        )

    def encode(self, message: MessageValue) -> bytes:
        if not isinstance(message, ExactMessage):
            raise WireProfileError()
        return WireDocument(_envelope(message, message.data.value), limits=self._limits).encode()

    def decode(self, payload: bytes) -> ExactMessage:
        return self.decode_stream((payload,))

    def decode_stream(self, chunks: Iterable[bytes]) -> ExactMessage:
        document = decode_wire_stream(chunks, limits=self._limits)
        metadata = _metadata(document, EXACT_MESSAGE_PROFILE)
        value = cast(Mapping[str, ExactValue], document.value)
        try:
            return ExactMessage(
                **_message_arguments(metadata),
                time=parse_message_time(metadata["time"]),
                data=WireDocument(value["data"], limits=self._limits),
            )
        except (ValueError, TypeError):
            pass
        raise WireShapeError()
