"""Bounded canonical codecs for versioned messages."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Protocol

from pytitect.core import JsonValue, Limits, canonical_json_bytes, validate_json
from pytitect.messaging.exact import MessageValue, _message_arguments, _metadata
from pytitect.messaging.model import (
    MESSAGE_PROFILE,
    Message,
    format_message_time,
    parse_message_time,
)
from pytitect.wire import (
    WireDocument,
    WireError,
    WireProfileError,
    WireShapeError,
    _legacy_value,
    _ordinary,
    decode_wire_stream,
)

MESSAGE_FIELDS = frozenset(
    {
        "id",
        "source",
        "specversion",
        "type",
        "subject",
        "time",
        "dataschema",
        "datacontenttype",
        "profile",
        "data",
        "correlationid",
        "causationid",
    }
)
REQUIRED_MESSAGE_FIELDS = MESSAGE_FIELDS - {"correlationid", "causationid"}


class MessageCodec(Protocol):
    """Explicit codec selected by media type at composition."""

    @property
    def media_type(self) -> str: ...

    def encode(self, message: MessageValue) -> bytes: ...

    def decode(self, payload: bytes) -> MessageValue: ...


class JsonMessageCodec:
    """Dependency-free canonical JSON codec with finite input bounds."""

    media_type = "application/json"

    def __init__(
        self, *, limits: Limits | None = None, max_envelope_bytes: int = 1_048_576
    ) -> None:
        if (
            isinstance(max_envelope_bytes, bool)
            or not isinstance(max_envelope_bytes, int)
            or max_envelope_bytes <= 0
        ):
            raise ValueError("max_envelope_bytes must be a positive integer")
        self._limits = limits or Limits()
        self._max_envelope_bytes = max_envelope_bytes

    def encode(self, message: MessageValue) -> bytes:
        if not isinstance(message, Message):
            raise WireProfileError()
        document: dict[str, JsonValue] = {
            "id": message.id,
            "source": message.source,
            "specversion": message.specversion,
            "type": message.type,
            "subject": message.subject,
            "time": format_message_time(message.time),
            "dataschema": message.dataschema,
            "datacontenttype": message.datacontenttype,
            "profile": message.profile,
            "data": message.data,
        }
        if message.correlationid is not None:
            document["correlationid"] = message.correlationid
        if message.causationid is not None:
            document["causationid"] = message.causationid
        validate_json(document, limits=self._limits)
        try:
            encoded = canonical_json_bytes(document)
        except ValueError:
            # Preserve legacy formatting for arbitrarily long integer tokens.
            encoded = WireDocument(
                _legacy_value(document),
                limits=replace(self._limits, max_body_bytes=self._max_envelope_bytes),
            ).encode()
        if len(encoded) > self._max_envelope_bytes:
            raise ValueError("encoded message exceeds max_envelope_bytes")
        return encoded

    def decode(self, payload: bytes) -> Message:
        """Decode through the bounded parser with legacy ValueError compatibility."""

        try:
            return self.decode_raw(payload)
        except WireError as error:
            message = {
                "limits": "encoded message exceeds max_envelope_bytes or JSON allocation limits",
                "shape": "message fields do not match the closed profile",
                "syntax": "message must be valid UTF-8 JSON",
                "precision": "JSON numbers must be finite",
                "unsupported_profile": "unsupported message profile",
            }[error.code]
        raise ValueError(message)

    def decode_raw(self, payload: bytes) -> Message:
        """Preview typed raw boundary preserving /1's binary64 decimal behavior."""

        return self.decode_stream((payload,))

    def decode_stream(self, chunks: Iterable[bytes]) -> Message:
        selected = replace(
            self._limits, max_body_bytes=min(self._limits.max_body_bytes, self._max_envelope_bytes)
        )
        document = decode_wire_stream(chunks, limits=selected)
        metadata = _metadata(document, MESSAGE_PROFILE)
        ordinary = _ordinary(document.value, checked=False)
        assert isinstance(ordinary, dict)
        try:
            return Message(
                **_message_arguments(metadata),
                time=parse_message_time(metadata["time"]),
                data=ordinary["data"],
            )
        except (ValueError, TypeError):
            pass
        raise WireShapeError()


class CodecRegistry:
    """An immutable media-type registry; no process-global registry exists."""

    def __init__(self, codecs: Iterable[MessageCodec]) -> None:
        registered: dict[str, MessageCodec] = {}
        for codec in codecs:
            if not codec.media_type or codec.media_type in registered:
                raise ValueError("codec media types must be non-empty and unique")
            registered[codec.media_type] = codec
        if not registered:
            raise ValueError("at least one codec is required")
        self._codecs: Mapping[str, MessageCodec] = MappingProxyType(registered)

    @property
    def media_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._codecs))

    def require(self, media_type: str) -> MessageCodec:
        try:
            return self._codecs[media_type]
        except KeyError as exc:
            raise LookupError(f"no codec registered for {media_type!r}") from exc
