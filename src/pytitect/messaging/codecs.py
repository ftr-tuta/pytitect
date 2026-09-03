"""Bounded canonical codecs for versioned messages."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Protocol, cast

from pytitect.core import JsonValue, Limits, canonical_json_bytes, validate_json
from pytitect.messaging.model import Message, format_message_time, parse_message_time

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

    def encode(self, message: Message) -> bytes: ...

    def decode(self, payload: bytes) -> Message: ...


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

    def encode(self, message: Message) -> bytes:
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
        encoded = canonical_json_bytes(document)
        if len(encoded) > self._max_envelope_bytes:
            raise ValueError("encoded message exceeds max_envelope_bytes")
        return encoded

    def decode(self, payload: bytes) -> Message:
        if len(payload) > self._max_envelope_bytes:
            raise ValueError("encoded message exceeds max_envelope_bytes")
        try:
            document = json.loads(
                payload,
                parse_constant=_reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("message must be valid UTF-8 JSON") from exc
        if not isinstance(document, dict):
            raise ValueError("message must be a JSON object")
        keys = set(document)
        if not keys >= REQUIRED_MESSAGE_FIELDS or not keys <= MESSAGE_FIELDS:
            raise ValueError("message fields do not match the closed profile")
        validate_json(cast(JsonValue, document), limits=self._limits)
        for name in REQUIRED_MESSAGE_FIELDS - {"data"}:
            if not isinstance(document[name], str):
                raise ValueError(f"message {name} must be a string")
        for name in ("correlationid", "causationid"):
            if name in document and not isinstance(document[name], str):
                raise ValueError(f"message {name} must be a string")
        return Message(
            id=document["id"],
            source=document["source"],
            specversion=document["specversion"],
            type=document["type"],
            subject=document["subject"],
            time=parse_message_time(document["time"]),
            dataschema=document["dataschema"],
            datacontenttype=document["datacontenttype"],
            profile=document["profile"],
            data=cast(JsonValue, document["data"]),
            correlationid=document.get("correlationid"),
            causationid=document.get("causationid"),
        )


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


def _reject_constant(value: str) -> JsonValue:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")
