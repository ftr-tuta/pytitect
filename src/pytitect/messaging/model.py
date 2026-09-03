"""Closed, versioned message envelope values."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pytitect.core import JsonValue, Limits, validate_json

MESSAGE_PROFILE = "titect-message/1"
CLOUD_EVENTS_SPEC_VERSION = "1.0"
JSON_CONTENT_TYPE = "application/json"

_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_EVENT_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,254}$")


def format_message_time(value: datetime) -> str:
    """Render an aware UTC timestamp with exactly millisecond precision."""

    _require_millisecond_utc(value)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_message_time(value: str) -> datetime:
    """Parse the single timestamp representation accepted by the profile."""

    if not _TIMESTAMP.fullmatch(value):
        raise ValueError("message time must be an RFC 3339 UTC timestamp in milliseconds")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    _require_millisecond_utc(parsed)
    return parsed


@dataclass(frozen=True, slots=True)
class Message:
    """A closed CloudEvents 1.0 message in the `titect-message/1` profile."""

    id: str
    source: str
    type: str
    subject: str
    time: datetime
    dataschema: str
    data: JsonValue
    correlationid: str | None = None
    causationid: str | None = None
    specversion: str = CLOUD_EVENTS_SPEC_VERSION
    datacontenttype: str = JSON_CONTENT_TYPE
    profile: str = MESSAGE_PROFILE

    def __post_init__(self) -> None:
        for name in ("id", "source", "subject", "dataschema"):
            value = getattr(self, name)
            if not value or value != value.strip():
                raise ValueError(f"message {name} must be non-empty and trimmed")
        if not _EVENT_TYPE.fullmatch(self.type):
            raise ValueError("message type is invalid")
        for name in ("correlationid", "causationid"):
            value = getattr(self, name)
            if value is not None and (not value or value != value.strip()):
                raise ValueError(f"message {name} must be non-empty and trimmed when present")
        if self.specversion != CLOUD_EVENTS_SPEC_VERSION:
            raise ValueError(f"specversion must be {CLOUD_EVENTS_SPEC_VERSION}")
        if self.datacontenttype != JSON_CONTENT_TYPE:
            raise ValueError(f"datacontenttype must be {JSON_CONTENT_TYPE}")
        if self.profile != MESSAGE_PROFILE:
            raise ValueError(f"profile must be {MESSAGE_PROFILE}")
        _require_millisecond_utc(self.time)
        validate_json(self.data, limits=Limits())


def _require_millisecond_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("message time must be timezone-aware UTC")
    if value.astimezone(UTC).microsecond % 1000:
        raise ValueError("message time must have millisecond precision")
