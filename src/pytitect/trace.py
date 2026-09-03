"""Validated W3C Trace Context values without a tracing runtime or exporter."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from pytitect.core import RequestContext

_LOWER_HEX = re.compile(r"\A[0-9a-f]+\Z")
_TRACESTATE_KEY = re.compile(r"\A[a-z0-9][a-z0-9_*/@-]{0,255}\Z")
_MAX_TRACEPARENT_LENGTH = 1_024
_MAX_TRACESTATE_LENGTH = 512
_MAX_TRACESTATE_MEMBERS = 32


@dataclass(frozen=True, slots=True)
class TraceContext:
    """A validated version-00 W3C trace position and optional vendor state."""

    trace_id: str
    parent_id: str
    trace_flags: int = 0
    tracestate: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        _hex_identifier(self.trace_id, 32, "trace_id")
        _hex_identifier(self.parent_id, 16, "parent_id")
        if (
            isinstance(self.trace_flags, bool)
            or not isinstance(self.trace_flags, int)
            or not 0 <= self.trace_flags <= 0x03
        ):
            raise ValueError("trace_flags may contain only sampled and random-trace-ID bits")
        _validate_tracestate(self.tracestate)

    @property
    def sampled(self) -> bool:
        return bool(self.trace_flags & 0x01)

    @property
    def random_trace_id(self) -> bool:
        return bool(self.trace_flags & 0x02)

    @classmethod
    def parse(cls, traceparent: str, tracestate: str | None = None) -> TraceContext:
        return parse_trace_context(traceparent, tracestate)

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> TraceContext | None:
        return trace_context_from_headers(headers)

    def render_traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.parent_id}-{self.trace_flags:02x}"

    def render_tracestate(self) -> str | None:
        if not self.tracestate:
            return None
        return ",".join(f"{key}={value}" for key, value in self.tracestate)

    def to_headers(self) -> Mapping[str, str]:
        headers = {"traceparent": self.render_traceparent()}
        tracestate = self.render_tracestate()
        if tracestate is not None:
            headers["tracestate"] = tracestate
        return headers


@dataclass(frozen=True, slots=True)
class TracedRequestContext:
    """Explicit association between an application request and propagated trace context."""

    request: RequestContext
    trace: TraceContext


def bind_trace_context(request: RequestContext, trace: TraceContext) -> TracedRequestContext:
    return TracedRequestContext(request, trace)


def parse_trace_context(traceparent: str, tracestate: str | None = None) -> TraceContext:
    """Parse W3C headers and normalize supported or additive future versions to version 00."""

    if not isinstance(traceparent, str) or not 55 <= len(traceparent) <= _MAX_TRACEPARENT_LENGTH:
        raise ValueError("traceparent has an invalid length")
    if any(ord(character) < 0x20 or ord(character) > 0x7E for character in traceparent):
        raise ValueError("traceparent must contain printable ASCII")
    if traceparent[2] != "-" or traceparent[35] != "-" or traceparent[52] != "-":
        raise ValueError("traceparent has invalid delimiters")
    version_text = traceparent[:2]
    if _LOWER_HEX.fullmatch(version_text) is None or version_text == "ff":
        raise ValueError("traceparent has an invalid version")
    version = int(version_text, 16)
    if version == 0 and len(traceparent) != 55:
        raise ValueError("version 00 traceparent must contain exactly four fields")
    if version > 0 and len(traceparent) > 55 and traceparent[55] != "-":
        raise ValueError("future traceparent fields must follow a delimiter")
    trace_id = traceparent[3:35]
    parent_id = traceparent[36:52]
    flags_text = traceparent[53:55]
    _hex_identifier(trace_id, 32, "trace_id")
    _hex_identifier(parent_id, 16, "parent_id")
    if _LOWER_HEX.fullmatch(flags_text) is None:
        raise ValueError("trace_flags must be lowercase hexadecimal")
    members = parse_tracestate(tracestate) if tracestate is not None else ()
    flags = int(flags_text, 16) & 0x03
    return TraceContext(trace_id, parent_id, flags, members)


def parse_tracestate(value: str) -> tuple[tuple[str, str], ...]:
    """Parse one combined W3C tracestate field with a documented 512-character limit."""

    if not isinstance(value, str) or len(value) > _MAX_TRACESTATE_LENGTH:
        raise ValueError("tracestate exceeds 512 characters")
    raw_members = value.split(",")
    if len(raw_members) > _MAX_TRACESTATE_MEMBERS:
        raise ValueError("tracestate contains more than 32 members")
    members: list[tuple[str, str]] = []
    for raw_member in raw_members:
        member = raw_member.strip(" \t")
        if not member:
            continue
        if "=" not in member:
            raise ValueError("tracestate members must contain equals")
        key, member_value = member.split("=", 1)
        if _TRACESTATE_KEY.fullmatch(key) is None:
            raise ValueError("tracestate contains an invalid key")
        _tracestate_value(member_value)
        members.append((key, member_value))
    if len({key for key, _ in members}) != len(members):
        raise ValueError("tracestate keys must be unique")
    return tuple(members)


def trace_context_from_headers(headers: Mapping[str, str]) -> TraceContext | None:
    """Read trace headers case-insensitively without mutating or trusting the request."""

    selected: dict[str, str] = {}
    for raw_name, raw_value in headers.items():
        name = str(raw_name).casefold()
        if name not in {"traceparent", "tracestate"}:
            continue
        if name in selected:
            raise ValueError(f"duplicate {name} header")
        selected[name] = str(raw_value)
    traceparent = selected.get("traceparent")
    if traceparent is None:
        if "tracestate" in selected:
            raise ValueError("tracestate requires traceparent")
        return None
    return parse_trace_context(traceparent, selected.get("tracestate"))


def _hex_identifier(value: str, length: int, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != length
        or _LOWER_HEX.fullmatch(value) is None
        or set(value) == {"0"}
    ):
        raise ValueError(f"{name} must be non-zero lowercase hexadecimal with length {length}")


def _validate_tracestate(value: tuple[tuple[str, str], ...]) -> None:
    if not isinstance(value, tuple) or len(value) > _MAX_TRACESTATE_MEMBERS:
        raise ValueError("tracestate must be a tuple with at most 32 members")
    keys: list[str] = []
    for member in value:
        if not isinstance(member, tuple) or len(member) != 2:
            raise ValueError("tracestate members must be key/value tuples")
        key, member_value = member
        if not isinstance(key, str) or _TRACESTATE_KEY.fullmatch(key) is None:
            raise ValueError("tracestate contains an invalid key")
        _tracestate_value(member_value)
        keys.append(key)
    if len(set(keys)) != len(keys):
        raise ValueError("tracestate keys must be unique")
    rendered = ",".join(f"{key}={member_value}" for key, member_value in value)
    if len(rendered) > _MAX_TRACESTATE_LENGTH:
        raise ValueError("tracestate exceeds 512 characters")


def _tracestate_value(value: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256 or value.endswith(" "):
        raise ValueError("tracestate contains an invalid value")
    if any(ord(character) < 0x20 or ord(character) > 0x7E for character in value):
        raise ValueError("tracestate values must contain printable ASCII")
    if "," in value or "=" in value:
        raise ValueError("tracestate values cannot contain comma or equals")


__all__ = [
    "TraceContext",
    "TracedRequestContext",
    "bind_trace_context",
    "parse_trace_context",
    "parse_tracestate",
    "trace_context_from_headers",
]
