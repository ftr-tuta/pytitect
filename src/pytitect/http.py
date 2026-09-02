"""Framework-neutral RFC 9457 Problem Details rendering."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final
from urllib.parse import quote

from pytitect.core import Clock, JsonValue, SystemClock

_RESERVED: Final = frozenset({"type", "title", "status", "detail", "instance", "timestamp"})


@dataclass(frozen=True, slots=True)
class Problem:
    status: int
    code: str
    detail: str | None = None
    instance: str | None = None
    extensions: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 400 <= self.status <= 599:
            raise ValueError("problem status must be between 400 and 599")
        if not self.code or any(char in self.code for char in "/?#"):
            raise ValueError("problem code must be a non-empty URI path segment")
        overlap = _RESERVED.intersection(self.extensions)
        if overlap:
            raise ValueError(f"problem extensions use reserved fields: {sorted(overlap)!r}")


TitleProvider = Callable[[Problem], str]
TimestampFormatter = Callable[[datetime], str]
ExtensionProvider = Callable[[Problem], Mapping[str, JsonValue]]


def _default_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ProblemRenderer:
    type_base_uri: str
    title_provider: TitleProvider
    clock: Clock = field(default_factory=SystemClock)
    timestamp_formatter: TimestampFormatter = _default_timestamp
    extension_provider: ExtensionProvider = lambda problem: {}

    def __post_init__(self) -> None:
        if not self.type_base_uri.endswith("/"):
            raise ValueError("type_base_uri must end with '/'")

    def as_dict(self, problem: Problem) -> dict[str, JsonValue]:
        provided = dict(self.extension_provider(problem))
        overlap = _RESERVED.intersection(provided)
        if overlap:
            raise ValueError(f"extension provider used reserved fields: {sorted(overlap)!r}")
        document: dict[str, JsonValue] = {
            "type": f"{self.type_base_uri}{quote(problem.code, safe='-._~')}",
            "title": self.title_provider(problem),
            "status": problem.status,
            "timestamp": self.timestamp_formatter(self.clock.now()),
        }
        if problem.detail is not None:
            document["detail"] = problem.detail
        if problem.instance is not None:
            document["instance"] = problem.instance
        document.update(problem.extensions)
        document.update(provided)
        return document

    def render(self, problem: Problem) -> bytes:
        return json.dumps(self.as_dict(problem), ensure_ascii=False, separators=(",", ":")).encode()

    @property
    def content_type(self) -> str:
        return "application/problem+json"


def static_titles(titles: Mapping[str, str], *, default: str = "Request failed") -> TitleProvider:
    copied: Mapping[str, str] = dict(titles)
    return lambda problem: copied.get(problem.code, default)
