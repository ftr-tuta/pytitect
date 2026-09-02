"""Small explicit adapters from DRF requests to framework-neutral values."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RequestView:
    method: str
    absolute_uri: str
    headers: Mapping[str, str]
    body: bytes


def adapt_request(request: Any, *, include_headers: frozenset[str] = frozenset()) -> RequestView:
    headers = {
        name.lower(): str(value)
        for name, value in request.headers.items()
        if name.lower() in include_headers
    }
    return RequestView(
        method=str(request.method).upper(),
        absolute_uri=str(request.build_absolute_uri()),
        headers=headers,
        body=bytes(request.body),
    )
