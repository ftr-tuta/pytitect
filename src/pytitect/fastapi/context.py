"""Explicit request and runtime context helpers for FastAPI applications."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pytitect.aio import AsyncCommandRuntime, AsyncQueryRuntime
from pytitect.core import OpaqueId, RequestContext
from pytitect.trace import TraceContext, trace_context_from_headers


@dataclass(frozen=True, slots=True)
class EventPlatformContext:
    command_runtime: AsyncCommandRuntime
    query_runtime: AsyncQueryRuntime


@dataclass(frozen=True, slots=True)
class FastAPIRequestContext:
    request: RequestContext
    trace: TraceContext | None


def request_context_from_headers(
    headers: Mapping[str, str],
    *,
    request_id_header: str = "x-request-id",
    correlation_id_header: str = "x-correlation-id",
) -> FastAPIRequestContext:
    normalized = {key.lower(): value for key, value in headers.items()}
    request_id = normalized.get(request_id_header.lower())
    if request_id is None or not request_id.strip():
        raise ValueError("request ID header is required")
    correlation_id = normalized.get(correlation_id_header.lower())
    context = RequestContext(
        request_id=OpaqueId(request_id),
        correlation_id=None if correlation_id is None else OpaqueId(correlation_id),
    )
    trace = trace_context_from_headers(normalized)
    return FastAPIRequestContext(context, trace)


def request_context_from_request(request: Any) -> FastAPIRequestContext:
    """Adapt a Starlette-compatible request without registering a dependency."""

    return request_context_from_headers(dict(request.headers))
