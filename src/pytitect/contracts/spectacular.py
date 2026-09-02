"""Opt-in drf-spectacular helpers; importing this module registers nothing."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., object])


def canonical_operation(
    *,
    operation_id: str,
    request: object | None = None,
    responses: Mapping[int | str, object] | None = None,
    tags: tuple[str, ...] = (),
) -> Callable[[F], F]:
    """Return an explicit decorator without mutating spectacular settings or registries."""

    try:
        from drf_spectacular.utils import extend_schema
    except ImportError as error:
        raise RuntimeError("install pytitect[contracts] to use schema helpers") from error
    decorator = extend_schema(
        operation_id=operation_id,
        request=request,
        responses=dict(responses or {}),
        tags=list(tags),
    )
    return decorator


def problem_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["type", "title", "status"],
        "properties": {
            "type": {"type": "string", "format": "uri"},
            "title": {"type": "string"},
            "status": {"type": "integer", "minimum": 400, "maximum": 599},
            "detail": {"type": "string"},
            "instance": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"},
        },
        "additionalProperties": True,
    }
