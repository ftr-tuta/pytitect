"""Opt-in DRF exception handler that renders Problem Details."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pytitect.http import Problem, ProblemRenderer


def make_exception_handler(
    renderer: ProblemRenderer,
    *,
    classify: Callable[[Exception, int, Any], Problem] | None = None,
) -> Callable[[Exception, dict[str, Any]], Any]:
    """Create a handler without changing REST_FRAMEWORK settings."""

    from rest_framework.response import Response
    from rest_framework.views import exception_handler

    def handler(exc: Exception, context: dict[str, Any]) -> Any:
        response = exception_handler(exc, context)
        if response is None:
            return None
        if classify is None:
            problem = Problem(
                status=response.status_code,
                code="validation-error" if response.status_code == 400 else "request-failed",
                detail="The request could not be processed.",
            )
        else:
            problem = classify(exc, response.status_code, response.data)
        original_headers = dict(response.items())
        rendered = Response(renderer.as_dict(problem), status=problem.status)
        for name, value in original_headers.items():
            if name.casefold() != "content-type":
                rendered[name] = value
        rendered["Content-Type"] = renderer.content_type
        return rendered

    return handler
