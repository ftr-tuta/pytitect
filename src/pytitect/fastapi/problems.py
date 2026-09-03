"""Opt-in Problem Details response adaptation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pytitect.http import Problem, ProblemRenderer


def problem_response(problem: Problem, *, renderer: ProblemRenderer) -> Any:
    from fastapi.responses import Response

    payload = renderer.render(problem)
    return Response(
        content=payload,
        status_code=problem.status,
        media_type="application/problem+json",
    )


def make_exception_handler(
    classify: Callable[[Exception], Problem | None],
    *,
    renderer: ProblemRenderer,
) -> Callable[[Any, Exception], Any]:
    """Create a handler; the consumer still registers it on an application."""

    async def handler(request: Any, error: Exception) -> Any:
        del request
        problem = classify(error)
        if problem is None:
            raise error
        return problem_response(problem, renderer=renderer)

    return handler
