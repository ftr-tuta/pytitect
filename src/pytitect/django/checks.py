"""Django checks that are registered only through an explicit function call."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any


def register_checks(checks: Iterable[Callable[..., list[Any]]], *, tag: str = "pytitect") -> None:
    """Register consumer-selected checks. This function is deliberately not called on import."""

    from django.core.checks import register

    for check in checks:
        register(tag)(check)
