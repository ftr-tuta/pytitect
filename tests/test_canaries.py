from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

import pytest

from pytitect.canaries import (
    Canary,
    CanaryCrashed,
    CanaryFailed,
    CanaryPassed,
    CanarySkipped,
    CanarySuite,
    CanaryTimedOut,
)
from pytitect.core import JsonScalar


class CollectingObserver:
    def __init__(self) -> None:
        self.events: list[tuple[str, Mapping[str, JsonScalar]]] = []

    def observe(self, name: str, attributes: Mapping[str, JsonScalar]) -> None:
        self.events.append((name, attributes))


def test_canary_suite_is_one_explicit_round() -> None:
    observer = CollectingObserver()
    suite = CanarySuite(
        (
            Canary("healthy", lambda: (True, None, {"protocol": "v2"})),
            Canary("unhealthy", lambda: (False, "mismatch", {})),
        ),
        observer,
    )
    outcomes = suite.run_once()
    assert isinstance(outcomes[0], CanaryPassed)
    assert isinstance(outcomes[1], CanaryFailed)
    assert [event[0] for event in observer.events] == ["canary.passed", "canary.failed"]
    with pytest.raises(ValueError):
        Canary("", lambda: (True, None, {}))


def test_canary_suite_classifies_failures_and_continues() -> None:
    observer = CollectingObserver()

    def timeout():  # type: ignore[no-untyped-def]
        raise TimeoutError("I/O deadline")

    def crash():  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    instant = datetime(2026, 1, 1, tzinfo=UTC)
    suite = CanarySuite(
        (
            Canary("timeout", timeout),
            Canary("crash", crash),
            Canary(
                "skip",
                lambda: CanarySkipped("ignored", instant, instant, "not configured"),
            ),
            Canary("later", lambda: (True, None, {})),
        ),
        observer,
    )
    outcomes = suite.run_once()
    assert isinstance(outcomes[0], CanaryTimedOut)
    assert isinstance(outcomes[1], CanaryCrashed)
    assert isinstance(outcomes[2], CanarySkipped)
    assert isinstance(outcomes[3], CanaryPassed)
