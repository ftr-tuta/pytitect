from __future__ import annotations

from collections.abc import Mapping

import pytest

from pytitect.canaries import Canary, CanaryFailed, CanaryPassed, CanarySuite
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
