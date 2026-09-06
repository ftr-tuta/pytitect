from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pytitect import Deadline, Limits, OpaqueId, PytitectRuntime, RequestContext
from pytitect.core import (
    canonical_json_bytes,
    hmac_sha256_fingerprint,
    sha256_fingerprint,
    validate_json,
)
from pytitect.http import Problem, ProblemRenderer, static_titles
from pytitect.observability import (
    AttributeMode,
    AttributeRule,
    Event,
    FailureIsolatedObserver,
    ObservationPolicy,
    StructuredObserver,
)


def test_deadline_limits_context_and_runtime(  # type: ignore[no-untyped-def]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    deadline = Deadline.after(timedelta(seconds=5), clock=clock)
    assert deadline.remaining(clock=clock) == timedelta(seconds=5)
    clock.advance(timedelta(seconds=5))
    assert deadline.expired(clock=clock)
    with pytest.raises(ValueError):
        Deadline(datetime.now())
    with pytest.raises(ValueError):
        Deadline.after(timedelta(seconds=-1), clock=clock)
    with pytest.raises(ValueError):
        Limits(max_body_bytes=0)

    source = {"safe": "yes"}
    context = RequestContext(OpaqueId("request-1"), attributes=source)
    source["safe"] = "changed"
    assert context.attributes == {"safe": "yes"}
    with pytest.raises(TypeError):
        context.attributes["x"] = 1  # type: ignore[index]
    with pytest.raises(ValueError):
        OpaqueId(" bad ")
    runtime = PytitectRuntime(clock=clock)
    with pytest.raises(FrozenInstanceError):
        runtime.clock = clock  # type: ignore[misc]
    monkeypatch.setattr(clock, "value", datetime(2026, 2, 1, tzinfo=UTC))
    assert runtime.clock.now().month == 2


@given(st.dictionaries(st.text(max_size=8), st.integers(), max_size=8))
def test_canonical_fingerprints_are_order_independent(value: dict[str, int]) -> None:
    reversed_value = dict(reversed(list(value.items())))
    assert canonical_json_bytes(value) == canonical_json_bytes(reversed_value)
    assert sha256_fingerprint(value) == sha256_fingerprint(reversed_value)
    assert len(hmac_sha256_fingerprint(value, key=b"key")) == 64


def test_json_limits_and_invalid_values() -> None:
    validate_json({"ok": [1, True, None]})
    for value in (float("nan"), float("inf"), {1: "bad"}):
        with pytest.raises(ValueError):
            validate_json(value)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        validate_json([[[1]]], limits=Limits(max_json_depth=2))
    with pytest.raises(ValueError):
        hmac_sha256_fingerprint({}, key=b"")


def test_problem_renderer_is_configurable_and_reserved() -> None:
    from tests.conftest import ManualClock

    renderer = ProblemRenderer(
        "https://errors.example/",
        static_titles({"bad-input": "Bad input"}),
        clock=ManualClock(),
        extension_provider=lambda problem: {"trace": problem.code},
    )
    problem = Problem(422, "bad-input", detail="Nope", extensions={"field": "name"})
    document = json.loads(renderer.render(problem))
    assert document == {
        "type": "https://errors.example/bad-input",
        "title": "Bad input",
        "status": 422,
        "timestamp": "2026-01-01T00:00:00Z",
        "detail": "Nope",
        "field": "name",
        "trace": "bad-input",
    }
    assert renderer.content_type == "application/problem+json"
    with pytest.raises(ValueError):
        Problem(399, "bad")
    with pytest.raises(ValueError):
        Problem(400, "bad", extensions={"status": 401})
    with pytest.raises(ValueError):
        ProblemRenderer("https://errors.example", static_titles({}))
    bad_renderer = ProblemRenderer(
        "https://errors.example/",
        static_titles({}),
        extension_provider=lambda problem: {"title": problem.code},
    )
    with pytest.raises(ValueError):
        bad_renderer.render(Problem(400, "bad"))


def test_observability_filters_hashes_and_redacts() -> None:
    events: list[Event] = []
    policy = ObservationPolicy(
        {
            "operation": AttributeRule(),
            "subject": AttributeRule(AttributeMode.HASH),
            "error": AttributeRule(AttributeMode.REDACT),
        },
        hash_key=b"local-observation-key",
    )
    observer = StructuredObserver(events.append, policy=policy)
    observer.observe(
        "operation.finished",
        {"operation": "create", "subject": "123", "error": "detail", "unknown": "drop"},
    )
    assert events[0].attributes["operation"] == "create"
    assert events[0].attributes["subject"] != "123"
    assert events[0].attributes["error"] == "[REDACTED]"
    assert "unknown" not in events[0].attributes
    with pytest.raises(ValueError):
        ObservationPolicy({"authorization_header": AttributeRule()})
    with pytest.raises(ValueError):
        ObservationPolicy({"subject": AttributeRule(AttributeMode.HASH)})


def test_problem_limits_and_failure_isolated_observer() -> None:
    renderer = ProblemRenderer(
        "https://errors.example/",
        static_titles({}),
        limits=Limits(
            max_body_bytes=128,
            max_json_depth=2,
            max_json_items=16,
            max_metadata_items=1,
            max_string_length=32,
        ),
    )
    with pytest.raises(ValueError):
        renderer.render(Problem(400, "bad", extensions={"one": 1, "two": 2}))
    with pytest.raises(ValueError):
        renderer.render(Problem(400, "bad", extensions={"number": float("nan")}))

    failures = []

    class Broken:
        def observe(self, name, attributes):  # type: ignore[no-untyped-def]
            del name, attributes
            raise RuntimeError("  sensitive   detail  ")

    isolated = FailureIsolatedObserver(
        Broken(), failures.append, limits=Limits(max_string_length=8)
    )
    isolated.observe("event", {})
    assert failures[0].exception_type == "RuntimeE"
    assert failures[0].message == "sensitiv"

    fallback_failure = FailureIsolatedObserver(Broken(), lambda failure: 1 / 0)
    fallback_failure.observe("event", {})
