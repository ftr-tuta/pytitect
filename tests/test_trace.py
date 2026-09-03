from __future__ import annotations

import string
from contextlib import suppress

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pytitect import OpaqueId, RequestContext
from pytitect.observability import pseudonymous_attribute
from pytitect.trace import (
    TraceContext,
    bind_trace_context,
    parse_trace_context,
    parse_tracestate,
    trace_context_from_headers,
)

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"
TRACEPARENT = f"00-{TRACE_ID}-{PARENT_ID}-03"


def test_trace_context_parses_renders_and_binds_explicitly() -> None:
    trace = parse_trace_context(TRACEPARENT, "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7")
    assert trace.sampled is True
    assert trace.random_trace_id is True
    assert trace.render_traceparent() == TRACEPARENT
    assert trace.render_tracestate() == "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"
    assert trace.to_headers() == {
        "traceparent": TRACEPARENT,
        "tracestate": "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7",
    }
    request = RequestContext(OpaqueId("request-1"))
    assert bind_trace_context(request, trace).request is request


def test_trace_headers_are_case_insensitive_and_closed() -> None:
    trace = TraceContext.from_headers({"TraceParent": TRACEPARENT, "TraceState": "vendor=value"})
    assert trace == TraceContext(TRACE_ID, PARENT_ID, 3, (("vendor", "value"),))
    assert trace_context_from_headers({"Accept": "application/json"}) is None
    with pytest.raises(ValueError, match="requires"):
        trace_context_from_headers({"tracestate": "vendor=value"})
    with pytest.raises(ValueError, match="duplicate"):
        trace_context_from_headers({"TraceParent": TRACEPARENT, "traceparent": TRACEPARENT})


@pytest.mark.parametrize(
    "value",
    [
        f"00-{'0' * 32}-{PARENT_ID}-00",
        f"00-{TRACE_ID}-{'0' * 16}-00",
        f"00-{TRACE_ID.upper()}-{PARENT_ID}-00",
        f"ff-{TRACE_ID}-{PARENT_ID}-00",
        f"00-{TRACE_ID}-{PARENT_ID}-0g",
        f"00-{TRACE_ID}-{PARENT_ID}-00-extra",
        f"0-{TRACE_ID}-{PARENT_ID}-00",
    ],
)
def test_traceparent_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_trace_context(value)


def test_future_traceparent_is_safely_downgraded() -> None:
    context = parse_trace_context(f"01-{TRACE_ID}-{PARENT_ID}-ff-future")
    assert context.render_traceparent() == f"00-{TRACE_ID}-{PARENT_ID}-03"
    current = parse_trace_context(f"00-{TRACE_ID}-{PARENT_ID}-ff")
    assert current.render_traceparent() == f"00-{TRACE_ID}-{PARENT_ID}-03"


def test_tracestate_limits_grammar_and_empty_members() -> None:
    assert parse_tracestate(" ,vendor= value,,second=ok\t") == (
        ("vendor", " value"),
        ("second", "ok"),
    )
    assert parse_tracestate("") == ()
    for value in (
        "UPPER=value",
        "vendor=",
        "vendor=value, vendor=again",
        "vendor=bad=value",
        "vendor=bad\nvalue",
        ",".join(f"v{i}=x" for i in range(33)),
        f"vendor={'x' * 257}",
        f"vendor={'x' * 505}",
    ):
        with pytest.raises(ValueError):
            parse_tracestate(value)


@given(st.text(alphabet=string.printable, max_size=1_100))
def test_traceparent_parser_never_leaks_unexpected_exceptions(value: str) -> None:
    with suppress(ValueError):
        parse_trace_context(value)


def test_observability_pseudonym_matches_public_fixture() -> None:
    assert (
        pseudonymous_attribute("synthetic-subject", key=b"synthetic-observation-key")
        == "f85114da2b3b89893d92da07ff4993cd"
    )
    with pytest.raises(ValueError):
        pseudonymous_attribute("value", key=b"")
