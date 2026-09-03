import asyncio

from pytitect.fastapi import (
    event_platform_lifespan,
    event_platform_openapi_components,
    idempotency_key_from_headers,
    request_context_from_headers,
)


def test_context_and_idempotency_are_explicit_header_adapters() -> None:
    context = request_context_from_headers(
        {
            "X-Request-ID": "request-1",
            "X-Correlation-ID": "correlation-1",
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        }
    )
    assert str(context.request.request_id) == "request-1"
    assert context.trace is not None
    assert idempotency_key_from_headers({"Idempotency-Key": "key-1"}).value == "key-1"


def test_openapi_components_are_route_neutral() -> None:
    components = event_platform_openapi_components()
    assert "TitectMessageV1" in components["schemas"]
    assert "paths" not in components


def test_lifespan_builds_and_closes_only_explicit_resources() -> None:
    events: list[str] = []

    async def build() -> str:
        events.append("build")
        return "resource"

    async def close(resource: object) -> None:
        events.append(f"close:{resource}")

    async def exercise() -> None:
        async with event_platform_lifespan(
            object(), factories={"broker": build}, close=close
        ) as resources:
            assert resources == {"broker": "resource"}
            events.append("use")

    asyncio.run(exercise())
    assert events == ["build", "use", "close:resource"]
