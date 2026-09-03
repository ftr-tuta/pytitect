"""Consumer-invoked lifespan resource composition."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Any

type ResourceFactory = Callable[[], Awaitable[Any]]
type ResourceCloser = Callable[[Any], Awaitable[None]]


@asynccontextmanager
async def event_platform_lifespan(
    app: Any,
    *,
    factories: Mapping[str, ResourceFactory],
    close: ResourceCloser,
) -> AsyncIterator[Mapping[str, Any]]:
    """Build explicitly selected resources and close only resources built here."""

    del app
    resources: dict[str, Any] = {}
    try:
        for name, factory in factories.items():
            if not name or name in resources:
                raise ValueError("lifespan resource names must be non-empty and unique")
            resources[name] = await factory()
        yield resources
    finally:
        for resource in reversed(tuple(resources.values())):
            await close(resource)
