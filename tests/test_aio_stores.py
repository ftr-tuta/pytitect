import asyncio
from datetime import UTC, datetime

import pytest

from pytitect.aio import (
    AsyncCheckpointStoreHarness,
    AsyncInboxStoreHarness,
    AsyncOutboxStoreHarness,
    InMemoryAsyncCheckpointStore,
    InMemoryAsyncInboxStore,
    InMemoryAsyncOutboxStore,
)
from pytitect.core import OpaqueId
from pytitect.outbox import OutboxEnvelope


def test_async_reference_store_harnesses() -> None:
    now = datetime(2026, 9, 3, tzinfo=UTC)

    async def exercise() -> None:
        await AsyncInboxStoreHarness(InMemoryAsyncInboxStore).exercise(now=now)
        await AsyncOutboxStoreHarness[str](InMemoryAsyncOutboxStore).exercise(
            payload="payload", now=now
        )
        await AsyncCheckpointStoreHarness(InMemoryAsyncCheckpointStore).exercise()

    asyncio.run(exercise())


def test_async_store_capacity_is_finite() -> None:
    now = datetime(2026, 9, 3, tzinfo=UTC)

    async def exercise() -> None:
        store = InMemoryAsyncOutboxStore[str](capacity=1)
        await store.add(OutboxEnvelope(OpaqueId("one"), "events", "one", now, now))
        with pytest.raises(OverflowError, match="capacity"):
            await store.add(OutboxEnvelope(OpaqueId("two"), "events", "two", now, now))

    asyncio.run(exercise())
