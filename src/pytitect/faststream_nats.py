"""Optional FastStream/NATS bridge that delegates to the Pytitect consumer runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from pytitect.aio import AsyncConsumer
from pytitect.messaging import JsonMessageCodec, Message


class FastStreamNatsDelivery:
    def __init__(
        self,
        message: Message,
        *,
        acknowledge: Callable[[], Awaitable[None]],
        retry_delivery: Callable[[timedelta | None], Awaitable[None]],
        terminate_delivery: Callable[[], Awaitable[None]],
    ) -> None:
        self._message = message
        self._acknowledge = acknowledge
        self._retry_delivery = retry_delivery
        self._terminate_delivery = terminate_delivery
        self._settled = False

    @property
    def message(self) -> Message:
        return self._message

    async def ack(self) -> None:
        self._unsettled()
        await self._acknowledge()
        self._settled = True

    async def retry(self, *, delay: timedelta | None = None) -> None:
        self._unsettled()
        await self._retry_delivery(delay)
        self._settled = True

    async def terminate(self) -> None:
        self._unsettled()
        await self._terminate_delivery()
        self._settled = True

    def _unsettled(self) -> None:
        if self._settled:
            raise RuntimeError("FastStream delivery is already settled")


class FastStreamNatsAdapter:
    """Creates an unregistered handler; consumers own decorators and broker lifecycle."""

    def __init__(self, consumer: AsyncConsumer, *, codec: JsonMessageCodec | None = None) -> None:
        self._consumer = consumer
        self._codec = codec or JsonMessageCodec()

    async def handle(self, payload: bytes, raw_message: Any) -> str:
        message = self._codec.decode(payload)

        async def acknowledge() -> None:
            await raw_message.ack()

        async def retry_delivery(delay: timedelta | None) -> None:
            method = getattr(raw_message, "nak", None) or getattr(raw_message, "nack", None)
            if method is None:
                raise TypeError("FastStream NATS message exposes neither nak nor nack")
            if delay is None:
                await method()
            else:
                await method(delay=delay.total_seconds())

        async def terminate_delivery() -> None:
            method = getattr(raw_message, "term", None) or getattr(raw_message, "reject", None)
            if method is None:
                raise TypeError("FastStream NATS message exposes neither term nor reject")
            await method()

        delivery = FastStreamNatsDelivery(
            message,
            acknowledge=acknowledge,
            retry_delivery=retry_delivery,
            terminate_delivery=terminate_delivery,
        )
        return await self._consumer.process(delivery)

    def subscriber_handler(self) -> Callable[[bytes, Any], Awaitable[str]]:
        """Return a callable for consumer-owned FastStream registration."""

        return self.handle


__all__ = ["FastStreamNatsAdapter", "FastStreamNatsDelivery"]
