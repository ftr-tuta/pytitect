"""Low-level NATS JetStream publication and pull-delivery adapters."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import timedelta
from typing import Any, Protocol

from pytitect.messaging import (
    JsonMessageCodec,
    MessageCodec,
    MessageValue,
    PublicationConfirmed,
    PublicationRejected,
    PublicationResult,
    PublicationRetryable,
    PublicationUncertain,
    TransportCapabilities,
)

NATS_CAPABILITIES = TransportCapabilities(
    ordered_delivery=False,
    broker_deduplication=True,
    topology_management=True,
    max_message_bytes=1_048_576,
)


class JetStreamContext(Protocol):
    async def publish(self, subject: str, payload: bytes, *, headers: Mapping[str, str]) -> Any: ...


class PullSubscription(Protocol):
    async def fetch(self, batch: int, *, timeout: float) -> Sequence[Any]: ...


class NatsJetStreamPublisher:
    """Publishes only through an explicitly supplied JetStream context."""

    capabilities = NATS_CAPABILITIES

    def __init__(self, jetstream: JetStreamContext, *, codec: MessageCodec | None = None) -> None:
        self._jetstream = jetstream
        self._codec = codec or JsonMessageCodec(
            max_envelope_bytes=NATS_CAPABILITIES.max_message_bytes
        )

    async def publish(self, *, destination: str, message: MessageValue) -> PublicationResult:
        if not destination:
            return PublicationRejected("destination is empty")
        try:
            acknowledgment = await self._jetstream.publish(
                destination,
                self._codec.encode(message),
                headers={
                    "Content-Type": message.datacontenttype,
                    "Nats-Msg-Id": message.id,
                    "Titect-Profile": message.profile,
                },
            )
        except Exception as exc:
            return classify_nats_publication_error(exc)
        sequence = getattr(acknowledgment, "seq", None)
        if sequence is None:
            return PublicationUncertain("JetStream acknowledgement has no sequence")
        stream = getattr(acknowledgment, "stream", "jetstream")
        return PublicationConfirmed(f"{stream}:{sequence}")


class NatsDelivery:
    def __init__(self, raw_message: Any, *, codec: MessageCodec | None = None) -> None:
        self._raw = raw_message
        self._codec = codec or JsonMessageCodec(
            max_envelope_bytes=NATS_CAPABILITIES.max_message_bytes
        )
        self._message = self._codec.decode(raw_message.data)
        self._settled = False

    @property
    def message(self) -> MessageValue:
        return self._message

    async def ack(self) -> None:
        self._unsettled()
        await self._raw.ack()
        self._settled = True

    async def retry(self, *, delay: timedelta | None = None) -> None:
        self._unsettled()
        if delay is None:
            await self._raw.nak()
        else:
            if delay < timedelta(0):
                raise ValueError("NAK delay must not be negative")
            await self._raw.nak(delay=delay.total_seconds())
        self._settled = True

    async def terminate(self) -> None:
        self._unsettled()
        await self._raw.term()
        self._settled = True

    def _unsettled(self) -> None:
        if self._settled:
            raise RuntimeError("NATS delivery is already settled")


class NatsPullDeliverySource:
    def __init__(
        self,
        subscription: PullSubscription,
        *,
        fetch_timeout: timedelta = timedelta(seconds=5),
        codec: MessageCodec | None = None,
    ) -> None:
        if fetch_timeout <= timedelta(0):
            raise ValueError("fetch timeout must be positive")
        self._subscription = subscription
        self._fetch_timeout = fetch_timeout
        self._codec = codec

    async def deliveries(self, *, batch_size: int) -> AsyncIterator[NatsDelivery]:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        raw_messages = await self._subscription.fetch(
            batch_size, timeout=self._fetch_timeout.total_seconds()
        )
        for raw in raw_messages:
            yield NatsDelivery(raw, codec=self._codec)


def classify_nats_publication_error(error: Exception) -> PublicationResult:
    """Preserve transport uncertainty; only explicit rejections authorize retry."""

    name = type(error).__name__.lower()
    if "noresponders" in name:
        return PublicationRetryable(type(error).__name__)
    if isinstance(error, (TimeoutError, ConnectionError, OSError)) or any(
        marker in name for marker in ("timeout", "connection", "unavailable")
    ):
        return PublicationUncertain(type(error).__name__)
    return PublicationRejected(type(error).__name__)
