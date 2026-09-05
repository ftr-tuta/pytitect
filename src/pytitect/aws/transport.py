"""Low-level AWS EventBridge publication and SQS delivery adapters."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator, Callable, Mapping
from concurrent.futures import Executor
from datetime import timedelta
from typing import Any

from pytitect.core import JsonValue, canonical_json_bytes
from pytitect.messaging import (
    JsonMessageCodec,
    Message,
    PublicationConfirmed,
    PublicationRejected,
    PublicationResult,
    PublicationRetryable,
    PublicationUncertain,
    TransportCapabilities,
)

AWS_CAPABILITIES = TransportCapabilities(
    ordered_delivery=False,
    broker_deduplication=False,
    topology_management=True,
    max_message_bytes=256 * 1024,
)

_RETRYABLE_CODES = frozenset(
    {
        "InternalFailure",
        "InternalException",
        "ServiceUnavailable",
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
    }
)


class EventBridgePublisher:
    """Publishes integration events to one explicit custom EventBridge bus."""

    capabilities = AWS_CAPABILITIES

    def __init__(
        self,
        client: Any,
        *,
        event_bus_name: str,
        executor: Executor,
        max_concurrency: int = 8,
        codec: JsonMessageCodec | None = None,
    ) -> None:
        if not event_bus_name:
            raise ValueError("a custom EventBridge bus name is required")
        if isinstance(max_concurrency, bool) or max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        self._client = client
        self._event_bus_name = event_bus_name
        self._executor = executor
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._codec = codec or JsonMessageCodec(
            max_envelope_bytes=AWS_CAPABILITIES.max_message_bytes
        )

    async def publish(self, *, destination: str, message: Message) -> PublicationResult:
        if destination != self._event_bus_name:
            return PublicationRejected("destination does not match the configured custom bus")
        detail = self._codec.encode(message).decode("utf-8")
        entry = {
            "Source": message.source,
            "DetailType": message.type,
            "Detail": detail,
            "EventBusName": self._event_bus_name,
            "Time": message.time,
        }
        try:
            response = await self._call(lambda: self._client.put_events(Entries=[entry]))
        except Exception as exc:
            return classify_aws_error(exc)
        entries = response.get("Entries", [])
        if response.get("FailedEntryCount", 0) or len(entries) != 1:
            failed = entries[0] if entries else {}
            code = str(failed.get("ErrorCode", "UnknownFailure"))
            message_text = str(failed.get("ErrorMessage", "EventBridge rejected the entry"))
            if code in _RETRYABLE_CODES:
                return PublicationRetryable(f"{code}: {message_text}"[:512])
            return PublicationRejected(f"{code}: {message_text}"[:512])
        event_id = entries[0].get("EventId")
        if not event_id:
            return PublicationUncertain("EventBridge response has no EventId")
        return PublicationConfirmed(str(event_id))

    async def _call[ResultT](self, operation: Callable[[], ResultT]) -> ResultT:
        await self._semaphore.acquire()
        try:
            future = asyncio.get_running_loop().run_in_executor(self._executor, operation)
        except BaseException:
            self._semaphore.release()
            raise
        future.add_done_callback(lambda completed: self._semaphore.release())
        return await asyncio.shield(future)


class SqsDelivery:
    def __init__(
        self,
        client: Any,
        *,
        queue_url: str,
        raw_message: Mapping[str, Any],
        executor: Executor,
        semaphore: asyncio.Semaphore,
        codec: JsonMessageCodec | None = None,
    ) -> None:
        if not queue_url:
            raise ValueError("queue URL must not be empty")
        self._client = client
        self._queue_url = queue_url
        self._raw = raw_message
        self._executor = executor
        self._semaphore = semaphore
        self._codec = codec or JsonMessageCodec(
            max_envelope_bytes=AWS_CAPABILITIES.max_message_bytes
        )
        self._message = self._decode_body(str(raw_message["Body"]))
        self._receipt = str(raw_message["ReceiptHandle"])
        self._settled = False

    @property
    def message(self) -> Message:
        return self._message

    async def ack(self) -> None:
        await self._delete()

    async def retry(self, *, delay: timedelta | None = None) -> None:
        self._unsettled()
        if delay is None:
            return
        seconds = math.ceil(delay.total_seconds())
        if delay < timedelta(0) or seconds > 43_200:
            raise ValueError("SQS visibility delay must be between 0 and 43200 seconds")
        await self._call(
            lambda: self._client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt,
                VisibilityTimeout=seconds,
            )
        )

    async def terminate(self) -> None:
        """Delete only after the runtime has persisted durable quarantine."""

        await self._delete()

    async def _delete(self) -> None:
        self._unsettled()
        await self._call(
            lambda: self._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt,
            )
        )
        self._settled = True

    async def _call[ResultT](self, operation: Callable[[], ResultT]) -> ResultT:
        await self._semaphore.acquire()
        try:
            future = asyncio.get_running_loop().run_in_executor(self._executor, operation)
        except BaseException:
            self._semaphore.release()
            raise
        future.add_done_callback(lambda completed: self._semaphore.release())
        return await asyncio.shield(future)

    def _decode_body(self, body: str) -> Message:
        try:
            document = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError("SQS body must be JSON") from exc
        detail: JsonValue
        if isinstance(document, dict) and "detail" in document:
            detail = document["detail"]
        else:
            detail = document
        encoded = detail.encode() if isinstance(detail, str) else canonical_json_bytes(detail)
        return self._codec.decode(encoded)

    def _unsettled(self) -> None:
        if self._settled:
            raise RuntimeError("SQS delivery is already settled")


class SqsDeliverySource:
    def __init__(
        self,
        client: Any,
        *,
        queue_url: str,
        executor: Executor,
        max_concurrency: int = 8,
        wait_time: timedelta = timedelta(seconds=20),
        visibility_timeout: timedelta | None = None,
        codec: JsonMessageCodec | None = None,
    ) -> None:
        if not queue_url:
            raise ValueError("queue URL must not be empty")
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if not timedelta(0) <= wait_time <= timedelta(seconds=20):
            raise ValueError("SQS wait_time must be between 0 and 20 seconds")
        if visibility_timeout is not None and not (
            timedelta(0) <= visibility_timeout <= timedelta(hours=12)
        ):
            raise ValueError("SQS visibility timeout must be between 0 and 12 hours")
        self._client = client
        self._queue_url = queue_url
        self._executor = executor
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._wait_time = wait_time
        self._visibility_timeout = visibility_timeout
        self._codec = codec

    async def deliveries(self, *, batch_size: int) -> AsyncIterator[SqsDelivery]:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= 10
        ):
            raise ValueError("SQS batch_size must be between 1 and 10")
        arguments: dict[str, Any] = {
            "QueueUrl": self._queue_url,
            "MaxNumberOfMessages": batch_size,
            "WaitTimeSeconds": int(self._wait_time.total_seconds()),
            "MessageAttributeNames": ["All"],
        }
        if self._visibility_timeout is not None:
            arguments["VisibilityTimeout"] = int(self._visibility_timeout.total_seconds())
        await self._semaphore.acquire()
        try:
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, lambda: self._client.receive_message(**arguments)
            )
        except BaseException:
            self._semaphore.release()
            raise
        future.add_done_callback(lambda completed: self._semaphore.release())
        response = await asyncio.shield(future)
        for raw in response.get("Messages", []):
            yield SqsDelivery(
                self._client,
                queue_url=self._queue_url,
                raw_message=raw,
                executor=self._executor,
                semaphore=self._semaphore,
                codec=self._codec,
            )


def classify_aws_error(error: Exception) -> PublicationResult:
    response = getattr(error, "response", {})
    code = str(response.get("Error", {}).get("Code", type(error).__name__))
    if isinstance(error, (TimeoutError, ConnectionError, OSError)) or any(
        marker in code.lower() for marker in ("timeout", "connection")
    ):
        return PublicationUncertain(code)
    if code in _RETRYABLE_CODES:
        return PublicationRetryable(code)
    return PublicationRejected(code)
