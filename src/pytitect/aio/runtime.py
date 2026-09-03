"""Bounded command, query, relay, and consumer runtimes."""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from pytitect.aio.ports import AsyncDelivery, AsyncOutboxStore, AsyncPublisher
from pytitect.aio.quarantine import (
    QuarantinePolicy,
    RejectedDeliveryStore,
    rejected_delivery,
)
from pytitect.aio.uow import AsyncUnitOfWorkFactory
from pytitect.application import (
    Command,
    CommandRegistry,
    Decision,
    HandlingContext,
    Query,
    QueryRegistry,
)
from pytitect.core import Clock, JsonValue, OpaqueId, SystemClock
from pytitect.inbox import InboxAccepted, InboxDuplicate
from pytitect.messaging import (
    JsonMessageCodec,
    Message,
    PublicationConfirmed,
    PublicationRejected,
    PublicationRetryable,
    RoutingTable,
)
from pytitect.outbox import OutboxClaim, RetryPolicy


@dataclass(frozen=True, slots=True)
class CommandExecuted:
    decision: Decision


@dataclass(frozen=True, slots=True)
class QueryExecuted:
    result: JsonValue


class RetryableProcessingError(Exception):
    """A delivery failure that should remain available for retry."""


class PermanentProcessingError(Exception):
    """A delivery failure eligible for durable quarantine."""


type MessageHandler = Callable[[Message, HandlingContext], Decision | Awaitable[Decision]]


class AsyncCommandRuntime:
    def __init__(
        self,
        registry: CommandRegistry,
        unit_of_work: AsyncUnitOfWorkFactory,
        *,
        timeout: timedelta = timedelta(seconds=30),
    ) -> None:
        _positive_timeout(timeout)
        self._registry = registry
        self._unit_of_work = unit_of_work
        self._timeout = timeout

    async def execute(self, command: Command, context: HandlingContext) -> CommandExecuted:
        async with asyncio.timeout(self._timeout.total_seconds()):
            decision = self._registry.dispatch(command, context)
            async with self._unit_of_work() as transaction:
                await transaction.save_decision(decision)
                await transaction.commit()
            return CommandExecuted(decision)


class AsyncQueryRuntime:
    def __init__(
        self, registry: QueryRegistry, *, timeout: timedelta = timedelta(seconds=30)
    ) -> None:
        _positive_timeout(timeout)
        self._registry = registry
        self._timeout = timeout

    async def execute(self, query: Query, context: HandlingContext) -> QueryExecuted:
        async with asyncio.timeout(self._timeout.total_seconds()):
            return QueryExecuted(self._registry.dispatch(query, context))


@dataclass(frozen=True, slots=True)
class RelaySummary:
    claimed: int
    delivered: int
    retried: int
    failed: int


class AsyncRelay:
    def __init__(
        self,
        store: AsyncOutboxStore[Message],
        publisher: AsyncPublisher,
        routes: RoutingTable,
        *,
        retry_policy: RetryPolicy | None = None,
        clock: Clock | None = None,
        claim_ttl: timedelta = timedelta(minutes=1),
        publish_timeout: timedelta = timedelta(seconds=30),
        concurrency: int = 8,
    ) -> None:
        _positive_integer(concurrency, "concurrency")
        _positive_timeout(claim_ttl)
        _positive_timeout(publish_timeout)
        self._store = store
        self._publisher = publisher
        self._routes = routes
        self._retry = retry_policy or RetryPolicy()
        self._clock = clock or SystemClock()
        self._claim_ttl = claim_ttl
        self._publish_timeout = publish_timeout
        self._concurrency = concurrency

    async def run_once(self, *, limit: int) -> RelaySummary:
        _positive_integer(limit, "limit")
        now = self._clock.now()
        claims = await self._store.claim(now=now, limit=limit, claim_ttl=self._claim_ttl)
        counters = [0, 0, 0]
        semaphore = asyncio.Semaphore(self._concurrency)

        async def publish(claim: OutboxClaim[Message]) -> None:
            async with semaphore:
                envelope = claim.envelope
                try:
                    async with asyncio.timeout(self._publish_timeout.total_seconds()):
                        result = await self._publisher.publish(
                            destination=self._routes.destination_for(envelope.payload.type),
                            message=envelope.payload,
                        )
                except asyncio.CancelledError:
                    raise
                except (TimeoutError, ConnectionError, OSError) as exc:
                    result = PublicationRetryable(type(exc).__name__)
                if isinstance(result, PublicationConfirmed):
                    counters[0] += int(await self._store.delivered(claim, at=now))
                elif (
                    isinstance(result, PublicationRetryable)
                    and envelope.attempt + 1 < self._retry.max_attempts
                ):
                    attempt = envelope.attempt + 1
                    counters[1] += int(
                        await self._store.retry(
                            claim,
                            available_at=now + self._retry.delay(attempt),
                        )
                    )
                else:
                    reason = (
                        result.reason
                        if isinstance(result, (PublicationRetryable, PublicationRejected))
                        else "publication rejected"
                    )
                    counters[2] += int(await self._store.failed(claim, reason=reason, at=now))

        tasks: list[asyncio.Task[None]] = []
        async with asyncio.TaskGroup() as group:
            for claim in claims:
                tasks.append(group.create_task(publish(claim)))
        if any(task.cancelled() for task in tasks):
            raise asyncio.CancelledError
        return RelaySummary(len(claims), *counters)


@dataclass(frozen=True, slots=True)
class ConsumerSummary:
    acknowledged: int
    retried: int
    terminated: int


class AsyncConsumer:
    def __init__(
        self,
        *,
        consumer: str,
        namespace: str,
        handler: MessageHandler,
        unit_of_work: AsyncUnitOfWorkFactory,
        quarantine: RejectedDeliveryStore,
        quarantine_policy: QuarantinePolicy | None = None,
        clock: Clock | None = None,
        reservation_ttl: timedelta = timedelta(minutes=5),
        handler_timeout: timedelta = timedelta(seconds=30),
        concurrency: int = 8,
        queue_capacity: int = 32,
    ) -> None:
        if not consumer or not namespace:
            raise ValueError("consumer and namespace must not be empty")
        _positive_timeout(reservation_ttl)
        _positive_timeout(handler_timeout)
        _positive_integer(concurrency, "concurrency")
        _positive_integer(queue_capacity, "queue_capacity")
        self._consumer = consumer
        self._namespace = namespace
        self._handler = handler
        self._unit_of_work = unit_of_work
        self._quarantine = quarantine
        self._quarantine_policy = quarantine_policy or QuarantinePolicy()
        self._clock = clock or SystemClock()
        self._reservation_ttl = reservation_ttl
        self._handler_timeout = handler_timeout
        self._concurrency = concurrency
        self._queue_capacity = queue_capacity
        self._codec = JsonMessageCodec()

    async def process(self, delivery: AsyncDelivery) -> str:
        message = delivery.message
        now = self._clock.now()
        from pytitect.inbox import InboxScope

        scope = InboxScope(self._namespace, message.source, self._consumer)
        token = uuid.uuid4().hex
        try:
            async with asyncio.timeout(self._handler_timeout.total_seconds()):
                outcome = "retried"
                async with self._unit_of_work() as transaction:
                    reservation = await transaction.reserve_message(
                        scope,
                        OpaqueId(message.id),
                        token=token,
                        now=now,
                        ttl=self._reservation_ttl,
                    )
                    if isinstance(reservation, InboxDuplicate):
                        await transaction.commit()
                        outcome = "acknowledged"
                    elif not isinstance(reservation, InboxAccepted):
                        await transaction.rollback()
                    else:
                        context = HandlingContext(
                            message_id=message.id,
                            correlation_id=message.correlationid,
                            causation_id=message.causationid,
                        )
                        decision_or_awaitable = self._handler(message, context)
                        decision = (
                            await decision_or_awaitable
                            if inspect.isawaitable(decision_or_awaitable)
                            else decision_or_awaitable
                        )
                        await transaction.save_decision(decision)
                        if not await transaction.complete_message(
                            scope, OpaqueId(message.id), token=token, now=now
                        ):
                            raise RuntimeError("inbox completion compare-and-set failed")
                        await transaction.commit()
                        outcome = "acknowledged"
            if outcome == "acknowledged":
                await delivery.ack()
            else:
                await delivery.retry()
            return outcome
        except asyncio.CancelledError:
            raise
        except PermanentProcessingError as exc:
            return await self._quarantine_delivery(delivery, str(exc), now)
        except (TimeoutError, RetryableProcessingError, ConnectionError, OSError):
            await delivery.retry()
            return "retried"
        except Exception:
            await delivery.retry()
            return "retried"

    async def run(self, deliveries: AsyncIterator[AsyncDelivery]) -> ConsumerSummary:
        queue: asyncio.Queue[AsyncDelivery | None] = asyncio.Queue(self._queue_capacity)
        counts = {"acknowledged": 0, "retried": 0, "terminated": 0}

        async def produce() -> None:
            async for delivery in deliveries:
                await queue.put(delivery)
            for _ in range(self._concurrency):
                await queue.put(None)

        async def consume() -> None:
            while True:
                delivery = await queue.get()
                try:
                    if delivery is None:
                        return
                    outcome = await self.process(delivery)
                    counts[outcome] += 1
                finally:
                    queue.task_done()

        async with asyncio.TaskGroup() as group:
            group.create_task(produce())
            for _ in range(self._concurrency):
                group.create_task(consume())
        return ConsumerSummary(counts["acknowledged"], counts["retried"], counts["terminated"])

    async def _quarantine_delivery(
        self, delivery: AsyncDelivery, reason: str, failed_at: datetime
    ) -> str:
        message = delivery.message
        encoded = self._codec.encode(message)
        try:
            record = rejected_delivery(
                quarantine_id=f"{self._consumer}:{message.source}:{message.id}",
                message_id=message.id,
                source=message.source,
                consumer=self._consumer,
                failed_at=failed_at,
                reason=reason,
                encoded_payload=encoded,
                policy=self._quarantine_policy,
                metadata={"event_type": message.type},
            )
            await self._quarantine.add(record)
        except asyncio.CancelledError:
            raise
        except Exception:
            await delivery.retry()
            return "retried"
        await delivery.terminate()
        return "terminated"


def _positive_timeout(value: timedelta) -> None:
    if value <= timedelta(0):
        raise ValueError("timeouts and leases must be positive")


def _positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
