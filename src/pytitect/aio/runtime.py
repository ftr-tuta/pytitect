"""Bounded command, query, relay, and consumer runtimes."""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from pytitect.aio.observation import RuntimeFact, RuntimeObservation
from pytitect.aio.ports import AsyncDelivery, AsyncOutboxStore, AsyncPublisher
from pytitect.aio.quarantine import (
    QuarantinePolicy,
    RejectedDeliveryStore,
    rejected_delivery,
)
from pytitect.aio.resilience import (
    Deadline,
    RetryBudget,
    RetryComposition,
    RetryDeferred,
    SettlementResult,
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
    DeliveryAck,
    DeliveryDisposition,
    DeliveryRetry,
    DeliveryTerminated,
    JsonMessageCodec,
    MessageCodec,
    MessageValue,
    PublicationConfirmed,
    PublicationRetryable,
    PublicationUncertain,
    RoutingTable,
)
from pytitect.operations import MetricSink, OperationalSink, RuntimeRole
from pytitect.outbox import OutboxClaim, RetryPolicy


@dataclass(frozen=True, slots=True)
class CommandExecuted:
    decision: Decision


@dataclass(frozen=True, slots=True)
class QueryExecuted:
    result: JsonValue


class RuntimeBusyError(RuntimeError):
    """The instance has no admission capacity; delivery remains caller-owned."""


class RetryableProcessingError(Exception):
    """A delivery failure that should remain available for retry."""


class PermanentProcessingError(Exception):
    """A delivery failure eligible for durable quarantine."""


type MessageHandler = Callable[[MessageValue, HandlingContext], Decision | Awaitable[Decision]]


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
    deferred: int = 0
    stale: int = 0
    uncertain: int = 0
    busy: bool = False


class AsyncRelay:
    def __init__(
        self,
        store: AsyncOutboxStore[MessageValue],
        publisher: AsyncPublisher,
        routes: RoutingTable,
        *,
        retry_policy: RetryPolicy | None = None,
        clock: Clock | None = None,
        claim_ttl: timedelta = timedelta(minutes=1),
        publish_timeout: timedelta = timedelta(seconds=30),
        concurrency: int = 8,
        max_admitted: int = 32,
        max_retained_bytes: int = 8 * 1024 * 1024,
        round_timeout: timedelta = timedelta(minutes=1),
        resilience: RetryComposition | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        observer: OperationalSink | None = None,
        metrics: MetricSink | None = None,
    ) -> None:
        for value, name in (
            (concurrency, "concurrency"),
            (max_admitted, "max_admitted"),
            (max_retained_bytes, "max_retained_bytes"),
        ):
            _positive_integer(value, name)
        for duration in (claim_ttl, publish_timeout, round_timeout):
            _positive_timeout(duration)
        self._store = store
        self._publisher = publisher
        self._routes = routes
        self._retry = retry_policy or RetryPolicy()
        if resilience is not None and retry_policy is not None:
            raise ValueError("choose resilience or retry_policy, not both")
        self._resilience = resilience or RetryComposition(self._retry, RetryBudget(10_000))
        self._retry = self._resilience.policy
        self._clock = clock or SystemClock()
        self._claim_ttl = claim_ttl
        self._publish_timeout = publish_timeout
        self._concurrency = concurrency
        self._max_admitted = max_admitted
        self._max_bytes = max_retained_bytes
        self._round_timeout = round_timeout
        self._monotonic = monotonic
        self._running = False
        self._observation = RuntimeObservation(RuntimeRole.RELAY, observer, metrics)

    async def run_once(self, *, limit: int) -> RelaySummary:
        _positive_integer(limit, "limit")
        if self._running:
            self._observation.emit(RuntimeFact.BUSY, self._clock.now())
            return RelaySummary(0, 0, 0, 0, busy=True)
        self._running = True
        try:
            return await self._run(min(limit, self._max_admitted))
        except asyncio.CancelledError:
            self._observation.emit(RuntimeFact.CANCELLED, self._clock.now())
            raise
        finally:
            self._running = False
            self._observation.emit(RuntimeFact.STOPPED, self._clock.now())

    async def _run(self, limit: int) -> RelaySummary:
        deadline = Deadline.after(self._round_timeout, monotonic=self._monotonic)
        authority_deadline = Deadline.after(self._claim_ttl, monotonic=self._monotonic)
        async with asyncio.timeout(deadline.remaining):
            claims = await self._store.claim(
                now=self._clock.now(),
                limit=limit,
                claim_ttl=self._claim_ttl,
                max_bytes=self._max_bytes,
            )
        self._observation.emit(RuntimeFact.ADMITTED, self._clock.now())
        counters = dict.fromkeys(
            ("delivered", "retried", "failed", "deferred", "stale", "uncertain"), 0
        )
        iterator = iter(claims)

        async def publish(claim: OutboxClaim[MessageValue]) -> None:
            envelope = claim.envelope
            now = self._clock.now()
            self._observation.lag(envelope.occurred_at, now)
            if claim.claimed_until <= now or not authority_deadline.remaining:
                counters["stale"] += 1
                self._observation.emit(RuntimeFact.STALE, now)
                return
            if not deadline.remaining:
                settlement = await self._store.defer(claim, at=now, available_at=now)
                counters["deferred" if settlement else "stale"] += 1
                return
            try:
                async with asyncio.timeout(
                    min(
                        deadline.remaining,
                        self._publish_timeout.total_seconds(),
                    )
                ):
                    result = await self._publisher.publish(
                        destination=self._routes.destination_for(envelope.payload.type),
                        message=envelope.payload,
                    )
            except asyncio.CancelledError:
                raise
            except (TimeoutError, ConnectionError, OSError) as exc:
                # A transport failure after sending cannot prove the broker rejected it.
                result = PublicationUncertain(type(exc).__name__)
            now = self._clock.now()
            if not authority_deadline.remaining:
                counters["stale"] += 1
                self._observation.emit(RuntimeFact.STALE, now)
                return
            if isinstance(result, PublicationUncertain):
                settlement = await self._store.uncertain(claim, reason=result.reason, at=now)
                outcome = "uncertain" if settlement else "stale"
                counters[outcome] += 1
                self._observation.emit(RuntimeFact(outcome), now)
                return
            if isinstance(result, PublicationConfirmed):
                settlement = await self._store.delivered(claim, at=now)
                outcome = "delivered"
            elif (
                isinstance(result, PublicationRetryable)
                and envelope.attempt + 1 < self._retry.max_attempts
            ):
                retry = self._resilience.schedule(
                    envelope.attempt + 1,
                    now=now,
                    deadline=deadline,
                    retry_after=result.retry_after,
                )
                if isinstance(retry, RetryDeferred):
                    settlement = await self._store.defer(
                        claim, at=now, available_at=now + retry.delay
                    )
                    outcome = "deferred"
                else:
                    settlement = await self._store.retry(
                        claim, at=now, available_at=now + retry.delay
                    )
                    outcome = "retried"
            else:
                settlement = await self._store.failed(claim, reason=result.reason, at=now)
                outcome = "failed"
            if settlement is SettlementResult.STALE:
                outcome = "stale"
            counters[outcome] += 1
            self._observation.emit(RuntimeFact(outcome), self._clock.now())

        async def worker() -> None:
            for claim in iterator:
                await publish(claim)

        tasks = []
        async with asyncio.TaskGroup() as group:
            for _ in range(min(self._concurrency, len(claims))):
                tasks.append(group.create_task(worker()))
        if any(task.cancelled() for task in tasks):
            raise asyncio.CancelledError
        return RelaySummary(
            len(claims),
            counters["delivered"],
            counters["retried"],
            counters["failed"],
            counters["deferred"],
            counters["stale"],
            counters["uncertain"],
        )


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
        monotonic: Callable[[], float] = time.monotonic,
        max_message_bytes: int = 1024 * 1024,
        codec: MessageCodec | None = None,
        max_retained_bytes: int = 40 * 1024 * 1024,
        observer: OperationalSink | None = None,
        metrics: MetricSink | None = None,
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
        _positive_integer(max_message_bytes, "max_message_bytes")
        _positive_integer(max_retained_bytes, "max_retained_bytes")
        if max_message_bytes > max_retained_bytes:
            raise ValueError("retained byte budget must fit one maximum message")
        self._codec = codec or JsonMessageCodec()
        self._max_message_bytes = max_message_bytes
        self._admitted = min(concurrency + queue_capacity, max_retained_bytes // max_message_bytes)
        self._observation = RuntimeObservation(RuntimeRole.CONSUMER, observer, metrics)
        self._running = False
        self._processing = 0
        self._monotonic = monotonic

    async def process(self, delivery: AsyncDelivery) -> DeliveryDisposition:
        if self._running or self._processing >= min(self._concurrency, self._admitted):
            raise RuntimeBusyError("consumer has no admission capacity")
        self._processing += 1
        try:
            if len(self._codec.encode(delivery.message)) > self._max_message_bytes:
                raise ValueError("delivery exceeds configured message byte limit")
            return await self._process(delivery)
        finally:
            self._processing -= 1

    async def _process(self, delivery: AsyncDelivery) -> DeliveryDisposition:
        message = delivery.message
        self._observation.emit(RuntimeFact.ADMITTED, self._clock.now())
        self._observation.lag(message.time, self._clock.now())
        from pytitect.inbox import InboxScope

        scope = InboxScope(self._namespace, message.source, self._consumer)
        token = uuid.uuid4().hex
        committing = False
        authority_deadline = Deadline.after(self._reservation_ttl, monotonic=self._monotonic)
        try:
            async with asyncio.timeout(self._handler_timeout.total_seconds()):
                outcome = "retried"
                async with self._unit_of_work() as transaction:
                    reservation = await transaction.reserve_message(
                        scope,
                        OpaqueId(message.id),
                        token=token,
                        now=self._clock.now(),
                        ttl=self._reservation_ttl,
                    )
                    if isinstance(reservation, InboxDuplicate):
                        committing = True
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
                        if (
                            not authority_deadline.remaining
                            or not await transaction.complete_message(
                                scope, OpaqueId(message.id), token=token, now=self._clock.now()
                            )
                        ):
                            raise RetryableProcessingError("inbox execution authority expired")
                        committing = True
                        await transaction.commit()
                        outcome = "acknowledged"
        except asyncio.CancelledError:
            self._observation.emit(RuntimeFact.CANCELLED, self._clock.now())
            raise
        except PermanentProcessingError as exc:
            if committing:
                raise
            return await self._quarantine_delivery(delivery, str(exc), self._clock.now())
        except (TimeoutError, RetryableProcessingError):
            if committing:
                self._observation.emit(RuntimeFact.UNCERTAIN, self._clock.now())
                raise
            self._observation.emit(RuntimeFact.ROLLED_BACK, self._clock.now())
            await delivery.retry()
            return DeliveryRetry()
        except Exception:
            self._observation.emit(
                RuntimeFact.UNCERTAIN if committing else RuntimeFact.FAILED,
                self._clock.now(),
            )
            raise
        if outcome == "acknowledged":
            self._observation.emit(RuntimeFact.COMMITTED, self._clock.now())
            await delivery.ack()
            self._observation.emit(RuntimeFact.ACKNOWLEDGED, self._clock.now())
            return DeliveryAck()
        await delivery.retry()
        self._observation.emit(RuntimeFact.RETRIED, self._clock.now())
        return DeliveryRetry()

    async def run(self, deliveries: AsyncIterator[AsyncDelivery]) -> ConsumerSummary:
        if self._running or self._processing:
            raise RuntimeBusyError("consumer is already running")
        self._running = True
        queue: asyncio.Queue[tuple[AsyncDelivery, float] | None] = asyncio.Queue(
            self._queue_capacity
        )
        admission = asyncio.Semaphore(self._admitted)
        counts = {"acknowledged": 0, "retried": 0, "terminated": 0}

        async def produce() -> None:
            iterator = aiter(deliveries)
            while True:
                await admission.acquire()
                try:
                    delivery = await anext(iterator)
                except StopAsyncIteration:
                    admission.release()
                    break
                if len(self._codec.encode(delivery.message)) > self._max_message_bytes:
                    raise ValueError("delivery exceeds configured message byte limit")
                await queue.put((delivery, asyncio.get_running_loop().time()))
            for _ in range(self._concurrency):
                await queue.put(None)

        async def consume() -> None:
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return
                    delivery, admitted_at = item
                    remaining = self._handler_timeout.total_seconds() - (
                        asyncio.get_running_loop().time() - admitted_at
                    )
                    if remaining <= 0:
                        await delivery.retry()
                        outcome: DeliveryDisposition = DeliveryRetry()
                    else:
                        async with asyncio.timeout(remaining):
                            outcome = await self._process(delivery)
                    key = (
                        "acknowledged"
                        if isinstance(outcome, DeliveryAck)
                        else "terminated"
                        if isinstance(outcome, DeliveryTerminated)
                        else "retried"
                    )
                    counts[key] += 1
                finally:
                    if item is not None:
                        admission.release()
                    queue.task_done()

        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(produce())
                for _ in range(self._concurrency):
                    group.create_task(consume())
        finally:
            self._running = False
            self._observation.emit(RuntimeFact.STOPPED, self._clock.now())
        return ConsumerSummary(counts["acknowledged"], counts["retried"], counts["terminated"])

    async def _quarantine_delivery(
        self, delivery: AsyncDelivery, reason: str, failed_at: datetime
    ) -> DeliveryDisposition:
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
            return DeliveryRetry()
        await delivery.terminate()
        self._observation.emit(RuntimeFact.TERMINATED, self._clock.now())
        return DeliveryTerminated(record.quarantine_id)


def _positive_timeout(value: timedelta) -> None:
    if value <= timedelta(0):
        raise ValueError("timeouts and leases must be positive")


def _positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
