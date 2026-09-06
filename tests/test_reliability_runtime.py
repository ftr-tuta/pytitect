import asyncio
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from pytitect.aio import (
    AsyncConsumer,
    AsyncRelay,
    Deadline,
    InMemoryAsyncOutboxStore,
    InMemoryAsyncUnitOfWorkFactory,
    InMemoryRejectedDeliveryStore,
    PermanentProcessingError,
    RetryBudget,
    RetryComposition,
    RetryDeferred,
    RetryScheduled,
    SettlementResult,
)
from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.messaging import (
    DeliveryAck,
    DeliveryRetry,
    JsonMessageCodec,
    PublicationConfirmed,
    PublicationRetryable,
    PublicationUncertain,
    Route,
    RoutingTable,
)
from pytitect.operations import BacklogLimits, BacklogSnapshot
from pytitect.outbox import OutboxEnvelope, RetryPolicy
from tests.test_aio_runtime import Delivery, event


@dataclass
class Clock:
    utc: datetime = datetime(2026, 9, 5, tzinfo=UTC)
    elapsed: float = 0.0

    def now(self):
        return self.utc

    def monotonic(self):
        return self.elapsed


def test_retry_saturates_before_overflow_and_rejects_nonfinite_values():
    assert RetryPolicy(max_attempts=100).delay(60) == timedelta(minutes=15)
    assert RetryPolicy().delay(10**1000) == timedelta(minutes=15)
    assert RetryPolicy(multiplier=1).delay(10**1000) == timedelta(seconds=1)
    assert RetryPolicy(initial_delay=timedelta(days=1)).delay(1) == timedelta(minutes=15)
    for value in (float("inf"), float("-inf"), float("nan"), 0.5):
        with pytest.raises(ValueError):
            RetryPolicy(multiplier=value)
    for value in (0, True, 1.5):
        with pytest.raises(ValueError):
            RetryPolicy(max_attempts=value)
        with pytest.raises(ValueError):
            RetryPolicy().delay(value)


def test_retry_jitter_hint_deadline_and_shared_instance_budget():
    clock = Clock()
    deadline = Deadline.after(timedelta(seconds=20), monotonic=clock.monotonic)
    generator = random.Random(41)
    budget = RetryBudget(2)
    composition = RetryComposition(RetryPolicy(), budget, generator.random)
    scheduled = composition.schedule(
        1, now=clock.now(), deadline=deadline, retry_after=clock.now() + timedelta(seconds=3)
    )
    assert scheduled == RetryScheduled(timedelta(seconds=3))
    assert (
        0 <= composition.schedule(2, now=clock.now(), deadline=deadline).delay.total_seconds() <= 2
    )
    assert composition.schedule(1, now=clock.now(), deadline=deadline).reason == "budget"
    assert RetryBudget(1).remaining == 1 and budget.remaining == 0
    clock.utc -= timedelta(days=1)
    clock.elapsed = 19.5
    deferred = RetryComposition(RetryPolicy(), RetryBudget(1)).schedule(
        1, now=clock.now(), deadline=deadline
    )
    assert deferred == RetryDeferred(timedelta(seconds=1), "deadline")
    assert (
        RetryComposition(RetryPolicy(), RetryBudget(1))
        .schedule(10, now=clock.now(), deadline=deadline)
        .reason
        == "attempts"
    )
    for value in (-1, True, 1.1):
        with pytest.raises(ValueError):
            RetryBudget(value)
    for value in (float("nan"), float("inf"), -0.1, 1.1):
        with pytest.raises(ValueError):
            RetryComposition(RetryPolicy(), RetryBudget(1), lambda value=value: value).schedule(
                1, now=clock.now(), deadline=deadline
            )
    with pytest.raises(ValueError):
        Deadline.after(timedelta(0))
    with pytest.raises(ValueError):
        composition.schedule(1, now=datetime(2026, 1, 1), deadline=deadline)
    with pytest.raises(ValueError):
        composition.schedule(
            1, now=clock.now(), deadline=deadline, retry_after=datetime(2026, 1, 1)
        )


def test_failure_time_retry_hint_and_expired_consumer_authority():
    async def run():
        clock = Clock()
        store = InMemoryAsyncOutboxStore()
        msg = event()
        await store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, clock.now(), clock.now()))

        class Publisher:
            async def publish(self, **kwargs):
                clock.utc += timedelta(seconds=5)
                return PublicationRetryable("later", clock.now() + timedelta(seconds=3))

        summary = await AsyncRelay(
            store, Publisher(), RoutingTable([Route(msg.type, "events")]), clock=clock
        ).run_once(limit=1)
        assert summary.retried == 1
        assert not await store.claim(
            now=clock.now() + timedelta(seconds=2), limit=1, claim_ttl=timedelta(seconds=10)
        )
        claim = (
            await store.claim(
                now=clock.now() + timedelta(seconds=3), limit=1, claim_ttl=timedelta(seconds=10)
            )
        )[0]
        assert claim.envelope.available_at - clock.now() == timedelta(seconds=3)
        factory = InMemoryAsyncUnitOfWorkFactory()

        async def handler(msg, context):
            clock.utc += timedelta(seconds=2)
            return Decision(result=1)

        consumer = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=handler,
            unit_of_work=factory,
            quarantine=InMemoryRejectedDeliveryStore(),
            clock=clock,
            reservation_ttl=timedelta(seconds=1),
        )
        log = []
        assert await consumer.process(Delivery(msg, log)) == DeliveryRetry()
        assert not factory.decisions and log == ["retry:None"]

    asyncio.run(run())


def test_fixed_worker_count_byte_admission_busy_and_cancellation_recovery():
    async def run():
        clock = Clock()
        store = InMemoryAsyncOutboxStore()
        for index in range(100):
            msg = event(str(index))
            await store.add(
                OutboxEnvelope(OpaqueId(msg.id), "events", msg, clock.now(), clock.now())
            )
        entered, release = asyncio.Event(), asyncio.Event()
        active = peak = 0

        class Publisher:
            async def publish(self, **kwargs):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                entered.set()
                try:
                    await release.wait()
                    return PublicationConfirmed("ok")
                finally:
                    active -= 1

        size = len(JsonMessageCodec().encode(event("99")))
        relay = AsyncRelay(
            store,
            Publisher(),
            RoutingTable([Route(event().type, "events")]),
            clock=clock,
            concurrency=2,
            max_admitted=8,
            max_retained_bytes=size * 3,
        )
        baseline = len(asyncio.all_tasks())
        task = asyncio.create_task(relay.run_once(limit=1000000))
        await entered.wait()
        assert (await relay.run_once(limit=1)).busy
        assert len(asyncio.all_tasks()) <= baseline + 3
        release.set()
        summary = await task
        assert 1 <= summary.claimed <= 3 and summary.delivered == summary.claimed and peak <= 2
        entered.clear()
        release.clear()
        task = asyncio.create_task(relay.run_once(limit=8))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        clock.utc += timedelta(minutes=2)
        release.set()
        assert not (await relay.run_once(limit=8)).busy

    asyncio.run(run())


def test_queue_expiry_uncertainty_budget_deferral_and_observer_failure():
    async def run():
        clock = Clock()

        class Observer:
            def emit(self, event):
                assert set(event.attributes) == {"role", "outcome"}
                raise OSError("offline")

        class Publisher:
            async def publish(self, **kwargs):
                clock.utc += timedelta(seconds=2)
                return PublicationConfirmed("ok")

        store = InMemoryAsyncOutboxStore()
        for index in range(2):
            msg = event(str(index))
            await store.add(
                OutboxEnvelope(OpaqueId(msg.id), "events", msg, clock.now(), clock.now())
            )
        routes = RoutingTable([Route(event().type, "events")])
        summary = await AsyncRelay(
            store,
            Publisher(),
            routes,
            clock=clock,
            concurrency=1,
            claim_ttl=timedelta(seconds=1),
            observer=Observer(),
        ).run_once(limit=2)
        assert summary.stale == 2 and summary.delivered == 0

        class Unknown:
            async def publish(self, **kwargs):
                return PublicationUncertain("confirmation lost")

        summary = await AsyncRelay(store, Unknown(), routes, clock=clock).run_once(limit=1)
        assert summary.uncertain == 1
        clock.utc += timedelta(days=1)
        claims = await store.claim(now=clock.now(), limit=10, claim_ttl=timedelta(seconds=30))
        assert len(claims) == 1
        # Only explicit reconciliation releases an uncertain publication for another attempt.
        uncertain_id = next(iter(store._uncertain))
        stamp = store._uncertain[uncertain_id]
        assert await store.resolve_uncertain(
            uncertain_id,
            expected_at=stamp,
            delivered=False,
            available_at=clock.now(),
            at=clock.now(),
        )
        assert not await store.resolve_uncertain(
            uncertain_id,
            expected_at=stamp,
            delivered=True,
            available_at=clock.now(),
            at=clock.now(),
        )

        class Retry:
            async def publish(self, **kwargs):
                return PublicationRetryable("later")

        summary = await AsyncRelay(
            store,
            Retry(),
            routes,
            clock=clock,
            resilience=RetryComposition(RetryPolicy(), RetryBudget(0)),
        ).run_once(limit=1)
        assert summary.deferred == 1
        assert (
            await store.defer(claims[0], at=clock.now(), available_at=clock.now())
            is SettlementResult.DEFERRED
        )
        clock.elapsed = 0

        class SlowAdmission(InMemoryAsyncOutboxStore):
            async def claim(self, **kwargs):
                result = await super().claim(**kwargs)
                clock.elapsed = 100
                return result

        slow = SlowAdmission()
        msg = event()
        await slow.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, clock.now(), clock.now()))
        assert (
            await AsyncRelay(
                slow,
                Retry(),
                routes,
                clock=clock,
                monotonic=clock.monotonic,
                claim_ttl=timedelta(minutes=5),
            ).run_once(limit=1)
        ).deferred == 1

    asyncio.run(run())


def test_backlog_readiness_is_consumer_selected():
    limits = BacklogLimits(5, timedelta(seconds=2), 100)
    assert limits.evaluate(BacklogSnapshot(5, timedelta(seconds=2), 100)).ready
    assert not limits.evaluate(BacklogSnapshot(6, timedelta(seconds=1), 1)).ready
    assert not limits.evaluate(BacklogSnapshot(1, timedelta(seconds=3), 1)).ready
    assert not limits.evaluate(BacklogSnapshot(1, timedelta(seconds=1), 101)).ready
    for factory in (BacklogLimits, BacklogSnapshot):
        with pytest.raises(ValueError):
            factory(-1, timedelta(0), 0)


def test_consumer_direct_admission_busy_and_backwards_clock_authority():
    from pytitect.aio import RuntimeBusyError

    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        clock = Clock()
        factory = InMemoryAsyncUnitOfWorkFactory()

        async def handler(msg, context):
            entered.set()
            await release.wait()
            clock.utc -= timedelta(hours=1)
            clock.elapsed += 2
            return Decision()

        consumer = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=handler,
            unit_of_work=factory,
            quarantine=InMemoryRejectedDeliveryStore(),
            concurrency=1,
            clock=clock,
            monotonic=clock.monotonic,
            reservation_ttl=timedelta(seconds=1),
        )
        task = asyncio.create_task(consumer.process(Delivery(event(), [])))
        await entered.wait()
        with pytest.raises(RuntimeBusyError):
            await consumer.process(Delivery(event("two"), []))

        async def empty():
            if False:
                yield

        with pytest.raises(RuntimeBusyError):
            await consumer.run(empty())
        release.set()
        assert await task == DeliveryRetry()
        assert factory.decisions == ()
        tiny = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=lambda m, c: Decision(),
            unit_of_work=factory,
            quarantine=InMemoryRejectedDeliveryStore(),
            max_message_bytes=1,
            max_retained_bytes=1,
        )
        with pytest.raises(ValueError, match="byte limit"):
            await tiny.process(Delivery(event(), []))

        async def oversized():
            yield Delivery(event(), [])

        with pytest.raises(ExceptionGroup):
            await tiny.run(oversized())
        with pytest.raises(ValueError):
            AsyncConsumer(
                consumer="test",
                namespace="test",
                handler=lambda m, c: Decision(),
                unit_of_work=factory,
                quarantine=InMemoryRejectedDeliveryStore(),
                max_message_bytes=2,
                max_retained_bytes=1,
            )

    asyncio.run(run())


@pytest.mark.parametrize(
    "error", [TimeoutError(), OSError(), PermanentProcessingError("commit failure")]
)
def test_commit_failure_and_ack_failure_remain_visible(error):
    async def run():
        factory = InMemoryAsyncUnitOfWorkFactory()

        class Unit:
            def __init__(self):
                self.inner = factory()

            async def __aenter__(self):
                await self.inner.__aenter__()
                return self

            async def __aexit__(self, *args):
                await self.inner.__aexit__(*args)

            def __getattr__(self, name):
                return getattr(self.inner, name)

            async def commit(self):
                await self.inner.commit()
                raise error

        log = []
        consumer = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=lambda m, c: Decision(),
            unit_of_work=Unit,
            quarantine=InMemoryRejectedDeliveryStore(),
        )
        with pytest.raises(type(error)):
            await consumer.process(Delivery(event(), log))
        assert not log and len(factory.decisions) == 1

        class BrokenAck(Delivery):
            async def ack(self):
                raise OSError("ACK unavailable")

        consumer = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=lambda m, c: Decision(),
            unit_of_work=factory,
            quarantine=InMemoryRejectedDeliveryStore(),
        )
        with pytest.raises(OSError):
            await consumer.process(BrokenAck(event(), log))
        assert not log and len(factory.decisions) == 1

    asyncio.run(run())


def test_lag_observation_is_finite_and_sink_failures_preserve_settlement():
    async def run():
        from dataclasses import replace

        from tests.test_aio_runtime import Publisher

        observed = []

        class Metrics:
            def record(self, metric):
                observed.append(metric)
                raise ConnectionError("collector unavailable")

        clock, metrics = Clock(), Metrics()
        msg = event()
        store = InMemoryAsyncOutboxStore()
        await store.add(OutboxEnvelope(OpaqueId(msg.id), "events", msg, msg.time, msg.time))
        summary = await AsyncRelay(
            store,
            Publisher(),
            RoutingTable([Route(msg.type, "events")]),
            clock=clock,
            metrics=metrics,
        ).run_once(limit=1)
        assert summary.delivered == 1
        factory, log = InMemoryAsyncUnitOfWorkFactory(), []
        consumer = AsyncConsumer(
            consumer="test",
            namespace="test",
            handler=lambda message, context: Decision(result=message.id),
            unit_of_work=factory,
            quarantine=InMemoryRejectedDeliveryStore(),
            clock=clock,
            metrics=metrics,
        )
        assert await consumer.process(Delivery(msg, log)) == DeliveryAck()
        future = replace(msg, id="future", time=clock.now() + timedelta(days=1))
        assert await consumer.process(Delivery(future, log)) == DeliveryAck()
        assert log == ["ack", "ack"] and len(factory.decisions) == 2
        assert {item.name for item in observed} == {"runtime.message_age_seconds"}
        assert [dict(item.attributes) for item in observed] == [
            {"role": "relay"},
            {"role": "consumer"},
            {"role": "consumer"},
        ]
        assert observed[0].value == observed[1].value > 0
        assert observed[2].value == 0

    asyncio.run(run())
