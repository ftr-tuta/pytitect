import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pytitect.aio import (
    AsyncIdempotencyCoordinator,
    AsyncIdempotencyStoreHarness,
    AsyncProjectionRuntime,
    AsyncReceiptStoreHarness,
    InMemoryAsyncEventStore,
    InMemoryAsyncIdempotencyStore,
    InMemoryAsyncJobStore,
    InMemoryAsyncProcessManagerStore,
    InMemoryAsyncProjectionStore,
    InMemoryAsyncReceiptStore,
)
from pytitect.application import Decision
from pytitect.event_sourcing import NewEvent, Snapshot, StreamId
from pytitect.idempotency import (
    IdempotencyPolicy,
    IdempotencyScope,
    RequestFingerprint,
    StaleReservation,
)
from pytitect.jobs import Job, JobRetried, JobSucceeded, JobTerminated, StaleJobClaim
from pytitect.processes import (
    ProcessDecision,
    ProcessEffect,
    ProcessEffectKind,
    ProcessKey,
    TimerSchedule,
)
from pytitect.projections import (
    ProjectionDefinition,
    ProjectionKey,
    ProjectionVersionMismatch,
    RebuildRun,
    RebuildStatus,
)
from tests.conftest import ManualClock

NOW = datetime(2026, 9, 5, tzinfo=UTC)
TTL = timedelta(seconds=10)


def test_async_idempotency_and_receipt_conformance_and_coordinator():
    async def run():
        await AsyncIdempotencyStoreHarness(InMemoryAsyncIdempotencyStore).exercise(
            value={"ok": True}, now=NOW
        )
        await AsyncReceiptStoreHarness(InMemoryAsyncReceiptStore).exercise(
            value={"ok": True}, now=NOW
        )
        coordinator = AsyncIdempotencyCoordinator(
            InMemoryAsyncIdempotencyStore(),
            IdempotencyPolicy(TTL, TTL * 10, TTL * 10),
            ManualClock(NOW),
        )
        args = dict(
            scope=IdempotencyScope("test", "subject", "operation"),
            fingerprint=RequestFingerprint.from_json(None),
        )
        first = await coordinator.begin(key="one", **args)
        await coordinator.renew(first.token)
        await coordinator.complete(first.token, 1)
        assert isinstance(await coordinator.abandon(first.token), StaleReservation)
        uncertain = await coordinator.begin(key="uncertain", **args)
        await coordinator.uncertain(uncertain.token, "unknown")
        abandon = await coordinator.begin(key="abandon", **args)
        await coordinator.abandon(abandon.token)

    asyncio.run(run())


def test_async_event_projection_rebuild_reference_conformance():
    async def run():
        events = InMemoryAsyncEventStore(capacity=10)
        stream = StreamId("synthetic", "one")
        await events.append(
            stream,
            expected_version=0,
            events=[NewEvent(str(i), "changed", {}, NOW) for i in range(3)],
        )
        assert await events.watermark() == 3
        assert len((await events.read_stream(stream, after_version=0, limit=2)).events) == 2
        assert await events.load_snapshot(stream) is None
        assert await events.save_snapshot(Snapshot(stream, 3, {}, NOW), expected_version=None)
        assert (await events.load_snapshot(stream)).version == 3
        store = InMemoryAsyncProjectionStore()
        key = ProjectionKey("count", "test")
        definition = ProjectionDefinition(1, 0, lambda state, event: state + 1)
        runtime = AsyncProjectionRuntime(store, events)
        result = await runtime.project_once(key, definition, limit=2)
        assert result.state.checkpoint == 2
        assert isinstance(
            await runtime.project_once(key, ProjectionDefinition(2, 0, lambda s, e: s), limit=2),
            ProjectionVersionMismatch,
        )
        await store.begin_rebuild(RebuildRun("rebuild", key, 2, 3, 1, 0, 0))
        definition = ProjectionDefinition(2, 0, lambda state, event: state + 1)
        for _ in range(3):
            result = await runtime.resume_rebuild("rebuild", definition)
        assert result.status is RebuildStatus.COMPLETED
        assert await runtime.resume_rebuild("rebuild", definition) == result
        with pytest.raises(LookupError):
            await runtime.resume_rebuild("missing", definition)
        with pytest.raises(ValueError):
            await runtime.resume_rebuild("rebuild", ProjectionDefinition(3, 0, lambda s, e: s))
        await store.begin_rebuild(RebuildRun("invalid", key, 3, 4, 10, 0, 0))
        await runtime.resume_rebuild("invalid", ProjectionDefinition(3, 0, lambda s, e: s + 1))
        with pytest.raises(RuntimeError, match="coverage"):
            await runtime.resume_rebuild("invalid", ProjectionDefinition(3, 0, lambda s, e: s + 1))

    asyncio.run(run())


def test_async_job_and_timer_reference_fences_and_expiry():
    async def run():
        jobs = InMemoryAsyncJobStore(capacity=10)
        for identity in ("success", "retry", "terminal"):
            await jobs.schedule(Job(identity, "test", {}, NOW))
        claims = await jobs.claim(now=NOW, limit=3, claim_ttl=TTL)
        byid = {claim.job.job_id: claim for claim in claims}
        assert isinstance(
            await jobs.succeed(byid["success"], decision=Decision(), at=NOW), JobSucceeded
        )
        assert isinstance(
            await jobs.retry(byid["retry"], reason="retry", run_at=NOW + TTL, at=NOW), JobRetried
        )
        assert isinstance(
            await jobs.terminate(byid["terminal"], reason="done", at=NOW), JobTerminated
        )
        for method, kwargs in [
            (jobs.succeed, {"decision": Decision()}),
            (jobs.retry, {"reason": "later", "run_at": NOW}),
            (jobs.terminate, {"reason": "done"}),
        ]:
            assert isinstance(await method(byid["retry"], at=NOW + TTL, **kwargs), StaleJobClaim)
            with pytest.raises(ValueError):
                await method(byid["retry"], at=NOW.replace(tzinfo=None), **kwargs)
        processes = InMemoryAsyncProcessManagerStore()
        key = ProcessKey("test", "one")
        assert await processes.load(key) is None
        effect = ProcessEffect("one", ProcessEffectKind.TASK, "task", {})
        await processes.apply(
            key,
            expected_version=0,
            decision=ProcessDecision({}, schedule=(TimerSchedule("one", NOW, effect),)),
            at=NOW,
        )
        claim = (await processes.claim_timers(now=NOW, limit=1, claim_ttl=TTL))[0]
        assert not await processes.complete_timer(claim, at=NOW + TTL)
        with pytest.raises(ValueError):
            await processes.complete_timer(claim, at=NOW.replace(tzinfo=None))
        assert await processes.complete_timer(claim, at=NOW)
        with pytest.raises(ValueError):
            await processes.apply(
                key,
                expected_version=1,
                decision=ProcessDecision({}, schedule=(TimerSchedule("one", NOW, effect),)),
                at=NOW,
            )

    asyncio.run(run())
