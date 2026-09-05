import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from pytitect.aio import AsyncProjectionRuntime
from pytitect.application import Decision
from pytitect.core import OpaqueId
from pytitect.event_sourcing import (
    AppendCommitted,
    DuplicateEventId,
    NewEvent,
    Snapshot,
    StreamId,
    WrongExpectedVersion,
)
from pytitect.inbox import InboxDuplicate, InboxScope
from pytitect.jobs import Job, JobDuplicate, JobRetried, JobSucceeded, JobTerminated, StaleJobClaim
from pytitect.processes import (
    ProcessApplied,
    ProcessDecision,
    ProcessEffect,
    ProcessEffectKind,
    ProcessKey,
    StaleProcessVersion,
    TimerSchedule,
)
from pytitect.projections import (
    ProjectionApplied,
    ProjectionDefinition,
    ProjectionKey,
    RebuildRun,
    RebuildStatus,
)
from pytitect.sqlalchemy.events import SQLAlchemyEventStore
from pytitect.sqlalchemy.projections import SQLAlchemyProjectionStore
from pytitect.sqlalchemy.workflows import SQLAlchemyJobStore, SQLAlchemyProcessStore
from tests.integration.support import (
    Database,
    Effect,
    Event,
    Inbox,
    JobRow,
    Log,
    Process,
    Projection,
    Rebuild,
    SnapshotRow,
    Timer,
    now,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]
TTL = timedelta(seconds=30)


def event_store(session):
    return SQLAlchemyEventStore(
        session,
        event_model=Event,
        log_model=Log,
        snapshot_model=SnapshotRow,
        log_id="test",
        max_page_size=10,
    )


def projection_store(session):
    return SQLAlchemyProjectionStore(
        session, projection_model=Projection, rebuild_model=Rebuild, events=event_store(session)
    )


async def save_effect(session, effect):
    from pytitect.core import OpaqueId
    from pytitect.outbox import OutboxEnvelope
    from pytitect.sqlalchemy.stores import SQLAlchemyOutboxStore
    from tests.integration.support import CODEC, Outbox, message

    msg = message(effect.effect_id)
    await SQLAlchemyOutboxStore(session, Outbox, CODEC).add(
        OutboxEnvelope(OpaqueId(msg.id), "effects", msg, msg.time, msg.time)
    )
    session.add(Effect(identity=effect.effect_id, value=effect.payload["value"]))
    await session.flush()


async def save_decision(session, decision):
    session.add(Effect(identity=decision.result["identity"], value=1))
    await session.flush()


def process_store(session):
    return SQLAlchemyProcessStore(
        session, process_model=Process, timer_model=Timer, save_effect=save_effect
    )


def test_process_inbox_effects_timers_are_atomic_and_fenced_after_restart():
    async def run():
        async with Database() as db:
            key = ProcessKey("synthetic", "one")
            scope = InboxScope("test", "source", "process")
            effect = ProcessEffect(
                "one", ProcessEffectKind.INTEGRATION_EVENT, "changed", {"value": 1}
            )
            timer_effect = replace(effect, effect_id="timer")
            decision = ProcessDecision(
                {"count": 1},
                effects=(effect,),
                schedule=(TimerSchedule("timer", now(), timer_effect),),
            )
            async with db.sessions() as session, session.begin():
                store = process_store(session)
                result = await store.apply_message(
                    key,
                    expected_version=0,
                    decision=decision,
                    inbox_model=Inbox,
                    scope=scope,
                    message_id=OpaqueId("input"),
                    ttl=TTL,
                )
                assert isinstance(result, ProcessApplied)
            async with db.sessions() as session, session.begin():
                store = process_store(session)
                assert (await store.load(key)).version == 1
                assert isinstance(
                    await store.apply_message(
                        key,
                        expected_version=0,
                        decision=decision,
                        inbox_model=Inbox,
                        scope=scope,
                        message_id=OpaqueId("input"),
                        ttl=TTL,
                    ),
                    InboxDuplicate,
                )
                assert isinstance(
                    await store.apply(key, expected_version=0, decision=decision, at=now()),
                    StaleProcessVersion,
                )
                old = (await store.claim_timers(now=now(), limit=1, claim_ttl=TTL))[0]
            async with db.sessions() as session, session.begin():
                await session.execute(update(Timer).values(claimed_until=now() - TTL))
            async with db.sessions() as session, session.begin():
                store = process_store(session)
                current = (await store.claim_timers(now=now(), limit=1, claim_ttl=TTL))[0]
                assert current.timer.fencing_token > old.timer.fencing_token
                assert not await store.complete_timer(old, at=now())
                assert await store.complete_timer(current, at=now())
                assert not await store.complete_timer(current, at=now())
            async with db.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Effect)) == 2
                assert (
                    await session.scalar(
                        select(func.count()).select_from(Timer).where(Timer.terminal.is_(True))
                    )
                    == 1
                )
            # Reusing a retired timer cannot resurrect an old authority or partially update state.
            async with db.sessions() as session, session.begin():
                with pytest.raises(IntegrityError):
                    await process_store(session).apply(
                        key, expected_version=1, decision=decision, at=now()
                    )
            async with db.sessions() as session:
                assert (await process_store(session).load(key)).version == 1
                assert await session.scalar(select(func.count()).select_from(Effect)) == 2

            # A callback failure rolls back state, inbox and scheduled timers together.
            async def fail_effect(session, effect):
                await save_effect(session, replace(effect, effect_id="rollback"))
                raise OSError("synthetic persistence failure")

            async with db.sessions() as session, session.begin():
                with pytest.raises(OSError):
                    await SQLAlchemyProcessStore(
                        session, process_model=Process, timer_model=Timer, save_effect=fail_effect
                    ).apply_message(
                        key,
                        expected_version=1,
                        decision=ProcessDecision({}, effects=(effect,)),
                        inbox_model=Inbox,
                        scope=scope,
                        message_id=OpaqueId("rollback"),
                        ttl=TTL,
                    )
            async with db.sessions() as session:
                assert await session.get(Effect, "rollback") is None
                assert await session.scalar(select(func.count()).select_from(Inbox)) == 1

    asyncio.run(run())


def test_jobs_concurrent_claims_expiry_takeover_and_atomic_effects():
    async def run():
        async with Database() as db:
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyJobStore(session, JobRow, save_decision=save_decision)
                for identity in ("one", "two"):
                    await store.schedule(Job(identity, "test", {}, now(), max_attempts=2))
                assert isinstance(await store.schedule(Job("one", "test", {}, now())), JobDuplicate)

            async def claim():
                async with db.sessions() as session, session.begin():
                    return await SQLAlchemyJobStore(
                        session, JobRow, save_decision=save_decision
                    ).claim(now=now(), limit=1, claim_ttl=TTL)

            groups = await asyncio.gather(claim(), claim())
            old, other = groups[0][0], groups[1][0]
            assert old.job.job_id != other.job.job_id
            async with db.sessions() as session, session.begin():
                await session.execute(
                    update(JobRow)
                    .where(JobRow.job_id == old.job.job_id)
                    .values(claimed_until=now() - TTL)
                )
            current = (await claim())[0]
            assert current.fencing_token > old.fencing_token
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyJobStore(session, JobRow, save_decision=save_decision)
                assert isinstance(
                    await store.succeed(
                        old, decision=Decision(result={"identity": "stale"}), at=now()
                    ),
                    StaleJobClaim,
                )
                assert isinstance(
                    await store.succeed(
                        current, decision=Decision(result={"identity": "success"}), at=now()
                    ),
                    JobSucceeded,
                )
                assert isinstance(
                    await store.retry(other, reason="later", run_at=now(), at=now()), JobRetried
                )
            retried = (await claim())[0]
            async with db.sessions() as session, session.begin():
                store = SQLAlchemyJobStore(session, JobRow, save_decision=save_decision)
                assert isinstance(
                    await store.retry(retried, reason="exhausted", run_at=now(), at=now()),
                    JobTerminated,
                )
            async with db.sessions() as session:
                assert await session.get(Effect, "stale") is None
                assert await session.get(Effect, "success") is not None
            assert not await claim()

    asyncio.run(run())


def test_log_commit_order_rollback_uniqueness_snapshots_and_fixed_rebuild():
    async def run():
        async with Database() as db:
            stream = StreamId("synthetic", "one")

            def events(start, count):
                return [
                    NewEvent(str(i), "increment", {"value": 1}, now())
                    for i in range(start, start + count)
                ]

            async with db.sessions() as first:
                await first.begin()
                append = await event_store(first).append(
                    stream, expected_version=0, events=events(1, 2)
                )
                assert isinstance(append, AppendCommitted)
                started = asyncio.Event()

                async def second_append():
                    async with db.sessions() as second, second.begin():
                        started.set()
                        return await event_store(second).append(
                            StreamId("synthetic", "two"), expected_version=0, events=events(3, 1)
                        )

                task = asyncio.create_task(second_append())
                await started.wait()
                async with db.sessions() as reader:
                    assert not (await event_store(reader).read_all(limit=10)).events
                await first.commit()
                later = await task
                assert later.events[0].global_position == 3
            async with db.sessions() as session, session.begin():
                store = event_store(session)
                assert isinstance(
                    await store.append(stream, expected_version=0, events=events(4, 1)),
                    WrongExpectedVersion,
                )
                assert isinstance(
                    await store.append(stream, expected_version=2, events=events(1, 1)),
                    DuplicateEventId,
                )
                assert await store.watermark() == 3
                with pytest.raises(ValueError, match="snapshot"):
                    await store.save_snapshot(Snapshot(stream, 9, {}, now()), expected_version=None)
                assert await store.save_snapshot(
                    Snapshot(stream, 2, {}, now()), expected_version=None
                )
                assert not await store.save_snapshot(
                    Snapshot(stream, 2, {}, now()), expected_version=None
                )
            key = ProjectionKey("count", "test")
            definition = ProjectionDefinition(
                1, {"count": 0}, lambda state, event: {"count": state["count"] + 1}
            )
            async with db.sessions() as session, session.begin():
                assert await projection_store(session).begin_rebuild(
                    RebuildRun("rebuild", key, 1, 3, 1, 0, {"count": 0})
                )
            for _ in range(3):
                async with db.sessions() as session, session.begin():
                    advanced = await AsyncProjectionRuntime(
                        projection_store(session), event_store(session)
                    ).resume_rebuild("rebuild", definition)
            assert advanced.status is RebuildStatus.COMPLETED
            async with db.sessions() as session, session.begin():
                state = await projection_store(session).load(key)
                assert state.checkpoint == 3 and state.state == {"count": 3}
                assert (await event_store(session).load_snapshot(stream)).version == 2
                assert isinstance(
                    await AsyncProjectionRuntime(
                        projection_store(session), event_store(session)
                    ).project_once(key, definition, limit=2),
                    ProjectionApplied,
                )
                await event_store(session).append(stream, expected_version=2, events=events(4, 1))
            async with db.sessions() as session, session.begin():
                result = await AsyncProjectionRuntime(
                    projection_store(session), event_store(session)
                ).project_once(key, definition, limit=2)
                assert result.state.checkpoint == 4 and result.state.state == {"count": 4}
                assert await projection_store(session).begin_rebuild(
                    RebuildRun("old", key, 2, 3, 3, 0, {"count": 0})
                )
                assert (
                    await projection_store(session).advance_rebuild(
                        "old",
                        expected_position=0,
                        state={"count": 3},
                        next_position=3,
                        complete=True,
                    )
                    is None
                )
            async with db.sessions() as session:
                assert (await projection_store(session).load(key)).checkpoint == 4
                assert (await projection_store(session).load_rebuild("old")).next_position == 0

    asyncio.run(run())
