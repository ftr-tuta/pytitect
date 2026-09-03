from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pytitect.application import Decision
from pytitect.jobs import (
    InMemoryJobStore,
    Job,
    JobRunner,
    JobSchedule,
    JobState,
    ScheduleKind,
    StaleJobClaim,
)

NOW = datetime(2026, 9, 3, tzinfo=UTC)


@dataclass(frozen=True)
class Clock:
    def now(self) -> datetime:
        return NOW


def test_expired_job_claim_is_replaced_with_higher_fence() -> None:
    store = InMemoryJobStore()
    store.schedule(Job("job-1", "task", {}, NOW))
    first = store.claim(now=NOW, limit=1, claim_ttl=timedelta(seconds=5))[0]
    second = store.claim(now=NOW + timedelta(seconds=5), limit=1, claim_ttl=timedelta(seconds=5))[0]
    assert second.fencing_token == first.fencing_token + 1
    assert isinstance(store.succeed(first, decision=Decision(), at=NOW), StaleJobClaim)


def test_runner_retries_then_records_decision_atomically_on_success() -> None:
    store = InMemoryJobStore()
    store.schedule(Job("job-1", "task", {}, NOW))
    calls = 0

    def handler(job: Job) -> Decision:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return Decision(result={"done": True})

    runner = JobRunner(store, {"task": handler}, clock=Clock())
    first = runner.run_once(limit=1, claim_ttl=timedelta(seconds=5))
    assert first.retried == 1
    retried = store.get("job-1")
    assert retried is not None and retried.state is JobState.SCHEDULED
    claim = store.claim(now=retried.run_at, limit=1, claim_ttl=timedelta(seconds=5))[0]
    assert store.succeed(claim, decision=handler(claim.job), at=retried.run_at)
    assert len(store.decisions) == 1


def test_fixed_interval_uses_intended_schedule_without_drift() -> None:
    store = InMemoryJobStore()
    store.add_schedule(
        JobSchedule(
            "schedule-1",
            "task",
            {},
            NOW,
            ScheduleKind.FIXED_INTERVAL,
            interval=timedelta(minutes=5),
        )
    )
    assert store.materialize(now=NOW + timedelta(hours=1), limit=1) == 1
    assert store.get("schedule-1:0").run_at == NOW  # type: ignore[union-attr]
    assert store.materialize(now=NOW + timedelta(hours=1), limit=1) == 1
    assert store.get("schedule-1:1").run_at == NOW + timedelta(minutes=5)  # type: ignore[union-attr]
