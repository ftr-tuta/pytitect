"""Preview durable jobs, tasks, leases, retries, and schedules."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Protocol

from pytitect.application import Decision
from pytitect.core import Clock, JsonValue, SystemClock, validate_json
from pytitect.outbox import RetryPolicy


class JobState(StrEnum):
    SCHEDULED = "scheduled"
    SUCCEEDED = "succeeded"
    TERMINAL = "terminal"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class Job:
    job_id: str
    task: str
    payload: JsonValue
    run_at: datetime
    max_attempts: int = 10
    attempt: int = 0
    state: JobState = JobState.SCHEDULED
    last_failure: str | None = None

    def __post_init__(self) -> None:
        if not self.job_id or not self.task:
            raise ValueError("job identity and task must not be empty")
        if self.max_attempts <= 0 or not 0 <= self.attempt <= self.max_attempts:
            raise ValueError("job attempts are invalid")
        validate_json(self.payload)
        _utc(self.run_at)


@dataclass(frozen=True, slots=True)
class JobClaim:
    job: Job
    claim_id: str
    claimed_until: datetime
    fencing_token: int

    def __post_init__(self) -> None:
        if not self.claim_id or self.fencing_token <= 0:
            raise ValueError("job claim identity and fencing token are required")
        _utc(self.claimed_until)


@dataclass(frozen=True, slots=True)
class JobScheduled:
    pass


@dataclass(frozen=True, slots=True)
class JobDuplicate:
    pass


type JobScheduleResult = JobScheduled | JobDuplicate


@dataclass(frozen=True, slots=True)
class JobSucceeded:
    pass


@dataclass(frozen=True, slots=True)
class JobRetried:
    run_at: datetime


@dataclass(frozen=True, slots=True)
class JobTerminated:
    reason: str


@dataclass(frozen=True, slots=True)
class StaleJobClaim:
    pass


type JobTransition = JobSucceeded | JobRetried | JobTerminated | StaleJobClaim


class ScheduleKind(StrEnum):
    ONE_SHOT = "one_shot"
    FIXED_INTERVAL = "fixed_interval"
    CONSUMER_POLICY = "consumer_policy"


@dataclass(frozen=True, slots=True)
class JobSchedule:
    schedule_id: str
    task: str
    payload: JsonValue
    next_run: datetime
    kind: ScheduleKind
    interval: timedelta | None = None
    policy: str | None = None
    max_attempts: int = 10
    sequence: int = 0
    active: bool = True

    def __post_init__(self) -> None:
        if not self.schedule_id or not self.task:
            raise ValueError("schedule identity and task must not be empty")
        _utc(self.next_run)
        validate_json(self.payload)
        if self.kind is ScheduleKind.FIXED_INTERVAL and (
            self.interval is None or self.interval <= timedelta(0)
        ):
            raise ValueError("fixed-interval schedules require a positive interval")
        if self.kind is ScheduleKind.CONSUMER_POLICY and not self.policy:
            raise ValueError("consumer-policy schedules require a policy name")
        if self.kind is ScheduleKind.ONE_SHOT and (
            self.interval is not None or self.policy is not None
        ):
            raise ValueError("one-shot schedules do not accept interval or policy")


type NextRunPolicy = Callable[[JobSchedule], datetime | None]


class JobStore(Protocol):
    def schedule(self, job: Job) -> JobScheduleResult: ...

    def claim(self, *, now: datetime, limit: int, claim_ttl: timedelta) -> Sequence[JobClaim]: ...

    def succeed(self, claim: JobClaim, *, decision: Decision, at: datetime) -> JobTransition: ...

    def retry(self, claim: JobClaim, *, reason: str, run_at: datetime) -> JobTransition: ...

    def terminate(self, claim: JobClaim, *, reason: str, at: datetime) -> JobTransition: ...


@dataclass(slots=True)
class _StoredJob:
    job: Job
    claim_id: str | None = None
    claimed_until: datetime | None = None
    fencing_token: int = 0


class InMemoryJobStore:
    """Finite process-local job store with no durability or cross-process coordination."""

    def __init__(self, *, capacity: int = 10_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("job capacity must be a positive integer")
        self._capacity = capacity
        self._jobs: dict[str, _StoredJob] = {}
        self._schedules: dict[str, JobSchedule] = {}
        self._decisions: list[tuple[str, Decision]] = []
        self._lock = threading.RLock()

    @property
    def decisions(self) -> tuple[tuple[str, Decision], ...]:
        with self._lock:
            return tuple(self._decisions)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            stored = self._jobs.get(job_id)
            return None if stored is None else stored.job

    def schedule(self, job: Job) -> JobScheduleResult:
        with self._lock:
            if job.job_id in self._jobs:
                return JobDuplicate()
            self._ensure_capacity(1)
            self._jobs[job.job_id] = _StoredJob(job)
            return JobScheduled()

    def claim(self, *, now: datetime, limit: int, claim_ttl: timedelta) -> Sequence[JobClaim]:
        _utc(now)
        _claim_arguments(limit, claim_ttl)
        claims: list[JobClaim] = []
        with self._lock:
            eligible = sorted(
                self._jobs.values(), key=lambda item: (item.job.run_at, item.job.job_id)
            )
            for item in eligible:
                if len(claims) >= limit:
                    break
                if item.job.state is not JobState.SCHEDULED or item.job.run_at > now:
                    continue
                if item.claimed_until is not None and item.claimed_until > now:
                    continue
                item.fencing_token += 1
                item.claim_id = uuid.uuid4().hex
                item.claimed_until = now + claim_ttl
                claims.append(
                    JobClaim(item.job, item.claim_id, item.claimed_until, item.fencing_token)
                )
        return claims

    def succeed(self, claim: JobClaim, *, decision: Decision, at: datetime) -> JobTransition:
        _utc(at)
        with self._lock:
            item = self._valid(claim)
            if item is None:
                return StaleJobClaim()
            if len(self._decisions) >= self._capacity:
                raise OverflowError("job decision capacity exceeded")
            item.job = replace(item.job, state=JobState.SUCCEEDED)
            self._decisions.append((item.job.job_id, decision))
            self._release(item)
            return JobSucceeded()

    def retry(self, claim: JobClaim, *, reason: str, run_at: datetime) -> JobTransition:
        _utc(run_at)
        sanitized = _reason(reason)
        with self._lock:
            item = self._valid(claim)
            if item is None:
                return StaleJobClaim()
            attempt = item.job.attempt + 1
            if attempt >= item.job.max_attempts:
                item.job = replace(
                    item.job,
                    attempt=attempt,
                    state=JobState.TERMINAL,
                    last_failure=sanitized,
                )
                self._release(item)
                return JobTerminated(sanitized)
            item.job = replace(
                item.job,
                attempt=attempt,
                run_at=run_at,
                last_failure=sanitized,
            )
            self._release(item)
            return JobRetried(run_at)

    def terminate(self, claim: JobClaim, *, reason: str, at: datetime) -> JobTransition:
        _utc(at)
        sanitized = _reason(reason)
        with self._lock:
            item = self._valid(claim)
            if item is None:
                return StaleJobClaim()
            item.job = replace(item.job, state=JobState.TERMINAL, last_failure=sanitized)
            self._release(item)
            return JobTerminated(sanitized)

    def add_schedule(self, schedule: JobSchedule) -> bool:
        with self._lock:
            if schedule.schedule_id in self._schedules:
                return False
            self._ensure_capacity(1)
            self._schedules[schedule.schedule_id] = schedule
            return True

    def materialize(
        self,
        *,
        now: datetime,
        limit: int,
        policies: Mapping[str, NextRunPolicy] | None = None,
    ) -> int:
        _utc(now)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("materialize limit must be a positive integer")
        selected_policies = policies or {}
        created = 0
        with self._lock:
            schedules = sorted(
                self._schedules.values(), key=lambda item: (item.next_run, item.schedule_id)
            )
            for schedule in schedules:
                if created >= limit or not schedule.active or schedule.next_run > now:
                    continue
                job_id = f"{schedule.schedule_id}:{schedule.sequence}"
                if job_id not in self._jobs:
                    self._ensure_capacity(1)
                    self._jobs[job_id] = _StoredJob(
                        Job(
                            job_id,
                            schedule.task,
                            schedule.payload,
                            schedule.next_run,
                            schedule.max_attempts,
                        )
                    )
                    created += 1
                self._schedules[schedule.schedule_id] = self._advance_schedule(
                    schedule, selected_policies
                )
        return created

    def _advance_schedule(
        self, schedule: JobSchedule, policies: Mapping[str, NextRunPolicy]
    ) -> JobSchedule:
        if schedule.kind is ScheduleKind.ONE_SHOT:
            return replace(schedule, sequence=schedule.sequence + 1, active=False)
        if schedule.kind is ScheduleKind.FIXED_INTERVAL:
            assert schedule.interval is not None
            return replace(
                schedule,
                sequence=schedule.sequence + 1,
                next_run=schedule.next_run + schedule.interval,
            )
        assert schedule.policy is not None
        try:
            next_run = policies[schedule.policy](schedule)
        except KeyError as exc:
            raise LookupError(f"unknown next-run policy: {schedule.policy}") from exc
        if next_run is None:
            return replace(schedule, sequence=schedule.sequence + 1, active=False)
        _utc(next_run)
        if next_run <= schedule.next_run:
            raise ValueError("next-run policy must advance time")
        return replace(schedule, sequence=schedule.sequence + 1, next_run=next_run)

    def _valid(self, claim: JobClaim) -> _StoredJob | None:
        item = self._jobs.get(claim.job.job_id)
        if (
            item is None
            or item.claim_id != claim.claim_id
            or item.fencing_token != claim.fencing_token
            or item.job.state is not JobState.SCHEDULED
        ):
            return None
        return item

    @staticmethod
    def _release(item: _StoredJob) -> None:
        item.claim_id = None
        item.claimed_until = None

    def _ensure_capacity(self, count: int) -> None:
        if len(self._jobs) + len(self._schedules) + len(self._decisions) + count > self._capacity:
            raise OverflowError("job store capacity exceeded")


class PermanentJobError(Exception):
    pass


type JobHandler = Callable[[Job], Decision]


@dataclass(frozen=True, slots=True)
class JobRunSummary:
    claimed: int
    succeeded: int
    retried: int
    terminated: int


class JobRunner:
    """One explicit bounded run; constructing it does not start a worker."""

    def __init__(
        self,
        store: JobStore,
        handlers: Mapping[str, JobHandler],
        *,
        clock: Clock | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._store = store
        self._handlers = dict(handlers)
        self._clock = clock or SystemClock()
        self._retry = retry_policy or RetryPolicy()

    def run_once(self, *, limit: int, claim_ttl: timedelta) -> JobRunSummary:
        now = self._clock.now()
        claims = self._store.claim(now=now, limit=limit, claim_ttl=claim_ttl)
        succeeded = retried = terminated = 0
        for claim in claims:
            try:
                handler = self._handlers[claim.job.task]
                decision = handler(claim.job)
            except PermanentJobError as exc:
                result = self._store.terminate(claim, reason=str(exc), at=now)
            except Exception as exc:
                attempt = claim.job.attempt + 1
                result = self._store.retry(
                    claim,
                    reason=type(exc).__name__,
                    run_at=now + self._retry.delay(attempt),
                )
            else:
                result = self._store.succeed(claim, decision=decision, at=now)
            succeeded += int(isinstance(result, JobSucceeded))
            retried += int(isinstance(result, JobRetried))
            terminated += int(isinstance(result, JobTerminated))
        return JobRunSummary(len(claims), succeeded, retried, terminated)


def _reason(reason: str) -> str:
    sanitized = " ".join(reason.split())[:512]
    if not sanitized:
        raise ValueError("job failure reason must not be empty")
    return sanitized


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("job timestamps must be timezone-aware UTC")


def _claim_arguments(limit: int, claim_ttl: timedelta) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("claim limit must be a positive integer")
    if claim_ttl <= timedelta(0):
        raise ValueError("claim ttl must be positive")


__all__ = [
    "InMemoryJobStore",
    "Job",
    "JobClaim",
    "JobDuplicate",
    "JobRetried",
    "JobRunSummary",
    "JobRunner",
    "JobSchedule",
    "JobScheduled",
    "JobState",
    "JobStore",
    "JobSucceeded",
    "JobTerminated",
    "PermanentJobError",
    "ScheduleKind",
    "StaleJobClaim",
]
