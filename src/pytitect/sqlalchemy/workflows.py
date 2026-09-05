"""Optional process, timer and job adapters; all effects use the caller transaction."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.application import Decision
from pytitect.core import Clock, OpaqueId, SystemClock
from pytitect.inbox import InboxAccepted, InboxDecision, InboxScope
from pytitect.jobs import (
    Job,
    JobClaim,
    JobDuplicate,
    JobRetried,
    JobScheduled,
    JobScheduleResult,
    JobState,
    JobSucceeded,
    JobTerminated,
    JobTransition,
    StaleJobClaim,
    _claim_arguments,
    _reason,
    _utc,
)
from pytitect.processes import (
    ProcessApplied,
    ProcessApplyResult,
    ProcessDecision,
    ProcessEffect,
    ProcessEffectKind,
    ProcessKey,
    ProcessState,
    ProcessStatus,
    ProcessTimer,
    ProcessTimerClaim,
    StaleProcessVersion,
)
from pytitect.sqlalchemy.stores import SQLAlchemyInboxStore
from pytitect.sqlalchemy.uow import DecisionSaver

type EffectSaver = Callable[[AsyncSession, ProcessEffect], Awaitable[None]]


class SQLAlchemyProcessStore:
    def __init__(
        self,
        session: AsyncSession,
        *,
        process_model: type[Any],
        timer_model: type[Any],
        save_effect: EffectSaver,
    ) -> None:
        self.session, self.model, self.timer_model = session, process_model, timer_model
        self.save_effect = save_effect

    def _key(self, model: type[Any], key: ProcessKey) -> tuple[Any, ...]:
        return (model.process_name == key.name, model.instance_id == key.instance_id)

    def _state(self, row: Any) -> ProcessState:
        return ProcessState(
            ProcessKey(row.process_name, row.instance_id),
            row.version,
            ProcessStatus(row.status),
            row.state,
            row.updated_at,
        )

    async def load(self, key: ProcessKey) -> ProcessState | None:
        row = await self.session.execute(select(self.model).where(*self._key(self.model, key)))
        current = row.scalar_one_or_none()
        return None if current is None or current.version == 0 else self._state(current)

    async def apply(
        self,
        key: ProcessKey,
        *,
        expected_version: int,
        decision: ProcessDecision,
        at: datetime,
    ) -> ProcessApplyResult:
        _utc(at)
        if expected_version < 0:
            raise ValueError("expected process version must not be negative")
        async with self.session.begin_nested() as transaction:
            await self.session.execute(
                insert(self.model)
                .values(
                    process_name=key.name,
                    instance_id=key.instance_id,
                    version=0,
                    status=ProcessStatus.RUNNING.value,
                    state={},
                    updated_at=at,
                )
                .on_conflict_do_nothing(index_elements=["process_name", "instance_id"])
            )
            row = (
                await self.session.execute(
                    select(self.model)
                    .where(*self._key(self.model, key))
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one()
            if row.version != expected_version:
                actual = row.version
                await transaction.rollback()
                return StaleProcessVersion(expected_version, actual)
            cancelled = (
                await self.session.execute(
                    update(self.timer_model)
                    .where(
                        *self._key(self.timer_model, key),
                        self.timer_model.timer_id.in_(decision.cancel_timers),
                        self.timer_model.terminal.is_(False),
                    )
                    .values(terminal=True, claim_id=None, claimed_until=None)
                    .returning(self.timer_model.timer_id)
                )
            ).all()
            for timer in decision.schedule:
                self.session.add(
                    self.timer_model(
                        process_name=key.name,
                        instance_id=key.instance_id,
                        timer_id=timer.timer_id,
                        due_at=timer.due_at,
                        effect_id=timer.effect.effect_id,
                        effect_kind=timer.effect.kind.value,
                        effect_name=timer.effect.name,
                        effect_payload=timer.effect.payload,
                        terminal=False,
                        fencing_token=0,
                    )
                )
            for effect in decision.effects:
                await self.save_effect(self.session, effect)
            row.version, row.status, row.state, row.updated_at = (
                expected_version + 1,
                decision.status.value,
                decision.state,
                at,
            )
            await self.session.flush()
            return ProcessApplied(
                self._state(row), len(decision.effects), len(decision.schedule), len(cancelled)
            )

    async def apply_message(
        self,
        key: ProcessKey,
        *,
        expected_version: int,
        decision: ProcessDecision,
        inbox_model: type[Any],
        scope: InboxScope,
        message_id: OpaqueId[object],
        ttl: timedelta,
        clock: Clock | None = None,
    ) -> ProcessApplyResult | InboxDecision:
        selected_clock = clock or SystemClock()
        async with self.session.begin_nested() as transaction:
            inbox = SQLAlchemyInboxStore(self.session, inbox_model)
            token = uuid.uuid4().hex
            reservation = await inbox.begin(
                scope, message_id, token=token, now=selected_clock.now(), ttl=ttl
            )
            if not isinstance(reservation, InboxAccepted):
                return reservation
            result = await self.apply(
                key, expected_version=expected_version, decision=decision, at=selected_clock.now()
            )
            if isinstance(result, StaleProcessVersion):
                await transaction.rollback()
                return result
            if not await inbox.complete(scope, message_id, token=token, now=selected_clock.now()):
                raise RuntimeError("process inbox authority expired")
            return result

    async def claim_timers(
        self,
        *,
        now: datetime,
        limit: int,
        claim_ttl: timedelta,
    ) -> Sequence[ProcessTimerClaim]:
        _utc(now)
        _claim_arguments(limit, claim_ttl)
        m = self.timer_model
        rows = (
            (
                await self.session.execute(
                    select(m)
                    .where(
                        m.terminal.is_(False),
                        m.due_at <= now,
                        m.claimed_until.is_(None) | (m.claimed_until <= now),
                    )
                    .order_by(m.due_at, m.process_name, m.instance_id, m.timer_id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        )
        claims = []
        for row in rows:
            row.fencing_token += 1
            row.claim_id, row.claimed_until = uuid.uuid4().hex, now + claim_ttl
            timer = ProcessTimer(
                ProcessKey(row.process_name, row.instance_id),
                row.timer_id,
                row.due_at,
                ProcessEffect(
                    row.effect_id,
                    ProcessEffectKind(row.effect_kind),
                    row.effect_name,
                    row.effect_payload,
                ),
                row.fencing_token,
            )
            claims.append(ProcessTimerClaim(timer, row.claim_id, row.claimed_until))
        await self.session.flush()
        return claims

    async def complete_timer(self, claim: ProcessTimerClaim, *, at: datetime) -> bool:
        _utc(at)
        m = self.timer_model
        async with self.session.begin_nested() as transaction:
            row = (
                await self.session.execute(
                    select(m)
                    .where(
                        *self._key(m, claim.timer.process),
                        m.timer_id == claim.timer.timer_id,
                        m.claim_id == claim.claim_id,
                        m.fencing_token == claim.timer.fencing_token,
                        m.terminal.is_(False),
                        m.claimed_until == claim.claimed_until,
                        m.claimed_until > func.greatest(at, func.clock_timestamp()),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if row is None:
                return False
            await self.save_effect(self.session, claim.timer.effect)
            changed = (
                await self.session.execute(
                    update(m)
                    .where(
                        *self._key(m, claim.timer.process),
                        m.timer_id == claim.timer.timer_id,
                        m.claim_id == claim.claim_id,
                        m.fencing_token == claim.timer.fencing_token,
                        m.claimed_until > func.greatest(at, func.clock_timestamp()),
                    )
                    .values(terminal=True, claim_id=None, claimed_until=None)
                    .returning(m.timer_id)
                    .execution_options(synchronize_session=False)
                )
            ).scalar_one_or_none()
            if changed is None:
                await transaction.rollback()
                return False
            return True


class SQLAlchemyJobStore:
    def __init__(
        self, session: AsyncSession, model: type[Any], *, save_decision: DecisionSaver
    ) -> None:
        self.session, self.model, self.save_decision = session, model, save_decision

    def _job(self, row: Any) -> Job:
        return Job(
            row.job_id,
            row.task,
            row.payload,
            row.run_at,
            row.max_attempts,
            row.attempt,
            JobState(row.state),
            row.failure_reason,
        )

    async def get(self, job_id: str) -> Job | None:
        row = (
            await self.session.execute(select(self.model).where(self.model.job_id == job_id))
        ).scalar_one_or_none()
        return None if row is None else self._job(row)

    async def schedule(self, job: Job) -> JobScheduleResult:
        statement = (
            insert(self.model)
            .values(
                job_id=job.job_id,
                task=job.task,
                payload=job.payload,
                run_at=job.run_at,
                max_attempts=job.max_attempts,
                attempt=job.attempt,
                state=job.state.value,
                terminal=job.state is not JobState.SCHEDULED,
                failure_reason=job.last_failure,
                fencing_token=0,
            )
            .on_conflict_do_nothing(index_elements=["job_id"])
            .returning(self.model.job_id)
        )
        changed = (await self.session.execute(statement)).scalar_one_or_none()
        return JobScheduled() if changed is not None else JobDuplicate()

    async def claim(self, *, now: datetime, limit: int, claim_ttl: timedelta) -> Sequence[JobClaim]:
        _utc(now)
        _claim_arguments(limit, claim_ttl)
        m = self.model
        rows = (
            (
                await self.session.execute(
                    select(m)
                    .where(
                        m.state == JobState.SCHEDULED.value,
                        m.run_at <= now,
                        m.claimed_until.is_(None) | (m.claimed_until <= now),
                    )
                    .order_by(m.run_at, m.job_id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        )
        claims = []
        for row in rows:
            row.fencing_token += 1
            row.claim_id, row.claimed_until = uuid.uuid4().hex, now + claim_ttl
            claims.append(
                JobClaim(self._job(row), row.claim_id, row.claimed_until, row.fencing_token)
            )
        await self.session.flush()
        return claims

    async def _settle(self, claim: JobClaim, at: datetime, **values: Any) -> bool:
        _utc(at)
        m = self.model
        await self.session.execute(
            select(m.job_id).where(m.job_id == claim.job.job_id).with_for_update()
        )
        statement = (
            update(m)
            .where(
                m.job_id == claim.job.job_id,
                m.claim_id == claim.claim_id,
                m.fencing_token == claim.fencing_token,
                m.claimed_until == claim.claimed_until,
                m.claimed_until > func.greatest(at, func.clock_timestamp()),
                m.state == JobState.SCHEDULED.value,
            )
            .values(**values, claim_id=None, claimed_until=None)
            .returning(m.job_id)
            .execution_options(synchronize_session=False)
        )
        return (await self.session.execute(statement)).scalar_one_or_none() is not None

    async def succeed(self, claim: JobClaim, *, decision: Decision, at: datetime) -> JobTransition:
        async with self.session.begin_nested() as transaction:
            await self.save_decision(self.session, decision)
            if not await self._settle(claim, at, state=JobState.SUCCEEDED.value, terminal=True):
                await transaction.rollback()
                return StaleJobClaim()
            return JobSucceeded()

    async def retry(
        self, claim: JobClaim, *, reason: str, run_at: datetime, at: datetime
    ) -> JobTransition:
        _utc(run_at)
        reason = _reason(reason)
        attempt = claim.job.attempt + 1
        terminal = attempt >= claim.job.max_attempts
        if not await self._settle(
            claim,
            at,
            attempt=attempt,
            run_at=run_at,
            failure_reason=reason,
            terminal=terminal,
            state=JobState.TERMINAL.value if terminal else JobState.SCHEDULED.value,
        ):
            return StaleJobClaim()
        return JobTerminated(reason) if terminal else JobRetried(run_at)

    async def terminate(self, claim: JobClaim, *, reason: str, at: datetime) -> JobTransition:
        reason = _reason(reason)
        if not await self._settle(
            claim, at, failure_reason=reason, terminal=True, state=JobState.TERMINAL.value
        ):
            return StaleJobClaim()
        return JobTerminated(reason)
