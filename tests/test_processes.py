from datetime import UTC, datetime, timedelta

from pytitect.processes import (
    InMemoryProcessManagerStore,
    ProcessApplied,
    ProcessDecision,
    ProcessEffect,
    ProcessEffectKind,
    ProcessKey,
    ProcessManagerBinding,
    ProcessManagerRegistry,
    ProcessManagerRuntime,
    ProcessStatus,
    StaleProcessVersion,
    TimerSchedule,
)

NOW = datetime(2026, 9, 3, tzinfo=UTC)


def effect(identifier: str, kind: ProcessEffectKind = ProcessEffectKind.COMMAND) -> ProcessEffect:
    return ProcessEffect(identifier, kind, "continue", {"step": 2})


def test_process_state_effects_and_timers_transition_atomically() -> None:
    store = InMemoryProcessManagerStore()
    key = ProcessKey("example-process", "instance-1")
    decision = ProcessDecision(
        {"step": 1},
        effects=(effect("effect-1"),),
        schedule=(TimerSchedule("timer-1", NOW, effect("timer-effect")),),
    )
    applied = store.apply(key, expected_version=0, decision=decision, at=NOW)
    assert isinstance(applied, ProcessApplied)
    assert applied.state.version == 1
    assert store.pending_effects == ((key, effect("effect-1")),)
    stale = store.apply(key, expected_version=0, decision=decision, at=NOW)
    assert stale == StaleProcessVersion(0, 1)


def test_timer_claims_are_fenced_and_stale_workers_cannot_complete() -> None:
    store = InMemoryProcessManagerStore()
    key = ProcessKey("example-process", "instance-1")
    store.apply(
        key,
        expected_version=0,
        decision=ProcessDecision(
            {}, schedule=(TimerSchedule("timer", NOW, effect("timer-effect")),)
        ),
        at=NOW,
    )
    first = store.claim_timers(now=NOW, limit=1, claim_ttl=timedelta(seconds=5))[0]
    second = store.claim_timers(
        now=NOW + timedelta(seconds=5), limit=1, claim_ttl=timedelta(seconds=5)
    )[0]
    assert second.timer.fencing_token == first.timer.fencing_token + 1
    assert not store.complete_timer(first)
    assert store.complete_timer(second)


def test_runtime_supports_explicit_compensation_decisions() -> None:
    store = InMemoryProcessManagerStore()
    registry = ProcessManagerRegistry(
        [
            ProcessManagerBinding(
                "example-process",
                lambda state, value: ProcessDecision(
                    {"failed": True},
                    ProcessStatus.COMPENSATING,
                    effects=(effect("compensate-1", ProcessEffectKind.COMPENSATION),),
                ),
            )
        ]
    )
    result = ProcessManagerRuntime(store, registry, now=lambda: NOW).handle(
        ProcessKey("example-process", "instance-1"), {"failed": True}
    )
    assert isinstance(result, ProcessApplied)
    assert result.state.status is ProcessStatus.COMPENSATING
