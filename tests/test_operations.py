import asyncio
from datetime import timedelta

from pytitect.aio import OperationalSupervisor, SupervisedTask
from pytitect.operations import (
    ProbeResult,
    ReadinessPolicy,
    RuntimeRole,
    evaluate_readiness,
    safe_failure_reason,
)


class Probe:
    def __init__(self, name: str, ready: bool) -> None:
        self._name = name
        self._ready = ready

    @property
    def name(self) -> str:
        return self._name

    async def check(self) -> ProbeResult:
        return ProbeResult(self.name, self._ready)


def test_readiness_is_role_specific() -> None:
    api = asyncio.run(
        evaluate_readiness(ReadinessPolicy(RuntimeRole.API, (Probe("database", True),)))
    )
    relay = asyncio.run(
        evaluate_readiness(
            ReadinessPolicy(
                RuntimeRole.RELAY,
                (Probe("database", True), Probe("broker", False)),
            )
        )
    )
    assert api.ready
    assert not relay.ready


def test_supervisor_requests_stop_then_cancels_after_grace() -> None:
    events: list[str] = []

    async def cooperative(stop: asyncio.Event) -> None:
        await stop.wait()
        events.append("cooperative")

    async def stuck(stop: asyncio.Event) -> None:
        del stop
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("cancelled")
            raise

    async def exercise() -> object:
        supervisor = OperationalSupervisor(
            [SupervisedTask("cooperative", cooperative), SupervisedTask("stuck", stuck)]
        )
        running = asyncio.create_task(supervisor.run())
        await asyncio.sleep(0)
        summary = await supervisor.shutdown(grace=timedelta(milliseconds=1))
        await running
        return summary

    summary = asyncio.run(exercise())
    assert summary.completed == 1
    assert summary.cancelled == 1
    assert events == ["cooperative", "cancelled"]


def test_failure_reasons_are_single_line_and_bounded() -> None:
    reason = safe_failure_reason(ValueError("private\ntext"), max_chars=20)
    assert "\n" not in reason
    assert len(reason) <= 20
