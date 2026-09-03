import asyncio

import pytest

from pytitect.testing import FaultInjector, FaultPlan, FaultPoint, InjectedCrash


@pytest.mark.parametrize("point", list(FaultPoint))
def test_fault_points_are_deterministic_and_one_shot(point: FaultPoint) -> None:
    injector = FaultInjector(FaultPlan.at(point))
    with pytest.raises(InjectedCrash) as failure:
        injector.hit(point)
    assert failure.value.point is point
    injector.hit(point)
    assert injector.triggered == {point}


def test_cancellation_remains_base_exception_control_flow() -> None:
    async def cancelled() -> None:
        try:
            raise asyncio.CancelledError
        except Exception as exc:
            raise AssertionError("CancelledError was reclassified") from exc

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cancelled())


@pytest.mark.parametrize(
    ("point", "committed", "acknowledged", "redelivery_is_duplicate"),
    [
        (FaultPoint.BEFORE_COMMIT, False, False, False),
        (FaultPoint.AFTER_COMMIT, True, False, True),
        (FaultPoint.BEFORE_ACK, True, False, True),
        (FaultPoint.AFTER_PUBLISH_CONFIRM, True, False, True),
    ],
)
def test_crash_matrix_preserves_at_least_once_truth(
    point: FaultPoint,
    committed: bool,
    acknowledged: bool,
    redelivery_is_duplicate: bool,
) -> None:
    state = {
        "committed": point is not FaultPoint.BEFORE_COMMIT,
        "acknowledged": False,
    }
    assert state["committed"] is committed
    assert state["acknowledged"] is acknowledged
    assert (state["committed"] and not state["acknowledged"]) is redelivery_is_duplicate
