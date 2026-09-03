"""Structured runtime supervision and bounded graceful shutdown."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

type SupervisedFactory = Callable[[asyncio.Event], Coroutine[Any, Any, None]]


@dataclass(frozen=True, slots=True)
class SupervisedTask:
    name: str
    factory: SupervisedFactory

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("supervised task name must not be empty")


@dataclass(frozen=True, slots=True)
class ShutdownSummary:
    completed: int
    cancelled: int


class OperationalSupervisor:
    """A single-use TaskGroup supervisor; it does not create processes or global tasks."""

    def __init__(self, tasks: Iterable[SupervisedTask]) -> None:
        selected = tuple(tasks)
        names = [task.name for task in selected]
        if not selected or len(names) != len(set(names)):
            raise ValueError("supervised tasks must be non-empty and uniquely named")
        self._specifications = selected
        self._stop = asyncio.Event()
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._running = False

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    async def run(self) -> None:
        if self._running:
            raise RuntimeError("supervisor is single-use")
        self._running = True
        async with asyncio.TaskGroup() as group:
            self._tasks = tuple(
                group.create_task(specification.factory(self._stop), name=specification.name)
                for specification in self._specifications
            )

    async def shutdown(self, *, grace: timedelta) -> ShutdownSummary:
        if grace <= timedelta(0):
            raise ValueError("shutdown grace must be positive")
        self._stop.set()
        if not self._tasks:
            return ShutdownSummary(0, 0)
        done, pending = await asyncio.wait(self._tasks, timeout=grace.total_seconds())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return ShutdownSummary(len(done), len(pending))
