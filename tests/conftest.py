from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class ManualClock:
    value: datetime = datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta
