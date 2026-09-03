"""Finite, explicit retention and archival plan values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class MaintenanceSummary:
    selected: int
    affected: int
    dry_run: bool

    def __post_init__(self) -> None:
        if (
            isinstance(self.selected, bool)
            or not isinstance(self.selected, int)
            or self.selected < 0
            or isinstance(self.affected, bool)
            or not isinstance(self.affected, int)
            or self.affected < 0
            or self.affected > self.selected
        ):
            raise ValueError("maintenance counts must be non-negative and affected <= selected")
        if self.dry_run and self.affected:
            raise ValueError("dry-run maintenance cannot report affected records")
        if not isinstance(self.dry_run, bool):
            raise ValueError("maintenance dry_run must be a boolean")


@dataclass(frozen=True, slots=True)
class _MaintenancePlan:
    cutoff: datetime
    batch_size: int = 1_000
    dry_run: bool = False

    def __post_init__(self) -> None:
        _utc(self.cutoff)
        if (
            isinstance(self.batch_size, bool)
            or not isinstance(self.batch_size, int)
            or self.batch_size <= 0
        ):
            raise ValueError("maintenance batch_size must be a positive integer")
        if not isinstance(self.dry_run, bool):
            raise ValueError("maintenance dry_run must be a boolean")


@dataclass(frozen=True, slots=True)
class PurgeIdempotencyPlan(_MaintenancePlan):
    include_uncertain: bool = False

    def __post_init__(self) -> None:
        _MaintenancePlan.__post_init__(self)
        if not isinstance(self.include_uncertain, bool):
            raise ValueError("include_uncertain must be a boolean")


@dataclass(frozen=True, slots=True)
class PurgeReplayPlan(_MaintenancePlan):
    pass


@dataclass(frozen=True, slots=True)
class PurgeInboxPlan(_MaintenancePlan):
    pass


@dataclass(frozen=True, slots=True)
class PurgeReceiptsPlan(_MaintenancePlan):
    include_uncertain: bool = False

    def __post_init__(self) -> None:
        _MaintenancePlan.__post_init__(self)
        if not isinstance(self.include_uncertain, bool):
            raise ValueError("include_uncertain must be a boolean")


@dataclass(frozen=True, slots=True)
class PurgeDeliveredOutboxPlan(_MaintenancePlan):
    pass


@dataclass(frozen=True, slots=True)
class ArchiveFailedOutboxPlan(_MaintenancePlan):
    pass


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("maintenance cutoff must be timezone-aware UTC")
