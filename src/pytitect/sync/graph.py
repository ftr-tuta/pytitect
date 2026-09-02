"""Finite dataset dependency validation and stable ordering."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DependencyGraphLimits:
    max_datasets: int = 256
    max_partitions: int = 1_024

    def __post_init__(self) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (self.max_datasets, self.max_partitions)
        ):
            raise ValueError("dependency graph limits must be positive")


@dataclass(frozen=True, slots=True)
class DependencyCycle:
    datasets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DependencyLimitExceeded:
    resource: str
    limit: int


@dataclass(frozen=True, slots=True)
class DependencyOrder:
    datasets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DependencyClosure:
    datasets: tuple[str, ...]


type DependencyValidation = DependencyOrder | DependencyCycle | DependencyLimitExceeded
type DependencyClosureResult = DependencyClosure | DependencyCycle | DependencyLimitExceeded


class DatasetDependencyGraph:
    def __init__(
        self,
        dependencies: Mapping[str, Iterable[str]],
        *,
        limits: DependencyGraphLimits | None = None,
    ) -> None:
        self._limits = limits or DependencyGraphLimits()
        copied: dict[str, frozenset[str]] = {}
        for dataset, parents in dependencies.items():
            _dataset(dataset)
            copied[dataset] = frozenset(parents)
            for parent in copied[dataset]:
                _dataset(parent)
        for parent in {item for parents in copied.values() for item in parents}:
            copied.setdefault(parent, frozenset())
        self._dependencies = copied

    def validate(self) -> DependencyValidation:
        if len(self._dependencies) > self._limits.max_datasets:
            return DependencyLimitExceeded("datasets", self._limits.max_datasets)
        return self._order(frozenset(self._dependencies))

    def topological_order(self, datasets: Iterable[str] | None = None) -> DependencyValidation:
        selected = frozenset(self._dependencies if datasets is None else datasets)
        if len(selected) > self._limits.max_datasets:
            return DependencyLimitExceeded("datasets", self._limits.max_datasets)
        unknown = selected - self._dependencies.keys()
        if unknown:
            raise ValueError(f"unknown datasets: {sorted(unknown)!r}")
        return self._order(selected)

    def closure(
        self,
        datasets: Iterable[str],
        *,
        partitions: Iterable[str] = (),
    ) -> DependencyClosureResult:
        requested = frozenset(datasets)
        partition_set = frozenset(partitions)
        if len(partition_set) > self._limits.max_partitions:
            return DependencyLimitExceeded("partitions", self._limits.max_partitions)
        unknown = requested - self._dependencies.keys()
        if unknown:
            raise ValueError(f"unknown datasets: {sorted(unknown)!r}")
        selected = set(requested)
        pending = list(requested)
        while pending:
            current = pending.pop()
            for parent in self._dependencies[current]:
                if parent not in selected:
                    selected.add(parent)
                    pending.append(parent)
        if len(selected) > self._limits.max_datasets:
            return DependencyLimitExceeded("datasets", self._limits.max_datasets)
        ordered = self._order(frozenset(selected))
        if isinstance(ordered, DependencyOrder):
            return DependencyClosure(ordered.datasets)
        return ordered

    def _order(self, selected: frozenset[str]) -> DependencyOrder | DependencyCycle:
        pending = {name: set(self._dependencies[name] & selected) for name in selected}
        output: list[str] = []
        while pending:
            ready = sorted(name for name, parents in pending.items() if not parents)
            if not ready:
                return DependencyCycle(tuple(sorted(pending)))
            for name in ready:
                output.append(name)
                pending.pop(name)
            for parents in pending.values():
                parents.difference_update(ready)
        return DependencyOrder(tuple(output))


def _dataset(value: str) -> None:
    if not value or value != value.strip():
        raise ValueError("dataset names must be non-empty and trimmed")
