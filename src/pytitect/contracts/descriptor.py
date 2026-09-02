"""Protocol version and capability negotiation."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ProtocolDescriptor:
    name: str
    version: str
    capabilities: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.name or not self.version:
            raise ValueError("protocol name and version must not be empty")
        if any(not capability for capability in self.capabilities):
            raise ValueError("capabilities must not be empty")


@dataclass(frozen=True, slots=True)
class ContractAccepted:
    descriptor: ProtocolDescriptor


@dataclass(frozen=True, slots=True)
class MissingVersion:
    expected: str


@dataclass(frozen=True, slots=True)
class VersionMismatch:
    expected: str
    received: str


@dataclass(frozen=True, slots=True)
class MissingCapabilities:
    missing: frozenset[str]


type ContractDecision = ContractAccepted | MissingVersion | VersionMismatch | MissingCapabilities


@dataclass(frozen=True, slots=True)
class ExactVersionPolicy:
    descriptor: ProtocolDescriptor

    def decide(
        self,
        received_version: str | None,
        *,
        required_capabilities: frozenset[str] = frozenset(),
    ) -> ContractDecision:
        if received_version is None:
            return MissingVersion(self.descriptor.version)
        if received_version != self.descriptor.version:
            return VersionMismatch(self.descriptor.version, received_version)
        missing = required_capabilities - self.descriptor.capabilities
        if missing:
            return MissingCapabilities(missing)
        return ContractAccepted(self.descriptor)
