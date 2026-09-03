"""Typed transport capability declarations and negotiation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TransportCapabilities:
    ordered_delivery: bool
    broker_deduplication: bool
    topology_management: bool
    max_message_bytes: int
    profiles: frozenset[str] = frozenset({"standard"})

    def __post_init__(self) -> None:
        if self.max_message_bytes <= 0 or not self.profiles:
            raise ValueError("transport capabilities require finite size and profiles")


@dataclass(frozen=True, slots=True)
class CapabilityRequirements:
    ordered_delivery: bool = False
    broker_deduplication: bool = False
    topology_management: bool = False
    max_message_bytes: int = 1
    profile: str = "standard"

    def __post_init__(self) -> None:
        if self.max_message_bytes <= 0 or not self.profile:
            raise ValueError("capability requirements require finite size and profile")


@dataclass(frozen=True, slots=True)
class CapabilitiesAccepted:
    pass


@dataclass(frozen=True, slots=True)
class CapabilitiesRejected:
    missing: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.missing:
            raise ValueError("rejected capabilities require at least one reason")


type CapabilityDecision = CapabilitiesAccepted | CapabilitiesRejected


def negotiate_capabilities(
    offered: TransportCapabilities, required: CapabilityRequirements
) -> CapabilityDecision:
    missing: list[str] = []
    if required.ordered_delivery and not offered.ordered_delivery:
        missing.append("ordered_delivery")
    if required.broker_deduplication and not offered.broker_deduplication:
        missing.append("broker_deduplication")
    if required.topology_management and not offered.topology_management:
        missing.append("topology_management")
    if required.max_message_bytes > offered.max_message_bytes:
        missing.append("max_message_bytes")
    if required.profile not in offered.profiles:
        missing.append(f"profile:{required.profile}")
    if missing:
        return CapabilitiesRejected(tuple(missing))
    return CapabilitiesAccepted()
