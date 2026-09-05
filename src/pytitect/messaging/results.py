"""Transport-neutral typed publication and delivery results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class PublicationConfirmed:
    transport_id: str

    def __post_init__(self) -> None:
        if not self.transport_id:
            raise ValueError("confirmed publication requires a transport ID")


@dataclass(frozen=True, slots=True)
class PublicationRetryable:
    reason: str
    retry_after: datetime | None = None

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("retryable publication requires a reason")


@dataclass(frozen=True, slots=True)
class PublicationRejected:
    reason: str

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("rejected publication requires a reason")


@dataclass(frozen=True, slots=True)
class PublicationUncertain:
    reason: str


type PublicationResult = (
    PublicationConfirmed | PublicationRetryable | PublicationRejected | PublicationUncertain
)


@dataclass(frozen=True, slots=True)
class DeliveryAck:
    pass


@dataclass(frozen=True, slots=True)
class DeliveryRetry:
    delay: timedelta | None = None


@dataclass(frozen=True, slots=True)
class DeliveryTerminated:
    quarantine_id: str

    def __post_init__(self) -> None:
        if not self.quarantine_id:
            raise ValueError("terminated delivery requires a durable quarantine ID")


type DeliveryDisposition = DeliveryAck | DeliveryRetry | DeliveryTerminated
