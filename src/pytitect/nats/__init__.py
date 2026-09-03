"""Low-level explicit NATS JetStream adapter."""

from pytitect.nats.topology import (
    ConsumerSpec,
    NatsTopology,
    NatsTopologyBackend,
    NatsTopologyPlan,
    StreamSpec,
    TopologyAction,
    TopologyOperation,
    apply_nats_topology,
    plan_nats_topology,
)
from pytitect.nats.transport import (
    NATS_CAPABILITIES,
    NatsDelivery,
    NatsJetStreamPublisher,
    NatsPullDeliverySource,
    classify_nats_publication_error,
)

__all__ = [
    "NATS_CAPABILITIES",
    "ConsumerSpec",
    "NatsDelivery",
    "NatsJetStreamPublisher",
    "NatsPullDeliverySource",
    "NatsTopology",
    "NatsTopologyBackend",
    "NatsTopologyPlan",
    "StreamSpec",
    "TopologyAction",
    "TopologyOperation",
    "apply_nats_topology",
    "classify_nats_publication_error",
    "plan_nats_topology",
]
