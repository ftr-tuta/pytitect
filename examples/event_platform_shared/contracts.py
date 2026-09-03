"""Synthetic contracts shared by the FastAPI and Django examples."""

from pytitect.application import Command, Decision, DomainEvent, HandlingContext


def decide(command: Command, context: HandlingContext) -> Decision:
    return Decision(
        result={"message_id": context.message_id, "accepted": True},
        domain_events=(DomainEvent("example.accepted", command.payload),),
    )
