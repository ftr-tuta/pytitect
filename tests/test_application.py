from datetime import UTC, datetime

import pytest

from pytitect.application import (
    Command,
    CommandBinding,
    CommandRegistry,
    Decision,
    DomainEvent,
    HandlingContext,
    IntegrationEvent,
    Query,
    QueryBinding,
    QueryRegistry,
    Task,
)
from pytitect.messaging import Message


def test_command_decision_keeps_effect_kinds_distinct() -> None:
    external = Message(
        id="event-1",
        source="urn:example:test",
        type="example.changed.v1",
        subject="example/1",
        time=datetime(2026, 9, 3, tzinfo=UTC),
        dataschema="urn:example:changed:1",
        data={"version": 1},
    )

    def handle(command: Command, context: HandlingContext) -> Decision:
        assert context.message_id == "command-1"
        return Decision(
            result={"accepted": command.payload},
            domain_events=(DomainEvent("changed", command.payload),),
            integration_events=(IntegrationEvent(external),),
            commands=(Command("follow-up", None),),
            tasks=(Task("recalculate", {"bounded": True}),),
        )

    registry = CommandRegistry([CommandBinding("change", handle)])
    decision = registry.dispatch(Command("change", {"value": 2}), HandlingContext("command-1"))
    assert decision.result == {"accepted": {"value": 2}}
    assert decision.domain_events[0].name == "changed"
    assert decision.integration_events[0].message is external
    assert decision.commands[0].name == "follow-up"
    assert decision.tasks[0].name == "recalculate"


def test_query_registry_validates_result_and_is_closed() -> None:
    registry = QueryRegistry(
        [QueryBinding("lookup", lambda query, context: {"request": context.message_id})]
    )
    assert registry.dispatch(Query("lookup", None), HandlingContext("query-1")) == {
        "request": "query-1"
    }
    with pytest.raises(LookupError):
        registry.dispatch(Query("missing", None), HandlingContext("query-2"))


def test_duplicate_bindings_are_rejected() -> None:
    binding = CommandBinding("same", lambda command, context: Decision())
    with pytest.raises(ValueError, match="duplicate command"):
        CommandRegistry([binding, binding])
