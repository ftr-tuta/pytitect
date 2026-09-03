import asyncio
import json
from concurrent.futures import Executor, Future
from datetime import UTC, datetime, timedelta

from pytitect.aws import EventBridgePublisher, SqsDelivery
from pytitect.messaging import JsonMessageCodec, Message, PublicationConfirmed, PublicationRetryable


class ImmediateExecutor(Executor):
    def submit(self, fn: object, /, *args: object, **kwargs: object) -> Future[object]:
        future: Future[object] = Future()
        try:
            future.set_result(fn(*args, **kwargs))  # type: ignore[operator]
        except BaseException as exc:
            future.set_exception(exc)
        return future


def event() -> Message:
    return Message(
        id="message-1",
        source="urn:example:test",
        type="example.changed.v1",
        subject="example/1",
        time=datetime(2026, 9, 3, tzinfo=UTC),
        dataschema="urn:example:changed:1",
        data={"ok": True},
    )


class EventBridge:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.entries: list[dict[str, object]] = []

    def put_events(self, *, Entries: list[dict[str, object]]) -> dict[str, object]:
        self.entries = Entries
        return self.response


def test_eventbridge_preserves_complete_envelope_in_detail() -> None:
    client = EventBridge({"FailedEntryCount": 0, "Entries": [{"EventId": "aws-1"}]})
    result = asyncio.run(
        EventBridgePublisher(
            client,
            event_bus_name="custom-bus",
            executor=ImmediateExecutor(),
            max_concurrency=1,
        ).publish(destination="custom-bus", message=event())
    )
    assert isinstance(result, PublicationConfirmed)
    assert JsonMessageCodec().decode(client.entries[0]["Detail"].encode()) == event()  # type: ignore[union-attr]


def test_eventbridge_partial_failure_is_typed() -> None:
    client = EventBridge(
        {
            "FailedEntryCount": 1,
            "Entries": [{"ErrorCode": "ThrottlingException", "ErrorMessage": "later"}],
        }
    )
    result = asyncio.run(
        EventBridgePublisher(client, event_bus_name="bus", executor=ImmediateExecutor()).publish(
            destination="bus", message=event()
        )
    )
    assert isinstance(result, PublicationRetryable)


class Sqs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None]] = []

    def delete_message(self, **kwargs: object) -> None:
        self.calls.append(("delete", None))

    def change_message_visibility(self, **kwargs: object) -> None:
        self.calls.append(("visibility", int(kwargs["VisibilityTimeout"])))


def test_sqs_delivery_maps_visibility_and_delete_ack() -> None:
    client = Sqs()
    body = json.dumps({"detail": json.loads(JsonMessageCodec().encode(event()))})

    async def exercise(executor: Executor) -> None:
        delivery = SqsDelivery(
            client,
            queue_url="https://synthetic.invalid/queue",
            raw_message={"Body": body, "ReceiptHandle": "receipt"},
            executor=executor,
            semaphore=asyncio.Semaphore(1),
        )
        await delivery.retry(delay=timedelta(seconds=7))
        await delivery.ack()

    asyncio.run(exercise(ImmediateExecutor()))
    assert client.calls == [("visibility", 7), ("delete", None)]
