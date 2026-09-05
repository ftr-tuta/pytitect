"""Real clients, isolated resources, bounded waits, actual delivery settlement."""

import asyncio
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import boto3
import nats
import pytest
from botocore.config import Config
from nats.js.api import ConsumerConfig

from pytitect.aws import EventBridgePublisher, SqsDeliverySource
from pytitect.messaging import PublicationConfirmed
from pytitect.nats import NatsDelivery, NatsJetStreamPublisher
from tests.integration.support import message, required_environment


async def connect_nats():
    client = await nats.connect(
        required_environment("TEST_NATS_URL"),
        connect_timeout=3,
        allow_reconnect=False,
        max_reconnect_attempts=0,
    )
    await client.jetstream().account_info()  # Fails when JetStream was not enabled.
    return client


@pytest.mark.integration
@pytest.mark.nats
def test_jetstream_publish_receive_retry_ack_and_closed_connection():
    async def run():
        client = await connect_nats()
        js = client.jetstream()
        identity = "pytitect_" + uuid.uuid4().hex
        created = False
        try:
            await js.add_stream(
                name=identity, subjects=[identity], max_msgs=20, max_bytes=1024 * 1024
            )
            created = True
            subscription = await js.pull_subscribe(
                identity,
                durable="test",
                config=ConsumerConfig(ack_wait=0.3, max_deliver=5, max_ack_pending=2),
            )
            result = await NatsJetStreamPublisher(js).publish(
                destination=identity, message=message()
            )
            assert isinstance(result, PublicationConfirmed)
            first = NatsDelivery((await subscription.fetch(1, timeout=3))[0])
            await first.retry(delay=timedelta(milliseconds=50))
            second = NatsDelivery((await subscription.fetch(1, timeout=3))[0])
            assert second.message == first.message
            await second.ack()
            await client.flush(timeout=3)
            # A transport flush is not a JetStream settlement confirmation.
            deadline = time.monotonic() + 3
            while (await js.consumer_info(identity, "test")).num_ack_pending:
                assert time.monotonic() < deadline, "JetStream ACK was not settled"
                await asyncio.sleep(0.01)
            await js.delete_stream(identity)
            created = False
            await client.close()
            assert not isinstance(
                await NatsJetStreamPublisher(js).publish(destination=identity, message=message()),
                PublicationConfirmed,
            )
        finally:
            if created:
                await js.delete_stream(identity)
            await client.close()

    asyncio.run(run())


def aws_roundtrip(*, endpoint: str | None):
    region = os.environ.get("AWS_REGION", "us-east-1")
    config = Config(
        region_name=region, connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1}
    )
    credentials = (
        {} if endpoint is None else {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
    )
    events = boto3.client("events", endpoint_url=endpoint, config=config, **credentials)
    sqs = boto3.client("sqs", endpoint_url=endpoint, config=config, **credentials)
    identity = "pytitect-" + uuid.uuid4().hex
    queue_url = None
    bus_created = rule_created = target_created = False
    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            events.create_event_bus(Name=identity)
            bus_created = True
            queue_url = sqs.create_queue(QueueName=identity, Attributes={"VisibilityTimeout": "1"})[
                "QueueUrl"
            ]
            arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
                "Attributes"
            ]["QueueArn"]
            rule = events.put_rule(
                Name=identity,
                EventBusName=identity,
                EventPattern=json.dumps({"source": ["urn:example:reliability"]}),
            )["RuleArn"]
            rule_created = True
            sqs.set_queue_attributes(
                QueueUrl=queue_url,
                Attributes={
                    "Policy": json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {"Service": "events.amazonaws.com"},
                                    "Action": "sqs:SendMessage",
                                    "Resource": arn,
                                    "Condition": {"ArnEquals": {"aws:SourceArn": rule}},
                                }
                            ],
                        }
                    )
                },
            )
            assert (
                events.put_targets(
                    Rule=identity, EventBusName=identity, Targets=[{"Id": "queue", "Arn": arn}]
                )["FailedEntryCount"]
                == 0
            )
            target_created = True

            async def run():
                publisher = EventBridgePublisher(events, event_bus_name=identity, executor=executor)
                assert isinstance(
                    await publisher.publish(destination=identity, message=message()),
                    PublicationConfirmed,
                )
                source = SqsDeliverySource(
                    sqs, queue_url=queue_url, executor=executor, wait_time=timedelta(seconds=1)
                )
                deadline = time.monotonic() + 30
                deliveries = []
                while not deliveries and time.monotonic() < deadline:
                    deliveries = [item async for item in source.deliveries(batch_size=1)]
                assert len(deliveries) == 1, (
                    "EventBridge did not deliver to SQS before the deadline"
                )
                await deliveries[0].retry(delay=timedelta(0))
                redelivery = [item async for item in source.deliveries(batch_size=1)]
                assert len(redelivery) == 1 and redelivery[0].message == deliveries[0].message
                await redelivery[0].ack()
                assert not [item async for item in source.deliveries(batch_size=1)]

            asyncio.run(run())
        finally:
            if target_created:
                events.remove_targets(Rule=identity, EventBusName=identity, Ids=["queue"])
            if rule_created:
                events.delete_rule(Name=identity, EventBusName=identity)
            if queue_url:
                sqs.delete_queue(QueueUrl=queue_url)
            if bus_created:
                events.delete_event_bus(Name=identity)
            events.close()
            sqs.close()


@pytest.mark.integration
@pytest.mark.localstack
def test_localstack_eventbridge_to_sqs():
    aws_roundtrip(endpoint=required_environment("LOCALSTACK_ENDPOINT"))


@pytest.mark.integration
@pytest.mark.aws_live
def test_manual_real_aws_eventbridge_to_sqs():
    assert required_environment("PYTITECT_REAL_AWS") == "1"
    aws_roundtrip(endpoint=None)
