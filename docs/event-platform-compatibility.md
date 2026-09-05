# Event platform compatibility

| Surface | Classification | Supported line or profile |
| --- | --- | --- |
| Messaging, async runtimes, sagas, jobs, projections, event sourcing | Preview | `titect-message/1`; explicit `titect-message/2` |
| Django | Low-level | `>=5.2.1,<6.2`; tested on 5.2, 6.0, and 6.1 |
| FastAPI | Low-level | `>=0.141,<0.142`; Starlette is not pinned directly |
| SQLAlchemy async | Low-level | `>=2.0.52,<2.1`; PostgreSQL 15–18 |
| psycopg | Low-level | `>=3.3.5,<4` |
| nats-py | Low-level | `>=2.15,<3`; NATS Server 2.14.5 canaries |
| boto3 | Low-level | `>=1.43.88,<2`; LocalStack 4.14.0 canaries |
| FastStream NATS | Low-level | `>=0.7.5,<0.8` |
| Store harnesses and fault injection | Testing | finite, instance-local test tools |

NATS and AWS have semantic parity for the `/1` envelope, inbox/outbox, retries, terminal quarantine, and
ACK-after-commit rules. Their broker capabilities differ: JetStream supports message-ID
deduplication; SQS Standard promises neither ordering nor broker deduplication. Ordered delivery and
direct SQS publication are rejected capabilities in this release candidate. RabbitMQ and Kafka are
out of scope.

The package contains only abstract adapter shapes. Applications own concrete table names,
constraints, serializers, migrations, URLs, processes, and authorization. SQLAlchemy requires one
consumer-created `AsyncSession` per concurrent task.

Exact `/2` selects `ExactJsonMessageCodec` throughout byte storage, NATS delivery and consumer
admission. EventBridge/SQS JSON transformation does not have exact-token evidence and explicitly
rejects this selection. The default `/1` codec and legacy sync mapping API remain available.
[Raw boundaries and compatibility results](exact-wire.md) specify numeric conversion and optional
page-integrity negotiation. Cross-language release acceptance still requires integrated paired evidence.
