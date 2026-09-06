# Independent reliability foundations

These changes are Preview runtime APIs and optional Low-level adapters. Stable synchronous ports,
root imports and both `/1` wire bundles keep their existing signatures and dependency boundaries.
The original architecture audit is retained at
[commit 966fa95](https://github.com/ftr-tuta/pytitect/blob/966fa9541352ef8d96e23c537ba8255a7fad2cd3/docs/large-scale-architecture-roadmap.adoc).

## Runtime adoption

`AsyncOutboxStore.retry` now requires `at` independently of `available_at`. Settlement returns
`SettlementResult.APPLIED`, `STALE` or `DEFERRED`; only `STALE` is false when converted to `bool`.
`defer` releases current authority without consuming an attempt. `uncertain` holds the identity
until `resolve_uncertain` compares the recorded uncertainty timestamp and applies the consumer's
reconciliation decision. Uncertain publication never enters automatic retry or terminal retention.
A process killed before it records its publication result still leaves an expiring claim; that
at-least-once crash window requires a receiving inbox. Cancellation does not settle that claim.

Each transition samples UTC again. Elapsed operation and admission budgets use monotonic time;
a backwards wall-clock jump does not extend the runtime's original execution authority. SQL
settlement verifies identity, fencing/claim identity and expiration inside the transaction. Runtime
checks cannot replace storage checks. No operation renews authority implicitly.

`AsyncRelay` admits at most `max_admitted` messages and `max_retained_bytes` encoded payload bytes
per round, including work waiting for a fixed set of `concurrency` workers. `limit` cannot enlarge
these configured bounds. Simultaneous `run_once` calls on one instance return `RelaySummary.busy`.
Distinct instances may claim and settle concurrently. Messages larger than the byte budget remain
pending: the consumer must select compatible transport/message limits and monitor backlog age.
The byte limit measures payload representation, not Python heap overhead; task/count limits also
bound per-message overhead. Reference stores retain up to their finite configured capacity.

`AsyncConsumer.process` returns `DeliveryAck`, `DeliveryRetry` or `DeliveryTerminated`. Unexpected
exceptions propagate to the caller and `OperationalSupervisor`; timeout during commit remains
visible as uncertainty. Only `RetryableProcessingError` and a timeout proven before commit request
retry. `PermanentProcessingError` requires durable quarantine before termination. Failed quarantine
leaves delivery recoverable. Direct processing and a running intake loop do not overlap on one
instance; `RuntimeBusyError` leaves admission and settlement with the caller. The intake loop
reserves bounded capacity before pulling the next delivery. Application/broker prefetch must have
its own compatible byte and count limits.

`RetryPolicy` preserves its defaults and saturates before exponentiation can overflow. Invalid
attempt counts and non-finite multipliers are rejected. `RetryComposition` combines that policy,
an injected jitter fraction, a monotonic `Deadline`, and an explicitly shared `RetryBudget`.
Server `retry_after` is a minimum even when it exceeds the local backoff cap. Insufficient deadline
or aggregate allowance yields explicit deferral. Budgets are finite, instance-owned, process-local,
and never automatically replenished. Sharing a budget does not establish a distributed quota.

Optional runtime observation emits only fixed transition facts with finite `role` and `outcome`
attributes. No payload, identity, credential, dynamic exception text or arbitrary label is emitted.
An optional `metrics` sink receives `runtime.message_age_seconds` with only the finite `role`
attribute. Message age uses persisted UTC timestamps, clamps negative clock skew to zero and
does not participate in authority or deadline decisions.
Sinks must be synchronous and nonblocking, and own their retention; sink exceptions cannot alter
commits, dispositions or returned outcomes. `BacklogLimits` evaluates consumer-selected pending,
age and byte thresholds. Compose this result with the role's selected `ReadinessPolicy` probes.

## SQLAlchemy and Django transactions

Use `SQLAlchemyRelayStore(session_factory, model, serializer)` for a concurrent relay. Every call
opens a fresh session and short transaction, then closes it before publication. Session-bound
`SQLAlchemyOutboxStore` remains suitable for adding messages in a local transaction. Never pass one
session-bound store to concurrent relay workers. Factories and engines are borrowed; callers own
pool sizes, database timeouts, connection disposal and worker lifecycle.

`SQLAlchemyIdempotentRequest` explicitly receives the session factory, idempotency and receipt
models, serializer and `IdempotencyPolicy`. Its mutation callback receives the same session used
for reservation, receipt and outbox. The callback must not commit or perform external effects.
Consumers choose scope, fingerprint, receipt identity, payload/result retention and HTTP mapping.
A confirmed result is `RequestCommitted`; a repeated identity returns `Replay`. Scope and
fingerprint conflicts remain separate. See the
[FastAPI composition](../examples/fastapi_event_platform/composition.py).

A mutation exception before commit rolls back and propagates. A commit exception triggers a query
through a new session after closing the failed session. Confirmed data can replay; absent evidence
or an unavailable reconciliation query returns `Uncertain`, never a claim of confirmed rollback.
The coordinator does not retry an uncertain operation. Retained uncertainty is not expired into a
new execution. Result retention is an explicit identity-reuse boundary selected by the consumer.

The Django async bridge now takes `DjangoRelayStore` with a consumer subclass of
`AbstractRelayOutboxModel`, which adds nullable uncertainty fields to the Stable abstract outbox
shape. Each operation runs through the selected `DjangoTransactionRunner` and `DjangoAsyncBridge`.
Consumers own the required schema change; the package supplies no migration. The synchronous
`DjangoOutboxStore` contract remains unchanged. Django's transactional operation samples time again
when completing idempotency. Live tests exercise the published adapters, independent Django
connections and the async transaction bridge.

## Implemented persistence inventory

| Boundary | Optional SQLAlchemy implementation | Required consumer identity |
| --- | --- | --- |
| Inbox | `SQLAlchemyInboxStore` | namespace, source, consumer, message ID |
| Outbox | `SQLAlchemyOutboxStore`, `SQLAlchemyRelayStore` | unique message ID |
| Checkpoint | `SQLAlchemyCheckpointStore` | unique stream |
| Quarantine | `SQLAlchemyRejectedDeliveryStore` | unique quarantine ID |
| Idempotency | `SQLAlchemyIdempotencyStore` | namespace, subject, operation, key; unique token |
| Receipts | `SQLAlchemyReceiptStore` | unique receipt ID |
| Processes and timers | `SQLAlchemyProcessStore` | process name/instance; process name/instance/timer ID |
| Jobs | `SQLAlchemyJobStore` | unique job ID |
| Events and snapshots | `SQLAlchemyEventStore` | log identity plus identities described below |
| Projection/rebuild | `SQLAlchemyProjectionStore` | projection name/partition; unique rebuild run ID |
| Confirmed retention | `SQLAlchemyRetention` | concrete model primary key |

`ModelBundle` slots for leases, generations, mutation batches and schedules do not imply a
SQLAlchemy implementation. Django has its separately documented Stable bindings. Async reference
stores are finite process-local conformance fixtures, not durable implementations.

Process state, inbox completion, effects and timer decisions can share `apply_message`'s transaction.
Effect callbacks must write local outbox work in that session. Job success callbacks follow the
same rule. Stale versions/fences cannot commit their staged effects. Terminal job and timer rows
remain as identity/authority tombstones. Consumer archival or migrations must preserve that history.
Retention only purges confirmed terminal records in bounded pages; uncertain/pending work and
lease/job/timer authority are not automatically removed. Interrupted maintenance rolls back.

Event models require uniqueness on `(log_id, event_id)`, `(log_id, global_position)` and
`(log_id, category, stream_id, stream_version)`. Snapshot identity is
`(log_id, category, stream_id)`. Each explicitly selected log has one `EventLogModelMixin` position
row locked until commit. This orders commits within that log and intentionally serializes its
append writers; partition by configuring separate logs. A standalone SQL sequence cannot provide
this guarantee. All writers must honor the adapter's lock. Snapshots cannot exceed stream coverage
or regress their version. The package defines no global ordering across independent logs.

Projection state/checkpoint writes cover the next authoritative event page. Rebuilds start at zero,
retain a fixed committed watermark, process finite pages and persist progress. Cutover refuses to
replace a projection whose version or checkpoint has advanced beyond the rebuilt state. Consumers
coordinate live projection writes and cutover; a refused cutover requires a newer rebuild or an
explicitly coordinated pause. Reducers, compensations, schedules and all concrete models remain
application-owned.

## Reproducing validation

Install development dependencies and explicitly provision only the isolated services used by a run:

```console
uv sync --all-extras --frozen
uv run pytest --no-cov
uv run python tool/integration_environment.py -- uv run python tool/verify.py
uv run python tool/integration_environment.py --services postgres,nats -- uv run python -m benchmarks.python.run --output /tmp/capacity.json
asciidoctor --failure-level WARN -o /tmp/roadmap.html docs/large-scale-architecture-roadmap.adoc
```

`pytest` defaults to fast tests. `tool/verify.py` selects the full live suite, excluding manual AWS.
Selected integrations fail if their endpoints are absent or unhealthy. Set `TEST_POSTGRES_DSN`,
`TEST_NATS_URL` and `LOCALSTACK_ENDPOINT` to use already provisioned test services. The provisioning
tool enables JetStream explicitly, uses finite health deadlines and removes only its generated
containers. Individual test schemas, streams, queues and buses have unique identities and cleanup.

The required CI integration job enforces unchanged 85% global, 90% core and both 95% critical
coverage floors, including new async/durable paths. PostgreSQL adapter changes also run versions
15–18. The real AWS workflow remains manual, requires credentials and reports service failures.
LocalStack evidence is emulator evidence. No Dartitect change or paired protocol claim is included.

The Python benchmark uses real loopback HTTP, PostgreSQL and JetStream. Offered-load, saturation,
process-death recovery and soak runs record seeds, package/platform versions, all response/error
categories, useful throughput, tail latency, backlog age, retries, memory, tasks, connection and
lock-wait observations. Correctness and resource bounds are initial gates. Latency thresholds
require reproducible reviewed baselines; the fixture does not claim production capacity. Weekly or
manual soak jobs supplement the required representative run. Issue #43 remains partial pending its
paired-client acceptance work; #34 and #40 remain open.
