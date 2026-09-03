# Event platform architecture

Pytitect's event platform is a set of explicit, consumer-owned building blocks. The contracts and
runtimes are Preview APIs during the 1.6 release-candidate series; framework and broker adapters are
Low-level APIs; conformance harnesses are Testing APIs. The Stable 1.0 API remains unchanged.

## Ownership and boundaries

Applications own composition, persistence models, schema migrations, transaction boundaries,
routing, authorization, broker topology, process lifecycle, and every external side effect.
Pytitect does not select adapters, create global registries, start workers, inspect framework
settings, or fall back from one protocol to another. Importing `pytitect` remains dependency-free
and side-effect free.

Reference stores are finite, process-local test implementations. They provide thread or task safety
where documented but do not coordinate processes and are not durable. Production persistence is
supplied through explicit Django models or SQLAlchemy model bundles owned by the application.

## Delivery contract

The platform implements at-least-once delivery. A consumer first reserves the scoped message ID,
performs local state changes, appends derived external work to the outbox, and completes the inbox
entry in one transaction. It acknowledges the broker only after that transaction commits.
Consequently:

- a crash before commit leaves no accepted delivery and the broker redelivers;
- a crash after commit but before acknowledgement causes a duplicate, which the inbox suppresses;
- a confirmed publication followed by a crash before the outbox row is marked delivered can publish
  twice, so the receiving inbox remains mandatory;
- broker unavailability does not make an API role unready unless a consumer-selected backlog policy
  says otherwise;
- no broker transport is described as exactly once.

NATS JetStream and AWS EventBridge-to-SQS preserve the same envelope, inbox, outbox, retry, and
terminal-failure semantics. NATS can offer broker-side message-ID deduplication, but it does not
replace the inbox. SQS Standard makes no ordering or broker-deduplication promise. Ordered profiles,
direct SQS publication, RabbitMQ, and Kafka are not implemented in this release candidate.

## Envelope profile

`titect-message/1` is a closed JSON profile over CloudEvents 1.0.2. Required fields identify the
message, source, event type, subject, UTC occurrence time, payload schema, content type, and data.
Optional correlation and causation IDs carry flow identity. Routing is a separate configuration
concern and never changes the event type. Encoders reject unknown fields, non-JSON values, excessive
depth or size, imprecise timestamps, and unsupported content types. Canonical bytes and fixture
digests allow independent implementations to prove interoperability.

## Failure state machines

Outbox rows move from `pending` to a leased `claimed` state. A successful broker acknowledgement
moves the authoritative claim to `delivered`. A retryable failure increments the attempt, clears the
claim, and schedules bounded backoff. A permanent failure or exhausted retry budget moves it to
`failed`; cleanup is always an explicit maintenance operation.

Inbox identities are scoped by namespace, source, consumer, and message ID. They move from absent to
an expiring reservation, then to completed. Only the current token may complete or abandon a
reservation. Completion is retained long enough to cover the source redelivery window.

Rejected deliveries are terminal only after durable quarantine succeeds. Quarantine records bounded
metadata, a SHA-256 digest, and a sanitized reason. Payload retention is disabled by default and must
be enabled by an explicit policy with a finite size limit. If quarantine fails, the delivery remains
retryable.

Process managers use optimistic state versions. Each handled input atomically updates state, records
the inbox entry, emits outbox messages, and schedules or cancels durable timers. Compensations are
ordinary explicit decisions rather than automatic rollback. Timer claims use leases and fencing.

Jobs progress through scheduled, claimed, succeeded, or terminal states. Expired leases may be
reclaimed; stale claim tokens cannot commit. Retry and next-run policies are finite and supplied at
composition. Fixed-interval scheduling is based on the intended schedule, avoiding unbounded drift.

Projections combine a named checkpoint and version guard with each local projection update. Rebuilds
are finite, resumable runs with explicit bounds. Event streams append at an expected version and
reject stale writers. Reads are paginated with finite limits; snapshots are optional hints and never
replace the authoritative stream.

## Async execution and cancellation

Async ports are separate from synchronous ports. Runtime queues and concurrency are bounded.
Timeouts are explicit and supervision uses structured concurrency. `asyncio.CancelledError` always
propagates after local cleanup; it is never reclassified as a retryable handler or broker failure.
Each concurrent SQLAlchemy task receives a distinct `AsyncSession` from a consumer-owned factory.

Shutdown stops intake, permits already admitted work to finish within a consumer-selected grace
period, releases uncommitted claims where possible, and cancels remaining tasks. A stopped runtime
does not own or close resources it did not create.

## Topology and operations

Topology is data. Adapters expose validate, plan, and apply operations; importing or constructing an
adapter changes nothing remotely. Plans identify additions and safe updates and reject destructive or
unsupported changes. Applying a plan is an explicit operator action.

Readiness is role-specific: API, relay, consumer, scheduler, and projection roles report only their
required dependencies and backlog policies. Observations use a bounded vocabulary, sanitized
attributes, optional trace propagation, and failure isolation. Message payloads, credentials,
authorization material, and unbounded exception text are not emitted by default.

## Atomicity rule

Local state, inbox completion, outbox append, saga state, job transitions, projection checkpoints,
event appends, snapshots, and related timers are atomic when they participate in one decision. No
external effect occurs inside that transaction. Every derived external effect is represented by an
outbox message and performed later by a relay.
