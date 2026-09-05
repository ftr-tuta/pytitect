# Event platform adoption

The 1.6 release-candidate event platform is opt-in. Existing 1.0 imports and synchronous contracts
remain unchanged. Start with the message contract and an in-memory conformance test, then introduce
one durability boundary and one transport at a time.

## Recommended sequence

1. Declare an immutable `MessageTypeRegistry` and separate `RoutingTable` in the application
   composition root.
2. Run canonical `titect-message/1` positive and negative fixtures through every implementation.
3. Implement an `AsyncUnitOfWork` that commits domain state, inbox completion, outbox append, and
   related local workflow state in one database transaction.
4. Exercise store harnesses and fault points before connecting a broker.
5. Select either JetStream or EventBridge-to-SQS and validate its capability declaration. Do not
   depend on broker deduplication or ordering when the declared capability is false.
6. Describe topology as application-owned data, review the plan, and apply it explicitly.
7. Add role-specific readiness and backlog policies, then enable the expanded infrastructure matrix.

<!-- executable -->
```python
from pytitect.messaging import MessageType, MessageTypeRegistry, Route, RoutingTable

types = MessageTypeRegistry([MessageType("example.changed.v1", "urn:example:schema:changed:1")])
routes = RoutingTable([Route("example.changed.v1", "logical-changes")])
assert types.resolve("example.changed.v1").version == 1
assert routes.destination_for("example.changed.v1") == "logical-changes"
```

Reference stores are finite and process-local. They are appropriate for tests and examples, not for
durability or process coordination. Applications provide concrete Django or SQLAlchemy models,
constraints, serializers, migrations, and transaction placement.

## Framework composition

FastAPI helpers adapt headers, Problem Details, OpenAPI components, and explicitly selected lifespan
resources. They do not create routes or middleware. Django helpers bridge a complete synchronous
transaction into async processing and settle the broker delivery after the block returns. The
FastStream adapter returns a handler for application-owned decorator registration.

## Operational rollout

Deploy API, relay, consumer, scheduler, and projection roles independently. An API readiness policy
normally requires its local database but not a live broker because commands commit to an outbox.
Relay and consumer roles normally require the broker. A consumer-selected finite backlog limit may
make any role unready. During shutdown, stop intake, wait for admitted work for a finite grace period,
and then cancel remaining tasks; cancellation must propagate.

See [independent reliability foundations](reliability-foundations.md) for current async adoption,
implemented store inventory, live conformance and reproducible Python capacity validation.
