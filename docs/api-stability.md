# Public API stability

Every entry in `tool/public-api.txt` is classified by these complete, ordered rules. The documentation
quality checker applies the same rules to the committed snapshot, so no public symbol is unclassified.

## Stable

Public symbols default to Stable. This includes the dependency-free core, HTTP values, idempotency,
inbox/outbox, checkpoints, receipts, leases, retention plans, observability, and trace context. Stable
APIs follow semantic-versioning compatibility after 1.0.

## Preview

Exports from `pytitect.wire`, `pytitect.sync`, `pytitect.messaging`, `pytitect.application`, `pytitect.aio`,
`pytitect.operations`, `pytitect.processes`, `pytitect.jobs`, `pytitect.projections`, and
`pytitect.event_sourcing` are Preview during the 1.6 release-candidate series. Versioned wire bundles
are closed independently from Python API maturity. Preview APIs require an explicit changelog entry
and migration guidance when changed.

## Testing

All public names ending in `Harness`, plus exports from `pytitect.canaries` and `pytitect.testing`,
are Testing APIs. They exist to verify consumer implementations, fault behavior, and bounded health
probes, not as production orchestration. This rule takes precedence over module classifications.

## Low-level

Exports from `pytitect.contracts`, `pytitect.django`, `pytitect.drf`, `pytitect.security`,
`pytitect.sqlalchemy`, `pytitect.nats`, `pytitect.aws`, `pytitect.fastapi`, and
`pytitect.faststream_nats` are Low-level. They are supported, but consumers must explicitly supply
framework configuration, persistence, transactions, key resolution, routing, topology, lifecycle,
and policy. No automatic binding is implied.
