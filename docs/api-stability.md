# Public API stability

Every entry in `tool/public-api.txt` is classified by these complete, ordered rules. The documentation
quality checker applies the same rules to the committed snapshot, so no public symbol is unclassified.

## Stable

Public symbols default to Stable. This includes the dependency-free core, HTTP values, idempotency,
inbox/outbox, checkpoints, receipts, leases, retention plans, observability, and trace context. Stable
APIs follow semantic-versioning compatibility after 1.0.

## Preview

All symbols exported by `pytitect.sync` are Preview in 1.0. The neutral `titect-sync/1` bundle is
versioned and closed, but bilateral interoperability with an external client has not yet been proven.
Preview APIs require an explicit changelog entry and migration guidance when changed.

## Testing

All public names ending in `Harness`, plus exports from `pytitect.canaries`, are Testing APIs. They
exist to verify consumer implementations and bounded health probes, not as production orchestration.
This rule takes precedence over module-level classifications.

## Low-level

Exports from `pytitect.contracts`, `pytitect.django`, `pytitect.drf`, and `pytitect.security` are
Low-level. They are supported, but consumers must explicitly supply framework configuration,
persistence, transactions, key resolution, routing, and policy. No automatic binding is implied.
