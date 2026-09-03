# Migrating from 0.9.0a1 to 1.0

Pytitect 1.0 removes unsafe prerelease contracts without compatibility fallbacks. Apply these
changes before installing the 1.0 release candidate.

## Idempotency leases and retention

Replace the former single TTL with `IdempotencyPolicy.execution_lease_ttl`,
`result_retention_ttl`, and `uncertainty_retention_ttl`. Implement `renew()` and `abandon()` on
custom stores. The transition methods now return typed results rather than booleans, and completed
or uncertain retention begins at the transition timestamp.

## Resumable mutation batches

Construct `MutationBatchCoordinator` with a `MutationBatchStore` and an explicit clock. Persist
processing, partially committed, completed, and uncertain records. Commit each mutation item, its
idempotency receipt, and the batch advancement using one transaction and database alias.

## Scoped inbox identities

Create an `InboxScope(namespace, source, consumer)` for every inbox operation and pass it to
`begin()`, `complete()`, and `abandon()`. `InboxEnvelope` now contains `scope` instead of a standalone
`source` field.

For Django models:

1. Add non-null `namespace`, `source`, and `consumer` columns.
2. Backfill stable values that identify the protocol boundary, producer, and logical handler.
3. Replace the old `message_id` unique constraint with a unique constraint on
   `(namespace, source, consumer, message_id)`.
4. Deploy the model and adapter call-site changes together.

The package supplies an abstract model but no migrations. Consumers own schema rollout and any
compatibility window.

## Renamed low-level coordination APIs

Replace `CheckpointCoordinator` with `AtomicCheckpointCoordinator` or
`DeferredCheckpointCoordinator`, according to the required commit boundary. Replace
`DjangoFencedCommitFactory` with `DjangoFencedCommit`. `DjangoLeaseStore.from_model()` no longer
accepts the unused `decode_resource` argument.

## Outbox terminal retention

Pass an explicit UTC `at` instant to `OutboxStore.delivered()` and `OutboxStore.failed()`. Delivered
and terminally failed messages now remain in the outbox and continue to block duplicate message IDs.
Add nullable `delivered_at` and `failed_at` fields to concrete models, then use
`PurgeDeliveredOutboxPlan` or `ArchiveFailedOutboxPlan` for bounded cleanup.

Backfill `failed_at` for retained pre-1.0 failures only from an authoritative consumer timestamp.
Rows with a null `failed_at` remain excluded from archival until the consumer resolves that value.

Archive callbacks must write durable rows using the database alias supplied by
`DjangoRetentionMaintenance`. Do not perform external effects in that transaction.

## Opaque cursor payloads

Cursor payloads must contain at least one byte. Replace any prerelease call that encoded an empty
payload with an explicit, bounded protocol value before upgrading; the encoder now rejects empty
payloads instead of emitting a cursor that its strict decoder cannot accept.

Use the public store harnesses in tests for each custom store or callback adapter. Model-backed
Django stores must also be exercised on PostgreSQL; SQLite cannot validate the locking contract.
