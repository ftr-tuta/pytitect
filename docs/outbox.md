# Inbox, outbox, and checkpoints

Write a domain effect and its outbox envelope in one application transaction. A dispatcher claims a
finite batch for one round and records delivered, retryable, or permanent outcomes. Delivery and
terminal failure are retained lifecycle transitions: `delivered(claim, at=utc_instant)` records
`delivered_at`, while `failed(claim, reason=..., at=utc_instant)` records `failure_reason` and
`failed_at`. Both keep the message identity reserved until explicit maintenance removes it. Pytitect
does not run a scheduler and does not promise exactly-once delivery.

Use an inbox store to make handlers duplicate-tolerant. Every identity is the pair
`(InboxScope(namespace, source, consumer), message_id)`: a message identifier is isolated across
protocol namespaces, upstream sources, and logical consumers. Choose those scope values explicitly
and keep them stable for the lifetime of retained inbox records.

The in-memory inbox and outbox stores are finite, process-local references. They do not coordinate
workers in different processes and are not durable. Use a consumer-owned durable store when those
properties matter.

Use `PurgeDeliveredOutboxPlan` to delete delivered rows in a finite batch. Use
`ArchiveFailedOutboxPlan` to pass terminal failures to consumer-owned durable archival storage before
deleting them. Every plan has a UTC cutoff, a positive finite batch size, dry-run support, and a
`MaintenanceSummary(selected, affected, dry_run)` result. Dry runs never invoke archive callbacks.

An atomic checkpoint coordinator applies state and advances the checkpoint inside one transaction.
The deferred coordinator applies state first and attempts advancement only after that transaction
commits. If deferred advancement fails, the older checkpoint remains and replay must be safe.
