# Inbox, outbox, and checkpoints

Write a domain effect and its outbox envelope in one application transaction. A dispatcher claims a
finite batch for one round and records delivered, retryable, or permanent outcomes. Pytitect does not
run a scheduler and does not promise exactly-once delivery.

Use an inbox store to make handlers duplicate-tolerant. Every identity is the pair
`(InboxScope(namespace, source, consumer), message_id)`: a message identifier is isolated across
protocol namespaces, upstream sources, and logical consumers. Choose those scope values explicitly
and keep them stable for the lifetime of retained inbox records.

The in-memory inbox and outbox stores are finite, process-local references. They do not coordinate
workers in different processes and are not durable. Use a consumer-owned durable store when those
properties matter.

An atomic checkpoint coordinator applies state and advances the checkpoint inside one transaction.
The deferred coordinator applies state first and attempts advancement only after that transaction
commits. If deferred advancement fails, the older checkpoint remains and replay must be safe.
