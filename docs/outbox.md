# Inbox, outbox, and checkpoints

Write a domain effect and its outbox envelope in one application transaction. A dispatcher claims a
finite batch for one round and records delivered, retryable, or permanent outcomes. Pytitect does not
run a scheduler and does not promise exactly-once delivery.

Use an inbox store to make handlers duplicate-tolerant. A checkpoint coordinator applies durable
state inside a transaction and only schedules checkpoint advancement through `on_commit`. If that
callback fails after the state commit, the older checkpoint remains and replay must be safe.
