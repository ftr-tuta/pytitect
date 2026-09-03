# Checkpoint commit boundaries

`AtomicCheckpointCoordinator` applies each item and advances its checkpoint inside the same explicit
consumer transaction. Use it when the state store and checkpoint store share one durable transaction
boundary. A stale compare-and-set rolls the transaction back and returns `StaleCheckpoint`.

`DeferredCheckpointCoordinator` commits application state first and advances the checkpoint only
from an `on_commit` callback. It can therefore return
`StateCommittedCheckpointUnconfirmed`: the state is durable, the older checkpoint remains, and a
retry must be safe. This is an explicit recoverable outcome, not an exactly-once claim.

Both coordinators process finite item sequences. The consumer supplies the transaction, store,
routing, retry policy, and any external effects.
