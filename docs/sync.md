# Synchronization primitives

`pytitect.sync` contains finite building blocks, not a synchronization engine. It never creates a
scheduler, chooses a transport, opens a database, or binds a dataset automatically.

`OpaqueCursorCodec` emits a three-part `protected.body.auth` v1 envelope. The protected header is
RFC 8785 canonical JSON and binds the algorithm, key identifier, dataset, partition, and optional
UTC expiration. HS256 authenticates the encoded header and body and requires at least 32 key bytes.
A256GCM uses `cryptography`'s `AESGCM`, a 96-bit nonce, a 256-bit key, and the protected header as
additional authenticated data. Unknown keys, algorithms, versions, context changes, expiry,
tampering, noncanonical headers, and size excesses are rejected. Install `pytitect[sync]` before
calling cursor crypto methods.

`MutationBatchCoordinator` enforces unique item IDs, aggregate byte/item limits, stable receipt
order, batch leases, and per-item idempotency. It requires a `MutationBatchStore`, an item
`IdempotencyStore`, one caller-selected transaction boundary and alias, and an optional explicit
clock. `ALL_OR_NOTHING` reserves every item before the first mutation and commits the batch in one
transaction. `PER_ITEM` commits each mutation, its retained item receipt, and the corresponding
batch progress in one transaction.

Batch state is explicit: `processing`, `partially_committed`, `completed`, or `uncertain`. Execution
leases are renewed at item boundaries and before final completion. After an expired lease, one
worker may resume the stored prefix; it must prove every retained item receipt before continuing.
If proof is no longer possible, the batch becomes uncertain instead of reusing its identity.
Expiration of terminal retention removes the record on the next begin operation; expiration is not
a reusable batch state.

`BatchItemsCommittedEnvelopeUnconfirmed` means all item transactions committed but the final batch
transition was not confirmed. Retrying after the execution lease expires can prove those receipts
and complete the batch without repeating their mutations. Callers supply an `IdempotencyPolicy`, so
execution leases, completed-result retention, and uncertainty retention have independent lifetimes.

`InMemoryMutationBatchStore` is a finite, thread-safe reference implementation. Its state is
process-local and disappears on process exit; it is not a durable or multi-process store. Django
consumers can subclass `AbstractMutationBatchModel`, add a unique constraint on
`(namespace, batch_id)`, and construct `DjangoMutationBatchStore.from_model(...)`. Existing callers
that used an idempotency store as the batch envelope must provision this dedicated state and update
the coordinator constructor; there is intentionally no compatibility fallback.

`DatasetDependencyGraph` provides finite cycle validation, dependency closure, and stable
topological order. `GenerationGuard` compares a locked generation and performs the protected
mutation in the same transaction.

Use RFC 8785/I-JSON for wire cursors, interoperable fingerprints, and `interop/` fixtures.
`canonical_json_bytes()` remains a generic dependency-free stable encoder and does not claim RFC
8785 conformance.
