# Synchronization primitives

`pytitect.sync` contains finite building blocks, not a synchronization engine. It never creates a
scheduler, chooses a transport, opens a database, or binds a dataset automatically.

## Versioned wire contracts

`titect-sync/1` is the stable identifier of the neutral wire bundle. It is versioned independently
from the Python distribution. Every document carries that exact identifier and one closed `kind`;
`decode_sync_document()` rejects unknown fields at the envelope and every nested object. The
serializable dataclasses cover bootstrap and sessions, dataset capabilities, snapshot and delta
pages, upserts and tombstones, reset and generation mismatch decisions, readiness, page integrity,
and mutation outcomes. They are boundary values, not routing or execution behavior.

Wire timestamps are RFC 3339 UTC with exactly three fractional digits, for example
`2026-01-01T00:00:00.123Z`. Opaque identifiers are trimmed, control-character-free, and limited to
255 UTF-8 bytes by default. Dataset, capability, page, mutation, JSON-item, and document-byte limits
are finite and explicit through `SyncLimits`; consumers may select stricter decoding limits.

The normative bundle is in `interop/titect-sync/1`. It includes JSON Schema 2020-12, an OpenAPI 3.1
document containing reusable components and an empty `paths` object, header/capability/limit
registries, positive and negative fixtures, and a deterministic SHA-256 manifest. The bundle does
not select URLs, transport, authentication, authorization, or protocol fallback. An application
may bind legacy and new routes to the same service only through separate explicit boundary code.

## Operational primitives

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
