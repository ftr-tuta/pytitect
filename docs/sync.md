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
order, envelope idempotency, and per-item idempotency. `ALL_OR_NOTHING` reserves every item before
the first mutation in one caller-selected transaction. `PER_ITEM` commits each item independently,
replays completed items on resume, and reports `BatchItemsCommittedEnvelopeUnconfirmed` when only
the final envelope CAS is unconfirmed.

`DatasetDependencyGraph` provides finite cycle validation, dependency closure, and stable
topological order. `GenerationGuard` compares a locked generation and performs the protected
mutation in the same transaction.

Use RFC 8785/I-JSON for wire cursors, interoperable fingerprints, and `interop/` fixtures.
`canonical_json_bytes()` remains a generic dependency-free stable encoder and does not claim RFC
8785 conformance.
