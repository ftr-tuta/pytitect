# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/) and releases use
Semantic Versioning with the prerelease rules documented in `docs/versioning.md`.

## Unreleased

### Added

- Dedicated finite and Django/PostgreSQL mutation-batch stores with explicit processing,
  partially committed, completed, and uncertain states.
- Public conformance harnesses for replay, inbox, outbox, checkpoints, receipts, idempotency,
  leases, generations, transaction boundaries, and mutation batches.
- Finite process-local reference stores for checkpoints, receipts, and generations.
- Explicit bounded retention plans for idempotency, replay, inbox, receipts, and delivered outbox
  rows, plus transactional terminal-failure archival.
- Closed, serializable `titect-sync/1` contracts for bootstrap, sessions, datasets, snapshot/delta,
  reset, generation mismatch, readiness, integrity, and mutation outcomes.
- A route-free interoperability bundle with JSON Schema 2020-12, OpenAPI 3.1 components,
  capability/header/limit registries, executable fixtures, and a deterministic SHA-256 manifest.
- Validated W3C Trace Context parsing/rendering, explicit `RequestContext` and DRF adapters, and a
  fixture-compatible pseudonymous observability vocabulary.
- A synthetic Django/PostgreSQL reference project that installs the exact candidate wheel and
  exercises atomic mutations, receipts, outbox dispatch, retention, crash recovery, and shared
  legacy/versioned routes.
- Executable documentation, generated compatibility and API-stability references, plus separate
  aggregate coverage gates for the whole package and its highest-risk contracts.

### Changed

- Mutation batches renew execution leases at item boundaries, atomically retain per-item progress,
  and can safely resume a proven committed prefix after a worker crash.
- `BatchItemsCommittedEnvelopeUnconfirmed` is recoverable by retry while item receipts remain
  retained. The coordinator now requires a `MutationBatchStore` and uses an explicit clock instead
  of accepting an execution timestamp.
- Inbox identity is now `(InboxScope(namespace, source, consumer), message_id)`. Django concrete
  models must use the matching four-column unique constraint; there is no unscoped fallback.
- Django callback adapters use complete structural protocols, validate inputs at their public
  boundary, and no longer accept the unused lease `decode_resource` callback.
- Empty terminal outbox failure reasons are rejected consistently by reference, callback, and
  PostgreSQL stores.
- Outbox delivery and terminal failure now record UTC transition timestamps and retain rows until
  an explicit purge or archive plan removes them, keeping duplicate message IDs blocked.
- Routine idempotency, mutation-batch, and receipt cleanup excludes uncertain outcomes unless
  explicitly opted in.
- Opaque cursor encoding rejects empty payloads so every emitted cursor is accepted by the strict
  decoder.

## 1.0.0rc1 - candidate not materialized

Candidate metadata:

- PEP 440 source version: `1.0.0rc1`
- Derivable protected tag: `v1.0.0-rc.1`
- `materialized: false`
- Latest public distribution: `v0.9.0a1`

### Added

- Explicit PostgreSQL Django stores for idempotency, replay protection, inbox, outbox,
  checkpoints, receipts, leases, and sync generations, with consumer-owned callbacks/models
  and mandatory database aliases.
- One-alias transactional domain/idempotency/receipt/outbox operations and direct Django fenced
  commits.
- Authenticated RFC 8785 cursor envelopes, bounded mutation batches, dataset dependency graphs,
  generation guards, and exclusive uncertain-receipt reconciliation.
- Failure-isolated observers, classified canary crash/timeout/skip outcomes, normative interop
  fixtures, and a generated Django/PostgreSQL protocol-matrix canary.

### Changed

- Problem Details validate complete documents and output bounds; DRF adaptation preserves all
  original response headers except the replaced content type.
- Local references reject malformed pointer indices/escapes and duplicate YAML keys, and preserve
  `$ref` siblings through explicit `allOf` composition.
- Replay adapters persist digests rather than clear tokens or proofs. Released lease rows remain
  present so fencing tokens are monotonic.

### Removed

- `CheckpointCoordinator` and `DjangoFencedCommitFactory`. They were unsafe prerelease APIs and
  are replaced without compatibility shims by `AtomicCheckpointCoordinator`,
  `DeferredCheckpointCoordinator`, and `DjangoFencedCommit`.

This candidate makes no interoperability claims beyond the normative fixtures committed here.

## 0.9.0a1 - 2026-09-02

### Added

- Dependency-free core, HTTP Problem Details, and local contract tooling.
- Idempotency, receipts, inbox/outbox, checkpoints, leases/fencing, and safe observations.
- Explicit adapters for Django 5.2, DRF 3.16, drf-spectacular, and security protocols.
- Packaging, verification tools, synthetic examples, and CI/release policy.
