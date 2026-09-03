# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/) and releases use
Semantic Versioning with the prerelease rules documented in `docs/versioning.md`.

## Unreleased

### Added

- Dedicated finite and Django/PostgreSQL mutation-batch stores with explicit processing,
  partially committed, completed, and uncertain states.

### Changed

- Mutation batches renew execution leases at item boundaries, atomically retain per-item progress,
  and can safely resume a proven committed prefix after a worker crash.
- `BatchItemsCommittedEnvelopeUnconfirmed` is recoverable by retry while item receipts remain
  retained. The coordinator now requires a `MutationBatchStore` and uses an explicit clock instead
  of accepting an execution timestamp.

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
