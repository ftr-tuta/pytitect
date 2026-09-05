# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/) and releases use
Semantic Versioning with the prerelease rules documented in `docs/versioning.md`.

## Unreleased

### Added

- Preview immutable exact JSON tokens and raw/streamed sync/message boundaries with typed,
  payload-free failures; explicit `titect-message/2` codec and normative bundle.
- Optional `integrity-sha-256-exact-json-v1` page verification with explicit policy injection,
  bootstrap acknowledgement and no downgrade; an authoritative 232-case raw conformance corpus.

- Preview async idempotency/receipt and workflow ports, finite reference adapters and conformance
  harnesses; optional SQLAlchemy request, process/timer, job, event/snapshot, projection/rebuild and
  terminal-retention adapters with explicit consumer sessions, models and serialization.
- Session-factory relay transactions, fixed workers, count/byte admission limits, monotonic budgets,
  injected jitter, aggregate retry allowances, typed settlement/uncertainty and optional finite
  operational facts with consumer-selected backlog readiness limits.
- Live PostgreSQL/Django, JetStream and LocalStack conformance; deterministic subprocess crash
  barriers; a Python HTTP capacity fixture and required representative, wider PostgreSQL and soak CI.

### Changed

- Preview messaging, relay, consumer admission and NATS accept explicitly injected codecs for
  exact messages. SQLAlchemy byte serializers preserve the selected representation; EventBridge/SQS
  reject unsupported exact selection. See [exact wire adoption](docs/exact-wire.md).

- Preview consumers return typed delivery dispositions and propagate unexpected/uncertain failures.
  Async settlement checks current authority and takes an explicit transition timestamp. Uncertain
  publications require explicit reconciliation. See [adoption guidance](docs/reliability-foundations.md).
- Preview timers retain terminal identities, and projection rebuild cutover rejects regression.
  SQL event positions are serialized transactionally within a consumer-selected log.
- The Django async outbox bridge uses the explicit bounded relay binding and Preview uncertainty
  columns; consumers own model adoption. Stable synchronous port signatures and `/1` remain intact.

### Fixed

- Reject normalized invalid sync/message timestamps, oversized raw documents and duplicate-key
  allocation attacks. Preserve arbitrary wire-budget integer tokens without process-wide settings.
  Legacy `/1` binary64 decimal bytes remain unchanged; `/2` preserves original numeric tokens.

- Saturate Stable retry arithmetic before overflow and reject non-finite configuration.
- Sample time at persistent runtime transitions and fence expired work after queue/lock waits.
- Resolve concurrent SQLAlchemy checkpoint initialization with conditional insertion and atomic CAS.
- Close failed SQLAlchemy sessions and preserve AWS executor permits until outstanding calls finish.

## 1.6.0rc1 - 2026-09-03

### Added

- Closed CloudEvents 1.0.2 `titect-message/1` envelopes, canonical bounded JSON codecs,
  correlation/causation identity, immutable registries, separate routing, typed transport results,
  and deterministic JSON Schema, AsyncAPI 3.1, fixture, and manifest artifacts.
- Explicit command/query registries and pure decisions that distinguish domain events, integration
  events, commands, and tasks.
- Separate async reliability ports, finite process-local stores and harnesses, explicit units of
  work, bounded command/query runtimes, outbox relay, inbox consumer, backpressure, timeouts,
  cancellation propagation, and durable rejected-delivery quarantine contracts.
- Consumer-owned SQLAlchemy 2 async PostgreSQL model shapes, stores, `SKIP LOCKED` claims, and one
  explicit `AsyncSession` per unit of work.
- NATS JetStream acknowledged publication, pull delivery with ACK/NAK/TERM, message-ID deduplication
  hints, typed failures, and inert validate/plan/apply topology operations.
- AWS custom EventBridge bus publication and SQS Standard delivery through a bounded explicit
  executor, including partial-failure classification, visibility retries, delete acknowledgement,
  full-envelope `Detail`, and explicit topology plans.
- Route-free FastAPI context, idempotency, Problem Details, OpenAPI, and lifespan helpers; a bounded
  Django sync/async transaction bridge; and an unregistered FastStream/NATS handler adapter.
- Role-specific readiness, bounded metrics and structured observations, transport Trace Context,
  structured-concurrency supervision, and graceful finite shutdown.
- Optimistic process managers with explicit compensations and fenced durable timers; jobs with
  claims, leases, retries, terminal states, and one-shot, fixed-interval, or consumer-policy
  scheduling; atomic projection checkpoints with finite resumable rebuilds; and optimistic event
  streams with bounded pages and optional snapshots.
- Synthetic FastAPI and Django composition examples, deterministic fault injection, a crash matrix,
  PostgreSQL 15–18/NATS 2.14.5/LocalStack 4.14.0 opt-in canaries, and a protected OIDC AWS canary.

### Changed

- Expanded optional Django support to `>=5.2.1,<6.2` while retaining DRF `>=3.16,<4`.
- Classified event-platform contracts and runtimes as Preview, adapters as Low-level, and harnesses
  and fault injection as Testing during the release-candidate series.
- Expanded wheel smokes, optional-import checks, manifests, API snapshots, documentation checks, and
  release verification for every event-platform extra.

### Security

- Terminal broker settlement now requires durable quarantine. Payload retention is disabled by
  default; records contain bounded metadata, a SHA-256 digest, and a sanitized reason.
- The release workflow remains GitHub-only and emits wheel, sdist, SHA-256 checksums, SPDX SBOM, and
  provenance. PyPI and TestPyPI publication remain disabled.

## 1.0.0 - 2026-09-03

### Changed

- No functional changes since `1.0.0rc1`.
- Promoted the validated release candidate to the stable package version, classifier, and release
  manifest identity.

## 1.0.0rc1 - 2026-09-03

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

- Problem Details validate complete documents and output bounds; DRF adaptation preserves all
  original response headers except the replaced content type.
- Local references reject malformed pointer indices/escapes and duplicate YAML keys, and preserve
  `$ref` siblings through explicit `allOf` composition.
- Replay adapters persist digests rather than clear tokens or proofs. Released lease rows remain
  present so fencing tokens are monotonic.
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
- The GitHub-only release workflow extracts notes for the exact version, builds once, emits
  checksums, an SPDX SBOM, and provenance, rejects conflicting tags, and recovers same-SHA reruns.

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
