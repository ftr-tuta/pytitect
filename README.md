# Pytitect

Pytitect is a collection of explicit, consumer-owned building blocks for reliable Python
services. Its core has no runtime dependencies. Optional adapters support Django, FastAPI,
SQLAlchemy/PostgreSQL, NATS JetStream, AWS EventBridge/SQS, FastStream, and security contracts.

> **Release status:** the source version maps deterministically to a protected repository tag.
> Published tags, assets, and notes are authoritative in GitHub Releases; package-index publication
> is disabled.

## Design

Pytitect supplies policies, typed outcomes, ports, and bounded reference implementations. The
application retains ownership of its database schema, transaction placement, authentication,
authorization, routing, process model, and protocol binding. There is no global runtime and no
automatic protocol selection.

<!-- executable -->
```python
from datetime import timedelta

from pytitect import PytitectRuntime
from pytitect.idempotency import (
    IdempotencyCoordinator,
    IdempotencyPolicy,
    InMemoryIdempotencyStore,
)

runtime = PytitectRuntime()
coordinator = IdempotencyCoordinator(
    InMemoryIdempotencyStore[dict[str, object]](),
    policy=IdempotencyPolicy(
        execution_lease_ttl=timedelta(minutes=1),
        result_retention_ttl=timedelta(days=1),
        uncertainty_retention_ttl=timedelta(days=7),
    ),
    clock=runtime.clock,
)
```

Importing `pytitect` does not access settings, a database, the network, logging, or optional
frameworks. The in-memory stores are bounded test/reference tools; they do not promise durability,
cross-process coordination, or exactly-once delivery.

## Installation

The dependency-free core requires Python 3.12 or newer.

```console
pip install pytitect
pip install 'pytitect[django]'
pip install 'pytitect[drf,contracts]'
pip install 'pytitect[security]'
pip install 'pytitect[sync]'
pip install 'pytitect[fastapi,sqlalchemy,nats]'
pip install 'pytitect[aws]'
pip install 'pytitect[faststream-nats]'
```

The Stable 1.0 surface is unchanged. Event-platform contracts and runtimes are Preview APIs;
optional framework and transport adapters are Low-level APIs. Every adapter requires explicit
composition.

## Architecture and package map

- `pytitect.core`: clocks, deadlines, finite limits, request context, opaque IDs, fingerprints,
  and the explicit immutable runtime.
- `pytitect.http` and `pytitect.contracts`: Problem Details, exact version/capability decisions,
  deterministic manifests, and bounded local `$ref` resolution.
- `pytitect.idempotency` and `pytitect.receipts`: typed request coordination and state receipts.
- `pytitect.inbox`, `pytitect.outbox`, and `pytitect.checkpoints`: delivery primitives without a
  scheduler, worker, or schema.
- `pytitect.leases`: TTL ownership and monotonic fencing. Authority must be checked under the same
  lock and transaction as the protected mutation.
- `pytitect.maintenance`: bounded UTC-cutoff retention and archival plans with explicit dry runs.
- `pytitect.observability`: allowlisted structured events with hashing/redaction.
- `pytitect.trace`: validated W3C Trace Context values and explicit request-context association,
  without a tracing SDK or exporter.
- `pytitect.canaries`: consumer-triggered, one-round health probes with no scheduler.
- `pytitect.sync`: the neutral `titect-sync/1` contracts, authenticated opaque cursors, bounded
  mutation batches, dependency graphs, and generation guards without a sync engine or scheduler.
- `pytitect.django`, `pytitect.drf`, and `pytitect.security`: explicit optional adapters.
- `pytitect.messaging`, `pytitect.application`, and `pytitect.aio`: versioned messages, pure
  decisions, separate async ports, finite stores, and bounded runtimes.
- `pytitect.nats`, `pytitect.aws`, `pytitect.fastapi`, `pytitect.sqlalchemy`, and
  `pytitect.faststream_nats`: explicit Low-level adapters with no automatic binding or lifecycle.
- `pytitect.processes`, `pytitect.jobs`, `pytitect.projections`, and `pytitect.event_sourcing`:
  optimistic Preview workflow and persistence contracts.

See the focused guides in [`docs/`](docs/architecture.md), the executable
[`adoption path`](docs/adoption.md), the synthetic
[`Django/PostgreSQL reference`](examples/django_reference/README.md), and the
[`public API stability policy`](docs/api-stability.md). Prerelease adopters should also read the
[`1.0 migration guide`](docs/migration-1.0.md).
Event-platform adopters should start with the [`1.6 adoption guide`](docs/event-platform-adoption.md)
and review its [`compatibility matrix`](docs/event-platform-compatibility.md).
The [`large-scale architecture roadmap`](docs/large-scale-architecture-roadmap.adoc) records the
Pytitect/Dartitect audit, the FastAPI-led reference architecture, and prioritized reliability,
interoperability, and capacity-validation work.

## Security boundaries

Pytitect validates protocol proofs. It does not decide which session, connector, database, tenant,
or permission a proof authorizes. Key resolution and binding remain application decisions. Never
log raw bodies, credentials, cookies, tokens, idempotency keys, DSNs, or sensitive paths.

Report vulnerabilities using [SECURITY.md](SECURITY.md), not a public issue.

## Development

```console
uv sync --all-extras
uv run ruff check .
uv run mypy
uv run pytest
uv run python tool/verify.py
```

The project is BSD-3-Clause licensed.
