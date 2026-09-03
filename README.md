# Pytitect

Pytitect is a collection of explicit, consumer-owned building blocks for reliable Python
services. Its core has no runtime dependencies. Optional adapters support Django 5.2, Django
REST Framework, drf-spectacular, RFC 8785, DPoP, and HTTP Message Signatures.

> **Status:** the source is prepared as `1.0.0rc1`; candidate tag `v1.0.0-rc.1` has not
> been materialized. `v0.9.0a1` remains the latest public distribution.

## Design

Pytitect supplies policies, typed outcomes, ports, and bounded reference implementations. The
application retains ownership of its database schema, transaction placement, authentication,
authorization, routing, process model, and protocol binding. There is no global runtime and no
automatic protocol selection.

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
```

The supported web framework line is deliberately narrow: Django `5.2.x` and DRF `>=3.16,<4`.
The `pytitect.aio` namespace is reserved; this release has no FastAPI, ASGI, or async-store
implementation.

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

See the focused guides in [`docs/`](docs/architecture.md), the synthetic examples in
[`examples/`](examples/django_legacy/README.md), and the public API policy in
[`docs/versioning.md`](docs/versioning.md). Prerelease adopters should also read the
[`1.0 migration guide`](docs/migration-1.0.md).

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
