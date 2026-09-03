# Executable adoption path

Start with an in-memory store only for bounded, process-local tests. Supply all policy and time
sources explicitly:

<!-- executable -->
```python
from datetime import timedelta

from pytitect import PytitectRuntime
from pytitect.idempotency import IdempotencyCoordinator, IdempotencyPolicy, InMemoryIdempotencyStore

runtime = PytitectRuntime()
coordinator = IdempotencyCoordinator(
    InMemoryIdempotencyStore[dict[str, object]](capacity=100),
    policy=IdempotencyPolicy(
        execution_lease_ttl=timedelta(minutes=1),
        result_retention_ttl=timedelta(days=1),
        uncertainty_retention_ttl=timedelta(days=7),
    ),
    clock=runtime.clock,
)
assert coordinator is not None
```

For durable adoption, copy the ownership pattern—not application details—from the
[Django/PostgreSQL reference project](../examples/django_reference/README.md). Its tests install the
exact candidate wheel, run concrete consumer migrations, exercise crash/retry and retained outbox
terminals, and expose legacy and versioned routes over one service.

Run `uv run python tool/verify.py` before integration. The gate checks documentation, generated
compatibility, protocol manifests, public API, isolated imports, coverage tiers, tests, and artifacts.
