# Django/PostgreSQL reference project

This consumer-owned project demonstrates Pytitect's explicit Django integration against a real
PostgreSQL database. Every identifier and payload is synthetic. Nothing in this directory is
registered or imported by the `pytitect` package.

The example includes:

- concrete consumer models, constraints, indexes, and a consumer-owned migration;
- one transaction for the domain write, terminal receipt, outbox message, and idempotency result;
- rollback after a synthetic crash and a safe retry;
- legacy and `titect-sync/1` routes over the same service;
- one finite outbox dispatch round, terminal failure, retained rows, purge, and durable archival;
- an OpenAPI 3.1 document whose neutral sync components reference `interop/titect-sync/1`.

Run it with a disposable PostgreSQL database:

```console
export REFERENCE_POSTGRES_DSN=postgresql://postgres:postgres@localhost:5432/pytitect_reference
uv sync --project examples/django_reference --frozen
uv pip install --python examples/django_reference/.venv/bin/python dist/pytitect-*.whl
examples/django_reference/.venv/bin/python examples/django_reference/manage.py migrate
examples/django_reference/.venv/bin/python -m pytest examples/django_reference
```

The application chooses its database alias, transaction boundary, routing, payload, retention
schedule, dispatcher invocation, and archive table. Production consumers must make those choices
for their own durability, authorization, and operational requirements.
