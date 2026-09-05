# Synthetic FastAPI composition

`composition.build_app` requires an explicit `SQLAlchemyIdempotentRequest`, a local mutation
callback, authenticated scope mapping, receipt identity function and readiness policy. Consumers
supply their session factory, models, serializers and retention policy to the coordinator. Its
callback writes local state and outbox in the supplied session without committing or making
external effects. The example chooses its own HTTP routes and responses and provides independent
reconciliation with the original key and fingerprint after response loss.

The real HTTP/SQLAlchemy/PostgreSQL/JetStream workload is in
[`benchmarks/python`](../../benchmarks/python/README.md). See
[adoption and ownership](../../docs/reliability-foundations.md) before adapting either fixture.
