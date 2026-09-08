# Testing

Run lint, strict type checking, unit/property tests, package builds, smoke imports, the public API
snapshot, and manifest checks through `tool/verify.py`. Production persistence adapters should run the
provided conformance patterns plus transaction and real PostgreSQL concurrency tests.

The required global branch-coverage floor is 85%. A second aggregate gate requires 90% for core,
HTTP, and contract modules, and 95% for idempotency, sync, leases, trace parsing, and security
parsers. Real PostgreSQL crash, retry, takeover, retention, and fencing scenarios remain mandatory
independently of those percentages.

Security tests should include published vectors and malformed encodings, duplicate claims, wrong
context, time skew, replay, and payload limits. Package smoke tests must use clean environments for
the core and every supported extras combination.

See [independent reliability foundations](reliability-foundations.md) for current async adoption,
implemented store inventory, live conformance and reproducible Python capacity validation.
