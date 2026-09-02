# Testing

Run lint, strict type checking, unit/property tests, package builds, smoke imports, the public API
snapshot, and manifest checks through `tool/verify.py`. Production persistence adapters should run the
provided conformance patterns plus transaction and real PostgreSQL concurrency tests.

Security tests should include published vectors and malformed encodings, duplicate claims, wrong
context, time skew, replay, and payload limits. Package smoke tests must use clean environments for
the core and every supported extras combination.
