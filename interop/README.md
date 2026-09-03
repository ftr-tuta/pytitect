# Interoperability fixtures

These public fixtures define the Python candidate's wire-level expectations. They use RFC 8785
and I-JSON where canonical bytes or interoperable fingerprints are required; the dependency-free
`canonical_json_bytes()` helper is not a wire canonicalizer.

The fixtures are unilateral in this candidate and do not constitute completed bilateral
interoperability. Consumers must validate them against their own implementations.

The `titect-sync/1` directory is a versioned protocol bundle independent of the package version. It
contains closed JSON schemas, route-free OpenAPI 3.1 components, header/capability/limit registries,
positive and negative fixtures, and a deterministic SHA-256 manifest. Run
`python tool/sync_bundle.py` to validate its artifacts, executable fixtures, and manifest.
