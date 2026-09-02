# Interoperability fixtures

These public fixtures define the Python candidate's wire-level expectations. They use RFC 8785
and I-JSON where canonical bytes or interoperable fingerprints are required; the dependency-free
`canonical_json_bytes()` helper is not a wire canonicalizer.

The fixtures are unilateral in this candidate and do not constitute completed bilateral
interoperability. Consumers must validate them against their own implementations.
