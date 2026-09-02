# Interoperability fixtures

These public fixtures define the Python candidate's wire-level expectations. They use RFC 8785
and I-JSON where canonical bytes or interoperable fingerprints are required; the dependency-free
`canonical_json_bytes()` helper is not a wire canonicalizer.

The fixtures are prepared for future Dartitect consumption, but they are unilateral in this
candidate. They do not constitute completed bilateral interoperability and have not been executed
against AgroX or another private consumer matrix.
