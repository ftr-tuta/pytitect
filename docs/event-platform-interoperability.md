# Event platform interoperability

`interop/titect-message/1` is independent of the package version. Its JSON Schema 2020-12 document,
AsyncAPI 3.1 components, capabilities, canonical fixtures, and SHA-256 manifest define the wire
profile. AsyncAPI intentionally contains no servers, channel addresses, operations, or topology.
Applications bind the reusable message component to their own routes.

Python implementations validate the bundle with `python tool/message_bundle.py`. Dart and other
implementations should decode the same positive fixture, reject every negative fixture, encode the
positive fixture back to identical UTF-8 bytes, and compare each manifest hash. String, numeric,
nesting, metadata, and byte limits must be applied before allocating unbounded data.

Correlation identifies the wider flow. Causation identifies the immediate input. Neither changes the
message ID used for inbox identity and broker deduplication hints. Trace Context remains transport
metadata so the closed envelope stays interoperable across transports.

At-least-once delivery is the only cross-transport promise. An implementation must survive duplicate
delivery, crash after local commit but before ACK, crash after broker confirmation but before marking
an outbox row delivered, expired claims, and stale fencing tokens.

See [exact wire boundaries and explicit profile adoption](exact-wire.md) for Preview `/2`, legacy
binary64 compatibility, bounded raw input, and negotiated page integrity.
