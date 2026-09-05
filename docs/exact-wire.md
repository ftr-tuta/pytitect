# Exact wire boundaries and negotiated page integrity

`pytitect.wire` and the additions described here are Preview. `JsonValue`, root imports,
existing sync mapping APIs, `Message`, and the default `/1` codec remain available.
Applications select codecs, policy, transport, session persistence, authorization, transactions,
and responses explicitly. This document is the candidate cross-language contract for #40.

## Exact JSON representation

`ExactNumber(token)` represents a valid JSON numeric token without conversion. Its immutable
identity includes every character: `1`, `1.0`, `1e0`, `1E+000`, `-0`, and `-0.0` differ.
The grammar is `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`. Exponents are never expanded.
Integer tokens, including 4,301 digits, remain subject to the wire byte budget; no global
integer-conversion setting is changed. No float, integer, list, or mutable numeric representation
is silently accepted as an exact value. Arrays are tuples; objects are defensively frozen maps.

`WireDocument.encode()` emits strict UTF-8 without BOM, whitespace, or trailing newline.
Object keys sort lexicographically by Unicode scalar values, without normalization. Arrays retain
order. Strings escape quote as `\"`, backslash as `\\`, and BS/FF/LF/CR/TAB as
`\b`, `\f`, `\n`, `\r`, `\t`. Remaining U+0000–U+001F characters use lowercase
four-digit `\u00xx`. Slash and all other Unicode scalars are emitted literally, including
U+2028 and U+2029. Surrogates are forbidden; escaped valid surrogate pairs decode to one scalar.
Numbers emit their unchanged token. This is not RFC 8785 numeric canonicalization.

`ExactNumber.to_int()` explicitly accepts only integer tokens; `to_decimal()` checks Decimal's
exponent range; `to_float()` rejects any nonfinite or mathematically inexact binary64 conversion.
`WireDocument.to_json()` uses integer conversion for integer tokens and checked float conversion
for other tokens. These conversions discard lexical distinctions only at the caller's request.
For example, `0.1` requires Decimal; it cannot be converted by `to_float()` without precision loss.

## Raw boundaries and legacy behavior

Use `decode_wire(bytes)` or `decode_wire_stream(Iterable[bytes])`; the borrowed iterable is never
closed. Chunks may split UTF-8 characters, escapes, or numeric tokens. Parsing charges actual
bytes, including whitespace and overwritten members, before decoding each chunk. Each value
occurrence, including containers and the root, consumes one aggregate value; object keys consume
string budget but not a separate value. Root depth is zero. Every duplicate value consumes its
full allocation budget before the last occurrence replaces earlier values. Decoded string limits
count Unicode scalars. Cancellation and source failures propagate. Only finite, explicitly
configured input is retained; the parser cannot limit memory already allocated by its producer.

The typed errors have payload-free codes `syntax`, `limits`, `shape`, `unsupported_profile`,
`integrity`, and `precision`. No payload excerpts are included in error messages or causes.
`JsonMessageCodec.decode_raw()` and `.decode_stream()` expose those failures. `.decode()` keeps
the existing `ValueError` surface while using the bounded parser. Exact `/2` decoding exposes
the typed failures directly. Sync wire consumers use `decode_sync_raw()` or
`decode_sync_stream()`; passing `json.loads()` output to mapping validation does not establish
raw-input conformance. `SyncWireDocument.to_contract()` is an explicit checked conversion.

Legacy `titect-message/1` uses Python's finite binary64 decimal interpretation and deterministic
`json.dumps` formatting (UTF-8, sorted scalar keys, compact separators). Decimal tokens can round
or underflow: `1.00000000000000001` becomes `1.0`, `1e-7` becomes `1e-07`, `1e20` becomes
`1e+20`, and `1e-9999` becomes `0.0`. `-0.0` remains `-0.0`; integer `-0` becomes `0`.
Integer tokens do not pass through float. Overflow to infinity is rejected. Dart must implement
this compatibility encoding separately from its exact-token representation. These historical
results must remain in profile-specific expectations; changing them would require a new profile.

Both message and sync timestamps use exactly `YYYY-MM-DDTHH:MM:SS.sssZ`, with calendar validation
and exact reformat comparison. Normalization of invalid fields such as hour 24 is rejected.

## Explicit message profile

`titect-message/2` retains the same closed envelope and CloudEvents `specversion: "1.0"`, with
`profile: "titect-message/2"`. All envelope metadata and timestamp constraints remain as in `/1`.
`data` contains exact JSON. `ExactMessage` requires a `WireDocument`; `ExactJsonMessageCodec`
uses the exact encoding above. Its registry key is `application/json;profile="titect-message/2"`;
the envelope's `datacontenttype` remains `application/json`. Codecs reject the other profile.
No default or automatic profile fallback changes.

SQLAlchemy's injected byte serializer, the NATS codec, relay payload, and consumer admission codec
must all use the selected representation. Configure reference outbox `payload_size=codec.encode`
through a length-taking callable; exact payload sizing has no automatic codec selection.
The EventBridge/SQS adapter does not claim exact-token preservation through JSON transformation;
unsupported exact selection fails explicitly. Real PostgreSQL/JetStream evidence is required
before paired transport acceptance. This contract does not assert that evidence has passed.

## Optional sync page verification

Request `integrity-sha-256-exact-json-v1` in the existing bootstrap request `capabilities` array.
The provider acknowledges it using `Titect-Sync-Integrity: integrity-sha-256-exact-json-v1`.
Closed `/1` document fields do not change. The application persists this selection with its session
context and supplies the acknowledgement on subsequent responses. Missing, different, or unsupported
acknowledgement fails when requested. Absence of selection retains the legacy shape-only integrity
behavior; the older `integrity-sha-256` capability alone is not cryptographic verification evidence.

The injected `SyncIntegrityPolicy` verifies the complete sync envelope after removing only
`payload.integrity`. The built-in `ExactJsonSha256Integrity` hashes these concatenated bytes:

1. ASCII `titect-sync/1`, NUL, ASCII `integrity-sha-256-exact-json-v1`, NUL.
2. Deterministic exact JSON encoding of that complete envelope.

Protocol, kind, dataset, generation, ordered upserts and tombstones, every item value, and the
cursor remain covered. The integrity object declares `algorithm: "sha-256"`, a lowercase 64-digit
hex digest, and integer-token `item_count` equal to upserts plus tombstones (upserts only for
snapshots). Verification runs before returning a page for application or checkpoint advancement.
Hash verification is integrity checking, not authentication; consumers own trust and authorization.

`select_sync_integrity(requested, acknowledgement, policies=[...])` creates explicit session context.
Supply it as `integrity=` and the received header as `acknowledgement=` to the raw sync boundary.
`ExactJsonSha256Integrity.seal(document)` produces a sealed exact page for an explicitly selected
provider policy. No session, network, store, binding, or checkpoint is created by these helpers.

## Paired candidate handoff

Pytitect owns `interop/conformance/legacy-vectors.json`, preserved byte-for-byte from the existing
156-case Dart fixture, and its extension corpus with explicit expectations. Candidate reports must
pin committed Python and Dart SHAs, source versions, corpus hash, bundle manifest hashes and bundle
digests; run both Dart VM and Chrome. Candidate evidence is ineligible for release acceptance.
The Dart agent owns all Dart changes, the 20 existing durable recovery scenarios and their extensions,
and the real-client reconnect/load harness. Integrated acceptance must rerun against the protected
Python `main` commit after merge. Releases and tag publication are outside this task.
