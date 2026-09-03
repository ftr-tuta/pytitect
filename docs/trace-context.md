# Trace context and observable vocabulary

`pytitect.trace` validates and renders the W3C Trace Context fields without installing middleware,
starting spans, selecting a sampler, or binding an OpenTelemetry implementation. `TraceContext`
accepts nonzero lowercase 128-bit trace IDs, nonzero lowercase 64-bit parent IDs, one byte of trace
flags normalized to the sampled and random-trace-ID bits, and at most 32 unique tracestate members
within a documented 512-character bound. Version
`00` must have exactly four fields. Additive future versions are parsed from the defined prefix and
normalized to version `00`, with only the sampled and random-trace-ID flags retained.

Use `trace_context_from_headers()` for a case-insensitive mapping, or
`TraceContext.parse(traceparent, tracestate)`. A tracestate without traceparent is invalid. The
parser preserves valid member order and leading spaces inside opaque values. It rejects uppercase
identifiers, zero identifiers, duplicate keys, control characters, oversize fields, and invalid
delimiters.

`bind_trace_context()` returns a `TracedRequestContext`; this makes association with
`RequestContext` visible at the call site. DRF users may call `adapt_trace_context(request)`
explicitly. Pytitect does not read request headers automatically or register framework hooks.

The common safe observability vocabulary is `operation`, `outcome`, `protocol`, `sync_kind`,
`dataset_hash`, `trace_sampled`, `item_count`, and `duration_ms`. Raw dataset, item, client, session,
receipt, and mutation identifiers are deliberately absent. `pseudonymous_attribute()` implements
the fixture-compatible keyed BLAKE2b-128 pseudonym used by `ObservationPolicy`; applications own
the secret key, its rotation, and any decision to retain or export events.

Trace IDs and tracestate are correlation data, not authorization claims. Consumers must define
trust boundaries, recording limits, cross-origin exposure, and whether incoming context should be
continued or discarded. There is no OpenTelemetry adapter in Pytitect 1.0.
