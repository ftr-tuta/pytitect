# DRF integration

Derive request serializers from `ClosedSerializer` to reject unknown keys. Use strict fields where
JSON type coercion would obscure a contract mismatch. `BoundedJSONField` applies explicit byte,
depth, item, and string limits.

`make_exception_handler` returns a handler; the consumer installs it in DRF settings. Pytitect never
changes `REST_FRAMEWORK`. Schema decorators are also returned explicitly and do not register a global
schema.

`adapt_trace_context(request)` is an opt-in strict adapter for W3C `traceparent` and `tracestate`
headers. It returns `None` when neither header is present and raises `ValueError` for malformed
context. The consumer decides how that boundary error is represented and whether the context is
trusted or propagated.
