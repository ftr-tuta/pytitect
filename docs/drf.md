# DRF integration

Derive request serializers from `ClosedSerializer` to reject unknown keys. Use strict fields where
JSON type coercion would obscure a contract mismatch. `BoundedJSONField` applies explicit byte,
depth, item, and string limits.

`make_exception_handler` returns a handler; the consumer installs it in DRF settings. Pytitect never
changes `REST_FRAMEWORK`. Schema decorators are also returned explicitly and do not register a global
schema.
