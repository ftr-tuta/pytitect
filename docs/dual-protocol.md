# Dual-protocol coexistence

Bind legacy and new protocol views separately in application URL configuration. Each binding selects
its descriptor, serializers, authentication, authorization, and error renderer explicitly. Do not
infer a protocol from payload shape and do not fall back after validation fails. Shared domain services
may sit behind both bindings when their input types make the boundary explicit.
