# Idempotency

The client supplies the idempotency key. The application supplies a scope containing namespace,
subject, and operation and a fingerprint of the canonical request. The first atomic reservation
returns `Execute`; an identical completed request returns `Replay`; a changed fingerprint returns
`Conflict`; concurrent work returns `InProgress`; and an effect whose outcome cannot be established
returns `Uncertain`.

`IdempotencyPolicy` keeps execution authority separate from terminal-state retention. An active
worker may `renew()` its short execution lease. `complete()` starts result retention at the
completion transition, while `mark_uncertain()` starts the independently configured uncertainty
retention period. `abandon()` releases only a current executing reservation. Every transition
returns a typed result, including `StaleReservation`; callers must not treat an expired token as
authority to publish a result.

Never silently retry an uncertain non-idempotent effect. Persist the reservation and effect in the
same consumer-owned transaction when possible. The memory store is bounded, process-local, and not
appropriate for multi-worker durability.
