# Django integration

Construct `DjangoTransactionBoundary(using="alias")` explicitly. It delegates `atomic()` and
`on_commit()` to that exact database alias. Checks are registered only by calling
`pytitect.django.checks.register_checks`. Abstract models are opt-in and never provide package
migrations.

The concrete PostgreSQL adapters require a non-empty `using` alias and can be constructed either
with `from_model(...)` for a concrete subclass of a Pytitect abstract model or with
`from_callbacks(...)` for an existing consumer schema. Every callback receives the selected alias.
Pytitect does not provide tables, migrations, routers, or automatic alias selection. Concrete
models must define the documented unique constraints.

`DjangoIdempotencyStore`, `DjangoReplayStore`, `DjangoInboxStore`, `DjangoOutboxStore`,
`DjangoCheckpointStore`, `DjangoReceiptStore`, `DjangoLeaseStore`,
`DjangoMutationBatchStore`, and
`DjangoGenerationStore` use PostgreSQL row locks. Outbox claims are ordered and use
`select_for_update(skip_locked=True)`. Replay rows contain SHA-256 digests, never clear proofs.
Lease rows survive release so their fencing tokens remain monotonic.

Mutation batch models must uniquely constrain `(namespace, batch_id)`. Each item mutation, its
idempotency receipt, and partial batch advancement must use the same alias and transaction. The
adapter retains completed and uncertain rows until their configured terminal retention expires;
expired execution leases allow one locked worker to resume instead of deleting partial progress.

`DjangoFencedCommit.commit()` locks authority and performs the protected mutation in one
transaction. `DjangoTransactionalOperation` rejects mixed aliases during construction and commits
the domain mutation, terminal receipt, outbox messages, and idempotency completion atomically.
Expected compare-and-set failures return typed rollback results; unexpected exceptions propagate
after Django rolls the transaction back. There is no distributed-transaction fallback.

Normal receipt transitions keep `UNCERTAIN` terminal. Only `ReceiptReconciler`, backed by the
store's locked `reconcile_uncertain()` CAS, may resolve uncertainty to completed, rejected, or
conflicted, and concurrent reconciliation has a single durable winner.
