# Django integration

Construct `DjangoTransactionBoundary(using="alias")` explicitly. It delegates `atomic()` and
`on_commit()` to that exact database alias. Checks are registered only by calling
`pytitect.django.checks.register_checks`. Abstract models are opt-in and never provide package
migrations.

For fencing, provide callbacks that select and lock the consumer's authority row and extract its
token. The callback, comparison, and protected mutation run in the same Django transaction.
