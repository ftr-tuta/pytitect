# Architecture

Pytitect is a single distribution with a dependency-free core and explicit optional adapters. Core
objects are immutable where practical and receive clocks, limits, stores, observers, resolvers, and
transaction boundaries from the application. Expected boundary failures are typed values; exceptions
indicate invalid configuration, broken invariants, or unexpected integration failures.

The package has no startup hook. It registers no settings, schemas, models, routes, middleware,
signals, loggers, authentication classes, workers, or OpenAPI components on import. Abstract Django
models live in an explicit module, and a consumer that uses them owns the concrete subclasses and
migrations.

Durability is expressed through ports. Process-local stores are only bounded reference and test
implementations. A production store must use atomic compare-and-set operations and the consumer's
transaction. Outbox delivery is at-least-once; handlers and inbox processing must tolerate duplicates.
