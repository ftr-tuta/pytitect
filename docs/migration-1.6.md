# Migrating from 1.0 to the 1.6 release candidate

No migration is required for Stable 1.0 APIs. The package root exports exactly the same symbols and
remains dependency-free. New event-platform APIs are available only through explicit submodules and
optional extras.

To adopt messaging, create explicit message IDs and millisecond UTC timestamps; the codec never
generates or rounds either value. Move broker destinations out of event type strings and into a
`RoutingTable`. Store the complete canonical envelope in transport payloads.

To adopt async processing, implement the async ports directly. Do not wrap a synchronous database
session that is shared between tasks. ACK only after the unit of work commits. Treat a publish
confirmation followed by a worker crash as a possible duplicate and retain inbox identities for the
source redelivery window.

Terminal rejection now means durable quarantine succeeded. The default policy stores a sanitized
reason, bounded metadata, and SHA-256 payload digest but not the payload. Enabling payload retention
is an explicit data-governance decision.

Process managers, jobs, projections, and event sourcing are independent Preview modules. Introduce
their concrete stores and migrations in the application. Preserve optimistic versions, lease fencing,
finite pages, and atomic local transitions. Put derived external effects in the outbox rather than
calling them inside a transaction.
