# Synthetic Django event-platform example

This example keeps composition, model names, migrations, URLs, and process invocation in the
application. `transaction_components()` demonstrates the explicit database alias and bounded async
bridge. Pure rules and contracts live in `examples/event_platform_shared` and are shared with the
FastAPI example.
