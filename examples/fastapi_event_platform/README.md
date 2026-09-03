# Synthetic FastAPI event-platform example

`composition.build_app()` is the application-owned composition root. The example deliberately
constructs its own routes and chooses the Pytitect helpers it uses. The package never imports this
example or registers these routes.
