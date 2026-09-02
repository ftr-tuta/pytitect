# Contributing

Use a short-lived branch and open a pull request against `main`. Keep commits conventional and
focused. Public documentation, identifiers, and messages must be in English. Add tests for behavior
and for boundary failures, and run `uv run python tool/verify.py` before requesting review.

Pytitect must remain neutral: never add consumer-specific domain rules, schemas, routes, workers,
credentials, or private artifacts. Optional integrations must be inert until the consumer imports
and calls them. New public API requires an API snapshot update and a changelog entry.

By participating, you agree to the Code of Conduct.
