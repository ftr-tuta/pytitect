# Canaries

A `CanarySuite` runs one explicit round of consumer-supplied probes and emits passed, failed,
crashed, timed-out, or skipped typed results. A `TimeoutError` is a timeout and every other
`Exception` is a crash; later probes still run. It has no timer, scheduler, network client, thread
termination, or retry loop. The consuming probe sets I/O timeouts and owns every external effect.

Route canary attributes through the privacy-first observation policy. Probe results should use
synthetic identifiers and must not expose bodies, credentials, tenant data, or sensitive paths.

The complete generated consumer snapshot under `canaries/django_protocol_matrix/` is outside the
Python distribution. Its provenance and exact Cookiecutter inputs are committed beside it. Its
concrete apps, models, migrations, credentials, protocol bindings, and URLs are consumer-owned.
