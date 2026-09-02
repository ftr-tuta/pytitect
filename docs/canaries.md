# Canaries

A `CanarySuite` runs one explicit round of consumer-supplied probes and emits passed or failed typed
results. It has no timer, scheduler, network client, or retry loop. The consuming service decides when
and where a probe runs and supplies any external effect.

Route canary attributes through the privacy-first observation policy. Probe results should use
synthetic identifiers and must not expose bodies, credentials, tenant data, or sensitive paths.
