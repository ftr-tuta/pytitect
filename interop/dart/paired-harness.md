# Candidate paired harness contract

The current `candidate.json` pins the last committed Dart source and deliberately cannot pass
until that source incorporates the authoritative Python corpus and the required harness extensions.
It is not an acceptance manifest. Update this pin to the Dart agent's committed SHA and version;
never remove the paired job from `CI / Required` to merge a candidate.

Python executes `python -m tool.paired_gate --dart-root <clean checkout> --dart-sha <pin>
--mode candidate --output <new directory>` with real `TEST_POSTGRES_DSN`, `TEST_NATS_URL` and
`CHROME_EXECUTABLE`. Reusing an evidence directory fails, preserving previous failed reports.
The gate aliases the service settings to `TITECT_POSTGRES_DSN` and `TITECT_NATS_URL` for the
Dart-owned fixtures. The artifacts and services remain test-owned and outside the SDK wheel.

The gate verifies both clean committed sources, source versions, the full 232-case corpus,
expectations, and all three bundle identities before execution. A candidate-reference JSON records
these identities, the execution mode, an execution identifier, and `releaseEligible: false`.
Dart VM and Chrome execute the same corpus and must emit the exact per-case expectation fields
specified in [the wire contract](../../docs/exact-wire.md). Differences cannot be normalized away.

The Dart native fixture builds with the existing command:

```text
dart build cli --root-package dartitect_drift -t tool/titect_fixture/composition/native_actor.dart -o <output>/native
```

The Dart agent owns the following two executables and their implementation:

```text
python tool/run_titect_recovery.py --python-root <source> --actor <binary> --reference-manifest <json> --output <directory> --django-python <python>
python tool/run_titect_capacity.py --python-root <source> --actor <binary> --reference-manifest <json> --output <directory>
```

`--reference-manifest` is the required candidate extension to the existing recovery driver. It
must validate the manifest against actual sources, versions, bundles, corpus and actor; it must not
rewrite the committed Dart pin or accept a candidate as a release. Capacity uses the real Dart
client and the Python/PostgreSQL/JetStream fixture. The three representative scenarios are
`offered`, `saturation`, and `recovery`; client disconnect/reconnect belongs in recovery.

Each driver emits `<name>.json` and `<name>.sha256`. The checksum file is lowercase SHA-256,
two spaces, the report filename, and a newline. Both reports contain `schemaVersion: 1`,
`status: "passed"` only after all checks, `releaseEligible: false`, the unchanged `reference`
manifest, and `nativeActorSha256` for the actual executed binary. Failures retain their reports
and exit nonzero. Unverified or unresolved contracts cannot pass. Checksums establish file
consistency; trusted execution at the recorded source and protected CI establish provenance.

`residualResources` must explicitly contain zero integer counts for `activeAuthorities`,
`childProcesses`, `openDatabases`, `openHttpClients`, `postgresConnections`, `queuedTasks`, and
`runningTasks`. The runner terminates only its own process group on failure or cancellation.

Recovery retains the 20 existing scenario names and adds `corrupted-page-rejection`,
`negotiated-policy-mismatch`, `exact-number-persistence`, and
`integrity-failure-state-and-checkpoint-unchanged`. `scenarios` contains unique `{name, passed}`
rows and any supporting measurements. The exact required inventory is executable in
`tool/paired_gate.py`; no successful-only subset can pass.

Capacity `results` uses the existing Python report field names: `scenario`, `passed`, `failures`,
`offered`, `statuses`, `duration_seconds`, `useful_operations`, `useful_throughput`,
`latency_seconds`, `peak_observations`, `recovery_seconds`, `durable`, and `load_generator`.
Every offered request must appear in status/rejection/error counts. Latency includes p50/p95/p99/max.
Resource observations include RSS, tasks, connections, database waits and backlog age. Durable
receipt/outbox/inbox counts reconcile after finite drain, with integer retry counts and no pending
outbox work. The existing server limits remain 512 MiB RSS, 100 sampled tasks and eight connections;
client budgets and observations must retain the Dart fixture's finite limits. No latency threshold
is introduced. The 30-minute soak remains a separately recorded required capacity acceptance run.

Integrated mode verifies the Python source is reachable from fetched upstream `main`; final paired
reports must name that integrated SHA. Protocol conformance alone never authorizes a release.
