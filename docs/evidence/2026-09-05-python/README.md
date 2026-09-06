# Local Python reliability evidence — 2026-09-05

These synthetic runs use source commit `b54e68cda565e943cdf21a0503b1c7004e3c82ec` with an
empty tracked diff. Each report records Python/package/platform identity, seed and parameters.
[infrastructure.json](infrastructure.json) records the exact Docker image IDs for PostgreSQL 16,
NATS 2.14.5 with JetStream enabled, and LocalStack 4.14.0. Every container, database schema and
broker resource belonged to this execution and was removed afterward.

| Report | Workload and result |
| --- | --- |
| [capacity.json](capacity.json) | Offered, saturation and process-recovery scenarios passed correctness and resource gates; 1,000 total offered requests, including all rejection and connection-error categories. |
| [soak.json](soak.json) | 60 seconds at 20 offered requests/second; 1,200 requests, 1,191 committed operations; receipt, local effect, outbox and receiving inbox counts reconcile, with no pending outbox work after drain. |
| [failed-budget.json](failed-budget.json) | Deliberate 1 MiB RSS budget failure. The runner exited unsuccessfully while retaining all 50 offered-request outcomes, resource observations and the explicit failed gate. This is a reporting test, not a passing capacity sample. |

The local full gate `uv run python tool/verify.py` passed for the production implementation at
`708ad6fe94c251fddbb66325d5034b22f76d1600`: 255 tests passed, with the manual real-AWS test
deselected and no selected integration skipped. Coverage was 88.36% globally, 91.04% for
core/HTTP/contracts, 96.58% for the existing risk tier and 95.55% for critical event-platform
paths. All existing floors were retained. Lint, formatting, strict typing, documentation,
public API/bundle checks, root optional-import checks, source/wheel builds, Twine and all
16 isolated wheel installation combinations passed. Subsequent benchmark-only changes were
exercised by the reports above. AsciiDoc was separately rendered with warnings treated as errors.

These runs took place on a shared development host, with other verification processes active.
They are reproducibility and correctness evidence, not reviewed latency baselines. Error-inclusive
latency percentiles include generator rejections at zero queue residence; status counts and useful
durable throughput must be read alongside those percentiles. Sampled task/connection/backlog values
can miss brief peaks; process RSS is the operating system's high-water observation. Recovery can
commit more operations than the HTTP client confirmed because a response may be lost after commit.

An earlier 60-second run at 50 offered requests/second reached its 20-second drain deadline with
466 pending outbox records, 2,581 receipts and 2,115 receiving inbox records. It failed the gate;
no successful sustainable rate is inferred from it. The final runner preserves JSON on failed
gates and makes the drain deadline explicit. The scheduled/manual 30-minute scenario uses a
recorded rate of 20 and a finite 120-second drain budget. That longer scenario was not run locally.

The required representative CI and PostgreSQL 15–18 expanded jobs run on the PR. Hosted artifacts
belong to their own commit and environment. LocalStack is emulator evidence; real AWS and paired
Dart/client scenarios are excluded. Issues #34, #40 and the remaining portion of #43 stay open.
