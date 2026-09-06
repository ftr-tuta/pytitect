# Exact wire candidate evidence — 2026-09-05

The soak runtime source is `8828767ed3f42cef674b9b096058cb969c9f233f` (`1.6.0rc1`), executed from
an isolated clean checkout on CPython 3.13.13. [identity.json](identity.json) records corpus and
bundle hashes. All 156 original inputs are preserved; the authoritative corpus contains 232 cases.
[verification.json](verification.json) records successful command exits, durations and clean source
after execution. These are Python candidate results; paired and release acceptance remain pending.

| Report | Measured result |
| --- | --- |
| [capacity.json](capacity.json) | Offered, saturation and process recovery passed: 1,000 total offered requests, with all rejection/error categories accounted for and no pending outbox after drain. |
| [soak.json](soak.json) | 30 minutes at 20 offered requests/second: 36,000 offered, 35,891 committed and received, 109 explicit HTTP 503 rejections. Receipt/outbox/inbox counts reconcile; 71,782 local and receiving effects. |
| [cleanup.json](cleanup.json) | All 15 locally created verification/capacity containers and both observed driver/soak processes were absent after completion. |
| [paired-failed.json](paired-failed.json) | Required hosted paired gate rejected the old Dart corpus before executing acceptance stages. This failed report is retained, not treated as acceptance. |

The soak retained the existing 120-second finite drain budget, 512 MiB process RSS limit, eight
connections and 100 sampled tasks. Final drain observation took 0.141 seconds. Useful throughput was
19.938 operations/second; error-inclusive latency p50/p95/p99/max was 0.0285/0.0946/0.2954/1.1551
seconds. Sampled peaks were 107,912 KiB RSS (105.4 MiB), 28 tasks, eight connections, 215 pending
outbox records, 12.191 seconds backlog age and zero database lock waiters. There were no publication
retries or pending outbox records after drain. These are observations, not new latency thresholds.

[infrastructure.json](infrastructure.json) records exact PostgreSQL 16, NATS 2.14.5 and LocalStack
4.14.0 image identities. The separate [PostgreSQL 15](postgres-15.json),
[PostgreSQL 17](postgres-17.json), and [PostgreSQL 18](postgres-18.json) runs each passed 14 real
adapter tests, including exact-number persistence and unchanged effects/checkpoints after failed
integrity. The combined live test also preserved exact bytes through PostgreSQL outbox storage,
relay publication, JetStream delivery and atomic consumer admission. Django and LocalStack ran in
the full gate; real AWS remained explicitly manual and was deselected.

The isolated runtime candidate passed the full `uv run python tool/verify.py` gate: 363 tests,
all 16 isolated wheel installation combinations, source/wheel builds, Twine, strict typing,
formatting, lint, docs, API and bundle checks. Its coverage was 88.96% globally, 91.19% for
core/HTTP/contracts, 96.83% for the parser/risk tier and 95.69% for critical event-platform paths.

Commit `13ffcb8a048ad89f7b4739abd6da5764adfc3498` adds only paired tooling, tests and CI configuration;
its `src/pytitect` tree is identical to the runtime candidate. Its final full gate on CPython 3.14.4
passed 381 tests with the manual AWS test deselected, all 16 isolated wheel installations and all
other gate stages. Coverage was 89.00%, 91.35%, 96.90% and 95.74% respectively. AsciiDoc rendered
with warnings treated as errors. Existing coverage floors were unchanged.
[final-verification.json](final-verification.json) and
[final-verification-infrastructure.json](final-verification-infrastructure.json) record this check.

The final runtime correction at `d964ad5f152f9403aa1d30fffc23d93bdb1ef370` makes both message codecs
honor an explicitly enlarged envelope budget during parsing; an explicitly smaller wire limit
also constrains encoding. Two regression cases cover payloads above the default 1 MiB budget.
This changes the SDK source after the soak candidate; the default budget and all corpus/bundle
identities are unchanged. The soak report remains pinned to its actual tested revision.
[Budget-fix verification](budget-fix-verification.json) and its [gate summary](budget-fix-gate.json)
record a clean committed-source full gate: 383 tests, one manual AWS test deselected, all 16
isolated installations, builds and the same coverage percentages and floors as the preceding run.

[Budget-fix capacity](budget-fix-capacity.json) passed offered, saturation and recovery scenarios
again, with 1,000 requests and all response/error categories recorded. The respective durable
useful operation counts were 96, 85 and 45, with zero pending outbox after each finite drain.
Recovery returned 44 HTTP 201 responses for 45 durable operations: one operation committed while
its response was lost during process termination. The report preserves that uncertainty rather
than equating responses with commits. [Infrastructure](budget-fix-infrastructure.json) records
the exact images; [cleanup](budget-fix-cleanup.json) confirms that these three additional containers
were absent after execution. Together with the preceding cleanup report, all 18 temporary
verification/capacity containers were removed.

These runs used a shared development host; other verification processes ran during the soak.
Sampled peaks can miss short bursts, and these synthetic results are not a reviewed production
latency baseline or a global capacity claim. Existing earlier failed budget/soak observations remain
in [the historical evidence](../2026-09-05-python/README.md). The new failed paired report's original
SHA-256 is `0da1212703ea0f14dcdd688cb85cf92ca58ff3d3aeb54a4b8e3f1523629663c4`.

[Hosted run identity](paired-ci-run.json), [checks](paired-ci-checks.json), and
[paired infrastructure](paired-infrastructure.json) accompany that failure. Hosted CI at `13ffcb8`
had 23 successful checks; the paired job and `CI / Required` aggregate failed on the corpus mismatch,
and the separately scheduled hosted soak was skipped. The current Dart pin is
`cd740acab63d540b6e8cdb562426d9c96fcfd55c`. Matching VM/Chrome conformance, all 24 required recovery
scenarios and real-client reconnect/load evidence are still required. No merge, integrated Python
SHA, issue closure, release or tag is claimed. Issues #40, #43 and #34 remain open.

The [final runtime hosted run](budget-fix-ci-run.json) at `d964ad5` also had
[23 successful checks](budget-fix-ci-checks.json), including PostgreSQL 15–18. Its paired job and
Required aggregate failed on the same old-corpus mismatch; the hosted soak was skipped.
[That failed artifact](budget-fix-paired-failed.json) and its
[infrastructure identities](budget-fix-paired-infrastructure.json) are retained separately.

[The initial cleanup inspection](cleanup-inspection-initial.json) failed because its absence
matcher expected uppercase Docker error text. The final inspection also covers the six earlier precommit verification containers. Their
[initial integration infrastructure](preliminary-infrastructure.json) and
[initial verification infrastructure](preliminary-verification-infrastructure.json) records
are cleanup context, not committed-source acceptance. The final inspection retains the actual lowercase
`no such object` responses for all 15 names; the initial failed inspection is preserved.

`SHA256SUMS` covers every JSON report in this directory. Reports retain original source identity,
parameters and output paths; local temporary paths describe the execution and are not package bindings.
