# Python reliability capacity fixture

Run `uv run python tool/integration_environment.py --services postgres,nats -- uv run python -m
benchmarks.python.run --output /tmp/capacity.json` from the repository root (as one command).
The development-only dependencies include HTTPX and Uvicorn. No benchmark application enters the wheel.

The generator uses fixed workers and a bounded queue. Every offered request, generator rejection,
HTTP rejection and connection error enters the report. Saturation offers eight times the selected
base rate and introduces a real PostgreSQL `pg_sleep` in local transactions. Recovery kills and
restarts the HTTP/relay/consumer process on the same durable schema and JetStream stream. The
post-fault drain compares receipts, local effects, outbox and receiving inbox through new sessions.
Seeds, package versions, platform, source identity and parameters are recorded.

Use `--scenarios soak --duration 1800 --rate 20 --drain-timeout 120 --max-requests 100000`
for the scheduled finite run. The offered rate and bounded drain deadline are recorded choices,
not a guaranteed sustainable capacity. Failed gates exit unsuccessfully and retain their JSON
measurements (including pending durable work); unexpected scenario failures retain a failure entry.
The initial
assertions cover correctness and configured concurrency/task/connection bounds and a recorded
512 MiB process RSS budget (override with `--max-rss-mib`), with no invented
latency threshold. Reports retain latency percentiles, rejected/error categories, useful throughput,
backlog age, retries, sampled task/connection/lock-wait counts and process peak RSS. Sampling can miss
short peaks; these are measurements alongside the admission contracts, not a heap-capacity proof.

The fixture shares synthetic models with `tests/integration/support.py`; schemas and broker resources
belong only to that execution and are removed afterward. Run it on dedicated infrastructure to
establish a latency baseline. LocalStack is exercised separately and no real AWS resources are
provisioned by this runner. Paired Dart/client scenarios remain outside this Python delivery.
