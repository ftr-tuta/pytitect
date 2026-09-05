"""Finite offered-load, saturation, crash-recovery and soak measurements.

Every offered request, rejection and error enters the report. Gates assert durable
correctness and finite resources; latency thresholds await reviewed repeatable baselines.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
import platform
import random
import socket
import subprocess
import sys
import time
import uuid
from collections import Counter
from contextlib import suppress
from pathlib import Path

import httpx
from sqlalchemy import func, select
from tests.integration.support import Database, Effect, Inbox, Outbox, ReceiptRow
from tests.integration.test_brokers import connect_nats


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


async def scenario(args, name):
    generator = random.Random(args.seed)
    offered_rate = args.rate * (8 if name == "saturation" else 1)
    async with Database() as db:
        broker = await connect_nats()
        js, subject = broker.jetstream(), "pytitect_" + uuid.uuid4().hex
        created = False
        server = None
        server_log = Path(args.output).with_suffix(f".{name}.server.log")
        server_log.parent.mkdir(parents=True, exist_ok=True)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]

        async def start():
            log = server_log.open("ab")
            try:
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "benchmarks.python.service",
                    "--schema",
                    db.schema,
                    "--subject",
                    subject,
                    "--port",
                    str(port),
                    "--db-delay",
                    "0.03" if name in ("saturation", "recovery") else "0",
                    stdout=log,
                    stderr=log,
                )
            finally:
                log.close()
            try:
                async with httpx.AsyncClient(timeout=1) as client:
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        if process.returncode is not None:
                            raise RuntimeError(f"HTTP fixture exited; see {server_log}")
                        try:
                            result = await client.get(f"http://127.0.0.1:{port}/metrics")
                            if result.status_code == 200:
                                return process
                        except httpx.HTTPError:
                            pass
                        await asyncio.sleep(0.05)
                raise TimeoutError("HTTP fixture readiness deadline exceeded")
            except BaseException:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
                raise

        try:
            await js.add_stream(
                name=subject,
                subjects=[subject],
                max_msgs=args.max_requests * 2,
                max_bytes=64 * 1024 * 1024,
            )
            created = True
            server = await start()
            statuses = Counter()
            latencies = []
            accepted = []
            peaks = Counter()
            count = min(args.max_requests, max(1, int(args.duration * offered_rate)))
            queue = asyncio.Queue(args.concurrency * 2)
            started = time.monotonic()
            stop = asyncio.Event()
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                timeout=3,
                limits=httpx.Limits(
                    max_connections=args.concurrency + 2,
                    max_keepalive_connections=args.concurrency + 2,
                ),
            ) as client:

                async def worker():
                    while True:
                        item = await queue.get()
                        try:
                            if item is None:
                                return
                            identity, offered = item
                            try:
                                response = await client.post(
                                    "/operations",
                                    headers={"idempotency-key": identity},
                                    json={"value": 1},
                                )
                                statuses[str(response.status_code)] += 1
                                if response.status_code in (200, 201):
                                    accepted.append(identity)
                            except httpx.HTTPError as error:
                                statuses[type(error).__name__] += 1
                            latencies.append(time.monotonic() - offered)
                        finally:
                            queue.task_done()

                async def sample():
                    while not stop.is_set():
                        try:
                            response = await client.get("/metrics")
                            if response.status_code == 200:
                                metrics = response.json()
                                assert metrics["background_ok"], "fixture background task failed"
                                for key, value in metrics.items():
                                    if isinstance(value, (int, float)):
                                        peaks[key] = max(peaks[key], value)
                        except httpx.HTTPError:
                            pass
                        with suppress(TimeoutError):
                            await asyncio.wait_for(stop.wait(), 0.1)

                async def offer():
                    for index in range(count):
                        due = started + index / offered_rate
                        await asyncio.sleep(max(0, due - time.monotonic()))
                        identity = f"s{args.seed}-{index}-{generator.randrange(1000000)}"
                        try:
                            queue.put_nowait((identity, time.monotonic()))
                        except asyncio.QueueFull:
                            statuses["generator_rejected"] += 1
                            latencies.append(0.0)
                    for _ in range(args.concurrency):
                        await queue.put(None)

                async def crash():
                    nonlocal server
                    if name == "recovery":
                        await asyncio.sleep(args.duration / 2)
                        server.kill()
                        await server.wait()
                        await asyncio.sleep(0.2)
                        server = await start()

                async with asyncio.TaskGroup() as group:
                    monitor = group.create_task(sample())
                    for _ in range(args.concurrency):
                        group.create_task(worker())
                    offer_task = group.create_task(offer())
                    crash_task = group.create_task(crash())
                    await offer_task
                    await queue.join()
                    await crash_task
                    elapsed = time.monotonic() - started
                    # Bounded recovery drain; examine durable facts through new connections.
                    recovery_started = time.monotonic()
                    deadline = recovery_started + 20
                    while True:
                        async with db.sessions() as session:
                            pending = await session.scalar(
                                select(func.count())
                                .select_from(Outbox)
                                .where(Outbox.delivered_at.is_(None))
                            )
                            committed = await session.scalar(
                                select(func.count()).select_from(ReceiptRow)
                            )
                            useful = await session.scalar(
                                select(func.count())
                                .select_from(Inbox)
                                .where(Inbox.completed_at.is_not(None))
                            )
                        if pending == 0 and committed == useful:
                            break
                        if time.monotonic() >= deadline:
                            raise AssertionError(
                                f"recovery drain failed: pending={pending}, "
                                f"committed={committed}, useful={useful}"
                            )
                        await asyncio.sleep(0.05)
                    recovery_seconds = time.monotonic() - recovery_started
                    for identity in accepted[:10]:
                        assert (await client.get("/reconciliation/" + identity)).status_code == 200
                    stop.set()
                    await monitor
            async with db.sessions() as session:
                effects = await session.scalar(select(func.count()).select_from(Effect))
                rows = await session.scalar(select(func.count()).select_from(Outbox))
                retries = await session.scalar(select(func.coalesce(func.sum(Outbox.attempt), 0)))
            assert sum(statuses.values()) == count
            assert effects == 2 * committed and rows == committed == useful
            assert (
                peaks["connections"] <= 8
                and peaks["tasks"] <= 100
                and peaks["active_requests"] <= 8
                and 0 < peaks["rss_kib"] <= args.max_rss_mib * 1024
            )
            return {
                "scenario": name,
                "seed": args.seed,
                "offered": count,
                "statuses": dict(statuses),
                "duration_seconds": elapsed,
                "useful_operations": useful,
                "useful_throughput": useful / (elapsed + recovery_seconds),
                "latency_seconds": {
                    "p50": percentile(latencies, 0.5),
                    "p95": percentile(latencies, 0.95),
                    "p99": percentile(latencies, 0.99),
                    "max": max(latencies, default=0),
                },
                "peak_observations": dict(peaks),
                "recovery_seconds": recovery_seconds,
                "durable": {
                    "receipts": committed,
                    "effects": effects,
                    "outbox": rows,
                    "inbox": useful,
                    "publication_retries": int(retries),
                },
                "load_generator": {
                    "workers": args.concurrency,
                    "queue_capacity": args.concurrency * 2,
                    "offered_rate": offered_rate,
                },
            }
        finally:
            if server is not None and server.returncode is None:
                server.terminate()
                try:
                    await asyncio.wait_for(server.wait(), 5)
                except TimeoutError:
                    server.kill()
                    await server.wait()
            if created:
                await js.delete_stream(subject)
            await broker.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", default="offered,saturation,recovery")
    parser.add_argument("--duration", type=float, default=2)
    parser.add_argument("--rate", type=float, default=50)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--max-requests", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=41)
    parser.add_argument("--max-rss-mib", type=int, default=512)
    parser.add_argument("--output", type=Path, default=Path("/tmp/pytitect-capacity.json"))
    args = parser.parse_args()
    if (
        not 0 < args.duration <= 3600
        or not 0 < args.rate <= 100000
        or not 1 <= args.concurrency <= 64
        or not 1 <= args.max_requests <= 1000000
        or not 1 <= args.max_rss_mib <= 1048576
    ):
        parser.error("finite positive duration, rate, concurrency and request limits are required")
    names = args.scenarios.split(",")
    if set(names) - {"offered", "saturation", "recovery", "soak"}:
        parser.error("unknown scenario")
    results = [asyncio.run(scenario(args, name)) for name in names]
    document = {
        "scope": "Python synthetic fixture; no paired client or global capacity claim",
        "seed": args.seed,
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "source_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"])
        ).hexdigest(),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "versions": {
            name: importlib.metadata.version(name)
            for name in (
                "pytitect",
                "SQLAlchemy",
                "psycopg",
                "nats-py",
                "fastapi",
                "uvicorn",
                "httpx",
            )
        },
        "parameters": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2) + "\n")
    print(f"Capacity correctness and resource gates passed: {args.output}")


if __name__ == "__main__":
    main()
