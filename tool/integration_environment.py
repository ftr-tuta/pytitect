#!/usr/bin/env python3
"""Run a command with isolated local PostgreSQL, JetStream and LocalStack services.

Requires Docker and the selected Python extras. Only containers created by this
invocation are removed; no volumes, networks or existing services are pruned.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.request import urlopen


def docker(*args: str, timeout: int = 180) -> str:
    return subprocess.check_output(["docker", *args], text=True, timeout=timeout).strip()


def healthy(service: str, endpoint: str) -> None:
    if service == "postgres":
        import psycopg

        with psycopg.connect(endpoint, connect_timeout=2) as connection:
            assert connection.execute("SELECT 1").fetchone() == (1,)
    elif service == "nats":
        import nats

        async def check() -> None:
            client = await nats.connect(endpoint, connect_timeout=2, allow_reconnect=False)
            try:
                await client.jetstream().account_info()
            finally:
                await client.close()

        asyncio.run(check())
    else:
        with urlopen(endpoint + "/_localstack/health", timeout=2) as response:
            health = json.load(response)
        if not all(
            health["services"].get(name) in {"available", "running"} for name in ("events", "sqs")
        ):
            raise RuntimeError("LocalStack services are not ready")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--services", default="postgres,nats,localstack")
    parser.add_argument("--postgres-version", default="16", choices=["15", "16", "17", "18"])
    parser.add_argument("--record", type=Path)
    parser.add_argument("--command-timeout", type=int, default=2700)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not 1 <= args.command_timeout <= 7200:
        parser.error("command timeout must be between 1 and 7200 seconds")
    services = args.services.split(",")
    if set(services) - {"postgres", "nats", "localstack"}:
        parser.error("unknown service")
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("a command is required after --")
    specs = {
        "postgres": (
            f"postgres:{args.postgres_version}",
            5432,
            "TEST_POSTGRES_DSN",
            ["-e", "POSTGRES_PASSWORD=pytitect", "-e", "POSTGRES_DB=pytitect"],
            [
                "-c",
                "max_connections=50",
                "-c",
                "statement_timeout=15000",
                "-c",
                "lock_timeout=10000",
            ],
        ),
        "nats": ("nats:2.14.5", 4222, "TEST_NATS_URL", [], ["-js"]),
        "localstack": (
            "localstack/localstack:4.14.0",
            4566,
            "LOCALSTACK_ENDPOINT",
            ["-e", "SERVICES=events,sqs"],
            [],
        ),
    }
    run_id = uuid.uuid4().hex
    created, evidence, env = [], {}, os.environ.copy()
    try:
        for service in services:
            image, port, variable, options, arguments = specs[service]
            name = f"pytitect-{run_id}-{service}"
            # Register the exact generated name before creation, so a partial failure is cleaned up.
            created.append(name)
            docker(
                "run",
                "-d",
                "--name",
                name,
                "--label",
                f"pytitect.test={run_id}",
                "-p",
                f"127.0.0.1::{port}",
                *options,
                image,
                *arguments,
            )
            endpoint = docker("port", name, f"{port}/tcp", timeout=10)
            endpoint = (
                "postgresql://postgres:pytitect@" + endpoint + "/pytitect"
                if service == "postgres"
                else ("nats://" if service == "nats" else "http://") + endpoint
            )
            deadline, last_error = time.monotonic() + 90, None
            while time.monotonic() < deadline:
                try:
                    healthy(service, endpoint)
                    break
                except Exception as error:
                    last_error = type(error).__name__
                    time.sleep(0.25)
            else:
                raise RuntimeError(f"{service} health deadline exceeded: {last_error}")
            env[variable] = endpoint
            evidence[service] = {
                "image": image,
                "image_id": docker("inspect", "--format", "{{.Image}}", name, timeout=10),
            }
            print(f"{service}: healthy ({image})", flush=True)
        if args.record:
            args.record.parent.mkdir(parents=True, exist_ok=True)
            args.record.write_text(
                json.dumps({"run_id": run_id, "services": evidence, "command": command}, indent=2)
                + "\n"
            )
        return subprocess.run(
            command, env=env, check=False, timeout=args.command_timeout
        ).returncode
    finally:
        for name in reversed(created):
            result = subprocess.run(
                ["docker", "rm", "-f", name],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            if result.returncode and "No such container" not in result.stderr:
                print(f"cleanup failed for {name}: {result.stderr}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
