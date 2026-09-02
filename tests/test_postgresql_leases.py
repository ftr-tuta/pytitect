from __future__ import annotations

import os
import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres


def test_two_workers_takeover_monotonic_token_and_stale_fence() -> None:
    dsn = os.environ.get("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    table = f"consumer_lease_{uuid.uuid4().hex}"
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with psycopg.connect(dsn, autocommit=True) as setup:
        setup.execute(
            f"""CREATE TABLE {table} (
                resource text PRIMARY KEY,
                owner text NOT NULL,
                fencing_token bigint NOT NULL,
                expires_at timestamptz NOT NULL,
                protected_value integer NOT NULL DEFAULT 0
            )"""
        )

    def acquire(owner: str, at: datetime) -> int | None:
        with psycopg.connect(dsn) as connection, connection.transaction():
            connection.execute(
                f"""INSERT INTO {table} (resource, owner, fencing_token, expires_at)
                VALUES ('job', %s, 0, %s)
                ON CONFLICT (resource) DO NOTHING""",
                (owner, at),
            )
            row = connection.execute(
                f"SELECT owner, fencing_token, expires_at FROM {table} "
                "WHERE resource = 'job' FOR UPDATE"
            ).fetchone()
            assert row is not None
            if row[2] > at and row[1] > 0:
                return None
            token = row[1] + 1
            connection.execute(
                f"UPDATE {table} SET owner=%s, fencing_token=%s, expires_at=%s "
                "WHERE resource='job'",
                (owner, token, at + timedelta(seconds=5)),
            )
            return token

    barrier = threading.Barrier(2)
    outcomes: list[int | None] = []

    def worker(owner: str) -> None:
        barrier.wait()
        outcomes.append(acquire(owner, now))

    workers = [threading.Thread(target=worker, args=(owner,)) for owner in ("a", "b")]
    for worker_thread in workers:
        worker_thread.start()
    for worker_thread in workers:
        worker_thread.join()
    assert sorted(outcome for outcome in outcomes if outcome is not None) == [1]
    assert outcomes.count(None) == 1

    takeover = acquire("c", now + timedelta(seconds=5))
    assert takeover == 2

    def fenced_mutation(token: int) -> bool:
        with psycopg.connect(dsn) as connection, connection.transaction():
            row = connection.execute(
                f"SELECT fencing_token FROM {table} WHERE resource='job' FOR UPDATE"
            ).fetchone()
            assert row is not None
            if row[0] != token:
                return False
            connection.execute(
                f"UPDATE {table} SET protected_value=protected_value+1 WHERE resource='job'"
            )
            return True

    assert not fenced_mutation(1)
    assert fenced_mutation(2)
    with psycopg.connect(dsn, autocommit=True) as check:
        assert check.execute(f"SELECT protected_value FROM {table}").fetchone() == (1,)
        check.execute(f"DROP TABLE {table}")
