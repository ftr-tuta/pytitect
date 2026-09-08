"""Exercise the published Django adapters using independent PostgreSQL connections."""

import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.postgres


def test_django_postgresql_stores_and_transaction_bridge() -> None:
    if not os.environ.get("TEST_POSTGRES_DSN"):
        pytest.fail("selected PostgreSQL integration requires TEST_POSTGRES_DSN")
    subprocess.run([sys.executable, "-m", "tests.integration.django_probe"], check=True, timeout=45)
