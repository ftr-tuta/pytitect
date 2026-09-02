from __future__ import annotations

import json
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from django.conf import settings

if not settings.configured:
    settings.configure(
        DEBUG=False,
        SECRET_KEY="test-only",
        USE_TZ=True,
        REST_FRAMEWORK={},
    )

import django
from rest_framework import serializers

django.setup()

from pytitect import Limits
from pytitect.django.transactions import DjangoTransactionBoundary
from pytitect.drf.fields import (
    BoundedJSONField,
    ClosedSerializer,
    StrictBooleanField,
    StrictCharField,
    StrictDecimalField,
    StrictIntegerField,
    StrictListField,
    StrictUUIDField,
)
from pytitect.drf.problems import make_exception_handler
from pytitect.http import ProblemRenderer, static_titles


def test_core_and_explicit_adapters_import_without_settings() -> None:
    root = Path(__file__).parents[1]
    code = "import pytitect, pytitect.django, pytitect.drf; print(pytitect.__version__)"
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0.9.0a1"


class PayloadSerializer(ClosedSerializer):
    name = StrictCharField()
    count = StrictIntegerField()
    active = StrictBooleanField()
    identifier = StrictUUIDField()
    amount = StrictDecimalField(max_digits=6, decimal_places=2)
    tags = StrictListField(child=StrictCharField(), max_length=2)
    data = BoundedJSONField(limits=Limits(max_body_bytes=64, max_json_depth=2))


class NestedSerializer(ClosedSerializer):
    payload = PayloadSerializer()


def test_strict_fields_accept_exact_json_types_and_reject_coercion() -> None:
    identifier = str(uuid.uuid4())
    valid = PayloadSerializer(
        data={
            "name": "test",
            "count": 2,
            "active": True,
            "identifier": identifier,
            "amount": "10.20",
            "tags": ["a"],
            "data": {"a": 1},
        }
    )
    assert valid.is_valid(), valid.errors
    assert valid.validated_data["amount"] == Decimal("10.20")
    invalid = PayloadSerializer(
        data={
            "name": 1,
            "count": "2",
            "active": 1,
            "identifier": uuid.UUID(identifier),
            "amount": 10.2,
            "tags": "a",
            "data": [[[1]]],
        }
    )
    assert not invalid.is_valid()
    assert set(invalid.errors) == {
        "name",
        "count",
        "active",
        "identifier",
        "amount",
        "tags",
        "data",
    }
    unknown = PayloadSerializer(data={"unknown": True})
    assert not unknown.is_valid() and set(unknown.errors) == {"unknown"}
    nested = NestedSerializer(
        data={
            "payload": {
                "name": "test",
                "count": 2,
                "active": True,
                "identifier": identifier,
                "amount": "10.20",
                "tags": ["a"],
                "data": {},
                "unknown": True,
            }
        }
    )
    assert not nested.is_valid()
    assert "unknown" in nested.errors["payload"]


def test_problem_handler_returns_problem_json() -> None:
    renderer = ProblemRenderer("https://errors.example/", static_titles({}))
    handler = make_exception_handler(renderer)
    response = handler(serializers.ValidationError({"field": ["bad"]}), {})
    assert response.status_code == 400
    assert response["Content-Type"] == "application/problem+json"
    assert response.data["type"].endswith("validation-error")


def test_django_transaction_boundary_uses_explicit_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, str]] = []

    class Atomic:
        def __enter__(self) -> None:
            seen.append(("enter", "events"))

        def __exit__(self, *args: object) -> None:
            seen.append(("exit", "events"))

    from django.db import transaction

    monkeypatch.setattr(transaction, "atomic", lambda *, using: Atomic())
    monkeypatch.setattr(
        transaction,
        "on_commit",
        lambda callback, *, using: (seen.append(("callback", using)), callback()),
    )
    boundary = DjangoTransactionBoundary("events")
    with boundary.atomic():
        pass
    boundary.on_commit(lambda: seen.append(("ran", "events")))
    assert seen == [
        ("enter", "events"),
        ("exit", "events"),
        ("callback", "events"),
        ("ran", "events"),
    ]
    assert "django" not in json.loads(json.dumps({"module": "pytitect"}))["module"]
