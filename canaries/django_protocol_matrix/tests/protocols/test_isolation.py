import pytest
from django.urls import reverse
from rest_framework.test import APIClient

from pytitect_protocol_matrix.legacy.models import LegacyMutation
from pytitect_protocol_matrix.mobile_v2.models import DomainMutation, IdempotencyRecord

pytestmark = pytest.mark.django_db(transaction=True)

MOBILE = {
    "HTTP_AUTHORIZATION": "DPoP synthetic-mobile-token",
    "HTTP_DPOP": "synthetic-dpop-proof",
    "HTTP_X_PROTOCOL_VERSION": "2",
}
ERP = {
    "HTTP_SIGNATURE_INPUT": (
        'sig1=("@method" "@path");keyid="synthetic-erp";alg="hmac-sha256"'
    ),
    "HTTP_SIGNATURE": "sig1=:cHl0aXRlY3QtY2FuYXJ5:",
    "HTTP_X_PROTOCOL_VERSION": "2",
}


def test_credentials_are_isolated_and_v2_failures_never_reach_legacy() -> None:
    client = APIClient()
    legacy_url = reverse("legacy:mutation")
    mobile_url = reverse("mobile_v2:mutation")
    erp_url = reverse("erp_v2:mutation")

    assert client.post(
        legacy_url, {"request_id": "one", "value": 1}, format="json", **MOBILE
    ).status_code in {
        401,
        403,
    }
    assert client.post(mobile_url, {"value": 1}, format="json", **ERP).status_code in {401, 403}
    assert client.post(erp_url, {"value": 1}, format="json", **MOBILE).status_code in {401, 403}
    assert LegacyMutation.objects.count() == 0
    assert DomainMutation.objects.count() == 0


def test_version_and_proof_are_checked_before_reservation() -> None:
    client = APIClient()
    url = reverse("mobile_v2:mutation")
    invalid_version = {**MOBILE, "HTTP_X_PROTOCOL_VERSION": "1"}
    response = client.post(
        url, {"value": 1}, format="json", HTTP_IDEMPOTENCY_KEY="same", **invalid_version
    )
    assert response.status_code == 400
    assert IdempotencyRecord.objects.count() == 0

    invalid_proof = {**MOBILE, "HTTP_DPOP": "invalid"}
    response = client.post(
        url, {"value": 1}, format="json", HTTP_IDEMPOTENCY_KEY="same", **invalid_proof
    )
    assert response.status_code in {401, 403}
    assert IdempotencyRecord.objects.count() == 0


def test_replay_conflict_and_receipt_lookup() -> None:
    client = APIClient()
    mutation_url = reverse("mobile_v2:mutation")
    first = client.post(
        mutation_url,
        {"value": 1},
        format="json",
        HTTP_IDEMPOTENCY_KEY="stable-key",
        **MOBILE,
    )
    assert first.status_code == 201, first.data
    replay = client.post(
        mutation_url,
        {"value": 1},
        format="json",
        HTTP_IDEMPOTENCY_KEY="stable-key",
        **MOBILE,
    )
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert DomainMutation.objects.count() == 1

    conflict = client.post(
        mutation_url,
        {"value": 2},
        format="json",
        HTTP_IDEMPOTENCY_KEY="stable-key",
        **MOBILE,
    )
    assert conflict.status_code == 409
    receipt_url = reverse("mobile_v2:receipt", args=[first.json()["receipt_id"]])
    receipt = client.get(receipt_url, **MOBILE)
    assert receipt.status_code == 200
    assert receipt.json()["state"] == "completed"
