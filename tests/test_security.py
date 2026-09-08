from __future__ import annotations

import base64
import hashlib
import uuid
from datetime import timedelta
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from http_message_signatures import HTTPMessageSigner, HTTPSignatureKeyResolver
from http_message_signatures.algorithms import HMAC_SHA256

from pytitect.security import (
    ContentDigestVerifier,
    DPoPVerifier,
    HttpMessageSignatureVerifier,
    InMemoryReplayStore,
    RejectedContentDigest,
    RejectedProof,
    SignedRequest,
    VerifiedContentDigest,
    VerifiedProof,
    access_token_hash,
    base64url_decode,
    base64url_encode,
    canonical_json,
    parse_ijson,
    validate_ijson,
)
from pytitect.security.encoding import jwk_thumbprint
from pytitect.security.replay import ReplayAccepted, ReplayCapacityExceeded, ReplayDetected
from pytitect.security.signatures import BackendVerification


def test_canonical_json_ijson_base64url_and_hashes() -> None:
    assert canonical_json({"b": 1, "a": 2}) == b'{"a":2,"b":1}'
    assert parse_ijson('{"ok":1}') == {"ok": 1}
    with pytest.raises(ValueError):
        parse_ijson('{"same":1,"same":2}')
    for invalid in (float("nan"), 9_007_199_254_740_992, "\ud800"):
        with pytest.raises(ValueError):
            validate_ijson(invalid)  # type: ignore[arg-type]
    encoded = base64url_encode(b"canonical")
    assert base64url_decode(encoded) == b"canonical"
    with pytest.raises(ValueError):
        base64url_decode(encoded + "=")
    assert access_token_hash("token") == base64url_encode(hashlib.sha256(b"token").digest())


def test_replay_store_atomic_decisions_and_capacity() -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    store = InMemoryReplayStore(capacity=1)
    assert isinstance(
        store.reserve("proof", "one", now=clock.now(), ttl=timedelta(seconds=1)), ReplayAccepted
    )
    assert isinstance(
        store.reserve("proof", "one", now=clock.now(), ttl=timedelta(seconds=1)), ReplayDetected
    )
    assert isinstance(
        store.reserve("proof", "two", now=clock.now(), ttl=timedelta(seconds=1)),
        ReplayCapacityExceeded,
    )
    clock.advance(timedelta(seconds=1))
    assert isinstance(
        store.reserve("proof", "two", now=clock.now(), ttl=timedelta(seconds=1)), ReplayAccepted
    )


def _public_jwk(private_key: ec.EllipticCurvePrivateKey) -> dict[str, str]:
    numbers = private_key.public_key().public_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": base64url_encode(numbers.x.to_bytes(32, "big")),
        "y": base64url_encode(numbers.y.to_bytes(32, "big")),
    }


def test_dpop_real_es256_context_and_replay() -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_jwk = _public_jwk(private_key)
    token = "access-token"
    claims = {
        "jti": str(uuid.uuid4()),
        "htm": "POST",
        "htu": "https://api.example/orders?ignored=yes",
        "iat": int(clock.now().timestamp()),
        "ath": access_token_hash(token),
        "nonce": "server-nonce",
    }
    proof = jwt.encode(
        claims,
        private_key,
        algorithm="ES256",
        headers={"typ": "dpop+jwt", "jwk": public_jwk},
    )
    verifier = DPoPVerifier(InMemoryReplayStore(), clock=clock)
    verified = verifier.verify(
        proof,
        method="POST",
        target_uri="https://api.example:443/orders?another=query",
        access_token=token,
        expected_nonce="server-nonce",
    )
    assert isinstance(verified, VerifiedProof)
    assert verified.key_id == jwk_thumbprint(public_jwk)
    assert isinstance(
        verifier.verify(
            proof,
            method="POST",
            target_uri="https://api.example/orders",
            access_token=token,
            expected_nonce="server-nonce",
        ),
        RejectedProof,
    )
    fresh = jwt.encode(
        {**claims, "jti": str(uuid.uuid4())},
        private_key,
        algorithm="ES256",
        headers={"typ": "dpop+jwt", "jwk": public_jwk},
    )
    rejected = verifier.verify(fresh, method="GET", target_uri="https://api.example/orders")
    assert isinstance(rejected, RejectedProof) and rejected.code == "method_mismatch"
    assert isinstance(
        verifier.verify("not-a-jwt", method="GET", target_uri="https://api.example/"),
        RejectedProof,
    )

    duplicate_claims = (
        base64url_encode(
            b'{"typ":"dpop+jwt","alg":"ES256","jwk":' + canonical_json(public_jwk) + b"}"
        )
        + "."
        + base64url_encode(
            b'{"jti":"00000000-0000-4000-8000-000000000000","jti":"duplicate",'
            b'"htm":"GET","htu":"https://api.example/","iat":0}'
        )
        + "."
        + base64url_encode(b"invalid-signature")
    )
    duplicate = verifier.verify(duplicate_claims, method="GET", target_uri="https://api.example/")
    assert isinstance(duplicate, RejectedProof) and duplicate.code == "malformed_proof"

    old = jwt.encode(
        {
            **claims,
            "jti": str(uuid.uuid4()),
            "iat": int((clock.now() - timedelta(hours=1)).timestamp()),
        },
        private_key,
        algorithm="ES256",
        headers={"typ": "dpop+jwt", "jwk": public_jwk},
    )
    stale = verifier.verify(
        old,
        method="POST",
        target_uri="https://api.example/orders",
        access_token=token,
        expected_nonce="server-nonce",
    )
    assert isinstance(stale, RejectedProof) and stale.code == "stale_proof"


def test_content_digest_known_sha256_and_limits() -> None:
    body = b"hello"
    header = f"sha-256=:{base64.b64encode(hashlib.sha256(body).digest()).decode()}:"
    verifier = ContentDigestVerifier(max_body_bytes=5)
    assert isinstance(verifier.verify(body, header), VerifiedContentDigest)
    assert isinstance(verifier.verify(b"other", header), RejectedContentDigest)
    assert isinstance(verifier.verify(b"too-big", header), RejectedContentDigest)
    assert isinstance(verifier.verify(body, None), RejectedContentDigest)


class HmacResolver(HTTPSignatureKeyResolver):
    secret = b"0123456789abcdef0123456789abcdef"

    def resolve_public_key(self, key_id: str) -> bytes:
        assert key_id == "test-key"
        return self.secret

    def resolve_private_key(self, key_id: str) -> bytes:
        assert key_id == "test-key"
        return self.secret


def test_http_message_signature_real_crypto_policy_and_replay() -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    body = b'{"ok":true}'
    digest = base64.b64encode(hashlib.sha256(body).digest()).decode()
    request = SignedRequest(
        "POST",
        "https://api.example/tasks",
        {"Content-Digest": f"sha-256=:{digest}:", "Content-Type": "application/json"},
        body,
    )
    signer = HTTPMessageSigner(signature_algorithm=HMAC_SHA256, key_resolver=HmacResolver())
    signer.sign(
        request,
        key_id="test-key",
        created=clock.now(),
        expires=clock.now() + timedelta(minutes=1),
        nonce="nonce-1",
        tag="application",
        covered_component_ids=("@method", "@target-uri", "content-digest"),
    )
    verifier = HttpMessageSignatureVerifier(
        key_resolver=lambda key_id: HmacResolver().resolve_public_key(key_id),
        replay_store=InMemoryReplayStore(),
        required_components=frozenset({"@method", "@target-uri", "content-digest"}),
        expected_tag="application",
        allowed_algorithms=frozenset({"hmac-sha256"}),
        expected_nonce="nonce-1",
        clock=clock,
    )
    assert isinstance(verifier.verify(request), VerifiedProof)
    replay = verifier.verify(request)
    assert isinstance(replay, RejectedProof) and replay.code == "replayed_signature"

    bad = SignedRequest("POST", request.target_uri, dict(request.headers), b"changed")
    rejected = verifier.verify(bad)
    assert isinstance(rejected, RejectedProof) and rejected.code == "digest_mismatch"


class FixedBackend:
    def __init__(self, verification: BackendVerification) -> None:
        self.verification = verification

    def verify(self, *args: Any, **kwargs: Any) -> BackendVerification:
        return self.verification


def test_http_message_signature_rejects_policy_boundaries() -> None:
    from tests.conftest import ManualClock

    clock = ManualClock()
    body = b"body"
    digest = base64.b64encode(hashlib.sha256(body).digest()).decode()
    request = SignedRequest(
        "POST",
        "https://api.example/",
        {
            "signature-input": "test=()",
            "signature": "test=:AA==:",
            "content-digest": f"sha-256=:{digest}:",
        },
        body,
    )

    def result(**changes: Any) -> BackendVerification:
        parameters: dict[str, object] = {
            "keyid": "key",
            "created": int(clock.now().timestamp()),
            "expires": int((clock.now() + timedelta(minutes=1)).timestamp()),
            "nonce": "nonce",
            "tag": "application",
        }
        parameters.update(changes.pop("parameters", {}))
        return BackendVerification(
            "test",
            changes.pop("algorithm", "hmac-sha256"),
            changes.pop(
                "covered_components",
                frozenset({"@method", "@target-uri", "content-digest"}),
            ),
            parameters,
        )

    def verify(verification: BackendVerification) -> RejectedProof:
        verifier = HttpMessageSignatureVerifier(
            key_resolver=lambda key_id: key_id,
            replay_store=InMemoryReplayStore(),
            required_components=frozenset({"@method", "@target-uri", "content-digest"}),
            expected_tag="application",
            allowed_algorithms=frozenset({"hmac-sha256"}),
            backend=FixedBackend(verification),
            clock=clock,
        )
        outcome = verifier.verify(request)
        assert isinstance(outcome, RejectedProof)
        return outcome

    assert verify(result(covered_components=frozenset({"@method"}))).code == "missing_components"
    assert verify(result(algorithm="ed25519")).code == "algorithm_rejected"
    assert verify(result(parameters={"created": "now"})).code == "missing_created"
    assert (
        verify(
            result(parameters={"expires": int((clock.now() + timedelta(hours=1)).timestamp())})
        ).code
        == "lifetime_exceeded"
    )
