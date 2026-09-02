"""Policy wrapper around the specialized RFC 9421 verification library."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from pytitect.core import Clock, SystemClock
from pytitect.security.digest import ContentDigestVerifier, RejectedContentDigest
from pytitect.security.outcomes import RejectedProof, VerifiedProof
from pytitect.security.replay import ReplayAccepted, ReplayStore

type HttpSignatureResult = VerifiedProof | RejectedProof
type KeyResolver = Callable[[str], object]


@dataclass(frozen=True, slots=True)
class SignedRequest:
    method: str
    target_uri: str
    headers: Mapping[str, str]
    body: bytes = b""

    @property
    def url(self) -> str:
        return self.target_uri


@dataclass(frozen=True, slots=True)
class BackendVerification:
    label: str
    algorithm: str
    covered_components: frozenset[str]
    parameters: Mapping[str, object]


class SignatureBackend(Protocol):
    def verify(
        self,
        request: SignedRequest,
        *,
        key_resolver: KeyResolver,
        allowed_algorithms: frozenset[str],
        expected_tag: str,
    ) -> BackendVerification: ...


class HttpMessageSignaturesBackend:
    """Cryptographic backend powered by ``http-message-signatures`` 2.x."""

    def verify(
        self,
        request: SignedRequest,
        *,
        key_resolver: KeyResolver,
        allowed_algorithms: frozenset[str],
        expected_tag: str,
    ) -> BackendVerification:
        try:
            from http_message_signatures import (  # type: ignore[attr-defined]
                HTTPMessageVerifier,
                HTTPSignatureKeyResolver,
            )
            from http_message_signatures.algorithms import (  # type: ignore[attr-defined]
                signature_algorithms,
            )
            from http_message_signatures.exceptions import InvalidSignature
            from http_message_signatures.structures import CaseInsensitiveDict
        except ImportError as error:
            raise RuntimeError(
                "install pytitect[signed-http] to verify HTTP Message Signatures"
            ) from error

        class Resolver(HTTPSignatureKeyResolver):
            def resolve_public_key(self, key_id: str) -> object:
                return key_resolver(key_id)

        class CryptographicVerifier(HTTPMessageVerifier):
            def validate_created_and_expires(self, sig_input: Any, max_age: Any = None) -> None:
                del sig_input, max_age  # Time policy is checked with the injected Pytitect clock.

        @dataclass(slots=True)
        class BackendRequest:
            method: str
            url: str
            headers: object

        backend_request = BackendRequest(
            request.method,
            request.target_uri,
            CaseInsensitiveDict(request.headers),  # type: ignore[no-untyped-call]
        )

        failures: list[Exception] = []
        for algorithm_id in sorted(allowed_algorithms):
            algorithm = signature_algorithms.get(algorithm_id)
            if algorithm is None:
                failures.append(ValueError(f"unsupported backend algorithm: {algorithm_id}"))
                continue
            try:
                results = CryptographicVerifier(
                    signature_algorithm=algorithm,
                    key_resolver=Resolver(),
                ).verify(backend_request, expect_tag=expected_tag)
                if len(results) != 1:
                    raise InvalidSignature("exactly one matching signature is required")
                result = results[0]
                components = frozenset(_component_name(key) for key in result.covered_components)
                return BackendVerification(
                    label=str(result.label),
                    algorithm=str(result.algorithm.algorithm_id),
                    covered_components=components,
                    parameters=dict(result.parameters),
                )
            except Exception as error:
                failures.append(error)
        detail = str(failures[-1]) if failures else "no allowed signature algorithm"
        raise ValueError(detail)


@dataclass(frozen=True, slots=True)
class HttpMessageSignatureVerifier:
    key_resolver: KeyResolver
    replay_store: ReplayStore
    required_components: frozenset[str]
    expected_tag: str
    allowed_algorithms: frozenset[str]
    backend: SignatureBackend = field(default_factory=HttpMessageSignaturesBackend)
    clock: Clock = field(default_factory=SystemClock)
    max_age: timedelta = timedelta(minutes=5)
    max_lifetime: timedelta = timedelta(minutes=5)
    clock_skew: timedelta = timedelta(seconds=30)
    replay_ttl: timedelta = timedelta(minutes=10)
    expected_nonce: str | None = None
    require_content_digest: bool = True
    content_digest_verifier: ContentDigestVerifier = field(default_factory=ContentDigestVerifier)

    def __post_init__(self) -> None:
        if not self.required_components or not self.expected_tag or not self.allowed_algorithms:
            raise ValueError("components, tag, and allowed algorithms must not be empty")
        if self.max_age <= timedelta(0) or self.max_lifetime <= timedelta(0):
            raise ValueError("signature age and lifetime must be positive")
        if self.clock_skew < timedelta(0) or self.replay_ttl <= timedelta(0):
            raise ValueError("signature skew and replay TTL are invalid")

    def verify(self, request: SignedRequest) -> HttpSignatureResult:
        headers = {name.lower(): value for name, value in request.headers.items()}
        if "signature-input" not in headers or "signature" not in headers:
            return RejectedProof("missing_signature", "Signature-Input and Signature are required")
        if self.require_content_digest:
            digest_result = self.content_digest_verifier.verify(
                request.body, headers.get("content-digest")
            )
            if isinstance(digest_result, RejectedContentDigest):
                return RejectedProof(digest_result.code, digest_result.detail)
            if "content-digest" not in self.required_components:
                return RejectedProof(
                    "unsigned_digest", "content-digest must be a required covered component"
                )
        normalized_request = SignedRequest(
            request.method,
            request.target_uri,
            headers,
            request.body,
        )
        try:
            verified = self.backend.verify(
                normalized_request,
                key_resolver=self.key_resolver,
                allowed_algorithms=self.allowed_algorithms,
                expected_tag=self.expected_tag,
            )
        except RuntimeError:
            raise
        except Exception:
            return RejectedProof("invalid_signature", "HTTP Message Signature verification failed")
        if verified.algorithm not in self.allowed_algorithms:
            return RejectedProof("algorithm_rejected", "signature algorithm is not allowed")
        missing = self.required_components - verified.covered_components
        if missing:
            return RejectedProof("missing_components", "signature omits required components")
        params = verified.parameters
        key_id = params.get("keyid")
        created = params.get("created")
        expires = params.get("expires")
        nonce = params.get("nonce")
        tag = params.get("tag")
        if not isinstance(key_id, str) or not key_id:
            return RejectedProof("missing_keyid", "signature keyid is required")
        if isinstance(created, bool) or not isinstance(created, int):
            return RejectedProof("missing_created", "integer created parameter is required")
        if isinstance(expires, bool) or not isinstance(expires, int):
            return RejectedProof("missing_expires", "integer expires parameter is required")
        if tag != self.expected_tag:
            return RejectedProof("tag_mismatch", "signature tag does not match")
        if not isinstance(nonce, str) or not nonce:
            return RejectedProof("missing_nonce", "a non-empty signature nonce is required")
        if self.expected_nonce is not None and nonce != self.expected_nonce:
            return RejectedProof("nonce_mismatch", "signature nonce does not match")
        issued_at = datetime.fromtimestamp(created, UTC)
        expires_at = datetime.fromtimestamp(expires, UTC)
        now = self.clock.now()
        if issued_at > now + self.clock_skew or issued_at < now - self.max_age - self.clock_skew:
            return RejectedProof("stale_signature", "created is outside the accepted time window")
        if expires_at < now - self.clock_skew or expires_at <= issued_at:
            return RejectedProof("expired_signature", "expires is invalid or in the past")
        if expires_at - issued_at > self.max_lifetime:
            return RejectedProof("lifetime_exceeded", "signature lifetime exceeds policy")
        proof_id = hashlib.sha256(
            f"{verified.label}\0{key_id}\0{nonce}\0{headers['signature']}".encode()
        ).hexdigest()
        replay = self.replay_store.reserve(
            "http-message-signature",
            proof_id,
            now=now,
            ttl=self.replay_ttl,
        )
        if not isinstance(replay, ReplayAccepted):
            return RejectedProof(
                "replayed_signature", "signature was already used or cannot be reserved"
            )
        return VerifiedProof(
            mechanism="http-message-signature",
            key_id=key_id,
            proof_id=proof_id,
            issued_at=issued_at,
            expires_at=expires_at,
            attributes={"algorithm": verified.algorithm, "tag": self.expected_tag},
        )


def _component_name(value: object) -> str:
    text = str(value)
    if text == '"@signature-params"':
        return "@signature-params"
    if text.startswith('"'):
        end = text.find('"', 1)
        if end > 0:
            return text[1:end]
    return text
