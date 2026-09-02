"""Strict ES256 DPoP proof verification with atomic replay control."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import cast
from urllib.parse import SplitResult, urlsplit, urlunsplit

from pytitect.core import Clock, JsonValue, SystemClock
from pytitect.security.canonical import parse_ijson
from pytitect.security.encoding import access_token_hash, base64url_decode, jwk_thumbprint
from pytitect.security.outcomes import RejectedProof, VerifiedProof
from pytitect.security.replay import ReplayAccepted, ReplayStore

type DPoPResult = VerifiedProof | RejectedProof
_HEADER_FIELDS = frozenset({"typ", "alg", "jwk"})
_CLAIM_FIELDS = frozenset({"jti", "htm", "htu", "iat", "ath", "nonce"})


@dataclass(frozen=True, slots=True)
class DPoPVerifier:
    replay_store: ReplayStore
    clock: Clock = field(default_factory=SystemClock)
    max_age: timedelta = timedelta(minutes=5)
    clock_skew: timedelta = timedelta(seconds=30)
    replay_ttl: timedelta = timedelta(minutes=10)

    def __post_init__(self) -> None:
        if self.max_age <= timedelta(0) or self.replay_ttl <= timedelta(0):
            raise ValueError("DPoP age and replay TTL must be positive")
        if self.clock_skew < timedelta(0):
            raise ValueError("DPoP clock skew must not be negative")

    def verify(
        self,
        proof: str,
        *,
        method: str,
        target_uri: str,
        access_token: str | None = None,
        expected_nonce: str | None = None,
    ) -> DPoPResult:
        try:
            parts = proof.split(".")
            if len(parts) != 3 or any(not part for part in parts):
                return RejectedProof("malformed_proof", "DPoP proof must be a compact JWS")
            header_raw = base64url_decode(parts[0])
            claims_raw = base64url_decode(parts[1])
            base64url_decode(parts[2])
            header_value = parse_ijson(header_raw)
            claims_value = parse_ijson(claims_raw)
            if not isinstance(header_value, dict) or not isinstance(claims_value, dict):
                return RejectedProof("malformed_proof", "JOSE header and claims must be objects")
            header = header_value
            claims = claims_value
            if set(header) != _HEADER_FIELDS:
                return RejectedProof("invalid_header", "DPoP JOSE header members are closed")
            if not set(claims).issubset(_CLAIM_FIELDS) or not {
                "jti",
                "htm",
                "htu",
                "iat",
            }.issubset(claims):
                return RejectedProof("invalid_claims", "DPoP claims are missing or not allowed")
            if header.get("typ") != "dpop+jwt" or header.get("alg") != "ES256":
                return RejectedProof("unsupported_proof", "only typ=dpop+jwt and ES256 are allowed")
            jwk_value = header.get("jwk")
            if not isinstance(jwk_value, dict):
                return RejectedProof("invalid_jwk", "DPoP requires an embedded public JWK")
            jwk = jwk_value
            if jwk.get("kty") != "EC" or jwk.get("crv") != "P-256" or "d" in jwk:
                return RejectedProof("invalid_jwk", "DPoP requires a public P-256 EC JWK")
            signature_rejection = self._verify_signature(proof, jwk)
            if signature_rejection is not None:
                return signature_rejection
            context_rejection = self._verify_context(
                claims,
                method=method,
                target_uri=target_uri,
                access_token=access_token,
                expected_nonce=expected_nonce,
            )
            if context_rejection is not None:
                return context_rejection
            jti = cast(str, claims["jti"])
            issued_at = datetime.fromtimestamp(cast(int, claims["iat"]), UTC)
            thumbprint = jwk_thumbprint(cast(Mapping[str, JsonValue], jwk))
            replay = self.replay_store.reserve(
                "dpop",
                f"{thumbprint}:{jti}",
                now=self.clock.now(),
                ttl=self.replay_ttl,
            )
            if not isinstance(replay, ReplayAccepted):
                return RejectedProof(
                    "replayed_proof", "DPoP proof was already used or cannot be reserved"
                )
            return VerifiedProof(
                mechanism="dpop",
                key_id=thumbprint,
                proof_id=jti,
                issued_at=issued_at,
                attributes={"nonce": cast(str, claims.get("nonce")) if "nonce" in claims else None},
            )
        except (ValueError, TypeError, KeyError, OverflowError, UnicodeError, json.JSONDecodeError):
            return RejectedProof("malformed_proof", "DPoP proof is malformed")

    def _verify_signature(self, proof: str, jwk: Mapping[str, JsonValue]) -> RejectedProof | None:
        try:
            import jwt

            key = jwt.PyJWK.from_dict(dict(jwk))
            jwt.decode(
                proof,
                key=key,
                algorithms=["ES256"],
                options={
                    "verify_aud": False,
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                    "require": [],
                },
            )
        except ImportError as error:
            raise RuntimeError("install pytitect[dpop] to verify DPoP") from error
        except Exception:
            return RejectedProof("invalid_signature", "DPoP signature verification failed")
        return None

    def _verify_context(
        self,
        claims: Mapping[str, JsonValue],
        *,
        method: str,
        target_uri: str,
        access_token: str | None,
        expected_nonce: str | None,
    ) -> RejectedProof | None:
        jti = claims.get("jti")
        htm = claims.get("htm")
        htu = claims.get("htu")
        iat = claims.get("iat")
        if not isinstance(jti, str) or not _is_uuid4(jti):
            return RejectedProof("invalid_jti", "jti must be a canonical UUID-v4")
        if not isinstance(htm, str) or htm != method.upper():
            return RejectedProof("method_mismatch", "htm does not match the request method")
        if not isinstance(htu, str):
            return RejectedProof("uri_mismatch", "htu must be a URI string")
        try:
            if _normalize_htu(htu) != _normalize_htu(target_uri):
                return RejectedProof("uri_mismatch", "htu does not match the request target")
        except ValueError:
            return RejectedProof(
                "uri_mismatch", "htu or request target is not an absolute HTTP URI"
            )
        if isinstance(iat, bool) or not isinstance(iat, int):
            return RejectedProof("invalid_iat", "iat must be an integer NumericDate")
        now = self.clock.now()
        issued_at = datetime.fromtimestamp(iat, UTC)
        if issued_at > now + self.clock_skew or issued_at < now - self.max_age - self.clock_skew:
            return RejectedProof("stale_proof", "iat is outside the accepted time window")
        ath = claims.get("ath")
        if access_token is not None:
            if not isinstance(ath, str) or ath != access_token_hash(access_token):
                return RejectedProof("ath_mismatch", "ath does not match the access token")
        elif ath is not None:
            return RejectedProof("unexpected_ath", "ath is not allowed without an access token")
        nonce = claims.get("nonce")
        if expected_nonce is not None and nonce != expected_nonce:
            return RejectedProof("nonce_mismatch", "nonce does not match the expected value")
        if nonce is not None and not isinstance(nonce, str):
            return RejectedProof("invalid_nonce", "nonce must be a string")
        return None


def _is_uuid4(value: str) -> bool:
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return parsed.version == 4 and str(parsed) == value


def _normalize_htu(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname or parsed.username:
        raise ValueError("not an absolute HTTP URI")
    port = parsed.port
    default_port = 80 if parsed.scheme.lower() == "http" else 443
    authority = parsed.hostname.lower()
    if ":" in authority and not authority.startswith("["):
        authority = f"[{authority}]"
    if port is not None and port != default_port:
        authority = f"{authority}:{port}"
    normalized = SplitResult(parsed.scheme.lower(), authority, parsed.path or "/", "", "")
    return urlunsplit(normalized)
