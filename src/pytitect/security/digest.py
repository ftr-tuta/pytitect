"""RFC 9530 Content-Digest verification for SHA-256."""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
from dataclasses import dataclass

_ITEM = re.compile(r"\s*(?P<algorithm>[a-z0-9_-]+)=:(?P<digest>[A-Za-z0-9+/]+={0,2}):\s*", re.I)


@dataclass(frozen=True, slots=True)
class VerifiedContentDigest:
    algorithm: str = "sha-256"


@dataclass(frozen=True, slots=True)
class RejectedContentDigest:
    code: str
    detail: str


type ContentDigestResult = VerifiedContentDigest | RejectedContentDigest


@dataclass(frozen=True, slots=True)
class ContentDigestVerifier:
    max_body_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        if self.max_body_bytes <= 0:
            raise ValueError("max_body_bytes must be positive")

    def verify(self, body: bytes, header: str | None) -> ContentDigestResult:
        if len(body) > self.max_body_bytes:
            return RejectedContentDigest("body_too_large", "body exceeds configured limit")
        if header is None:
            return RejectedContentDigest("missing_digest", "Content-Digest is required")
        digests: dict[str, str] = {}
        for item in header.split(","):
            match = _ITEM.fullmatch(item)
            if match is None:
                return RejectedContentDigest(
                    "malformed_digest", "Content-Digest is not a structured digest dictionary"
                )
            algorithm = match.group("algorithm").lower()
            if algorithm in digests:
                return RejectedContentDigest(
                    "malformed_digest", "Content-Digest contains a duplicate algorithm"
                )
            digests[algorithm] = match.group("digest")
        encoded = digests.get("sha-256")
        if encoded is None:
            return RejectedContentDigest("unsupported_digest", "a valid sha-256 digest is required")
        try:
            expected = base64.b64decode(encoded, validate=True)
        except ValueError:
            return RejectedContentDigest("malformed_digest", "digest is not canonical base64")
        if base64.b64encode(expected).decode() != encoded or len(expected) != 32:
            return RejectedContentDigest("malformed_digest", "digest is not canonical SHA-256")
        actual = hashlib.sha256(body).digest()
        if not hmac.compare_digest(actual, expected):
            return RejectedContentDigest("digest_mismatch", "body does not match Content-Digest")
        return VerifiedContentDigest()
