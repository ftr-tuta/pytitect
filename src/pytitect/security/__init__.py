"""Opt-in protocol security primitives."""

from pytitect.security.canonical import canonical_json, parse_ijson, validate_ijson
from pytitect.security.digest import (
    ContentDigestVerifier,
    RejectedContentDigest,
    VerifiedContentDigest,
)
from pytitect.security.dpop import DPoPVerifier
from pytitect.security.encoding import (
    access_token_hash,
    base64url_decode,
    base64url_encode,
    jwk_thumbprint,
)
from pytitect.security.outcomes import RejectedProof, VerifiedProof
from pytitect.security.replay import (
    InMemoryReplayStore,
    ReplayAccepted,
    ReplayCapacityExceeded,
    ReplayDetected,
    ReplayStore,
)
from pytitect.security.signatures import (
    BackendVerification,
    HttpMessageSignaturesBackend,
    HttpMessageSignatureVerifier,
    SignatureBackend,
    SignedRequest,
)

__all__ = [
    "BackendVerification",
    "ContentDigestVerifier",
    "DPoPVerifier",
    "HttpMessageSignatureVerifier",
    "HttpMessageSignaturesBackend",
    "InMemoryReplayStore",
    "RejectedContentDigest",
    "RejectedProof",
    "ReplayAccepted",
    "ReplayCapacityExceeded",
    "ReplayDetected",
    "ReplayStore",
    "SignatureBackend",
    "SignedRequest",
    "VerifiedContentDigest",
    "VerifiedProof",
    "access_token_hash",
    "base64url_decode",
    "base64url_encode",
    "canonical_json",
    "jwk_thumbprint",
    "parse_ijson",
    "validate_ijson",
]
