"""Canonical base64url and JOSE thumbprint helpers."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping

from pytitect.core import JsonValue
from pytitect.security.canonical import canonical_json


def base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def base64url_decode(value: str) -> bytes:
    if not value or "=" in value:
        raise ValueError("base64url input must be non-empty and unpadded")
    try:
        decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, UnicodeEncodeError) as error:
        raise ValueError("malformed base64url value") from error
    if base64url_encode(decoded) != value:
        raise ValueError("base64url value is not canonical")
    return decoded


def jwk_thumbprint(jwk: Mapping[str, JsonValue]) -> str:
    key_type = jwk.get("kty")
    if not isinstance(key_type, str):
        raise ValueError("JWK key type must be a string")
    required = {
        "EC": ("crv", "kty", "x", "y"),
        "RSA": ("e", "kty", "n"),
        "OKP": ("crv", "kty", "x"),
        "oct": ("k", "kty"),
    }.get(key_type)
    if required is None:
        raise ValueError("unsupported JWK key type")
    if any(not isinstance(jwk.get(name), str) for name in required):
        raise ValueError("JWK is missing required string members")
    selected: JsonValue = {name: jwk[name] for name in required}
    return base64url_encode(hashlib.sha256(canonical_json(selected)).digest())


def access_token_hash(access_token: str) -> str:
    if not access_token:
        raise ValueError("access token must not be empty")
    return base64url_encode(hashlib.sha256(access_token.encode()).digest())
