"""Bounded authenticated opaque cursors with explicit context binding."""

from __future__ import annotations

import hmac
import json
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol, cast

from pytitect.core import JsonValue
from pytitect.security import base64url_decode, base64url_encode
from pytitect.security.canonical import canonical_json, parse_ijson


class CursorAlgorithm(StrEnum):
    HS256 = "HS256"
    A256GCM = "A256GCM"


class CursorKeyResolver(Protocol):
    def __call__(self, kid: str, algorithm: CursorAlgorithm) -> bytes | None: ...


@dataclass(frozen=True, slots=True)
class CursorLimits:
    max_token_bytes: int = 16_384
    max_payload_bytes: int = 8_192
    max_context_length: int = 255

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive finite integer")


@dataclass(frozen=True, slots=True)
class CursorDecoded:
    payload: bytes
    kid: str
    expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class CursorRejected:
    code: str
    detail: str


type CursorDecodeResult = CursorDecoded | CursorRejected


class OpaqueCursorCodec:
    """Encode/decode a v1 ``protected.body.auth`` cursor envelope."""

    def __init__(
        self,
        key_resolver: CursorKeyResolver | Mapping[str, bytes],
        *,
        limits: CursorLimits | None = None,
        nonce_factory: Callable[[int], bytes] = secrets.token_bytes,
    ) -> None:
        if isinstance(key_resolver, Mapping):
            copied = dict(key_resolver)
            self._key_resolver: CursorKeyResolver = lambda kid, algorithm: copied.get(kid)
        else:
            self._key_resolver = key_resolver
        self._limits = limits or CursorLimits()
        self._nonce_factory = nonce_factory

    def encode(
        self,
        payload: bytes,
        *,
        dataset: str,
        partition: str,
        kid: str,
        algorithm: CursorAlgorithm | str = CursorAlgorithm.HS256,
        expires_at: datetime | None = None,
    ) -> str:
        selected = _algorithm(algorithm)
        _context(dataset, partition, kid, self._limits)
        if len(payload) > self._limits.max_payload_bytes:
            raise ValueError("cursor payload exceeds max_payload_bytes")
        header: dict[str, JsonValue] = {
            "alg": selected.value,
            "dataset": dataset,
            "kid": kid,
            "partition": partition,
            "v": 1,
        }
        if expires_at is not None:
            _utc(expires_at, "cursor expiration")
            header["exp"] = int(expires_at.timestamp())
        key = self._resolve_key(kid, selected)
        if selected is CursorAlgorithm.HS256:
            _hmac_key(key)
            protected = base64url_encode(canonical_json(header))
            body = base64url_encode(payload)
            auth = base64url_encode(hmac.digest(key, f"{protected}.{body}".encode(), "sha256"))
        else:
            _aes_key(key)
            nonce = self._nonce_factory(12)
            if len(nonce) != 12:
                raise ValueError("A256GCM nonce_factory must return exactly 12 bytes")
            header["nonce"] = base64url_encode(nonce)
            protected_bytes = canonical_json(header)
            protected = base64url_encode(protected_bytes)
            encrypted = _aesgcm(key).encrypt(nonce, payload, protected_bytes)
            body = base64url_encode(encrypted[:-16])
            auth = base64url_encode(encrypted[-16:])
        token = f"{protected}.{body}.{auth}"
        if len(token.encode()) > self._limits.max_token_bytes:
            raise ValueError("cursor exceeds max_token_bytes")
        return token

    def decode(
        self,
        token: str,
        *,
        dataset: str,
        partition: str,
        now: datetime | None = None,
    ) -> CursorDecodeResult:
        if len(token.encode()) > self._limits.max_token_bytes:
            return CursorRejected("cursor_too_large", "cursor exceeds max_token_bytes")
        try:
            protected, body, auth = token.split(".")
            protected_bytes = base64url_decode(protected)
            header_value = parse_ijson(protected_bytes)
            if not isinstance(header_value, dict):
                return CursorRejected("invalid_header", "cursor header must be an object")
            header = header_value
            if canonical_json(header) != protected_bytes:
                return CursorRejected("noncanonical_header", "cursor header is not RFC 8785")
            parsed = self._header(header, dataset=dataset, partition=partition)
            if isinstance(parsed, CursorRejected):
                return parsed
            algorithm, kid, expires_at, nonce = parsed
            key = self._key_resolver(kid, algorithm)
            if key is None:
                return CursorRejected("unknown_key", "cursor key identifier is unknown")
            body_bytes = base64url_decode(body)
            auth_bytes = base64url_decode(auth)
            if algorithm is CursorAlgorithm.HS256:
                _hmac_key(key)
                expected = hmac.digest(key, f"{protected}.{body}".encode(), "sha256")
                if not hmac.compare_digest(expected, auth_bytes):
                    return CursorRejected("invalid_auth", "cursor authentication failed")
                payload = body_bytes
            else:
                _aes_key(key)
                if nonce is None or len(auth_bytes) != 16:
                    return CursorRejected("invalid_nonce", "A256GCM cursor parameters are invalid")
                try:
                    payload = _aesgcm(key).decrypt(nonce, body_bytes + auth_bytes, protected_bytes)
                except Exception:
                    return CursorRejected("invalid_auth", "cursor authentication failed")
            if len(payload) > self._limits.max_payload_bytes:
                return CursorRejected("payload_too_large", "cursor payload exceeds its limit")
            selected_now = now or datetime.now(UTC)
            _utc(selected_now, "cursor comparison time")
            if expires_at is not None and selected_now >= expires_at:
                return CursorRejected("expired", "cursor has expired")
            return CursorDecoded(payload, kid, expires_at)
        except (UnicodeError, ValueError, json.JSONDecodeError):
            return CursorRejected("malformed", "cursor envelope is malformed")

    def _resolve_key(self, kid: str, algorithm: CursorAlgorithm) -> bytes:
        key = self._key_resolver(kid, algorithm)
        if key is None:
            raise ValueError("cursor key identifier is unknown")
        return key

    def _header(
        self,
        header: dict[str, JsonValue],
        *,
        dataset: str,
        partition: str,
    ) -> tuple[CursorAlgorithm, str, datetime | None, bytes | None] | CursorRejected:
        allowed = {"alg", "dataset", "exp", "kid", "nonce", "partition", "v"}
        if set(header) - allowed:
            return CursorRejected("invalid_header", "cursor header contains unknown members")
        if header.get("v") != 1:
            return CursorRejected("unsupported_version", "cursor version is not supported")
        try:
            algorithm = _algorithm(cast(str, header.get("alg")))
        except ValueError:
            return CursorRejected("unsupported_algorithm", "cursor algorithm is not supported")
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            return CursorRejected("invalid_header", "cursor kid is invalid")
        for value in (kid, header.get("dataset"), header.get("partition")):
            if (
                not isinstance(value, str)
                or not value
                or value != value.strip()
                or len(value) > self._limits.max_context_length
            ):
                return CursorRejected("invalid_header", "cursor context is invalid")
        if header.get("dataset") != dataset or header.get("partition") != partition:
            return CursorRejected("context_mismatch", "cursor dataset or partition does not match")
        expires_at: datetime | None = None
        exp = header.get("exp")
        if exp is not None:
            if isinstance(exp, bool) or not isinstance(exp, int):
                return CursorRejected("invalid_header", "cursor expiration is invalid")
            try:
                expires_at = datetime.fromtimestamp(exp, UTC)
            except (OverflowError, OSError, ValueError):
                return CursorRejected("invalid_header", "cursor expiration is invalid")
        encoded_nonce = header.get("nonce")
        nonce: bytes | None = None
        if algorithm is CursorAlgorithm.A256GCM:
            if not isinstance(encoded_nonce, str):
                return CursorRejected("invalid_nonce", "A256GCM cursor nonce is absent")
            nonce = base64url_decode(encoded_nonce)
            if len(nonce) != 12:
                return CursorRejected("invalid_nonce", "A256GCM cursor nonce must be 96 bits")
        elif encoded_nonce is not None:
            return CursorRejected("invalid_header", "HS256 cursor must not contain a nonce")
        return algorithm, kid, expires_at, nonce


def _algorithm(value: CursorAlgorithm | str) -> CursorAlgorithm:
    try:
        return CursorAlgorithm(value)
    except (TypeError, ValueError) as error:
        raise ValueError("algorithm must be HS256 or A256GCM") from error


def _context(dataset: str, partition: str, kid: str, limits: CursorLimits) -> None:
    for name, value in (("dataset", dataset), ("partition", partition), ("kid", kid)):
        if not value or value != value.strip() or len(value) > limits.max_context_length:
            raise ValueError(f"cursor {name} is invalid")


def _utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _hmac_key(key: bytes) -> None:
    if len(key) < 32:
        raise ValueError("HS256 cursor keys must contain at least 32 bytes")


def _aes_key(key: bytes) -> None:
    if len(key) != 32:
        raise ValueError("A256GCM cursor keys must contain exactly 32 bytes")


def _aesgcm(key: bytes):  # type: ignore[no-untyped-def]
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as error:
        raise RuntimeError("install pytitect[sync] for A256GCM cursors") from error
    return AESGCM(key)
