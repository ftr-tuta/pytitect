"""Preview bounded exact-token JSON; no implicit Python numeric conversions."""

from __future__ import annotations

import codecs
import json
import math
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType

from pytitect.core import JsonValue, Limits

__all__ = [
    "ExactNumber",
    "ExactValue",
    "WireDocument",
    "WireError",
    "WireIntegrityError",
    "WireLimitError",
    "WirePrecisionError",
    "WireProfileError",
    "WireShapeError",
    "WireSyntaxError",
    "decode_wire",
    "decode_wire_stream",
]


class WireError(ValueError):
    """A payload-free boundary failure. No input excerpt or offset is retained."""

    code = "wire"

    def __init__(self) -> None:
        super().__init__(self.code)


class WireSyntaxError(WireError):
    code = "syntax"


class WireLimitError(WireError):
    code = "limits"


class WireShapeError(WireError):
    code = "shape"


class WireProfileError(WireError):
    code = "unsupported_profile"


class WireIntegrityError(WireError):
    code = "integrity"


class WirePrecisionError(WireError):
    code = "precision"


_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z")
_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z")


@dataclass(frozen=True, slots=True)
class ExactNumber:
    """An original JSON number token; equality includes exponent spelling and zero sign."""

    token: str

    def __post_init__(self) -> None:
        if not isinstance(self.token, str) or not _NUMBER.fullmatch(self.token):
            raise WireSyntaxError()

    def to_int(self) -> int:
        """Convert integer tokens only, without a process-wide digit-limit change."""

        if not _INTEGER.fullmatch(self.token):
            raise WirePrecisionError()
        digits = self.token.removeprefix("-")
        result = 0
        for start in range(0, len(digits), 9):
            chunk = digits[start : start + 9]
            result = result * 10 ** len(chunk) + int(chunk)
        return -result if self.token.startswith("-") else result

    def to_decimal(self) -> Decimal:
        """Convert exactly when Decimal can represent the exponent."""

        try:
            result = Decimal(self.token)
            if result.is_finite():
                return result
        except InvalidOperation:
            pass
        raise WirePrecisionError()

    def to_float(self) -> float:
        """Reject nonfinite, rounded, overflowed, or underflowed binary64 conversions."""

        result = float(self.token)
        if not math.isfinite(result) or Decimal.from_float(result) != self.to_decimal():
            raise WirePrecisionError()
        return result


type ExactValue = (
    bool | str | ExactNumber | tuple[ExactValue, ...] | Mapping[str, ExactValue] | None
)


def _freeze(value: ExactValue, limits: Limits) -> ExactValue:
    count = 0

    def visit(item: ExactValue, depth: int) -> ExactValue:
        nonlocal count
        count += 1
        if count > limits.max_json_items or depth > limits.max_json_depth:
            raise WireLimitError()
        if item is None or isinstance(item, bool | ExactNumber):
            return item
        if isinstance(item, str):
            _string_limit(item, limits)
            return item
        if isinstance(item, tuple):
            return tuple(visit(child, depth + 1) for child in item)
        if isinstance(item, Mapping):
            result: dict[str, ExactValue] = {}
            for key, child in item.items():
                if not isinstance(key, str):
                    raise WireShapeError()
                _string_limit(key, limits)
                result[key] = visit(child, depth + 1)
            return MappingProxyType(result)
        raise WireShapeError()

    return visit(value, 0)


def _string_limit(value: str, limits: Limits) -> None:
    if len(value) > limits.max_string_length:
        raise WireLimitError()
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise WireSyntaxError()


def _pieces(value: ExactValue) -> Iterator[str]:
    if value is None:
        yield "null"
    elif isinstance(value, bool):
        yield "true" if value else "false"
    elif isinstance(value, ExactNumber):
        yield value.token
    elif isinstance(value, str):
        yield json.dumps(value, ensure_ascii=False)
    elif isinstance(value, tuple):
        yield "["
        for index, child in enumerate(value):
            if index:
                yield ","
            yield from _pieces(child)
        yield "]"
    else:
        yield "{"
        for index, key in enumerate(sorted(value)):
            if index:
                yield ","
            yield json.dumps(key, ensure_ascii=False)
            yield ":"
            yield from _pieces(value[key])
        yield "}"


@dataclass(frozen=True, slots=True, init=False)
class WireDocument:
    """Deeply immutable JSON document with finite deterministic UTF-8 encoding."""

    value: ExactValue
    limits: Limits

    def __init__(self, value: ExactValue, *, limits: Limits | None = None) -> None:
        selected = limits or Limits()
        object.__setattr__(self, "value", _freeze(value, selected))
        object.__setattr__(self, "limits", selected)
        self.encode()

    def encode(self) -> bytes:
        output = bytearray()
        for piece in _pieces(self.value):
            # Bound even a directly constructed giant numeric token before encoding it.
            if len(piece) > self.limits.max_body_bytes - len(output):
                raise WireLimitError()
            encoded = piece.encode("utf-8")
            if len(encoded) > self.limits.max_body_bytes - len(output):
                raise WireLimitError()
            output.extend(encoded)
        return bytes(output)

    def to_json(self) -> JsonValue:
        """Checked conversion: integer tokens to int, other numbers to exact binary64."""

        return _ordinary(self.value, checked=True)


def _ordinary(value: ExactValue, *, checked: bool) -> JsonValue:
    if isinstance(value, ExactNumber):
        if _INTEGER.fullmatch(value.token):
            return value.to_int()
        if checked:
            return value.to_float()
        result = float(value.token)
        if not math.isfinite(result):
            raise WirePrecisionError()
        return result
    if isinstance(value, tuple):
        return [_ordinary(item, checked=checked) for item in value]
    if isinstance(value, Mapping):
        return {key: _ordinary(item, checked=checked) for key, item in value.items()}
    return value


def _integer_token(value: int) -> str:
    """Base-ten rendering independent from CPython's process-wide conversion setting."""

    if value == 0:
        return "0"
    negative = value < 0
    remaining = abs(value)
    chunks: list[int] = []
    while remaining:
        remaining, chunk = divmod(remaining, 1_000_000_000)
        chunks.append(chunk)
    return (
        ("-" if negative else "")
        + str(chunks[-1])
        + "".join(f"{chunk:09d}" for chunk in reversed(chunks[:-1]))
    )


def _legacy_value(value: JsonValue) -> ExactValue:
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        return ExactNumber(_integer_token(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise WirePrecisionError()
        return ExactNumber(json.dumps(value, allow_nan=False))
    if isinstance(value, list):
        return tuple(_legacy_value(item) for item in value)
    return {key: _legacy_value(item) for key, item in value.items()}


class _Parser:
    def __init__(self, chunks: Iterable[bytes], limits: Limits) -> None:
        self._characters = self._decode(chunks, limits.max_body_bytes)
        self._lookahead: str | None = None
        self._limits = limits
        self._count = 0

    @staticmethod
    def _decode(chunks: Iterable[bytes], maximum: int) -> Iterator[str]:
        decoder = codecs.getincrementaldecoder("utf-8")("strict")
        size = 0
        for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise WireShapeError()
            size += len(chunk)
            if size > maximum:
                raise WireLimitError()
            yield from decoder.decode(chunk)
        yield from decoder.decode(b"", final=True)

    def peek(self) -> str:
        if self._lookahead is None:
            self._lookahead = next(self._characters, "")
        return self._lookahead

    def take(self) -> str:
        value = self.peek()
        self._lookahead = None
        return value

    def whitespace(self) -> None:
        while self.peek() and self.peek() in " \t\r\n":
            self.take()

    def require(self, expected: str) -> None:
        if self.take() != expected:
            raise WireSyntaxError()

    def string(self) -> str:
        self.require('"')
        characters: list[str] = []
        escapes = {
            '"': '"',
            "\\": "\\",
            "/": "/",
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
        }
        while True:
            character = self.take()
            if character == '"':
                return "".join(characters)
            if not character or ord(character) < 0x20:
                raise WireSyntaxError()
            if character == "\\":
                escape = self.take()
                if escape == "u":
                    code = self.hex_quad()
                    if 0xD800 <= code <= 0xDBFF:
                        self.require("\\")
                        self.require("u")
                        low = self.hex_quad()
                        if not 0xDC00 <= low <= 0xDFFF:
                            raise WireSyntaxError()
                        code = 0x10000 + ((code - 0xD800) << 10) + low - 0xDC00
                    elif 0xDC00 <= code <= 0xDFFF:
                        raise WireSyntaxError()
                    character = chr(code)
                elif escape in escapes:
                    character = escapes[escape]
                else:
                    raise WireSyntaxError()
            if len(characters) >= self._limits.max_string_length:
                raise WireLimitError()
            characters.append(character)

    def hex_quad(self) -> int:
        digits = "".join(self.take() for _ in range(4))
        if len(digits) != 4 or any(digit not in "0123456789abcdefABCDEF" for digit in digits):
            raise WireSyntaxError()
        return int(digits, 16)

    def value(self, depth: int = 0) -> ExactValue:
        self._count += 1
        if self._count > self._limits.max_json_items or depth > self._limits.max_json_depth:
            raise WireLimitError()
        self.whitespace()
        first = self.peek()
        if first == '"':
            return self.string()
        if first in ("{", "["):
            self.take()
            self.whitespace()
            close = "}" if first == "{" else "]"
            mapping: dict[str, ExactValue] = {}
            sequence: list[ExactValue] = []
            if self.peek() != close:
                while True:
                    if first == "{":
                        key = self.string()
                        self.whitespace()
                        self.require(":")
                        # Every value is charged before replacing a duplicate key.
                        mapping[key] = self.value(depth + 1)
                    else:
                        sequence.append(self.value(depth + 1))
                    self.whitespace()
                    if self.peek() == close:
                        break
                    self.require(",")
                    self.whitespace()
            self.require(close)
            return MappingProxyType(mapping) if first == "{" else tuple(sequence)
        for literal, value in (("null", None), ("true", True), ("false", False)):
            if first == literal[0]:
                for character in literal:
                    self.require(character)
                return value
        if first and first in "-0123456789":
            token: list[str] = []
            while self.peek() and self.peek() in "-+0123456789.eE":
                token.append(self.take())
            return ExactNumber("".join(token))
        raise WireSyntaxError()


def decode_wire(payload: bytes, *, limits: Limits | None = None) -> WireDocument:
    """Parse actual UTF-8 bytes, rejecting BOMs, invalid scalars and trailing input."""

    return decode_wire_stream((payload,), limits=limits)


def decode_wire_stream(chunks: Iterable[bytes], *, limits: Limits | None = None) -> WireDocument:
    """Incrementally parse a borrowed stream; never close it or suppress cancellation."""

    selected = limits or Limits()
    parser = _Parser(chunks, selected)
    try:
        value = parser.value()
        parser.whitespace()
        if parser.peek():
            raise WireSyntaxError()
        return WireDocument(value, limits=selected)
    except UnicodeError:
        failure: WireError = WireSyntaxError()
    except RecursionError:
        failure = WireLimitError()
    # Raise outside the exception handler to avoid retaining decoder input in __context__.
    raise failure
