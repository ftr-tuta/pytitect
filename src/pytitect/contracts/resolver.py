"""Bounded resolver for JSON References that are strictly local to a root."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Never, cast
from urllib.parse import unquote, urlsplit

from pytitect.core import JsonValue, validate_json


@dataclass(frozen=True, slots=True)
class ResolverLimits:
    max_depth: int = 32
    max_references: int = 1_000
    max_total_bytes: int = 8_388_608

    def __post_init__(self) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (self.max_depth, self.max_references, self.max_total_bytes)
        ):
            raise ValueError("resolver limits must be positive finite integers")


@dataclass(frozen=True, slots=True)
class ResolvedDocument:
    value: JsonValue
    references: int
    bytes_read: int


@dataclass(frozen=True, slots=True)
class RefRejected:
    code: str
    detail: str
    reference: str | None = None


type ResolveResult = ResolvedDocument | RefRejected


class _ExpectedFailure(Exception):
    def __init__(self, result: RefRejected) -> None:
        self.result = result


class LocalRefResolver:
    """Resolve internal and local-file refs without network or root escape."""

    def __init__(self, root: Path, *, limits: ResolverLimits | None = None) -> None:
        self._root = root.resolve(strict=True)
        if not self._root.is_dir():
            raise ValueError("resolver root must be a directory")
        self._limits = limits or ResolverLimits()

    def resolve(self, document: Path) -> ResolveResult:
        try:
            initial = self._safe_path(document, base=self._root)
            state = _ResolutionState(self, initial)
            raw = state.load(initial)
            value = state.walk(raw, current_file=initial, depth=0, stack=())
            return ResolvedDocument(value, state.references, state.bytes_read)
        except _ExpectedFailure as failure:
            return failure.result

    def _safe_path(self, value: str | Path, *, base: Path) -> Path:
        raw = str(value)
        parsed = urlsplit(raw)
        if parsed.scheme or parsed.netloc:
            raise _ExpectedFailure(
                RefRejected("network_ref", "network references are forbidden", raw)
            )
        path = Path(unquote(parsed.path))
        if path.is_absolute():
            raise _ExpectedFailure(
                RefRejected("absolute_path", "absolute paths are forbidden", raw)
            )
        try:
            candidate = (base / path).resolve(strict=True)
        except (FileNotFoundError, OSError):
            raise _ExpectedFailure(
                RefRejected("missing_file", "referenced file does not exist", raw)
            ) from None
        if not candidate.is_relative_to(self._root) or not candidate.is_file():
            raise _ExpectedFailure(
                RefRejected("root_escape", "reference escapes resolver root", raw)
            )
        return candidate


class _ResolutionState:
    def __init__(self, resolver: LocalRefResolver, initial: Path) -> None:
        self.resolver = resolver
        self.initial = initial
        self.references = 0
        self.bytes_read = 0
        self.cache: dict[Path, JsonValue] = {}

    def load(self, path: Path) -> JsonValue:
        cached = self.cache.get(path)
        if cached is not None:
            return cached
        payload = path.read_bytes()
        self.bytes_read += len(payload)
        if self.bytes_read > self.resolver._limits.max_total_bytes:
            self.reject("byte_limit", "referenced documents exceed max_total_bytes")
        try:
            if path.suffix.lower() == ".json":
                loaded: object = json.loads(
                    payload,
                    object_pairs_hook=_unique_object,
                    parse_constant=_invalid_number,
                )
            elif path.suffix.lower() in {".yaml", ".yml"}:
                try:
                    import yaml
                except ImportError:
                    self.reject("yaml_unavailable", "install pytitect[contracts] to resolve YAML")
                try:
                    loaded = yaml.load(payload, Loader=_unique_yaml_loader(yaml))
                except yaml.YAMLError as error:
                    self.reject("malformed_document", str(error))
            else:
                self.reject("unsupported_format", "only JSON and YAML files are supported")
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
            self.reject("malformed_document", str(error))
        if not isinstance(loaded, (dict, list, str, int, float, bool)) and loaded is not None:
            self.reject("invalid_document", "document is not JSON-compatible")
        result = cast(JsonValue, loaded)
        try:
            validate_json(result)
        except ValueError as error:
            self.reject("invalid_document", str(error))
        self.cache[path] = result
        return result

    def walk(
        self,
        value: JsonValue,
        *,
        current_file: Path,
        depth: int,
        stack: tuple[tuple[Path, str], ...],
    ) -> JsonValue:
        if depth > self.resolver._limits.max_depth:
            self.reject("depth_limit", "reference depth exceeds max_depth")
        if isinstance(value, list):
            return [
                self.walk(child, current_file=current_file, depth=depth, stack=stack)
                for child in value
            ]
        if not isinstance(value, dict):
            return value
        reference = value.get("$ref")
        if not isinstance(reference, str):
            return {
                key: self.walk(child, current_file=current_file, depth=depth, stack=stack)
                for key, child in value.items()
            }
        self.references += 1
        if self.references > self.resolver._limits.max_references:
            self.reject("reference_limit", "reference count exceeds max_references", reference)
        target_file, pointer = self.target(reference, current_file=current_file)
        marker = (target_file, pointer)
        if marker in stack:
            self.reject("cycle", "cyclic reference detected", reference)
        target = self.pointer(self.load(target_file), pointer, reference)
        resolved = self.walk(
            target,
            current_file=target_file,
            depth=depth + 1,
            stack=(*stack, marker),
        )
        siblings = {key: child for key, child in value.items() if key != "$ref"}
        if not siblings:
            return resolved
        walked_siblings = self.walk(siblings, current_file=current_file, depth=depth, stack=stack)
        if not isinstance(walked_siblings, dict):
            self.reject("invalid_siblings", "$ref siblings must form an object", reference)
        if "allOf" in walked_siblings:
            self.reject(
                "incompatible_siblings",
                "$ref siblings must not provide their own allOf composition",
                reference,
            )
        return {"allOf": [resolved, walked_siblings]}

    def target(self, reference: str, *, current_file: Path) -> tuple[Path, str]:
        parsed = urlsplit(reference)
        if parsed.scheme or parsed.netloc or parsed.query:
            self.reject("network_ref", "non-local references are forbidden", reference)
        if parsed.path:
            target = self.resolver._safe_path(parsed.path, base=current_file.parent)
        else:
            target = current_file
        return target, unquote(parsed.fragment)

    def pointer(self, document: JsonValue, pointer: str, reference: str) -> JsonValue:
        if not pointer:
            return document
        if not pointer.startswith("/"):
            self.reject("invalid_pointer", "JSON Pointer must start with '/'", reference)
        current: JsonValue = document
        for raw_token in pointer[1:].split("/"):
            token = self.decode_pointer_token(raw_token, reference)
            if isinstance(current, dict) and token in current:
                current = current[token]
            elif isinstance(current, list):
                try:
                    if token != "0" and (
                        not token
                        or token[0] == "0"
                        or any(character < "0" or character > "9" for character in token)
                    ):
                        raise ValueError
                    current = current[int(token)]
                except (ValueError, IndexError):
                    self.reject("missing_pointer", "JSON Pointer does not exist", reference)
            else:
                self.reject("missing_pointer", "JSON Pointer does not exist", reference)
        return current

    def decode_pointer_token(self, token: str, reference: str) -> str:
        output: list[str] = []
        index = 0
        while index < len(token):
            if token[index] != "~":
                output.append(token[index])
                index += 1
                continue
            if index + 1 >= len(token) or token[index + 1] not in {"0", "1"}:
                self.reject("invalid_pointer", "JSON Pointer contains an invalid escape", reference)
            output.append("~" if token[index + 1] == "0" else "/")
            index += 2
        return "".join(output)

    def reject(self, code: str, detail: str, reference: str | None = None) -> Never:
        raise _ExpectedFailure(RefRejected(code, detail, reference))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"duplicate JSON member: {key}")
        output[key] = value
    return output


def _invalid_number(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _unique_yaml_loader(yaml: Any) -> Any:
    safe_loader = yaml.SafeLoader

    class UniqueSafeLoader(safe_loader):  # type: ignore[misc, valid-type]
        pass

    def construct_mapping(loader: object, node: object, deep: bool = False) -> object:
        mapping: dict[object, object] = {}
        for key_node, value_node in node.value:  # type: ignore[attr-defined]
            key = loader.construct_object(key_node, deep=deep)  # type: ignore[attr-defined]
            if key in mapping:
                raise ValueError(f"duplicate YAML key: {key}")
            mapping[key] = loader.construct_object(value_node, deep=deep)  # type: ignore[attr-defined]
        return mapping

    UniqueSafeLoader.add_constructor("tag:yaml.org,2002:map", construct_mapping)
    return UniqueSafeLoader
