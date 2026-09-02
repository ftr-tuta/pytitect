"""Deterministic manifests for local contract files and trees."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


@dataclass(frozen=True, order=True, slots=True)
class ManifestEntry:
    path: str
    sha256: str
    size: int


@dataclass(frozen=True, slots=True)
class ContractManifest:
    entries: tuple[ManifestEntry, ...]

    @classmethod
    def from_tree(
        cls, root: Path, *, patterns: tuple[str, ...] = ("*.json", "*.yaml", "*.yml")
    ) -> ContractManifest:
        resolved_root = root.resolve(strict=True)
        paths: set[Path] = set()
        for pattern in patterns:
            paths.update(path for path in resolved_root.rglob(pattern) if path.is_file())
        return cls.from_paths(resolved_root, paths)

    @classmethod
    def from_paths(cls, root: Path, paths: Iterable[str | Path]) -> ContractManifest:
        resolved_root = root.resolve(strict=True)
        entries: list[ManifestEntry] = []
        for raw_path in paths:
            path = Path(raw_path)
            candidate = path if path.is_absolute() else resolved_root / path
            resolved = candidate.resolve(strict=True)
            if not resolved.is_relative_to(resolved_root) or not resolved.is_file():
                raise ValueError(f"manifest path escapes root or is not a file: {path}")
            payload = resolved.read_bytes()
            relative = PurePosixPath(resolved.relative_to(resolved_root)).as_posix()
            entries.append(
                ManifestEntry(relative, hashlib.sha256(payload).hexdigest(), len(payload))
            )
        entries.sort()
        if len({entry.path for entry in entries}) != len(entries):
            raise ValueError("manifest contains duplicate paths")
        return cls(tuple(entries))

    @property
    def digest(self) -> str:
        digest = hashlib.sha256()
        for entry in self.entries:
            digest.update(entry.path.encode())
            digest.update(b"\0")
            digest.update(entry.sha256.encode("ascii"))
            digest.update(b"\0")
            digest.update(str(entry.size).encode("ascii"))
            digest.update(b"\n")
        return digest.hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "algorithm": "sha256",
            "digest": self.digest,
            "files": [
                {"path": entry.path, "sha256": entry.sha256, "size": entry.size}
                for entry in self.entries
            ],
        }
