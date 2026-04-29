from __future__ import annotations

import tomllib
from pathlib import Path

from ..domain import Target


class TomlConfig:
    def __init__(self, path: Path):
        self._path = path

    def load(self) -> list[Target]:
        if not self._path.exists():
            raise FileNotFoundError(f"Config not found: {self._path}")
        raw = tomllib.loads(self._path.read_text())
        targets: list[Target] = []
        for entry in raw.get("competitions", []):
            targets.append(Target(
                slug=entry["slug"],
                kind="competition",
                files=tuple(entry.get("files", [])),
                unzip=entry.get("unzip", True),
            ))
        for entry in raw.get("datasets", []):
            targets.append(Target(
                slug=entry["slug"],
                kind="dataset",
                files=tuple(entry.get("files", [])),
                unzip=entry.get("unzip", True),
            ))
        return targets
