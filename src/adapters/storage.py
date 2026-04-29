from __future__ import annotations

from pathlib import Path

from ..domain import Target


class LocalStorage:
    """Lays out targets under <root>/competitions|datasets/<slug>/."""

    def __init__(self, root: Path):
        self._root = root

    def location(self, target: Target) -> Path:
        if target.kind == "competition":
            return self._root / "competitions" / target.slug
        return self._root / "datasets" / target.slug.replace("/", "__")

    def list_targets(self, kind: str) -> list[str]:
        subdir = self._root / ("competitions" if kind == "competition" else "datasets")
        if not subdir.exists():
            return []
        return sorted(p.name for p in subdir.iterdir() if p.is_dir())

    def list_files(self, target: Target) -> list[Path]:
        target_dir = self.location(target)
        if not target_dir.exists():
            return []
        return sorted(p for p in target_dir.rglob("*") if p.is_file())
