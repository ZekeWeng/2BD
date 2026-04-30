from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from ..domain import ConfigSource, DataSource, Storage, Target


class ConfigLoader:
    def __init__(self, source: ConfigSource):
        self._source = source

    def all(self) -> list[Target]:
        return self._source.load()

    def by_slug(self, slugs: Iterable[str]) -> list[Target]:
        wanted = set(slugs)
        return [target for target in self._source.load() if target.slug in wanted]


@dataclass(frozen=True)
class PullReport:
    target: Target
    fetched: list[Path]
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


class PullService:
    def __init__(self, source: DataSource, storage: Storage):
        self._source = source
        self._storage = storage

    def pull(self, target: Target, *, force: bool = False) -> PullReport:
        dest = self._storage.location(target)
        dest.mkdir(parents=True, exist_ok=True)
        return PullReport(target, self._source.fetch(target, dest, force=force))

    def _pull_safe(self, target: Target, *, force: bool) -> PullReport:
        try:
            return self.pull(target, force=force)
        except Exception as exc:
            return PullReport(target, [], error=str(exc))

    def pull_many(self, targets: Iterable[Target], *, force: bool = False,
                  workers: int = 1) -> list[PullReport]:
        targets = list(targets)
        if workers <= 1 or len(targets) <= 1:
            return [self._pull_safe(target, force=force) for target in targets]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return list(executor.map(
                lambda target: self._pull_safe(target, force=force), targets))
