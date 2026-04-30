from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from ..domain import Dataset, FilterClause, QueryEngine, Storage, Target


class CatalogService:
    def __init__(self, storage: Storage, engine: QueryEngine):
        self._storage = storage
        self._engine = engine

    def competitions(self) -> list[str]:
        return self._storage.list_targets("competition")

    def tables(self, slug: str) -> dict[str, Path]:
        target = Target(slug=slug, kind="competition")
        tables: dict[str, Path] = {}
        for file in self._storage.list_files(target):
            if not self._engine.supports(file):
                continue
            if file.stem in tables:
                raise ValueError(f"Duplicate stem {file.stem!r} in {slug}")
            tables[file.stem] = file
        return tables

    def open(self, slug: str, table: str) -> Dataset:
        tables = self.tables(slug)
        if table not in tables:
            raise KeyError(f"{table!r} not in {slug}. Available: {sorted(tables)}")
        return self._engine.open(tables[table])


class SubsetService:
    def __init__(self, catalog: CatalogService):
        self._catalog = catalog

    def query(self, slug: str, table: str, clauses: Iterable[FilterClause],
              cols: Iterable[str] | None = None) -> Dataset:
        dataset = self._catalog.open(slug, table).filter(clauses)
        if cols:
            dataset = dataset.select(cols)
        return dataset

    def export(self, slug: str, table: str, clauses: Iterable[FilterClause],
               out: Path, cols: Iterable[str] | None = None) -> Path:
        out.parent.mkdir(parents=True, exist_ok=True)
        self.query(slug, table, clauses, cols).sink(out)
        return out
