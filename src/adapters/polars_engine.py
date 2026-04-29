from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path

import polars as pl

from ..domain import FilterClause


_OPS = {
    "eq":       lambda c, v: c == v,
    "ne":       lambda c, v: c != v,
    "gt":       lambda c, v: c > v,
    "gte":      lambda c, v: c >= v,
    "lt":       lambda c, v: c < v,
    "lte":      lambda c, v: c <= v,
    "in":       lambda c, v: c.is_in(list(v)),
    "notin":    lambda c, v: ~c.is_in(list(v)),
    "between":  lambda c, v: c.is_between(v[0], v[1]),
    "contains": lambda c, v: c.cast(pl.Utf8).str.contains(str(v)),
    "isnull":   lambda c, v: c.is_null() if v else c.is_not_null(),
}


def _clause_expr(clause: FilterClause) -> pl.Expr:
    op = _OPS[clause.op]
    if isinstance(clause.field, tuple):
        per_column = [op(pl.col(f), clause.value) for f in clause.field]
        combined = per_column[0]
        for expr in per_column[1:]:
            combined = combined | expr
        return combined
    return op(pl.col(clause.field), clause.value)


def _to_expr(clauses: Iterable[FilterClause]) -> pl.Expr | None:
    combined: pl.Expr | None = None
    for clause in clauses:
        expr = _clause_expr(clause)
        combined = expr if combined is None else combined & expr
    return combined


class PolarsDataset:
    def __init__(self, lf: pl.LazyFrame):
        self._lf = lf

    @property
    def lf(self) -> pl.LazyFrame:
        return self._lf

    def filter(self, clauses):
        expr = _to_expr(clauses)
        return PolarsDataset(self._lf.filter(expr) if expr is not None else self._lf)

    def select(self, cols):
        cols = list(cols)
        return PolarsDataset(self._lf.select(cols) if cols else self._lf)

    def head(self, n: int):
        return self._lf.head(n).collect()

    def collect(self):
        return self._lf.collect()

    def sink(self, path: Path) -> None:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            self._lf.sink_parquet(path)
        elif suffix == ".csv":
            self._lf.sink_csv(path)
        elif suffix in {".jsonl", ".ndjson"}:
            self._lf.sink_ndjson(path)
        else:
            raise ValueError(f"Unsupported sink suffix: {path.suffix}")

    def schema(self) -> Mapping[str, str]:
        return {k: str(v) for k, v in self._lf.collect_schema().items()}

    def columns(self) -> list[str]:
        return self._lf.collect_schema().names()

    def __repr__(self) -> str:
        names = self._lf.collect_schema().names()
        preview = ", ".join(names[:5]) + (" …" if len(names) > 5 else "")
        return f"PolarsDataset({len(names)} cols: {preview})"

    def _repr_html_(self) -> str:
        try:
            return self._lf.head(10).collect()._repr_html_()
        except Exception as exc:
            return f"<pre>PolarsDataset preview failed: {exc}</pre>"


class PolarsEngine:
    SUPPORTED = {".csv", ".parquet", ".jsonl", ".ndjson"}

    def supports(self, path: Path) -> bool:
        return path.suffix.lower() in self.SUPPORTED

    def open(self, path: Path) -> PolarsDataset:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return PolarsDataset(pl.scan_csv(path, infer_schema_length=10_000,
                                             ignore_errors=True))
        if suffix == ".parquet":
            return PolarsDataset(pl.scan_parquet(path))
        if suffix in {".jsonl", ".ndjson"}:
            return PolarsDataset(pl.scan_ndjson(path))
        raise ValueError(f"Unsupported file: {path}")

    def concat(self, datasets) -> PolarsDataset:
        lfs = [d.lf for d in datasets]
        if not lfs:
            return PolarsDataset(pl.LazyFrame())
        return PolarsDataset(pl.concat(lfs, how="diagonal_relaxed"))

    def join(self, left: PolarsDataset, right: PolarsDataset, *,
             on: list[str], how: str = "inner") -> PolarsDataset:
        left_lf, right_lf = left.lf, right.lf
        left_schema = left_lf.collect_schema()
        right_schema = right_lf.collect_schema()
        for col in on:
            if left_schema.get(col) != right_schema.get(col):
                right_lf = right_lf.with_columns(pl.col(col).cast(left_schema[col]))
        return PolarsDataset(left_lf.join(right_lf, on=on, how=how))
