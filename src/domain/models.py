from __future__ import annotations

from dataclasses import dataclass
from typing import Any

VALID_OPS = (
    "eq", "ne", "gt", "gte", "lt", "lte",
    "in", "notin", "between", "contains", "isnull",
)


@dataclass(frozen=True)
class FilterClause:
    """A tuple of column names matches when ANY column satisfies (op, value)."""
    field: str | tuple[str, ...]
    op: str
    value: Any

    def __post_init__(self) -> None:
        if self.op not in VALID_OPS:
            raise ValueError(f"Unknown op {self.op!r}; valid: {VALID_OPS}")


@dataclass(frozen=True)
class Target:
    slug: str
    kind: str                       # "competition" | "dataset"
    files: tuple[str, ...] = ()     # empty = bulk download
    unzip: bool = True
