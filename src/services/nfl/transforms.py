from __future__ import annotations

from collections.abc import Callable

import polars as pl

from ...adapters import PolarsDataset

type Transform = Callable[[PolarsDataset], PolarsDataset]

FIELD_LENGTH = 120.0
FIELD_WIDTH = 53.3


def _to_float(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Float64, strict=False)


def normalize_direction(dataset: PolarsDataset) -> PolarsDataset:
    """Rotate left-direction plays 180° around the field center so every play
    runs left-to-right. x, y, o, and dir are all updated; playDirection is
    rewritten to "right"."""
    is_left = pl.col("playDirection") == "left"
    o = _to_float("o")
    direction = _to_float("dir")
    lf = dataset.lf.with_columns(
        pl.when(is_left).then(FIELD_LENGTH - pl.col("x")).otherwise(pl.col("x")).alias("x"),
        pl.when(is_left).then(FIELD_WIDTH - pl.col("y")).otherwise(pl.col("y")).alias("y"),
        pl.when(is_left).then((o + 180) % 360).otherwise(o).alias("o"),
        pl.when(is_left).then((direction + 180) % 360).otherwise(direction).alias("dir"),
        pl.lit("right").alias("playDirection"),
    )
    return PolarsDataset(lf)


def add_kinematics(dataset: PolarsDataset) -> PolarsDataset:
    """Project speed (s) and acceleration (a) along the dir axis into vx, vy,
    ax, ay. BDB convention: dir=0 → +y, increases clockwise."""
    rad = _to_float("dir").radians()
    s = pl.col("s")
    a = pl.col("a")
    lf = dataset.lf.with_columns(
        (s * rad.sin()).alias("vx"),
        (s * rad.cos()).alias("vy"),
        (a * rad.sin()).alias("ax"),
        (a * rad.cos()).alias("ay"),
    )
    return PolarsDataset(lf)
