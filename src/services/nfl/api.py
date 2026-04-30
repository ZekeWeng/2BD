from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ...domain import Dataset, QueryEngine
from ..querying import CatalogService
from .schema import (
    GAME_FILTER_KEYS, PLAYER_FILTER_KEYS, TABLE_PATTERNS, build_clauses,
)
from .transforms import Transform


class NFL:
    def __init__(self, catalog: CatalogService, engine: QueryEngine):
        self._catalog = catalog
        self._engine = engine

    def games(self, *, slug: str | None = None, **filters: Any) -> Dataset:
        return self._query("games", slug, filters)

    def players(self, *, slug: str | None = None, **filters: Any) -> Dataset:
        return self._query("players", slug, filters)

    def plays(self, *, slug: str | None = None, **filters: Any) -> Dataset:
        play_filters = {k: v for k, v in filters.items()
                        if k not in GAME_FILTER_KEYS - {"team"}}
        game_filters = {k: v for k, v in filters.items() if k in GAME_FILTER_KEYS}
        plays = self._query("plays", slug, play_filters)
        if not game_filters:
            return plays
        games = self._query("games", slug, game_filters).select(["gameId"])
        return self._engine.join(plays, games, on=["gameId"], how="inner")

    def tracking(self, *, slug: str | None = None,
                 transforms: Iterable[Transform] = (),
                 **filters: Any) -> Dataset:
        track_filters = {k: v for k, v in filters.items()
                         if k not in GAME_FILTER_KEYS | PLAYER_FILTER_KEYS}
        player_filters = {k: v for k, v in filters.items()
                          if k in PLAYER_FILTER_KEYS}
        game_filters = {k: v for k, v in filters.items() if k in GAME_FILTER_KEYS}

        dataset = self._query("tracking", slug, track_filters)
        if player_filters:
            players = self._query("players", slug, player_filters).select(["nflId"])
            dataset = self._engine.join(dataset, players, on=["nflId"], how="inner")
        if game_filters:
            games = self._query("games", slug, game_filters).select(["gameId"])
            dataset = self._engine.join(dataset, games, on=["gameId"], how="inner")
        for transform in transforms:
            dataset = transform(dataset)
        return dataset

    def _query(self, kind: str, slug: str | None,
               filters: dict[str, Any]) -> Dataset:
        slugs = [slug] if slug else self._catalog.competitions()
        prefixes = TABLE_PATTERNS[kind]
        matched: list[Dataset] = []
        for competition_slug in slugs:
            try:
                tables = self._catalog.tables(competition_slug)
            except FileNotFoundError:
                continue
            for stem in tables:
                if not any(stem == p
                           or stem.startswith(p + "_")
                           or (stem.startswith(p) and stem[len(p):].isdigit())
                           for p in prefixes):
                    continue
                dataset = self._catalog.open(competition_slug, stem)
                clauses = build_clauses(kind, set(dataset.columns()), filters)
                if clauses is None:
                    continue
                matched.append(dataset.filter(clauses))
        if not matched:
            raise LookupError(
                f"No {kind} table satisfies filters {filters!r} (searched: {slugs})"
            )
        return self._engine.concat(matched)
