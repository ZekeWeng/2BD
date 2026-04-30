from __future__ import annotations

from typing import Any

from ...domain import FilterClause

TABLE_PATTERNS: dict[str, tuple[str, ...]] = {
    "games":    ("games",),
    "plays":    ("plays",),
    "players":  ("players",),
    "tracking": ("tracking", "week"),
}

FIELD_MAP: dict[str, dict[str, list[str]]] = {
    "games": {
        "team":    ["homeTeamAbbr", "visitorTeamAbbr",
                    "homeTeamCode", "visitorTeamCode"],
        "season":  ["season"],
        "year":    ["season"],
        "week":    ["week"],
        "game_id": ["gameId"],
    },
    "plays": {
        "team":         ["possessionTeam", "defensiveTeam"],
        "offense":      ["possessionTeam"],
        "defense":      ["defensiveTeam"],
        "play_id":      ["playId"],
        "game_id":      ["gameId"],
        "yards_gained": ["yardsGained", "playResult"],
        "down":         ["down"],
        "quarter":      ["quarter"],
        "play_type":    ["playType", "play_type"],
    },
    "players": {
        "player_id": ["nflId"],
        "name":      ["displayName"],
        "player":    ["displayName"],
        "position":  ["position", "officialPosition"],
        "team":      ["teamAbbr"],
    },
    "tracking": {
        "player_id": ["nflId"],
        "team":      ["club", "team"],
        "play_id":   ["playId"],
        "game_id":   ["gameId"],
        "event":     ["event"],
        "frame":     ["frameId", "frame"],
    },
}

GAME_FILTER_KEYS = frozenset({"season", "year", "week", "team"})
PLAYER_FILTER_KEYS = frozenset({"name", "position", "player"})


def _is_listlike(value: Any) -> bool:
    return isinstance(value, (list, tuple, set))


def build_clauses(kind: str, columns: set[str], filters: dict[str, Any]
                  ) -> list[FilterClause] | None:
    """Returns None when the table cannot satisfy any filter; caller should skip it."""
    clauses: list[FilterClause] = []
    field_aliases = FIELD_MAP.get(kind, {})
    for name, value in filters.items():
        candidates = field_aliases.get(name, [name])
        present = tuple(c for c in candidates if c in columns)
        if not present:
            return None
        op = "in" if _is_listlike(value) else "eq"
        clause_value = list(value) if _is_listlike(value) else value
        field: str | tuple[str, ...] = present[0] if len(present) == 1 else present
        clauses.append(FilterClause(field, op, clause_value))
    return clauses
