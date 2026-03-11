"""
NBA data fetcher using the stats.nba.com API (no authentication required).

Endpoint used:
  GET https://stats.nba.com/stats/playercareerstats
      ?PlayerID={player_id}&PerMode=PerGame

The response contains a `resultSets` array. The "SeasonTotalsRegularSeason"
result set has per-game averages when PerMode=PerGame is requested.

Player IDs come from nba.com URLs, e.g.:
  https://www.nba.com/player/1627734/domantas-sabonis → ID 1627734

Returns the canonical full-stats dict matching the ACB / EuroLeague schema.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://stats.nba.com/stats/playercareerstats"
_TIMEOUT  = 15

# stats.nba.com requires specific headers to avoid 403
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nba.com/",
    "Origin":          "https://www.nba.com",
    "Connection":      "keep-alive",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

# Current NBA season year (start year of the season, e.g. 2024 for 2024-25)
_CURRENT_SEASON = "2024-25"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_minutes(value: Any) -> float | None:
    """Parse minutes — stats.nba.com returns a decimal float for PerGame."""
    return _safe_float(value)


def _parse_resultset(data: dict, set_name: str) -> tuple[list[str], list[list]]:
    """Extract headers and rows from a named resultSet."""
    for rs in data.get("resultSets", []):
        if rs.get("name") == set_name:
            return rs.get("headers", []), rs.get("rowSet", [])
    return [], []


def _row_to_dict(headers: list[str], row: list) -> dict:
    return dict(zip(headers, row))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_season_averages(player_id: str | int, season: str = _CURRENT_SEASON) -> dict:
    """
    Fetch per-game season averages for an NBA player from stats.nba.com.

    Args:
        player_id:  NBA player ID (e.g. 1627734 for Domantas Sabonis).
        season:     Season string in "YYYY-YY" format (e.g. "2024-25").

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    params = {
        "PlayerID": str(player_id),
        "PerMode": "PerGame",
    }
    logger.debug("NBA fetch: player_id=%s season=%s", player_id, season)

    try:
        resp = requests.get(_BASE_URL, headers=_HEADERS, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as exc:
        logger.warning("NBA HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("NBA request failed player_id=%s: %s", player_id, exc)
        return {}
    except ValueError as exc:
        logger.warning("NBA JSON parse error player_id=%s: %s", player_id, exc)
        return {}

    headers, rows = _parse_resultset(data, "SeasonTotalsRegularSeason")
    if not headers or not rows:
        logger.warning("NBA: no SeasonTotalsRegularSeason for player_id=%s", player_id)
        return {}

    # Find the row matching the requested season
    season_row: dict | None = None
    for row in reversed(rows):  # most recent first
        r = _row_to_dict(headers, row)
        if r.get("SEASON_ID") == season:
            season_row = r
            break

    # Fallback to most recent season if target not found
    if season_row is None and rows:
        season_row = _row_to_dict(headers, rows[-1])
        logger.info(
            "NBA: season %s not found for player_id=%s, using %s instead.",
            season, player_id, season_row.get("SEASON_ID"),
        )

    if season_row is None:
        return {}

    # stats.nba.com stores FG% as 0–1 decimal; convert to 0–100
    def _pct(v: Any) -> float | None:
        f = _safe_float(v)
        return round(f * 100, 1) if f is not None else None

    fgm  = _safe_float(season_row.get("FGM"))
    fga  = _safe_float(season_row.get("FGA"))
    fg3m = _safe_float(season_row.get("FG3M"))
    fg3a = _safe_float(season_row.get("FG3A"))

    # Derive 2-point made/attempted by subtracting 3-pointers from total FG
    t2m = round(fgm  - fg3m,  2) if fgm  is not None and fg3m  is not None else None
    t2a = round(fga  - fg3a,  2) if fga  is not None and fg3a  is not None else None
    t2_pct: float | None = None
    if t2m is not None and t2a and t2a > 0:
        t2_pct = round(t2m / t2a * 100, 1)

    player_name = season_row.get("PLAYER_NAME") or str(player_id)
    team_abbrev = season_row.get("TEAM_ABBREVIATION") or ""
    season_id   = season_row.get("SEASON_ID", season)

    return {
        "player_id":    str(player_id),
        "player_name":  player_name,
        "team":         team_abbrev,
        "source":       "nba",
        "competition":  "NBA",
        "season":       season_id,
        "date":         str(date.today()),
        "games_played": int(_safe_float(season_row.get("GP")) or 0) or None,
        # Scoring
        "pts":          _safe_float(season_row.get("PTS")),
        # 2-point shooting (derived)
        "t2m":          t2m,
        "t2a":          t2a,
        "t2_pct":       t2_pct,
        # 3-point shooting
        "t3m":          fg3m,
        "t3a":          fg3a,
        "t3_pct":       _pct(season_row.get("FG3_PCT")),
        # Free throws
        "ftm":          _safe_float(season_row.get("FTM")),
        "fta":          _safe_float(season_row.get("FTA")),
        "ft_pct":       _pct(season_row.get("FT_PCT")),
        # Rebounds
        "reb_off":      _safe_float(season_row.get("OREB")),
        "reb_def":      _safe_float(season_row.get("DREB")),
        "reb":          _safe_float(season_row.get("REB")),
        # Other
        "ast":          _safe_float(season_row.get("AST")),
        "stl":          _safe_float(season_row.get("STL")),
        "tov":          _safe_float(season_row.get("TOV")),
        "blk":          _safe_float(season_row.get("BLK")),
        "fouls":        _safe_float(season_row.get("PF")),
        "plus_minus":   _safe_float(season_row.get("PLUS_MINUS")),
        "val":          None,  # No NBA equivalent for ACB/EL valoration
        "min":          _parse_minutes(season_row.get("MIN")),
    }
