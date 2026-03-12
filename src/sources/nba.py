"""
NBA latest game box score via stats.nba.com playergamelog API.

Endpoint:
  GET https://stats.nba.com/stats/playergamelog
      ?PlayerID={id}&Season=2024-25&SeasonType=Regular+Season

Returns games in reverse-chronological order (most recent first).
We take row 0.

Player IDs from nba.com URLs, e.g.:
  https://www.nba.com/player/1627734/domantas-sabonis → ID 1627734
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL       = "https://stats.nba.com/stats/playergamelog"
_TIMEOUT        = 15
_CURRENT_SEASON = "2024-25"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept-Encoding":    "gzip, deflate, br",
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "Connection":         "keep-alive",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}


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


def _parse_nba_date(value: str) -> str:
    """Convert 'MAR 10, 2026' → 'YYYY-MM-DD'."""
    try:
        return datetime.strptime(value.strip(), "%b %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        return value


def _parse_minutes(value: Any) -> float | None:
    """NBA game log returns minutes as 'MM:SS' string."""
    if value is None:
        return None
    text = str(value).strip()
    if ":" in text:
        parts = text.split(":")
        try:
            return round(float(parts[0]) + float(parts[1]) / 60, 2)
        except (ValueError, IndexError):
            return None
    return _safe_float(text)


def _row_to_dict(headers: list[str], row: list) -> dict:
    return dict(zip(headers, row))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_season_averages(player_id: str | int, season: str = _CURRENT_SEASON) -> dict:
    """
    Fetch the most recent game box score for an NBA player.

    Args:
        player_id:  NBA player ID.
        season:     Season string "YYYY-YY" (e.g. "2024-25").

    Returns:
        Canonical single-game stats dict, or empty dict on failure.
    """
    params = {
        "PlayerID":   str(player_id),
        "Season":     season,
        "SeasonType": "Regular Season",
    }
    logger.debug("NBA game log fetch: player_id=%s season=%s", player_id, season)

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

    # Find the "PlayerGameLog" result set
    result_set = None
    for rs in data.get("resultSets", []):
        if rs.get("name") == "PlayerGameLog":
            result_set = rs
            break

    if result_set is None:
        logger.warning("NBA: PlayerGameLog result set missing for player_id=%s", player_id)
        return {}

    headers = result_set.get("headers", [])
    rows    = result_set.get("rowSet", [])

    if not rows:
        logger.warning("NBA: no game log rows for player_id=%s season=%s", player_id, season)
        return {}

    # Row 0 = most recent game
    row = _row_to_dict(headers, rows[0])

    fgm  = _safe_float(row.get("FGM"))
    fga  = _safe_float(row.get("FGA"))
    fg3m = _safe_float(row.get("FG3M"))
    fg3a = _safe_float(row.get("FG3A"))

    t2m = round(fgm  - fg3m, 1) if fgm  is not None and fg3m is not None else None
    t2a = round(fga  - fg3a, 1) if fga  is not None and fg3a is not None else None
    t2_pct = round(t2m / t2a * 100, 1) if t2m is not None and t2a else None

    def _pct(v: Any) -> float | None:
        f = _safe_float(v)
        return round(f * 100, 1) if f is not None else None

    matchup   = str(row.get("MATCHUP", ""))
    game_date = _parse_nba_date(str(row.get("GAME_DATE", "")))

    return {
        "player_id":   str(player_id),
        "source":      "nba",
        "competition": "NBA",
        "season":      season,
        "game_date":   game_date,
        "opponent":    matchup,
        "result":      str(row.get("WL", "")),
        "date":        str(date.today()),
        "min":         _parse_minutes(row.get("MIN")),
        "pts":         _safe_float(row.get("PTS")),
        "t2m":         t2m,                           "t2a":  t2a,                           "t2_pct":  t2_pct,
        "t3m":         fg3m,                          "t3a":  fg3a,                          "t3_pct":  _pct(row.get("FG3_PCT")),
        "ftm":         _safe_float(row.get("FTM")),   "fta":  _safe_float(row.get("FTA")),   "ft_pct":  _pct(row.get("FT_PCT")),
        "reb_off":     _safe_float(row.get("OREB")),
        "reb_def":     _safe_float(row.get("DREB")),
        "reb":         _safe_float(row.get("REB")),
        "ast":         _safe_float(row.get("AST")),
        "stl":         _safe_float(row.get("STL")),
        "tov":         _safe_float(row.get("TOV")),
        "blk":         _safe_float(row.get("BLK")),
        "fouls":       _safe_float(row.get("PF")),
        "plus_minus":  _safe_float(row.get("PLUS_MINUS")),
        "val":         None,
    }
