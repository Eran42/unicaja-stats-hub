"""
EuroLeague / EuroCup latest game box score via incrowdsports v2 API.

Two-step process:
  1. GET /competitions/{comp}/seasons/{season}/games?personCode={code}
     → returns list of games with codes and dates
  2. GET /competitions/{comp}/seasons/{season}/games/{gameCode}/stats
     → returns full box score; find player in home/away players list

Competition codes:
  EuroLeague : comp="E"  season="E2024"
  EuroCup    : comp="U"  season="U2024"

Player codes follow the P0XXXXX format (e.g. "P003842").
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE    = "https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions"
_TIMEOUT = 15

_EL_COMP   = "E"
_EC_COMP   = "U"
_EL_SEASON = "E2024"
_EC_SEASON = "U2024"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":   "application/json",
    "Origin":   "https://www.euroleaguebasketball.net",
    "Referer":  "https://www.euroleaguebasketball.net/",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().rstrip("%").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_minutes(value: Any) -> float | None:
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


def _pct(value: Any) -> float | None:
    """Normalise a percentage: if ≤ 1.0 treat as 0–1 decimal → multiply by 100."""
    v = _safe_float(value)
    if v is None:
        return None
    return round(v * 100, 1) if v <= 1.0 else round(v, 1)


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _get_json(url: str, params: dict | None = None) -> dict | list | None:
    try:
        resp = requests.get(url, headers=_HEADERS, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as exc:
        logger.warning("EL/EC HTTP error %s: %s", url, exc)
    except requests.RequestException as exc:
        logger.warning("EL/EC request failed %s: %s", url, exc)
    except ValueError as exc:
        logger.warning("EL/EC JSON parse error %s: %s", url, exc)
    return None


def _latest_game_code(
    competition: str, season: str, player_code: str
) -> tuple[int | None, str]:
    """
    Return (game_code, game_date_str) for the most recently played game.
    game_date_str is ISO date "YYYY-MM-DD" or "" if unknown.
    """
    url  = f"{_BASE}/{competition}/seasons/{season}/games"
    data = _get_json(url, params={"personCode": player_code})

    if not data or not isinstance(data, dict):
        return None, ""

    games = data.get("data", [])
    if not games:
        return None, ""

    # Filter to confirmed played games only — must have explicit status or a score
    played = [
        g for g in games
        if str(g.get("status", "")).lower() in ("played", "finished", "result")
        or g.get("score") is not None
    ]
    if not played:
        # No confirmed played games found — do not fall back to all games
        return None, ""

    # Sort by date descending; take most recent
    def _sort_key(g: dict) -> str:
        return str(g.get("date", ""))

    played.sort(key=_sort_key, reverse=True)
    latest = played[0]

    code      = latest.get("code") or latest.get("identifier", "").split("_")[-1]
    raw_date  = str(latest.get("date", ""))
    game_date = raw_date[:10] if raw_date else ""  # "YYYY-MM-DD"

    try:
        return int(code), game_date
    except (TypeError, ValueError):
        return None, game_date


def _fetch_game_stats(
    competition: str, season: str, game_code: int, player_code: str
) -> tuple[dict | None, str, str]:
    """
    Fetch a single game box score and extract the target player's stats.
    Returns (stats_dict, player_name, opponent_name).
    """
    url  = f"{_BASE}/{competition}/seasons/{season}/games/{game_code}/stats"
    data = _get_json(url)

    if not data or not isinstance(data, dict):
        return None, "", ""

    game_data = data.get("data", data)

    sides = ("home", "away")
    for i, side in enumerate(sides):
        team = game_data.get(side, {})
        players = team.get("players", [])
        for p in players:
            if p.get("code") == player_code or p.get("person", {}).get("code") == player_code:
                # Opponent is the other side's team name
                other_side = sides[1 - i]
                opp_team = game_data.get(other_side, {})
                opponent = (
                    opp_team.get("club", {}).get("name", "")
                    or opp_team.get("name", "")
                    or opp_team.get("teamCode", "")
                )
                # Player name
                person = p.get("person", {})
                player_name = (
                    person.get("name", "")
                    or f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
                )
                return p.get("stats", {}), player_name, opponent

    return None, "", ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_code: str,
    *,
    competition: str = _EL_COMP,
    season: str = _EL_SEASON,
) -> dict:
    """
    Fetch the most recent game box score for a EuroLeague or EuroCup player.

    Args:
        player_code:  incrowdsports player code, e.g. "P003842".
        competition:  "E" for EuroLeague, "U" for EuroCup.
        season:       Season code, e.g. "E2024" or "U2024".

    Returns:
        Canonical single-game stats dict, or empty dict on failure.
    """
    game_code, game_date = _latest_game_code(competition, season, player_code)

    if game_code is None:
        logger.warning("EL/EC: no played games found for player=%s", player_code)
        return {}

    logger.debug(
        "EL/EC: fetching game %s stats for player=%s (date=%s)",
        game_code, player_code, game_date,
    )
    stats, player_name, opponent = _fetch_game_stats(competition, season, game_code, player_code)

    if stats is None:
        logger.warning(
            "EL/EC: player %s not found in game %s box score", player_code, game_code
        )
        return {}

    competition_label = "EuroLeague" if competition == _EL_COMP else "EuroCup"

    t2m = _safe_float(stats.get("fieldGoalsMade2"))
    t2a = _safe_float(stats.get("fieldGoalsAttempted2"))
    t2_pct = round(t2m / t2a * 100, 1) if t2m is not None and t2a else _pct(stats.get("twoPointShootingPercentage"))

    t3m = _safe_float(stats.get("fieldGoalsMade3"))
    t3a = _safe_float(stats.get("fieldGoalsAttempted3"))
    t3_pct = round(t3m / t3a * 100, 1) if t3m is not None and t3a else _pct(stats.get("threePointShootingPercentage"))

    ftm = _safe_float(stats.get("freeThrowsMade"))
    fta = _safe_float(stats.get("freeThrowsAttempted"))
    ft_pct = round(ftm / fta * 100, 1) if ftm is not None and fta else _pct(stats.get("freeThrowShootingPercentage"))

    return {
        "player_id":   player_code,
        "player_name": player_name or player_code,
        "source":      competition_label.lower().replace(" ", ""),
        "competition": competition_label,
        "season":      season,
        "game_date":   game_date,
        "opponent":    opponent,
        "result":      "",
        "date":        str(date.today()),
        "min":         _parse_minutes(stats.get("timePlayed")),
        "pts":         _safe_float(stats.get("points")),
        "t2m":         t2m,   "t2a":  t2a,  "t2_pct":  t2_pct,
        "t3m":         t3m,   "t3a":  t3a,  "t3_pct":  t3_pct,
        "ftm":         ftm,   "fta":  fta,  "ft_pct":  ft_pct,
        "reb_off":     _safe_float(stats.get("offensiveRebounds")),
        "reb_def":     _safe_float(stats.get("defensiveRebounds")),
        "reb":         _safe_float(stats.get("totalRebounds")),
        "ast":         _safe_float(stats.get("assistances")),
        "stl":         _safe_float(stats.get("steals")),
        "tov":         _safe_float(stats.get("turnovers")),
        "blk":         _safe_float(stats.get("blocksFavour")),
        "fouls":       _safe_float(stats.get("foulsCommited")),
        "plus_minus":  _safe_float(stats.get("plusMinus")),
        "val":         _safe_float(stats.get("valuation")),
    }


def fetch_eurocup_player_stats(player_code: str, season: str = _EC_SEASON) -> dict:
    """Convenience wrapper for EuroCup stats."""
    return fetch_player_stats(player_code, competition=_EC_COMP, season=season)
