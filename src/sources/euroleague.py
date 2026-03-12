"""
EuroLeague / EuroCup latest game box score via incrowdsports v2 API.

Two-step process:
  1. GET /competitions/{comp}/seasons/{season}/games?limit=200
     → returns all season games; filter to status='result' (played)
  2. GET /competitions/{comp}/seasons/{season}/games/{gameCode}/stats
     → returns full box score; teams are under 'local'/'road' keys,
       players at p['player']['person']['code'] (no 'P' prefix)

Competition codes:
  EuroLeague : comp="E"  season="E2025"
  EuroCup    : comp="U"  season="U2025"

Player codes in the registry use the P0XXXXX format (e.g. "P003842").
The box score API uses the code without the leading 'P' (e.g. "003842").
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
_EL_SEASON = "E2025"
_EC_SEASON = "U2025"

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
    v = _safe_float(text)
    # API returns timePlayed in seconds (e.g. 1541 = 25.7 min)
    if v is not None and v > 100:
        return round(v / 60, 2)
    return v


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


def _fetch_game_stats(
    competition: str, season: str, game_code: int, bare_code: str
) -> tuple[dict | None, str, str]:
    """
    Fetch a single game box score and extract the target player's stats.

    Args:
        bare_code: player code WITHOUT the leading 'P' (e.g. "010581").

    Returns:
        (stats_dict, player_name, opponent_name) or (None, "", "") if not found.
    """
    url  = f"{_BASE}/{competition}/seasons/{season}/games/{game_code}/stats"
    data = _get_json(url)

    if not data or not isinstance(data, dict):
        return None, "", ""

    # Box score top-level keys are 'local' (home) and 'road' (away)
    sides = ("local", "road")
    for i, side in enumerate(sides):
        team = data.get(side, {})
        for p in team.get("players", []):
            person = p.get("player", {}).get("person", {})
            if person.get("code") == bare_code:
                # Opponent is the other side
                other_side = sides[1 - i]
                opp_team = data.get(other_side, {})
                # Club name is nested under the first player's club, or team-level
                opp_players = opp_team.get("players", [])
                opponent = ""
                if opp_players:
                    opponent = (
                        opp_players[0].get("player", {}).get("club", {}).get("name", "")
                    )
                player_name = person.get("name", "").title()
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
        player_code:  incrowdsports player code with 'P' prefix, e.g. "P003842".
        competition:  "E" for EuroLeague, "U" for EuroCup.
        season:       Season code, e.g. "E2025" or "U2025".

    Returns:
        Canonical single-game stats dict, or empty dict on failure.
    """
    # Box score uses code without 'P' prefix
    bare_code = player_code[1:] if player_code.startswith("P") else player_code

    # Fetch all season games (default limit returns only future games)
    url  = f"{_BASE}/{competition}/seasons/{season}/games"
    data = _get_json(url, params={"limit": 200})

    if not data or not isinstance(data, dict):
        logger.warning("EL/EC: could not fetch games list for %s/%s", competition, season)
        return {}

    games = data.get("data", [])

    # Keep only played games (status='result')
    played = [g for g in games if str(g.get("status", "")).lower() == "result"]
    if not played:
        logger.warning("EL/EC: no played games found in season %s", season)
        return {}

    played.sort(key=lambda g: str(g.get("date", "")), reverse=True)

    # Try the most recent games until we find one the player appeared in
    competition_label = "EuroLeague" if competition == _EL_COMP else "EuroCup"

    for game in played[:10]:
        raw_code = game.get("code")
        try:
            game_code = int(raw_code)
        except (TypeError, ValueError):
            continue

        raw_date  = str(game.get("date", ""))
        game_date = raw_date[:10] if raw_date else ""

        logger.debug(
            "EL/EC: checking game %s (%s) for player=%s",
            game_code, game_date, player_code,
        )
        stats, player_name, opponent = _fetch_game_stats(
            competition, season, game_code, bare_code
        )

        if stats is None:
            continue  # player not in this game — try the next one

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

    logger.warning("EL/EC: player %s not found in last 10 played games", player_code)
    return {}


def fetch_eurocup_player_stats(player_code: str, season: str = _EC_SEASON) -> dict:
    """Convenience wrapper for EuroCup stats."""
    return fetch_player_stats(player_code, competition=_EC_COMP, season=season)
