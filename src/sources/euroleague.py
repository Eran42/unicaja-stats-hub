"""
EuroLeague / EuroCup data fetcher using the incrowdsports v2 API.

Endpoint:
  GET https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/{comp}/seasons/{season}/people/{player_code}/stats?phaseTypeCode=RS

Competition codes:
  EuroLeague : comp="E"  season="E2024"
  EuroCup    : comp="U"  season="U2024"

Player codes follow the P0XXXXX format found on euroleaguebasketball.net
(e.g. "P003842" for Mathias Lessort).

Returns the canonical full-stats dict with per-game averages.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE = "https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions"
_TIMEOUT = 15

# Default season codes
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
    "Accept": "application/json",
    "Origin": "https://www.euroleaguebasketball.net",
    "Referer": "https://www.euroleaguebasketball.net/",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    """Parse a value to float. Handles percentage strings like '68.2%'."""
    if value is None:
        return None
    text = str(value).strip().rstrip("%").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_minutes(value: Any) -> float | None:
    """Parse minutes. Handles 'MM:SS', decimal strings, or numeric."""
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_code: str,
    *,
    competition: str = _EL_COMP,
    season: str = _EL_SEASON,
    phase: str = "RS",
) -> dict:
    """
    Fetch season stats for a EuroLeague or EuroCup player.

    Args:
        player_code:  incrowdsports player code, e.g. "P003842".
        competition:  "E" for EuroLeague, "U" for EuroCup.
        season:       Season code, e.g. "E2024" or "U2024".
        phase:        Phase type code: "RS" (regular season), "PO" (playoff).

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    url = f"{_BASE}/{competition}/seasons/{season}/people/{player_code}/stats"
    params = {"phaseTypeCode": phase}
    logger.debug("EuroLeague/EuroCup fetch: %s  params=%s", url, params)

    try:
        resp = requests.get(url, headers=_HEADERS, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data: dict = resp.json()
    except requests.HTTPError as exc:
        logger.warning("EL/EC HTTP error player=%s: %s", player_code, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("EL/EC request failed player=%s: %s", player_code, exc)
        return {}
    except ValueError as exc:
        logger.warning("EL/EC JSON parse error player=%s: %s", player_code, exc)
        return {}

    avg: dict = data.get("averagePerGame", {}) or {}
    acc: dict = data.get("accumulated", {}) or {}

    if not avg and not acc:
        logger.warning("EL/EC: no stats data for player=%s season=%s", player_code, season)
        return {}

    # Player / team info (may be absent)
    player_info: dict = data.get("player", {}) or {}
    team_info:   dict = data.get("club",   {}) or data.get("team", {}) or {}

    player_name = (
        player_info.get("name")
        or player_info.get("fullName")
        or player_info.get("alias")
        or player_code
    )
    team_name = (
        team_info.get("name")
        or team_info.get("alias")
        or team_info.get("abbreviatedName")
        or ""
    )

    gp = int(_safe_float(avg.get("gamesPlayed") or acc.get("gamesPlayed") or 0) or 0)

    # Percentages come as "68.2%" strings or already as floats
    t2_pct  = _safe_float(avg.get("twoPointShootingPercentage"))
    t3_pct  = _safe_float(avg.get("threePointShootingPercentage"))
    ft_pct  = _safe_float(avg.get("freeThrowShootingPercentage"))

    # Convert 0–1 decimal to 0–100 if percentages look like decimals
    def _normalise_pct(v: float | None) -> float | None:
        if v is None:
            return None
        return round(v * 100, 1) if v <= 1.0 else round(v, 1)

    competition_label = "EuroLeague" if competition == _EL_COMP else "EuroCup"

    return {
        "player_id":    player_code,
        "player_name":  player_name,
        "team":         team_name,
        "source":       competition_label.lower().replace(" ", ""),
        "competition":  competition_label,
        "season":       season,
        "date":         str(date.today()),
        "games_played": gp or None,
        # Scoring
        "pts":          _safe_float(avg.get("points")),
        # 2-point shooting
        "t2m":          _safe_float(avg.get("fieldGoalsMade2")),
        "t2a":          _safe_float(avg.get("fieldGoalsAttempted2")),
        "t2_pct":       _normalise_pct(t2_pct),
        # 3-point shooting
        "t3m":          _safe_float(avg.get("fieldGoalsMade3")),
        "t3a":          _safe_float(avg.get("fieldGoalsAttempted3")),
        "t3_pct":       _normalise_pct(t3_pct),
        # Free throws
        "ftm":          _safe_float(avg.get("freeThrowsMade")),
        "fta":          _safe_float(avg.get("freeThrowsAttempted")),
        "ft_pct":       _normalise_pct(ft_pct),
        # Rebounds
        "reb_off":      _safe_float(avg.get("offensiveRebounds")),
        "reb_def":      _safe_float(avg.get("defensiveRebounds")),
        "reb":          _safe_float(avg.get("totalRebounds")),
        # Other
        "ast":          _safe_float(avg.get("assistances")),
        "stl":          _safe_float(avg.get("steals")),
        "tov":          _safe_float(avg.get("turnovers")),
        "blk":          _safe_float(avg.get("blocksFavour")),
        "fouls":        _safe_float(avg.get("foulsCommited")),
        "plus_minus":   _safe_float(avg.get("plusMinus")),
        "val":          _safe_float(avg.get("valuation")),
        "min":          _parse_minutes(avg.get("timePlayed")),
    }


def fetch_eurocup_player_stats(player_code: str, season: str = _EC_SEASON) -> dict:
    """Convenience wrapper for EuroCup stats."""
    return fetch_player_stats(player_code, competition=_EC_COMP, season=season)
