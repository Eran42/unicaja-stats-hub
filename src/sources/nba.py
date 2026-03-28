"""
NBA latest game box score via ESPN Core API.

Endpoint flow:
  1. GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/{espn_id}/eventlog
     → find last event with "played": true; extract event_id and team_id
  2. GET .../events/{event_id}
     → shortName ("MEM @ SAC"), date
  3. GET .../events/{event_id}/competitions/{event_id}
     → competitors with homeAway + winner + score/$ref
  4. GET score/$ref for each competitor → home/away scores
  5. GET .../competitors/{team_id}/roster/{espn_id}/statistics/0
     → per-game stat categories

Player IDs are ESPN athlete IDs (different from nba.com player IDs).
Find via: https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/roster
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE    = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba"
_TIMEOUT = 15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(url: str) -> dict | None:
    try:
        r = requests.get(url, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("NBA ESPN fetch failed %s: %s", url, exc)
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stat_by_name(categories: list[dict], name: str) -> float | None:
    for cat in categories:
        for s in cat.get("stats", []):
            if s.get("name") == name:
                return _safe_float(s.get("value"))
    return None


def _parse_date(iso: str) -> str:
    """'2026-02-05T03:00Z' → '2026-02-05'"""
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_season_averages(espn_athlete_id: str | int) -> dict:
    """
    Fetch the most recent game box score for an NBA player via ESPN Core API.

    Args:
        espn_athlete_id: ESPN athlete ID (not the nba.com player ID).

    Returns:
        Canonical single-game stats dict, or empty dict on failure.
    """
    athlete_id = str(espn_athlete_id)

    # 1. Event log
    eventlog = _get(f"{_BASE}/athletes/{athlete_id}/eventlog")
    if not eventlog:
        return {}

    events = eventlog.get("events", {}).get("items", [])
    last_ev = next((ev for ev in reversed(events) if ev.get("played")), None)
    if not last_ev:
        logger.warning("NBA ESPN: no played events for athlete_id=%s", athlete_id)
        return {}

    event_ref  = last_ev["event"]["$ref"].split("?")[0]
    event_id   = event_ref.rstrip("/").rsplit("/", 1)[-1]
    team_id    = str(last_ev.get("teamId", ""))
    stats_ref  = last_ev.get("statistics", {}).get("$ref", "").split("?")[0]

    # 2. Event (shortName + date)
    event_data = _get(f"{_BASE}/events/{event_id}")
    if not event_data:
        return {}

    game_date  = _parse_date(event_data.get("date", ""))
    short_name = event_data.get("shortName", "")   # e.g. "MEM @ SAC"

    # 3. Competition (competitors with homeAway, winner, score ref)
    comp = _get(f"{_BASE}/events/{event_id}/competitions/{event_id}")
    if not comp:
        return {}

    home_score = away_score = None
    player_won = False
    opponent   = short_name   # fallback

    for c in comp.get("competitors", []):
        cteam_ref  = c.get("team", {}).get("$ref", "")
        cteam_id   = cteam_ref.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
        c_home     = c.get("homeAway") == "home"
        score_ref  = c.get("score", {}).get("$ref", "").split("?")[0]

        score_val = None
        if score_ref:
            score_data = _get(score_ref)
            if score_data:
                score_val = _safe_float(score_data.get("value"))

        if c_home:
            home_score = score_val
        else:
            away_score = score_val

        if cteam_id == team_id:
            player_won = bool(c.get("winner"))
            # Determine opponent from shortName: "MEM @ SAC" → opponent is the other team
            # shortName format: "AWAY @ HOME"; player's team is one of them
            parts = short_name.split(" @ ")
            if len(parts) == 2:
                if c_home:
                    opponent = "vs. " + parts[0]   # player is home, opponent is left
                else:
                    opponent = "@ " + parts[1]     # player is away, opponent is right

    # Build result string
    result = ""
    if home_score is not None and away_score is not None:
        # Find player's score vs opponent score
        # team_id is the player's team
        # need to know if player's team is home or away
        player_is_home = any(
            c.get("homeAway") == "home" and
            c.get("team", {}).get("$ref", "").rstrip("/").rsplit("/", 1)[-1].split("?")[0] == team_id
            for c in comp.get("competitors", [])
        )
        if player_is_home:
            my_score  = int(home_score)
            opp_score = int(away_score)
        else:
            my_score  = int(away_score)
            opp_score = int(home_score)
        prefix = "V" if player_won else "D"
        result = f"{prefix} {my_score}-{opp_score}"

    # 4. Per-game stats
    if not stats_ref:
        logger.warning("NBA ESPN: no stats ref for athlete_id=%s event=%s", athlete_id, event_id)
        return {}

    stats_data = _get(stats_ref)
    if not stats_data:
        return {}

    cats = stats_data.get("splits", {}).get("categories", [])

    fgm  = _stat_by_name(cats, "fieldGoalsMade")
    fga  = _stat_by_name(cats, "fieldGoalsAttempted")
    fg3m = _stat_by_name(cats, "threePointFieldGoalsMade")
    fg3a = _stat_by_name(cats, "threePointFieldGoalsAttempted")
    ftm  = _stat_by_name(cats, "freeThrowsMade")
    fta  = _stat_by_name(cats, "freeThrowsAttempted")

    t2m = round(fgm - fg3m, 1) if fgm is not None and fg3m is not None else None
    t2a = round(fga - fg3a, 1) if fga is not None and fg3a is not None else None
    t2_pct  = round(t2m / t2a * 100, 1) if t2m is not None and t2a else None
    t3_pct  = round(fg3m / fg3a * 100, 1) if fg3m is not None and fg3a else None
    ft_pct  = round(ftm / fta * 100, 1) if ftm is not None and fta else None

    minutes_raw = _stat_by_name(cats, "minutes")
    minutes = round(float(minutes_raw), 1) if minutes_raw is not None else None

    return {
        "player_id":   athlete_id,
        "player_name": "",                   # router fills via setdefault
        "source":      "nba",
        "competition": "NBA",
        "season":      "2025-26",
        "game_date":   game_date,
        "opponent":    opponent,
        "result":      result,
        "date":        str(date.today()),
        "min":         minutes,
        "pts":         _stat_by_name(cats, "points"),
        "t2m":         t2m,   "t2a": t2a,   "t2_pct": t2_pct,
        "t3m":         fg3m,  "t3a": fg3a,  "t3_pct": t3_pct,
        "ftm":         ftm,   "fta": fta,   "ft_pct": ft_pct,
        "reb_off":     _stat_by_name(cats, "offensiveRebounds"),
        "reb_def":     _stat_by_name(cats, "defensiveRebounds"),
        "reb":         _stat_by_name(cats, "rebounds"),
        "ast":         _stat_by_name(cats, "assists"),
        "stl":         _stat_by_name(cats, "steals"),
        "tov":         _stat_by_name(cats, "turnovers"),
        "blk":         _stat_by_name(cats, "blocks"),
        "fouls":       _stat_by_name(cats, "fouls"),
        "plus_minus":  _stat_by_name(cats, "plusMinus"),
        "val":         None,
    }
