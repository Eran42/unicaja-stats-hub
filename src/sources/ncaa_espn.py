"""
NCAA college basketball data fetcher using ESPN's game-log page.

Endpoint:
  GET https://www.espn.com/mens-college-basketball/player/gamelog/_/id/{id}/{slug}

The page has a game-log table with columns:
  Date, OPP, Result, MIN, FG (M-A), FG%, 3PT (M-A), 3P%, FT (M-A), FT%, REB, AST, BLK, STL, PF, TO, PTS

There is no pre-computed average row — we sum all game rows and divide by game count.

Player IDs come from ESPN URLs, e.g.:
  https://www.espn.com/mens-college-basketball/player/_/id/5313012/mario-saint-supery
  → ID 5313012, slug "mario-saint-supery"

Returns the canonical full-stats dict matching the ACB / EuroLeague schema.
Note: ESPN does not provide offensive/defensive rebound splits, so reb_off/reb_def are None.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.espn.com/mens-college-basketball/player/gamelog/_/id"
_TIMEOUT  = 20

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_slug(name: str) -> str:
    """
    Convert a player name to an ESPN URL slug.

    e.g. "Mario Saint-Supéry" → "mario-saint-supery"
    """
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name  = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    return slug


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().rstrip("%").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_made_attempted(cell_text: str) -> tuple[float | None, float | None]:
    """
    Parse "M-A" format like "6-12" → (6.0, 12.0).
    Also handles plain numbers or dashes.
    """
    text = cell_text.strip()
    if "-" in text:
        parts = text.split("-", 1)
        return _safe_float(parts[0]), _safe_float(parts[1])
    val = _safe_float(text)
    return val, None


def _cell_text(cell: Tag) -> str:
    return cell.get_text(separator=" ", strip=True)


# ---------------------------------------------------------------------------
# Column index detection
# ---------------------------------------------------------------------------

_KNOWN_HEADERS = ["date", "opp", "result", "min", "fg", "fg%", "3pt", "3p%",
                  "ft", "ft%", "reb", "ast", "blk", "stl", "pf", "to", "pts"]

def _find_col_map(header_row: list[str]) -> dict[str, int]:
    """Map canonical header names to column indices."""
    mapping: dict[str, int] = {}
    for idx, h in enumerate(header_row):
        key = h.lower().strip()
        mapping[key] = idx
    return mapping


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
) -> dict:
    """
    Fetch season averages for an NCAA player from ESPN game logs.

    Args:
        player_id:    ESPN player ID.
        player_name:  Player name used to build the URL slug.

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{player_id}/{slug}"
    logger.debug("ESPN NCAA fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
    except requests.HTTPError as exc:
        logger.warning("ESPN NCAA HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("ESPN NCAA request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------------------------------------------------
    # Find the game log table — look for a table whose headers include "PTS"
    # -----------------------------------------------------------------------
    game_table: Tag | None = None
    col_map: dict[str, int] = {}

    for table in soup.find_all("table"):
        headers: list[str] = []
        header_row = table.find("tr")
        if header_row:
            headers = [_cell_text(c).lower() for c in header_row.find_all(["th", "td"])]
        if "pts" in headers and ("fg" in headers or "min" in headers):
            game_table = table
            col_map = _find_col_map(headers)
            break

    if game_table is None or not col_map:
        logger.warning("ESPN NCAA: could not find game log table for player_id=%s", player_id)
        return {}

    # -----------------------------------------------------------------------
    # Parse all data rows (skip header rows and rows with no numeric data)
    # -----------------------------------------------------------------------
    sums: dict[str, float] = {
        "min": 0.0, "pts": 0.0,
        "fgm": 0.0, "fga": 0.0,
        "fg3m": 0.0, "fg3a": 0.0,
        "ftm": 0.0, "fta": 0.0,
        "reb": 0.0, "ast": 0.0,
        "blk": 0.0, "stl": 0.0,
        "pf": 0.0, "tov": 0.0,
    }
    games_played = 0

    def _get(cells: list, key: str) -> str:
        idx = col_map.get(key)
        if idx is not None and idx < len(cells):
            return _cell_text(cells[idx])
        return ""

    for row in game_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        # Skip header rows
        if cells[0].name == "th":
            continue
        # Check if PTS column has a numeric value
        pts_text = _get(cells, "pts")
        if not pts_text or not re.match(r"^\d", pts_text):
            continue

        pts_val = _safe_float(pts_text)
        if pts_val is None:
            continue

        games_played += 1

        sums["pts"] += pts_val or 0.0
        sums["min"] += _safe_float(_get(cells, "min")) or 0.0

        # Shooting: "6-12" format
        fgm, fga = _parse_made_attempted(_get(cells, "fg"))
        fg3m, fg3a = _parse_made_attempted(_get(cells, "3pt"))
        ftm, fta = _parse_made_attempted(_get(cells, "ft"))

        sums["fgm"]  += fgm  or 0.0
        sums["fga"]  += fga  or 0.0
        sums["fg3m"] += fg3m or 0.0
        sums["fg3a"] += fg3a or 0.0
        sums["ftm"]  += ftm  or 0.0
        sums["fta"]  += fta  or 0.0

        sums["reb"] += _safe_float(_get(cells, "reb")) or 0.0
        sums["ast"] += _safe_float(_get(cells, "ast")) or 0.0
        sums["blk"] += _safe_float(_get(cells, "blk")) or 0.0
        sums["stl"] += _safe_float(_get(cells, "stl")) or 0.0
        sums["pf"]  += _safe_float(_get(cells, "pf"))  or 0.0
        sums["tov"] += _safe_float(_get(cells, "to"))  or 0.0

    if games_played == 0:
        logger.warning("ESPN NCAA: no game rows found for player_id=%s", player_id)
        return {}

    def _avg(key: str) -> float | None:
        val = sums.get(key)
        if val is None:
            return None
        return round(val / games_played, 1)

    gp = games_played

    # 2-point derived stats
    fgm_avg  = sums["fgm"]  / gp
    fg3m_avg = sums["fg3m"] / gp
    fga_avg  = sums["fga"]  / gp
    fg3a_avg = sums["fg3a"] / gp

    t2m = round(fgm_avg - fg3m_avg, 1)
    t2a = round(fga_avg - fg3a_avg, 1)
    t2_pct = round(t2m / t2a * 100, 1) if t2a > 0 else None

    ft_pct = round(sums["ftm"] / sums["fta"] * 100, 1) if sums["fta"] > 0 else None
    t3_pct = round(sums["fg3m"] / sums["fg3a"] * 100, 1) if sums["fg3a"] > 0 else None

    return {
        "player_id":    str(player_id),
        "player_name":  player_name,
        "team":         "",  # filled by router
        "source":       "ncaa_espn",
        "competition":  "NCAA",
        "season":       "2025-26",
        "date":         str(date.today()),
        "games_played": gp,
        # Scoring
        "pts":          _avg("pts"),
        # 2-point shooting (derived)
        "t2m":          t2m,
        "t2a":          t2a,
        "t2_pct":       t2_pct,
        # 3-point shooting
        "t3m":          round(sums["fg3m"] / gp, 1),
        "t3a":          round(sums["fg3a"] / gp, 1),
        "t3_pct":       t3_pct,
        # Free throws
        "ftm":          round(sums["ftm"] / gp, 1),
        "fta":          round(sums["fta"] / gp, 1),
        "ft_pct":       ft_pct,
        # Rebounds (ESPN doesn't split off/def in game log)
        "reb_off":      None,
        "reb_def":      None,
        "reb":          _avg("reb"),
        # Other
        "ast":          _avg("ast"),
        "stl":          _avg("stl"),
        "tov":          _avg("tov"),
        "blk":          _avg("blk"),
        "fouls":        _avg("pf"),
        "plus_minus":   None,  # not in ESPN game log
        "val":          None,  # no NCAA equivalent
        "min":          round(sums["min"] / gp, 1),
    }
