"""
BCL (Basketball Champions League) data fetcher.

The BCL stats page at https://www.championsleague.basketball/en/stats provides
per-game leaders with full stat columns. We filter the table by player ID or name.

Alternatively, the player profile page may be accessible at:
  https://www.championsleague.basketball/en/player/{player_id}/

Returns the canonical full-stats dict.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_STATS_URL    = "https://www.championsleague.basketball/en/stats/players/all/"
_PLAYER_URL   = "https://www.championsleague.basketball/en/player"
_TIMEOUT      = 20

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.championsleague.basketball/",
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
    except (TypeError, ValueError):
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


def _cell_text(cell: Tag) -> str:
    return cell.get_text(strip=True)


# BCL stats column labels → canonical field names
_HEADER_MAP: dict[str, str] = {
    "gp":    "games_played",
    "g":     "games_played",
    "mpg":   "min",
    "min":   "min",
    "ppg":   "pts",
    "pts":   "pts",
    "2pmpg": "t2m",  "2pm": "t2m",  "2fgm": "t2m",
    "2papg": "t2a",  "2pa": "t2a",  "2fga": "t2a",
    "2p%":   "t2_pct", "2fg%": "t2_pct",
    "3pmpg": "t3m",  "3pm": "t3m",  "3fgm": "t3m",
    "3papg": "t3a",  "3pa": "t3a",  "3fga": "t3a",
    "3p%":   "t3_pct", "3fg%": "t3_pct",
    "ftmpg": "ftm",  "ftm": "ftm",
    "ftapg": "fta",  "fta": "fta",
    "ft%":   "ft_pct",
    "rpg":   "reb",  "reb": "reb",  "rt": "reb",
    "ro":    "reb_off", "orpg": "reb_off",
    "rd":    "reb_def", "drpg": "reb_def",
    "apg":   "ast",  "ast": "ast",  "as": "ast",
    "spg":   "stl",  "stl": "stl",  "st": "stl",
    "tpg":   "tov",  "to":  "tov",  "tov": "tov",
    "bpg":   "blk",  "blk": "blk",  "bs": "blk",
    "pf":    "fouls", "fouls": "fouls",
    "+/-":   "plus_minus",
    "eff":   "val",  "val": "val",
}


def _build_col_map(header_cells: list[Tag]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for idx, cell in enumerate(header_cells):
        key = _cell_text(cell).lower().strip()
        field = _HEADER_MAP.get(key)
        if field and field not in mapping:
            mapping[field] = idx
    return mapping


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(player_id: str | int, player_name: str = "") -> dict:
    """
    Fetch BCL season per-game averages for a player.

    Tries the player profile page first; falls back to the stats leaders page.

    Args:
        player_id:    BCL numeric player ID.
        player_name:  Used for matching in stats leaders table (optional).

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    # --- Attempt 1: player profile page ---
    result = _fetch_from_profile(player_id, player_name)
    if result:
        return result

    # --- Attempt 2: stats leaders table ---
    return _fetch_from_stats_page(player_id, player_name)


def _fetch_from_profile(player_id: str | int, player_name: str) -> dict:
    """Try fetching directly from the player profile URL."""
    url = f"{_PLAYER_URL}/{player_id}/"
    logger.debug("BCL profile fetch: %s", url)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError:
        return {}
    except requests.RequestException as exc:
        logger.debug("BCL profile request failed: %s", exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    return _parse_stats_table(soup, player_id, player_name, match_player=False)


def _fetch_from_stats_page(player_id: str | int, player_name: str) -> dict:
    """Try the BCL stats leaders page and find the player by name/id."""
    logger.debug("BCL stats page fetch")
    try:
        resp = requests.get(_STATS_URL, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("BCL stats page request failed: %s", exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    return _parse_stats_table(soup, player_id, player_name, match_player=True)


def _parse_stats_table(
    soup: BeautifulSoup,
    player_id: str | int,
    player_name: str,
    match_player: bool,
) -> dict:
    """Generic table parser for BCL pages."""
    id_str = str(player_id)
    name_lower = player_name.lower()

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        col_map = _build_col_map(header_cells)
        if len(col_map) < 4:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            row_text = " ".join(_cell_text(c) for c in cells)

            # When matching, check player id or name appears in the row
            if match_player:
                if id_str not in row_text and (not name_lower or name_lower not in row_text.lower()):
                    continue

            # Validate: pts column should have a number
            pts_idx = col_map.get("pts")
            if pts_idx is None or pts_idx >= len(cells):
                continue
            if not _cell_text(cells[pts_idx]).replace(".", "").isdigit():
                continue

            row_texts = [_cell_text(c) for c in cells]

            def _get(field: str) -> float | None:
                idx = col_map.get(field)
                if idx is not None and idx < len(row_texts):
                    return _safe_float(row_texts[idx])
                return None

            games_played_val = _get("games_played")

            return {
                "player_id":    id_str,
                "player_name":  player_name or id_str,
                "team":         "",  # filled by router
                "source":       "bcl",
                "competition":  "BCL",
                "season":       "2025-26",
                "date":         str(date.today()),
                "games_played": int(games_played_val) if games_played_val else None,
                "pts":          _get("pts"),
                "t2m":          _get("t2m"),
                "t2a":          _get("t2a"),
                "t2_pct":       _get("t2_pct"),
                "t3m":          _get("t3m"),
                "t3a":          _get("t3a"),
                "t3_pct":       _get("t3_pct"),
                "ftm":          _get("ftm"),
                "fta":          _get("fta"),
                "ft_pct":       _get("ft_pct"),
                "reb_off":      _get("reb_off"),
                "reb_def":      _get("reb_def"),
                "reb":          _get("reb"),
                "ast":          _get("ast"),
                "stl":          _get("stl"),
                "tov":          _get("tov"),
                "blk":          _get("blk"),
                "fouls":        _get("fouls"),
                "plus_minus":   _get("plus_minus"),
                "val":          _get("val"),
                "min":          _parse_minutes(_get("min")) if _get("min") else None,
            }

    logger.warning("BCL: could not find stats row for player_id=%s", player_id)
    return {}
