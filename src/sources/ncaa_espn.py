"""
NCAA college basketball latest game box score via ESPN game log.

Endpoint:
  GET https://www.espn.com/mens-college-basketball/player/gamelog/_/id/{id}/{slug}

The page has a game-log table. We take the last game row (most recent game).

Columns: Date, OPP, Result, MIN, FG (M-A), FG%, 3PT (M-A), 3P%, FT (M-A), FT%,
         REB, AST, BLK, STL, PF, TO, PTS

Note: ESPN game log does not provide offensive/defensive rebound splits.
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
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_slug(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name  = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().rstrip("%").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_made_attempted(cell_text: str) -> tuple[float | None, float | None]:
    text = cell_text.strip()
    if "-" in text:
        parts = text.split("-", 1)
        return _safe_float(parts[0]), _safe_float(parts[1])
    val = _safe_float(text)
    return val, None


def _cell_text(cell: Tag) -> str:
    return cell.get_text(separator=" ", strip=True)


def _find_col_map(header_row: list[str]) -> dict[str, int]:
    return {h.lower().strip(): idx for idx, h in enumerate(header_row)}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
) -> dict:
    """Fetch the most recent game box score for an NCAA player from ESPN."""
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{player_id}/{slug}"
    logger.debug("ESPN NCAA fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("ESPN NCAA HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("ESPN NCAA request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the game log table
    game_table: Tag | None = None
    col_map: dict[str, int] = {}

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [_cell_text(c).lower() for c in header_row.find_all(["th", "td"])]
        if "pts" in headers and ("fg" in headers or "min" in headers):
            game_table = table
            col_map    = _find_col_map(headers)
            break

    if game_table is None or not col_map:
        logger.warning("ESPN NCAA: no game log table for player_id=%s", player_id)
        return {}

    # Collect all valid game rows
    last_row_cells: list[Tag] | None = None
    last_game_date = ""
    last_opponent  = ""

    def _get(cells: list, key: str) -> str:
        idx = col_map.get(key)
        if idx is not None and idx < len(cells):
            return _cell_text(cells[idx])
        return ""

    for row in game_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells or cells[0].name == "th":
            continue
        pts_text = _get(cells, "pts")
        if not pts_text or not re.match(r"^\d", pts_text):
            continue
        # ESPN shows most recent game first — take the first valid row
        last_row_cells = cells
        last_game_date = _get(cells, "date")
        last_opponent  = _get(cells, "opp")
        break

    if last_row_cells is None:
        logger.warning("ESPN NCAA: no game rows for player_id=%s", player_id)
        return {}

    cells = last_row_cells

    fgm, fga   = _parse_made_attempted(_get(cells, "fg"))
    fg3m, fg3a = _parse_made_attempted(_get(cells, "3pt"))
    ftm, fta   = _parse_made_attempted(_get(cells, "ft"))

    t2m = round(fgm - fg3m, 1) if fgm is not None and fg3m is not None else None
    t2a = round(fga - fg3a, 1) if fga is not None and fg3a is not None else None
    t2_pct = round(t2m / t2a * 100, 1) if t2m is not None and t2a else None
    t3_pct = round(fg3m / fg3a * 100, 1) if fg3m is not None and fg3a else None
    ft_pct = round(ftm / fta * 100, 1) if ftm is not None and fta else None

    return {
        "player_id":   str(player_id),
        "player_name": player_name,
        "source":      "ncaa_espn",
        "competition": "NCAA",
        "season":      "2025-26",
        "game_date":   last_game_date,
        "opponent":    last_opponent,
        "result":      _get(cells, "result"),
        "date":        str(date.today()),
        "min":         _safe_float(_get(cells, "min")),
        "pts":         _safe_float(_get(cells, "pts")),
        "t2m":         t2m,   "t2a":  t2a,  "t2_pct":  t2_pct,
        "t3m":         fg3m,  "t3a":  fg3a, "t3_pct":  t3_pct,
        "ftm":         ftm,   "fta":  fta,  "ft_pct":  ft_pct,
        "reb_off":     None,
        "reb_def":     None,
        "reb":         _safe_float(_get(cells, "reb")),
        "ast":         _safe_float(_get(cells, "ast")),
        "stl":         _safe_float(_get(cells, "stl")),
        "tov":         _safe_float(_get(cells, "to")),
        "blk":         _safe_float(_get(cells, "blk")),
        "fouls":       _safe_float(_get(cells, "pf")),
        "plus_minus":  None,
        "val":         None,
    }
