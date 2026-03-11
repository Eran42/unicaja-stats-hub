"""
Eurobasket.com data fetcher — covers multiple leagues including:
  - Greek Basket League (GBL)
  - LNB Pro A (France)
  - Other leagues tracked by basketball.eurobasket.com

Endpoint:
  GET https://basketball.eurobasket.com/player/{First-Last-Name}/{player_id}

Player IDs come from eurobasket.com player profile URLs.
The player name is used to build the URL slug (First-Last format, ASCII only).

The page renders stats tables server-side with abbreviated column headers:
  G, MIN, PTS, 2FGM, 2FGA, 2FG%, 3FGM, 3FGA, 3FG%, FTM, FTA, FT%,
  RO, RD, RT, AS, TO, ST, BS, PF, +/-, RNK

We target the "Averages" section and filter rows by the competition name
(e.g. "Greek League", "LNB Pro A") to get the current-season per-game stats.
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

_BASE_URL = "https://basketball.eurobasket.com/player"
_TIMEOUT  = 20

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

# Competition name fragments to match (case-insensitive)
_COMP_ALIASES: dict[str, list[str]] = {
    "Greek League":  ["greece", "greek", "gbl", "basket league", "esake"],
    "LNB Pro A":     ["lnb", "pro a", "france", "betclic"],
    "Primera FEB":   ["primera", "feb", "primera feb"],
    "LEB Oro":       ["leb oro", "leb"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_slug(name: str) -> str:
    """Convert player name to eurobasket URL slug: "First-Last" (ASCII)."""
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name  = normalized.encode("ascii", "ignore").decode("ascii")
    # Keep letters, digits, spaces, hyphens; replace others with hyphen
    slug = re.sub(r"[^a-zA-Z0-9\s-]+", "", ascii_name)
    slug = re.sub(r"[\s]+", "-", slug.strip())
    return slug


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().rstrip("%").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_minutes(value: Any) -> float | None:
    """Parse 'MM:SS' or decimal string to decimal minutes."""
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


def _comp_matches(cell_text: str, competition: str) -> bool:
    """Return True if cell_text matches the competition name."""
    text = cell_text.lower()
    # Direct match
    if competition.lower() in text:
        return True
    # Alias match
    for aliases in _COMP_ALIASES.values():
        if competition.lower() in [a.lower() for a in aliases]:
            if any(a.lower() in text for a in aliases):
                return True
    return False


# ---------------------------------------------------------------------------
# Column detection
# ---------------------------------------------------------------------------

# Header keyword → stat field
_HEADER_MAP: dict[str, str] = {
    "g":     "games_played",
    "gp":    "games_played",
    "min":   "min",
    "pts":   "pts",
    "2fgm":  "t2m",
    "2fg":   "t2m",
    "2m":    "t2m",
    "2a":    "t2a",
    "2fga":  "t2a",
    "2fg%":  "t2_pct",
    "2%":    "t2_pct",
    "3fgm":  "t3m",
    "3fg":   "t3m",
    "3m":    "t3m",
    "3a":    "t3a",
    "3fga":  "t3a",
    "3fg%":  "t3_pct",
    "3%":    "t3_pct",
    "ftm":   "ftm",
    "ft":    "ftm",
    "fta":   "fta",
    "ft%":   "ft_pct",
    "ro":    "reb_off",
    "or":    "reb_off",
    "rd":    "reb_def",
    "dr":    "reb_def",
    "rt":    "reb",
    "tr":    "reb",
    "reb":   "reb",
    "as":    "ast",
    "ast":   "ast",
    "st":    "stl",
    "stl":   "stl",
    "to":    "tov",
    "tov":   "tov",
    "bs":    "blk",
    "blk":   "blk",
    "pf":    "fouls",
    "cm":    "fouls",
    "+/-":   "plus_minus",
    "pm":    "plus_minus",
    "rnk":   "val",
    "val":   "val",
    "rank":  "val",
}


def _build_col_map(header_cells: list[Tag]) -> dict[str, int]:
    """Map stat field names to column indices based on header text."""
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

def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
    competition: str = "Greek League",
) -> dict:
    """
    Fetch season per-game averages for a player tracked on basketball.eurobasket.com.

    Args:
        player_id:    Eurobasket.com numeric player ID.
        player_name:  Used to build the URL slug (First-Last).
        competition:  Competition label to select the correct stats row
                      (e.g. "Greek League", "LNB Pro A").

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{slug}/{player_id}"
    logger.debug("Eurobasket fetch: %s (competition=%s)", url, competition)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
    except requests.HTTPError as exc:
        logger.warning("Eurobasket HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("Eurobasket request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------------------------------------------------
    # Strategy: look for a stats table near an "Averages" heading.
    # The page has multiple tables (Totals, Averages, Per40, etc.).
    # We scan all tables for one that has known stat column headers.
    # -----------------------------------------------------------------------
    best_row: list[str] | None = None
    col_map: dict[str, int] = {}
    games_played: int | None = None

    for table in soup.find_all("table"):
        # Find header row
        header_rows = table.find_all("tr")
        if not header_rows:
            continue

        hrow = header_rows[0]
        hcells = hrow.find_all(["th", "td"])
        cmap = _build_col_map(hcells)

        # Only consider tables that have multiple known stat columns
        if len(cmap) < 5:
            continue

        # Look for a data row matching the competition
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            row_texts = [_cell_text(c) for c in cells]
            row_full = " ".join(row_texts).lower()

            # Check if this row is for the target competition
            if _comp_matches(row_full, competition):
                # Validate: PTS column should have a number
                pts_idx = cmap.get("pts")
                if pts_idx is not None and pts_idx < len(cells):
                    if re.match(r"^\d", _cell_text(cells[pts_idx])):
                        best_row = row_texts
                        col_map  = cmap
                        gp_idx   = cmap.get("games_played")
                        if gp_idx is not None and gp_idx < len(cells):
                            games_played = int(_safe_float(_cell_text(cells[gp_idx])) or 0) or None
                        break

        if best_row:
            break

    # Fallback: if competition not found by name, take the first numeric data row
    # from any table that has the right columns
    if best_row is None:
        for table in soup.find_all("table"):
            header_rows = table.find_all("tr")
            if not header_rows:
                continue
            hcells = header_rows[0].find_all(["th", "td"])
            cmap = _build_col_map(hcells)
            if len(cmap) < 5:
                continue
            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                pts_idx = cmap.get("pts")
                if pts_idx is not None and pts_idx < len(cells):
                    txt = _cell_text(cells[pts_idx])
                    if re.match(r"^\d", txt):
                        best_row = [_cell_text(c) for c in cells]
                        col_map  = cmap
                        gp_idx   = cmap.get("games_played")
                        if gp_idx is not None and gp_idx < len(cells):
                            games_played = int(_safe_float(_cell_text(cells[gp_idx])) or 0) or None
                        break
            if best_row:
                break

    if best_row is None or not col_map:
        logger.warning(
            "Eurobasket: could not find stats row for player_id=%s competition=%s",
            player_id, competition,
        )
        return {}

    def _get(field: str) -> float | None:
        idx = col_map.get(field)
        if idx is not None and idx < len(best_row):
            return _safe_float(best_row[idx])
        return None

    return {
        "player_id":    str(player_id),
        "player_name":  player_name,
        "team":         "",  # filled by router
        "source":       "eurobasket",
        "competition":  competition,
        "season":       "2025-26",
        "date":         str(date.today()),
        "games_played": games_played,
        # Scoring
        "pts":          _get("pts"),
        # 2-point shooting
        "t2m":          _get("t2m"),
        "t2a":          _get("t2a"),
        "t2_pct":       _get("t2_pct"),
        # 3-point shooting
        "t3m":          _get("t3m"),
        "t3a":          _get("t3a"),
        "t3_pct":       _get("t3_pct"),
        # Free throws
        "ftm":          _get("ftm"),
        "fta":          _get("fta"),
        "ft_pct":       _get("ft_pct"),
        # Rebounds
        "reb_off":      _get("reb_off"),
        "reb_def":      _get("reb_def"),
        "reb":          _get("reb"),
        # Other
        "ast":          _get("ast"),
        "stl":          _get("stl"),
        "tov":          _get("tov"),
        "blk":          _get("blk"),
        "fouls":        _get("fouls"),
        "plus_minus":   _get("plus_minus"),
        "val":          _get("val"),
        "min":          _parse_minutes(_get("min")) if _get("min") else None,
    }
