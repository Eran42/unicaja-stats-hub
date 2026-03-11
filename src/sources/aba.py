"""
ABA Liga data fetcher.

Endpoint:
  GET https://www.aba-liga.com/player/{player_id}/{season}/{league}/{slug}/

season codes:  25 = 2025-26,  24 = 2024-25
league codes:   1 = ABA Liga 1,  2 = ABA Liga 2

The page contains a stats table with 24 columns and rows for each game,
plus a "Total" row and an "Average" row at the bottom. We parse the Average row.

Column layout (0-indexed):
  0  Date / "Average"
  1  Opponent
  2  Min
  3  Pts
  4  %FG (overall)
  5  2pt made
  6  2pt attempted
  7  %2pt
  8  3pt made
  9  3pt attempted
  10 %3pt
  11 FT made
  12 FT attempted
  13 %FT
  14 reb_def (D)
  15 reb_off (O)
  16 reb total (T)
  17 ast
  18 stl
  19 tov
  20 blk (favour)
  21 blk (against) — ignored
  22 fouls committed
  23 fouls received — ignored
  24 +/-
  25 val
"""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.aba-liga.com/player"
_TIMEOUT  = 15
# season identifier: 25 = 2025-26 season
_CURRENT_SEASON = "25"
_LEAGUE = "1"  # ABA Liga 1

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
    Convert a player name to the ABA Liga URL slug.

    e.g. "Dragan Milosavljević" → "dragan-milosavljevic"
    """
    # Decompose unicode (ć → c + combining cedilla), keep only ASCII
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name  = normalized.encode("ascii", "ignore").decode("ascii")
    # Lowercase, replace spaces/underscores with hyphens, strip non-alphanumeric
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


def _cell_text(cell) -> str:
    return cell.get_text(strip=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
    season: str = _CURRENT_SEASON,
    league: str = _LEAGUE,
) -> dict:
    """
    Fetch season averages for an ABA Liga player.

    Args:
        player_id:    Numeric ABA Liga player ID.
        player_name:  Human-readable name used to build the URL slug.
        season:       Season code (e.g. "25" for 2025-26).
        league:       "1" for ABA Liga 1, "2" for ABA Liga 2.

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{player_id}/{season}/{league}/{slug}/"
    logger.debug("ABA fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
    except requests.HTTPError as exc:
        logger.warning("ABA HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("ABA request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------------------------------------------------
    # Locate the stats table — find the row whose first cell contains "Average"
    # -----------------------------------------------------------------------
    avg_row = None
    games_played: int | None = None

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        game_rows = 0
        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            first = _cell_text(cells[0]).lower()
            if "average" in first or "prosek" in first or "prosjek" in first:
                avg_row = cells
                break
            # Count game rows (rows with a date-like first cell)
            if re.match(r"\d{1,2}\.\d{1,2}\.", _cell_text(cells[0])):
                game_rows += 1
        if avg_row is not None:
            games_played = game_rows or None
            break

    if avg_row is None:
        logger.warning("ABA: could not find Average row for player_id=%s", player_id)
        return {}

    # -----------------------------------------------------------------------
    # Map cells to stat fields
    # ABA table has a variable leading column count depending on whether the
    # first column is Date or just a row label.  We detect by length.
    # Expected full row: label + 24 stat columns = 25+ cells
    # Stat columns start at index 1 (after the "Average" label cell).
    # -----------------------------------------------------------------------
    c = avg_row  # shorthand

    def _get(idx: int) -> str | None:
        """Return cell text at index idx, or None if out of range."""
        if idx < len(c):
            return _cell_text(c[idx])
        return None

    # Determine offset — some pages include an extra leading column (opponent logo)
    # We detect by checking if column 1 looks like minutes (e.g. "28:14" or "28.5")
    # vs something else. The safe approach: scan for the first numeric-looking cell.
    offset = 1  # default: skip the "Average" label in col 0

    # If the row has ≥ 26 cells, there's likely an extra leading column
    if len(c) >= 26:
        offset = 2

    def _s(rel: int) -> str | None:
        return _get(offset + rel)

    min_val  = _parse_minutes(_s(0))
    pts      = _safe_float(_s(1))
    # col 2: overall FG% — not in our schema, skip
    t2m      = _safe_float(_s(3))
    t2a      = _safe_float(_s(4))
    t2_pct   = _safe_float(_s(5))
    t3m      = _safe_float(_s(6))
    t3a      = _safe_float(_s(7))
    t3_pct   = _safe_float(_s(8))
    ftm      = _safe_float(_s(9))
    fta      = _safe_float(_s(10))
    ft_pct   = _safe_float(_s(11))
    reb_def  = _safe_float(_s(12))
    reb_off  = _safe_float(_s(13))
    reb      = _safe_float(_s(14))
    ast      = _safe_float(_s(15))
    stl      = _safe_float(_s(16))
    tov      = _safe_float(_s(17))
    blk      = _safe_float(_s(18))
    # col 19: blocks against — skip
    fouls    = _safe_float(_s(20))
    # col 21: fouls received — skip
    plus_minus = _safe_float(_s(22))
    val      = _safe_float(_s(23))

    return {
        "player_id":    str(player_id),
        "player_name":  player_name,
        "team":         "",   # filled by router from player registry
        "source":       "aba",
        "competition":  "ABA League",
        "season":       f"2025-26" if season == "25" else season,
        "date":         str(date.today()),
        "games_played": games_played,
        # Scoring
        "pts":          pts,
        # 2-point shooting
        "t2m":          t2m,
        "t2a":          t2a,
        "t2_pct":       t2_pct,
        # 3-point shooting
        "t3m":          t3m,
        "t3a":          t3a,
        "t3_pct":       t3_pct,
        # Free throws
        "ftm":          ftm,
        "fta":          fta,
        "ft_pct":       ft_pct,
        # Rebounds
        "reb_off":      reb_off,
        "reb_def":      reb_def,
        "reb":          reb,
        # Other
        "ast":          ast,
        "stl":          stl,
        "tov":          tov,
        "blk":          blk,
        "fouls":        fouls,
        "plus_minus":   plus_minus,
        "val":          val,
        "min":          min_val,
    }
