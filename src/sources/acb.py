"""
ACB (Liga Endesa) scraper — latest game box score.

URL: https://www.acb.com/jugador/todos-sus-partidos/id/{numeric_id}

The page lists every game played by the player this season.
We take the last row in the table (most recent game).

Column layout (game log):
  J (game#), PARTIDOS (opponent), Res. (result), Min.,
  PT (pts), T2 (combined M/A/%), T3 (combined M/A/%), T1 (FT combined M/A/%),
  T(D+O) (reb def/off/total), A (ast), BR (stl), BP (tov),
  C (fouls committed), F+C / M / F (foul details), +/-, V (val)
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.acb.com/jugador/todos-sus-partidos/id"
_TIMEOUT  = 15
_SLEEP    = 1

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer":         "https://www.acb.com/",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".").rstrip("%")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_minutes(text: str) -> float | None:
    text = text.strip()
    if ":" in text:
        parts = text.split(":")
        try:
            return round(float(parts[0]) + float(parts[1]) / 60, 2)
        except (ValueError, IndexError):
            return None
    return _safe_float(text)


def _parse_shot_cell(text: str) -> tuple[float | None, float | None, float | None]:
    """Parse 'M/A/%' combined cell → (made, attempted, pct)."""
    text = text.strip()
    if "/" in text:
        parts = [p.strip().rstrip("%") for p in text.split("/")]
        if len(parts) == 3:
            return _safe_float(parts[0]), _safe_float(parts[1]), _safe_float(parts[2])
        if len(parts) == 2:
            return _safe_float(parts[0]), _safe_float(parts[1]), None
    return _safe_float(text), None, None


def _parse_reb_cell(text: str) -> tuple[float | None, float | None, float | None]:
    """
    Parse rebounds cell.
    Formats: 'D+O' like '5+2', 'D/O/T' like '5/2/7', or plain number.
    Returns (reb_def, reb_off, reb_total).
    """
    text = text.strip()
    if "+" in text:
        parts = text.split("+")
        d = _safe_float(parts[0])
        o = _safe_float(parts[1]) if len(parts) > 1 else None
        t = (d + o) if d is not None and o is not None else None
        return d, o, t
    if "/" in text:
        parts = text.split("/")
        d = _safe_float(parts[0])
        o = _safe_float(parts[1]) if len(parts) > 1 else None
        t = _safe_float(parts[2]) if len(parts) > 2 else (
            (d + o) if d is not None and o is not None else None
        )
        return d, o, t
    total = _safe_float(text)
    return None, None, total


# ---------------------------------------------------------------------------
# Game row parsing
# ---------------------------------------------------------------------------

def _is_game_row(cells: list[Tag]) -> bool:
    """Return True if this row looks like an individual game row (not header/total/avg)."""
    if len(cells) < 5:
        return False
    first = cells[0].get_text(strip=True)
    # Game rows typically start with a game number or date
    return bool(re.match(r"^\d+", first))


def _parse_game_row(cells: list[Tag]) -> dict:
    """
    Parse one game row from the ACB game log.

    The column layout (0-indexed after the leading game# cell) is:
      0: game#  1: opponent/partidos  2: result  3: minutes
      4: pts    5: T2 (M/A/%)        6: T3       7: T1 (FT)
      8: T(D+O) or separate reb cols ...
    """
    n = len(cells)

    def g(idx: int) -> str:
        return cells[idx].get_text(strip=True) if idx < n else ""

    # Opponent/date may be in cell 1 (includes date info sometimes)
    opponent = g(1)
    result   = g(2)
    min_val  = _parse_minutes(g(3))
    pts      = _safe_float(g(4))

    # Shooting columns — detect combined vs split
    cell5 = g(5)
    if "/" in cell5:
        # Combined format: T2, T3, T1 each as "M/A/%"
        t2m, t2a, t2_pct = _parse_shot_cell(g(5))
        t3m, t3a, t3_pct = _parse_shot_cell(g(6))
        ftm, fta, ft_pct = _parse_shot_cell(g(7))
        reb_offset = 8
    else:
        # Split format: separate columns for each
        t3m  = _safe_float(g(5))
        t3a  = _safe_float(g(6))
        t3_pct = _safe_float(g(7))
        t2m  = _safe_float(g(8))
        t2a  = _safe_float(g(9))
        t2_pct = _safe_float(g(10))
        ftm  = _safe_float(g(11))
        fta  = _safe_float(g(12))
        ft_pct = _safe_float(g(13))
        reb_offset = 14

    # Rebounds
    reb_cell = g(reb_offset)
    if "+" in reb_cell or ("/" in reb_cell and reb_cell.count("/") >= 2):
        reb_def, reb_off, reb = _parse_reb_cell(reb_cell)
        ao = reb_offset + 1
    else:
        reb_def = _safe_float(reb_cell)
        reb_off = _safe_float(g(reb_offset + 1))
        reb     = _safe_float(g(reb_offset + 2))
        ao = reb_offset + 3

    ast   = _safe_float(g(ao))
    stl   = _safe_float(g(ao + 1))
    tov   = _safe_float(g(ao + 2))
    fouls = _safe_float(g(ao + 3))

    # +/- and val are near the end
    # Scan from the end: last cell = val, second-to-last = +/-
    plus_minus = _safe_float(g(n - 2)) if n >= 2 else None
    val        = _safe_float(g(n - 1))

    return {
        "opponent":   opponent,
        "result":     result,
        "min":        min_val,
        "pts":        pts,
        "t2m":        t2m,    "t2a":    t2a,    "t2_pct":  t2_pct,
        "t3m":        t3m,    "t3a":    t3a,    "t3_pct":  t3_pct,
        "ftm":        ftm,    "fta":    fta,    "ft_pct":  ft_pct,
        "reb_def":    reb_def, "reb_off": reb_off, "reb": reb,
        "ast":        ast,
        "stl":        stl,
        "tov":        tov,
        "fouls":      fouls,
        "plus_minus": plus_minus,
        "val":        val,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(player_id: str) -> dict:
    """
    Fetch the most recent game box score for an ACB player.

    Args:
        player_id: Numeric ACB player ID.

    Returns:
        Canonical single-game stats dict, or empty dict on failure.
    """
    url = f"{_BASE_URL}/{player_id}"
    logger.debug("ACB game log fetch: %s", url)
    time.sleep(_SLEEP)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("ACB HTTP error id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("ACB request failed id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "lxml")

    # Player name
    player_name = ""
    h1 = soup.find("h1")
    if h1:
        player_name = h1.get_text(strip=True)

    # Find the game log table
    target_table: Tag | None = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text(" ", strip=True).lower()
        if any(kw in text for kw in ("partidos", "rival", "jornada", "resultado")):
            target_table = tbl
            break
    if target_table is None:
        tables = soup.find_all("table")
        if tables:
            target_table = tables[0]

    if target_table is None:
        logger.warning("ACB: no game log table found for id=%s", player_id)
        return {}

    # Collect all game rows
    game_rows: list[list[Tag]] = []
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if _is_game_row(cells):
            game_rows.append(cells)

    if not game_rows:
        logger.warning("ACB: no game rows found for id=%s", player_id)
        return {}

    # Most recent = last row
    last_cells = game_rows[-1]
    stats = _parse_game_row(last_cells)

    # Try to extract game date from opponent/partidos cell text
    game_date = str(date.today())
    partidos_text = last_cells[1].get_text(strip=True) if len(last_cells) > 1 else ""
    date_match = re.search(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})", partidos_text)
    if date_match:
        game_date = partidos_text  # store as-is; best effort

    return {
        "player_id":   player_id,
        "player_name": player_name or player_id,
        "source":      "acb",
        "competition": "ACB",
        "season":      "2025-26",
        "game_date":   game_date,
        "opponent":    stats.get("opponent", ""),
        "result":      stats.get("result", ""),
        "date":        str(date.today()),
        "min":         stats["min"],
        "pts":         stats["pts"],
        "t2m":         stats["t2m"],   "t2a":  stats["t2a"],  "t2_pct":  stats["t2_pct"],
        "t3m":         stats["t3m"],   "t3a":  stats["t3a"],  "t3_pct":  stats["t3_pct"],
        "ftm":         stats["ftm"],   "fta":  stats["fta"],  "ft_pct":  stats["ft_pct"],
        "reb_off":     stats["reb_off"],
        "reb_def":     stats["reb_def"],
        "reb":         stats["reb"],
        "ast":         stats["ast"],
        "stl":         stats["stl"],
        "tov":         stats["tov"],
        "blk":         None,  # not in ACB game log columns
        "fouls":       stats["fouls"],
        "plus_minus":  stats["plus_minus"],
        "val":         stats["val"],
    }
