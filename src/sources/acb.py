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
    """Parse combined shooting cell → (made, attempted, pct).

    Handles formats:
      'M/A/%'      e.g. '10/14/71.4'
      'M/A pct%'   e.g. '10/14 71.0%'  (ACB game log format)
      'M/A'        e.g. '10/14'
    """
    text = text.strip()
    if "/" in text:
        slash_parts = text.split("/")
        made = _safe_float(slash_parts[0].strip())
        if len(slash_parts) >= 2:
            # Second part may be 'A pct%' or 'A/pct' or just 'A'
            second = slash_parts[1].strip()
            # Split on whitespace to separate attempted from pct
            tokens = second.split()
            attempted = _safe_float(tokens[0].rstrip("%"))
            if len(slash_parts) == 3:
                pct = _safe_float(slash_parts[2].strip().rstrip("%"))
            elif len(tokens) >= 2:
                pct = _safe_float(tokens[1].rstrip("%"))
            else:
                pct = None
            return made, attempted, pct
    return _safe_float(text), None, None


def _parse_reb_cell(text: str) -> tuple[float | None, float | None, float | None]:
    """
    Parse rebounds cell.
    Formats:
      'T(D+O)' like '4(2+2)' → total=4, def=2, off=2
      'D+O'    like '5+2'    → def=5, off=2, total=7
      'D/O/T'  like '5/2/7'  → def=5, off=2, total=7
      plain number            → total only
    Returns (reb_def, reb_off, reb_total).
    """
    text = text.strip()

    # Format: '4(2+2)' or '4 (2+2)' — total before parens, D+O inside
    paren_match = re.match(r"(\d+)\s*\((\d+)\+(\d+)\)", text)
    if paren_match:
        t = _safe_float(paren_match.group(1))
        d = _safe_float(paren_match.group(2))
        o = _safe_float(paren_match.group(3))
        return d, o, t

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
# Header detection
# ---------------------------------------------------------------------------

# ACB game log header labels (lowercase) → canonical field
_HEADER_MAP: dict[str, str] = {
    "partidos": "opponent",
    "res.":     "result",   "res": "result",
    "min.":     "min",      "min": "min",
    "pt":       "pts",      "pts": "pts",
    "t2":       "t2_combined",
    "t3":       "t3_combined",
    "t1":       "ft_combined",
    "t(d+o)":   "reb_combined",
    "a":        "ast",
    "br":       "stl",
    "bp":       "tov",
    "c":        "fouls",
    "+/-":      "plus_minus",  "+/--": "plus_minus",
    "v":        "val",
    # split variants
    "t2c":  "t2m",  "t2i": "t2a",  "%t2": "t2_pct",
    "t3c":  "t3m",  "t3i": "t3a",  "%t3": "t3_pct",
    "tlc":  "ftm",  "tli": "fta",  "%tl": "ft_pct",
    "ro":   "reb_off", "rd": "reb_def", "rt": "reb",
    "d":    "reb_def", "o":  "reb_off",
}


def _build_col_map(header_cells: list[Tag]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for idx, cell in enumerate(header_cells):
        key = cell.get_text(strip=True).lower()
        field = _HEADER_MAP.get(key)
        if field and field not in mapping:
            mapping[field] = idx
    return mapping


# ---------------------------------------------------------------------------
# Game row parsing
# ---------------------------------------------------------------------------

def _is_game_row(cells: list[Tag]) -> bool:
    """Return True if this row looks like an individual game row."""
    if len(cells) < 5:
        return False
    first = cells[0].get_text(strip=True)
    return bool(re.match(r"^\d+", first))


def _parse_game_row(cells: list[Tag], col_map: dict[str, int]) -> dict:
    """Parse one game row using header-derived column positions."""
    n = len(cells)

    def g(idx: int) -> str:
        return cells[idx].get_text(strip=True) if idx < n else ""

    def _at(field: str) -> str:
        idx = col_map.get(field)
        return g(idx) if idx is not None else ""

    # Opponent and result — use header-detected positions, fall back to col 1/2
    opp_idx = col_map.get("opponent")
    res_idx = col_map.get("result")
    opponent = g(opp_idx) if opp_idx is not None else g(1)
    result   = g(res_idx) if res_idx is not None else g(2)

    min_val = _parse_minutes(_at("min"))
    pts     = _safe_float(_at("pts"))

    # Shooting — combined cells take priority
    t2_raw = _at("t2_combined")
    if t2_raw:
        t2m, t2a, t2_pct = _parse_shot_cell(t2_raw)
    else:
        t2m  = _safe_float(_at("t2m"))
        t2a  = _safe_float(_at("t2a"))
        t2_pct = _safe_float(_at("t2_pct"))

    t3_raw = _at("t3_combined")
    if t3_raw:
        t3m, t3a, t3_pct = _parse_shot_cell(t3_raw)
    else:
        t3m  = _safe_float(_at("t3m"))
        t3a  = _safe_float(_at("t3a"))
        t3_pct = _safe_float(_at("t3_pct"))

    ft_raw = _at("ft_combined")
    if ft_raw:
        ftm, fta, ft_pct = _parse_shot_cell(ft_raw)
    else:
        ftm  = _safe_float(_at("ftm"))
        fta  = _safe_float(_at("fta"))
        ft_pct = _safe_float(_at("ft_pct"))

    # Rebounds
    reb_raw = _at("reb_combined")
    if reb_raw:
        reb_def, reb_off, reb = _parse_reb_cell(reb_raw)
    else:
        reb_def = _safe_float(_at("reb_def"))
        reb_off = _safe_float(_at("reb_off"))
        reb     = _safe_float(_at("reb"))

    return {
        "opponent":   opponent,
        "result":     result,
        "min":        min_val,
        "pts":        pts,
        "t2m":        t2m,    "t2a":    t2a,    "t2_pct":  t2_pct,
        "t3m":        t3m,    "t3a":    t3a,    "t3_pct":  t3_pct,
        "ftm":        ftm,    "fta":    fta,    "ft_pct":  ft_pct,
        "reb_def":    reb_def, "reb_off": reb_off, "reb": reb,
        "ast":        _safe_float(_at("ast")),
        "stl":        _safe_float(_at("stl")),
        "tov":        _safe_float(_at("tov")),
        "fouls":      _safe_float(_at("fouls")),
        "plus_minus": _safe_float(_at("plus_minus")),
        "val":        _safe_float(_at("val")),
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

    # Find the game log table — pick the one whose header row has the most
    # recognised columns (avoids picking the season-summary table first)
    target_table: Tag | None = None
    col_map: dict[str, int] = {}
    for tbl in soup.find_all("table"):
        tbl_rows = tbl.find_all("tr")
        for row in tbl_rows[:3]:
            cells = row.find_all(["th", "td"])
            candidate = _build_col_map(cells)
            if len(candidate) > len(col_map):
                col_map = candidate
                target_table = tbl

    if target_table is None:
        logger.warning("ACB: no game log table found for id=%s", player_id)
        return {}

    # Collect all game rows
    all_rows = target_table.find_all("tr")
    game_rows: list[list[Tag]] = []
    for row in all_rows:
        cells = row.find_all(["td", "th"])
        if _is_game_row(cells):
            game_rows.append(cells)

    if not game_rows:
        logger.warning("ACB: no game rows found for id=%s", player_id)
        return {}

    # Most recent *played* game = last row that has actual stats
    # Future scheduled games appear at the bottom with no stats (only a time in opponent column)
    last_cells: list[Tag] | None = None
    stats: dict = {}
    for cells in reversed(game_rows):
        candidate = _parse_game_row(cells, col_map)
        if candidate.get("min") is not None or candidate.get("pts") is not None:
            last_cells = cells
            stats = candidate
            break

    if last_cells is None:
        logger.warning("ACB: no played game rows found for id=%s", player_id)
        return {}

    # Extract game date from the linked game page in the PARTIDOS cell
    # The row cells don't contain dates, but the opponent link goes to /partido/ver/id/XXXXX
    game_date = ""
    for cell in last_cells:  # type: ignore[union-attr]
        link = cell.find("a", href=re.compile(r"/partido/ver/id/\d+"))
        if not link:
            continue
        game_url = "https://www.acb.com" + link["href"]
        try:
            time.sleep(0.5)
            gr = requests.get(game_url, headers=_HEADERS, timeout=_TIMEOUT)
            gr.raise_for_status()
            page_text = gr.text
            for dm in re.finditer(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", page_text):
                day, month, year = dm.group(1), dm.group(2), dm.group(3)
                if not (2020 <= int(year) <= 2035):
                    continue
                if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
                    continue
                try:
                    game_date = f"{year}-{int(month):02d}-{int(day):02d}"
                except ValueError:
                    pass
                break
        except requests.RequestException:
            pass
        break

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
