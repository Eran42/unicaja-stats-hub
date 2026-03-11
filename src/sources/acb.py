"""
ACB (Liga Endesa) scraper.

Fetches player season stats from acb.com career pages.
URL: https://www.acb.com/jugador/temporada-a-temporada/id/{numeric_id}

The table shows season totals; we convert to per-game averages.
source_id in the registry must be the numeric ACB player ID (e.g. "20209226").

Full stat schema returned (per game, where applicable):
  games_played, min, pts,
  t2m, t2a, t2_pct,
  t3m, t3a, t3_pct,
  ftm, fta, ft_pct,
  reb_off, reb_def, reb,
  ast, stl, tov, blk, fouls, plus_minus, val
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.acb.com/jugador/temporada-a-temporada/id"
_TIMEOUT = 15
_SLEEP = 1  # polite delay between requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.acb.com/",
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
    """Parse minutes as total float. Handles '348', '19:20', '19.3'."""
    text = text.strip()
    if ":" in text:
        parts = text.split(":")
        try:
            return float(parts[0]) + float(parts[1]) / 60
        except (ValueError, IndexError):
            return None
    return _safe_float(text)


def _per_game(total: float | None, gp: int) -> float | None:
    if total is None or gp == 0:
        return None
    return round(total / gp, 2)


def _parse_shot_cell(text: str) -> tuple[float | None, float | None, float | None]:
    """
    Parse a shooting cell that may be:
      - Combined: '15/22/68.2%'  → (made=15, attempted=22, pct=68.2)
      - Single number: '15'      → (15, None, None)
    """
    text = text.strip()
    if "/" in text:
        parts = [p.strip().rstrip("%") for p in text.split("/")]
        if len(parts) == 3:
            return _safe_float(parts[0]), _safe_float(parts[1]), _safe_float(parts[2])
        if len(parts) == 2:
            return _safe_float(parts[0]), _safe_float(parts[1]), None
    return _safe_float(text), None, None


# ---------------------------------------------------------------------------
# Table parsing
# ---------------------------------------------------------------------------

# Expected flat column order after expanding colspans in the two-row header.
# Used when the table has ~24 cells per data row (shooting stats in separate cells).
_COLS_SPLIT = {
    "season": 0, "team": 1, "games": 2, "min": 3, "pts": 4,
    "t3m": 5, "t3a": 6, "t3_pct": 7,
    "t2m": 8, "t2a": 9, "t2_pct": 10,
    "ftm": 11, "fta": 12, "ft_pct": 13,
    "reb_off": 14, "reb_def": 15, "reb": 16,
    "ast": 17, "stl": 18, "tov": 19,
    "blk": 20, "fouls": 21, "plus_minus": 22, "val": 23,
}

# Expected flat column order when shooting stats are combined ('X/Y/Z%' cells).
# This gives ~15 cells per data row.
_COLS_COMBINED = {
    "season": 0, "team": 1, "games": 2, "min": 3, "pts": 4,
    "t3_combined": 5,   # '15/22/68.2%'
    "t2_combined": 6,   # '16/40/40.0%'
    "ft_combined": 7,   # '28/32/87.5%'
    "reb_off": 8, "reb_def": 9, "reb": 10,
    "ast": 11, "stl": 12, "tov": 13,
    "blk": 14, "fouls": 15, "plus_minus": 16, "val": 17,
}


def _flatten_headers(table: Tag) -> list[str]:
    """
    Expand a two-row nested header into a flat list of lowercase labels,
    handling colspan attributes.
    """
    rows = table.find_all("tr", limit=3)
    if not rows:
        return []

    # First header row: expand colspans
    top: list[str] = []
    for cell in rows[0].find_all(["th", "td"]):
        span = int(cell.get("colspan", 1))
        label = cell.get_text(strip=True).lower()
        top.extend([label] * span)

    if len(rows) < 2:
        return top

    # Second header row (sub-headers)
    sub: list[str] = [
        cell.get_text(strip=True).lower()
        for cell in rows[1].find_all(["th", "td"])
    ]

    flat: list[str] = []
    for i, t in enumerate(top):
        s = sub[i] if i < len(sub) else ""
        flat.append(f"{t}.{s}" if s else t)

    return flat


def _find_current_season_row(table: Tag) -> list[str] | None:
    """
    Return the cell texts from the most recent season row.
    Prefers rows labelled with a recent year; falls back to the last data row.
    """
    all_rows = table.find_all("tr")
    data_rows = []
    for row in all_rows:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        first = cells[0].get_text(strip=True).lower()
        # Skip obvious header rows
        if any(kw in first for kw in ("temp", "temporada", "season", "club", "jug")):
            continue
        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) >= 5 and any(texts):
            data_rows.append(texts)

    if not data_rows:
        return None

    # Prefer the row whose season label contains the current or most recent year
    current_year = date.today().year % 100  # e.g. 26 for 2026
    for row in reversed(data_rows):
        label = row[0].lower()
        # Labels like "25-26", "2025-26"
        for yr in (current_year, current_year - 1):
            if str(yr) in label or str(yr + 2000) in label:
                return row

    # Fall back to the last data row
    return data_rows[-1]


def _extract_stats(cells: list[str]) -> dict:
    """
    Convert a data row (list of cell text) into the canonical stat dict.
    Handles both the ~24-column split format and the ~15-column combined format.
    """
    result: dict[str, Any] = {
        "season_label": None,
        "games_played": None,
        "min_total": None,
        "pts_total": None,
        "t3m_total": None, "t3a_total": None, "t3_pct": None,
        "t2m_total": None, "t2a_total": None, "t2_pct": None,
        "ftm_total": None, "fta_total": None, "ft_pct": None,
        "reb_off_total": None, "reb_def_total": None, "reb_total": None,
        "ast_total": None, "stl_total": None, "tov_total": None,
        "blk_total": None, "fouls_total": None,
        "plus_minus_total": None, "val_total": None,
    }

    n = len(cells)

    def g(idx: int) -> str:
        return cells[idx].strip() if idx < n else ""

    # Detect format by checking if cell 5 looks like 'X/Y/Z%' (combined)
    cell5 = g(5)
    is_combined = "/" in cell5

    if is_combined:
        # ~15-column combined format
        c = _COLS_COMBINED
        result["season_label"] = g(c["season"])
        result["games_played"] = int(_safe_float(g(c["games"])) or 0)
        result["min_total"] = _parse_minutes(g(c["min"]))
        result["pts_total"] = _safe_float(g(c["pts"]))

        t3m, t3a, t3p = _parse_shot_cell(g(c["t3_combined"]))
        result["t3m_total"], result["t3a_total"], result["t3_pct"] = t3m, t3a, t3p

        t2m, t2a, t2p = _parse_shot_cell(g(c["t2_combined"]))
        result["t2m_total"], result["t2a_total"], result["t2_pct"] = t2m, t2a, t2p

        ftm, fta, ftp = _parse_shot_cell(g(c["ft_combined"]))
        result["ftm_total"], result["fta_total"], result["ft_pct"] = ftm, fta, ftp

        # Rebounds: check if combined or split
        reb_cell = g(c["reb_off"])
        if "/" in reb_cell:
            parts = reb_cell.split("/")
            result["reb_off_total"] = _safe_float(parts[0])
            result["reb_def_total"] = _safe_float(parts[1]) if len(parts) > 1 else None
            result["reb_total"] = _safe_float(parts[2]) if len(parts) > 2 else None
        else:
            result["reb_off_total"] = _safe_float(reb_cell)
            result["reb_def_total"] = _safe_float(g(c["reb_def"]))
            result["reb_total"] = _safe_float(g(c["reb"]))

        result["ast_total"] = _safe_float(g(c["ast"]))
        result["stl_total"] = _safe_float(g(c["stl"]))
        result["tov_total"] = _safe_float(g(c["tov"]))
        result["blk_total"] = _safe_float(g(c["blk"]))
        result["fouls_total"] = _safe_float(g(c["fouls"]))
        result["plus_minus_total"] = _safe_float(g(c["plus_minus"]))
        result["val_total"] = _safe_float(g(c["val"]))

    else:
        # ~24-column split format
        c = _COLS_SPLIT
        result["season_label"] = g(c["season"])
        result["games_played"] = int(_safe_float(g(c["games"])) or 0)
        result["min_total"] = _parse_minutes(g(c["min"]))
        result["pts_total"] = _safe_float(g(c["pts"]))

        result["t3m_total"] = _safe_float(g(c["t3m"]))
        result["t3a_total"] = _safe_float(g(c["t3a"]))
        result["t3_pct"] = _safe_float(g(c["t3_pct"]))
        result["t2m_total"] = _safe_float(g(c["t2m"]))
        result["t2a_total"] = _safe_float(g(c["t2a"]))
        result["t2_pct"] = _safe_float(g(c["t2_pct"]))
        result["ftm_total"] = _safe_float(g(c["ftm"]))
        result["fta_total"] = _safe_float(g(c["fta"]))
        result["ft_pct"] = _safe_float(g(c["ft_pct"]))

        result["reb_off_total"] = _safe_float(g(c["reb_off"]))
        result["reb_def_total"] = _safe_float(g(c["reb_def"]))
        result["reb_total"] = _safe_float(g(c["reb"]))
        result["ast_total"] = _safe_float(g(c["ast"]))
        result["stl_total"] = _safe_float(g(c["stl"]))
        result["tov_total"] = _safe_float(g(c["tov"]))
        result["blk_total"] = _safe_float(g(c["blk"]))
        result["fouls_total"] = _safe_float(g(c["fouls"]))
        result["plus_minus_total"] = _safe_float(g(c["plus_minus"]))
        result["val_total"] = _safe_float(g(c["val"]))

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(player_id: str) -> dict:
    """
    Scrape season stats for an ACB player.

    Args:
        player_id: The numeric ACB player ID (e.g. "20209226").

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    url = f"{_BASE_URL}/{player_id}"
    logger.debug("ACB fetch: %s", url)
    time.sleep(_SLEEP)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("ACB HTTP error for id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("ACB request failed for id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "lxml")

    # Player name from <h1> or <title>
    player_name = ""
    h1 = soup.find("h1")
    if h1:
        player_name = h1.get_text(strip=True)
    if not player_name:
        title = soup.find("title")
        if title:
            player_name = title.get_text(strip=True).split("|")[0].strip()

    # Find the stats table
    tables = soup.find_all("table")
    target_table: Tag | None = None
    for tbl in tables:
        text = tbl.get_text(" ", strip=True).lower()
        if any(kw in text for kw in ("jug", "puntos", "tiros", "rebotes", "temporada")):
            target_table = tbl
            break
    if target_table is None and tables:
        target_table = tables[0]

    if target_table is None:
        logger.warning("ACB: no stats table found for id=%s", player_id)
        return {}

    cells = _find_current_season_row(target_table)
    if cells is None:
        logger.warning("ACB: no data rows found for id=%s", player_id)
        return {}

    raw = _extract_stats(cells)
    gp: int = raw["games_played"] or 0

    if gp == 0:
        logger.warning("ACB: games_played=0 for id=%s, skipping per-game calc.", player_id)

    # Convert totals to per-game averages
    def pg(key: str) -> float | None:
        return _per_game(raw.get(key), gp)

    # Minutes per game
    min_pg: float | None = None
    if raw["min_total"] is not None and gp:
        min_pg = round(raw["min_total"] / gp, 1)

    return {
        "player_id": player_id,
        "player_name": player_name or player_id,
        "date": str(date.today()),
        "season": raw.get("season_label", ""),
        "source": "acb",
        "games_played": gp or None,
        # Per-game
        "min":       min_pg,
        "pts":       pg("pts_total"),
        "t2m":       pg("t2m_total"),
        "t2a":       pg("t2a_total"),
        "t2_pct":    raw.get("t2_pct"),      # already a percentage
        "t3m":       pg("t3m_total"),
        "t3a":       pg("t3a_total"),
        "t3_pct":    raw.get("t3_pct"),
        "ftm":       pg("ftm_total"),
        "fta":       pg("fta_total"),
        "ft_pct":    raw.get("ft_pct"),
        "reb_off":   pg("reb_off_total"),
        "reb_def":   pg("reb_def_total"),
        "reb":       pg("reb_total"),
        "ast":       pg("ast_total"),
        "stl":       pg("stl_total"),
        "tov":       pg("tov_total"),
        "blk":       pg("blk_total"),
        "fouls":     pg("fouls_total"),
        "plus_minus": pg("plus_minus_total"),
        "val":       pg("val_total"),
    }
