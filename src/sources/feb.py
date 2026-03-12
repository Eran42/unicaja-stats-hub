"""
FEB (Spanish Basketball Federation) data fetcher.

Covers:
  - Primera FEB (second division)
  - LEB Oro (third division)

Endpoint:
  GET https://baloncestoenvivo.feb.es/jugador/{player_id}/

Player IDs come from the FEB website.

Returns the canonical full-stats dict.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_BASE_URL = "https://baloncestoenvivo.feb.es/jugador"
_TIMEOUT  = 20

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
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
    """Parse 'MM:SS' or decimal to decimal minutes."""
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


# ---------------------------------------------------------------------------
# Column header → stat field mapping
# ---------------------------------------------------------------------------

_HEADER_MAP: dict[str, str] = {
    "pj":   "games_played",
    "j":    "games_played",
    "min":  "min",
    "pts":  "pts",
    "t2c":  "t2m",
    "t2i":  "t2a",
    "%t2":  "t2_pct",
    "t3c":  "t3m",
    "t3i":  "t3a",
    "%t3":  "t3_pct",
    "tlc":  "ftm",
    "tli":  "fta",
    "%tl":  "ft_pct",
    "ro":   "reb_off",
    "rd":   "reb_def",
    "rt":   "reb",
    "as":   "ast",
    "br":   "stl",
    "bp":   "tov",
    "tp":   "blk",
    "fp":   "fouls",
    "+/-":  "plus_minus",
    "val":  "val",
    # Alternative spellings
    "tc":   "t2m",
    "ti":   "t2a",
    "reb":  "reb",
    "ast":  "ast",
    "rob":  "stl",
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

def fetch_player_stats(player_id: str | int) -> dict:
    """
    Fetch season averages for a FEB player.

    Args:
        player_id:  Numeric FEB player ID.

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    url = f"{_BASE_URL}/{player_id}/"
    logger.debug("FEB fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
    except requests.HTTPError as exc:
        logger.warning("FEB HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("FEB request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------------------------------------------------
    # Find player name (best effort)
    # -----------------------------------------------------------------------
    player_name = str(player_id)
    name_tag = soup.find("h1") or soup.find("h2")
    if name_tag:
        player_name = name_tag.get_text(strip=True) or player_name

    # -----------------------------------------------------------------------
    # Find the stats table — look for a row with "Media" or "Promedio"
    # (Spanish for Average), or the last data row in a stats table.
    # -----------------------------------------------------------------------
    avg_row: list[str] | None = None
    col_map: dict[str, int] = {}
    games_played: int | None = None

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Check for stat table headers
        header_cells = rows[0].find_all(["th", "td"])
        cmap = _build_col_map(header_cells)
        if len(cmap) < 4:
            continue

        game_count = 0
        found_row: list[str] | None = None

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            row_text = _cell_text(cells[0]).lower()

            # Average row: "media", "promedio", "average", or "totales" followed by last
            if any(kw in row_text for kw in ("media", "promedio", "average", "prom")):
                found_row = [_cell_text(c) for c in cells]
                break

            # Count numeric game rows
            if cells[0].get_text(strip=True).replace(".", "").isdigit() or \
               any(char.isdigit() for char in _cell_text(cells[0])):
                game_count += 1

        # If no explicit average row, compute from totals if a "total" row exists
        if found_row is None:
            for row in reversed(rows[1:]):
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                row_text = _cell_text(cells[0]).lower()
                if "total" in row_text or "totale" in row_text:
                    # Divide totals by game count
                    if game_count > 0:
                        total_cells = [_cell_text(c) for c in cells]
                        pts_idx = cmap.get("pts")
                        if pts_idx and pts_idx < len(total_cells):
                            total_pts = _safe_float(total_cells[pts_idx])
                            if total_pts is not None:
                                # Build averaged row
                                avg_cells: list[str] = []
                                for ci, txt in enumerate(total_cells):
                                    field = next((f for f, i in cmap.items() if i == ci), None)
                                    if field == "games_played":
                                        avg_cells.append(txt)
                                    elif field:
                                        v = _safe_float(txt)
                                        avg_cells.append(
                                            str(round(v / game_count, 1)) if v is not None else txt
                                        )
                                    else:
                                        avg_cells.append(txt)
                                found_row = avg_cells
                                games_played = game_count
                    break

        if found_row is not None:
            avg_row = found_row
            col_map = cmap
            if games_played is None:
                gp_idx = cmap.get("games_played")
                if gp_idx is not None and gp_idx < len(avg_row):
                    games_played = int(_safe_float(avg_row[gp_idx]) or 0) or None
            break

    if avg_row is None or not col_map:
        logger.warning("FEB: could not find stats row for player_id=%s", player_id)
        return {}

    def _get(field: str) -> float | None:
        idx = col_map.get(field)
        if idx is not None and idx < len(avg_row):
            return _safe_float(avg_row[idx])
        return None

    return {
        "player_id":    str(player_id),
        "player_name":  player_name,
        "team":         "",  # filled by router
        "source":       "feb",
        "competition":  "Primera FEB",
        "season":       "2025-26",
        "game_date":    "",   # FEB provides season averages, not per-game data
        "opponent":     "",
        "result":       "",
        "date":         str(date.today()),
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
