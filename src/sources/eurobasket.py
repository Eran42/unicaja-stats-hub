"""
Eurobasket.com latest game box score.

URL: https://basketball.eurobasket.com/player/{First-Last-Name}/{player_id}

The player page has a "Details" section (game-by-game table).
We take the last row (most recent game).

Covers: Greek Basket League, LNB Pro A, and other leagues.
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
    "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language":         "en-US,en;q=0.9",
    "Accept-Encoding":         "gzip, deflate, br",
    "Connection":              "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control":           "max-age=0",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_slug(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name  = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9\s-]+", "", ascii_name)
    return re.sub(r"\s+", "-", slug.strip())


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


def _parse_date_flexible(raw: str) -> str:
    """
    Try common date formats and return YYYY-MM-DD, or the raw string if none match.

    Handles: '15 Feb 2026', 'Feb 15, 2026', '15/02/2026', '02/15/2026',
             '2026-02-15', '15.02.2026', 'Feb 15'.
    For formats without a year, the season 2025-26 rule applies:
      month >= 9 → 2025, month <= 8 → 2026.
    """
    from datetime import datetime as _dt
    raw = raw.strip()
    if not raw:
        return raw

    # Already ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    candidates = [
        ("%d %b %Y", True),
        ("%b %d, %Y", True),
        ("%d/%m/%Y", True),
        ("%m/%d/%Y", True),
        ("%d.%m.%Y", True),
        ("%b %d", False),     # no year
        ("%d %b", False),     # no year
    ]
    for fmt, has_year in candidates:
        try:
            dt = _dt.strptime(raw, fmt)
            if not has_year:
                year = 2025 if dt.month >= 9 else 2026
                dt = dt.replace(year=year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def _parse_result(text: str) -> str:
    """
    Parse a result/score cell into canonical 'V 85-76' / 'D 76-85' format.

    Recognises score patterns ('85-76', '85:76') and W/L indicators.
    """
    text = text.strip()
    score_m = re.search(r"(\d{2,3})[:\-](\d{2,3})", text)
    score_str = f"{score_m.group(1)}-{score_m.group(2)}" if score_m else ""

    if re.search(r"\b(win|won|victory|victoria|w)\b", text, re.I):
        wl = "V"
    elif re.search(r"\b(loss|lost|defeat|derrota|l)\b", text, re.I):
        wl = "D"
    else:
        wl = ""

    if score_str and wl:
        return f"{wl} {score_str}"
    return score_str or wl


# Column header → stat field
_HEADER_MAP: dict[str, str] = {
    "min": "min", "pts": "pts",
    "2fgm": "t2m", "2fg": "t2m", "2m": "t2m",
    "2fga": "t2a", "2a": "t2a",
    "2fg%": "t2_pct", "2%": "t2_pct",
    "3fgm": "t3m", "3fg": "t3m", "3m": "t3m",
    "3fga": "t3a", "3a": "t3a",
    "3fg%": "t3_pct", "3%": "t3_pct",
    "ftm": "ftm", "ft": "ftm",
    "fta": "fta",
    "ft%": "ft_pct",
    "ro": "reb_off", "or": "reb_off",
    "rd": "reb_def", "dr": "reb_def",
    "rt": "reb", "tr": "reb", "reb": "reb",
    "as": "ast", "ast": "ast",
    "st": "stl", "stl": "stl",
    "to": "tov", "tov": "tov",
    "bs": "blk", "blk": "blk",
    "pf": "fouls", "cm": "fouls",
    "+/-": "plus_minus",
    "rnk": "val", "val": "val",
}


def _build_col_map(header_cells: list[Tag]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for idx, cell in enumerate(header_cells):
        key = _cell_text(cell).lower().strip()
        field = _HEADER_MAP.get(key)
        if field and field not in mapping:
            mapping[field] = idx
    return mapping


def _is_game_row(cells: list[Tag], col_map: dict[str, int]) -> bool:
    """True if the row contains numeric game data (not header/total/average/standings)."""
    if len(cells) < 5:
        return False
    pts_idx = col_map.get("pts")
    if pts_idx and pts_idx < len(cells):
        txt = _cell_text(cells[pts_idx])
        if re.match(r"^\d", txt):
            first = _cell_text(cells[0]).lower()
            # Reject summary/standings rows by keyword
            if any(kw in first for kw in (
                "total", "average", "avg", "media", "sum",
                "win", "loss", "draw", "pts", "gp", "played",
            )):
                return False
            # First cell must contain at least one digit (dates do; labels like "Wins" don't)
            if not re.search(r"\d", first):
                return False
            return True
    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
    competition: str = "Greek League",
) -> dict:
    """Fetch the most recent game box score for a player on basketball.eurobasket.com."""
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{slug}/{player_id}"
    logger.debug("Eurobasket fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("Eurobasket HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("Eurobasket request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Look for any table with a date/game column and stat columns
    last_row:   list[str] | None = None
    last_cells: list[Tag]  | None = None
    col_map:    dict[str, int]    = {}
    game_date = ""

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        hcells = rows[0].find_all(["th", "td"])
        cmap   = _build_col_map(hcells)
        if len(cmap) < 5:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if _is_game_row(cells, cmap):
                last_row   = [_cell_text(c) for c in cells]
                last_cells = cells
                col_map    = cmap
                game_date  = _cell_text(cells[0])

    if last_row is None or last_cells is None or not col_map:
        logger.warning(
            "Eurobasket: no game rows found for player_id=%s", player_id
        )
        return {}

    # Identify how many leading context columns (date, opponent, result) precede stats.
    # The minimum stat-column index in col_map tells us where stats begin.
    first_stat_col = min(col_map.values())  # e.g. if "min" is col 3, context fills cols 0-2

    # Extract context cells by position
    opponent   = last_row[1] if first_stat_col > 1 and len(last_row) > 1 else ""
    result_raw = last_row[2] if first_stat_col > 2 and len(last_row) > 2 else ""
    # Only treat cells[2] as result if it doesn't look like a pure stat number
    if result_raw and re.match(r"^\d+(\.\d+)?$", result_raw):
        result_raw = ""
    result = _parse_result(result_raw)

    def _get(field: str) -> float | None:
        idx = col_map.get(field)
        if idx is not None and idx < len(last_row):
            return _safe_float(last_row[idx])
        return None

    return {
        "player_id":   str(player_id),
        "player_name": player_name,
        "source":      "eurobasket",
        "competition": competition,
        "season":      "2025-26",
        "game_date":   _parse_date_flexible(game_date),
        "opponent":    opponent,
        "result":      result,
        "date":        str(date.today()),
        "min":         _parse_minutes(_get("min")) if _get("min") else None,
        "pts":         _get("pts"),
        "t2m":         _get("t2m"),    "t2a":  _get("t2a"),  "t2_pct":  _get("t2_pct"),
        "t3m":         _get("t3m"),    "t3a":  _get("t3a"),  "t3_pct":  _get("t3_pct"),
        "ftm":         _get("ftm"),    "fta":  _get("fta"),  "ft_pct":  _get("ft_pct"),
        "reb_off":     _get("reb_off"),
        "reb_def":     _get("reb_def"),
        "reb":         _get("reb"),
        "ast":         _get("ast"),
        "stl":         _get("stl"),
        "tov":         _get("tov"),
        "blk":         _get("blk"),
        "fouls":       _get("fouls"),
        "plus_minus":  _get("plus_minus"),
        "val":         _get("val"),
    }
