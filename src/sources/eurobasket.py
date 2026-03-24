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

    # Eurobasket.com uses M/D/YYYY — try that before D/M/YYYY.
    # For unambiguous cases (day > 12 or month > 12) strptime will reject the
    # wrong format automatically; for ambiguous ones (e.g. 3/11) M/D wins.
    candidates = [
        ("%d %b %Y", True),
        ("%b %d, %Y", True),
        ("%m/%d/%Y", True),
        ("%d/%m/%Y", True),
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


def _parse_ma_cell(text: str) -> tuple[float | None, float | None]:
    """Parse 'M-A' compound format (e.g. '3-7') into (made, attempts).

    Returns (None, None) for plain numbers or empty strings.
    """
    text = str(text).strip()
    m = re.match(r"^(\d+)[/\-](\d+)$", text)
    if m:
        return float(m.group(1)), float(m.group(2))
    return None, None


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


# Competition keyword matching — used to filter "Details" tables by the nearest
# <h4> heading ("Season: 2025-2026 (Greece-GBL)" etc.).
# Keys are lower-case canonical competition names (partial match OK).
# Values are substrings expected in the heading, any one of which counts as a match.
_COMP_KEYWORDS: dict[str, list[str]] = {
    "greek league":  ["greece", "gbl"],
    "lnb pro a":     ["france", "betclic", "pro a", "elite"],
    "aba league":    ["aba"],
    "euroleague":    ["euroleague"],
    "eurocup":       ["eurocup", "7days"],
    "bcl":           ["champions league", "bcl"],
    "acb":           ["acb"],
}


def _heading_matches(heading: str, competition: str) -> bool:
    """Return True if *heading* is consistent with the requested *competition*.

    Both arguments are compared case-insensitively.  If no keyword entry
    matches the competition, we default to True (no filtering).
    """
    h = heading.lower()
    c = competition.lower()
    keywords = None
    for key, kws in _COMP_KEYWORDS.items():
        if key in c or any(kw in c for kw in kws):
            keywords = kws
            break
    if not keywords:
        return True   # unknown competition → don't filter
    return any(kw in h for kw in keywords)


# Column header → stat field
_HEADER_MAP: dict[str, str] = {
    "min": "min", "pts": "pts",
    # Separate made/attempt/pct columns
    "2fgm": "t2m", "2fg": "t2m", "2m": "t2m",
    "2fga": "t2a", "2a": "t2a",
    "2fg%": "t2_pct", "2%": "t2_pct",
    "3fgm": "t3m", "3fg": "t3m", "3m": "t3m",
    "3fga": "t3a", "3a": "t3a",
    "3fg%": "t3_pct", "3%": "t3_pct",
    "ftm": "ftm", "fta": "fta",
    "ft%": "ft_pct",
    # Compound M-A columns (eurobasket.com "Details" tables)
    "2fgp": "t2m",   # value is "M-A"; t2a derived in extraction
    "3fgp": "t3m",   # value is "M-A"; t3a derived in extraction
    "ft":   "ftm",   # value is "M-A"; fta derived in extraction
    # Context columns used to extract opponent/result by name rather than position
    "against team": "_opponent",
    "result":       "_result_col",
    # Remaining stats
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

def _extract_record(
    row_data: list[str],
    col_map: dict[str, int],
    player_id: str | int,
    player_name: str,
    competition: str,
    today: str,
) -> dict | None:
    """Convert a single game row into a canonical stats record.

    Returns None if the row lacks a parseable game date.
    """
    game_date_raw = row_data[0]
    game_date = _parse_date_flexible(game_date_raw)
    if not game_date or not re.match(r"\d{4}-\d{2}-\d{2}", game_date):
        return None

    def _get(field: str) -> float | None:
        idx = col_map.get(field)
        return _safe_float(row_data[idx]) if idx is not None and idx < len(row_data) else None

    def _raw(field: str) -> str | None:
        idx = col_map.get(field)
        return row_data[idx] if idx is not None and idx < len(row_data) else None

    stat_indices = [col_map[k] for k in col_map if not k.startswith("_")]
    first_stat_col = min(stat_indices) if stat_indices else 0

    if "_opponent" in col_map:
        idx = col_map["_opponent"]
        opponent = row_data[idx] if idx < len(row_data) else ""
    elif first_stat_col > 1 and len(row_data) > 1:
        opponent = row_data[1]
    else:
        opponent = ""

    if "_result_col" in col_map:
        idx = col_map["_result_col"]
        result_raw = row_data[idx] if idx < len(row_data) else ""
    elif first_stat_col > 2 and len(row_data) > 2:
        result_raw = row_data[2]
        if result_raw and re.match(r"^\d+(\.\d+)?$", result_raw):
            result_raw = ""
    else:
        result_raw = ""
    result = _parse_result(result_raw)

    def _shoot(field: str, a_field: str, pct_field: str):
        raw = _raw(field)
        made, att = _parse_ma_cell(raw) if raw else (None, None)
        if made is None:
            made = _safe_float(raw)
            att  = _get(a_field)
        pct = _get(pct_field)
        if pct is None and made is not None and att:
            pct = round(made / att * 100, 1)
        return made, att, pct

    t2m, t2a, t2_pct = _shoot("t2m", "t2a", "t2_pct")
    t3m, t3a, t3_pct = _shoot("t3m", "t3a", "t3_pct")
    ftm, fta, ft_pct = _shoot("ftm", "fta", "ft_pct")

    return {
        "player_id":   str(player_id),
        "player_name": player_name,
        "source":      "eurobasket",
        "competition": competition,
        "season":      "2025-26",
        "game_date":   game_date,
        "opponent":    opponent,
        "result":      result,
        "date":        today,
        "min":         _parse_minutes(_raw("min")),
        "pts":         _get("pts"),
        "t2m":  t2m,  "t2a": t2a,  "t2_pct": t2_pct,
        "t3m":  t3m,  "t3a": t3a,  "t3_pct": t3_pct,
        "ftm":  ftm,  "fta": fta,  "ft_pct": ft_pct,
        "reb_off":    _get("reb_off"),
        "reb_def":    _get("reb_def"),
        "reb":        _get("reb"),
        "ast":        _get("ast"),
        "stl":        _get("stl"),
        "tov":        _get("tov"),
        "blk":        _get("blk"),
        "fouls":      _get("fouls"),
        "plus_minus": _get("plus_minus"),
        "val":        _get("val"),
    }


def _fetch_and_parse(
    player_id: str | int,
    player_name: str,
    competition: str,
) -> tuple[BeautifulSoup | None, str]:
    """Fetch the player page and return (soup, url). Returns (None, url) on error."""
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{slug}/{player_id}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser"), url
    except requests.HTTPError as exc:
        logger.warning("Eurobasket HTTP error player_id=%s: %s", player_id, exc)
    except requests.RequestException as exc:
        logger.warning("Eurobasket request failed player_id=%s: %s", player_id, exc)
    return None, url


def _iter_game_rows(
    soup: BeautifulSoup,
    competition: str,
):
    """Yield (row_data, col_map) for every valid game row matching *competition*."""
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        title_cells = rows[0].find_all(["th", "td"], recursive=False)
        if len(title_cells) == 1 and title_cells[0].get_text(strip=True).lower() == "details":
            hcells     = rows[1].find_all(["th", "td"], recursive=False)
            data_start = 2
        else:
            hcells     = rows[0].find_all(["th", "td"], recursive=False)
            data_start = 1

        cmap = _build_col_map(hcells)
        if len(cmap) < 5:
            continue

        if competition and data_start == 2:
            h4 = table.find_previous("h4")
            heading_text = h4.get_text(strip=True) if h4 else ""
            if heading_text and not _heading_matches(heading_text, competition):
                continue

        for row in rows[data_start:]:
            cells = row.find_all(["td", "th"], recursive=False)
            if _is_game_row(cells, cmap):
                yield [_cell_text(c) for c in cells], cmap


def fetch_season_stats(
    player_id: str | int,
    player_name: str = "player",
    competition: str = "Greek League",
) -> list[dict]:
    """Return all game box scores for the current season from basketball.eurobasket.com."""
    soup, _ = _fetch_and_parse(player_id, player_name, competition)
    if soup is None:
        return []

    today   = str(date.today())
    records = []
    for row_data, cmap in _iter_game_rows(soup, competition):
        rec = _extract_record(row_data, cmap, player_id, player_name, competition, today)
        if rec:
            records.append(rec)

    logger.info(
        "Eurobasket season fetch: %d games for player_id=%s (%s)",
        len(records), player_id, competition,
    )
    return records


def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
    competition: str = "Greek League",
) -> dict:
    """Fetch the most recent game box score for a player on basketball.eurobasket.com."""
    soup, _ = _fetch_and_parse(player_id, player_name, competition)
    if soup is None:
        return {}

    today    = str(date.today())
    last_rec = None
    for row_data, cmap in _iter_game_rows(soup, competition):
        rec = _extract_record(row_data, cmap, player_id, player_name, competition, today)
        if rec:
            last_rec = rec

    if last_rec is None:
        logger.warning("Eurobasket: no game rows found for player_id=%s", player_id)
        return {}

    return last_rec
