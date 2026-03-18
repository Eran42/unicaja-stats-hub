"""
BCL (Basketball Champions League) data fetcher.

Player profile URL (game-by-game log):
  https://www.championsleague.basketball/en/teams/{team-slug}/{player_id}-{name-slug}

When a slug is provided as player_id (contains "/"), the per-game log page is used
and the most recent game's box score is returned.

For legacy numeric IDs, falls back to the stats leaders page (season averages).
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_TEAM_URL  = "https://www.championsleague.basketball/en/teams"
_STATS_URL = "https://www.championsleague.basketball/en/stats/players/all/"
_TIMEOUT   = 20

# BCL team abbreviation → full team name (sourced from /en/teams listing)
_TEAM_NAMES: dict[str, str] = {
    "AEK":  "AEK BC",
    "ALBA": "ALBA Berlin",
    "CJB":  "Asisa Joventut",
    "SAB":  "BC Sabah",
    "HERZ": "Bnei Herzliya",
    "BUR":  "Bursaspor",
    "CHOL": "Cholet Basket",
    "DGC":  "Gran Canaria",
    "ELAN": "Elan Chalon",
    "NYMB": "ERA Nymburk",
    "OOST": "Filou Oostende",
    "WUE":  "Wurzburg Baskets",
    "GSM":  "Galatasaray",
    "HOLO": "Hapoel Holon",
    "IGO":  "Igokea",
    "KARD": "Karditsa",
    "SPAR": "Spartak",
    "LLTF": "La Laguna Tenerife",
    "MSB":  "Le Mans",
    "WARS": "Legia Warszawa",
    "MSK":  "Mersin",
    "HDB":  "Academics Heidelberg",
    "OLAJ": "Szolnoki Olajbanyasz",
    "TS":   "Pallacanestro Trieste",
    "LEV":  "Patrioti Levice",
    "PROM": "Promitheas Patras",
    "VILN": "Rytas Vilnius",
    "SLB":  "SL Benfica",
    "TOF":  "Tofas Bursa",
    "TPS":  "Trapani Shark",
    "UNI":  "Unicaja",
    "VEF":  "VEF Riga",
}

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


def _parse_shot_cell(cell: Tag) -> tuple[float | None, float | None, float | None]:
    """
    Parse a BCL shooting cell that contains two sub-divs: 'N/M' and 'X.X%'.
    Uses separator='|' to split them cleanly.
    Returns (made, attempted, pct) or (None, None, None) on failure.
    """
    parts = [p.strip() for p in cell.get_text(separator="|", strip=True).split("|") if p.strip()]
    if not parts:
        return None, None, None

    # First non-empty part should be "N/M"
    made_att = parts[0]
    m = re.match(r"^(\d+)/(\d+)$", made_att)
    if not m:
        return None, None, None

    made = float(m.group(1))
    att  = float(m.group(2))

    # Second part (optional) is "X.X%"
    if len(parts) >= 2:
        pct = _safe_float(parts[1])
    else:
        pct = round(made / att * 100, 1) if att > 0 else 0.0

    return made, att, pct


def _parse_game_cell(cell: Tag) -> tuple[str, str]:
    """
    Parse BCL game-info cell.
    Cell text with separator='|' looks like: 'vs|TS|,|17/03/2026|Round of 16'
    Returns (game_date as 'YYYY-MM-DD', opponent full name).
    """
    parts = [p.strip() for p in cell.get_text(separator="|", strip=True).split("|") if p.strip() and p.strip() != ","]
    # parts: ['vs', 'OPP', 'DD/MM/YYYY', 'phase']
    opp_abbrev = ""
    game_date = ""

    for part in parts:
        if re.match(r"^\d{2}/\d{2}/\d{4}$", part):
            day, month, year = part.split("/")
            game_date = f"{year}-{month}-{day}"
        elif part not in ("vs",) and not re.search(r"round|phase|group|final|quarter|semi", part, re.IGNORECASE):
            if not opp_abbrev:
                opp_abbrev = part

    opponent = _TEAM_NAMES.get(opp_abbrev, opp_abbrev)
    return game_date, opponent


# BCL stats column labels → canonical field names (for averages page fallback)
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
    Fetch BCL stats for a player.

    Args:
        player_id:
            - Slug format "team-slug/numeric_id-name-slug" → fetches game log page,
              returns the most recent game's box score with game_date populated.
            - Legacy numeric ID → falls back to season-averages stats page.
        player_name:  Canonical player name (used as fallback for display).

    Returns:
        Canonical stats dict, or empty dict on failure.
    """
    id_str = str(player_id)
    if "/" in id_str:
        return _fetch_from_team_page(id_str, player_name)
    # Legacy fallback: season averages
    return _fetch_from_stats_page(id_str, player_name)


def _fetch_from_team_page(slug: str, player_name: str) -> dict:
    """Fetch game-by-game log from team player profile page and return most recent game."""
    url = f"{_TEAM_URL}/{slug}"
    logger.debug("BCL team page fetch: %s", url)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("BCL team page request failed %s: %s", url, exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("BCL: no table found on %s", url)
        return {}

    rows = table.find_all("tr")
    if len(rows) < 3:
        return {}

    # Row 0: group headers; Row 1: column headers; Rows 2+: game rows + CUMULATED + AVERAGE
    header_row = rows[1].find_all(["th", "td"])

    # Build column index map from header text
    # Columns: Game(s), Min, PTS, FG, 2PT FG, 3PT FG, FT, OREB, DREB, REB, AST, PF, TO, STL, BLK, +/-, EFF
    col_idx: dict[str, int] = {}
    for i, cell in enumerate(header_row):
        label = cell.get_text(strip=True).upper()
        col_idx[label] = i

    # Collect game rows (skip CUMULATED / AVERAGE at the end)
    game_rows = []
    for row in rows[2:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        first = cells[0].get_text(strip=True).upper()
        if first in ("CUMULATED", "AVERAGE"):
            break
        game_rows.append(cells)

    if not game_rows:
        logger.warning("BCL: no game rows found for slug=%s", slug)
        return {}

    last_cells = game_rows[-1]

    def _gcell(label: str) -> str:
        idx = col_idx.get(label)
        if idx is None or idx >= len(last_cells):
            return ""
        return last_cells[idx].get_text(strip=True)

    game_date, opponent = _parse_game_cell(last_cells[0])

    # Shooting stats — use separator-aware parser
    def _shot(label: str) -> tuple[float | None, float | None, float | None]:
        idx = col_idx.get(label)
        if idx is None or idx >= len(last_cells):
            return None, None, None
        return _parse_shot_cell(last_cells[idx])

    t2m, t2a, t2_pct = _shot("2PT FG")
    t3m, t3a, t3_pct = _shot("3PT FG")
    ftm, fta, ft_pct = _shot("FT")

    # Recompute pct = 0.0 when attempts = 0
    if t2a == 0.0:  t2_pct = 0.0
    if t3a == 0.0:  t3_pct = 0.0
    if fta == 0.0:  ft_pct = 0.0

    return {
        "player_id":    slug,
        "player_name":  player_name or slug,
        "source":       "bcl",
        "competition":  "BCL",
        "season":       "2025-26",
        "date":         str(date.today()),
        "game_date":    game_date,
        "opponent":     opponent,
        "result":       "",
        "min":          _parse_minutes(_gcell("MIN")),
        "pts":          _safe_float(_gcell("PTS")),
        "t2m":  t2m,   "t2a":  t2a,  "t2_pct":  t2_pct,
        "t3m":  t3m,   "t3a":  t3a,  "t3_pct":  t3_pct,
        "ftm":  ftm,   "fta":  fta,  "ft_pct":  ft_pct,
        "reb_off":      _safe_float(_gcell("OREB")),
        "reb_def":      _safe_float(_gcell("DREB")),
        "reb":          _safe_float(_gcell("REB")),
        "ast":          _safe_float(_gcell("AST")),
        "stl":          _safe_float(_gcell("STL")),
        "tov":          _safe_float(_gcell("TO")),
        "blk":          _safe_float(_gcell("BLK")),
        "blk_against":  None,
        "fouls":        _safe_float(_gcell("PF")),
        "fouls_received": None,
        "plus_minus":   _safe_float(_gcell("+/-")),
        "val":          _safe_float(_gcell("EFF")),
    }


def _fetch_from_stats_page(player_id: str | int, player_name: str) -> dict:
    """Fallback: fetch season averages from BCL stats leaders page."""
    id_str = str(player_id)
    name_lower = player_name.lower()
    logger.debug("BCL stats page fetch for id=%s", id_str)
    try:
        resp = requests.get(_STATS_URL, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("BCL stats page request failed: %s", exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
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
            if id_str not in row_text and (not name_lower or name_lower not in row_text.lower()):
                continue

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

            return {
                "player_id":    id_str,
                "player_name":  player_name or id_str,
                "source":       "bcl",
                "competition":  "BCL",
                "season":       "2025-26",
                "date":         str(date.today()),
                "game_date":    "",
                "opponent":     "",
                "result":       "",
                "min":          _parse_minutes(_get("min")) if _get("min") else None,
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
                "blk_against":  None,
                "fouls":        _get("fouls"),
                "fouls_received": None,
                "plus_minus":   _get("plus_minus"),
                "val":          _get("val"),
            }

    logger.warning("BCL: could not find stats row for player_id=%s", player_id)
    return {}
