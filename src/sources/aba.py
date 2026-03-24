"""
ABA Liga latest game box score.

Endpoint:
  GET https://www.aba-liga.com/player/{player_id}/{season}/{league}/{slug}/

season codes:  25 = 2025-26
league codes:   1 = ABA Liga 1

The page contains a stats table with game rows (one per game played),
followed by a Total row and an Average row.
We take the last individual game row (most recent game).

Column layout (0-indexed after optional leading offset):
  0  Min   1  Pts   2  %FG   3  2ptM  4  2ptA  5  %2pt
  6  3ptM  7  3ptA  8  %3pt  9  FTM  10  FTA  11  %FT
  12 D-reb 13 O-reb 14 T-reb 15 Ast  16 Stl  17 Tov
  18 Blk+  19 Blk-  20 Fouls 21 FoulR 22 +/-  23 Val
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE_URL       = "https://www.aba-liga.com/player"
_TIMEOUT        = 15
_CURRENT_SEASON = "25"
_LEAGUE         = "1"

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


def _cell_text(cell) -> str:
    return cell.get_text(strip=True)


def _parse_aba_date(raw: str) -> str:
    """
    Normalise ABA date strings to YYYY-MM-DD.

    Handles:
      'DD.MM.YYYY'  e.g. '15.02.2026'
      'DD.MM.'      e.g. '15.02.'  — year inferred from season (2025-26)
    """
    raw = raw.strip()
    # Full date with year
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return f"{y}-{mo:02d}-{d:02d}"
        except (ValueError, OverflowError):
            pass
    # Short date without year (season 2025-26: Sep-Dec → 2025, Jan-Aug → 2026)
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.", raw)
    if m:
        d, mo = int(m.group(1)), int(m.group(2))
        year = 2025 if mo >= 9 else 2026
        return f"{year}-{mo:02d}-{d:02d}"
    return raw


def _parse_result(text: str) -> str:
    """
    Parse a result/score cell into canonical 'V 82-75' / 'D 75-82' format.

    Recognises:
      Score patterns: '82-75', '82:75'
      W/L letters:    'W', 'L', 'V', 'D'
      Language words: 'Pobjeda'/'Pobeda' (victory), 'Poraz' (defeat)
    """
    text = text.strip()
    score_m = re.search(r"(\d{2,3})[:\-](\d{2,3})", text)
    score_str = f"{score_m.group(1)}-{score_m.group(2)}" if score_m else ""

    if re.search(r"\bpobje[d]?a\b|\bpobeda\b|\bvictory\b|\bwin\b|\bvictoria\b", text, re.I):
        wl = "V"
    elif re.search(r"\bporaz\b|\bloss\b|\bdefeated?\b|\bderrota\b", text, re.I):
        wl = "D"
    else:
        wl_m = re.search(r"\b([WwVv])\b", text)
        if wl_m:
            wl = "V"
        else:
            wl_m = re.search(r"\b([LlDd])\b", text)
            wl = "D" if wl_m else ""

    if score_str and wl:
        return f"{wl} {score_str}"
    return score_str or wl


def _fetch_match_date(href: str) -> str:
    """Fetch an ABA match page and return the game date as YYYY-MM-DD."""
    if not href.startswith("http"):
        href = _BASE_URL.replace("/player", "") + "/" + href.lstrip("/")
    try:
        resp = requests.get(href, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException:
        return ""
    # Date appears as e.g. "Sunday, 05.10.2025 19:30 CET"
    m = re.search(r"\b(\d{1,2}\.\d{1,2}\.\d{4})\b", resp.text)
    return _parse_aba_date(m.group(1)) if m else ""


def _is_game_row(cells) -> bool:
    """True if the row looks like a game row.

    Accepts:
    - Date-first rows: 'DD.MM.' or 'DD.MM.YYYY' in first cell (legacy format).
    - Game-number-first rows: pure integer in first cell (current ABA format).
    """
    if len(cells) < 5:
        return False
    first = _cell_text(cells[0])
    if re.match(r"\d{1,2}\.\d{1,2}\.", first):
        return True
    if re.match(r"^\d{1,3}$", first):
        # Also reject summary rows whose second cell reads "Total" / "Average" etc.
        if len(cells) > 1:
            second = _cell_text(cells[1]).lower()
            if any(kw in second for kw in ("total", "average", "avg", "media")):
                return False
        return True
    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(
    player_id: str | int,
    player_name: str = "player",
    season: str = _CURRENT_SEASON,
    league: str = _LEAGUE,
) -> dict:
    """Fetch the most recent game box score for an ABA Liga player."""
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{player_id}/{season}/{league}/{slug}/"
    logger.debug("ABA fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("ABA HTTP error player_id=%s: %s", player_id, exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("ABA request failed player_id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the last game row across all tables.
    # Two row formats are supported:
    #   Legacy: col0=date (DD.MM.), col1=opponent, col2=result (optional), then stats
    #   Current: col0=game_number, col1=matchup link, then stats
    last_game_cells  = None
    last_game_date   = ""
    last_match_href  = ""
    last_is_gamenumber = False

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not _is_game_row(cells):
                continue
            last_game_cells = cells
            first_text = _cell_text(cells[0])
            if re.match(r"^\d{1,3}$", first_text):
                # Current game-number format — date must be fetched from match page
                last_is_gamenumber = True
                last_game_date     = ""
                a_tag = cells[1].find("a") if len(cells) > 1 else None
                last_match_href    = (a_tag["href"] if a_tag and a_tag.get("href") else "")
            else:
                last_is_gamenumber = False
                last_game_date     = first_text
                last_match_href    = ""

    if last_game_cells is None:
        logger.warning("ABA: no game rows found for player_id=%s", player_id)
        return {}

    # For game-number rows, fetch the match page to get the date.
    if last_is_gamenumber and last_match_href:
        last_game_date = _fetch_match_date(last_match_href)

    c = last_game_cells

    def _get(idx: int) -> str | None:
        return _cell_text(c[idx]) if idx < len(c) else None

    # Determine column offset (leading context columns before stats begin).
    #   Current format:  col0=game#, col1=matchup → offset=2
    #   Legacy long:     col0=date,  col1=opponent, col2=result → offset=3
    #   Legacy short:    col0=date,  col1=opponent+result combined → offset=2
    if last_is_gamenumber:
        offset = 2
    elif len(c) >= 26:
        offset = 3
    else:
        offset = 2

    # Extract opponent and result from the leading context cells.
    if last_is_gamenumber:
        # Matchup cell contains both teams; use the full text as opponent context.
        opponent = _get(1) or ""
        result   = ""
    elif offset == 3:
        opponent   = _get(1) or ""
        result_raw = _get(2) or ""
        result     = _parse_result(result_raw)
    else:
        # col1 may contain opponent name and/or result info
        combined = _get(1) or ""
        # If combined contains a score pattern, parse as result; take text before as opponent
        score_m = re.search(r"(\d{2,3})[:\-](\d{2,3})", combined)
        if score_m:
            opponent = combined[:score_m.start()].strip()
            result   = _parse_result(combined)
        else:
            opponent = combined
            result   = ""

    def _s(rel: int) -> str | None:
        return _get(offset + rel)

    return {
        "player_id":   str(player_id),
        "player_name": player_name,
        "source":      "aba",
        "competition": "ABA League",
        "season":      "2025-26",
        "game_date":   _parse_aba_date(last_game_date),
        "opponent":    opponent,
        "result":      result,
        "date":        str(date.today()),
        "min":         _parse_minutes(_s(0)),
        "pts":         _safe_float(_s(1)),
        # 2-point shooting
        "t2m":         _safe_float(_s(3)),
        "t2a":         _safe_float(_s(4)),
        "t2_pct":      _safe_float(_s(5)),
        # 3-point shooting
        "t3m":         _safe_float(_s(6)),
        "t3a":         _safe_float(_s(7)),
        "t3_pct":      _safe_float(_s(8)),
        # Free throws
        "ftm":         _safe_float(_s(9)),
        "fta":         _safe_float(_s(10)),
        "ft_pct":      _safe_float(_s(11)),
        # Rebounds
        "reb_def":     _safe_float(_s(12)),
        "reb_off":     _safe_float(_s(13)),
        "reb":         _safe_float(_s(14)),
        # Other
        "ast":         _safe_float(_s(15)),
        "stl":         _safe_float(_s(16)),
        "tov":         _safe_float(_s(17)),
        "blk":         _safe_float(_s(18)),
        "fouls":       _safe_float(_s(20)),
        "plus_minus":  _safe_float(_s(22)),
        "val":         _safe_float(_s(23)),
    }


def _cells_to_record(
    cells,
    is_gamenumber: bool,
    game_date_str: str,
    player_id: str | int,
    player_name: str,
    today: str,
) -> dict | None:
    """Convert an ABA game row (cells list) into a canonical stats record.

    Returns None if the row lacks a parseable game date.
    """
    game_date = _parse_aba_date(game_date_str) if game_date_str else ""
    if not game_date or not re.match(r"\d{4}-\d{2}-\d{2}", game_date):
        return None

    def _get(idx: int) -> str | None:
        return _cell_text(cells[idx]) if idx < len(cells) else None

    if is_gamenumber:
        offset   = 2
        opponent = _get(1) or ""
        result   = ""
    elif len(cells) >= 26:
        offset     = 3
        opponent   = _get(1) or ""
        result_raw = _get(2) or ""
        result     = _parse_result(result_raw)
    else:
        offset   = 2
        combined = _get(1) or ""
        score_m  = re.search(r"(\d{2,3})[:\-](\d{2,3})", combined)
        if score_m:
            opponent = combined[:score_m.start()].strip()
            result   = _parse_result(combined)
        else:
            opponent = combined
            result   = ""

    def _s(rel: int) -> str | None:
        return _get(offset + rel)

    return {
        "player_id":   str(player_id),
        "player_name": player_name,
        "source":      "aba",
        "competition": "ABA League",
        "season":      "2025-26",
        "game_date":   game_date,
        "opponent":    opponent,
        "result":      result,
        "date":        today,
        "min":         _parse_minutes(_s(0)),
        "pts":         _safe_float(_s(1)),
        "t2m":         _safe_float(_s(3)),
        "t2a":         _safe_float(_s(4)),
        "t2_pct":      _safe_float(_s(5)),
        "t3m":         _safe_float(_s(6)),
        "t3a":         _safe_float(_s(7)),
        "t3_pct":      _safe_float(_s(8)),
        "ftm":         _safe_float(_s(9)),
        "fta":         _safe_float(_s(10)),
        "ft_pct":      _safe_float(_s(11)),
        "reb_def":     _safe_float(_s(12)),
        "reb_off":     _safe_float(_s(13)),
        "reb":         _safe_float(_s(14)),
        "ast":         _safe_float(_s(15)),
        "stl":         _safe_float(_s(16)),
        "tov":         _safe_float(_s(17)),
        "blk":         _safe_float(_s(18)),
        "fouls":       _safe_float(_s(20)),
        "plus_minus":  _safe_float(_s(22)),
        "val":         _safe_float(_s(23)),
    }


def fetch_season_stats(
    player_id: str | int,
    player_name: str = "player",
    season: str = _CURRENT_SEASON,
    league: str = _LEAGUE,
) -> list[dict]:
    """Return all game box scores for the current season from aba-liga.com.

    For the current game-number table format, each game requires a separate
    request to the match page to retrieve the date.  Requests are spaced
    0.35 s apart to avoid hammering the server.
    """
    slug = _make_slug(player_name)
    url  = f"{_BASE_URL}/{player_id}/{season}/{league}/{slug}/"
    logger.debug("ABA season fetch: %s", url)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("ABA HTTP error player_id=%s: %s", player_id, exc)
        return []
    except requests.RequestException as exc:
        logger.warning("ABA request failed player_id=%s: %s", player_id, exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Pass 1: collect all game rows and their match-page hrefs.
    rows_info: list[tuple] = []  # (cells, is_gamenumber, href, date_str)
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not _is_game_row(cells):
                continue
            first_text = _cell_text(cells[0])
            if re.match(r"^\d{1,3}$", first_text):
                a_tag = cells[1].find("a") if len(cells) > 1 else None
                href  = a_tag["href"] if a_tag and a_tag.get("href") else ""
                rows_info.append((cells, True, href, ""))
            else:
                rows_info.append((cells, False, "", first_text))

    if not rows_info:
        logger.warning("ABA: no game rows found for player_id=%s", player_id)
        return []

    # Pass 2: fetch dates for game-number rows (one request per game).
    resolved: list[tuple] = []
    for cells, is_gn, href, date_str in rows_info:
        if is_gn and href:
            fetched = _fetch_match_date(href)
            resolved.append((cells, is_gn, href, fetched))
            time.sleep(0.35)
        else:
            resolved.append((cells, is_gn, href, date_str))

    # Pass 3: build records, skip rows without valid dates.
    today   = str(date.today())
    records = []
    for cells, is_gn, _, date_str in resolved:
        rec = _cells_to_record(cells, is_gn, date_str, player_id, player_name, today)
        if rec:
            records.append(rec)

    logger.info(
        "ABA season fetch: %d games for player_id=%s", len(records), player_id
    )
    return records
