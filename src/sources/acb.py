"""
ACB (Liga Endesa) scraper — latest game box score.

URL: https://www.acb.com/jugador/todos-sus-partidos/id/{numeric_id}

The page lists every game played by the player this season.
We take the last row in the table (most recent game).

Column layout (game log):
  J (game#), PARTIDOS (opponent), Res. (result), Min.,
  PT (pts), T2 (combined M/A/%), T3 (combined M/A/%), T1 (FT combined M/A/%),
  T(D+O) (reb def/off/total), A (ast), BR (stl), BP (tov),
  Tap > F+C (total blocks as 'N+M' string), +/-, V (val)

Blocks (F=favor, C=contra) and fouls (F=committed, C=received) are extracted from
the linked game stats page (/partido/ver/id/XXXXX) which has separate TAP.F, TAP.C,
FP.F, FP.C sub-columns in the box score table.
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
      'T(D+O)' like '4(2+2)' → total=4, off=2, def=2
      'D+O'    like '5+2'    → def=5, off=2, total=7
      'D/O/T'  like '5/2/7'  → def=5, off=2, total=7
      plain number            → total only
    Returns (reb_def, reb_off, reb_total).

    NOTE: ACB game log uses T(D+O) format where the labels are counterintuitive.
    The live.acb.com legend explicitly maps DR→Offensive and OR→Defensive, meaning
    the first sub-value (D) = offensive rebounds and second (O) = defensive rebounds.
    We swap them here so that reb_def and reb_off are stored correctly.
    """
    text = text.strip()

    # Format: '4(2+2)' or '4 (2+2)' — total before parens, D+O inside.
    # Despite the label 'D', the first sub-value is offensive; second is defensive.
    paren_match = re.match(r"(\d+)\s*\((\d+)\+(\d+)\)", text)
    if paren_match:
        t   = _safe_float(paren_match.group(1))
        off = _safe_float(paren_match.group(2))   # mislabeled 'D' = offensive
        def_ = _safe_float(paren_match.group(3))  # mislabeled 'O' = defensive
        return def_, off, t

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
    "res.":     "result",   "res": "result",   "r": "result",   "v/d": "result",
    "min.":     "min",      "min": "min",
    "pt":       "pts",      "pts": "pts",
    "t2":       "t2_combined",
    "t3":       "t3_combined",
    "t1":       "ft_combined",
    "t(d+o)":   "reb_combined",
    "a":        "ast",
    "br":       "stl",
    "bp":       "tov",
    # Blocks (Tapones): "f+c" sub-column under TAP. parent header = total blocks (favor+contra)
    # The standalone "C" at index 12 is NOT fouls — it is cumulative/other (skip it).
    "f+c":      "blk",
    # Fouls: "f" sub-column under FALTAS parent header = per-game fouls committed
    # (index 15; the second "C" at index 16 is fouls received — ignored)
    "f":        "fouls",
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
        "blk":        _safe_float(_at("blk")),   # "Fav" sub-column of Tapones
        # Fouls committed ("C" sub-column): capped at 5 (ACB foul-out limit).
        # Values > 5 indicate a season-cumulative cell, not per-game.
        "fouls":      _f if (_f := _safe_float(_at("fouls"))) is None or _f <= 5 else None,
        "plus_minus": _safe_float(_at("plus_minus")),
        "val":        _safe_float(_at("val")),
    }


# ---------------------------------------------------------------------------
# Game-page helpers
# ---------------------------------------------------------------------------

def _expand_header_row(row: Tag) -> list[str]:
    """Expand colspan attributes → flat list of column labels (uppercase)."""
    flat: list[str] = []
    for cell in row.find_all(["th", "td"]):
        span = int(cell.get("colspan", 1))
        flat.extend([cell.get_text(strip=True).upper()] * span)
    return flat


def _extract_player_boxscore(
    html: str, player_name: str
) -> dict[str, float | None]:
    """
    Parse an ACB game stats page (/partido/ver/id/XXXXX) to find the player's
    row and return blocks favor/against and fouls committed/received.

    The box score tables have a two-row header structure:
      Parent row (after colspan expansion):
        ... TAP TAP ... FP FP ...
      Sub-header row:
        ... F   C  ... F  C  ...

    We expand the parent row to find the flat column indices of TAP and FP,
    then use those indices to extract the data values for the matched player.

    Returns dict with keys: blk, blk_against, fouls, fouls_received.
    All values are float | None.
    """
    empty: dict[str, float | None] = {
        "blk": None,
        "blk_against": None,
        "fouls": None,
        "fouls_received": None,
    }
    soup = BeautifulSoup(html, "lxml")

    def _ascii_tokens(text: str) -> frozenset[str]:
        """Normalise to ASCII, lowercase, split — for accent-insensitive matching."""
        import unicodedata as _ud
        ascii_text = (
            _ud.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode()
        )
        return frozenset(
            t for t in re.sub(r"[^\w\s]", " ", ascii_text.lower()).split()
            if len(t) >= 3
        )

    # Build token set from player name for fuzzy row matching (ASCII-normalised)
    pname_tokens = _ascii_tokens(player_name)
    if not pname_tokens:
        return empty

    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 3:
            continue

        # Find the parent header row that contains TAP and/or FP labels
        tap_start: int | None = None
        fp_start: int | None = None

        for hrow in rows[:4]:
            flat = _expand_header_row(hrow)
            for i, label in enumerate(flat):
                if label in ("TAP", "TAP.") and tap_start is None:
                    tap_start = i
                if label == "FP" and fp_start is None:
                    fp_start = i
            if tap_start is not None or fp_start is not None:
                break

        if tap_start is None and fp_start is None:
            continue

        # Find the player's data row (td cells) and extract by flat-index
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 10:
                continue

            # Player name is typically in the second cell (index 1 = "Nombre")
            name_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            if not name_raw:
                continue
            # Use ASCII-normalised tokens to avoid encoding mismatches
            name_tokens = _ascii_tokens(name_raw)
            if not pname_tokens.intersection(name_tokens):
                continue

            # Matched — extract values
            n = len(cells)

            def _gcell(idx: int | None) -> float | None:
                if idx is None or idx >= n:
                    return None
                return _safe_float(cells[idx].get_text(strip=True))

            candidate = {
                "blk":           _gcell(tap_start),
                "blk_against":   _gcell(tap_start + 1) if tap_start is not None else None,
                "fouls":         _gcell(fp_start),
                "fouls_received": _gcell(fp_start + 1) if fp_start is not None else None,
            }
            # Skip rows where ALL target values are empty (wrong team, same name)
            if any(v is not None for v in candidate.values()):
                return candidate

    return empty


def _extract_result_from_game_page(html: str, opponent_name: str) -> str:
    """
    Parse an ACB game stats page (/partido/ver/id/XXXXX) to extract the
    full result string, e.g. 'V 95-80' or 'D 76-82'.

    Strategy:
      1. Look for local/visitante score elements (ACB-specific class/id patterns).
      2. Regex fallback: find two prominent 2-3 digit numbers separated by '-'.
      3. Determine W/L by matching opponent_name against the team names on the page;
         fall back to score comparison if names can't be matched.
    """
    soup = BeautifulSoup(html, "lxml")
    opp_lower = opponent_name.strip().lower()

    local_score:   int | None = None
    visitor_score: int | None = None
    local_name  = ""
    visitor_name = ""

    # --- Strategy 1: id/class patterns ACB uses for local vs visitante ----------
    kw_pairs = [
        ("local",     "visitante"),
        ("local",     "visitor"),
        ("home",      "away"),
    ]

    def _first_score(els: list) -> int | None:
        for el in els:
            t = el.get_text(strip=True)
            if re.match(r"^\d{2,3}$", t):
                return int(t)
        return None

    def _first_name(els: list) -> str:
        for el in els:
            t = el.get_text(separator=" ", strip=True)
            # Reject pure-number elements and very short strings
            if re.search(r"[A-Za-zÀ-ÿ]{3,}", t) and not re.match(r"^\d+$", t):
                return t
        return ""

    for kw_l, kw_v in kw_pairs:
        loc_els = (
            soup.find_all(id=re.compile(kw_l, re.I))
            + soup.find_all(class_=re.compile(kw_l, re.I))
        )
        vis_els = (
            soup.find_all(id=re.compile(kw_v, re.I))
            + soup.find_all(class_=re.compile(kw_v, re.I))
        )
        ls = _first_score(loc_els)
        vs = _first_score(vis_els)
        if ls is not None and vs is not None:
            local_score   = ls
            visitor_score = vs
            local_name    = _first_name(loc_els)
            visitor_name  = _first_name(vis_els)
            break

    # --- Strategy 2: regex scan on raw text -----------------------------------
    if local_score is None or visitor_score is None:
        # Look for a 'NNN - NNN' pattern; take the first match with 2-3 digit numbers
        for m in re.finditer(r"\b(\d{2,3})\s*[-\u2013]\s*(\d{2,3})\b", html):
            a, b = int(m.group(1)), int(m.group(2))
            # Sanity check: basketball scores don't go below 40 or above 200
            if 40 <= a <= 200 and 40 <= b <= 200:
                local_score   = a
                visitor_score = b
                break

    if local_score is None or visitor_score is None:
        logger.debug("ACB game page: could not extract score")
        return ""

    score_str = f"{local_score}-{visitor_score}"

    # --- Determine W/L --------------------------------------------------------
    # If we have both team names, try to match the opponent to identify player's side
    if opp_lower and local_name and visitor_name:
        opp_is_local = (
            opp_lower in local_name.lower() or local_name.lower() in opp_lower
        )
        opp_is_visit = (
            opp_lower in visitor_name.lower() or visitor_name.lower() in opp_lower
        )
        if opp_is_local and not opp_is_visit:
            # Player is the visitor: player wins if visitor_score > local_score
            won = visitor_score > local_score
            return f"{'V' if won else 'D'} {score_str}"
        if opp_is_visit and not opp_is_local:
            # Player is local: player wins if local_score > visitor_score
            won = local_score > visitor_score
            return f"{'V' if won else 'D'} {score_str}"

    # Opponent name match was ambiguous — return score only
    return score_str


def _find_player_team_abbr(game_rows: list, col_map: dict) -> str:
    """Find the player's team abbreviation from PARTIDOS matchup strings.

    The player's team appears in every game row (as home or away), so it is
    the most-frequent token when all matchup strings are split on '-'.
    """
    from collections import Counter
    opp_idx = col_map.get("opponent")
    if opp_idx is None:
        return ""
    tokens: list[str] = []
    for cells in game_rows:
        raw = cells[opp_idx].get_text(strip=True) if opp_idx < len(cells) else ""
        if "-" in raw:
            left, _, right = raw.partition("-")
            tokens.extend([left.strip(), right.strip()])
    if not tokens:
        return ""
    return Counter(tokens).most_common(1)[0][0]


def _strip_matchup(matchup: str, player_team_abbr: str) -> str:
    """Given 'TeamA-TeamB', return whichever side is NOT the player's team."""
    if not player_team_abbr or "-" not in matchup:
        return matchup
    left, _, right = matchup.partition("-")
    if left.strip() == player_team_abbr:
        return right.strip()
    if right.strip() == player_team_abbr:
        return left.strip()
    return matchup


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
    all_tables = soup.find_all("table")
    logger.debug("ACB id=%s: found %d table(s) in page", player_id, len(all_tables))
    if not all_tables:
        # No tables at all — likely a JS-rendered page or wrong URL
        snippet = resp.text[:500].replace("\n", " ")
        logger.warning(
            "ACB id=%s: page has no <table> elements (JS-rendered?). "
            "URL=%s  HTTP=%s  snippet=%r",
            player_id, url, resp.status_code, snippet,
        )
        return {}

    target_table: Tag | None = None
    col_map: dict[str, int] = {}
    for tbl in all_tables:
        tbl_rows = tbl.find_all("tr")
        # Search up to 5 header rows (some tables have multi-row headers)
        for row in tbl_rows[:5]:
            cells = row.find_all(["th", "td"])
            candidate = _build_col_map(cells)
            if len(candidate) > len(col_map):
                col_map = candidate
                target_table = tbl

    logger.debug(
        "ACB id=%s: best table col_map=%s",
        player_id, list(col_map.keys()),
    )

    if target_table is None or len(col_map) < 3:
        logger.warning(
            "ACB id=%s: no usable game log table found (best col_map has %d field(s): %s)",
            player_id, len(col_map), list(col_map.keys()),
        )
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

    # Determine player's team abbreviation from all matchup strings so we can
    # return just the opponent (not the full "TeamA-TeamB" matchup).
    player_team_abbr = _find_player_team_abbr(game_rows, col_map)

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

    # Extract game date, result, and per-player blocks/fouls from the linked game page.
    # The game log row gives us V/D from the "Res." column (often empty); the game
    # stats page (/partido/ver/id/XXXXX) always has the full score and separate
    # TAP.F/TAP.C and FP.F/FP.C sub-columns for blocks and fouls.
    game_date   = ""
    game_result = stats.get("result", "").strip()  # V or D from game log, may be empty
    opponent    = _strip_matchup(stats.get("opponent", ""), player_team_abbr)
    detail_stats: dict[str, float | None] = {}

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

            # Date
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

            # Result / score — extract from the game stats page
            extracted = _extract_result_from_game_page(page_text, opponent)
            if extracted:
                # If we already have V/D from the game log but no score, combine them
                if game_result in ("V", "D") and not re.search(r"\d", extracted):
                    game_result = f"{game_result} {extracted}"
                else:
                    game_result = extracted

            # Blocks and fouls — separate F/C columns in the box score table
            detail_stats = _extract_player_boxscore(page_text, player_name or player_id)
            logger.debug("ACB game detail stats for %s: %s", player_name, detail_stats)

        except requests.RequestException:
            pass
        break

    def _z(v: float | None) -> float:
        """ACB tracks this field — treat missing/empty cell as genuine zero."""
        return v if v is not None else 0.0

    # Prefer detail-page blocks/fouls (separate F/C columns); fall back to game-log values
    blk           = detail_stats.get("blk")
    blk_against   = detail_stats.get("blk_against")
    fouls         = detail_stats.get("fouls")
    fouls_received = detail_stats.get("fouls_received")

    # If detail page didn't yield blocks, fall back to game-log combined column
    if blk is None:
        blk = _z(stats["blk"])

    return {
        "player_id":      player_id,
        "player_name":    player_name or player_id,
        "source":         "acb",
        "competition":    "ACB",
        "season":         "2025-26",
        "game_date":      game_date,
        "opponent":       opponent,
        "result":         game_result,
        "date":           str(date.today()),
        "min":            stats["min"],          # keep None — player may not have played
        "pts":            _z(stats["pts"]),
        "t2m":            _z(stats["t2m"]),   "t2a":  _z(stats["t2a"]),  "t2_pct":  stats["t2_pct"],
        "t3m":            _z(stats["t3m"]),   "t3a":  _z(stats["t3a"]),  "t3_pct":  stats["t3_pct"],
        "ftm":            _z(stats["ftm"]),   "fta":  _z(stats["fta"]),  "ft_pct":  stats["ft_pct"],
        "reb_off":        _z(stats["reb_off"]),
        "reb_def":        _z(stats["reb_def"]),
        "reb":            _z(stats["reb"]),
        "ast":            _z(stats["ast"]),
        "stl":            _z(stats["stl"]),
        "tov":            _z(stats["tov"]),
        "blk":            blk,
        "blk_against":    blk_against,
        "fouls":          fouls if fouls is not None else (
                              stats["fouls"]  # keep None if >5 cap triggered
                          ),
        "fouls_received": fouls_received,
        "plus_minus":     _z(stats["plus_minus"]),
        "val":            _z(stats["val"]),
    }


# ---------------------------------------------------------------------------
# CLI debug helper  — run directly: python -m src.sources.acb <player_id>
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys as _sys
    import json as _json

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s  %(name)s - %(message)s",
        stream=_sys.stdout,
    )

    _id = _sys.argv[1] if len(_sys.argv) > 1 else "20210659"  # default: Shermadini
    print(f"\n--- ACB debug fetch: player_id={_id} ---\n")
    result = fetch_player_stats(_id)
    if result:
        print(_json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("(no data returned)")
