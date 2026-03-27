"""
FEB (Spanish Basketball Federation) scraper — v2.

Strategy
--------
1. Fetch the Primera FEB results page (latest jornada) to find the team's
   most recent game ID, date, and final score.
2. Fetch /partido/{game_id} box score HTML.
3. Find the player's row within the team's section.
4. Return per-game stats with game_date, opponent, and result.

player_id format
----------------
  "{TEAM_TOKEN}/{PLAYER_TOKEN}"  e.g.  "ESTUDIANTES/GRANGER"

TEAM_TOKEN  — upper-case substring of the team name on the results page
              (e.g. "ESTUDIANTES" matches "MOVISTAR ESTUDIANTES").
PLAYER_TOKEN — upper-case substring of the player's surname as shown on
               the box score in "LAST, FIRST" format (e.g. "GRANGER").
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_TIMEOUT = 20
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# Results page for Primera FEB 2025/26 — update g/t/nm each season if needed
_RESULTS_URL   = "https://baloncestoenvivo.feb.es/resultados.aspx?g=1&t=2025&nm=primerafeb"
_GAME_BASE_URL = "https://baloncestoenvivo.feb.es/partido"

# Scan up to this many past jornadas if team absent from the latest
_MAX_JORNADAS_BACK = 3


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _get(url: str) -> str | None:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.text
    except requests.RequestException as exc:
        logger.warning("FEB request failed %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Results-page parsing
# ---------------------------------------------------------------------------

def _parse_results(html: str, team_token: str) -> dict | None:
    """
    Find the most recent played game for *team_token* on the results page.

    Row structure discovered from the live site:
      <tr>
        <td>TEAM1 - TEAM2</td>          (team names separated by " - ")
        <td><a href="...p=XXXXX">NN-NN</a></td>  (score + game link)
        <td>DD/MM/YYYY</td>             (date)
        <td>HH:MM</td>                  (time)
      </tr>
    """
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=re.compile(r"[Pp]artido", re.I)):
        href = a.get("href", "")
        gid_m = re.search(r"[?&]p=(\d+)", href) or re.search(r"/partido/(\d+)", href)
        if not gid_m:
            continue

        # Skip games not yet played ("*-*")
        score_text = a.get_text(strip=True)
        if "*" in score_text or not re.search(r"\d", score_text):
            continue

        game_id = gid_m.group(1)
        tr = a.find_parent("tr")
        if not tr:
            continue

        cells = tr.find_all("td")
        if len(cells) < 2:
            continue

        teams_text = cells[0].get_text(" ", strip=True).upper()
        if team_token not in teams_text:
            continue

        # Parse team names (split on " - ")
        parts = [p.strip() for p in teams_text.split(" - ") if p.strip()]
        local_team   = parts[0] if len(parts) > 0 else ""
        visitor_team = parts[1] if len(parts) > 1 else ""

        # Parse score ("80-78")
        score_m = re.search(r"(\d+)\s*-\s*(\d+)", score_text)
        if not score_m:
            continue
        local_score   = int(score_m.group(1))
        visitor_score = int(score_m.group(2))

        # Parse date from td[2] if present
        game_date = ""
        if len(cells) > 2:
            dm = re.search(r"(\d{2})/(\d{2})/(\d{4})", cells[2].get_text())
            if dm:
                game_date = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"

        return {
            "game_id":      game_id,
            "game_date":    game_date,
            "local_team":   local_team,
            "visitor_team": visitor_team,
            "local_score":  local_score,
            "visitor_score": visitor_score,
        }

    return None


def _find_game_for_team(team_token: str) -> dict | None:
    """Try latest jornada, then step back up to _MAX_JORNADAS_BACK jornadas."""
    html = _get(_RESULTS_URL)
    if html:
        info = _parse_results(html, team_token)
        if info:
            return info

        # Find current jornada number and try previous ones
        jm = re.search(r"[Jj]ornada\s+(\d+)", html)
        if jm:
            current_j = int(jm.group(1))
            for offset in range(1, _MAX_JORNADAS_BACK + 1):
                back_html = _get(f"{_RESULTS_URL}&j={current_j - offset}")
                if back_html:
                    info = _parse_results(back_html, team_token)
                    if info:
                        return info

    return None


# ---------------------------------------------------------------------------
# Shot-cell parser  "4/850%"  →  (made=4, att=8, pct=50.0)
# ---------------------------------------------------------------------------

def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _parse_shot_cell(text: str) -> tuple[float | None, float | None, float | None]:
    """
    Parse a combined "made/attendedPCT%" cell into (made, attempted, pct).

    The site concatenates attempted and pct without a space:
      "4/850%"    → made=4  att=8   pct=50.0
      "3/742,9%"  → made=3  att=7   pct=42.9
      "4/4100%"   → made=4  att=4   pct=100.0
      "26/3672,2%"→ made=26 att=36  pct=72.2
    """
    text = text.strip()
    slash = text.find("/")
    if slash < 0:
        return None, None, None
    made = _safe_float(text[:slash])
    if made is None:
        return None, None, None

    rest = text[slash + 1:]
    if not rest.endswith("%"):
        return made, None, None
    raw = rest[:-1]  # strip trailing "%"

    if not raw:
        return made, None, None

    # ---- decimal pct (comma or dot) -----------------------------------------
    if "," in raw or ("." in raw):
        sep = max(raw.rfind(","), raw.rfind("."))
        frac_str  = raw[sep + 1:]   # e.g. "9" or "5"
        int_part  = raw[:sep]       # e.g. "742" or "3672"

        # pct integer part is at most 2 digits (0–99 for decimal values)
        if len(int_part) >= 2:
            pct_int_str = int_part[-2:]
            att_str     = int_part[:-2]
        else:
            pct_int_str = int_part
            att_str     = "0"

        try:
            pct = float(f"{int(pct_int_str)}.{frac_str}")
            att = float(att_str) if att_str else 0.0
            return made, att, pct
        except ValueError:
            return made, None, None

    # ---- integer pct ----------------------------------------------------------
    # Try extracting 3, 2, or 1 trailing digits as pct; the rest is attempted.
    for pct_len in (3, 2, 1):
        if len(raw) >= pct_len:
            pct_candidate = int(raw[-pct_len:].lstrip("0") or "0")
            if 0 <= pct_candidate <= 100:
                att_str = raw[:-pct_len] if len(raw) > pct_len else ""
                try:
                    att = float(att_str) if att_str else 0.0
                    return made, att, float(pct_candidate)
                except ValueError:
                    continue

    return made, None, None


def _parse_minutes(text: str) -> float | None:
    """'MM:SS' → decimal minutes."""
    text = str(text).strip()
    if ":" in text:
        parts = text.split(":")
        try:
            return round(float(parts[0]) + float(parts[1]) / 60, 2)
        except (ValueError, IndexError):
            return None
    return _safe_float(text)


# ---------------------------------------------------------------------------
# Box-score parsing
# ---------------------------------------------------------------------------
#
# Table structure (rows[0] = colspan group header, rows[1] = column names):
#
#   row0: | | Rebotes | | Tapones | | Faltas | |
#   row1: I | D | Jugador | MIN | PT | T2 | T3 | TC | TL | RO | RD | RT |
#          AS | BR | BP | TF | TC | MT | FC | FR | VA | +/-
#   row2+: player data rows
#   last:  totals row (empty D cell)
#
# Column indices (0-based):
#   0:I  1:D(jersey)  2:Jugador  3:MIN  4:PT  5:T2  6:T3  7:TC(skip)
#   8:TL  9:RO  10:RD  11:RT  12:AS  13:BR  14:BP  15:TF  16:TC_against
#   17:MT(skip)  18:FC  19:FR  20:VA  21:+/-

_COL = {
    "min":            3,
    "pts":            4,
    "t2":             5,
    "t3":             6,
    # 7 = TC (total shots, skip — redundant)
    "tl":             8,
    "reb_off":        9,
    "reb_def":        10,
    "reb":            11,
    "ast":            12,
    "stl":            13,
    "tov":            14,
    "blk":            15,
    "blk_against":    16,
    # 17 = MT (disqualifications, skip)
    "fouls":          18,
    "fouls_received": 19,
    "val":            20,
    "plus_minus":     21,
}


def _parse_game_box_score(html: str, player_token: str) -> dict | None:
    """
    Find *player_token* in any stats table and return raw stats dict.
    Also returns game_date if found in the page.
    """
    soup = BeautifulSoup(html, "html.parser")

    game_date = ""
    page_text = soup.get_text(" ")
    dm = re.search(r"Fecha\s+(\d{2})/(\d{2})/(\d{4})", page_text)
    if dm:
        game_date = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        # Need at least: row0 (group header), row1 (col header), 1+ data rows
        if len(rows) < 3:
            continue

        # Column header is in rows[1]; confirm it looks like a stats table
        col_header_text = rows[1].get_text(" ").upper()
        if "MIN" not in col_header_text or "PT" not in col_header_text:
            continue

        for row in rows[2:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 10:
                continue
            name_cell = cells[2].get_text(strip=True).upper() if len(cells) > 2 else ""
            if player_token not in name_cell:
                continue

            def _c(i: int) -> str:
                return cells[i].get_text(strip=True) if i < len(cells) else ""

            t2m, t2a, t2_pct = _parse_shot_cell(_c(_COL["t2"]))
            t3m, t3a, t3_pct = _parse_shot_cell(_c(_COL["t3"]))
            ftm, fta, ft_pct = _parse_shot_cell(_c(_COL["tl"]))

            return {
                "game_date":       game_date,
                "min":             _parse_minutes(_c(_COL["min"])),
                "pts":             _safe_float(_c(_COL["pts"])),
                "t2m":  t2m, "t2a":  t2a, "t2_pct":  t2_pct,
                "t3m":  t3m, "t3a":  t3a, "t3_pct":  t3_pct,
                "ftm":  ftm, "fta":  fta, "ft_pct":  ft_pct,
                "reb_off":         _safe_float(_c(_COL["reb_off"])),
                "reb_def":         _safe_float(_c(_COL["reb_def"])),
                "reb":             _safe_float(_c(_COL["reb"])),
                "ast":             _safe_float(_c(_COL["ast"])),
                "stl":             _safe_float(_c(_COL["stl"])),
                "tov":             _safe_float(_c(_COL["tov"])),
                "blk":             _safe_float(_c(_COL["blk"])),
                "blk_against":     _safe_float(_c(_COL["blk_against"])),
                "fouls":           _safe_float(_c(_COL["fouls"])),
                "fouls_received":  _safe_float(_c(_COL["fouls_received"])),
                "val":             _safe_float(_c(_COL["val"])),
                "plus_minus":      _safe_float(_c(_COL["plus_minus"])),
            }

    logger.warning("FEB: player '%s' not found in box score", player_token)
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(player_id: str) -> dict:
    """
    Fetch last game box score for a FEB player.

    Args:
        player_id:  "TEAMTOKEN/PLAYERTOKEN"  e.g. "ESTUDIANTES/GRANGER"

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    parts = str(player_id).split("/", 1)
    if len(parts) != 2:
        logger.warning("FEB: invalid player_id '%s' (expected TEAM/PLAYER)", player_id)
        return {}

    team_token   = parts[0].upper()
    player_token = parts[1].upper()

    # Step 1 — find the team's latest game
    game_info = _find_game_for_team(team_token)
    if not game_info:
        logger.warning("FEB: no recent game found for team '%s'", team_token)
        return {}

    game_id      = game_info["game_id"]
    game_date    = game_info.get("game_date", "")
    local_team   = game_info.get("local_team", "")
    visitor_team = game_info.get("visitor_team", "")
    local_score  = game_info.get("local_score", 0)
    visitor_score = game_info.get("visitor_score", 0)

    logger.debug("FEB: game %s  %s %s-%s %s  (looking for %s)",
                 game_id, local_team, local_score, visitor_score, visitor_team, player_token)

    # Step 2 — fetch and parse box score
    box_html = _get(f"{_GAME_BASE_URL}/{game_id}")
    if not box_html:
        return {}

    stats = _parse_game_box_score(box_html, player_token)
    if not stats:
        return {}

    if not game_date and stats.get("game_date"):
        game_date = stats["game_date"]

    # Step 3 — determine opponent and result
    is_local  = team_token in local_team
    if is_local:
        team_score, opp_score = local_score, visitor_score
        opponent = visitor_team.title()
    else:
        team_score, opp_score = visitor_score, local_score
        opponent = local_team.title()

    win_loss = "V" if team_score > opp_score else "D"
    result   = f"{win_loss} {team_score}-{opp_score}"

    return {
        "player_id":       player_id,
        "player_name":     "",        # filled by router from registry
        "team":            "",        # filled by router from registry
        "source":          "feb",
        "competition":     "Primera FEB",
        "season":          "2025-26",
        "game_date":       game_date,
        "opponent":        opponent,
        "result":          result,
        "date":            str(date.today()),
        "min":             stats["min"],
        "pts":             stats["pts"],
        "t2m":             stats["t2m"],  "t2a":  stats["t2a"],  "t2_pct":  stats["t2_pct"],
        "t3m":             stats["t3m"],  "t3a":  stats["t3a"],  "t3_pct":  stats["t3_pct"],
        "ftm":             stats["ftm"],  "fta":  stats["fta"],  "ft_pct":  stats["ft_pct"],
        "reb_off":         stats["reb_off"],
        "reb_def":         stats["reb_def"],
        "reb":             stats["reb"],
        "ast":             stats["ast"],
        "stl":             stats["stl"],
        "tov":             stats["tov"],
        "blk":             stats["blk"],
        "blk_against":     stats["blk_against"],
        "fouls":           stats["fouls"],
        "fouls_received":  stats["fouls_received"],
        "plus_minus":      stats["plus_minus"],
        "val":             stats["val"],
    }
