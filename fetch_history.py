"""
fetch_history.py — one-off historical data fetch for all tracked players, 2025-26 season.

Collects ALL played games this season (not just the most recent) for every
active player in the registry. Writes results grouped by game date into
data/stats/{YYYY-MM-DD}.json and data/stats/{YYYY-MM-DD}.csv, merging with
any records already in those files.

Supported sources:
  ✓ acb          — full game log; fetches individual game pages for dates/results
  ✓ euroleague   — incrowdsports API, bulk one-pass over all played games
  ✓ eurocup      — incrowdsports API, bulk one-pass over all played games
  ✓ nba          — stats.nba.com playergamelog, all games in one response
  ✓ aba          — ABA Liga HTML page, accumulates all game rows
  ✓ eurobasket   — basketball.eurobasket.com, accumulates all game rows
  ✓ ncaa_espn    — ESPN game log, accumulates all rows
  ✗ feb          — broken (HTTP 500) and only season averages; skipped
  ✗ lega         — only season averages; no per-game endpoint; skipped
  ✗ bcl          — only season averages; no per-game endpoint; skipped

Usage:
    python fetch_history.py
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import date as date_cls
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Import parsing helpers from existing scrapers
# ---------------------------------------------------------------------------

from src.sources.acb import (
    _safe_float    as _acb_safe_float,
    _parse_minutes as _acb_parse_minutes,
    _build_col_map as _acb_build_col_map,
    _is_game_row   as _acb_is_game_row,
    _parse_game_row as _acb_parse_game_row,
    _extract_result_from_game_page as _acb_extract_result,
    _HEADERS as _ACB_HEADERS,
    _BASE_URL as _ACB_BASE_URL,
    _TIMEOUT as _ACB_TIMEOUT,
    _SLEEP   as _ACB_SLEEP,
)

from src.sources.euroleague import (
    _get_json      as _el_get_json,
    _safe_float    as _el_safe_float,
    _parse_minutes as _el_parse_minutes,
    _pct           as _el_pct,
    _team_score    as _el_team_score,
    _BASE          as _EL_BASE,
    _EL_COMP, _EC_COMP, _EL_SEASON, _EC_SEASON,
    _HEADERS as _EL_HEADERS,
)

from src.sources.nba import (
    _safe_float    as _nba_safe_float,
    _parse_minutes as _nba_parse_minutes,
    _parse_nba_date,
    _parse_opponent as _nba_parse_opponent,
    _row_to_dict   as _nba_row_to_dict,
    _BASE_URL as _NBA_BASE_URL,
    _HEADERS  as _NBA_HEADERS,
    _TIMEOUT  as _NBA_TIMEOUT,
    _CURRENT_SEASON as _NBA_SEASON,
)

from src.sources.aba import (
    _safe_float    as _aba_safe_float,
    _parse_minutes as _aba_parse_minutes,
    _cell_text     as _aba_cell_text,
    _parse_aba_date,
    _parse_result  as _aba_parse_result,
    _is_game_row   as _aba_is_game_row,
    _make_slug     as _aba_make_slug,
    _BASE_URL as _ABA_BASE_URL,
    _TIMEOUT  as _ABA_TIMEOUT,
    _HEADERS  as _ABA_HEADERS,
    _CURRENT_SEASON as _ABA_SEASON,
    _LEAGUE         as _ABA_LEAGUE,
)

from src.sources.eurobasket import (
    _safe_float    as _eb_safe_float,
    _parse_minutes as _eb_parse_minutes,
    _cell_text     as _eb_cell_text,
    _parse_date_flexible as _eb_parse_date,
    _parse_result  as _eb_parse_result,
    _build_col_map as _eb_build_col_map,
    _is_game_row   as _eb_is_game_row,
    _make_slug     as _eb_make_slug,
    _BASE_URL as _EB_BASE_URL,
    _TIMEOUT  as _EB_TIMEOUT,
    _HEADERS  as _EB_HEADERS,
)

from src.sources.ncaa_espn import (
    _safe_float          as _espn_safe_float,
    _parse_made_attempted as _espn_parse_ma,
    _cell_text           as _espn_cell_text,
    _parse_espn_date,
    _find_col_map        as _espn_find_col_map,
    _make_slug           as _espn_make_slug,
    _BASE_URL as _ESPN_BASE_URL,
    _TIMEOUT  as _ESPN_TIMEOUT,
    _HEADERS  as _ESPN_HEADERS,
)

from src.players import get_active_players

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("fetch_history")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TODAY   = str(date_cls.today())
_SEASON = "2025-26"

# ACB: cache game pages so multiple players sharing a game don't double-fetch.
# Maps game_id → (game_date, result_str)
_acb_game_page_cache: dict[str, tuple[str, str]] = {}


# =============================================================================
# ACB — all games
# =============================================================================

def _fetch_all_acb(player_id: str, player_name: str) -> list[dict]:
    """Return stat dicts for ALL ACB games in the 2025-26 season."""
    url = f"{_ACB_BASE_URL}/{player_id}"
    logger.info("ACB history: %s  url=%s", player_name, url)
    time.sleep(_ACB_SLEEP)

    try:
        resp = requests.get(url, headers=_ACB_HEADERS, timeout=_ACB_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ACB request failed id=%s: %s", player_id, exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Find the game log table with the most recognised column headers
    all_tables = soup.find_all("table")
    target_table = None
    col_map: dict[str, int] = {}
    for tbl in all_tables:
        for row in tbl.find_all("tr")[:5]:
            cells = row.find_all(["th", "td"])
            candidate = _acb_build_col_map(cells)
            if len(candidate) > len(col_map):
                col_map = candidate
                target_table = tbl

    if target_table is None or len(col_map) < 3:
        logger.warning("ACB: no usable game log table for id=%s", player_id)
        return []

    results: list[dict] = []

    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not _acb_is_game_row(cells):
            continue

        stats = _acb_parse_game_row(cells, col_map)
        if stats.get("min") is None and stats.get("pts") is None:
            continue  # future / unplayed game

        opponent    = stats.get("opponent", "")
        result_str  = stats.get("result", "").strip()

        # Extract game page link → date + result
        game_date = ""
        game_id   = None
        for cell in cells:
            link = cell.find("a", href=re.compile(r"/partido/ver/id/(\d+)"))
            if link:
                m = re.search(r"/partido/ver/id/(\d+)", link["href"])
                if m:
                    game_id = m.group(1)
                break

        if game_id:
            if game_id in _acb_game_page_cache:
                game_date, cached_result = _acb_game_page_cache[game_id]
                if cached_result:
                    result_str = cached_result
            else:
                try:
                    time.sleep(0.4)
                    game_url = f"https://www.acb.com/partido/ver/id/{game_id}"
                    gr = requests.get(game_url, headers=_ACB_HEADERS, timeout=_ACB_TIMEOUT)
                    gr.raise_for_status()
                    page_text = gr.text

                    # Date
                    for dm in re.finditer(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", page_text):
                        d, mo, y = dm.group(1), dm.group(2), dm.group(3)
                        if not (2020 <= int(y) <= 2035):
                            continue
                        if not (1 <= int(mo) <= 12 and 1 <= int(d) <= 31):
                            continue
                        game_date = f"{y}-{int(mo):02d}-{int(d):02d}"
                        break

                    # Result
                    extracted = _acb_extract_result(page_text, opponent)
                    if extracted:
                        if result_str in ("V", "D") and not re.search(r"\d", extracted):
                            result_str = f"{result_str} {extracted}"
                        else:
                            result_str = extracted

                    _acb_game_page_cache[game_id] = (game_date, result_str)

                except requests.RequestException:
                    _acb_game_page_cache[game_id] = ("", "")

        if not game_date:
            continue  # Can't confirm when this game was played

        results.append({
            "player_id":   player_id,
            "player_name": player_name,
            "source":      "acb",
            "competition": "ACB",
            "season":      _SEASON,
            "game_date":   game_date,
            "opponent":    opponent,
            "result":      result_str,
            "date":        TODAY,
            "min":         stats["min"],
            "pts":         stats["pts"],
            "t2m":         stats["t2m"],   "t2a":  stats["t2a"],   "t2_pct":  stats["t2_pct"],
            "t3m":         stats["t3m"],   "t3a":  stats["t3a"],   "t3_pct":  stats["t3_pct"],
            "ftm":         stats["ftm"],   "fta":  stats["fta"],   "ft_pct":  stats["ft_pct"],
            "reb_off":     stats["reb_off"],
            "reb_def":     stats["reb_def"],
            "reb":         stats["reb"],
            "ast":         stats["ast"],
            "stl":         stats["stl"],
            "tov":         stats["tov"],
            "blk":         None,
            "fouls":       stats["fouls"],
            "plus_minus":  stats["plus_minus"],
            "val":         stats["val"],
        })

    logger.info("ACB history: %d games for %s", len(results), player_name)
    return results


# =============================================================================
# EuroLeague / EuroCup — bulk fetch (one pass over all played games)
# =============================================================================

def _build_el_stat_dict(
    stats: dict,
    player_code: str,
    player_name: str,
    team: str,
    opponent: str,
    result: str,
    game_date: str,
    competition_label: str,
    season: str,
) -> dict:
    """Build a canonical stat dict from an incrowdsports stats object."""
    t2m = _el_safe_float(stats.get("fieldGoalsMade2"))
    t2a = _el_safe_float(stats.get("fieldGoalsAttempted2"))
    t2_pct = (
        round(t2m / t2a * 100, 1) if t2m is not None and t2a
        else _el_pct(stats.get("twoPointShootingPercentage"))
    )
    t3m = _el_safe_float(stats.get("fieldGoalsMade3"))
    t3a = _el_safe_float(stats.get("fieldGoalsAttempted3"))
    t3_pct = (
        round(t3m / t3a * 100, 1) if t3m is not None and t3a
        else _el_pct(stats.get("threePointShootingPercentage"))
    )
    ftm = _el_safe_float(stats.get("freeThrowsMade"))
    fta = _el_safe_float(stats.get("freeThrowsAttempted"))
    ft_pct = (
        round(ftm / fta * 100, 1) if ftm is not None and fta
        else _el_pct(stats.get("freeThrowShootingPercentage"))
    )
    return {
        "player_id":   player_code,
        "player_name": player_name,
        "team":        team,
        "source":      competition_label.lower().replace(" ", ""),
        "competition": competition_label,
        "season":      season,
        "game_date":   game_date,
        "opponent":    opponent,
        "result":      result,
        "date":        TODAY,
        "min":         _el_parse_minutes(stats.get("timePlayed")),
        "pts":         _el_safe_float(stats.get("points")),
        "t2m":         t2m,   "t2a":  t2a,  "t2_pct":  t2_pct,
        "t3m":         t3m,   "t3a":  t3a,  "t3_pct":  t3_pct,
        "ftm":         ftm,   "fta":  fta,  "ft_pct":  ft_pct,
        "reb_off":     _el_safe_float(stats.get("offensiveRebounds")),
        "reb_def":     _el_safe_float(stats.get("defensiveRebounds")),
        "reb":         _el_safe_float(stats.get("totalRebounds")),
        "ast":         _el_safe_float(stats.get("assistances")),
        "stl":         _el_safe_float(stats.get("steals")),
        "tov":         _el_safe_float(stats.get("turnovers")),
        "blk":         _el_safe_float(stats.get("blocksFavour")),
        "fouls":       _el_safe_float(stats.get("foulsCommited")),
        "plus_minus":  _el_safe_float(stats.get("plusMinus")),
        "val":         _el_safe_float(stats.get("valuation")),
    }


def _fetch_all_el_ec(
    tracked_codes: dict[str, tuple[str, str, str]],  # bare_code → (player_name, code_with_P, team)
    competition: str,
    season: str,
) -> list[dict]:
    """
    Bulk-fetch all played games in a competition/season.

    One API call per game box score, checking whether any tracked player appeared.
    Returns a flat list of stat dicts across all tracked players.
    """
    comp_label = "EuroLeague" if competition == _EL_COMP else "EuroCup"
    url  = f"{_EL_BASE}/{competition}/seasons/{season}/games"
    data = _el_get_json(url, params={"limit": 400})

    if not data or not isinstance(data, dict):
        logger.warning("%s: could not fetch games list", comp_label)
        return []

    games  = data.get("data", [])
    played = [g for g in games if str(g.get("status", "")).lower() == "result"]
    played.sort(key=lambda g: str(g.get("date", "")))  # ascending

    logger.info("%s: %d played games found; fetching box scores...", comp_label, len(played))

    all_results: list[dict] = []

    for idx, game in enumerate(played, 1):
        raw_code = game.get("code")
        try:
            game_code = int(raw_code)
        except (TypeError, ValueError):
            continue

        raw_date  = str(game.get("date", ""))
        game_date = raw_date[:10] if raw_date else ""
        if not game_date:
            continue

        box_url  = f"{_EL_BASE}/{competition}/seasons/{season}/games/{game_code}/stats"
        box_data = _el_get_json(box_url)

        if not box_data or not isinstance(box_data, dict):
            continue

        sides = ("local", "road")
        for i_side, side in enumerate(sides):
            team_data  = box_data.get(side, {})
            my_score   = _el_team_score(team_data)
            other_side = sides[1 - i_side]
            opp_data   = box_data.get(other_side, {})
            opp_score  = _el_team_score(opp_data)

            opp_players = opp_data.get("players", [])
            opponent = ""
            if opp_players:
                opponent = opp_players[0].get("player", {}).get("club", {}).get("name", "")

            result = ""
            if my_score is not None and opp_score is not None:
                wl     = "V" if my_score > opp_score else "D"
                result = f"{wl} {my_score}-{opp_score}"

            for p in team_data.get("players", []):
                person    = p.get("player", {}).get("person", {})
                bare_code = person.get("code", "")
                if bare_code not in tracked_codes:
                    continue

                player_name, player_code_with_P, team = tracked_codes[bare_code]
                stat_dict = _build_el_stat_dict(
                    p.get("stats", {}),
                    player_code_with_P,
                    player_name,
                    team,
                    opponent,
                    result,
                    game_date,
                    comp_label,
                    season,
                )
                all_results.append(stat_dict)

        if idx % 25 == 0:
            logger.info("%s: processed %d/%d games", comp_label, idx, len(played))

    # Report per-player
    per_player: dict[str, int] = defaultdict(int)
    for r in all_results:
        per_player[r["player_name"]] += 1
    for pn, cnt in per_player.items():
        logger.info("%s history: %d games for %s", comp_label, cnt, pn)

    return all_results


# =============================================================================
# NBA — all games
# =============================================================================

def _fetch_all_nba(player_id: str, player_name: str) -> list[dict]:
    """Return stat dicts for ALL NBA games in the current season."""
    logger.info("NBA history: %s", player_name)
    params = {
        "PlayerID":   str(player_id),
        "Season":     _NBA_SEASON,
        "SeasonType": "Regular Season",
    }
    try:
        resp = requests.get(_NBA_BASE_URL, headers=_NBA_HEADERS, params=params, timeout=_NBA_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("NBA request failed player_id=%s: %s", player_id, exc)
        return []

    result_set = next(
        (rs for rs in data.get("resultSets", []) if rs.get("name") == "PlayerGameLog"),
        None,
    )
    if result_set is None:
        return []

    headers = result_set.get("headers", [])
    rows    = result_set.get("rowSet", [])

    def _pct(v):
        f = _nba_safe_float(v)
        return round(f * 100, 1) if f is not None else None

    results: list[dict] = []
    for raw_row in rows:
        row = _nba_row_to_dict(headers, raw_row)

        fgm  = _nba_safe_float(row.get("FGM"))
        fga  = _nba_safe_float(row.get("FGA"))
        fg3m = _nba_safe_float(row.get("FG3M"))
        fg3a = _nba_safe_float(row.get("FG3A"))

        t2m    = round(fgm - fg3m, 1) if fgm is not None and fg3m is not None else None
        t2a    = round(fga - fg3a, 1) if fga is not None and fg3a is not None else None
        t2_pct = round(t2m / t2a * 100, 1) if t2m is not None and t2a else None

        game_date = _parse_nba_date(str(row.get("GAME_DATE", "")))
        if not game_date or not re.match(r"\d{4}-\d{2}-\d{2}", game_date):
            continue

        matchup = str(row.get("MATCHUP", ""))
        results.append({
            "player_id":   str(player_id),
            "player_name": player_name,
            "source":      "nba",
            "competition": "NBA",
            "season":      _NBA_SEASON,
            "game_date":   game_date,
            "opponent":    _nba_parse_opponent(matchup),
            "result":      str(row.get("WL", "")),
            "date":        TODAY,
            "min":         _nba_parse_minutes(row.get("MIN")),
            "pts":         _nba_safe_float(row.get("PTS")),
            "t2m":         t2m,   "t2a":  t2a,  "t2_pct":  t2_pct,
            "t3m":         fg3m,  "t3a":  fg3a, "t3_pct":  _pct(row.get("FG3_PCT")),
            "ftm":         _nba_safe_float(row.get("FTM")),
            "fta":         _nba_safe_float(row.get("FTA")),
            "ft_pct":      _pct(row.get("FT_PCT")),
            "reb_off":     _nba_safe_float(row.get("OREB")),
            "reb_def":     _nba_safe_float(row.get("DREB")),
            "reb":         _nba_safe_float(row.get("REB")),
            "ast":         _nba_safe_float(row.get("AST")),
            "stl":         _nba_safe_float(row.get("STL")),
            "tov":         _nba_safe_float(row.get("TOV")),
            "blk":         _nba_safe_float(row.get("BLK")),
            "fouls":       _nba_safe_float(row.get("PF")),
            "plus_minus":  _nba_safe_float(row.get("PLUS_MINUS")),
            "val":         None,
        })

    logger.info("NBA history: %d games for %s", len(results), player_name)
    return results


# =============================================================================
# ABA Liga — all games
# =============================================================================

def _fetch_all_aba(player_id: str, player_name: str) -> list[dict]:
    """Return stat dicts for ALL ABA Liga games in the 2025-26 season."""
    slug = _aba_make_slug(player_name)
    url  = f"{_ABA_BASE_URL}/{player_id}/{_ABA_SEASON}/{_ABA_LEAGUE}/{slug}/"
    logger.info("ABA history: %s  url=%s", player_name, url)

    try:
        resp = requests.get(url, headers=_ABA_HEADERS, timeout=_ABA_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ABA request failed player_id=%s: %s", player_id, exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    all_game_rows: list = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if _aba_is_game_row(cells):
                all_game_rows.append(cells)

    if not all_game_rows:
        logger.warning("ABA: no game rows for player_id=%s", player_id)
        return []

    results: list[dict] = []
    for c in all_game_rows:
        n = len(c)

        def _get(idx: int) -> str | None:
            return _aba_cell_text(c[idx]) if idx < n else None

        raw_date  = _get(0) or ""
        game_date = _parse_aba_date(raw_date)
        if not game_date or not re.match(r"\d{4}-\d{2}-\d{2}", game_date):
            continue

        offset = 3 if n >= 26 else 2
        if offset == 3:
            opponent   = _get(1) or ""
            result_raw = _get(2) or ""
            result     = _aba_parse_result(result_raw)
        else:
            combined = _get(1) or ""
            score_m  = re.search(r"(\d{2,3})[:\-](\d{2,3})", combined)
            if score_m:
                opponent = combined[:score_m.start()].strip()
                result   = _aba_parse_result(combined)
            else:
                opponent = combined
                result   = ""

        def _s(rel: int) -> str | None:
            return _get(offset + rel)

        results.append({
            "player_id":   str(player_id),
            "player_name": player_name,
            "source":      "aba",
            "competition": "ABA League",
            "season":      _SEASON,
            "game_date":   game_date,
            "opponent":    opponent,
            "result":      result,
            "date":        TODAY,
            "min":         _aba_parse_minutes(_s(0)),
            "pts":         _aba_safe_float(_s(1)),
            "t2m":         _aba_safe_float(_s(3)),
            "t2a":         _aba_safe_float(_s(4)),
            "t2_pct":      _aba_safe_float(_s(5)),
            "t3m":         _aba_safe_float(_s(6)),
            "t3a":         _aba_safe_float(_s(7)),
            "t3_pct":      _aba_safe_float(_s(8)),
            "ftm":         _aba_safe_float(_s(9)),
            "fta":         _aba_safe_float(_s(10)),
            "ft_pct":      _aba_safe_float(_s(11)),
            "reb_def":     _aba_safe_float(_s(12)),
            "reb_off":     _aba_safe_float(_s(13)),
            "reb":         _aba_safe_float(_s(14)),
            "ast":         _aba_safe_float(_s(15)),
            "stl":         _aba_safe_float(_s(16)),
            "tov":         _aba_safe_float(_s(17)),
            "blk":         _aba_safe_float(_s(18)),
            "fouls":       _aba_safe_float(_s(20)),
            "plus_minus":  _aba_safe_float(_s(22)),
            "val":         _aba_safe_float(_s(23)),
        })

    logger.info("ABA history: %d games for %s", len(results), player_name)
    return results


# =============================================================================
# EuroBasket — all games
# =============================================================================

def _fetch_all_eurobasket(player_id: str, player_name: str, competition: str) -> list[dict]:
    """Return stat dicts for ALL games from basketball.eurobasket.com."""
    slug = _eb_make_slug(player_name)
    url  = f"{_EB_BASE_URL}/{slug}/{player_id}"
    logger.info("EuroBasket history: %s (%s)  url=%s", player_name, competition, url)

    try:
        resp = requests.get(url, headers=_EB_HEADERS, timeout=_EB_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("EuroBasket request failed player_id=%s: %s", player_id, exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[dict] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        hcells = rows[0].find_all(["th", "td"])
        cmap   = _eb_build_col_map(hcells)
        if len(cmap) < 5:
            continue

        first_stat_col = min(cmap.values())

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not _eb_is_game_row(cells, cmap):
                continue

            row_text  = [_eb_cell_text(c) for c in cells]
            game_date = _eb_parse_date(row_text[0])
            if not game_date or not re.match(r"\d{4}-\d{2}-\d{2}", game_date):
                continue

            opponent   = row_text[1] if first_stat_col > 1 and len(row_text) > 1 else ""
            result_raw = row_text[2] if first_stat_col > 2 and len(row_text) > 2 else ""
            if result_raw and re.match(r"^\d+(\.\d+)?$", result_raw):
                result_raw = ""
            result = _eb_parse_result(result_raw)

            def _get(field: str) -> float | None:
                idx = cmap.get(field)
                if idx is not None and idx < len(row_text):
                    return _eb_safe_float(row_text[idx])
                return None

            results.append({
                "player_id":   str(player_id),
                "player_name": player_name,
                "source":      "eurobasket",
                "competition": competition,
                "season":      _SEASON,
                "game_date":   game_date,
                "opponent":    opponent,
                "result":      result,
                "date":        TODAY,
                "min":         _eb_parse_minutes(_get("min")) if _get("min") else None,
                "pts":         _get("pts"),
                "t2m":         _get("t2m"),   "t2a":  _get("t2a"),  "t2_pct":  _get("t2_pct"),
                "t3m":         _get("t3m"),   "t3a":  _get("t3a"),  "t3_pct":  _get("t3_pct"),
                "ftm":         _get("ftm"),   "fta":  _get("fta"),  "ft_pct":  _get("ft_pct"),
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
            })

    logger.info("EuroBasket history: %d games for %s", len(results), player_name)
    return results


# =============================================================================
# NCAA ESPN — all games
# =============================================================================

def _fetch_all_ncaa_espn(player_id: str, player_name: str) -> list[dict]:
    """Return stat dicts for ALL games from ESPN college basketball game log."""
    slug = _espn_make_slug(player_name)
    url  = f"{_ESPN_BASE_URL}/{player_id}/{slug}"
    logger.info("ESPN NCAA history: %s  url=%s", player_name, url)

    try:
        resp = requests.get(url, headers=_ESPN_HEADERS, timeout=_ESPN_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ESPN NCAA request failed player_id=%s: %s", player_id, exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    game_table = None
    col_map: dict[str, int] = {}
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [_espn_cell_text(c).lower() for c in header_row.find_all(["th", "td"])]
        if "pts" in headers and ("fg" in headers or "min" in headers):
            game_table = table
            col_map    = _espn_find_col_map(headers)
            break

    if game_table is None or not col_map:
        logger.warning("ESPN NCAA: no game log table for player_id=%s", player_id)
        return []

    def _get(cells: list, key: str) -> str:
        idx = col_map.get(key)
        if idx is not None and idx < len(cells):
            return _espn_cell_text(cells[idx])
        return ""

    results: list[dict] = []
    for row in game_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells or cells[0].name == "th":
            continue
        pts_text = _get(cells, "pts")
        if not pts_text or not re.match(r"^\d", pts_text):
            continue

        game_date = _parse_espn_date(_get(cells, "date"))
        if not game_date or not re.match(r"\d{4}-\d{2}-\d{2}", game_date):
            continue

        fgm, fga   = _espn_parse_ma(_get(cells, "fg"))
        fg3m, fg3a = _espn_parse_ma(_get(cells, "3pt"))
        ftm, fta   = _espn_parse_ma(_get(cells, "ft"))

        t2m    = round(fgm - fg3m, 1) if fgm is not None and fg3m is not None else None
        t2a    = round(fga - fg3a, 1) if fga is not None and fg3a is not None else None
        t2_pct = round(t2m / t2a * 100, 1) if t2m is not None and t2a else None
        t3_pct = round(fg3m / fg3a * 100, 1) if fg3m is not None and fg3a else None
        ft_pct = round(ftm / fta * 100, 1) if ftm is not None and fta else None

        results.append({
            "player_id":   str(player_id),
            "player_name": player_name,
            "source":      "ncaa_espn",
            "competition": "NCAA",
            "season":      _SEASON,
            "game_date":   game_date,
            "opponent":    _get(cells, "opp"),
            "result":      _get(cells, "result"),
            "date":        TODAY,
            "min":         _espn_safe_float(_get(cells, "min")),
            "pts":         _espn_safe_float(_get(cells, "pts")),
            "t2m":         t2m,   "t2a":  t2a,  "t2_pct":  t2_pct,
            "t3m":         fg3m,  "t3a":  fg3a, "t3_pct":  t3_pct,
            "ftm":         ftm,   "fta":  fta,  "ft_pct":  ft_pct,
            "reb_off":     None,
            "reb_def":     None,
            "reb":         _espn_safe_float(_get(cells, "reb")),
            "ast":         _espn_safe_float(_get(cells, "ast")),
            "stl":         _espn_safe_float(_get(cells, "stl")),
            "tov":         _espn_safe_float(_get(cells, "to")),
            "blk":         _espn_safe_float(_get(cells, "blk")),
            "fouls":       _espn_safe_float(_get(cells, "pf")),
            "plus_minus":  None,
            "val":         None,
        })

    logger.info("ESPN NCAA history: %d games for %s", len(results), player_name)
    return results


# =============================================================================
# Storage: merge new records into existing per-date files
# =============================================================================

def _merge_and_save(all_records: list[dict]) -> None:
    """
    Group records by game_date, merge with existing daily files, and save.

    De-duplication key: (player_name, source, competition, game_date).
    Existing records take priority; new records are only added if no duplicate exists.
    """
    # Group by game_date
    by_date: dict[str, list[dict]] = defaultdict(list)
    for rec in all_records:
        gdate = rec.get("game_date", "")
        if gdate and re.match(r"\d{4}-\d{2}-\d{2}", gdate):
            by_date[gdate].append(rec)

    logger.info("Records span %d unique game dates", len(by_date))

    stats_dir = Path(__file__).parent / "data" / "stats"
    stats_dir.mkdir(parents=True, exist_ok=True)

    total_new = 0
    for gdate in sorted(by_date):
        new_recs  = by_date[gdate]
        json_path = stats_dir / f"{gdate}.json"

        # Load existing
        existing: list[dict] = []
        if json_path.exists():
            try:
                with json_path.open(encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                pass

        existing_keys = {
            (r.get("player_name"), r.get("source"), r.get("competition"), r.get("game_date"))
            for r in existing
        }

        added = []
        for rec in new_recs:
            key = (
                rec.get("player_name"),
                rec.get("source"),
                rec.get("competition"),
                rec.get("game_date"),
            )
            if key not in existing_keys:
                added.append(rec)
                existing_keys.add(key)

        if not added:
            continue

        merged = existing + added
        merged.sort(key=lambda r: (r.get("player_name", ""), r.get("source", "")))

        # Write JSON
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2, default=str)

        # Write CSV
        csv_path = stats_dir / f"{gdate}.csv"
        pd.DataFrame(merged).to_csv(csv_path, index=False, encoding="utf-8")

        logger.info("  %s  → %d total records (%d new)", gdate, len(merged), len(added))
        total_new += len(added)

    logger.info("Done. %d new records written across %d dates.", total_new, len(by_date))


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    players = get_active_players()
    logger.info("Loaded %d active players from registry", len(players))

    all_records: list[dict] = []

    # ------------------------------------------------------------------
    # EuroLeague — bulk fetch (all tracked EL players in one pass)
    # ------------------------------------------------------------------
    el_tracked: dict[str, tuple[str, str, str]] = {}  # bare_code → (name, code_P, team)
    ec_tracked: dict[str, tuple[str, str, str]] = {}

    for player in players:
        for src in player.sources:
            if not src.is_ready:
                continue
            bare = src.id[1:] if src.id.startswith("P") else src.id
            if src.type == "euroleague":
                el_tracked[bare] = (player.name, src.id, player.team)
            elif src.type == "eurocup":
                ec_tracked[bare] = (player.name, src.id, player.team)

    if el_tracked:
        logger.info("--- EuroLeague bulk fetch (%d players) ---", len(el_tracked))
        all_records.extend(_fetch_all_el_ec(el_tracked, _EL_COMP, _EL_SEASON))

    if ec_tracked:
        logger.info("--- EuroCup bulk fetch (%d players) ---", len(ec_tracked))
        all_records.extend(_fetch_all_el_ec(ec_tracked, _EC_COMP, _EC_SEASON))

    # ------------------------------------------------------------------
    # Per-player sources
    # ------------------------------------------------------------------
    for player in players:
        for src in player.sources:
            if not src.is_ready:
                continue

            recs: list[dict] = []

            if src.type == "acb":
                recs = _fetch_all_acb(src.id, player.name)

            elif src.type == "nba":
                recs = _fetch_all_nba(src.id, player.name)

            elif src.type == "aba":
                recs = _fetch_all_aba(src.id, player.name)

            elif src.type == "eurobasket":
                recs = _fetch_all_eurobasket(src.id, player.name, src.competition)

            elif src.type == "ncaa_espn":
                recs = _fetch_all_ncaa_espn(src.id, player.name)

            elif src.type in ("euroleague", "eurocup"):
                pass  # already handled in bulk fetch above

            else:
                logger.debug("Skipping %s source for %s (no per-game data)", src.type, player.name)
                continue

            # Ensure registry fields override API fields
            for r in recs:
                r["player_name"] = player.name
                r.setdefault("team", player.team)

            all_records.extend(recs)

    logger.info("Total records collected: %d", len(all_records))
    _merge_and_save(all_records)


if __name__ == "__main__":
    main()
