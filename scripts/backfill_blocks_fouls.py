#!/usr/bin/env python3
"""
One-off backfill: populate blk_against and fouls_received for historical records.

Strategy:
  EuroLeague / EuroCup — fetch the full season game list, iterate every played
    game until the player is found, extract blocksAgainst + foulsReceived.
  ACB — fetch the player game-log page, build an opponent→game-detail-URL map,
    match each historical record by opponent text, then call
    _extract_player_boxscore() on the game detail page.

Run from the repo root:
    python scripts/backfill_blocks_fouls.py
"""

from __future__ import annotations

import json
import logging
import re
import time
import glob
import sys
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

# Ensure the repo root is on the path so src.* imports work.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.sources.acb import (
    _HEADERS as _ACB_HEADERS,
    _TIMEOUT as _ACB_TIMEOUT,
    _BASE_URL as _ACB_BASE_URL,
    _build_col_map,
    _is_game_row,
    _extract_player_boxscore,
)
from src.sources.euroleague import (
    _BASE as _EL_BASE,
    _HEADERS as _EL_HEADERS,
    _TIMEOUT as _EL_TIMEOUT,
    _EL_COMP, _EC_COMP, _EL_SEASON, _EC_SEASON,
    _safe_float,
    _get_json,
    _fetch_game_stats,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

STATS_DIR = Path(__file__).resolve().parent.parent / "data" / "stats"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_all_files() -> dict[str, list[dict]]:
    """Return {filename: records_list} for all JSON stats files."""
    result: dict[str, list[dict]] = {}
    for f in sorted(glob.glob(str(STATS_DIR / "*.json"))):
        result[f] = json.loads(Path(f).read_text(encoding="utf-8"))
    return result


def _needs_fix(rec: dict) -> bool:
    src = rec.get("source", "")
    if src not in ("acb", "euroleague", "eurocup"):
        return False
    return rec.get("blk_against") is None or rec.get("fouls_received") is None


# ---------------------------------------------------------------------------
# EuroLeague / EuroCup backfill
# ---------------------------------------------------------------------------

def _fetch_el_all_stats(
    player_code: str,
    competition: str,
    season: str,
) -> dict[str, dict]:
    """
    Fetch ALL played games for a player this season.
    Returns {game_date: {blk_against, fouls_received}}.
    """
    bare_code = player_code[1:] if player_code.startswith("P") else player_code
    url  = f"{_EL_BASE}/{competition}/seasons/{season}/games"
    data = _get_json(url, {"limit": 200})
    if not data or not isinstance(data, dict):
        return {}

    played = [
        g for g in data.get("data", [])
        if str(g.get("status", "")).lower() == "result"
    ]
    played.sort(key=lambda g: str(g.get("date", "")), reverse=True)

    result: dict[str, dict] = {}
    for game in played:
        raw_code = game.get("code")
        try:
            game_code = int(raw_code)
        except (TypeError, ValueError):
            continue

        raw_date  = str(game.get("date", ""))
        game_date = raw_date[:10] if raw_date else ""
        if not game_date:
            continue

        stats, _, _, _ = _fetch_game_stats(competition, season, game_code, bare_code)
        if stats is None:
            continue

        result[game_date] = {
            "blk_against":    _safe_float(stats.get("blocksAgainst")),
            "fouls_received": _safe_float(stats.get("foulsReceived")),
        }
        time.sleep(0.1)

    return result


def backfill_euroleague(all_files: dict[str, list[dict]]) -> int:
    """
    For each EuroLeague/EuroCup record with None blk_against or fouls_received,
    look up the correct values from the API and patch in place.
    Returns total number of records updated.
    """
    # Group needed lookups by (player_id, source) → set of game_dates
    player_dates: dict[tuple[str, str], set[str]] = {}
    for records in all_files.values():
        for rec in records:
            if not _needs_fix(rec) or rec.get("source") not in ("euroleague", "eurocup"):
                continue
            key = (rec.get("player_id", ""), rec.get("source", ""))
            player_dates.setdefault(key, set()).add(rec.get("game_date", ""))

    if not player_dates:
        logger.info("EL/EC: nothing to backfill")
        return 0

    # Fetch full season stats for each player once
    logger.info("EL/EC: fetching stats for %d player+competition combos", len(player_dates))
    cache: dict[tuple[str, str], dict[str, dict]] = {}
    for (player_id, source), dates in player_dates.items():
        comp   = _EL_COMP if source == "euroleague" else _EC_COMP
        season = _EL_SEASON if source == "euroleague" else _EC_SEASON
        logger.info("  %s  [%s]  need dates: %s", player_id, source, sorted(dates))
        cache[(player_id, source)] = _fetch_el_all_stats(player_id, comp, season)
        time.sleep(0.3)

    # Apply patches
    updated = 0
    for records in all_files.values():
        for rec in records:
            if not _needs_fix(rec) or rec.get("source") not in ("euroleague", "eurocup"):
                continue
            key = (rec.get("player_id", ""), rec.get("source", ""))
            game_date = rec.get("game_date", "")
            patch = cache.get(key, {}).get(game_date)
            if patch:
                if rec.get("blk_against") is None and patch.get("blk_against") is not None:
                    rec["blk_against"] = patch["blk_against"]
                    updated += 1
                if rec.get("fouls_received") is None and patch.get("fouls_received") is not None:
                    rec["fouls_received"] = patch["fouls_received"]
                    updated += 1

    logger.info("EL/EC: patched %d field(s)", updated)
    return updated


# ---------------------------------------------------------------------------
# ACB backfill
# ---------------------------------------------------------------------------

def _fetch_acb_game_map(player_id: str) -> dict[str, str]:
    """
    Fetch the ACB game log for a player and return {opponent_text: game_detail_url}.
    Skips players whose page redirects (blocked by ACB anti-scraping).
    """
    url = f"{_ACB_BASE_URL}/{player_id}"
    try:
        resp = requests.get(
            url, headers=_ACB_HEADERS, timeout=_ACB_TIMEOUT, allow_redirects=False
        )
        if resp.status_code in (301, 302):
            logger.warning("ACB id=%s: redirected (blocked)", player_id)
            return {}
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ACB id=%s: request failed: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    all_tables = soup.find_all("table")

    target_table: Tag | None = None
    col_map: dict[str, int] = {}
    for tbl in all_tables:
        for row in tbl.find_all("tr")[:5]:
            cells = row.find_all(["th", "td"])
            candidate = _build_col_map(cells)
            if len(candidate) > len(col_map):
                col_map = candidate
                target_table = tbl

    if target_table is None or len(col_map) < 3:
        logger.warning("ACB id=%s: no game log table found", player_id)
        return {}

    game_map: dict[str, str] = {}
    opp_idx = col_map.get("opponent")
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not _is_game_row(cells):
            continue
        # Opponent text
        opp = cells[opp_idx].get_text(strip=True) if opp_idx is not None and opp_idx < len(cells) else ""
        if not opp:
            continue
        # Game detail link
        link = None
        for cell in cells:
            a = cell.find("a", href=re.compile(r"/partido/ver/id/\d+"))
            if a:
                link = "https://www.acb.com" + a["href"]
                break
        if link:
            game_map[opp] = link

    return game_map


def _fetch_acb_detail(game_url: str, player_name: str) -> dict[str, float | None]:
    """Fetch the ACB game detail page and extract player blocks/fouls."""
    try:
        time.sleep(0.4)
        resp = requests.get(game_url, headers=_ACB_HEADERS, timeout=_ACB_TIMEOUT)
        resp.raise_for_status()
        return _extract_player_boxscore(resp.text, player_name)
    except requests.RequestException as exc:
        logger.warning("ACB game detail fetch failed %s: %s", game_url, exc)
        return {}


def backfill_acb(all_files: dict[str, list[dict]]) -> int:
    """
    For each ACB record with None blk_against or fouls_received, re-fetch the
    game detail page and patch in place.
    Returns total number of records updated.
    """
    # Group by player_id → set of (opponent, player_name) pairs
    player_info: dict[str, dict[str, Any]] = {}
    for records in all_files.values():
        for rec in records:
            if not _needs_fix(rec) or rec.get("source") != "acb":
                continue
            pid  = rec.get("player_id", "")
            name = rec.get("player_name", pid)
            opp  = rec.get("opponent", "")
            if pid not in player_info:
                player_info[pid] = {"name": name, "opponents": set()}
            player_info[pid]["opponents"].add(opp)

    if not player_info:
        logger.info("ACB: nothing to backfill")
        return 0

    logger.info("ACB: fetching game logs for %d player(s)", len(player_info))

    # Fetch game log → {opponent: game_url} for each player
    game_maps: dict[str, dict[str, str]] = {}
    for pid, info in player_info.items():
        logger.info("  ACB id=%s  (%s)", pid, info["name"])
        game_maps[pid] = _fetch_acb_game_map(pid)
        time.sleep(1.0)

    # For each opponent that needs backfill, fetch game detail once
    # Cache: (player_id, opponent) → {blk, blk_against, fouls, fouls_received}
    detail_cache: dict[tuple[str, str], dict] = {}

    for pid, info in player_info.items():
        gmap = game_maps.get(pid, {})
        for opp in info["opponents"]:
            game_url = gmap.get(opp)
            if not game_url:
                logger.debug("ACB id=%s: no game URL for opponent=%r", pid, opp)
                continue
            logger.info("  Fetching game detail: %s (player=%s)", game_url, info["name"])
            detail = _fetch_acb_detail(game_url, info["name"])
            detail_cache[(pid, opp)] = detail

    # Apply patches
    updated = 0
    for records in all_files.values():
        for rec in records:
            if not _needs_fix(rec) or rec.get("source") != "acb":
                continue
            key = (rec.get("player_id", ""), rec.get("opponent", ""))
            patch = detail_cache.get(key, {})
            if rec.get("blk_against") is None and patch.get("blk_against") is not None:
                rec["blk_against"] = patch["blk_against"]
                updated += 1
            if rec.get("fouls_received") is None and patch.get("fouls_received") is not None:
                rec["fouls_received"] = patch["fouls_received"]
                updated += 1

    logger.info("ACB: patched %d field(s)", updated)
    return updated


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_all(all_files: dict[str, list[dict]]) -> None:
    for fpath, records in all_files.items():
        Path(fpath).write_text(
            json.dumps(records, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    logger.info("Saved %d file(s)", len(all_files))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Loading historical stats files...")
    all_files = _load_all_files()
    total_records = sum(len(v) for v in all_files.values())
    needs_fix = sum(
        1 for records in all_files.values()
        for r in records if _needs_fix(r)
    )
    logger.info("%d total records, %d need blk_against/fouls_received backfill", total_records, needs_fix)

    el_updated  = backfill_euroleague(all_files)
    acb_updated = backfill_acb(all_files)

    total_updated = el_updated + acb_updated
    logger.info("Total fields patched: %d", total_updated)

    if total_updated > 0:
        save_all(all_files)
        logger.info("Done. Run `python main.py` to regenerate today's data with the new schema.")
    else:
        logger.info("Nothing changed — no files written.")


if __name__ == "__main__":
    main()
