#!/usr/bin/env python3
"""
Targeted fix for remaining null fields after main backfill:

1. fouls=None for Francis Alonso (3 games) and Kameron Taylor (1 game):
   Re-fetch game detail pages using the game log page to find URLs by date.

2. Encoding-mismatched records in 2026-03-12 and 2026-03-13:
   Copy correct values from sibling records in other files (same player/game_date).

3. Jaime Fernández 2025-11-23: blk/blk_against/fouls_received all None.
   Re-fetch game detail page.

Run from repo root: python scripts/fix_remaining_nulls.py
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.sources.acb import (
    _HEADERS, _TIMEOUT, _BASE_URL,
    _build_col_map, _is_game_row, _extract_player_boxscore,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

STATS_DIR = Path(__file__).resolve().parent.parent / "data" / "stats"


# ---------------------------------------------------------------------------
# Helper: fetch game log and return {date_approx: game_url} by scanning all rows
# ---------------------------------------------------------------------------

def _get_game_urls_by_opponent(player_id: str) -> dict[str, str]:
    """
    Returns {opponent_text: game_detail_url} for ALL games in the player's log.
    """
    url = f"{_BASE_URL}/{player_id}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT, allow_redirects=False)
        if resp.status_code in (301, 302):
            logger.warning("ACB id=%s: redirected (blocked)", player_id)
            return {}
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ACB id=%s: %s", player_id, exc)
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    target_table = None
    col_map: dict[str, int] = {}
    for tbl in soup.find_all("table"):
        for row in tbl.find_all("tr")[:5]:
            cells = row.find_all(["th", "td"])
            candidate = _build_col_map(cells)
            if len(candidate) > len(col_map):
                col_map = candidate
                target_table = tbl

    if target_table is None:
        return {}

    opp_idx = col_map.get("opponent")
    result: dict[str, str] = {}
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not _is_game_row(cells):
            continue
        opp = cells[opp_idx].get_text(strip=True) if opp_idx and opp_idx < len(cells) else ""
        link = None
        for cell in cells:
            a = cell.find("a", href=re.compile(r"/partido/ver/id/\d+"))
            if a:
                link = "https://www.acb.com" + a["href"]
                break
        if opp and link:
            result[opp] = link
    return result


def _fetch_detail(game_url: str, player_name: str) -> dict:
    time.sleep(0.5)
    try:
        resp = requests.get(game_url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        return _extract_player_boxscore(resp.text, player_name)
    except requests.RequestException as exc:
        logger.warning("Detail fetch failed %s: %s", game_url, exc)
        return {}


# ---------------------------------------------------------------------------
# Fix 1: fouls=None records — re-fetch game detail and apply fouls
# ---------------------------------------------------------------------------

def fix_fouls_none(all_files: dict[str, list[dict]]) -> int:
    """
    For ACB records where fouls=None, fetch the game detail page and patch fouls.
    Also patches any other still-null fields (blk, blk_against, fouls_received).
    """
    # Group: player_id → {player_name, set of opponents needing fix}
    needs: dict[str, dict] = {}
    for records in all_files.values():
        for rec in records:
            if rec.get("source") != "acb" or not rec.get("game_date"):
                continue
            if all(rec.get(f) is not None for f in ("blk", "blk_against", "fouls", "fouls_received")):
                continue
            pid = rec.get("player_id", "")
            name = rec.get("player_name", pid)
            opp = rec.get("opponent", "")
            if pid not in needs:
                needs[pid] = {"name": name, "opponents": set()}
            needs[pid]["opponents"].add(opp)

    if not needs:
        logger.info("fix_fouls_none: nothing to fix")
        return 0

    # Fetch game log maps
    game_maps: dict[str, dict[str, str]] = {}
    for pid in needs:
        logger.info("Fetching game log for ACB id=%s (%s)", pid, needs[pid]["name"])
        game_maps[pid] = _get_game_urls_by_opponent(pid)
        time.sleep(1.0)

    # Fetch detail pages
    detail_cache: dict[tuple, dict] = {}
    for pid, info in needs.items():
        gmap = game_maps.get(pid, {})
        for opp in info["opponents"]:
            url = gmap.get(opp)
            if not url:
                logger.warning("No game URL for id=%s opponent=%r", pid, opp)
                continue
            logger.info("Fetching detail: %s", url)
            detail_cache[(pid, opp)] = _fetch_detail(url, info["name"])

    # Apply patches
    updated = 0
    for records in all_files.values():
        for rec in records:
            if rec.get("source") != "acb" or not rec.get("game_date"):
                continue
            key = (rec.get("player_id", ""), rec.get("opponent", ""))
            patch = detail_cache.get(key, {})
            for fld in ("blk", "blk_against", "fouls", "fouls_received"):
                if rec.get(fld) is None and patch.get(fld) is not None:
                    rec[fld] = patch[fld]
                    updated += 1

    logger.info("fix_fouls_none: patched %d field(s)", updated)
    return updated


# ---------------------------------------------------------------------------
# Fix 2: encoding-mismatched records — copy from sibling records
# ---------------------------------------------------------------------------

def fix_encoding_mismatches(all_files: dict[str, list[dict]]) -> int:
    """
    Build a lookup of known-good values from records that have all fields populated,
    keyed by (player_name_canonical, source, game_date).
    Then patch records with None fields using those known-good values.
    """
    import unicodedata

    def _canon(name: str) -> str:
        return (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode()
            .lower()
            .strip()
        )

    # Build good-values cache
    good: dict[tuple, dict] = {}
    fields = ("blk", "blk_against", "fouls", "fouls_received")
    for records in all_files.values():
        for rec in records:
            if rec.get("source") not in ("acb", "euroleague", "eurocup"):
                continue
            if not rec.get("game_date"):
                continue
            if any(rec.get(f) is None for f in fields):
                continue
            key = (_canon(rec.get("player_name", "")), rec.get("source"), rec.get("game_date"))
            good[key] = {f: rec[f] for f in fields}

    # Patch bad records
    updated = 0
    for records in all_files.values():
        for rec in records:
            src = rec.get("source", "")
            if src not in ("acb", "euroleague", "eurocup"):
                continue
            if not rec.get("game_date"):
                continue
            if all(rec.get(f) is not None for f in fields):
                continue
            key = (_canon(rec.get("player_name", "")), src, rec.get("game_date"))
            patch = good.get(key, {})
            for fld in fields:
                if rec.get(fld) is None and patch.get(fld) is not None:
                    rec[fld] = patch[fld]
                    updated += 1

    logger.info("fix_encoding_mismatches: patched %d field(s)", updated)
    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Loading stats files...")
    all_files = {
        str(p): json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(STATS_DIR.glob("*.json"))
    }

    total = fix_fouls_none(all_files)
    total += fix_encoding_mismatches(all_files)

    logger.info("Total fields patched: %d", total)
    if total > 0:
        for fpath, records in all_files.items():
            Path(fpath).write_text(
                json.dumps(records, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        logger.info("Saved all files.")
    else:
        logger.info("Nothing changed.")


if __name__ == "__main__":
    main()
