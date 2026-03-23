#!/usr/bin/env python3
"""
Backfill all ACB season games for a single player.

Usage (from repo root):
    python scripts/backfill_player_acb.py <player_id> "<Canonical Name>" "<Team>"

Example:
    python scripts/backfill_player_acb.py 30000049 "Melvin Ejim" "Hiopos Lleida"
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.sources.acb import (
    _HEADERS, _TIMEOUT, _BASE_URL,
    _build_col_map, _is_game_row, _parse_game_row,
    _extract_player_boxscore, _extract_result_from_game_page,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

STATS_DIR = Path(__file__).resolve().parent.parent / "data" / "stats"


def _fetch_all_acb_games(player_id: str, player_name: str, team: str) -> list[dict]:
    """Fetch every game from the ACB game log and return full stat dicts."""
    url = f"{_BASE_URL}/{player_id}"
    logger.info("Fetching game log: %s", url)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT, allow_redirects=False)
        if resp.status_code in (301, 302):
            logger.warning("ACB id=%s redirected (blocked)", player_id)
            return []
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("ACB request failed: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Find the game log table
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
        logger.error("No game log table found for id=%s", player_id)
        return []

    # Collect all played game rows
    all_rows = []
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not _is_game_row(cells):
            continue
        parsed = _parse_game_row(cells, col_map)
        if parsed.get("min") is None and parsed.get("pts") is None:
            continue  # skip rows with no stats

        # Find game detail link in this row
        game_url = None
        for cell in cells:
            a = cell.find("a", href=re.compile(r"/partido/ver/id/\d+"))
            if a:
                game_url = "https://www.acb.com" + a["href"]
                break

        all_rows.append((parsed, game_url))

    logger.info("Found %d game rows", len(all_rows))

    records = []
    for i, (stats, game_url) in enumerate(all_rows, 1):
        game_date = ""
        game_result = stats.get("result", "")
        detail_stats: dict = {}

        if game_url:
            logger.info("  [%d/%d] Fetching detail: %s", i, len(all_rows), game_url)
            time.sleep(0.5)
            try:
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
                    game_date = f"{year}-{int(month):02d}-{int(day):02d}"
                    break

                # Result
                extracted = _extract_result_from_game_page(page_text, stats.get("opponent", ""))
                if extracted:
                    if game_result in ("V", "D") and not re.search(r"\d", extracted):
                        game_result = f"{game_result} {extracted}"
                    else:
                        game_result = extracted

                detail_stats = _extract_player_boxscore(page_text, player_name)

            except requests.RequestException as exc:
                logger.warning("  Detail fetch failed: %s", exc)

        if not game_date:
            logger.warning("  No date found for game %d, skipping", i)
            continue

        record = {
            "player_id":   player_id,
            "player_name": player_name,
            "team":        team,
            "source":      "acb",
            "competition": "ACB",
            "season":      "2025-26",
            "game_date":   game_date,
            "opponent":    stats.get("opponent", ""),
            "result":      game_result,
            "date":        str(date.today()),
            "min":         stats.get("min"),
            "pts":         stats.get("pts"),
            "t2m":         stats.get("t2m"),   "t2a":  stats.get("t2a"),  "t2_pct": stats.get("t2_pct"),
            "t3m":         stats.get("t3m"),   "t3a":  stats.get("t3a"),  "t3_pct": stats.get("t3_pct"),
            "ftm":         stats.get("ftm"),   "fta":  stats.get("fta"),  "ft_pct": stats.get("ft_pct"),
            "reb_off":     stats.get("reb_off"),
            "reb_def":     stats.get("reb_def"),
            "reb":         stats.get("reb"),
            "ast":         stats.get("ast"),
            "stl":         stats.get("stl"),
            "tov":         stats.get("tov"),
            "blk":         detail_stats.get("blk", stats.get("blk")),
            "blk_against": detail_stats.get("blk_against"),
            "fouls":       detail_stats.get("fouls", stats.get("fouls")),
            "fouls_received": detail_stats.get("fouls_received"),
            "plus_minus":  stats.get("plus_minus"),
            "val":         stats.get("val"),
        }
        logger.info("  → %s  %s  pts=%s", game_date, stats.get("opponent", ""), record["pts"])
        records.append(record)

    return records


def main() -> None:
    if len(sys.argv) < 4:
        print("Usage: python scripts/backfill_player_acb.py <player_id> '<Name>' '<Team>'")
        sys.exit(1)

    player_id   = sys.argv[1]
    player_name = sys.argv[2]
    team        = sys.argv[3]

    games = _fetch_all_acb_games(player_id, player_name, team)
    logger.info("Fetched %d games total", len(games))

    if not games:
        logger.info("Nothing to write.")
        return

    # Load existing stats files
    all_files: dict[str, list[dict]] = {}
    for p in sorted(STATS_DIR.glob("*.json")):
        all_files[str(p)] = json.loads(p.read_text(encoding="utf-8"))

    # De-dup: skip (player_id, game_date) combos already present
    existing: set[tuple] = set()
    for records in all_files.values():
        for r in records:
            if r.get("player_id") == player_id and r.get("game_date"):
                existing.add(r["game_date"])

    added = 0
    for game in games:
        gdate = game["game_date"]
        if gdate in existing:
            logger.debug("Skip %s (already exists)", gdate)
            continue
        fpath = str(STATS_DIR / f"{gdate}.json")
        if fpath not in all_files:
            all_files[fpath] = []
        all_files[fpath].append(game)
        existing.add(gdate)
        added += 1

    logger.info("Adding %d new records", added)
    if added > 0:
        for fpath, records in all_files.items():
            Path(fpath).write_text(
                json.dumps(records, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        logger.info("Saved all files.")
    else:
        logger.info("Nothing new to write.")


if __name__ == "__main__":
    main()
