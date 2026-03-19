#!/usr/bin/env python3
"""
One-off backfill: fetch all past BCL game-by-game records for tracked players
and merge them into the appropriate data/stats/{date}.json files.

Run from repo root:
    python scripts/backfill_bcl.py
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

from src.sources.bcl import _HEADERS, _TIMEOUT, _TEAM_NAMES, _parse_shot_cell, _parse_minutes, _safe_float

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

STATS_DIR = Path(__file__).resolve().parent.parent / "data" / "stats"

# Players to backfill: (slug, canonical_name, team)
BCL_PLAYERS = [
    ("la-laguna-tenerife/151899-giorgi-shermadini",  "Giorgi Shermadini",    "Lenovo Tenerife"),
    ("la-laguna-tenerife/175433-jaime-fernandez",    "Jaime Fernández",      "Lenovo Tenerife"),
    ("la-laguna-tenerife/201675-tim-abromaitis",     "Tim Abromaitis",       "Lenovo Tenerife"),
    ("aek-bc/161021-mindaugas-kuzminskas",           "Mindaugas Kuzminskas", "AEK BC"),
]


def _fetch_all_games(slug: str, player_name: str, team: str) -> list[dict]:
    """Fetch all game rows from the BCL team player profile page."""
    url = f"https://www.championsleague.basketball/en/teams/{slug}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("BCL fetch failed %s: %s", url, exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("BCL: no table on %s", url)
        return []

    rows = table.find_all("tr")
    if len(rows) < 3:
        return []

    header_row = rows[1].find_all(["th", "td"])
    col_idx: dict[str, int] = {}
    for i, cell in enumerate(header_row):
        col_idx[cell.get_text(strip=True).upper()] = i

    records = []
    for row in rows[2:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        first = cells[0].get_text(strip=True).upper()
        if first in ("CUMULATED", "AVERAGE"):
            break

        # Parse game info cell
        parts = [p.strip() for p in cells[0].get_text(separator="|", strip=True).split("|")
                 if p.strip() and p.strip() != ","]
        opp_abbrev = ""
        game_date = ""
        for part in parts:
            if re.match(r"^\d{2}/\d{2}/\d{4}$", part):
                day, month, year = part.split("/")
                game_date = f"{year}-{month}-{day}"
            elif part not in ("vs",) and not re.search(r"round|phase|group|final|quarter|semi", part, re.IGNORECASE):
                if not opp_abbrev:
                    opp_abbrev = part

        if not game_date:
            continue

        opponent = _TEAM_NAMES.get(opp_abbrev, opp_abbrev)

        def _gcell(label: str) -> str:
            idx = col_idx.get(label)
            return cells[idx].get_text(strip=True) if idx is not None and idx < len(cells) else ""

        def _shot(label: str):
            idx = col_idx.get(label)
            if idx is None or idx >= len(cells):
                return None, None, None
            return _parse_shot_cell(cells[idx])

        t2m, t2a, t2_pct = _shot("2PT FG")
        t3m, t3a, t3_pct = _shot("3PT FG")
        ftm, fta, ft_pct = _shot("FT")

        if t2a == 0.0:  t2_pct = 0.0
        if t3a == 0.0:  t3_pct = 0.0
        if fta == 0.0:  ft_pct = 0.0

        # Skip DNP rows (no minutes logged)
        minutes = _parse_minutes(_gcell("MIN"))
        if minutes is None:
            continue

        records.append({
            "player_id":      slug,
            "player_name":    player_name,
            "team":           team,
            "source":         "bcl",
            "competition":    "BCL",
            "season":         "2025-26",
            "date":           str(date.today()),
            "game_date":      game_date,
            "opponent":       opponent,
            "result":         "",
            "min":            minutes,
            "pts":            _safe_float(_gcell("PTS")),
            "t2m":  t2m,     "t2a":  t2a,   "t2_pct":  t2_pct,
            "t3m":  t3m,     "t3a":  t3a,   "t3_pct":  t3_pct,
            "ftm":  ftm,     "fta":  fta,   "ft_pct":  ft_pct,
            "reb_off":        _safe_float(_gcell("OREB")),
            "reb_def":        _safe_float(_gcell("DREB")),
            "reb":            _safe_float(_gcell("REB")),
            "ast":            _safe_float(_gcell("AST")),
            "stl":            _safe_float(_gcell("STL")),
            "tov":            _safe_float(_gcell("TO")),
            "blk":            _safe_float(_gcell("BLK")),
            "blk_against":    None,
            "fouls":          _safe_float(_gcell("PF")),
            "fouls_received": None,
            "plus_minus":     _safe_float(_gcell("+/-")),
            "val":            _safe_float(_gcell("EFF")),
        })

    return records


def main() -> None:
    # Load all existing stats files
    all_files: dict[str, list[dict]] = {}
    for p in sorted(STATS_DIR.glob("*.json")):
        all_files[str(p)] = json.loads(p.read_text(encoding="utf-8"))

    # De-dup key for existing BCL records: (player_id, game_date)
    existing: set[tuple] = set()
    for records in all_files.values():
        for r in records:
            if r.get("source") == "bcl" and r.get("game_date"):
                existing.add((r.get("player_id", ""), r.get("game_date", "")))

    total_added = 0
    for slug, name, team in BCL_PLAYERS:
        logger.info("Fetching all BCL games for %s (%s)", name, slug)
        games = _fetch_all_games(slug, name, team)
        logger.info("  Found %d game(s)", len(games))

        for game in games:
            gdate = game.get("game_date", "")
            key = (slug, gdate)
            if key in existing:
                logger.debug("  Skip %s %s (already in files)", name, gdate)
                continue

            # Find or create the file for this game_date
            fpath = str(STATS_DIR / f"{gdate}.json")
            if fpath not in all_files:
                all_files[fpath] = []
            all_files[fpath].append(game)
            existing.add(key)
            total_added += 1
            logger.info("  + %s %s vs %s", name, gdate, game.get("opponent", ""))

        time.sleep(1.0)

    logger.info("Total new BCL records: %d", total_added)

    if total_added > 0:
        for fpath, records in all_files.items():
            Path(fpath).write_text(
                json.dumps(records, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        logger.info("Saved all files.")
    else:
        logger.info("Nothing new to add.")


if __name__ == "__main__":
    main()
