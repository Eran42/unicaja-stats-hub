"""
One-off backfill: fetch all 2025-26 ACB season games for Xavier Castañeda
(ACB id 30003967) and merge into data/stats/{game_date}.json files.

He played J1-J8 with Unicaja, then on loan at MoraBanc Andorra from J13.
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
from bs4 import BeautifulSoup, Tag

# Reuse ACB scraper helpers
sys.path.insert(0, str(Path(__file__).parent))
from src.sources.acb import (
    _HEADERS,
    _TIMEOUT,
    _build_col_map,
    _extract_player_boxscore,
    _extract_result_from_game_page,
    _is_game_row,
    _parse_game_row,
    _safe_float,
    _strip_matchup,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PLAYER_ID   = "30003967"
PLAYER_NAME = "Xavier Castañeda"
STATS_DIR   = Path("data/stats")

# Jornadas 1-8 Unicaja; 13+ MoraBanc Andorra
UNICAJA_JORNADAS = set(range(1, 9))

_ES_MONTHS = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


def _jornada_num(cells: list[Tag]) -> int | None:
    """Extract jornada number from first cell (e.g. '1', '13')."""
    text = cells[0].get_text(strip=True)
    m = re.match(r"^(\d+)", text)
    return int(m.group(1)) if m else None


def _fetch_game_page(game_url: str) -> tuple[str, str, dict]:
    """Fetch a game page and return (game_date, result_str, detail_stats)."""
    game_date = ""
    result_str = ""
    detail_stats: dict = {}

    try:
        time.sleep(0.6)
        gr = requests.get(game_url, headers=_HEADERS, timeout=_TIMEOUT)
        gr.raise_for_status()
        page_text = gr.text

        # Date
        dm = re.search(r"(\d{1,2})\s+([a-záéíóú]{3})\s+(20\d{2})", page_text, re.I)
        if dm:
            mon = _ES_MONTHS.get(dm.group(2).lower()[:3], 0)
            if mon:
                game_date = f"{dm.group(3)}-{mon:02d}-{int(dm.group(1)):02d}"
        if not game_date:
            for dm2 in re.finditer(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", page_text):
                d, mo, yr = dm2.group(1), dm2.group(2), dm2.group(3)
                if 2020 <= int(yr) <= 2035 and 1 <= int(mo) <= 12 and 1 <= int(d) <= 31:
                    game_date = f"{yr}-{int(mo):02d}-{int(d):02d}"
                    break

        # Result
        result_str = _extract_result_from_game_page(page_text, "")

        # Blocks/fouls from /estadisticas page
        stats_url = re.sub(r"/resumen$", "/estadisticas", gr.url)
        if stats_url != gr.url:
            try:
                time.sleep(0.3)
                sr = requests.get(stats_url, headers=_HEADERS, timeout=_TIMEOUT)
                sr.raise_for_status()
                detail_stats = _extract_player_boxscore(sr.text, PLAYER_NAME)
            except requests.RequestException:
                detail_stats = {}
        if not any(v is not None for v in detail_stats.values()):
            detail_stats = _extract_player_boxscore(page_text, PLAYER_NAME)

    except requests.RequestException as exc:
        logger.warning("Game page error %s: %s", game_url, exc)

    return game_date, result_str, detail_stats


def fetch_all_games() -> list[dict]:
    url = f"https://www.acb.com/jugador/todos-sus-partidos/id/{PLAYER_ID}"
    logger.info("Fetching ACB game log: %s", url)
    resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Find best table
    all_tables = soup.find_all("table")
    target_table: Tag | None = None
    col_map: dict[str, int] = {}
    best_count = -1
    for tbl in all_tables:
        tbl_rows = tbl.find_all("tr")
        cmap_candidate: dict[str, int] = {}
        for row in tbl_rows[:5]:
            cells = row.find_all(["th", "td"])
            c = _build_col_map(cells)
            if len(c) > len(cmap_candidate):
                cmap_candidate = c
        game_row_count = sum(
            1 for row in tbl_rows
            for cells in [row.find_all(["td", "th"])]
            if _is_game_row(cells)
        )
        if len(cmap_candidate) > len(col_map) or (
            len(cmap_candidate) == len(col_map) and game_row_count > best_count
        ):
            col_map = cmap_candidate
            target_table = tbl
            best_count = game_row_count

    if not target_table or len(col_map) < 3:
        logger.error("No usable table found")
        return []

    logger.info("col_map: %s", list(col_map.keys()))

    records = []
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not _is_game_row(cells):
            continue

        stats = _parse_game_row(cells, col_map)
        # Skip rows with no stats (DNP or future games)
        if stats.get("min") is None and stats.get("pts") is None:
            logger.info("Skipping row (no stats): %s", cells[0].get_text(strip=True))
            continue

        jornada = _jornada_num(cells)
        team = "Unicaja" if (jornada is not None and jornada in UNICAJA_JORNADAS) else "MoraBanc Andorra"

        # Determine opponent from matchup string
        matchup = stats.get("opponent", "")
        # For the matchup, try to figure out which side Castañeda was on
        # If team "Unicaja" or "MoraBanc" appears, strip it out
        team_abbr = "Unicaja" if team == "Unicaja" else "MoraBanc And"
        opponent = _strip_matchup(matchup, team_abbr)
        if opponent == matchup:
            # _strip_matchup didn't find the team abbr — try partial match
            if "-" in matchup:
                left, _, right = matchup.partition("-")
                if team_abbr.lower() in left.lower():
                    opponent = right.strip()
                elif team_abbr.lower() in right.lower():
                    opponent = left.strip()

        # Find game URL for date, result, blk/fouls
        game_url = ""
        for cell in cells:
            live_link = cell.find("a", href=re.compile(r"live\.acb\.com"))
            old_link  = cell.find("a", href=re.compile(r"/partido/ver/id/\d+"))
            if live_link:
                m_id = re.search(r"-(\d+)/", live_link["href"])
                game_url = f"https://www.acb.com/partido/ver/id/{m_id.group(1)}" if m_id else ""
            elif old_link:
                game_url = "https://www.acb.com" + old_link["href"]
            if game_url:
                break

        game_date, result_str, detail_stats = ("", "", {})
        if game_url:
            game_date, result_str, detail_stats = _fetch_game_page(game_url)

        # Merge result
        game_result = stats.get("result", "").strip()
        if result_str:
            score_m = re.search(r"\d{2,3}-\d{2,3}", result_str)
            if game_result in ("V", "D") and score_m:
                game_result = f"{game_result} {score_m.group()}"
            elif result_str.startswith(("V ", "D ")):
                game_result = result_str
            elif game_result in ("V", "D") and result_str.strip():
                game_result = f"{game_result} {result_str.strip()}"
            else:
                game_result = result_str

        def _z(v):
            return v if v is not None else 0.0

        blk           = detail_stats.get("blk")
        blk_against   = detail_stats.get("blk_against")
        fouls         = detail_stats.get("fouls")
        fouls_received = detail_stats.get("fouls_received")
        if blk is None:
            blk = _z(stats.get("blk"))

        record = {
            "player_id":      PLAYER_ID,
            "player_name":    PLAYER_NAME,
            "team":           team,
            "source":         "acb",
            "competition":    "ACB",
            "season":         "2025-26",
            "game_date":      game_date,
            "opponent":       opponent,
            "result":         game_result,
            "date":           str(date.today()),
            "min":            stats["min"],
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
            "fouls":          fouls if fouls is not None else stats.get("fouls"),
            "fouls_received": fouls_received,
            "plus_minus":     _z(stats["plus_minus"]),
            "val":            _z(stats["val"]),
        }
        logger.info("J%s (%s) %s vs %s  %s pts  date=%s", jornada, team, game_date, opponent, record["pts"], game_date)
        records.append(record)

    return records


def merge_into_stats(records: list[dict]) -> None:
    STATS_DIR.mkdir(parents=True, exist_ok=True)
    by_date: dict[str, list[dict]] = {}
    for r in records:
        gd = r.get("game_date", "")
        if not gd:
            logger.warning("Skipping record with no game_date: %s", r)
            continue
        by_date.setdefault(gd, []).append(r)

    for gd, new_rows in sorted(by_date.items()):
        json_path = STATS_DIR / f"{gd}.json"
        existing: list[dict] = []
        if json_path.exists():
            try:
                existing = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        # De-duplicate: remove any existing Castañeda ACB records for this date
        existing = [
            r for r in existing
            if not (r.get("player_name") == PLAYER_NAME and r.get("source") == "acb")
        ]
        merged = existing + new_rows
        json_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Wrote %d record(s) to %s (total %d)", len(new_rows), json_path, len(merged))


if __name__ == "__main__":
    records = fetch_all_games()
    logger.info("Fetched %d game records", len(records))
    if records:
        merge_into_stats(records)
        logger.info("Done.")
    else:
        logger.error("No records fetched — nothing written.")
