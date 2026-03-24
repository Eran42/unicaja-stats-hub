"""
Backfill historical game data for national-league sources that were previously
broken (eurobasket.com and ABA scrapers).

For each player/source pair listed below, fetches the full 2025-26 season game
log and distributes each game into the matching data/stats/{YYYY-MM-DD}.json
(and .csv) file, honouring the canonical de-duplication key
(player_name, source, competition, game_date).

Usage:
    python backfill_national_leagues.py
"""

from __future__ import annotations

import csv
import json
import logging
import os
import unicodedata
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backfill")

# ---------------------------------------------------------------------------
# Config — (player_name_in_registry, scraper_type, player_id, competition)
# ---------------------------------------------------------------------------
PLAYERS: list[tuple[str, str, str, str]] = [
    # eurobasket sources
    ("Mindaugas Kuzminskas", "eurobasket", "26892",  "Greek League"),
    ("Nemanja Nedović",      "eurobasket", "130801", "LNB Pro A"),
    ("Mathias Lessort",      "eurobasket", "252481", "Greek League"),   # may be empty
    ("Tyson Carter",         "eurobasket", "402220", "ABA League"),
    # ABA direct sources
    ("Axel Bouteille",        "aba", "5073", "ABA League"),
    ("Dylan Osetkowski",      "aba", "5100", "ABA League"),
    ("Dragan Milosavljević",  "aba", "1076", "ABA League"),
]

STATS_DIR = Path(__file__).parent / "data" / "stats"

FIELDS = [
    "player_id", "player_name", "team", "source", "competition", "season",
    "game_date", "opponent", "result", "date",
    "min", "pts",
    "t2m", "t2a", "t2_pct",
    "t3m", "t3a", "t3_pct",
    "ftm", "fta", "ft_pct",
    "reb_off", "reb_def", "reb",
    "ast", "stl", "tov", "blk", "fouls", "plus_minus", "val",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dedup_key(r: dict) -> tuple:
    return (
        unicodedata.normalize("NFC", r.get("player_name", "")),
        r.get("source", ""),
        r.get("competition", ""),
        r.get("game_date", ""),
    )


def _norm_name(r: dict) -> dict:
    r2 = dict(r)
    r2["player_name"] = unicodedata.normalize("NFC", r2.get("player_name", ""))
    return r2


def load_day(day: str) -> tuple[list[dict], set]:
    """Return (records, seen_keys) for a given YYYY-MM-DD."""
    path = STATS_DIR / f"{day}.json"
    if not path.exists():
        return [], set()
    with open(path, encoding="utf-8") as f:
        records = [_norm_name(r) for r in json.load(f)]
    seen = {_dedup_key(r) for r in records}
    return records, seen


def save_day(day: str, records: list[dict]) -> None:
    path = STATS_DIR / f"{day}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    csv_path = STATS_DIR / f"{day}.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Group all new records by game_date so we only open each file once.
    new_records: dict[str, list[dict]] = {}  # game_date -> list of new records

    for player_name, scraper_type, player_id, competition in PLAYERS:
        logger.info("Fetching %s | %s | id=%s", player_name, competition, player_id)

        if scraper_type == "eurobasket":
            from src.sources.eurobasket import fetch_season_stats
            records = fetch_season_stats(player_id, player_name=player_name, competition=competition)
        elif scraper_type == "aba":
            from src.sources.aba import fetch_season_stats  # type: ignore[assignment]
            records = fetch_season_stats(player_id, player_name=player_name)
        else:
            logger.warning("Unknown scraper type: %s", scraper_type)
            continue

        logger.info("  -> %d game(s) returned", len(records))
        for rec in records:
            rec = _norm_name(rec)
            gd = rec.get("game_date", "")
            if not gd:
                continue
            new_records.setdefault(gd, []).append(rec)

    # Merge into daily files.
    total_added = 0
    for game_date in sorted(new_records):
        existing, seen = load_day(game_date)
        added = 0
        for rec in new_records[game_date]:
            k = _dedup_key(rec)
            if k not in seen:
                existing.append(rec)
                seen.add(k)
                added += 1
        if added:
            save_day(game_date, existing)
            logger.info("  %s: added %d record(s) (%d total)", game_date, added, len(existing))
            total_added += added
        else:
            logger.info("  %s: nothing new", game_date)

    logger.info("Backfill complete — %d new record(s) written across %d day(s).",
                total_added, len([gd for gd in new_records if any(
                    _dedup_key(r) not in set() for r in new_records[gd]
                )]))


if __name__ == "__main__":
    main()
