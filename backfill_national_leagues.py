"""
Backfill historical game data for national-league sources that were previously
broken (eurobasket.com and ABA scrapers).

For each player/source pair listed below, fetches the full 2025-26 season game
log and distributes each game into the matching data/stats/{YYYY-MM-DD}.json
(and .csv) file, honouring the canonical de-duplication key
(player_name, source, competition, game_date).

Existing records are *upserted*: if a matching record already exists with a
missing team or empty result, it is updated in place with the fresh values.

Usage:
    python backfill_national_leagues.py
"""

from __future__ import annotations

import csv
import json
import logging
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

STATS_DIR   = Path(__file__).parent / "data" / "stats"
REGISTRY    = Path(__file__).parent / "data" / "players" / "registry.json"

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

def _load_team_map() -> dict[str, str]:
    """Return {canonical_player_name: team} from the registry."""
    with open(REGISTRY, encoding="utf-8") as f:
        entries = json.load(f)
    return {
        unicodedata.normalize("NFC", e["name"]): e.get("team", "")
        for e in entries
    }


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


def _needs_update(existing: dict, fresh: dict) -> bool:
    """True if the existing record is missing team or result that fresh provides."""
    if not existing.get("team") and fresh.get("team"):
        return True
    if not existing.get("result") and fresh.get("result"):
        return True
    return False


def load_day(day: str) -> tuple[list[dict], dict]:
    """Return (records, key_to_index) for a given YYYY-MM-DD."""
    path = STATS_DIR / f"{day}.json"
    if not path.exists():
        return [], {}
    with open(path, encoding="utf-8") as f:
        records = [_norm_name(r) for r in json.load(f)]
    key_to_idx = {_dedup_key(r): i for i, r in enumerate(records)}
    return records, key_to_idx


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
    team_map = _load_team_map()

    # Group all new records by game_date so we only open each file once.
    new_records: dict[str, list[dict]] = {}  # game_date -> list of new records

    for player_name, scraper_type, player_id, competition in PLAYERS:
        logger.info("Fetching %s | %s | id=%s", player_name, competition, player_id)
        player_team = team_map.get(unicodedata.normalize("NFC", player_name), "")

        if scraper_type == "eurobasket":
            from src.sources.eurobasket import fetch_season_stats
            records = fetch_season_stats(player_id, player_name=player_name, competition=competition)
        elif scraper_type == "aba":
            from src.sources.aba import fetch_season_stats  # type: ignore[assignment]
            records = fetch_season_stats(player_id, player_name=player_name, player_team=player_team)
        else:
            logger.warning("Unknown scraper type: %s", scraper_type)
            continue

        logger.info("  -> %d game(s) returned", len(records))
        for rec in records:
            rec = _norm_name(rec)
            # Inject team from registry (router normally does this via setdefault)
            if player_team and not rec.get("team"):
                rec["team"] = player_team
            gd = rec.get("game_date", "")
            if not gd:
                continue
            new_records.setdefault(gd, []).append(rec)

    # Merge / upsert into daily files.
    total_added   = 0
    total_updated = 0
    for game_date in sorted(new_records):
        existing, key_to_idx = load_day(game_date)
        added   = 0
        updated = 0
        for rec in new_records[game_date]:
            k = _dedup_key(rec)
            if k in key_to_idx:
                # Upsert: update missing team/result in the existing record
                idx = key_to_idx[k]
                if _needs_update(existing[idx], rec):
                    if not existing[idx].get("team") and rec.get("team"):
                        existing[idx]["team"] = rec["team"]
                    if not existing[idx].get("result") and rec.get("result"):
                        existing[idx]["result"] = rec["result"]
                    updated += 1
            else:
                existing.append(rec)
                key_to_idx[k] = len(existing) - 1
                added += 1
        if added or updated:
            save_day(game_date, existing)
            logger.info("  %s: added %d, updated %d (%d total)",
                        game_date, added, updated, len(existing))
            total_added   += added
            total_updated += updated
        else:
            logger.info("  %s: nothing new", game_date)

    logger.info(
        "Backfill complete — %d new record(s) added, %d updated across %d day file(s).",
        total_added, total_updated, len(new_records),
    )


if __name__ == "__main__":
    main()
