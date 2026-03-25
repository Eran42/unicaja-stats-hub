"""
Backfill all 2025-26 season games for newly added players.

New players: Dani Díez, Aleksa Avramović, Jeffrey Brooks, Kyle Wiltjer
Run from repo root:  python backfill_new_players.py
"""
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(name)s - %(message)s",
)
logger = logging.getLogger("backfill")

STATS_DIR = "data/stats"

# De-dup key
def _key(r: dict) -> tuple:
    return (r.get("player_name"), r.get("source"), r.get("competition"), r.get("game_date"))


def load_existing() -> dict[str, list[dict]]:
    """Load all existing stats files keyed by date string."""
    files: dict[str, list[dict]] = {}
    for fn in os.listdir(STATS_DIR):
        if fn.endswith(".json"):
            d = fn[:-5]
            with open(os.path.join(STATS_DIR, fn), encoding="utf-8") as f:
                files[d] = json.load(f)
    return files


def collect_existing_keys(files: dict[str, list[dict]]) -> set[tuple]:
    keys: set[tuple] = set()
    for records in files.values():
        for r in records:
            keys.add(_key(r))
    return keys


def save_files(files: dict[str, list[dict]]) -> None:
    for d, records in files.items():
        path = os.path.join(STATS_DIR, f"{d}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        logger.info("Saved %s (%d records)", path, len(records))


def inject_records(
    new_records: list[dict],
    files: dict[str, list[dict]],
    existing_keys: set[tuple],
) -> int:
    """Merge new_records into files dict. Returns count of new records added."""
    added = 0
    # Group by game_date
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in new_records:
        gd = r.get("game_date") or ""
        if gd and gd >= "2025-09-01":
            by_date[gd].append(r)

    for gd, records in by_date.items():
        for r in records:
            k = _key(r)
            if k not in existing_keys:
                if gd not in files:
                    files[gd] = []
                files[gd].append(r)
                existing_keys.add(k)
                added += 1
    return added


def main() -> None:
    from src.sources import euroleague, eurobasket, aba, acb

    files = load_existing()
    existing_keys = collect_existing_keys(files)
    total_added = 0

    # -----------------------------------------------------------------------
    # Aleksa Avramović — EuroLeague (P003983) + ABA League (3857)
    # -----------------------------------------------------------------------
    logger.info("=== Aleksa Avramović ===")
    avra_name = "Aleksa Avramović"

    logger.info("  EuroLeague backfill…")
    el_records = euroleague.fetch_season_stats("P003983")
    for r in el_records:
        r["player_name"] = avra_name
    n = inject_records(el_records, files, existing_keys)
    logger.info("  EuroLeague: %d new records", n)
    total_added += n

    logger.info("  ABA League backfill…")
    aba_records = aba.fetch_season_stats(3857, player_name="Aleksa Avramovic", player_team="Dubai Basketball")
    for r in aba_records:
        r["player_name"] = avra_name
    n = inject_records(aba_records, files, existing_keys)
    logger.info("  ABA League: %d new records", n)
    total_added += n

    # -----------------------------------------------------------------------
    # Jeffrey Brooks — Lega A via eurobasket (136973) + BCL
    # -----------------------------------------------------------------------
    logger.info("=== Jeffrey Brooks ===")
    brooks_name = "Jeffrey Brooks"

    logger.info("  Lega A backfill…")
    lega_records = eurobasket.fetch_season_stats(136973, player_name="Jeffrey Brooks", competition="Lega A")
    for r in lega_records:
        r["player_name"] = brooks_name
    n = inject_records(lega_records, files, existing_keys)
    logger.info("  Lega A: %d new records", n)
    total_added += n

    # -----------------------------------------------------------------------
    # Kyle Wiltjer — Lega A via eurobasket (201473) + EuroCup (P008052)
    # -----------------------------------------------------------------------
    logger.info("=== Kyle Wiltjer ===")
    wiltjer_name = "Kyle Wiltjer"

    logger.info("  Lega A backfill…")
    lega_w_records = eurobasket.fetch_season_stats(201473, player_name="Kyle Wiltjer", competition="Lega A")
    for r in lega_w_records:
        r["player_name"] = wiltjer_name
    n = inject_records(lega_w_records, files, existing_keys)
    logger.info("  Lega A: %d new records", n)
    total_added += n

    logger.info("  EuroCup backfill…")
    ec_records = euroleague.fetch_season_stats("P008052", competition="U", season="U2025")
    for r in ec_records:
        r["player_name"] = wiltjer_name
    n = inject_records(ec_records, files, existing_keys)
    logger.info("  EuroCup: %d new records", n)
    total_added += n

    # -----------------------------------------------------------------------
    # Dani Díez — ACB (20204156)
    # -----------------------------------------------------------------------
    logger.info("=== Dani Díez ===")
    diez_name = "Dani Díez"

    logger.info("  ACB backfill (this makes many HTTP requests — please wait)…")
    acb_records = acb.fetch_season_stats("20204156")
    for r in acb_records:
        r["player_name"] = diez_name
    n = inject_records(acb_records, files, existing_keys)
    logger.info("  ACB: %d new records", n)
    total_added += n

    # -----------------------------------------------------------------------
    # Save
    # -----------------------------------------------------------------------
    logger.info("Total new records to save: %d", total_added)
    if total_added > 0:
        save_files(files)
        logger.info("Done.")
    else:
        logger.info("Nothing new to save.")


if __name__ == "__main__":
    main()
