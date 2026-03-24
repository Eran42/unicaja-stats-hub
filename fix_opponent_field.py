"""
Fix the opponent field across all historical stats files.

For ACB and ABA records, the opponent was stored as a full matchup string
like "Barça-Real Madrid" or "Partizan-Budućnost" instead of just the
opposing team. This script extracts just the opponent by finding the
player's own team abbreviation (the token that appears in every game)
and removing it from the matchup.

Usage:
    python fix_opponent_field.py
"""
from __future__ import annotations

import csv
import json
import logging
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fix_opponent")

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

MATCHUP_SOURCES = {"acb", "aba"}


def _looks_like_matchup(opponent: str) -> bool:
    """True if the opponent field looks like 'TeamA-TeamB' rather than a single team."""
    return "-" in opponent and not opponent.startswith("-") and not opponent.endswith("-")


def _strip_matchup(matchup: str, player_team_abbr: str) -> str:
    """Return the opponent half of 'TeamA-TeamB'."""
    if not player_team_abbr or "-" not in matchup:
        return matchup
    left, _, right = matchup.partition("-")
    left, right = left.strip(), right.strip()
    if left == player_team_abbr:
        return right
    if right == player_team_abbr:
        return left
    return matchup  # couldn't match — leave unchanged


def main() -> None:
    # Load all records grouped by (player_name, source).
    # key -> list of (file_path, record_index, opponent_str)
    player_games: dict[tuple, list[str]] = defaultdict(list)

    all_files: list[tuple[Path, list[dict]]] = []
    for f in sorted(STATS_DIR.glob("*.json")):
        records = json.loads(f.read_text(encoding="utf-8"))
        all_files.append((f, records))
        for r in records:
            src = r.get("source", "")
            if src not in MATCHUP_SOURCES:
                continue
            opp = r.get("opponent", "")
            if _looks_like_matchup(opp):
                key = (
                    unicodedata.normalize("NFC", r.get("player_name", "")),
                    src,
                )
                player_games[key].append(opp)

    # For each player+source, find their team abbreviation.
    player_team_abbr: dict[tuple, str] = {}
    for key, matchups in player_games.items():
        tokens: list[str] = []
        for m in matchups:
            left, _, right = m.partition("-")
            tokens.extend([left.strip(), right.strip()])
        if tokens:
            abbr = Counter(tokens).most_common(1)[0][0]
            player_team_abbr[key] = abbr
            logger.info("%-30s %-4s → team abbr: %s", key[0], key[1], abbr)

    # Apply fix to all records in all files.
    total_fixed = 0
    for f, records in all_files:
        changed = False
        for r in records:
            src = r.get("source", "")
            if src not in MATCHUP_SOURCES:
                continue
            opp = r.get("opponent", "")
            if not _looks_like_matchup(opp):
                continue
            key = (
                unicodedata.normalize("NFC", r.get("player_name", "")),
                src,
            )
            abbr = player_team_abbr.get(key, "")
            if not abbr:
                continue
            cleaned = _strip_matchup(opp, abbr)
            if cleaned != opp:
                r["opponent"] = cleaned
                changed = True
                total_fixed += 1

        if changed:
            f.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
            # Rewrite CSV too
            csv_path = f.with_suffix(".csv")
            with open(csv_path, "w", encoding="utf-8", newline="") as cf:
                w = csv.DictWriter(cf, fieldnames=FIELDS, extrasaction="ignore")
                w.writeheader()
                w.writerows(records)
            logger.info("Updated %s", f.name)

    logger.info("Done — fixed opponent in %d record(s).", total_fixed)


if __name__ == "__main__":
    main()
