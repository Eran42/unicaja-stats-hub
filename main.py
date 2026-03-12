"""
Unicaja Baloncesto Stats Hub — main entry point.

Usage:
  python main.py               # run once and exit
  python main.py --schedule    # run once, then repeat daily at --time (default 06:00)
  python main.py --schedule --time 08:30

The script:
  1. Loads active players from data/players/registry.json (seeds it on first run).
  2. Fetches current-season stats for each player via src/router.
  3. Saves results to data/stats/{today}.json and data/stats/{today}.csv.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import schedule
import time as _time

from src.players import get_active_players, seed_registry
from src.router import fetch_all_stats
from src.storage import save_daily_stats, save_csv_snapshot

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Core job
# ---------------------------------------------------------------------------

def run_job() -> None:
    """Fetch stats for all active players and persist results."""
    today = str(date.today())
    logger.info("=== Stats run starting for %s ===", today)

    players = get_active_players()
    if not players:
        logger.warning("No active players found — check registry.json.")
        return

    logger.info("Fetching stats for %d active player(s)…", len(players))
    stats = fetch_all_stats(players)

    if not stats:
        logger.warning("No stats returned — nothing to save.")
        return

    json_path = save_daily_stats(stats, today)
    csv_path  = save_csv_snapshot(stats, today)

    logger.info("Saved %d record(s) → %s", len(stats), json_path)
    logger.info("CSV snapshot          → %s", csv_path)
    logger.info("=== Run complete ===")

    # Print a quick summary table to stdout
    _print_summary(stats)


def _fmt(val: object, decimals: int = 1) -> str:
    """Format a stat value: float → '12.3', None/missing → '—'."""
    if val is None:
        return "—"
    try:
        return f"{float(val):.{decimals}f}"
    except (TypeError, ValueError):
        return str(val)


def _print_summary(stats: list[dict]) -> None:
    """
    Print a full-schema summary table matching the ACB box-score format.

    Columns: Player | Competition | GP | MIN | PTS | T2 | T3 | FT | RO | RD | RT | AST | STL | TOV | BLK | F | +/- | VAL
    """
    NW = 26   # player name width
    CW = 14   # competition width
    SW = 5    # stat column width

    DW = 10  # date column width

    header = (
        f"{'Player':<{NW}} {'Competition':<{CW}} {'Game':>{DW}}"
        f" {'MIN':>{SW}} {'PTS':>{SW}}"
        f" {'T2%':>{SW}} {'T3%':>{SW}} {'FT%':>{SW}}"
        f" {'T2M':>{SW}} {'T2A':>{SW}}"
        f" {'T3M':>{SW}} {'T3A':>{SW}}"
        f" {'FTM':>{SW}} {'FTA':>{SW}}"
        f" {'RO':>{SW}} {'RD':>{SW}} {'RT':>{SW}}"
        f" {'AST':>{SW}} {'STL':>{SW}} {'TOV':>{SW}} {'BLK':>{SW}}"
        f" {'F':>{SW}} {'+/-':>{SW}} {'VAL':>{SW}}"
    )
    sep = "─" * len(header)

    print(f"\n{sep}")
    print(header)
    print(sep)

    for s in sorted(stats, key=lambda x: (x.get("player_name", ""), x.get("competition", ""))):
        name      = (s.get("player_name") or "")[:NW]
        comp      = (s.get("competition") or "")[:CW]
        game_date = (s.get("game_date") or "—")[:DW]

        print(
            f"{name:<{NW}} {comp:<{CW}} {game_date:>{DW}}"
            f" {_fmt(s.get('min')):>{SW}} {_fmt(s.get('pts')):>{SW}}"
            f" {_fmt(s.get('t2_pct')):>{SW}} {_fmt(s.get('t3_pct')):>{SW}} {_fmt(s.get('ft_pct')):>{SW}}"
            f" {_fmt(s.get('t2m')):>{SW}} {_fmt(s.get('t2a')):>{SW}}"
            f" {_fmt(s.get('t3m')):>{SW}} {_fmt(s.get('t3a')):>{SW}}"
            f" {_fmt(s.get('ftm')):>{SW}} {_fmt(s.get('fta')):>{SW}}"
            f" {_fmt(s.get('reb_off')):>{SW}} {_fmt(s.get('reb_def')):>{SW}} {_fmt(s.get('reb')):>{SW}}"
            f" {_fmt(s.get('ast')):>{SW}} {_fmt(s.get('stl')):>{SW}} {_fmt(s.get('tov')):>{SW}} {_fmt(s.get('blk')):>{SW}}"
            f" {_fmt(s.get('fouls')):>{SW}} {_fmt(s.get('plus_minus')):>{SW}} {_fmt(s.get('val')):>{SW}}"
        )

    print(sep + "\n")
    print(f"  {len(stats)} record(s) from {len({s.get('player_name') for s in stats})} player(s)\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unicaja Baloncesto Stats Hub",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Keep running and repeat the job daily at --time.",
    )
    parser.add_argument(
        "--time",
        default="06:00",
        metavar="HH:MM",
        help="Time of day to run the scheduled job (24-hour clock).",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="(Re-)seed registry.json from the built-in player list and exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.seed:
        seed_registry()
        logger.info("Registry seeded. Run without --seed to fetch stats.")
        return

    # Always run once immediately
    run_job()

    if args.schedule:
        logger.info("Scheduling daily run at %s.", args.time)
        schedule.every().day.at(args.time).do(run_job)
        while True:
            schedule.run_pending()
            _time.sleep(30)


if __name__ == "__main__":
    main()
