"""
File-based storage for Unicaja Baloncesto Stats Hub.

Stats are stored under data/stats/ as:
  {date}.json  — primary store (list of stat dicts)
  {date}.csv   — convenience snapshot via pandas

No database is used; all reads/writes are plain file I/O.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent
_STATS_DIR = _PROJECT_ROOT / "data" / "stats"


def _ensure_stats_dir() -> Path:
    """Create the stats directory if it does not exist, then return it."""
    _STATS_DIR.mkdir(parents=True, exist_ok=True)
    return _STATS_DIR


def save_daily_stats(stats: list[dict], date: str) -> Path:
    """
    Write a list of stat dicts to data/stats/{date}.json.

    Args:
        stats: List of normalized stat dicts from router.fetch_all_stats().
        date:  ISO date string, e.g. "2025-01-15".

    Returns:
        The Path of the written file.
    """
    stats_dir = _ensure_stats_dir()
    out_path = stats_dir / f"{date}.json"

    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(stats, fh, ensure_ascii=False, indent=2, default=str)

    logger.info("Saved %d stat record(s) to %s", len(stats), out_path)
    return out_path


def save_csv_snapshot(stats: list[dict], date: str) -> Path:
    """
    Write the same stats to data/stats/{date}.csv using pandas.

    Args:
        stats: List of normalized stat dicts.
        date:  ISO date string, e.g. "2025-01-15".

    Returns:
        The Path of the written file.
    """
    stats_dir = _ensure_stats_dir()
    out_path = stats_dir / f"{date}.csv"

    if not stats:
        logger.warning("No stats to write — creating empty CSV at %s", out_path)
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(stats)

    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info("Saved CSV snapshot (%d rows) to %s", len(df), out_path)
    return out_path


def load_stats(date: str) -> list[dict]:
    """
    Load a previously saved JSON stats file.

    Args:
        date: ISO date string, e.g. "2025-01-15".

    Returns:
        List of stat dicts, or an empty list if the file does not exist.
    """
    stats_dir = _ensure_stats_dir()
    in_path = stats_dir / f"{date}.json"

    if not in_path.exists():
        logger.warning("Stats file not found: %s", in_path)
        return []

    with in_path.open(encoding="utf-8") as fh:
        data: list[dict] = json.load(fh)

    logger.info("Loaded %d stat record(s) from %s", len(data), in_path)
    return data


def get_all_dates() -> list[str]:
    """
    Return a sorted list of all dates for which stats have been saved.

    Scans data/stats/ for *.json files and extracts the date stem
    (e.g. "2025-01-15" from "2025-01-15.json").

    Returns:
        Sorted list of ISO date strings (ascending), e.g. ["2025-01-15", "2025-01-16"].
    """
    stats_dir = _ensure_stats_dir()
    json_files = sorted(stats_dir.glob("*.json"))
    dates = [f.stem for f in json_files if f.stem != ".gitkeep"]
    logger.debug("Found %d date(s) with saved stats.", len(dates))
    return dates
