"""
Backfill V/D prefix for historical records where result is stored as raw score.

Sources handled:
  - ncaa_espn : ESPN uses 'W N-N' / 'L N-N'; convert W→V, L→D
  - eurobasket : raw 'A-B' score where A = player's team, B = opponent; derive V/D
  - acb        : raw 'A-B' score (local-visitor); needs game-page re-fetch per player

Usage:
    python backfill_vd_results.py               # fix ncaa_espn + eurobasket
    python backfill_vd_results.py --acb         # also re-scrape ACB game pages
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backfill_vd")

STATS_DIR = Path(__file__).parent / "data" / "stats"
REGISTRY  = Path(__file__).parent / "data" / "players" / "registry.json"

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

_ACB_BASE = "https://www.acb.com"
_TIMEOUT  = 15
_SLEEP    = 0.5

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer":         "https://www.acb.com/",
}


# ---------------------------------------------------------------------------
# Load / save helpers
# ---------------------------------------------------------------------------

def _load_files() -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for f in sorted(STATS_DIR.glob("*.json")):
        result[f.name] = json.loads(f.read_text(encoding="utf-8"))
    return result


def _save_file(fname: str, records: list[dict]) -> None:
    path = STATS_DIR / fname
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    csv_path = path.with_suffix(".csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)


# ---------------------------------------------------------------------------
# ncaa_espn: W → V, L → D
# ---------------------------------------------------------------------------

def _fix_ncaa(records: list[dict]) -> int:
    fixed = 0
    for r in records:
        if r.get("source") != "ncaa_espn":
            continue
        result = r.get("result", "")
        if not result or result.startswith("V ") or result.startswith("D "):
            continue
        new = re.sub(r"^W\b", "V", re.sub(r"^L\b", "D", result))
        if new != result:
            r["result"] = new
            fixed += 1
    return fixed


# ---------------------------------------------------------------------------
# eurobasket: derive V/D from first vs second number (first = player's team)
# ---------------------------------------------------------------------------

def _fix_eurobasket(records: list[dict]) -> int:
    fixed = 0
    for r in records:
        if r.get("source") != "eurobasket":
            continue
        result = r.get("result", "")
        if not result or result.startswith("V ") or result.startswith("D "):
            continue
        m = re.match(r"^(\d{2,3})-(\d{2,3})$", result.strip())
        if not m:
            continue
        a, b = int(m.group(1)), int(m.group(2))
        wl = "V" if a > b else ("D" if b > a else "")
        if wl:
            r["result"] = f"{wl} {result}"
            fixed += 1
    return fixed


# ---------------------------------------------------------------------------
# ACB: re-fetch game pages to determine V/D
# ---------------------------------------------------------------------------

_ES_MONTHS = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


def _parse_acb_date(text: str) -> str:
    """Parse ACB page title date like '5 oct 2025' → '2025-10-05'."""
    m = re.search(r"(\d{1,2})\s+([a-záéíóú]{3})\s+(20\d{2})", text, re.I)
    if not m:
        return ""
    day  = int(m.group(1))
    mon  = _ES_MONTHS.get(m.group(2).lower()[:3], 0)
    year = int(m.group(3))
    if not mon:
        return ""
    return f"{year}-{mon:02d}-{day:02d}"


def _fix_acb(all_files: dict[str, list[dict]]) -> int:
    """Re-fetch ACB game log + game pages to add V/D prefix to historical records.

    ACB game log now shows V/D in the Res. column (not the full score), and links
    to live.acb.com pages. We use the game ID in those URLs to build the old
    acb.com/partido/ver/id/{id} URL for date + score extraction.
    """
    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    acb_players = [
        (p["name"], s["id"])
        for p in registry
        for s in p.get("sources", [])
        if s.get("type") == "acb" and s.get("id") != "TBD"
    ]

    # Build index: (nfc_player_name, game_date) → list of (fname, record_idx)
    needs_vd: dict[tuple[str, str], list[tuple[str, int]]] = {}
    for fname, records in all_files.items():
        for idx, r in enumerate(records):
            if r.get("source") != "acb":
                continue
            result = r.get("result", "")
            if not result or result.startswith("V ") or result.startswith("D "):
                continue
            key = (unicodedata.normalize("NFC", r.get("player_name", "")), r.get("game_date", ""))
            if key not in needs_vd:
                needs_vd[key] = []
            needs_vd[key].append((fname, idx))

    if not needs_vd:
        logger.info("ACB: no records need V/D fix")
        return 0

    logger.info("ACB: %d (player, game_date) pairs need V/D", len(needs_vd))

    total = 0
    for player_name, player_id in acb_players:
        nfc_name = unicodedata.normalize("NFC", player_name)

        # Check if this player has any records needing update
        player_keys = [k for k in needs_vd if k[0] == nfc_name]
        if not player_keys:
            continue

        time.sleep(_SLEEP)
        logger.info("ACB: fetching game log for %s (id=%s)", player_name, player_id)
        try:
            url = f"{_ACB_BASE}/jugador/todos-sus-partidos/id/{player_id}"
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("ACB: failed to fetch %s: %s", player_name, exc)
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue
                if not re.match(r"^\d+$", cells[0].get_text(strip=True)):
                    continue

                # V/D from Res. column (column 2)
                res_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                wl = res_cell.upper() if res_cell.upper() in ("V", "D") else ""
                if not wl:
                    continue

                # Game ID from live.acb.com link
                link = row.find("a", href=re.compile(r"live\.acb\.com"))
                if not link:
                    continue
                m_id = re.search(r"-(\d+)/", link["href"])
                if not m_id:
                    continue
                game_id = m_id.group(1)
                game_url = f"{_ACB_BASE}/partido/ver/id/{game_id}"

                time.sleep(0.3)
                try:
                    gr = requests.get(game_url, headers=_HEADERS, timeout=_TIMEOUT)
                    gr.raise_for_status()
                    page_text = gr.text
                except requests.RequestException:
                    continue

                game_date = _parse_acb_date(page_text)
                if not game_date:
                    continue

                key = (nfc_name, game_date)
                if key not in needs_vd:
                    continue

                # Get existing score from the historical record and prepend V/D
                locs = needs_vd[key]
                fname0, idx0 = locs[0]
                existing_result = all_files[fname0][idx0].get("result", "")
                score_m = re.search(r"\d{2,3}-\d{2,3}", existing_result)
                if not score_m:
                    logger.warning("ACB: no score in existing result for %s %s: %r",
                                   player_name, game_date, existing_result)
                    continue

                new_result = f"{wl} {score_m.group()}"
                for fname, idx in locs:
                    all_files[fname][idx]["result"] = new_result
                    total += 1
                logger.info("ACB: %s %s → %s", player_name, game_date, new_result)
                del needs_vd[key]

    if needs_vd:
        logger.warning("ACB: %d game-date pairs could not be resolved:", len(needs_vd))
        for k in list(needs_vd)[:10]:
            logger.warning("  %s", k)

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(do_acb: bool) -> None:
    all_files = _load_files()
    total_ncaa = 0
    total_eb   = 0
    total_acb  = 0

    for fname, records in all_files.items():
        changed_ncaa = _fix_ncaa(records)
        changed_eb   = _fix_eurobasket(records)
        total_ncaa  += changed_ncaa
        total_eb    += changed_eb
        if changed_ncaa or changed_eb:
            _save_file(fname, records)
            logger.info("Updated %s (ncaa=%d eurobasket=%d)", fname, changed_ncaa, changed_eb)

    logger.info("ncaa_espn fixed: %d  eurobasket fixed: %d", total_ncaa, total_eb)

    if do_acb:
        total_acb = _fix_acb(all_files)
        # Write back changed ACB files
        for fname, records in all_files.items():
            if any(r.get("source") == "acb" and (r.get("result","").startswith("V ") or r.get("result","").startswith("D ")) for r in records):
                _save_file(fname, records)
        logger.info("acb fixed: %d", total_acb)

    logger.info("Done — total updated: %d", total_ncaa + total_eb + total_acb)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--acb", action="store_true", help="Also re-scrape ACB game pages")
    args = parser.parse_args()
    main(do_acb=args.acb)
