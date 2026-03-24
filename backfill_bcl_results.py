"""
Backfill result field for all historical BCL game records.

Fetches each player's BCL profile page, collects all game hrefs, then
fetches each game page for the final score, and updates matching records
in the daily JSON/CSV files.

Usage:
    python backfill_bcl_results.py
"""
from __future__ import annotations

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
logger = logging.getLogger("bcl_backfill")

STATS_DIR = Path(__file__).parent / "data" / "stats"
BCL_BASE  = "https://www.championsleague.basketball"
TIMEOUT   = 20
SLEEP     = 0.5

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         BCL_BASE + "/",
}

# (canonical_player_name, bcl_slug, player_team_abbr)
PLAYERS = [
    ("Mindaugas Kuzminskas", "aek-bc/161021-mindaugas-kuzminskas",             "AEK"),
    ("Giorgi Shermadini",    "la-laguna-tenerife/151899-giorgi-shermadini",     "LLTF"),
    ("Jaime Fernández",      "la-laguna-tenerife/175433-jaime-fernandez",       "LLTF"),
    ("Tim Abromaitis",       "la-laguna-tenerife/201675-tim-abromaitis",        "LLTF"),
]


def _fetch_game_links(slug: str) -> list[tuple[str, str]]:
    """Return [(game_date, game_href), ...] from a BCL player profile page."""
    url = f"{BCL_BASE}/en/teams/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Failed fetching profile %s: %s", url, exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    results = []
    for row in rows[2:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        first = cells[0].get_text(strip=True).upper()
        if first in ("CUMULATED", "AVERAGE"):
            break
        # Parse game cell
        cell = cells[0]
        a_tag = cell.find("a", href=re.compile(r"/en/games/"))
        if not a_tag:
            continue
        game_href = a_tag["href"]
        # Extract date from cell text
        m = re.search(r"(\d{2}/\d{2}/\d{4})", cell.get_text())
        if not m:
            continue
        day, month, year = m.group(1).split("/")
        game_date = f"{year}-{month}-{day}"
        results.append((game_date, game_href))

    return results


def _fetch_result(game_href: str, player_team_abbr: str) -> str:
    """Fetch BCL game page and return 'V score-score' / 'D score-score'."""
    url = BCL_BASE + game_href if not game_href.startswith("http") else game_href
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Failed fetching game %s: %s", url, exc)
        return ""

    # URL: /en/games/ID-TEAMA-TEAMB
    path = game_href.rstrip("/").split("/")[-1]
    parts = path.split("-")
    team_a, team_b = "", ""
    for i, p in enumerate(parts):
        if p.isdigit():
            remaining = parts[i + 1:]
            if len(remaining) >= 2:
                team_a = remaining[0]
                team_b = "-".join(remaining[1:])
            break

    # Find last quarter's cumulative score (Q4, or OT if present).
    # The page embeds JSON as a JS-escaped string, so quotes appear as \" in resp.text.
    text = resp.text.replace('\\"', '"')
    final_a: int | None = None
    final_b: int | None = None
    for m in re.finditer(r'"Q\d+":\{"name":"[^"]+","scoreA":(\d+),"scoreB":(\d+)', text):
        final_a, final_b = int(m.group(1)), int(m.group(2))
    for m in re.finditer(r'"OT\d*":\{"name":"[^"]+","scoreA":(\d+),"scoreB":(\d+)', text):
        final_a, final_b = int(m.group(1)), int(m.group(2))

    if final_a is None or final_b is None:
        return ""

    abbr = player_team_abbr.upper()
    if team_a.upper() == abbr:
        player_sc, opp_sc = final_a, final_b
    elif team_b.upper() == abbr:
        player_sc, opp_sc = final_b, final_a
    else:
        return f"{final_a}-{final_b}"

    wl = "V" if player_sc > opp_sc else "D"
    return f"{wl} {player_sc}-{opp_sc}"


def main() -> None:
    # Load all stats files into memory
    all_files: dict[str, list[dict]] = {}
    for f in sorted(STATS_DIR.glob("*.json")):
        all_files[f.name] = json.loads(f.read_text(encoding="utf-8"))

    total_updated = 0

    for player_name, slug, team_abbr in PLAYERS:
        logger.info("Processing %s (slug=%s, team=%s)", player_name, slug, team_abbr)
        game_links = _fetch_game_links(slug)
        logger.info("  Found %d game links", len(game_links))

        for game_date, game_href in game_links:
            # Only update records that lack a result
            fname = f"{game_date}.json"
            if fname not in all_files:
                continue
            records = all_files[fname]
            targets = [
                r for r in records
                if (unicodedata.normalize("NFC", r.get("player_name", "")) ==
                    unicodedata.normalize("NFC", player_name)
                    and r.get("source") == "bcl"
                    and not r.get("result"))
            ]
            if not targets:
                continue

            time.sleep(SLEEP)
            result = _fetch_result(game_href, team_abbr)
            if not result:
                logger.warning("  No result for %s %s (%s)", player_name, game_date, game_href)
                continue

            for r in targets:
                r["result"] = result
                total_updated += 1
            logger.info("  %s %s → %s", player_name, game_date, result)

    # Write back changed files
    for fname, records in all_files.items():
        if any(r.get("source") == "bcl" and r.get("result") for r in records):
            path = STATS_DIR / fname
            path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
            csv_path = path.with_suffix(".csv")
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
                w.writeheader()
                w.writerows(records)

    logger.info("Done — updated %d BCL record(s).", total_updated)


if __name__ == "__main__":
    main()
