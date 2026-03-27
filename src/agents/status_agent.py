"""
Status Agent — determines why a player has no stats today.

For each player who returned no data from the pipeline, the agent:
  1. Searches DuckDuckGo for recent injury / absence news.
  2. Uses Claude Haiku to classify the result and write a one-line note.
  3. Writes the result to data/players/status.json (one entry per player).

Entries are refreshed at most once per calendar day to minimise API calls.

Requires ANTHROPIC_API_KEY in the environment.  If the key is absent the
agent still runs but skips the Claude step and stores only the raw snippet.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()  # load ANTHROPIC_API_KEY from .env if present

logger = logging.getLogger(__name__)

_STATUS_FILE = Path("data/players/status.json")
_TIMEOUT = 12
_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Status values — used by the dashboard for styling
STATUS_INJURED    = "injured"
STATUS_REST       = "rest"
STATUS_SUSPENDED  = "suspended"
STATUS_NO_GAME    = "no_game"
STATUS_UNKNOWN    = "unknown"


# ---------------------------------------------------------------------------
# Web search
# ---------------------------------------------------------------------------

def _ddg_search(query: str) -> str:
    """
    Search DuckDuckGo HTML and return a concatenated string of the top-5 snippets.
    Falls back to an empty string on any error.
    """
    try:
        r = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers=_SEARCH_HEADERS,
            timeout=_TIMEOUT,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        snippets: list[str] = []
        for div in soup.select(".result__body")[:6]:
            title   = div.select_one(".result__title")
            snippet = div.select_one(".result__snippet")
            t = title.get_text(strip=True)   if title   else ""
            s = snippet.get_text(strip=True) if snippet else ""
            if t or s:
                snippets.append(f"{t} — {s}" if s else t)
        return "\n".join(snippets)
    except Exception as exc:
        logger.warning("DDG search failed for %r: %s", query, exc)
        return ""


def _search_player_news(player_name: str, team: str) -> str:
    """Two-pass search: injury-specific first, then general."""
    injury_q = f'"{player_name}" basketball injury OR injured OR absent OR DNP OR suspended 2026'
    results  = _ddg_search(injury_q)
    if not results:
        general_q = f'"{player_name}" {team} basketball 2026'
        results   = _ddg_search(general_q)
    return results or "No search results found."


# ---------------------------------------------------------------------------
# Claude analysis
# ---------------------------------------------------------------------------

def _analyze_with_claude(player_name: str, team: str, search_results: str) -> dict:
    """
    Ask Claude Haiku to classify the player's absence and write a note.
    Returns {"status": <str>, "note": <str>}.
    """
    try:
        import anthropic  # optional dep — only needed at call time
    except ImportError:
        logger.warning("anthropic package not installed — skipping Claude analysis")
        return {"status": STATUS_UNKNOWN, "note": search_results[:120] or "No data today."}

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — skipping Claude analysis for %s", player_name)
        return {"status": STATUS_UNKNOWN, "note": "No data today."}

    prompt = (
        f"Basketball player {player_name} (team: {team}) has no stats in today's pipeline run.\n\n"
        f"Web search results:\n{search_results}\n\n"
        "Based on these results, classify why the player has no stats today.\n"
        "Respond ONLY with a JSON object — no markdown, no explanation:\n"
        '{"status": "injured|rest|suspended|no_game|unknown", "note": "One-sentence reason."}\n\n'
        "Guidelines:\n"
        "- injured: player is hurt or ill\n"
        "- rest: scheduled rest / load management\n"
        "- suspended: disciplinary absence\n"
        "- no_game: team had no game today (most common reason)\n"
        "- unknown: can't determine from results\n"
        "Keep the note under 80 characters.  If no game, say 'No game today.'"
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg    = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        m    = re.search(r'\{[^{}]+\}', text, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            return {
                "status": parsed.get("status", STATUS_UNKNOWN),
                "note":   parsed.get("note",   "No data today."),
            }
    except Exception as exc:
        logger.warning("Claude analysis failed for %s: %s", player_name, exc)

    return {"status": STATUS_UNKNOWN, "note": "No data today."}


# ---------------------------------------------------------------------------
# Status file I/O
# ---------------------------------------------------------------------------

def _load_status() -> dict:
    if _STATUS_FILE.exists():
        try:
            return json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_status(status: dict) -> None:
    _STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(players_no_data: list[dict], today: str | None = None) -> None:
    """
    Check absence reasons for every player in *players_no_data* and update
    data/players/status.json.

    Args:
        players_no_data:  list of dicts with at least ``name`` and ``team`` keys.
        today:            ISO date string (default: today).
    """
    today = today or str(date.today())
    status  = _load_status()
    changed = False

    for player in players_no_data:
        name = player.get("name", "")
        team = player.get("team", "")
        if not name:
            continue

        existing = status.get(name, {})

        # Skip if we already ran today for this player
        if existing.get("updated") == today:
            logger.debug("Status for %s already current (%s), skipping.", name, today)
            continue

        logger.info("Status agent: researching %s (%s)...", name, team)
        try:
            news   = _search_player_news(name, team)
            result = _analyze_with_claude(name, team, news)
        except Exception as exc:
            logger.warning("Status agent failed for %s: %s", name, exc)
            result = {"status": STATUS_UNKNOWN, "note": "No data today."}

        status[name] = {
            "status":  result["status"],
            "note":    result["note"],
            "updated": today,
        }
        changed = True
        logger.info(
            "  %s → [%s] %s", name, result["status"], result["note"]
        )
        time.sleep(0.5)   # be polite to DDG

    if changed:
        _save_status(status)
        logger.info("status.json updated for %d player(s).", sum(1 for p in players_no_data
                    if status.get(p.get("name",""), {}).get("updated") == today))
