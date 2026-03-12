"""
Italian Lega Basket (LBA Serie A) data fetcher.

Legabasket.it is a Next.js application that loads stats dynamically via
internal API calls. We first probe the __NEXT_DATA__ JSON embedded in the
HTML, then try known API endpoint patterns.

Endpoint patterns attempted:
  1. https://www.legabasket.it/statistiche/giocatori/{player_id}
  2. https://www.legabasket.it/_next/data/{build_id}/statistiche/giocatori/{player_id}.json
  3. https://www.legabasket.it/api/statistiche/giocatori/{player_id}

Player IDs come from legabasket.it.

Returns the canonical full-stats dict.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_TIMEOUT = 20
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
    "Referer": "https://www.legabasket.it/",
}

_JSON_HEADERS = {**_HEADERS, "Accept": "application/json, text/plain, */*"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().rstrip("%").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_minutes(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if ":" in text:
        parts = text.split(":")
        try:
            return round(float(parts[0]) + float(parts[1]) / 60, 2)
        except (ValueError, IndexError):
            return None
    return _safe_float(text)


def _build_stat_dict(
    player_id: Any,
    player_name: str,
    d: dict,
) -> dict:
    """Convert a parsed stat dict (field→value) to canonical schema."""

    def _g(key: str) -> float | None:
        return _safe_float(d.get(key))

    return {
        "player_id":    str(player_id),
        "player_name":  player_name,
        "team":         d.get("team", ""),
        "source":       "lega",
        "competition":  "Lega A",
        "season":       "2025-26",
        "date":         str(date.today()),
        "game_date":    "",   # Lega A provides season averages, not per-game data
        "opponent":     "",
        "result":       "",
        "games_played": int(_g("games_played") or 0) or None,
        "pts":          _g("pts"),
        "t2m":          _g("t2m"),
        "t2a":          _g("t2a"),
        "t2_pct":       _g("t2_pct"),
        "t3m":          _g("t3m"),
        "t3a":          _g("t3a"),
        "t3_pct":       _g("t3_pct"),
        "ftm":          _g("ftm"),
        "fta":          _g("fta"),
        "ft_pct":       _g("ft_pct"),
        "reb_off":      _g("reb_off"),
        "reb_def":      _g("reb_def"),
        "reb":          _g("reb"),
        "ast":          _g("ast"),
        "stl":          _g("stl"),
        "tov":          _g("tov"),
        "blk":          _g("blk"),
        "fouls":        _g("fouls"),
        "plus_minus":   _g("plus_minus"),
        "val":          _g("val"),
        "min":          _parse_minutes(_g("min")) if _g("min") else None,
    }


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

# Mapping from Italian/English label variants → canonical field
_FIELD_ALIASES: dict[str, str] = {
    "pj": "games_played", "gp": "games_played", "g": "games_played",
    "min": "min", "minuti": "min",
    "pts": "pts", "punti": "pts", "ppg": "pts",
    "t2c": "t2m", "2pm": "t2m", "2fgm": "t2m",
    "t2i": "t2a", "2pa": "t2a", "2fga": "t2a",
    "%t2": "t2_pct", "2p%": "t2_pct",
    "t3c": "t3m", "3pm": "t3m", "3fgm": "t3m",
    "t3i": "t3a", "3pa": "t3a", "3fga": "t3a",
    "%t3": "t3_pct", "3p%": "t3_pct",
    "tlc": "ftm", "ftm": "ftm",
    "tli": "fta", "fta": "fta",
    "%tl": "ft_pct", "ft%": "ft_pct",
    "ro": "reb_off", "roff": "reb_off",
    "rd": "reb_def", "rdef": "reb_def",
    "rt": "reb", "reb": "reb", "rimbalzi": "reb", "rpg": "reb",
    "as": "ast", "ast": "ast", "assist": "ast", "apg": "ast",
    "br": "stl", "stl": "stl", "rubate": "stl", "spg": "stl",
    "bp": "tov", "to": "tov", "tov": "tov", "perse": "tov",
    "tp": "blk", "blk": "blk", "stoppate": "blk", "bpg": "blk",
    "fp": "fouls", "pf": "fouls", "falli": "fouls",
    "+/-": "plus_minus",
    "val": "val", "valutazione": "val",
}


def _parse_next_data(html: str, player_id: str) -> dict | None:
    """Try to extract player stats from the embedded __NEXT_DATA__ JSON."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except (json.JSONDecodeError, ValueError):
        return None

    # Walk the nested props/pageProps looking for player stats
    page_props = (
        data.get("props", {})
            .get("pageProps", {})
    )
    if not page_props:
        return None

    # Try common keys that might hold stats
    for key in ("player", "giocatore", "stats", "statistiche", "data"):
        obj = page_props.get(key)
        if isinstance(obj, dict) and ("pts" in obj or "punti" in obj or "ppg" in obj):
            return _normalize_json_stats(obj, player_id)

    return None


def _normalize_json_stats(obj: dict, player_id: str) -> dict:
    """Normalize a JSON stats dict using field aliases."""
    d: dict[str, Any] = {}
    for raw_key, value in obj.items():
        canonical = _FIELD_ALIASES.get(raw_key.lower().strip())
        if canonical:
            d[canonical] = value
    player_name = (
        obj.get("name") or obj.get("nome") or
        obj.get("fullName") or obj.get("player_name") or player_id
    )
    return _build_stat_dict(player_id, str(player_name), d)


def _parse_html_table(html: str, player_id: str, player_name: str) -> dict | None:
    """Fall back to HTML table parsing."""
    soup = BeautifulSoup(html, "html.parser")

    # Try to get player name from h1/h2
    name_tag = soup.find("h1") or soup.find("h2")
    if name_tag:
        n = name_tag.get_text(strip=True)
        if n:
            player_name = n

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
        # Build col_map
        col_map: dict[str, int] = {}
        for idx, h in enumerate(headers):
            field = _FIELD_ALIASES.get(h)
            if field and field not in col_map:
                col_map[field] = idx

        if len(col_map) < 4:
            continue

        # Find the average/media row
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            first_text = cells[0].get_text(strip=True).lower()
            if any(kw in first_text for kw in ("media", "average", "avg", "prom")):
                row_vals = [c.get_text(strip=True) for c in cells]
                d: dict[str, Any] = {}
                for field, idx in col_map.items():
                    if idx < len(row_vals):
                        d[field] = row_vals[idx]
                return _build_stat_dict(player_id, player_name, d)

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_player_stats(player_id: str | int) -> dict:
    """
    Fetch season averages for an Italian Lega A player.

    Args:
        player_id:  Numeric legabasket.it player ID.

    Returns:
        Canonical per-game stats dict, or empty dict on failure.
    """
    pid = str(player_id)

    url_candidates = [
        f"https://www.legabasket.it/statistiche/giocatori/{pid}",
        f"https://www.legabasket.it/giocatori/{pid}",
        f"https://www.legabasket.it/player/{pid}",
    ]

    for url in url_candidates:
        logger.debug("Lega fetch: %s", url)
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
        except requests.HTTPError:
            continue
        except requests.RequestException as exc:
            logger.debug("Lega request failed %s: %s", url, exc)
            continue

        html = resp.text

        # Try __NEXT_DATA__ first
        result = _parse_next_data(html, pid)
        if result:
            return result

        # Try HTML table
        result = _parse_html_table(html, pid, pid)
        if result:
            return result

    # Try the Next.js data API — discover build ID from homepage
    build_id = _get_next_build_id()
    if build_id:
        for path in (
            f"/statistiche/giocatori/{pid}",
            f"/giocatori/{pid}",
        ):
            api_url = f"https://www.legabasket.it/_next/data/{build_id}{path}.json"
            logger.debug("Lega Next.js data API: %s", api_url)
            try:
                resp = requests.get(api_url, headers=_JSON_HEADERS, timeout=_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    page_props = data.get("pageProps", {})
                    for key in ("player", "giocatore", "stats", "statistiche", "data"):
                        obj = page_props.get(key)
                        if isinstance(obj, dict):
                            result = _normalize_json_stats(obj, pid)
                            if result:
                                return result
            except Exception as exc:
                logger.debug("Lega Next.js API failed: %s", exc)

    logger.warning("Lega: could not fetch stats for player_id=%s", player_id)
    return {}


def _get_next_build_id() -> str | None:
    """Extract the Next.js build ID from the legabasket.it homepage."""
    try:
        resp = requests.get(
            "https://www.legabasket.it/",
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        m = re.search(r'"buildId"\s*:\s*"([^"]+)"', resp.text)
        return m.group(1) if m else None
    except Exception:
        return None
