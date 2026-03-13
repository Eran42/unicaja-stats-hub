"""
Source router for Unicaja Baloncesto Stats Hub.

For each player, iterates over player.sources and calls the appropriate
scraper module. Returns a flat list of stat dicts — one per (player, source).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.players import Player, PlayerSource

logger = logging.getLogger(__name__)

# Lazy imports live inside _fetch_source() so missing optional deps
# don't break the whole run.


def _fetch_source(player: "Player", src: "PlayerSource") -> dict | None:
    """
    Dispatch a single PlayerSource to its scraper module.

    Returns a normalized stat dict on success, None on failure/skip.
    """
    sid  = src.id
    stype = src.type

    if not src.is_ready:
        logger.warning(
            "Skipping %s [%s] — source_id is TBD or type '%s' is unsupported.",
            player.name, src.competition, stype,
        )
        return None

    logger.info(
        "Fetching %s | %-12s | type=%-12s id=%s",
        player.name, src.competition, stype, sid,
    )

    try:
        result: dict | None = None

        if stype == "acb":
            from src.sources import acb
            result = acb.fetch_player_stats(sid)

        elif stype == "euroleague":
            from src.sources import euroleague
            result = euroleague.fetch_player_stats(sid)

        elif stype == "eurocup":
            from src.sources import euroleague
            result = euroleague.fetch_eurocup_player_stats(sid)

        elif stype == "nba":
            from src.sources import nba
            result = nba.fetch_season_averages(sid)

        elif stype == "aba":
            from src.sources import aba
            result = aba.fetch_player_stats(sid, player_name=player.name)

        elif stype == "feb":
            from src.sources import feb
            result = feb.fetch_player_stats(sid)

        elif stype == "eurobasket":
            from src.sources import eurobasket
            result = eurobasket.fetch_player_stats(
                sid, player_name=player.name, competition=src.competition
            )

        elif stype == "lega":
            from src.sources import lega
            result = lega.fetch_player_stats(sid)

        elif stype == "bcl":
            from src.sources import bcl
            result = bcl.fetch_player_stats(sid, player_name=player.name)

        elif stype == "ncaa_espn":
            from src.sources import ncaa_espn
            result = ncaa_espn.fetch_player_stats(sid, player_name=player.name)

        else:
            logger.warning(
                "Unknown source type '%s' for %s [%s] — skipping.",
                stype, player.name, src.competition,
            )
            return None

        if result:
            # Always use registry as the canonical source for identity fields
            result["player_name"] = player.name
            result.setdefault("team",        player.team)
            result.setdefault("competition", src.competition)
            result["_player_name_registry"] = player.name  # canonical name
            return result

        logger.info(
            "No data returned for %s [%s].", player.name, src.competition
        )
        return None

    except ImportError as exc:
        logger.warning(
            "Scraper module for type '%s' not yet implemented: %s",
            stype, exc,
        )
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed fetching %s [%s] (type=%s id=%s): %s",
            player.name, src.competition, stype, sid, exc,
            exc_info=True,
        )
        return None


def fetch_player_all_sources(player: "Player") -> list[dict]:
    """
    Fetch stats from every ready source for a single player.

    Returns a list of stat dicts (one per competition that returned data).
    """
    results: list[dict] = []
    for src in player.sources:
        stat = _fetch_source(player, src)
        if stat is not None:
            results.append(stat)
    return results


def fetch_all_stats(players: list["Player"]) -> list[dict]:
    """
    Fetch stats for all players across all their sources.

    Returns a flat list of stat dicts.
    Each dict has at minimum: player_name, team, competition, source, date.
    """
    results:  list[dict] = []
    skipped:  list[str]  = []

    for player in players:
        if not player.active:
            logger.debug("Skipping inactive player: %s", player.name)
            continue

        player_results = fetch_player_all_sources(player)

        if player_results:
            results.extend(player_results)
        else:
            skipped.append(player.name)

    logger.info(
        "fetch_all_stats complete: %d records from %d players; %d players had no data.",
        len(results),
        len(results) and len({r["_player_name_registry"] for r in results}),
        len(skipped),
    )
    if skipped:
        logger.info("No data: %s", ", ".join(skipped))

    # Remove internal tracking field before returning
    for r in results:
        r.pop("_player_name_registry", None)

    return results
