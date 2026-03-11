"""
Player registry manager for Unicaja Baloncesto Stats Hub.

Each player can have multiple sources (e.g. ACB + EuroLeague), so stats are
fetched from every competition they play in.

Registry is persisted in data/players/registry.json.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent
_REGISTRY_PATH = _PROJECT_ROOT / "data" / "players" / "registry.json"

# ---------------------------------------------------------------------------
# Source types — each maps to a scraper module in src/sources/
# ---------------------------------------------------------------------------
SOURCE_TYPES = {
    "acb",          # acb.com  (Liga Endesa) — numeric player ID
    "euroleague",   # incrowdsports EuroLeague feed — P0XXXXX code
    "eurocup",      # incrowdsports EuroCup feed — P0XXXXX code
    "nba",          # stats.nba.com — NBA player ID (e.g. 1627734 for Sabonis)
    "aba",          # aba-liga.com (ABA League) — numeric player ID
    "feb",          # baloncestoenvivo.feb.es (Primera FEB / LEB Oro) — numeric ID
    "eurobasket",   # basketball.eurobasket.com — covers Greek League, LNB Pro A, etc.
    "lega",         # legabasket.it (Italian Lega A) — numeric player ID
    "bcl",          # championsleague.basketball (FIBA BCL) — numeric ID
    "ncaa_espn",    # ESPN Deportes — NCAA — ESPN player ID
}


@dataclass
class PlayerSource:
    """One data source entry for a player."""
    competition: str   # Human-readable label, e.g. "ACB", "EuroLeague"
    type: str          # Scraper key — must be in SOURCE_TYPES
    id: str            # Source-specific player ID; "TBD" = not yet known

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "PlayerSource":
        return cls(
            competition=d["competition"],
            type=d["type"],
            id=d.get("id", "TBD"),
        )

    @property
    def is_ready(self) -> bool:
        """True if the ID is known and the source type is supported."""
        return self.id != "TBD" and self.type in SOURCE_TYPES


@dataclass
class Player:
    """A tracked Unicaja alumni player."""
    name: str
    team: str                          # Current team (display only)
    country: str                       # Nationality
    active: bool
    sources: list[PlayerSource] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "team": self.team,
            "country": self.country,
            "active": self.active,
            "sources": [s.to_dict() for s in self.sources],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Player":
        return cls(
            name=d["name"],
            team=d.get("team", ""),
            country=d.get("country", ""),
            active=bool(d.get("active", True)),
            sources=[PlayerSource.from_dict(s) for s in d.get("sources", [])],
        )

    @property
    def ready_sources(self) -> list[PlayerSource]:
        """Sources with a known ID and supported type."""
        return [s for s in self.sources if s.is_ready]


# ---------------------------------------------------------------------------
# Seed data — 20 tracked Unicaja alumni, 2025-26 season
# IDs marked "TBD" are filled in after Task 8 (source ID lookup)
# ---------------------------------------------------------------------------
_SEED_PLAYERS: list[dict] = [
    # --- NBA ---
    # Note: balldontlie.io now requires API key; using stats.nba.com (player ID 1627734)
    # Sabonis had season-ending knee surgery Feb 2026; stats are partial season.
    {
        "name": "Domas Sabonis",
        "team": "Sacramento Kings",
        "country": "Lithuania",
        "active": True,
        "sources": [
            {"competition": "NBA", "type": "nba", "id": "1627734"},
        ],
    },
    # --- EuroLeague + domestic ---
    # Lessort: fibula fracture Dec 2024, ankle issues 2025-26; limited appearances.
    {
        "name": "Mathias Lessort",
        "team": "Panathinaikos",
        "country": "France",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague",  "id": "P003842"},
            {"competition": "Greek League","type": "eurobasket",  "id": "252481"},
        ],
    },
    {
        "name": "Nemanja Nedović",
        "team": "AS Monaco",
        "country": "Serbia",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague", "id": "P000848"},
            {"competition": "LNB Pro A",  "type": "eurobasket", "id": "130801"},
        ],
    },
    {
        "name": "Dylan Osetkowski",
        "team": "Partizan Mozzart Bet",
        "country": "United States",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague", "id": "P010581"},
            {"competition": "ABA League", "type": "aba",        "id": "5100"},
        ],
    },
    {
        "name": "Darío Brizuela",
        "team": "FC Barcelona",
        "country": "Spain",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague", "id": "P009992"},
            {"competition": "ACB",        "type": "acb",        "id": "20209919"},
        ],
    },
    # Carter: pulmonary embolism diagnosis Round 4 — partial EuroLeague stats only.
    {
        "name": "Tyson Carter",
        "team": "Crvena zvezda",
        "country": "United States",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague", "id": "P011305"},
            {"competition": "ABA League", "type": "aba",        "id": "5075"},
        ],
    },
    {
        "name": "Yankuba Sima",
        "team": "Valencia Basket",
        "country": "Spain",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague", "id": "P005596"},
            {"competition": "ACB",        "type": "acb",        "id": "20213230"},
        ],
    },
    {
        "name": "Kameron Taylor",
        "team": "Valencia Basket",
        "country": "United States",
        "active": True,
        "sources": [
            {"competition": "EuroLeague", "type": "euroleague", "id": "P011217"},
            {"competition": "ACB",        "type": "acb",        "id": "30001981"},
        ],
    },
    # --- ACB only ---
    {
        "name": "Giorgi Shermadini",
        "team": "Lenovo Tenerife",
        "country": "Georgia",
        "active": True,
        "sources": [
            {"competition": "ACB", "type": "acb", "id": "20210659"},
        ],
    },
    {
        "name": "Rubén Guerrero",
        "team": "MoraBanc Andorra",
        "country": "Spain",
        "active": True,
        "sources": [
            {"competition": "ACB", "type": "acb", "id": "20210707"},
        ],
    },
    # Lima re-joined Burgos mid-season (Jan 2026); stats are partial.
    {
        "name": "Augusto Lima",
        "team": "San Pablo Burgos",
        "country": "Brazil",
        "active": True,
        "sources": [
            {"competition": "ACB", "type": "acb", "id": "20200277"},
        ],
    },
    # --- EuroCup + ABA ---
    {
        "name": "Axel Bouteille",
        "team": "Budućnost VOLI",
        "country": "France",
        "active": True,
        "sources": [
            {"competition": "EuroCup",   "type": "eurocup", "id": "P003840"},
            {"competition": "ABA League", "type": "aba",     "id": "5073"},
        ],
    },
    # --- Greek League + BCL ---
    {
        "name": "Mindaugas Kuzminskas",
        "team": "AEK Athens",
        "country": "Lithuania",
        "active": True,
        "sources": [
            {"competition": "Greek League", "type": "eurobasket", "id": "26892"},
            {"competition": "BCL",          "type": "bcl",        "id": "161021"},
        ],
    },
    # --- ABA League ---
    {
        "name": "Dragan Milosavljević",
        "team": "Igokea m:tel",
        "country": "Serbia",
        "active": True,
        "sources": [
            {"competition": "ABA League", "type": "aba", "id": "1076"},
        ],
    },
    # --- Primera FEB / LEB Oro (baloncestoenvivo.feb.es) ---
    # Granger and Salin both at CB Estudiantes (Primera FEB, formerly LEB Oro).
    {
        "name": "Jayson Granger",
        "team": "CB Estudiantes",
        "country": "Uruguay",
        "active": True,
        "sources": [
            {"competition": "Primera FEB", "type": "feb", "id": "951466"},
        ],
    },
    {
        "name": "Sasu Salin",
        "team": "CB Estudiantes",
        "country": "Finland",
        "active": True,
        "sources": [
            {"competition": "Primera FEB", "type": "feb", "id": "791394"},
        ],
    },
    # --- Italian Lega A (legabasket.it) ---
    {
        "name": "Kyle Wiltjer",
        "team": "Umana Reyer Venezia",
        "country": "Canada",
        "active": True,
        "sources": [
            {"competition": "Lega A", "type": "lega", "id": "7079"},
        ],
    },
    # --- NCAA (ESPN Deportes) ---
    {
        "name": "Mario Saint-Supéry",
        "team": "Gonzaga Bulldogs",
        "country": "Spain",
        "active": True,
        "sources": [
            {"competition": "NCAA", "type": "ncaa_espn", "id": "5313012"},
        ],
    },
    # --- Free agent ---
    {
        "name": "Adam Waczyński",
        "team": "Free Agent",
        "country": "Poland",
        "active": False,
        "sources": [],
    },
]


# ---------------------------------------------------------------------------
# Registry I/O
# ---------------------------------------------------------------------------

def load_registry() -> list[Player]:
    """Load players from registry.json, falling back to seed data."""
    if not _REGISTRY_PATH.exists():
        logger.warning(
            "Registry not found at %s — using seed data. "
            "Run with --seed to persist.",
            _REGISTRY_PATH,
        )
        return [Player.from_dict(d) for d in _SEED_PLAYERS]

    with _REGISTRY_PATH.open(encoding="utf-8") as fh:
        raw: list[dict] = json.load(fh)

    players = [Player.from_dict(entry) for entry in raw]
    logger.info("Loaded %d players from registry.", len(players))
    return players


def save_registry(players: list[Player]) -> None:
    """Persist a list of Player objects to registry.json."""
    _REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _REGISTRY_PATH.open("w", encoding="utf-8") as fh:
        json.dump([p.to_dict() for p in players], fh, ensure_ascii=False, indent=2)
    logger.info("Saved %d players to registry.", len(players))


def get_active_players() -> list[Player]:
    """Return only active players that have at least one ready source."""
    all_players = load_registry()
    active = [p for p in all_players if p.active]
    logger.info(
        "%d active player(s), %d with at least one ready source.",
        len(active),
        sum(1 for p in active if p.ready_sources),
    )
    return active


def seed_registry() -> None:
    """Write seed data to registry.json (overwrites existing file)."""
    players = [Player.from_dict(d) for d in _SEED_PLAYERS]
    save_registry(players)
    logger.info("Registry seeded with %d players.", len(players))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_registry()
    players = get_active_players()
    print(f"\nTracked players ({len(players)}):")
    for p in players:
        src_summary = ", ".join(
            f"{s.competition}({'✓' if s.is_ready else 'TBD'})"
            for s in p.sources
        ) or "no sources"
        print(f"  {p.name:<28} {p.team:<30} [{src_summary}]")
