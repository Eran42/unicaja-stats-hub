# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Fetch stats for all players (run once and exit)
python main.py

# Run once, then repeat daily at a given time
python main.py --schedule --time 06:00

# Re-seed registry from built-in defaults
python main.py --seed

# Debug a single scraper from the repo root
python -m src.sources.acb 2012109      # ACB by player id
python -m src.sources.euroleague P003842   # EuroLeague by player code
python -m src.sources.euroleague --find lessort  # Search by name

# Launch the Streamlit dashboard
streamlit run app.py
```

## Architecture

The pipeline is: **registry → router → scrapers → storage → dashboard**

- `data/players/registry.json` — list of tracked players; each entry has `name`, `team`, `active`, and a `sources` array with `{type, id, competition}` entries
- `src/players.py` — loads/seeds the registry; `get_active_players()` returns `Player` objects; `PlayerSource.is_ready` is `False` when `id` is `"TBD"`
- `src/router.py` — iterates players and sources, lazy-imports the matching scraper module, calls `fetch_player_stats(id)`, sets `player_name`/`team`/`competition` defaults via `setdefault`
- `src/sources/` — one module per league; each exports `fetch_player_stats(id) -> dict`; returns empty dict on failure (never raises)
- `src/storage.py` — writes `data/stats/{date}.json` and `data/stats/{date}.csv`
- `app.py` — Streamlit dashboard reading the latest stats files from `data/stats/`

### Source types

| `type` | Module | Data kind |
|---|---|---|
| `acb` | `acb.py` | Last game box score (scraped from acb.com) |
| `euroleague` | `euroleague.py` | Last game box score (incrowdsports v2 API) |
| `eurocup` | `euroleague.py` | Last game box score (same API, comp="U") |
| `nba` | `nba.py` | Season averages (stats.nba.com API) |
| `aba` | `aba.py` | Last game box score (scraped) |
| `feb` | `feb.py` | Season averages (baloncestoenvivo.feb.es) |
| `eurobasket` | `eurobasket.py` | Last game box score (basketball.eurobasket.com) |
| `lega` | `lega.py` | Season averages (legabasket.it) |
| `bcl` | `bcl.py` | Season averages (basketball champions league) |
| `ncaa_espn` | `ncaa_espn.py` | Last game box score (ESPN) |

### Canonical stat schema

All scrapers return a dict with these keys (use `None` for unavailable fields, never omit):

```
player_id, player_name, team, source, competition, season,
game_date (ISO "YYYY-MM-DD" or "" for averages),
opponent, result ("V 85-76" / "D 76-85" or "" for averages),
date (today, ISO),
min, pts,
t2m, t2a, t2_pct, t3m, t3a, t3_pct, ftm, fta, ft_pct,
reb_off, reb_def, reb, ast, stl, tov, blk, fouls, plus_minus, val
```

### Dashboard rules

- **First table (recent games):** Show only games from the last 24 hours relative to the current moment. No game older than 24 hours should appear. This window always rolls with the current time. **Rows with no `game_date` must be excluded entirely** — if we can't confirm when a game was played, it has no place in this table. Do not show "No game played" placeholders here either; only confirmed games with a valid date appear.
- **Second table (per-player list):** May include results beyond the last 24 hours. Additional results found from other sources can be added. Always sort by `game_date` descending. **Rows with no `game_date` must also be excluded** — a result without a known date is unverifiable and should not be stored or displayed.

### Continuous improvement

After every run, review what each scraper returned and identify gaps or quality issues per source and competition. For example: missing fields, stale data, wrong stat types, unreliable endpoints, or competitions where a different source would give better coverage. Then fix the code — change the data source, adjust the parser, or add fallback logic — so the next run handles it better. These improvements should be committed alongside the stats output, not deferred.

### Player name canonicalisation

All stored records must use the exact canonical name from `data/players/registry.json`. After any bulk fetch, historical backfill, or data import, run a name audit before committing:

1. **Accent/encoding duplicates** — scrapers and external APIs often return the same player under different encodings:
   - Correct Unicode: `Darío Brizuela`, `Jaime Fernández`, `Mario Saint-Supéry`, `Rubén Guerrero`, `Nemanja Nedović`, `Dragan Milosavljević`, `Mindaugas Kuzminskas`
   - Watch for UTF-8 bytes misread as latin-1 (mojibake), e.g. `\xc3\xad` instead of `\xed` for `í`
2. **"Last, First" format** — EuroLeague incrowdsports API sometimes returns names as `Brizuela, Dario` or `Nedovic, Nemanja`; always remap to canonical
3. **P-codes as player names** — old daily files may have the EuroLeague player code (e.g. `P009992`) where the name should be; remap using the registry
4. **De-duplication key** — records are de-duplicated by `(player_name, source, competition, game_date)`; a name mismatch creates phantom duplicates that appear as separate players in the dashboard

After any bulk data operation, verify with:
```python
python -c "
import json, glob, unicodedata
names = set()
for f in sorted(glob.glob('data/stats/*.json')):
    for r in json.load(open(f, encoding='utf-8')):
        names.add(r.get('player_name', ''))
for n in sorted(names):
    print(unicodedata.normalize('NFKD', n).encode('ascii','ignore').decode(), '|', n)
"
```
Any name that doesn't exactly match a registry entry must be fixed before committing.

### Pre-publish checklist

Before committing or publishing any stats output, go through each of these steps:

1. Review all rows for obvious errors (missing values, implausible stats, wrong opponents/results).
2. Cross-validate suspicious entries using web search or other authoritative sources (league websites, box score aggregators).
3. Correct or remove any rows that cannot be verified.
4. Only publish after the data has been reviewed and validated.

### Known source issues (updated 2026-03-13)

#### ACB (`acb.py`)
- **Rebounds order**: The game-log column `T(D+O)` uses reversed labeling — the first sub-value is offensive, the second defensive (confirmed by `live.acb.com` legend: `DR=Offensive, OR=Defensive`). Fixed in `_parse_reb_cell` (swap on paren format).
- **Fouls column**: The `C` header at position 12 can hold a season-cumulative value (> 5) instead of per-game fouls. Values > 5 are now discarded. Real per-game fouls seem to come from a second `C` column further right that `_build_col_map` ignores — investigate and remap if accuracy improves.
- **Result V/D prefix**: `_extract_result_from_game_page` fails to determine Win/Loss for most games because the PARTIDOS cell contains both team names joined by `-`. Results are stored as raw scores only. Fix: split PARTIDOS on `-` and match each part against the game page's local/visitor team names.
- **FEB source (500 errors)**: `baloncestoenvivo.feb.es` returns HTTP 500 for all requests as of 2026-03-13. The API may have changed or the site is down. Need to find an alternative endpoint or scrape a different FEB URL.

#### EuroBasket (`eurobasket.py`)
- Returning no game rows for Lessort (Greek League, id=252481), Nedović (LNB Pro A, id=130801), and Kuzminskas (Greek League, id=26892). Player IDs or season path may have changed. Verify correct IDs on `basketball.eurobasket.com` before next run.

#### ABA League (`aba.py`)
- No game rows for Osetkowski (id=5100, suspended for anti-doping), Bouteille (id=5073), Carter (id=5075), and Milosavljević (id=1076). Check whether these IDs are still valid for the current season slug, or if players changed teams.

#### EuroLeague player availability
- **Tyson Carter** (P011305, Crvena Zvezda): hospitalized with pulmonary embolism in Oct–Nov 2025; missed most of the season. ID is correct but he hasn't appeared in the last 20 played EuroLeague games. Monitor and re-enable when he resumes regular play.
- **Yankuba Sima** (P005596): ID not found in EuroLeague last-20-games window. He plays limited minutes for Valencia — verify whether his EuroLeague ID is current.

#### NBA (`nba.py`)
- `stats.nba.com` regularly times out (15 s limit). Consider raising the timeout or adding a retry, or switch to an unofficial mirror endpoint.

### CI

`.github/workflows/daily_fetch.yml` runs `python main.py` at 07:00 UTC daily, then commits any new files under `data/stats/` using `git push origin HEAD`.
