"""
Unicaja Baloncesto Stats Hub — Streamlit web app.

Main table   : all tracked players — games played in last 24 h shown with stats,
               everyone else shown as "Did not play".
History table: select a player → every game we've ever collected, one row each.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from src.players import get_active_players
from src.storage import get_all_dates, load_stats

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Unicaja Stats Hub",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STAT_COLS = [
    "min", "pts",
    "t2m", "t2a", "t2_pct",
    "t3m", "t3a", "t3_pct",
    "ftm", "fta", "ft_pct",
    "reb_off", "reb_def", "reb",
    "ast", "stl", "tov", "blk", "fouls", "plus_minus", "val",
]

_COL_LABELS = {
    "player_name": "Player",
    "team":        "Team",
    "competition": "Competition",
    "game_date":   "Game Date",
    "opponent":    "Opponent",
    "result":      "Result",
    "min":         "MIN",
    "pts":         "PTS",
    "t2m":         "T2M",  "t2a":  "T2A",  "t2_pct":  "T2%",
    "t3m":         "T3M",  "t3a":  "T3A",  "t3_pct":  "T3%",
    "ftm":         "FTM",  "fta":  "FTA",  "ft_pct":  "FT%",
    "reb_off":     "RO",   "reb_def": "RD", "reb":     "RT",
    "ast":         "AST",  "stl":  "STL",  "tov":     "TOV",
    "blk":         "BLK",  "fouls": "F",   "plus_minus": "+/-",
    "val":         "VAL",
}

_DISPLAY_COLS = (
    ["player_name", "team", "competition", "game_date", "opponent", "result"]
    + _STAT_COLS
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _canonical_name(name: str) -> frozenset:
    """
    Normalise a player name into a frozenset of lowercase ASCII tokens so that
    'Lessort, Mathias' and 'Mathias Lessort' compare as equal.
    """
    ascii_name = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return frozenset(re.sub(r"[^a-z ]", "", ascii_name.lower()).split())


def _is_real_name(name: str) -> bool:
    """Return False for strings that look like player IDs (e.g. '20200277', 'P003842')."""
    return not re.match(r"^P?\d+$", name.strip())


def _game_is_within_24h(game_date: str) -> bool:
    """True if game_date (YYYY-MM-DD) falls on or after yesterday's calendar date."""
    if not game_date or game_date in ("—", "N/A"):
        return False
    try:
        gd = datetime.strptime(game_date[:10], "%Y-%m-%d").date()
        cutoff = (datetime.now() - timedelta(hours=24)).date()
        return gd >= cutoff
    except ValueError:
        return False


def _fmt_val(val: object, field: str = "") -> str:
    """Format a stat cell. Percentages get 1 decimal; all other stats are integers."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    try:
        f = float(val)
        if field in {"t2_pct", "t3_pct", "ft_pct"}:
            return f"{f:.1f}"
        return str(int(round(f)))
    except (TypeError, ValueError):
        return str(val) if val != "" else "N/A"


def _build_row(record: dict) -> dict:
    row = {}
    for field in _DISPLAY_COLS:
        label = _COL_LABELS.get(field, field)
        if field in _STAT_COLS:
            row[label] = _fmt_val(record.get(field), field)
        else:
            row[label] = record.get(field) or "N/A"
    return row


def _did_not_play_row(player_name: str, team: str) -> dict:
    row = {_COL_LABELS.get(f, f): "—" for f in _DISPLAY_COLS}
    row["Player"]    = player_name
    row["Team"]      = team
    row["Game Date"] = "Did not play"
    return row


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_latest() -> tuple[str, list[dict]]:
    dates = get_all_dates()
    if not dates:
        return "", []
    latest = dates[-1]
    return latest, load_stats(latest)


@st.cache_data(ttl=300)
def _load_all() -> dict[str, list[dict]]:
    return {d: load_stats(d) for d in get_all_dates()}


# ---------------------------------------------------------------------------
# Main table
# ---------------------------------------------------------------------------

def render_latest(records: list[dict]) -> None:
    cutoff_label = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
    st.subheader(f"Last 24 hours — since {cutoff_label}")

    if not records:
        st.warning("No data yet. Run `python main.py` to fetch stats.")
        return

    # Split records into played (valid date, within 24h) and the rest.
    # Records with no game_date are excluded from both categories.
    played_records: list[dict] = []
    for rec in records:
        gd = str(rec.get("game_date", ""))
        if gd and gd not in ("", "—", "N/A") and _game_is_within_24h(gd):
            played_records.append(rec)

    # Build a set of canonical name tokens for players who played.
    played_canonical: set[frozenset] = {
        _canonical_name(r["player_name"])
        for r in played_records
        if r.get("player_name")
    }

    played_rows: list[dict] = [_build_row(r) for r in played_records]

    # Add "Did not play" for every active registry player not in played set.
    did_not_play_rows: list[dict] = []
    for player in sorted(get_active_players(), key=lambda p: p.name):
        if _canonical_name(player.name) not in played_canonical:
            did_not_play_rows.append(_did_not_play_row(player.name, player.team))

    all_rows = played_rows + did_not_play_rows
    if not all_rows:
        st.info("No data available.")
        return

    df = pd.DataFrame(all_rows)
    st.caption(
        f"🟢 **{len(played_rows)}** game(s) in the last 24 h · "
        f"⚪ **{len(did_not_play_rows)}** player(s) did not play"
    )
    st.dataframe(df, use_container_width=True, hide_index=True, height=600)


# ---------------------------------------------------------------------------
# Historical table
# ---------------------------------------------------------------------------

def render_history(all_data: dict[str, list[dict]]) -> None:
    st.subheader("Player game history")

    if not all_data:
        st.info("No historical data yet.")
        return

    # Collect real player names (exclude IDs stored as names).
    all_names: set[str] = set()
    for records in all_data.values():
        for r in records:
            name = r.get("player_name", "")
            if name and _is_real_name(name):
                all_names.add(name)

    selected = st.selectbox(
        "Select player",
        options=sorted(all_names),
        key="history_player",
    )
    if not selected:
        return

    selected_canonical = _canonical_name(selected)

    seen: set[tuple] = set()
    game_rows: list[dict] = []

    for run_date in sorted(all_data.keys()):
        for rec in all_data[run_date]:
            name = rec.get("player_name", "")
            if not name or _canonical_name(name) != selected_canonical:
                continue
            # Exclude records with no game_date — unverifiable.
            gd = str(rec.get("game_date", ""))
            if not gd or gd in ("", "—", "N/A"):
                continue
            key = (rec.get("competition", ""), gd)
            if key in seen:
                continue
            seen.add(key)
            game_rows.append(_build_row(rec))

    if not game_rows:
        st.info(f"No game records found for {selected}.")
        return

    df = pd.DataFrame(game_rows)
    if "Game Date" in df.columns:
        df = df.sort_values("Game Date", ascending=False)

    st.caption(f"{len(game_rows)} game(s) collected for **{selected}**")
    st.dataframe(df, use_container_width=True, hide_index=True, height=500)


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

st.title("🏀 Unicaja Baloncesto — Ex-Players Stats")
st.caption("Latest game box scores for former Unicaja players.")

dates = get_all_dates()
if not dates:
    st.warning("No data yet. Run `python main.py` to fetch stats.")
    st.stop()

_, latest_records = _load_latest()
all_data = _load_all()

render_latest(latest_records)

st.divider()

render_history(all_data)
