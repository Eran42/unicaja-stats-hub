"""
Unicaja Baloncesto Stats Hub — Streamlit web app.

Main table   : latest run's data — all tracked players, games-first ordering.
History table: select a player → every game we've ever collected, one row each.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

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

# All stat columns in display order
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

# How far back a game_date can be and still count as "played this window"
_WINDOW_DAYS = 2


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_latest() -> tuple[str, list[dict]]:
    """Return (run_date, records) for the most recent saved run."""
    dates = get_all_dates()
    if not dates:
        return "", []
    latest = dates[-1]
    return latest, load_stats(latest)


@st.cache_data(ttl=300)
def _load_all() -> dict[str, list[dict]]:
    """Return {date: records} for every saved run."""
    return {d: load_stats(d) for d in get_all_dates()}


def _fmt_val(val: object) -> str:
    """Format a stat cell: float → '12.3', None → 'N/A'."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    try:
        f = float(val)
        return f"{f:.1f}"
    except (TypeError, ValueError):
        return str(val) if val != "" else "N/A"


def _game_is_recent(game_date: str, run_date: str) -> bool:
    """
    True if game_date is within _WINDOW_DAYS of run_date.
    Handles ISO dates (YYYY-MM-DD) and best-effort for other formats.
    """
    if not game_date or game_date in ("—", "N/A"):
        return False
    try:
        gd = datetime.strptime(game_date[:10], "%Y-%m-%d").date()
        rd = datetime.strptime(run_date[:10], "%Y-%m-%d").date()
        return (rd - gd).days <= _WINDOW_DAYS
    except ValueError:
        # Can't parse date — assume recent if it exists
        return bool(game_date)


def _build_row(record: dict) -> dict:
    """Build a display row from a stat record."""
    row = {}
    for field in _DISPLAY_COLS:
        label = _COL_LABELS.get(field, field)
        if field in _STAT_COLS:
            row[label] = _fmt_val(record.get(field))
        else:
            row[label] = record.get(field) or "N/A"
    return row


def _no_game_row(player_name: str, team: str) -> dict:
    """Placeholder row for a player who didn't play this window."""
    row = {_COL_LABELS.get(f, f): "—" for f in _DISPLAY_COLS}
    row["Player"]      = player_name
    row["Team"]        = team
    row["Competition"] = "—"
    row["Game Date"]   = "No game played"
    row["Opponent"]    = "—"
    row["Result"]      = "—"
    return row


# ---------------------------------------------------------------------------
# Main table
# ---------------------------------------------------------------------------

def render_latest(run_date: str, records: list[dict]) -> None:
    st.subheader(f"Latest games — run {run_date}")

    if not records:
        st.warning("No data yet. Run `python main.py` to fetch stats.")
        return

    # Build a lookup: player_name → list of records (one per competition)
    by_player: dict[str, list[dict]] = {}
    for r in records:
        name = r.get("player_name", "Unknown")
        by_player.setdefault(name, []).append(r)

    played_rows: list[dict]    = []
    no_game_rows: list[dict]   = []

    for name, player_records in sorted(by_player.items()):
        for rec in player_records:
            gd = str(rec.get("game_date", ""))
            if _game_is_recent(gd, run_date):
                played_rows.append(_build_row(rec))
            else:
                no_game_rows.append(_no_game_row(
                    name, rec.get("team", "")
                ))

    all_rows = played_rows + no_game_rows
    if not all_rows:
        st.info("No recent games found in this run.")
        return

    df = pd.DataFrame(all_rows)

    st.caption(
        f"🟢 **{len(played_rows)}** game(s) played · "
        f"⚪ **{len(no_game_rows)}** player(s) with no recent game"
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

    # Collect all unique player names
    all_names: set[str] = set()
    for records in all_data.values():
        for r in records:
            name = r.get("player_name")
            if name:
                all_names.add(name)

    selected = st.selectbox(
        "Select player",
        options=sorted(all_names),
        key="history_player",
    )
    if not selected:
        return

    # Gather every game record for this player across all runs,
    # deduplicated by (competition, game_date)
    seen: set[tuple] = set()
    game_rows: list[dict] = []

    for run_date in sorted(all_data.keys()):
        for rec in all_data[run_date]:
            if rec.get("player_name") != selected:
                continue
            key = (rec.get("competition", ""), str(rec.get("game_date", "")))
            if key in seen:
                continue
            seen.add(key)
            game_rows.append(_build_row(rec))

    if not game_rows:
        st.info(f"No game records found for {selected}.")
        return

    df = pd.DataFrame(game_rows)

    # Sort by Game Date descending
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

run_date, latest_records = _load_latest()
all_data = _load_all()

render_latest(run_date, latest_records)

st.divider()

render_history(all_data)
