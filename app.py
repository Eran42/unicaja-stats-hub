"""
Unicaja Baloncesto Stats Hub — Streamlit web app.

Reads pre-fetched stats from data/stats/{date}.json files.
Data is refreshed daily via GitHub Actions (see .github/workflows/daily_fetch.yml).

Run locally:  streamlit run app.py
"""

from __future__ import annotations

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
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STAT_COLS = [
    "player_name", "team", "competition", "game_date",
    "min", "pts",
    "t2m", "t2a", "t2_pct",
    "t3m", "t3a", "t3_pct",
    "ftm", "fta", "ft_pct",
    "reb_off", "reb_def", "reb",
    "ast", "stl", "tov", "blk", "fouls", "plus_minus", "val",
]

_COL_LABELS = {
    "player_name":  "Player",
    "team":         "Team",
    "competition":  "Competition",
    "game_date":    "Game Date",
    "min":          "MIN",
    "pts":          "PTS",
    "t2m":          "T2M",
    "t2a":          "T2A",
    "t2_pct":       "T2%",
    "t3m":          "T3M",
    "t3a":          "T3A",
    "t3_pct":       "T3%",
    "ftm":          "FTM",
    "fta":          "FTA",
    "ft_pct":       "FT%",
    "reb_off":      "RO",
    "reb_def":      "RD",
    "reb":          "RT",
    "ast":          "AST",
    "stl":          "STL",
    "tov":          "TOV",
    "blk":          "BLK",
    "fouls":        "F",
    "plus_minus":   "+/-",
    "val":          "VAL",
}

# Columns shown in the "compact" view
_COMPACT_COLS = [
    "player_name", "team", "competition", "game_date",
    "min", "pts", "t2_pct", "t3_pct", "ft_pct",
    "reb", "ast", "stl", "tov", "val",
]


@st.cache_data(ttl=300)
def _load(date: str) -> pd.DataFrame:
    """Load stats for a date and return a display-ready DataFrame."""
    records = load_stats(date)
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Ensure all expected columns exist
    for col in _STAT_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[_STAT_COLS].copy()

    # Round floats to 1 decimal place
    float_cols = df.select_dtypes(include="float").columns
    df[float_cols] = df[float_cols].round(1)

    # Rename to display labels
    df = df.rename(columns=_COL_LABELS)

    return df


def _highlight_pts(val: object) -> str:
    """Green tint for high scorers."""
    try:
        v = float(val)
        if v >= 15:
            return "background-color: #1a3a1a; color: #7fff7f"
        if v >= 10:
            return "background-color: #0d2a0d"
    except (TypeError, ValueError):
        pass
    return ""


def _highlight_pct(val: object) -> str:
    """Colour code shooting percentages."""
    try:
        v = float(val)
        if v >= 60:
            return "color: #7fff7f"
        if v >= 45:
            return "color: #b8ffb8"
        if v <= 25:
            return "color: #ff7f7f"
    except (TypeError, ValueError):
        pass
    return ""


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🏀 Unicaja Stats Hub")
    st.caption("Former Unicaja players — current season stats")
    st.divider()

    dates = get_all_dates()
    if not dates:
        st.warning("No data yet. Run `python main.py` to fetch stats.")
        st.stop()

    # Most recent date selected by default
    selected_date = st.selectbox(
        "Date",
        options=list(reversed(dates)),
        format_func=lambda d: d,
    )

    st.divider()

    df_full = _load(selected_date)
    if df_full.empty:
        st.warning(f"No records found for {selected_date}.")
        st.stop()

    # Competition filter
    competitions = sorted(df_full["Competition"].dropna().unique())
    selected_comps = st.multiselect(
        "Competition",
        options=competitions,
        default=competitions,
    )

    # View mode
    view = st.radio("View", ["Full stats", "Compact"], horizontal=True)

    st.divider()
    st.caption(f"Last update: **{selected_date}**")
    st.caption(f"{len(df_full)} records · {df_full['Player'].nunique()} players")


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

# Apply competition filter
df = df_full[df_full["Competition"].isin(selected_comps)].reset_index(drop=True)

if view == "Compact":
    compact_labels = [_COL_LABELS[c] for c in _COMPACT_COLS]
    display_cols = [c for c in compact_labels if c in df.columns]
    df = df[display_cols]

# Header
col1, col2, col3 = st.columns([3, 1, 1])
with col1:
    st.subheader(f"Stats — {selected_date}")
with col2:
    st.metric("Players", df["Player"].nunique() if "Player" in df else 0)
with col3:
    latest = df["Game Date"].max() if "Game Date" in df.columns else "—"
    st.metric("Latest game", latest)

# Stats table
pct_cols = [c for c in ["T2%", "T3%", "FT%"] if c in df.columns]
pts_col  = ["PTS"] if "PTS" in df.columns else []

styled = (
    df.style
    .applymap(_highlight_pts,  subset=pts_col)
    .applymap(_highlight_pct,  subset=pct_cols)
    .format(precision=1, na_rep="—", subset=df.select_dtypes("float").columns.tolist())
)

st.dataframe(
    styled,
    use_container_width=True,
    height=600,
    hide_index=True,
)

# ---------------------------------------------------------------------------
# Per-player breakdown
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Player breakdown")

players = sorted(df_full["Player"].dropna().unique())
selected_player = st.selectbox("Select player", options=players)

if selected_player:
    player_df = df_full[df_full["Player"] == selected_player].reset_index(drop=True)

    # Summary metrics row
    for _, row in player_df.iterrows():
        comp = row.get("Competition", "")
        cols = st.columns(8)
        metrics = [
            ("Competition", comp),
            ("Game Date", row.get("Game Date", "—")),
            ("PTS", row.get("PTS", "—")),
            ("REB", row.get("RT",  "—")),
            ("AST", row.get("AST", "—")),
            ("T3%", row.get("T3%", "—")),
            ("FT%", row.get("FT%", "—")),
            ("VAL", row.get("VAL", "—")),
        ]
        for col, (label, value) in zip(cols, metrics):
            col.metric(label, value)
        st.divider()
