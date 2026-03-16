"""
Unicaja Baloncesto Stats Hub — Streamlit web app.

Main table   : only players who played a confirmed game in the last 24 h.
History table: select a player → every game we've ever collected, one row each.
"""

from __future__ import annotations

import re
import unicodedata
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
# Branding
# ---------------------------------------------------------------------------

_UNICAJA_GREEN      = "#006633"
_UNICAJA_GREEN_DARK = "#004d26"
_UNICAJA_GREEN_PALE = "#eef7f1"
_UNICAJA_GREEN_MID  = "#d4edde"
_UNICAJA_PURPLE     = "#6B2FA0"

def _inject_css() -> None:
    st.markdown(
        f"""
        <style>
        /* ── Top accent bar ── */
        [data-testid="stHeader"] {{
            background: {_UNICAJA_GREEN};
            height: 4px;
        }}

        /* ── Title / headings ── */
        h1 {{
            color: {_UNICAJA_GREEN_DARK} !important;
            font-weight: 800 !important;
            letter-spacing: -0.5px;
        }}
        /* ── Divider ── */
        hr {{
            border-color: {_UNICAJA_GREEN_MID} !important;
            border-width: 2px !important;
        }}

        /* ── Selectbox labels ── */
        [data-testid="stSelectbox"] label,
        [data-testid="stSelectbox"] [data-testid="stWidgetLabel"] p {{
            color: {_UNICAJA_PURPLE} !important;
            font-weight: 600 !important;
        }}

        /* ── Selectbox focus ring + border ── */
        [data-testid="stSelectbox"] > div:focus-within {{
            border-color: {_UNICAJA_PURPLE} !important;
            box-shadow: 0 0 0 2px rgba(107, 47, 160, 0.18) !important;
        }}

        /* ── Selectbox dropdown highlight (hovered / selected option) ── */
        [data-baseweb="select"] [aria-selected="true"],
        [data-baseweb="menu"] [role="option"]:hover {{
            background-color: rgba(107, 47, 160, 0.12) !important;
            color: {_UNICAJA_PURPLE} !important;
        }}

        /* ── Info / warning boxes ── */
        [data-testid="stNotification"] {{
            border-left: 4px solid {_UNICAJA_GREEN} !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> None:
    st.markdown(
        f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 18px 0 6px 0;
            border-bottom: 3px solid {_UNICAJA_GREEN};
            margin-bottom: 8px;
        ">
            <div style="
                background: {_UNICAJA_GREEN};
                color: white;
                font-size: 28px;
                font-weight: 900;
                letter-spacing: 1px;
                padding: 6px 16px;
                border-radius: 4px;
                font-family: sans-serif;
                line-height: 1;
            ">UNICAJA</div>
            <div>
                <div style="font-size: 22px; font-weight: 700; font-family: sans-serif;">
                    Ex-Players Stats
                </div>
                <div style="font-size: 13px; opacity: 0.65; font-family: sans-serif;">
                    Latest game box scores for former Unicaja Baloncesto players
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
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

_ROW_HEIGHT_PX  = 35
_HEADER_HEIGHT_PX = 38


# Columns that open a new group — get a thick left border separator
_GROUP_DIVIDERS = {"T2M", "T3M", "FTM", "RO"}

# Column-group tints: (even-row bg, odd-row bg)
_COL_GROUP_COLORS: dict[str, tuple[str, str]] = {
    "T2M": ("rgba(37,99,235,0.07)",  "rgba(37,99,235,0.15)"),
    "T2A": ("rgba(37,99,235,0.07)",  "rgba(37,99,235,0.15)"),
    "T2%": ("rgba(37,99,235,0.07)",  "rgba(37,99,235,0.15)"),
    "T3M": ("rgba(234,88,12,0.07)",  "rgba(234,88,12,0.15)"),
    "T3A": ("rgba(234,88,12,0.07)",  "rgba(234,88,12,0.15)"),
    "T3%": ("rgba(234,88,12,0.07)",  "rgba(234,88,12,0.15)"),
    "FTM": ("rgba(202,138,4,0.07)",  "rgba(202,138,4,0.15)"),
    "FTA": ("rgba(202,138,4,0.07)",  "rgba(202,138,4,0.15)"),
    "FT%": ("rgba(202,138,4,0.07)",  "rgba(202,138,4,0.15)"),
    "RO":  ("rgba(13,148,136,0.07)", "rgba(13,148,136,0.15)"),
    "RD":  ("rgba(13,148,136,0.07)", "rgba(13,148,136,0.15)"),
    "RT":  ("rgba(13,148,136,0.07)", "rgba(13,148,136,0.15)"),
}


def _style_table(df: pd.DataFrame, stripe: str) -> pd.DataFrame:
    """
    Return a same-shape DataFrame of CSS strings.
    Grouped columns get a fixed tint (two shades for alternating rows).
    Ungrouped columns get the standard row stripe on odd rows.
    """
    out = pd.DataFrame("", index=df.index, columns=df.columns)
    for i in df.index:
        odd = bool(i % 2)
        for col in df.columns:
            if col in _COL_GROUP_COLORS:
                bg = _COL_GROUP_COLORS[col][1] if odd else _COL_GROUP_COLORS[col][0]
            else:
                bg = stripe if odd else ""
            parts = []
            if bg:
                parts.append(f"background-color: {bg}")
            if col in _GROUP_DIVIDERS:
                parts.append("border-left: 2px solid rgba(80,80,80,0.30)")
            out.loc[i, col] = "; ".join(parts)
    return out


def render_latest(records: list[dict]) -> None:
    cutoff_label = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
    st.markdown(
        f'<h3 style="color:{_UNICAJA_GREEN};font-weight:700;">Last 24 hours — since {cutoff_label}</h3>',
        unsafe_allow_html=True,
    )

    if not records:
        st.warning("No data yet. Run `python main.py` to fetch stats.")
        return

    # Only keep records with a confirmed game_date within the last 24 h.
    played_records: list[dict] = [
        rec for rec in records
        if str(rec.get("game_date", "")) not in ("", "—", "N/A")
        and _game_is_within_24h(str(rec.get("game_date", "")))
    ]

    played_rows: list[dict] = [_build_row(r) for r in played_records]

    if not played_rows:
        st.info("No games in the last 24 hours.")
        return

    df = pd.DataFrame(played_rows)
    height = _HEADER_HEIGHT_PX + len(played_rows) * _ROW_HEIGHT_PX + 4
    st.caption(f"🟢 **{len(played_rows)}** game(s) in the last 24 h")
    st.dataframe(
        df.style.apply(_style_table, stripe="rgba(0,102,51,0.08)", axis=None),
        use_container_width=True,
        hide_index=True,
        height=height,
    )


# ---------------------------------------------------------------------------
# Historical table
# ---------------------------------------------------------------------------

def render_history(all_data: dict[str, list[dict]]) -> None:
    st.markdown(
        f'<h3 style="color:{_UNICAJA_PURPLE};font-weight:700;">Game history</h3>',
        unsafe_allow_html=True,
    )

    if not all_data:
        st.info("No historical data yet.")
        return

    # Collect real player names and all valid game dates.
    all_names: set[str] = set()
    all_game_dates: set[str] = set()
    for records in all_data.values():
        for r in records:
            name = r.get("player_name", "")
            if name and _is_real_name(name):
                all_names.add(name)
            gd = str(r.get("game_date", ""))
            if gd and gd not in ("", "—", "N/A"):
                all_game_dates.add(gd[:10])

    _ANY_PLAYER = "— All players —"
    _ANY_DATE   = "— All dates —"

    col_player, col_date, _ = st.columns([1, 1, 2])
    with col_player:
        selected_player = st.selectbox(
            "Filter by player",
            options=[_ANY_PLAYER] + sorted(all_names),
            key="history_player",
        )
    with col_date:
        selected_date = st.selectbox(
            "Filter by date",
            options=[_ANY_DATE] + sorted(all_game_dates, reverse=True),
            key="history_date",
        )

    filter_player = selected_player if selected_player != _ANY_PLAYER else None
    filter_date   = selected_date   if selected_date   != _ANY_DATE   else None

    if filter_player is None and filter_date is None:
        st.info("Select a player or a date to browse game records.")
        return

    selected_canonical = _canonical_name(filter_player) if filter_player else None

    seen: set[tuple] = set()
    game_rows: list[dict] = []

    for run_date in sorted(all_data.keys()):
        for rec in all_data[run_date]:
            # Exclude records with no game_date — unverifiable.
            gd = str(rec.get("game_date", ""))
            if not gd or gd in ("", "—", "N/A"):
                continue
            # Date filter
            if filter_date and gd[:10] != filter_date:
                continue
            # Player filter
            name = rec.get("player_name", "")
            if filter_player:
                if not name or _canonical_name(name) != selected_canonical:
                    continue
            elif not name or not _is_real_name(name):
                continue
            key = (rec.get("player_name", ""), rec.get("competition", ""), gd[:10])
            if key in seen:
                continue
            seen.add(key)
            game_rows.append(_build_row(rec))

    if not game_rows:
        label = filter_player or filter_date
        st.info(f"No game records found for {label}.")
        return

    df = pd.DataFrame(game_rows)
    if "Game Date" in df.columns:
        df = df.sort_values("Game Date", ascending=False)

    if filter_player and filter_date:
        caption = f"<strong>{len(game_rows)}</strong> game(s) for <strong>{filter_player}</strong> on <strong>{filter_date}</strong>"
    elif filter_player:
        caption = f"<strong>{len(game_rows)}</strong> game(s) collected for <strong>{filter_player}</strong>"
    else:
        caption = f"<strong>{len(game_rows)}</strong> game(s) played on <strong>{filter_date}</strong>"

    st.markdown(
        f'<p style="font-size:12px;color:{_UNICAJA_PURPLE};">{caption}</p>',
        unsafe_allow_html=True,
    )
    height = min(_HEADER_HEIGHT_PX + len(game_rows) * _ROW_HEIGHT_PX + 4, 500)
    st.dataframe(
        df.style.apply(_style_table, stripe="rgba(107,47,160,0.08)", axis=None),
        use_container_width=True,
        hide_index=True,
        height=height,
    )


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

_inject_css()
_render_header()

dates = get_all_dates()
if not dates:
    st.warning("No data yet. Run `python main.py` to fetch stats.")
    st.stop()

_, latest_records = _load_latest()
all_data = _load_all()

render_latest(latest_records)

st.divider()

render_history(all_data)
