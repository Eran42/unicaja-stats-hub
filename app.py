"""
Unicaja Baloncesto Stats Hub — Streamlit web app.

Main table   : only players who played a confirmed game in the last 24 h.
History table: select a player → every game we've ever collected, one row each.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

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
# Registry team lookup — populated once at startup
# ---------------------------------------------------------------------------

def _load_team_lookup() -> dict[str, str]:
    """Return {canonical_player_name: team} from the registry."""
    path = os.path.join(os.path.dirname(__file__), "data", "players", "registry.json")
    try:
        with open(path, encoding="utf-8") as f:
            return {p["name"]: p.get("team", "") for p in json.load(f)}
    except Exception:
        return {}

_TEAM_LOOKUP: dict[str, str] = _load_team_lookup()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STAT_COLS = [
    "min", "pts",
    "t2m", "t2a", "t2_pct",
    "t3m", "t3a", "t3_pct",
    "ftm", "fta", "ft_pct",
    "reb_off", "reb_def", "reb",
    "ast", "stl", "tov", "blk", "blk_against", "fouls", "fouls_received", "plus_minus", "val",
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
    "blk":         "BLK",  "blk_against": "BLK-A",
    "fouls":       "F",    "fouls_received": "FR",
    "plus_minus":  "+/-",
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


_PCT_FIELDS = {"t2_pct", "t3_pct", "ft_pct"}


def _make_stat_formatter(fmt: str):
    """Return a callable that applies *fmt* to numbers but passes strings through.

    Pre-formatted strings (used by the average row) are returned unchanged so
    that the average row can display one decimal while game rows stay as integers.
    """
    def _fmt(val):
        if isinstance(val, str):
            return val          # already formatted by _build_avg_row
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "N/A"
        try:
            return fmt.format(val)
        except (TypeError, ValueError):
            return str(val)
    return _fmt


# Styler format spec per display-label — keeps sorting numeric, display pretty
_STAT_FORMAT: dict[str, object] = {
    _COL_LABELS[f]: _make_stat_formatter("{:.1f}" if f in _PCT_FIELDS else "{:.0f}")
    for f in _STAT_COLS
}


def _to_num(val: object) -> float | None:
    """Convert a raw stat value to float, or None if missing/unparseable."""
    if val is None or val == "":
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _build_row(record: dict) -> dict:
    row = {}
    for field in _DISPLAY_COLS:
        label = _COL_LABELS.get(field, field)
        if field in _STAT_COLS:
            row[label] = _to_num(record.get(field))
        elif field == "team":
            team = record.get("team") or ""
            if not team:
                team = _TEAM_LOOKUP.get(record.get("player_name", ""), "")
            row[label] = team or "N/A"
        else:
            row[label] = record.get(field) or "N/A"

    # When attempts = 0, percentage is 0.0 (not N/A — the shot was taken 0 times)
    for attempts_field, pct_field in (("t2a", "t2_pct"), ("t3a", "t3_pct"), ("fta", "ft_pct")):
        attempts_label = _COL_LABELS[attempts_field]
        pct_label      = _COL_LABELS[pct_field]
        if row.get(attempts_label) == 0.0 and row.get(pct_label) is None:
            row[pct_label] = 0.0

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

_ROW_HEIGHT_PX    = 35
_HEADER_HEIGHT_PX = 50   # AG Grid alpine renders ~49px; set explicitly via headerHeight
_GRID_PAD_PX      = 20   # extra pixels for AG Grid borders + horizontal scrollbar

_AGGRID_CSS = {
    ".ag-header-cell-text": {"font-size": "12px !important"},
    ".ag-cell":             {"font-size": "12px !important", "padding-left": "6px !important", "padding-right": "6px !important"},
    ".ag-header-cell":      {"padding-left": "4px !important", "padding-right": "4px !important"},
    ".ag-header-cell-filter-button": {"display": "none !important"},
    ".ag-header-cell-menu-button":   {"display": "none !important"},
}

# ---------------------------------------------------------------------------
# Column config
# ---------------------------------------------------------------------------

_TEXT_WIDTHS: dict[str, int] = {
    "Player":      150,
    "Team":         95,
    "Competition":  95,
    "Game Date":   102,
    "Opponent":    150,
    "Result":       84,
}

# Stat columns: widths sized for 12px font with 6px cell padding each side.
# Minimum needed: content_width + 12px padding.  "100.0" ≈ 35px → 47px min.
_STAT_WIDTHS: dict[str, int] = {
    "MIN": 48, "PTS": 44,
    "T2M": 44, "T2A": 44, "T2%": 52,
    "T3M": 44, "T3A": 44, "T3%": 52,
    "FTM": 44, "FTA": 44, "FT%": 52,
    "RO":  42, "RD":  42, "RT":  44,
    "AST": 44, "STL": 44, "TOV": 44,
    "BLK": 44, "BLK-A": 56,
    "F":   38, "FR":   38,
    "+/-": 48, "VAL":  48,
}


def _col_config() -> dict:
    cfg = {}
    for label, w in _TEXT_WIDTHS.items():
        cfg[label] = st.column_config.TextColumn(label, width=w)
    for label, w in _STAT_WIDTHS.items():
        cfg[label] = st.column_config.NumberColumn(label, width=w)
    return cfg


# Columns that open a new group — get a thick left border separator
_GROUP_DIVIDERS = {"T2M", "T3M", "FTM", "RO", "BLK", "F"}

# Column-group tints: (even-row bg, odd-row bg)
_COL_GROUP_COLORS: dict[str, tuple[str, str]] = {
    "T2M":   ("rgba(37,99,235,0.07)",   "rgba(37,99,235,0.15)"),
    "T2A":   ("rgba(37,99,235,0.07)",   "rgba(37,99,235,0.15)"),
    "T2%":   ("rgba(37,99,235,0.07)",   "rgba(37,99,235,0.15)"),
    "T3M":   ("rgba(234,88,12,0.07)",   "rgba(234,88,12,0.15)"),
    "T3A":   ("rgba(234,88,12,0.07)",   "rgba(234,88,12,0.15)"),
    "T3%":   ("rgba(234,88,12,0.07)",   "rgba(234,88,12,0.15)"),
    "FTM":   ("rgba(202,138,4,0.07)",   "rgba(202,138,4,0.15)"),
    "FTA":   ("rgba(202,138,4,0.07)",   "rgba(202,138,4,0.15)"),
    "FT%":   ("rgba(202,138,4,0.07)",   "rgba(202,138,4,0.15)"),
    "RO":    ("rgba(13,148,136,0.07)",  "rgba(13,148,136,0.15)"),
    "RD":    ("rgba(13,148,136,0.07)",  "rgba(13,148,136,0.15)"),
    "RT":    ("rgba(13,148,136,0.07)",  "rgba(13,148,136,0.15)"),
    "BLK":   ("rgba(99,102,241,0.07)",  "rgba(99,102,241,0.15)"),
    "BLK-A": ("rgba(99,102,241,0.07)",  "rgba(99,102,241,0.15)"),
    "F":     ("rgba(239,68,68,0.07)",   "rgba(239,68,68,0.15)"),
    "FR":    ("rgba(239,68,68,0.07)",   "rgba(239,68,68,0.15)"),
}


def _style_table(df: pd.DataFrame, stripe: str, avg_index: int | None = None) -> pd.DataFrame:
    """
    Return a same-shape DataFrame of CSS strings.
    Grouped columns get a fixed tint (two shades for alternating rows).
    Ungrouped columns get the standard row stripe on odd rows.
    The avg_index row (if given) gets a bold summary style.
    """
    out = pd.DataFrame("", index=df.index, columns=df.columns)
    for i in df.index:
        if i == avg_index:
            for col in df.columns:
                parts = [
                    "background-color: rgba(80,80,80,0.12)",
                    "font-weight: 700",
                    "border-top: 2px solid rgba(80,80,80,0.40)",
                ]
                if col in _GROUP_DIVIDERS:
                    parts.append("border-left: 2px solid rgba(80,80,80,0.30)")
                out.loc[i, col] = "; ".join(parts)
            continue
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


# Pairs: (made_label, attempts_label, pct_label) for true-shooting-% computation
_PCT_TRIPLES = [
    (_COL_LABELS["t2m"], _COL_LABELS["t2a"], _COL_LABELS["t2_pct"]),
    (_COL_LABELS["t3m"], _COL_LABELS["t3a"], _COL_LABELS["t3_pct"]),
    (_COL_LABELS["ftm"], _COL_LABELS["fta"], _COL_LABELS["ft_pct"]),
]


def _build_aggrid(df: pd.DataFrame, stripe: str, avg_row: dict | None = None) -> dict:
    """Build AgGrid options. stripe sets odd-row background for ungrouped columns."""
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        resizable=True, sortable=True, filter=False,
        suppressMenu=True,
        sortingOrder=["desc", "asc", None],
    )

    all_widths = {**_TEXT_WIDTHS, **_STAT_WIDTHS}
    stat_labels = {_COL_LABELS[f] for f in _STAT_COLS}
    pct_labels  = {_COL_LABELS[f] for f in _PCT_FIELDS}
    avg_bg      = "rgba(80,80,80,0.12)"
    avg_border  = "2px solid rgba(80,80,80,0.40)"
    grp_border  = "2px solid rgba(80,80,80,0.30)"

    for col in df.columns:
        w         = all_widths.get(col, 42)
        is_div    = col in _GROUP_DIVIDERS
        is_stat   = col in stat_labels
        is_pct    = col in pct_labels

        if col in _COL_GROUP_COLORS:
            even_bg, odd_bg = _COL_GROUP_COLORS[col]
        else:
            even_bg, odd_bg = "transparent", stripe

        bl = grp_border if is_div else "none"

        # Value formatter: pass strings through, format numbers
        if is_pct:
            vfmt = JsCode("""function(p){
                if(p.node.rowPinned==='bottom') return p.value==null?'N/A':String(p.value);
                if(p.value==null) return 'N/A';
                return typeof p.value==='string'?p.value:p.value.toFixed(1);
            }""")
        elif is_stat:
            vfmt = JsCode("""function(p){
                if(p.node.rowPinned==='bottom') return p.value==null?'N/A':String(p.value);
                if(p.value==null) return 'N/A';
                return typeof p.value==='string'?p.value:Math.round(p.value).toString();
            }""")
        else:
            vfmt = None

        cell_style = JsCode(f"""function(p){{
            if(p.node.rowPinned==='bottom'){{
                return{{fontWeight:'700',backgroundColor:'{avg_bg}',
                        borderTop:'{avg_border}',borderLeft:'{bl}'}};
            }}
            var odd=p.node.rowIndex%2!==0;
            return{{backgroundColor:odd?'{odd_bg}':'{even_bg}',borderLeft:'{bl}'}};
        }}""")

        kwargs = dict(width=w, minWidth=w, cellStyle=cell_style, resizable=True)
        if vfmt is not None:
            kwargs["valueFormatter"] = vfmt
        gb.configure_column(col, **kwargs)

    gb.configure_grid_options(
        pinnedBottomRowData=[avg_row] if avg_row is not None else [],
        rowHeight=_ROW_HEIGHT_PX,
        headerHeight=_HEADER_HEIGHT_PX,
        suppressMovableColumns=True,
        suppressHeaderMenuButton=True,
    )
    return gb.build()


def _build_avg_row(df: pd.DataFrame, n_games: int) -> dict:
    """Return a summary row with values pre-formatted to 1 decimal as strings.

    Using strings lets the Styler pass them through unchanged while game rows
    continue to render as integers (via the numeric format in _STAT_FORMAT).
    Percentages are computed as sum(made)/sum(attempts)*100, not mean(pct).
    """
    row: dict = {}
    stat_labels = {_COL_LABELS[f] for f in _STAT_COLS}
    pct_labels  = {triple[2] for triple in _PCT_TRIPLES}

    for col in df.columns:
        if col not in stat_labels:
            row[col] = ""
        elif col in pct_labels:
            row[col] = "N/A"  # filled below from made/attempts
        else:
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            row[col] = f"{series.mean():.1f}" if len(series) else "N/A"

    # True shooting percentages: sum(made) / sum(attempts) * 100
    for made_lbl, att_lbl, pct_lbl in _PCT_TRIPLES:
        made_s = pd.to_numeric(df[made_lbl], errors="coerce").dropna()
        att_s  = pd.to_numeric(df[att_lbl],  errors="coerce").dropna()
        total_att = att_s.sum()
        row[pct_lbl] = (
            f"{made_s.sum() / total_att * 100:.1f}" if total_att > 0 else "N/A"
        )

    # Label the row
    row[_COL_LABELS["player_name"]] = f"Average ({n_games} games)"
    return row



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
    height = _HEADER_HEIGHT_PX + len(played_rows) * _ROW_HEIGHT_PX + _GRID_PAD_PX
    st.caption(f"🟢 **{len(played_rows)}** game(s) in the last 24 h")
    AgGrid(
        df,
        gridOptions=_build_aggrid(df, stripe="rgba(0,102,51,0.08)"),
        height=height,
        use_container_width=True,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        update_mode="NO_UPDATE",
        theme="alpine",
        custom_css=_AGGRID_CSS,
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
        df = df.sort_values("Game Date", ascending=False).reset_index(drop=True)

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

    # Avg row only makes sense when browsing a single player's full history
    show_avg  = filter_player is not None and filter_date is None
    avg_row   = _build_avg_row(df, len(game_rows)) if show_avg else None
    grid_opts = _build_aggrid(df, stripe="rgba(107,47,160,0.08)", avg_row=avg_row)

    pinned_rows = 1 if show_avg else 0
    grid_height = min(
        _HEADER_HEIGHT_PX + len(df) * _ROW_HEIGHT_PX + pinned_rows * _ROW_HEIGHT_PX + _GRID_PAD_PX,
        500 + _ROW_HEIGHT_PX,
    )
    AgGrid(
        df,
        gridOptions=grid_opts,
        height=grid_height,
        use_container_width=True,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        update_mode="NO_UPDATE",
        theme="alpine",
        custom_css={
            # Reduce font to 12px
            ".ag-header-cell-text": {"font-size": "12px !important"},
            ".ag-cell":             {"font-size": "12px !important", "padding-left": "6px !important", "padding-right": "6px !important"},
            # Alpine default is 16px per side in headers — reduced to 4px
            ".ag-header-cell":      {"padding-left": "4px !important", "padding-right": "4px !important"},
            # Hide unused placeholder buttons that consume column width
            ".ag-header-cell-filter-button": {"display": "none !important"},
            ".ag-header-cell-menu-button":   {"display": "none !important"},
        },
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
