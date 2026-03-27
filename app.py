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
from streamlit_autorefresh import st_autorefresh

try:
    import folium
    from streamlit_folium import st_folium
    _FOLIUM_OK = True
except ImportError:
    _FOLIUM_OK = False

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


def _section_heading(text: str) -> str:
    """Consistent h3 style: dark green text, purple left-border accent."""
    return (
        f'<h3 style="'
        f"color:{_UNICAJA_GREEN_DARK};"
        f"font-weight:700;"
        f"margin:4px 0 8px 0;"
        f"padding-left:12px;"
        f"border-left:4px solid {_UNICAJA_PURPLE};"
        f'">{text}</h3>'
    )


def _render_header(run_date: str = "") -> None:
    date_html = ""
    if run_date:
        try:
            from datetime import datetime as _dt
            dt = _dt.strptime(run_date, "%Y-%m-%d")
            label = dt.strftime("%d %b %Y").lstrip("0")
        except Exception:
            label = run_date
        date_html = (
            f'<div style="font-size:11px;font-family:sans-serif;'
            f'color:{_UNICAJA_PURPLE};margin-top:3px;font-weight:500;">'
            f'📅 Data as of {label}</div>'
        )

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
                <div style="font-size: 13px; opacity: 0.85; font-family: sans-serif;">
                    Latest game box scores for former Unicaja Baloncesto players
                </div>
                {date_html}
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

def _load_registry() -> list[dict]:
    path = os.path.join(os.path.dirname(__file__), "data", "players", "registry.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _load_player_status() -> dict:
    path = os.path.join(os.path.dirname(__file__), "data", "players", "status.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

_TEAM_LOOKUP:    dict[str, str]    = _load_team_lookup()
_REGISTRY:       list[dict]        = _load_registry()
_PLAYER_STATUS:  dict              = _load_player_status()


# ---------------------------------------------------------------------------
# Map — city coordinates and region views
# ---------------------------------------------------------------------------

# (lat, lon) for each team in the registry.
# Teams sharing a city are offset by ~0.05° so their pins don't overlap.
_TEAM_COORDS: dict[str, tuple[float, float]] = {
    # Spain
    "FC Barcelona":            (41.381,   2.173),
    "Valencia Basket":         (39.470,  -0.376),
    "Lenovo Tenerife":         (28.464, -16.252),
    "MoraBanc Andorra":        (42.506,   1.522),
    "San Pablo Burgos":        (42.344,  -3.697),
    "Hiopos Lleida":           (41.618,   0.620),
    "Río Breogán":             (43.362,  -8.412),
    "CB Estudiantes":          (40.417,  -3.704),
    # Serbia — Partizan and Crvena zvezda share Belgrade; offset slightly
    "Partizan Mozzart Bet":    (44.790,  20.452),
    "Crvena zvezda":           (44.820,  20.480),
    # Greece — Panathinaikos and AEK share Athens; offset slightly
    "Panathinaikos":           (37.968,  23.718),
    "AEK BC":                  (37.999,  23.750),
    # France / Monaco
    "AS Monaco":               (43.738,   7.424),
    # Italy
    "Umana Reyer Venezia":     (45.441,  12.316),
    "Pallacanestro Trieste":   (45.650,  13.777),
    # Montenegro
    "Budućnost VOLI":          (42.430,  19.259),
    # Bosnia
    "Igokea m:tel":            (44.454,  17.281),
    # Poland
    "WKS Śląsk Wrocław":      (51.108,  17.038),
    # UAE
    "Dubai Basketball":        (25.205,  55.271),
    # USA
    "Sacramento Kings":        (38.580, -121.499),
    "Gonzaga Bulldogs":        (47.667, -117.402),
}


# ---------------------------------------------------------------------------
# Map — data helpers
# ---------------------------------------------------------------------------

def _best_record_per_player(all_data: dict[str, list[dict]]) -> dict[str, dict]:
    """Return the single most recent valid game record per player name."""
    best: dict[str, dict] = {}
    for records in all_data.values():
        for rec in records:
            name = rec.get("player_name", "")
            if not name or not _is_real_name(name):
                continue
            gd = str(rec.get("game_date", ""))
            if not gd or gd in ("", "—", "N/A"):
                continue
            if name not in best or gd > str(best[name].get("game_date", "")):
                best[name] = rec
    return best


def _build_map_data(all_data: dict[str, list[dict]]) -> dict[str, dict]:
    """
    Group players by team. For each team return:
      coords     : (lat, lon)
      any_recent : True if any player played in last 24 h
      players    : list of per-player dicts with stats + status
    """
    best = _best_record_per_player(all_data)
    teams: dict[str, dict] = {}

    for player in _REGISTRY:
        name = player["name"]
        team = player.get("team", "")
        if not team or team not in _TEAM_COORDS:
            continue

        rec          = best.get(name)
        status_info  = _PLAYER_STATUS.get(name, {})
        recent       = bool(rec and _game_is_within_24h(str(rec.get("game_date", ""))))

        entry: dict = {
            "name":        name,
            "team":        team,
            "status":      status_info.get("status", "active"),
            "status_note": status_info.get("note", ""),
            "recent":      recent,
        }
        if rec:
            entry.update({
                "competition": rec.get("competition", ""),
                "game_date":   str(rec.get("game_date", "")),
                "result":      rec.get("result") or "",
                "pts":         rec.get("pts"),
                "reb":         rec.get("reb"),
                "ast":         rec.get("ast"),
                "plus_minus":  rec.get("plus_minus"),
                "val":         rec.get("val"),
            })
        else:
            entry.update({
                "competition": "",
                "game_date": "",
                "result": "",
                "pts": None, "reb": None, "ast": None, "plus_minus": None, "val": None,
            })

        if team not in teams:
            teams[team] = {
                "coords":     _TEAM_COORDS[team],
                "any_recent": False,
                "players":    [],
            }
        teams[team]["players"].append(entry)
        if recent:
            teams[team]["any_recent"] = True

    return teams


def _stat_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        return str(int(float(val)))
    except Exception:
        return "—"


def _player_card_html(p: dict) -> str:
    name  = p["name"]
    team  = p["team"]
    comp  = p.get("competition", "")
    gd    = p.get("game_date", "")
    res   = p.get("result", "") or ""
    note  = p.get("status_note", "")

    recent = p.get("recent", False)

    if gd:
        game_label = "Last game: " if not recent else ""
        meta  = f"{game_label}{comp} · {gd}" + (f" · {res}" if res else "")
        stats = (
            "<table style='width:100%;border-collapse:collapse;margin-top:5px;'>"
            "<tr style='background:#f5f5f5;text-align:center;'>"
            "<th style='padding:3px 6px;font-size:10px;font-weight:600;color:#555;'>PTS</th>"
            "<th style='padding:3px 6px;font-size:10px;font-weight:600;color:#555;'>REB</th>"
            "<th style='padding:3px 6px;font-size:10px;font-weight:600;color:#555;'>AST</th>"
            "<th style='padding:3px 6px;font-size:10px;font-weight:600;color:#555;'>+/-</th>"
            "<th style='padding:3px 6px;font-size:10px;font-weight:600;color:#6B2FA0;'>VAL</th>"
            "</tr>"
            "<tr style='text-align:center;'>"
            f"<td style='padding:4px 6px;font-size:14px;font-weight:700;'>{_stat_str(p.get('pts'))}</td>"
            f"<td style='padding:4px 6px;font-size:14px;font-weight:700;'>{_stat_str(p.get('reb'))}</td>"
            f"<td style='padding:4px 6px;font-size:14px;font-weight:700;'>{_stat_str(p.get('ast'))}</td>"
            f"<td style='padding:4px 6px;font-size:14px;font-weight:700;'>{_stat_str(p.get('plus_minus'))}</td>"
            f"<td style='padding:4px 6px;font-size:14px;font-weight:700;color:#6B2FA0;'>{_stat_str(p.get('val'))}</td>"
            "</tr>"
            "</table>"
        )
        body = f"<div style='font-size:10px;color:#666;margin:2px 0 3px;'>{meta}</div>{stats}"
    else:
        reason = note if note else "No recent game data"
        body   = f"<div style='font-size:10px;color:#999;margin-top:4px;'>⚠ {reason}</div>"

    return (
        "<div style='font-family:sans-serif;min-width:200px;"
        "margin-bottom:7px;padding-bottom:7px;border-bottom:1px solid #e8e8e8;'>"
        f"<div style='font-size:13px;font-weight:700;color:#1a1a1a;'>🏀 {name}</div>"
        f"<div style='font-size:10px;font-weight:600;color:#006633;margin-bottom:1px;'>{team}</div>"
        f"{body}"
        "</div>"
    )


def render_map(all_data: dict[str, list[dict]]) -> None:
    if not _FOLIUM_OK:
        st.info("Install `folium` and `streamlit-folium` to enable the map.")
        return

    st.markdown(_section_heading("🌍 Where are they now?"), unsafe_allow_html=True)

    st.markdown(
        "<div style='font-size:12px;margin-bottom:4px;'>"
        "<span style='color:#006633;font-size:16px;'>●</span> played last 24 h &nbsp;&nbsp;"
        "<span style='color:#999;font-size:16px;'>●</span> no recent game"
        "</div>",
        unsafe_allow_html=True,
    )

    map_data = _build_map_data(all_data)

    # Fit the initial view to Europe (where 19/21 teams are).
    # Sacramento Kings and Gonzaga Bulldogs are rendered but reachable by panning.
    europe_coords = [
        data["coords"] for data in map_data.values() if data["coords"][1] > -20
    ]
    fit_coords = europe_coords if europe_coords else [data["coords"] for data in map_data.values()]

    m = folium.Map(
        location=[0, 0],
        zoom_start=2,
        tiles="CartoDB positron",
        control_scale=False,
        max_bounds=True,
        min_zoom=2,
    )

    for team, data in map_data.items():
        clat, clon = data["coords"]
        color      = "#006633" if data["any_recent"] else "#999999"
        last_names = ", ".join(p["name"].split()[-1] for p in data["players"])
        tooltip    = f"<b style='font-size:12px;'>{team}</b><br><span style='font-size:11px;'>{last_names}</span>"
        popup_html = "".join(_player_card_html(p) for p in data["players"])

        is_active = data["any_recent"]
        folium.CircleMarker(
            location=[clat, clon],
            radius=12 if is_active else 7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.9 if is_active else 0.55,
            weight=2 if is_active else 1.5,
            tooltip=folium.Tooltip(tooltip),
            popup=folium.Popup(popup_html, max_width=260),
        ).add_to(m)

    m.fit_bounds(fit_coords, padding=[35, 35], max_zoom=6)

    # returning map_data triggers no Streamlit rerun so popups stay open
    st_folium(m, use_container_width=True, height=420)


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

# Column groups for the AgGrid header — label → (group name, [col labels in order])
_COL_GROUPS_DEF: list[tuple[str, list[str]]] = [
    ("2PT",   [_COL_LABELS["t2m"],      _COL_LABELS["t2a"],          _COL_LABELS["t2_pct"]]),
    ("3PT",   [_COL_LABELS["t3m"],      _COL_LABELS["t3a"],          _COL_LABELS["t3_pct"]]),
    ("FT",    [_COL_LABELS["ftm"],      _COL_LABELS["fta"],          _COL_LABELS["ft_pct"]]),
    ("REB",   [_COL_LABELS["reb_off"],  _COL_LABELS["reb_def"],      _COL_LABELS["reb"]]),
    ("BLK",   [_COL_LABELS["blk"],      _COL_LABELS["blk_against"]]),
    ("FOULS", [_COL_LABELS["fouls"],    _COL_LABELS["fouls_received"]]),
]
_GROUPED_COLS:    set[str]          = {c for _, cols in _COL_GROUPS_DEF for c in cols}
_GROUP_FIRST_COL: dict[str, tuple]  = {cols[0]: (name, cols) for name, cols in _COL_GROUPS_DEF}

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

def _is_dark() -> bool:
    """True when the browser reports prefers-color-scheme: dark (via ?_dark=1)."""
    return st.query_params.get("_dark") == "1"


def _inject_dark_mode_detector() -> None:
    """Inject JS that syncs prefers-color-scheme → URL param → Streamlit rerun."""
    st.markdown(
        """
<script>
(function () {
    var dark  = window.matchMedia('(prefers-color-scheme: dark)').matches ? '1' : '0';
    var url   = new URL(window.location.href);
    if (url.searchParams.get('_dark') !== dark) {
        url.searchParams.set('_dark', dark);
        window.history.replaceState({}, '', url.toString());
        // Dispatching popstate causes Streamlit to pick up the new query param
        // and rerun the script, so the correct AG Grid theme is chosen.
        window.dispatchEvent(new PopStateEvent('popstate', {state: history.state}));
    }
})();
</script>
""",
        unsafe_allow_html=True,
    )


_AGGRID_CSS_BASE = {
    ".ag-header-cell-text": {"font-size": "12px !important"},
    ".ag-cell":             {"font-size": "12px !important", "padding-left": "6px !important", "padding-right": "6px !important"},
    ".ag-header-cell":      {"padding-left": "4px !important", "padding-right": "4px !important"},
    ".ag-header-cell-filter-button": {"display": "none !important"},
    ".ag-header-cell-menu-button":   {"display": "none !important"},
    # Column group headers (2PT / 3PT / FT / REB)
    ".ag-header-group-cell": {
        "font-size": "11px !important",
        "font-weight": "700 !important",
        "color": "#6B2FA0 !important",
        "border-bottom": "2px solid rgba(107,47,160,0.35) !important",
    },
    ".ag-header-group-cell-label": {"justify-content": "center !important"},
}

_AGGRID_CSS_DARK_EXTRA = {
    ".ag-root-wrapper":    {"background-color": "#0e1117 !important", "border-color": "#3d3d3d !important", "color": "#fafafa !important"},
    ".ag-header":          {"background-color": "#262730 !important", "border-bottom-color": "#3d3d3d !important"},
    ".ag-header-cell":     {"background-color": "#262730 !important", "color": "#fafafa !important",
                            "border-color": "#3d3d3d !important",
                            "padding-left": "4px !important", "padding-right": "4px !important"},
    ".ag-header-cell-text": {"font-size": "12px !important", "color": "#fafafa !important"},
    ".ag-row":             {"border-color": "#3d3d3d !important"},
    ".ag-floating-bottom": {"border-top-color": "#555 !important"},
    ".ag-body-viewport":   {"background-color": "#0e1117 !important"},
    ".ag-center-cols-viewport": {"background-color": "#0e1117 !important"},
    ".ag-header-group-cell": {
        "background-color": "#262730 !important",
        "color": "#c4a5e8 !important",
        "border-bottom": "2px solid rgba(196,165,232,0.35) !important",
    },
}


def _aggrid_css() -> dict:
    css = dict(_AGGRID_CSS_BASE)
    if _is_dark():
        css.update(_AGGRID_CSS_DARK_EXTRA)
    return css

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


def _build_aggrid(df: pd.DataFrame, stripe: str, avg_row: dict | None = None, dark: bool = False) -> dict:
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

    # Dark-mode adjustments
    text_color  = "#fafafa" if dark else "inherit"
    avg_bg      = "rgba(120,120,120,0.30)" if dark else "rgba(80,80,80,0.12)"
    avg_border  = "2px solid rgba(180,180,180,0.50)" if dark else "2px solid rgba(80,80,80,0.40)"
    grp_border  = "2px solid rgba(180,180,180,0.30)" if dark else "2px solid rgba(80,80,80,0.30)"
    base_even   = "#0e1117" if dark else "transparent"
    base_odd    = ("#1c1c2e" if dark else stripe) if not stripe.startswith("rgba") else (
                  stripe.replace("0.08", "0.25") if dark else stripe
    )

    for col in df.columns:
        w         = all_widths.get(col, 42)
        is_div    = col in _GROUP_DIVIDERS
        is_stat   = col in stat_labels
        is_pct    = col in pct_labels

        if col in _COL_GROUP_COLORS:
            e_bg, o_bg = _COL_GROUP_COLORS[col]
            if dark:
                # Boost opacity of group tints for dark backgrounds
                e_bg = e_bg.replace("0.07", "0.20")
                o_bg = o_bg.replace("0.15", "0.35")
            even_bg, odd_bg = e_bg, o_bg
        else:
            even_bg, odd_bg = base_even, base_odd

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
                        borderTop:'{avg_border}',borderLeft:'{bl}',color:'{text_color}'}};
            }}
            var odd=p.node.rowIndex%2!==0;
            return{{backgroundColor:odd?'{odd_bg}':'{even_bg}',borderLeft:'{bl}',color:'{text_color}'}};
        }}""")

        kwargs = dict(width=w, cellStyle=cell_style, resizable=True)
        if vfmt is not None:
            kwargs["valueFormatter"] = vfmt
        gb.configure_column(col, **kwargs)

    gb.configure_grid_options(
        pinnedBottomRowData=[avg_row] if avg_row is not None else [],
        rowHeight=_ROW_HEIGHT_PX,
        headerHeight=_HEADER_HEIGHT_PX,
        groupHeaderHeight=28,
        suppressMovableColumns=True,
        suppressHeaderMenuButton=True,
        onGridSizeChanged=JsCode("function(p){p.api.sizeColumnsToFit();}"),
    )
    go = gb.build()

    # Wrap shooting / rebound columns into visible group headers (2PT / 3PT / FT / REB).
    by_field: dict[str, dict] = {c["field"]: c for c in go["columnDefs"]}
    new_defs: list[dict] = []
    seen: set[str] = set()
    for col_def in go["columnDefs"]:
        field = col_def.get("field", "")
        if field in seen:
            continue
        if field in _GROUP_FIRST_COL:
            grp_name, grp_fields = _GROUP_FIRST_COL[field]
            new_defs.append({
                "headerName": grp_name,
                "marryChildren": True,
                "children": [by_field[f] for f in grp_fields if f in by_field],
            })
            seen.update(grp_fields)
        elif field not in _GROUPED_COLS:
            new_defs.append(col_def)
            seen.add(field)
    go["columnDefs"] = new_defs
    return go


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



_STATUS_ICON = {
    "injured":   "🤕",
    "rest":      "😴",
    "suspended": "🚫",
    "unknown":   "❓",
}


def _render_absent_players(rows: list[dict]) -> None:
    """Render a compact table of players who didn't play, with their absence reason."""
    count = len(rows)
    lines = []
    for r in rows:
        icon = _STATUS_ICON.get(r["status"], "❓")
        lines.append(
            f"<tr>"
            f"<td style='padding:5px 10px 5px 0;font-weight:600;white-space:nowrap;'>{icon} {r['player_name']}</td>"
            f"<td style='padding:5px 10px;color:#555;white-space:nowrap;'>{r['team']}</td>"
            f"<td style='padding:5px 10px;color:#666;font-size:12px;'>{r['competition']}</td>"
            f"<td style='padding:5px 0 5px 10px;color:#888;font-size:12px;font-style:italic;'>{r['note']}</td>"
            f"</tr>"
        )
    table_html = (
        "<table style='border-collapse:collapse;width:100%;margin-top:6px;'>"
        "<thead><tr>"
        "<th style='text-align:left;font-size:11px;color:#999;padding:3px 10px 3px 0;font-weight:500;'>Player</th>"
        "<th style='text-align:left;font-size:11px;color:#999;padding:3px 10px;font-weight:500;'>Team</th>"
        "<th style='text-align:left;font-size:11px;color:#999;padding:3px 10px;font-weight:500;'>Competition</th>"
        "<th style='text-align:left;font-size:11px;color:#999;padding:3px 0 3px 10px;font-weight:500;'>Note</th>"
        "</tr></thead>"
        "<tbody>" + "".join(lines) + "</tbody>"
        "</table>"
    )
    heading = (
        f'<div style="font-size:14px;font-weight:700;color:{_UNICAJA_PURPLE};margin-bottom:0;">'
        f'Did not play'
        f'<span style="margin-left:9px;background:{_UNICAJA_PURPLE};color:white;'
        f'padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;">'
        f'{count}</span></div>'
    )
    container = (
        f'<div style="'
        f'margin-top:16px;'
        f'padding:10px 14px 14px 14px;'
        f'background:rgba(107,47,160,0.05);'
        f'border-left:3px solid rgba(107,47,160,0.5);'
        f'border-radius:0 6px 6px 0;">'
        f'{heading}{table_html}</div>'
    )
    st.markdown(container, unsafe_allow_html=True)


def _absent_player_rows(latest_records: list[dict]) -> list[dict]:
    """
    Return rows for active players whose team played in the last 24 h but
    who have no game record themselves.

    'Team played' is determined from the actual records: if any tracked player
    on the same team has a game_date within the last 24 h, we know the team
    had a fixture — so any teammate missing from the records is a DNP.
    """
    # Teams that had at least one game in the last 24 h
    teams_with_recent: set[str] = set()
    names_with_recent: set[str] = set()
    for r in latest_records:
        if _game_is_within_24h(str(r.get("game_date", ""))):
            names_with_recent.add(r["player_name"])
            team = r.get("team") or _TEAM_LOOKUP.get(r.get("player_name", ""), "")
            if team:
                teams_with_recent.add(team)

    if not teams_with_recent:
        return []

    rows = []
    for player in _REGISTRY:
        name = player["name"]
        team = player.get("team", "")

        if not player.get("active", True):
            continue
        if name in names_with_recent:
            continue
        # Only show if the player's team actually played today
        if team not in teams_with_recent:
            continue

        info   = _PLAYER_STATUS.get(name, {})
        note   = info.get("note", "No data available")
        status = info.get("status", "unknown")

        competitions = ", ".join(
            s["competition"] for s in player.get("sources", [])
            if s.get("competition")
        )
        rows.append({
            "player_name": name,
            "team":        team,
            "competition": competitions,
            "note":        note,
            "status":      status,
        })

    return rows


def render_latest(records: list[dict]) -> None:
    cutoff_label = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
    st.markdown(_section_heading(f"Last 24 hours — since {cutoff_label}"), unsafe_allow_html=True)

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
    absent_rows: list[dict] = _absent_player_rows(records)

    if not played_rows and not absent_rows:
        st.info("No games in the last 24 hours.")
        return

    if played_rows:
        df = pd.DataFrame(played_rows)
        height = _HEADER_HEIGHT_PX + len(played_rows) * _ROW_HEIGHT_PX + _GRID_PAD_PX
        dark = _is_dark()
        count = len(played_rows)
        st.markdown(
            f'<div style="margin:4px 0 6px;">'
            f'<span style="background:{_UNICAJA_GREEN};color:white;padding:2px 10px;'
            f'border-radius:10px;font-size:12px;font-weight:700;">'
            f'{"1 game" if count == 1 else f"{count} games"}'
            f'</span></div>',
            unsafe_allow_html=True,
        )
        AgGrid(
            df,
            gridOptions=_build_aggrid(df, stripe="rgba(0,102,51,0.08)", dark=dark),
            height=height,
            use_container_width=True,
            allow_unsafe_jscode=True,
            fit_columns_on_grid_load=True,
            update_mode="NO_UPDATE",
            theme="alpine",
            custom_css=_aggrid_css(),
        )

    if absent_rows:
        _render_absent_players(absent_rows)


# ---------------------------------------------------------------------------
# Historical table
# ---------------------------------------------------------------------------

def render_history(all_data: dict[str, list[dict]]) -> None:
    st.markdown(_section_heading("Game history"), unsafe_allow_html=True)

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

    st.markdown(
        f'<p style="font-size:12px;font-weight:600;color:{_UNICAJA_PURPLE};'
        f'margin-bottom:2px;margin-top:2px;">Browse records</p>',
        unsafe_allow_html=True,
    )
    sorted_dates = sorted(all_game_dates, reverse=True)
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
            options=[_ANY_DATE] + sorted_dates,
            index=1,
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
    dark      = _is_dark()
    show_avg  = filter_player is not None and filter_date is None
    avg_row   = _build_avg_row(df, len(game_rows)) if show_avg else None
    grid_opts = _build_aggrid(df, stripe="rgba(107,47,160,0.08)", avg_row=avg_row, dark=dark)

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
        custom_css=_aggrid_css(),
    )


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

# Auto-refresh every 5 minutes — aligned with cache TTL so new scraping
# results are picked up promptly without requiring a manual page reload.
st_autorefresh(interval=5 * 60 * 1000, key="data_refresh")

_inject_dark_mode_detector()
_inject_css()

dates = get_all_dates()
_render_header(dates[-1] if dates else "")

if not dates:
    st.warning("No data yet. Run `python main.py` to fetch stats.")
    st.stop()

_, latest_records = _load_latest()
all_data = _load_all()

render_map(all_data)

st.divider()

render_latest(latest_records)

st.divider()

render_history(all_data)
