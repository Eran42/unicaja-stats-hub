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
from streamlit_autorefresh import st_autorefresh

try:
    import folium
    from folium import MacroElement
    from jinja2 import Template as _JinjaTemplate
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
        /* ── Remove Streamlit's default top padding so the header sits flush ── */
        .main .block-container {{
            padding-top: 0.5rem !important;
            padding-bottom: 1rem !important;
        }}

        /* ── Top accent bar ── */
        [data-testid="stHeader"] {{
            background: {_UNICAJA_GREEN};
            height: 4px;
        }}

        /* ── Tighten dividers ── */
        [data-testid="stDivider"] {{
            margin-top: 6px !important;
            margin-bottom: 6px !important;
        }}
        hr {{
            border-color: {_UNICAJA_GREEN_MID} !important;
            border-width: 2px !important;
            margin: 0 !important;
        }}

        /* ── Collapse default vertical margins around markdown blocks ── */
        [data-testid="stMarkdownContainer"] > p {{
            margin-bottom: 4px !important;
        }}

        /* ── Title / headings ── */
        h1 {{
            color: {_UNICAJA_GREEN_DARK} !important;
            font-weight: 800 !important;
            letter-spacing: -0.5px;
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


def _inject_select_keyboard_fix() -> None:
    """
    On touch devices, prevent the virtual keyboard from opening when the user
    taps a Streamlit selectbox.

    st.markdown <script> tags are stripped by React's dangerouslySetInnerHTML,
    so we use st.components.v1.html (a real iframe) which executes scripts, and
    target window.parent.document to reach the Streamlit page DOM — the same
    pattern used by the scroll-to-history injection.

    Two complementary techniques:
    - focusin listener: fires synchronously when the input is focused, sets
      inputmode="none" before the browser can decide to show a keyboard.
    - MutationObserver: pre-patches inputs as React renders new selectboxes,
      so inputmode="none" is already set before the first tap.
    """
    import streamlit.components.v1 as _cv1
    _cv1.html(
        "<script>"
        "(function(){"
        "var d=window.parent.document;"
        "function patch(el){"
        "el.setAttribute('inputmode','none');"
        "}"
        "function patchAll(){"
        "d.querySelectorAll('[data-baseweb=\"select\"] input').forEach(patch);"
        "}"
        "d.addEventListener('focusin',function(e){"
        "if(e.target.closest&&e.target.closest('[data-baseweb=\"select\"]')&&e.target.tagName==='INPUT')"
        "patch(e.target);"
        "},true);"
        "patchAll();"
        "new MutationObserver(patchAll).observe(d.body,{childList:true,subtree:true});"
        "})();"
        "</script>",
        height=0,
    )


def _section_heading(text: str) -> str:
    """Consistent h3 style: dark green text, purple left-border accent."""
    return (
        f'<h3 style="'
        f"color:{_UNICAJA_GREEN_DARK};"
        f"font-weight:700;"
        f"margin:2px 0 6px 0;"
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
            f'<div style="font-size:12px;font-family:sans-serif;'
            f'color:{_UNICAJA_PURPLE};margin-top:4px;font-weight:600;">'
            f'📅 {label}</div>'
        )

    st.markdown(
        f"""
        <div style="
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 14px;
            padding: 4px 0 8px 0;
            border-bottom: 3px solid {_UNICAJA_GREEN};
            margin-bottom: 6px;
        ">
            <div style="
                background: {_UNICAJA_GREEN};
                color: white;
                font-size: clamp(22px, 6vw, 34px);
                font-weight: 900;
                letter-spacing: 2px;
                padding: 8px 18px;
                border-radius: 6px;
                font-family: sans-serif;
                line-height: 1;
                flex-shrink: 0;
            ">UNICAJA</div>
            <div style="min-width: 0;">
                <div style="
                    font-size: clamp(18px, 5vw, 26px);
                    font-weight: 800;
                    font-family: sans-serif;
                    color: {_UNICAJA_GREEN_DARK};
                    line-height: 1.1;
                ">Ex-Players Stats</div>
                <div style="
                    font-size: clamp(11px, 3vw, 13px);
                    opacity: 0.75;
                    font-family: sans-serif;
                    margin-top: 3px;
                ">Latest game box scores for former Unicaja Baloncesto players</div>
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

def _load_team_coords() -> dict[str, tuple[float, float]]:
    """Load team → (lat, lon) from data/players/team_coords.json.

    Stored as JSON arrays [lat, lon] so the roster-tracker agent can add
    new teams without touching app.py.  Falls back to an empty dict so the
    map degrades gracefully when a team has no coordinates yet.
    """
    path = os.path.join(os.path.dirname(__file__), "data", "players", "team_coords.json")
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        return {team: tuple(coords) for team, coords in raw.items()}
    except Exception:
        return {}

def _load_player_photos() -> dict[str, str]:
    path = os.path.join(os.path.dirname(__file__), "data", "players", "photos.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

_TEAM_LOOKUP:    dict[str, str]              = _load_team_lookup()
_REGISTRY:       list[dict]                  = _load_registry()
_PLAYER_STATUS:  dict                        = _load_player_status()
_TEAM_COORDS:    dict[str, tuple[float, float]] = _load_team_coords()
_PLAYER_PHOTOS:  dict[str, str]              = _load_player_photos()


# ---------------------------------------------------------------------------
# Map — city coordinates and region views
# ---------------------------------------------------------------------------


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
    Group players by city (identical coords). Teams in the same city share one
    pin so they are visible without zooming in.

    Returns a dict keyed by "lat,lon" with:
      coords     : (lat, lon)
      any_recent : True if any player played in last 24 h
      players    : list of per-player dicts with stats + status
    """
    best  = _best_record_per_player(all_data)

    # Teams that had at least one tracked player with a game in the last 24 h
    teams_with_recent: set[str] = set()
    for rec in best.values():
        if rec and _game_is_within_24h(str(rec.get("game_date", ""))):
            t = rec.get("team", "") or _TEAM_LOOKUP.get(rec.get("player_name", ""), "")
            if t:
                teams_with_recent.add(t)

    pins: dict[str, dict] = {}

    for player in _REGISTRY:
        name = player["name"]
        team = player.get("team", "")
        if not team or team not in _TEAM_COORDS:
            continue

        coords = _TEAM_COORDS[team]
        pin_key = f"{coords[0]:.4f},{coords[1]:.4f}"

        rec         = best.get(name)
        status_info = _PLAYER_STATUS.get(name, {})
        recent      = bool(rec and _game_is_within_24h(str(rec.get("game_date", ""))))

        entry: dict = {
            "name":             name,
            "team":             team,
            "status":           status_info.get("status", "active"),
            "status_note":      status_info.get("note", ""),
            "recent":           recent,
            # True when the player's team had a game in the last 24 h but this
            # player has no matching record — shown as DNP in the map card.
            "team_played_today": (team in teams_with_recent) and not recent,
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

        if pin_key not in pins:
            pins[pin_key] = {
                "coords":     coords,
                "any_recent": False,
                "players":    [],
            }
        pins[pin_key]["players"].append(entry)
        if recent:
            pins[pin_key]["any_recent"] = True

    return pins


def _stat_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        return str(int(float(val)))
    except Exception:
        return "—"


def _player_card_html(p: dict, nav_lat: float | None = None) -> str:
    name  = p["name"]
    team  = p["team"]
    comp  = p.get("competition", "")
    gd    = p.get("game_date", "")
    res   = p.get("result", "") or ""
    note  = p.get("status_note", "")

    recent = p.get("recent", False)

    # Determine if the player has been absent for >7 days
    absent_days: int | None = None
    if gd:
        try:
            from datetime import date as _date
            last = _date.fromisoformat(gd)
            absent_days = (_date.today() - last).days
        except ValueError:
            pass

    absent = absent_days is not None and absent_days > 7

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
        if absent and note:
            body += (
                f"<div style='margin-top:5px;padding:4px 6px;background:#fff8e6;"
                f"border-left:3px solid #f0a500;border-radius:3px;"
                f"font-size:10px;color:#7a5500;'>⚠ {note}</div>"
            )
    else:
        if p.get("team_played_today"):
            _icon = _STATUS_ICON.get(p.get("status", ""), "⚠️")
            _reason = note if note else "Did not play"
            body = (
                f"<div style='margin-top:4px;padding:4px 7px;"
                f"background:#fff8e6;border-left:3px solid #f0a500;border-radius:3px;"
                f"font-size:10px;color:#7a5500;line-height:1.4;'>"
                f"{_icon} <strong>DNP</strong> — {_reason}"
                f"</div>"
            )
        else:
            reason = note if note else "No recent game data"
            body   = f"<div style='font-size:10px;color:#999;margin-top:4px;'>⚠ {reason}</div>"

    photo_url    = _PLAYER_PHOTOS.get(name, "")
    avatar_border = "#006633" if recent else "#bbbbbb"
    if photo_url:
        # Zoom and position differ by photo source:
        # - cortextech (EuroLeague): full-body jersey shot, face in top ~25% → zoom 3x into top
        # - cdn.nba.com: head-and-shoulders official crop → mild zoom
        # - eurobasket.com: already a close-up headshot → minimal adjustment
        if "cortextech" in photo_url:
            # EuroLeague official: full-body jersey shot, face in top ~25%
            img_style = (
                "width:100%;height:100%;object-fit:cover;object-position:center top;"
                "transform:scale(3.2);transform-origin:center 10%;"
            )
        elif "cdn.nba.com" in photo_url:
            # NBA: jersey shot similar to cortextech but slightly tighter framing
            img_style = (
                "width:100%;height:100%;object-fit:cover;object-position:center top;"
                "transform:scale(2.5);transform-origin:center 12%;"
            )
        else:
            # eurobasket.com: already a close-up portrait headshot
            img_style = (
                "width:100%;height:100%;object-fit:cover;object-position:center 10%;"
            )
        avatar = (
            f"<div style='width:42px;height:42px;border-radius:50%;overflow:hidden;"
            f"border:2px solid {avatar_border};flex-shrink:0;'>"
            f"<img src='{photo_url}' style='{img_style}'>"
            f"</div>"
        )
    else:
        avatar = (
            f"<div style='width:42px;height:42px;border-radius:50%;background:#e8f0eb;"
            f"border:2px solid {avatar_border};display:flex;align-items:center;"
            "justify-content:center;font-size:18px;flex-shrink:0;'>🏀</div>"
        )

    name_color = "#1a1a1a" if recent else "#888888"
    header = (
        "<div style='display:flex;align-items:center;gap:9px;margin-bottom:5px;'>"
        f"{avatar}"
        "<div>"
        f"<div style='font-size:13px;font-weight:700;color:{name_color};line-height:1.2;'>{name}</div>"
        f"<div style='font-size:10px;font-weight:600;color:#006633;'>{team}</div>"
        "</div>"
        "</div>"
    )

    # Green left-border accent for recently-active players; muted for others
    card_style = (
        "font-family:sans-serif;min-width:210px;margin-bottom:7px;"
        "padding-bottom:7px;border-bottom:1px solid #e8e8e8;"
        "cursor:pointer;"
    )
    if recent:
        card_style += "border-left:3px solid #006633;padding-left:8px;"

    # "→ History" link — fires a synthetic Leaflet map click at an encoded
    # latitude so st_folium returns it via last_clicked; Python maps it back
    # to this player's name.  Entirely within the Leaflet iframe, no cross-
    # frame JS needed.
    if nav_lat is not None:
        # window.map is set by streamlit-folium itself after map initialisation.
        nav_js = (
            f"(function(){{"
            f"if(window.map){{"
            f"window.map.closePopup();"
            f"window.map.fire('click',{{latlng:L.latLng({nav_lat:.4f},0)}});"
            f"}}}})()"
        )
        nav_link = (
            "<div onclick=\"" + nav_js + "\" "
            "style='margin-top:6px;padding:8px 10px;border-top:1px solid #eee;"
            "background:#eef7f1;border-radius:4px;"
            "color:#006633;font-size:12px;font-weight:700;cursor:pointer;"
            "text-align:center;user-select:none;display:block;'>"
            "→ View history</div>"
        )
    else:
        nav_link = ""

    return (
        f"<div style='{card_style}'>"
        f"{header}"
        f"{body}"
        f"{nav_link}"
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

    all_coords = [data["coords"] for data in map_data.values()]
    eu_coords  = [c for c in all_coords if c[1] > -20]

    m = folium.Map(
        location=[0, 0],
        zoom_start=2,
        tiles="CartoDB positron",
        control_scale=False,
        max_bounds=True,
        min_zoom=2,
    )

    # Belt-and-suspenders CSS: hide Leaflet tooltips on touch/coarse-pointer
    # devices.  Placed in the document <head> so it's parsed before any markers
    # are added.  The popupopen JS handler in each popup's onerror is the primary
    # fix; this CSS is a fallback for the first render.
    m.get_root().header.add_child(folium.Element(
        "<style>"
        "@media (pointer: coarse) {"
        "  .leaflet-tooltip { display: none !important; }"
        "}"
        "</style>"
    ))

    # Build stable per-player encoded latitudes for "→ History" synthetic clicks.
    # Each player gets a unique lat in [100.0, 200.0) that encodes their index.
    # Python maps it back to the player name when st_folium reports last_clicked.
    _nav_names = [rp["name"] for rp in _REGISTRY if rp.get("name")]
    _player_nav_lat: dict[str, float] = {
        name: round(100.0 + i / 1000.0, 4)
        for i, name in enumerate(_nav_names)
    }
    _nav_lat_to_name: dict[float, str] = {v: k for k, v in _player_nav_lat.items()}

    for _pin_key, data in map_data.items():
        clat, clon = data["coords"]
        color      = "#006633" if data["any_recent"] else "#999999"
        teams_in_pin = list(dict.fromkeys(p["team"] for p in data["players"]))
        team_label   = " / ".join(teams_in_pin)
        last_names   = ", ".join(p["name"].split()[-1] for p in data["players"])
        tooltip      = f"<b style='font-size:12px;'>{team_label}</b><br><span style='font-size:11px;'>{last_names}</span>"
        popup_html   = "".join(
            _player_card_html(p, nav_lat=_player_nav_lat.get(p["name"]))
            for p in data["players"]
        )

        is_active = data["any_recent"]
        _mob = _is_mobile()

        # Hide tooltip DOM elements the moment the popup content renders.
        # We directly manipulate .leaflet-tooltip nodes (bypassing Leaflet API
        # which sometimes fails to visually remove the tooltip) and also
        # register a popupopen handler so every subsequent popup does the same.
        _hide_js = (
            "var ts=document.querySelectorAll('.leaflet-tooltip');"
            "for(var i=0;i<ts.length;i++)ts[i].style.display='none';"
            "if(window.map&&!window._tFix){"
            "window._tFix=1;"
            "window.map.on('popupopen',function(){"
            "var tt=document.querySelectorAll('.leaflet-tooltip');"
            "for(var j=0;j<tt.length;j++)tt[j].style.display='none';});"
            "}"
        )
        _popup_content = (
            f"<img src='x' onerror=\"{_hide_js}\" style='display:none'>"
            + popup_html
        )

        folium.CircleMarker(
            location=[clat, clon],
            radius=(14 if is_active else 10) if _mob else (9 if is_active else 6),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.9 if is_active else 0.55,
            weight=2 if is_active else 1.5,
            tooltip=None if _mob else folium.Tooltip(tooltip),
            popup=folium.Popup(_popup_content, max_width=260),
        ).add_to(m)

    # Desktop: Python fit_bounds runs inside Leaflet init → always correct.
    # Mobile: MacroElement added to the MAP (not root) renders its script AFTER
    #   the map init block. map.getSize().x inside whenReady then reflects the
    #   true container width. Tested: L.map() < fitBounds < whenReady in HTML.
    if all_coords:
        m.fit_bounds(all_coords, padding=[35, 35], max_zoom=6)

    if eu_coords:
        _eu_sw = [min(c[0] for c in eu_coords) - 3, min(c[1] for c in eu_coords) - 5]
        _eu_ne = [max(c[0] for c in eu_coords) + 5, max(c[1] for c in eu_coords) + 5]

        class _MobileFit(MacroElement):
            def __init__(self, sw, ne):
                super().__init__()
                self._name = "MobileFit"
                self.sw0, self.sw1 = sw[0], sw[1]
                self.ne0, self.ne1 = ne[0], ne[1]
                # Triple-quoted template avoids f-string / Jinja2 brace conflicts.
                # Numeric vars (sw0 etc.) sidestep list-subscript parsing issues.
                self._template = _JinjaTemplate(
                    "{% macro script(this, kwargs) %}\n"
                    "{{ this._parent.get_name() }}.whenReady(function(){\n"
                    "if({{ this._parent.get_name() }}.getSize().x<600){\n"
                    "{{ this._parent.get_name() }}.fitBounds(\n"
                    "[[{{ this.sw0 }},{{ this.sw1 }}],[{{ this.ne0 }},{{ this.ne1 }}]],\n"
                    "{padding:[5,5],maxZoom:5});}\n"
                    "});\n"
                    "{% endmacro %}"
                )

        _MobileFit(_eu_sw, _eu_ne).add_to(m)

    _map_height = 320 if _is_mobile() else 420
    result = st_folium(m, use_container_width=True, height=_map_height,
                       returned_objects=["last_clicked"])

    # Detect synthetic clicks from "→ History" links (lat in 99.9–200 range).
    # Use _processing_click latch so the stale last_clicked on the post-rerun
    # doesn't re-trigger navigation.
    if st.session_state.pop("_processing_click", False):
        pass  # skip — this is the immediate rerun we triggered ourselves
    else:
        _lc = (result or {}).get("last_clicked") or {}
        _clat = round(_lc.get("lat", 0), 4)
        # Only act on a NEW encoded click — st_folium persists last_clicked
        # across all reruns until the user clicks elsewhere, so without this
        # guard every rerun (including after the user changes the selectbox)
        # would override history_player back to the map-clicked player.
        if 99.9 < _clat < 200.0 and _clat != st.session_state.get("_last_nav_lat"):
            _nav_name = _nav_lat_to_name.get(_clat)
            if _nav_name:
                st.session_state["_last_nav_lat"] = _clat
                st.session_state["history_player"] = _nav_name
                st.session_state["_scroll_to_history"] = True
                st.session_state["_processing_click"] = True
                st.rerun()

    # Surface any tracked players whose team has no coordinates yet.
    unmapped = [
        p["name"]
        for p in _REGISTRY
        if p.get("active", True) and p.get("team", "") not in _TEAM_COORDS
    ]
    if unmapped:
        st.markdown(
            f'<p style="font-size:11px;color:#999;margin-top:4px;">'
            f'⚠ Not shown on map (team coordinates pending): '
            f'{", ".join(unmapped)}</p>',
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

def _is_mobile() -> bool:
    """True when the browser viewport is < 768 px wide (via ?_mobile=1)."""
    return st.query_params.get("_mobile") == "1"




def _inject_mobile_detector() -> None:
    """Inject JS that syncs viewport width → ?_mobile URL param → Streamlit rerun.

    st.markdown <script> tags are stripped by React's dangerouslySetInnerHTML,
    so we use st.components.v1.html (a real iframe) and target window.parent.
    """
    import streamlit.components.v1 as _cv1
    _cv1.html(
        "<script>"
        "(function(){"
        "var w=window.parent;"
        "var mob=w.innerWidth<768?'1':'0';"
        "var url=new URL(w.location.href);"
        "if(url.searchParams.get('_mobile')!==mob){"
        "url.searchParams.set('_mobile',mob);"
        "w.history.replaceState({},\"\",url.toString());"
        "w.dispatchEvent(new PopStateEvent('popstate',{state:w.history.state}));"
        "}"
        "})();"
        "</script>",
        height=0,
    )


# ---------------------------------------------------------------------------
# Column config for st.dataframe
# ---------------------------------------------------------------------------

# Pairs: (made_label, attempts_label, pct_label) for average-row computation
_PCT_TRIPLES = [
    (_COL_LABELS["t2m"], _COL_LABELS["t2a"], _COL_LABELS["t2_pct"]),
    (_COL_LABELS["t3m"], _COL_LABELS["t3a"], _COL_LABELS["t3_pct"]),
    (_COL_LABELS["ftm"], _COL_LABELS["fta"], _COL_LABELS["ft_pct"]),
]

_TEXT_WIDTHS: dict[str, int] = {
    "Player":      150,
    "Team":         95,
    "Competition":  95,
    "Game Date":   102,
    "Opponent":    150,
    "Result":       84,
}
_STAT_WIDTHS: dict[str, int] = {
    "MIN": 55, "PTS": 50,
    "T2M": 50, "T2A": 50, "T2%": 58,
    "T3M": 50, "T3A": 50, "T3%": 58,
    "FTM": 50, "FTA": 50, "FT%": 58,
    "RO":  48, "RD":  48, "RT":  50,
    "AST": 50, "STL": 50, "TOV": 50,
    "BLK": 50, "BLK-A": 62,
    "F":   44, "FR":   44,
    "+/-": 55, "VAL":  55,
}


def _col_config() -> dict[str, st.column_config.Column]:
    cfg: dict[str, st.column_config.Column] = {}
    for label, w in _TEXT_WIDTHS.items():
        cfg[label] = st.column_config.TextColumn(label, width=w)
    pct_labels = {_COL_LABELS[f] for f in _PCT_FIELDS}
    for label, w in _STAT_WIDTHS.items():
        if label in pct_labels:
            cfg[label] = st.column_config.NumberColumn(label, width=w, format="%.1f")
        else:
            cfg[label] = st.column_config.NumberColumn(label, width=w, format="%d")
    return cfg


def _render_table(df: pd.DataFrame, height: int, avg_df: pd.DataFrame | None = None) -> None:
    """Render df as a native st.dataframe. If avg_df given, show it below as a summary."""
    col_cfg = _col_config()
    st.dataframe(df, width="stretch", hide_index=True,
                 height=height, column_config=col_cfg)
    if avg_df is not None:
        st.dataframe(avg_df, width="stretch", hide_index=True,
                     height=58, column_config=col_cfg)


def _build_avg_df(df: pd.DataFrame, n_games: int) -> pd.DataFrame:
    """Return a 1-row DataFrame with per-column averages for the history table."""
    row: dict = {}
    stat_labels = {_COL_LABELS[f] for f in _STAT_COLS}
    pct_labels  = {triple[2] for triple in _PCT_TRIPLES}

    for col in df.columns:
        if col not in stat_labels:
            row[col] = f"Avg ({n_games} games)" if col == _COL_LABELS["player_name"] else ""
        elif col in pct_labels:
            row[col] = None  # filled below
        else:
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            row[col] = round(series.mean(), 1) if len(series) else None

    for made_lbl, att_lbl, pct_lbl in _PCT_TRIPLES:
        made_s = pd.to_numeric(df[made_lbl], errors="coerce").dropna()
        att_s  = pd.to_numeric(df[att_lbl],  errors="coerce").dropna()
        total_att = att_s.sum()
        row[pct_lbl] = round(made_s.sum() / total_att * 100, 1) if total_att > 0 else None

    return pd.DataFrame([row])



_STATUS_ICON = {
    "injured":   "🤕",
    "rest":      "😴",
    "suspended": "🚫",
    "unknown":   "❓",
}




def render_latest(records: list[dict], run_date: str = "") -> None:
    _label = ""
    if run_date:
        try:
            import os as _os
            import subprocess as _sp
            from datetime import datetime as _dt
            _root = _os.path.dirname(__file__)
            _rel  = f"data/stats/{run_date}.json"
            _r = _sp.run(
                ["git", "log", "-1", "--format=%ct", _rel],
                capture_output=True, text=True, cwd=_root,
            )
            if _r.returncode == 0 and _r.stdout.strip():
                _mtime = _dt.fromtimestamp(int(_r.stdout.strip()))
            else:
                # fallback: file mtime (less accurate on cloud deployments)
                _mtime = _dt.fromtimestamp(_os.path.getmtime(_os.path.join(_root, _rel)))
            _label = _mtime.strftime("%d %b %Y %H:%M").lstrip("0")
        except Exception:
            try:
                _label = _dt.strptime(run_date, "%Y-%m-%d").strftime("%d %b %Y").lstrip("0")
            except Exception:
                _label = run_date
    _updated = f" &nbsp;<span style='font-size:13px;font-weight:500;color:#888;'>Updated · {_label}</span>" if _label else ""
    st.markdown(_section_heading(f"Last 24 hours{_updated}"), unsafe_allow_html=True)

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

    if played_rows:
        df = pd.DataFrame(played_rows)
        count = len(played_rows)
        height = min(36 + count * 36 + 20, 600)
        st.markdown(
            f'<div style="margin:4px 0 6px;">'
            f'<span style="background:{_UNICAJA_GREEN};color:white;padding:2px 10px;'
            f'border-radius:10px;font-size:12px;font-weight:700;">'
            f'{"1 game" if count == 1 else f"{count} games"}'
            f'</span></div>',
            unsafe_allow_html=True,
        )
        _render_table(df, height=height)



# ---------------------------------------------------------------------------
# Historical table
# ---------------------------------------------------------------------------

def render_history(all_data: dict[str, list[dict]]) -> None:
    # Anchor for deep-linking from map card clicks
    st.markdown('<div id="game-history"></div>', unsafe_allow_html=True)
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

    # Reset the date filter whenever the player selection changes to a specific player,
    # so that selecting a player always shows their full history, not a filtered slice.
    current_player_ss = st.session_state.get("history_player", _ANY_PLAYER)
    prev_player_ss    = st.session_state.get("_history_player_prev", _ANY_PLAYER)
    if current_player_ss != prev_player_ss and current_player_ss != _ANY_PLAYER:
        st.session_state["history_date"] = _ANY_DATE
    st.session_state["_history_player_prev"] = current_player_ss

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
            index=0,
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
    show_avg = filter_player is not None and filter_date is None
    avg_df   = _build_avg_df(df, len(game_rows)) if show_avg else None
    height   = min(36 + len(df) * 36 + 20, 540)
    _render_table(df, height=height, avg_df=avg_df)


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

# Auto-refresh every 5 minutes — aligned with cache TTL so new scraping
# results are picked up promptly without requiring a manual page reload.
st_autorefresh(interval=5 * 60 * 1000, key="data_refresh")

_inject_mobile_detector()
_inject_css()
_inject_select_keyboard_fix()

st.markdown(
    """
    <div style="background:#c0392b;color:white;padding:14px 20px;border-radius:8px;
                font-size:1.1rem;font-weight:600;text-align:center;margin-bottom:12px;">
        ⚠️ This page is no longer maintained.
        The new, better-supported dashboard is at
        <a href="https://eran42.github.io/unicaja-stats-hub/"
           style="color:white;text-decoration:underline;" target="_blank">
            eran42.github.io/unicaja-stats-hub
        </a>
    </div>
    """,
    unsafe_allow_html=True,
)

dates = get_all_dates()
_render_header(dates[-1] if dates else "")

if not dates:
    st.warning("No data yet. Run `python main.py` to fetch stats.")
    st.stop()

latest_date, latest_records = _load_latest()
all_data = _load_all()

render_map(all_data)

st.divider()

render_latest(latest_records, run_date=latest_date)

st.divider()

render_history(all_data)

st.divider()
st.link_button("💬 Leave feedback", "https://docs.google.com/forms/d/e/1FAIpQLSfOZf6HxTN9qDne21NV8FBItupLsgxXoEmy97FYugUplLbHaQ/viewform?usp=publish-editor")

# After a map-pin click triggers st.rerun(), scroll to the history section.
# A monotonic counter is embedded in the HTML so Streamlit never reuses a
# cached iframe — without it the setTimeout only fires on the first scroll.
if st.session_state.pop("_scroll_to_history", False):
    import streamlit.components.v1 as _c
    _n = st.session_state.get("_scroll_n", 0) + 1
    st.session_state["_scroll_n"] = _n
    _c.html(
        f"<script>/* {_n} */"
        "setTimeout(function(){"
        "var el=window.parent.document.getElementById('game-history');"
        "if(el)el.scrollIntoView({behavior:'smooth',block:'start'});"
        "},200);"
        "</script>",
        height=0,
    )
