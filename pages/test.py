from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import re

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Test | Utility-scale Solar PV Installed Capacity", layout="wide")

REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
YELLOW = "#FACC15"
YELLOW_DARK = "#D97706"
BLUE = "#2563EB"
GREY = "#64748B"
RED = "#DC2626"

DEFAULT_START = date(2021, 1, 1)
TODAY = date.today()
DEFAULT_END = TODAY

# =========================================================
# STYLE
# =========================================================
def section(title: str, icon: str = "🧪") -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
            color: white;
            padding: 14px 18px;
            border-radius: 14px;
            font-weight: 850;
            font-size: 1.35rem;
            margin-top: 8px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">{icon} {title}</div>
        """,
        unsafe_allow_html=True,
    )


def subsection(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            margin-top: 16px;
            margin-bottom: 8px;
            padding: 8px 12px;
            color: #0F172A;
            background: #F4FCF8;
            border-left: 5px solid {CORP_GREEN};
            font-size: 1.02rem;
            font-weight: 800;
            border-radius: 8px;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def chart_style(chart, height: int = 420):
    return (
        chart.properties(height=height)
        .configure_view(stroke="#E5E7EB", fill="white")
        .configure_axis(
            grid=True,
            gridColor="#E5E7EB",
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor="#111827",
            titleColor="#111827",
            labelFontSize=12,
            titleFontSize=13,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=12,
            titleFontSize=12,
            symbolStrokeWidth=3,
        )
    )


def normalise_title(value: str) -> str:
    s = str(value or "").strip().lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }
    for a, b in replacements.items():
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


# =========================================================
# REE API
# =========================================================
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_ree_capacity_window(start_day: date, end_day: date) -> dict:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "month",
        **REE_PENINSULAR_PARAMS,
    }
    url = f"{REE_API_BASE}/generacion/potencia-instalada"
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def parse_capacity_payload(payload: dict) -> pd.DataFrame:
    rows: list[dict] = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = str(attrs.get("title") or item.get("id") or "").strip()
        values = attrs.get("values", []) or []
        for val in values:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None).to_period("M").to_timestamp()
            cap = pd.to_numeric(val.get("value"), errors="coerce")
            if pd.isna(cap):
                continue
            rows.append(
                {
                    "datetime": dt,
                    "title": title,
                    "title_norm": normalise_title(title),
                    "capacity_mw": float(cap),
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["datetime", "title", "title_norm", "capacity_mw"])
    return (
        out.groupby(["datetime", "title", "title_norm"], as_index=False)["capacity_mw"]
        .sum()
        .sort_values(["datetime", "title"])
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False, ttl=3600)
def load_capacity_monthly(start_day: date, end_day: date) -> pd.DataFrame:
    """Robust monthly pull. Tries full window, then falls back to year-by-year."""
    frames: list[pd.DataFrame] = []

    try:
        payload = fetch_ree_capacity_window(start_day, end_day)
        parsed = parse_capacity_payload(payload)
        if not parsed.empty:
            frames.append(parsed)
    except Exception:
        pass

    # Year-by-year fallback to make the test robust.
    if not frames:
        for year in range(start_day.year, end_day.year + 1):
            s = max(start_day, date(year, 1, 1))
            e = min(end_day, date(year, 12, 31))
            if s > e:
                continue
            try:
                payload = fetch_ree_capacity_window(s, e)
                parsed = parse_capacity_payload(payload)
                if not parsed.empty:
                    frames.append(parsed)
            except Exception:
                continue

    if not frames:
        return pd.DataFrame(columns=["datetime", "title", "title_norm", "capacity_mw"])

    out = pd.concat(frames, ignore_index=True)
    return (
        out.groupby(["datetime", "title", "title_norm"], as_index=False)["capacity_mw"]
        .sum()
        .sort_values(["datetime", "title"])
        .reset_index(drop=True)
    )


# =========================================================
# PV / AUTOCONSUMO SERIES SELECTION
# =========================================================
def candidate_titles(df: pd.DataFrame, pattern: str) -> list[str]:
    if df.empty:
        return []
    mask = df["title_norm"].str.contains(pattern, regex=True, na=False)
    return sorted(df.loc[mask, "title"].dropna().astype(str).unique().tolist())


def build_capacity_series(df: pd.DataFrame, title: str | None, series_name: str) -> pd.DataFrame:
    if df.empty or not title:
        return pd.DataFrame(columns=["datetime", "series", "capacity_mw"])
    out = df[df["title"] == title].copy()
    if out.empty:
        return pd.DataFrame(columns=["datetime", "series", "capacity_mw"])
    out = (
        out.groupby("datetime", as_index=False)["capacity_mw"]
        .sum()
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    out["series"] = series_name
    return out[["datetime", "series", "capacity_mw"]]


def build_utility_scale(total_pv: pd.DataFrame, autoconsumo: pd.DataFrame) -> pd.DataFrame:
    if total_pv.empty:
        return pd.DataFrame(columns=["datetime", "series", "capacity_mw"])
    base = total_pv.rename(columns={"capacity_mw": "pv_total_mw"})[["datetime", "pv_total_mw"]].copy()
    if autoconsumo.empty:
        base["autoconsumo_mw"] = pd.NA
        base["capacity_mw"] = pd.NA
    else:
        auto = autoconsumo.rename(columns={"capacity_mw": "autoconsumo_mw"})[["datetime", "autoconsumo_mw"]].copy()
        base = base.merge(auto, on="datetime", how="left")
        base["capacity_mw"] = base["pv_total_mw"] - base["autoconsumo_mw"]
    base["series"] = "Utility-scale PV = Solar PV − autoconsumo"
    return base[["datetime", "series", "capacity_mw"]]


def utility_table(total_pv: pd.DataFrame, autoconsumo: pd.DataFrame, utility: pd.DataFrame) -> pd.DataFrame:
    if total_pv.empty:
        return pd.DataFrame()
    out = total_pv.rename(columns={"capacity_mw": "Solar PV total MW"})[["datetime", "Solar PV total MW"]].copy()
    if not autoconsumo.empty:
        out = out.merge(
            autoconsumo.rename(columns={"capacity_mw": "Autoconsumo MW"})[["datetime", "Autoconsumo MW"]],
            on="datetime",
            how="left",
        )
    else:
        out["Autoconsumo MW"] = pd.NA
    if not utility.empty:
        out = out.merge(
            utility.rename(columns={"capacity_mw": "Utility-scale PV MW"})[["datetime", "Utility-scale PV MW"]],
            on="datetime",
            how="left",
        )
    else:
        out["Utility-scale PV MW"] = pd.NA
    out["Month"] = pd.to_datetime(out["datetime"]).dt.strftime("%b-%Y")
    cols = ["Month", "Solar PV total MW", "Autoconsumo MW", "Utility-scale PV MW"]
    return out[cols].sort_values("Month").reset_index(drop=True)


# =========================================================
# CHARTS
# =========================================================
def evolution_chart(series_df: pd.DataFrame):
    if series_df.empty:
        return None
    order = [
        "Solar PV total from REE",
        "Autoconsumo PV from REE",
        "Utility-scale PV = Solar PV − autoconsumo",
    ]
    colours = [GREY, RED, YELLOW_DARK]
    chart = alt.Chart(series_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("datetime:T", title="Month", axis=alt.Axis(format="%b-%Y", labelAngle=-35)),
        y=alt.Y("capacity_mw:Q", title="Installed capacity (MW)", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "series:N",
            title="Series",
            sort=order,
            scale=alt.Scale(domain=order, range=colours),
        ),
        tooltip=[
            alt.Tooltip("datetime:T", title="Month", format="%b-%Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("capacity_mw:Q", title="MW", format=",.0f"),
        ],
    ).properties(title="Solar PV installed capacity test | Raw total, autoconsumo, and utility-scale proxy")
    return chart_style(chart, height=460)


def latest_cards(table: pd.DataFrame) -> None:
    if table.empty:
        return
    latest = table.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Latest Solar PV total", f"{latest['Solar PV total MW']:,.0f} MW" if pd.notna(latest["Solar PV total MW"]) else "—")
    c2.metric("Latest autoconsumo PV", f"{latest['Autoconsumo MW']:,.0f} MW" if pd.notna(latest["Autoconsumo MW"]) else "—")
    c3.metric("Latest utility-scale PV proxy", f"{latest['Utility-scale PV MW']:,.0f} MW" if pd.notna(latest["Utility-scale PV MW"]) else "—")


# =========================================================
# PAGE
# =========================================================
section("Utility-scale Solar PV installed capacity — test bench", "☀️")
st.caption(
    "Goal: test whether REE's monthly installed-capacity payload exposes a separate autoconsumo series. "
    "When it does, utility-scale PV can be calculated transparently as Solar PV total − autoconsumo."
)

c1, c2 = st.columns(2)
with c1:
    start_day = st.date_input("Start date", value=DEFAULT_START, min_value=date(2021, 1, 1), max_value=TODAY)
with c2:
    end_day = st.date_input("End date", value=DEFAULT_END, min_value=date(2021, 1, 1), max_value=TODAY)

if start_day > end_day:
    st.error("Start date must be before end date.")
    st.stop()

with st.spinner("Fetching REE installed-capacity series..."):
    raw = load_capacity_monthly(start_day, end_day)

if raw.empty:
    st.error("REE returned no installed-capacity series for the selected window.")
    st.stop()

subsection("1) Detect REE series titles")
all_titles = sorted(raw["title"].dropna().astype(str).unique().tolist())
pv_candidates = candidate_titles(raw, r"fotovolta|photovolta|solar pv")
autoconsumo_candidates = candidate_titles(raw, r"autocons|self.?consump")

left, right = st.columns(2)
with left:
    total_pv_title = st.selectbox(
        "Series used as Solar PV total",
        options=[""] + pv_candidates + [t for t in all_titles if t not in pv_candidates],
        index=1 if pv_candidates else 0,
        key="pv_total_title",
    )
with right:
    autoconsumo_title = st.selectbox(
        "Series used as autoconsumo PV",
        options=[""] + autoconsumo_candidates + [t for t in all_titles if t not in autoconsumo_candidates],
        index=1 if autoconsumo_candidates else 0,
        key="autoconsumo_title",
    )

if not autoconsumo_candidates:
    st.warning(
        "No obvious title containing 'autoconsumo' was found in the REE installed-capacity payload. "
        "Use the dropdown to inspect alternatives; if REE does not expose a dedicated series, the subtraction cannot be made from this payload alone."
    )

with st.expander("Show all REE titles received"):
    title_df = (
        raw[["title"]]
        .drop_duplicates()
        .sort_values("title")
        .reset_index(drop=True)
    )
    st.dataframe(title_df, use_container_width=True, hide_index=True)

subsection("2) Monthly capacity comparison")
total_pv = build_capacity_series(raw, total_pv_title or None, "Solar PV total from REE")
autoconsumo = build_capacity_series(raw, autoconsumo_title or None, "Autoconsumo PV from REE")
utility = build_utility_scale(total_pv, autoconsumo)

comparison_series = pd.concat([total_pv, autoconsumo, utility], ignore_index=True)
chart = evolution_chart(comparison_series.dropna(subset=["capacity_mw"]))
if chart is not None:
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Select valid REE titles above to populate the comparison chart.")

table = utility_table(total_pv, autoconsumo, utility)
latest_cards(table)

if not table.empty:
    st.dataframe(
        table.style.format(
            {
                "Solar PV total MW": "{:,.0f}",
                "Autoconsumo MW": "{:,.0f}",
                "Utility-scale PV MW": "{:,.0f}",
            },
            na_rep="—",
        ),
        use_container_width=True,
        hide_index=True,
    )

subsection("3) Raw monthly records for debugging")
st.dataframe(
    raw.sort_values(["datetime", "title"]).reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "This is a test page. Once the title mapping is confirmed, the same logic can replace the Day Ahead installed-capacity block."
)
