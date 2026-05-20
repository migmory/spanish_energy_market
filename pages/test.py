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
st.set_page_config(page_title="Test | Utility-scale Solar PV installed capacity", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
HIST_INSTALLED_CAP_FILE = DATA_DIR / "installed_capacity_monthly.xlsx"

REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}
LIVE_START_DATE = date(2026, 1, 1)

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
YELLOW = "#FACC15"
YELLOW_DARK = "#D97706"
BLUE = "#2563EB"
GREY = "#64748B"
RED = "#DC2626"
SOFT_YELLOW = "#FEF3C7"
SOFT_BLUE = "#EFF6FF"

LOCAL_CAP_TECH_MAP = {
    "Solar fotovoltaica": "Solar PV",
    "Solar Fotovoltaica": "Solar PV",
    "Solar photovoltaic": "Solar PV",
    "Fotovoltaica": "Solar PV",
}

# =========================================================
# STYLE
# =========================================================
def section(title: str, icon: str = "☀️") -> None:
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


def chart_style(chart, height: int = 440):
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


def normalize_text(value: str) -> str:
    s = str(value or "").strip().lower()
    for a, b in {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n"}.items():
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


def parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT

    m = re.match(r"^(\d{4})00(\d{1,2})(\d{3})T", s)
    if m:
        y, mth, d = m.groups()
        return pd.Timestamp(int(y), int(mth), int(d))

    m = re.match(r"^(\d{4})0(\d{2})(\d{2})T", s)
    if m:
        y, mth, d = m.groups()
        return pd.Timestamp(int(y), int(mth), int(d))

    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if m:
        d, mth, y = m.groups()
        return pd.Timestamp(int(y), int(mth), int(d))

    m = re.match(r"^(\d{2})/(\d{4})$", s)
    if m:
        mth, y = m.groups()
        return pd.Timestamp(int(y), int(mth), 1)

    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise")
    except Exception:
        return pd.NaT


def month_end(d: date) -> date:
    first_next = date(d.year + (1 if d.month == 12 else 0), 1 if d.month == 12 else d.month + 1, 1)
    return first_next - timedelta(days=1)


# =========================================================
# HISTORICAL LOCAL CAPACITY
# =========================================================
@st.cache_data(show_spinner=False)
def load_historical_installed_capacity() -> pd.DataFrame:
    cols = ["datetime", "technology", "capacity_mw", "source", "raw_title"]
    if not HIST_INSTALLED_CAP_FILE.exists():
        return pd.DataFrame(columns=cols)

    try:
        raw = pd.read_excel(HIST_INSTALLED_CAP_FILE, sheet_name="data", header=None)
    except Exception:
        return pd.DataFrame(columns=cols)

    dates = [parse_mixed_date(v) for v in raw.iloc[4, 1:].tolist()]
    tech_rows = raw.iloc[5:19, :].copy()

    records = []
    for _, row in tech_rows.iterrows():
        raw_title = str(row.iloc[0]).strip()
        technology = LOCAL_CAP_TECH_MAP.get(raw_title, raw_title)
        for col_idx, dt in enumerate(dates, start=1):
            if pd.isna(dt):
                continue
            value = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.isna(value):
                continue
            records.append(
                {
                    "datetime": pd.Timestamp(dt).normalize().to_period("M").to_timestamp(),
                    "technology": technology,
                    "capacity_mw": float(value),
                    "source": "Historical local file",
                    "raw_title": raw_title,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=cols)

    return (
        out.groupby(["datetime", "technology", "source", "raw_title"], as_index=False)["capacity_mw"]
        .last()
        .sort_values(["datetime", "technology"])
        .reset_index(drop=True)
    )


# =========================================================
# REE LIVE CAPACITY
# =========================================================
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_ree_capacity_window(start_day: date, end_day: date) -> tuple[dict | None, dict]:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "month",
        **REE_PENINSULAR_PARAMS,
    }
    url = f"{REE_API_BASE}/generacion/potencia-instalada"
    try:
        response = requests.get(url, params=params, timeout=60)
        status = int(response.status_code)
        content_type = response.headers.get("content-type", "")
        response.raise_for_status()
        payload = response.json()
        return payload, {
            "window_start": start_day.isoformat(),
            "window_end": end_day.isoformat(),
            "status": "OK",
            "http_status": status,
            "content_type": content_type,
            "included_items": len(payload.get("included", []) or []),
            "data_items": len(payload.get("data", []) or []),
            "message": "",
        }
    except Exception as exc:
        return None, {
            "window_start": start_day.isoformat(),
            "window_end": end_day.isoformat(),
            "status": "ERROR",
            "http_status": None,
            "content_type": "",
            "included_items": 0,
            "data_items": 0,
            "message": str(exc),
        }


def parse_ree_capacity_payload(payload: dict | None, request_tag: str) -> pd.DataFrame:
    cols = ["datetime", "technology", "capacity_mw", "source", "raw_title", "title_norm", "request_tag"]
    if not payload:
        return pd.DataFrame(columns=cols)

    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        raw_title = str(attrs.get("title") or item.get("id") or "").strip()
        title_norm = normalize_text(raw_title)
        technology = LOCAL_CAP_TECH_MAP.get(raw_title, raw_title)
        values = attrs.get("values", []) or []
        for value_row in values:
            dt = pd.to_datetime(value_row.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None).to_period("M").to_timestamp()
            cap = pd.to_numeric(value_row.get("value"), errors="coerce")
            if pd.isna(cap):
                continue
            rows.append(
                {
                    "datetime": dt,
                    "technology": technology,
                    "capacity_mw": float(cap),
                    "source": "REE API",
                    "raw_title": raw_title,
                    "title_norm": title_norm,
                    "request_tag": request_tag,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=cols)
    return out[cols].sort_values(["datetime", "raw_title"]).reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_installed_capacity_with_diagnostics(end_day: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    cols = ["datetime", "technology", "capacity_mw", "source", "raw_title", "title_norm", "request_tag"]
    diag_rows = []
    frames = []

    start_day = LIVE_START_DATE
    if end_day < start_day:
        return pd.DataFrame(columns=cols), pd.DataFrame(diag_rows)

    candidate_windows: list[tuple[date, date, str]] = []
    candidate_windows.append((start_day, end_day, "full_2026_to_selected_end"))

    first_this_month = date(end_day.year, end_day.month, 1)
    previous_month_end = first_this_month - timedelta(days=1)
    if previous_month_end >= start_day:
        candidate_windows.append((start_day, previous_month_end, "full_2026_to_previous_month_end"))

    month_cursor = date(2026, 1, 1)
    while month_cursor <= end_day:
        window_end = min(month_end(month_cursor), end_day)
        candidate_windows.append((month_cursor, window_end, f"single_month_{month_cursor:%Y_%m}"))
        if month_cursor.month == 12:
            month_cursor = date(month_cursor.year + 1, 1, 1)
        else:
            month_cursor = date(month_cursor.year, month_cursor.month + 1, 1)

    for s_day, e_day, tag in candidate_windows:
        payload, diag = fetch_ree_capacity_window(s_day, e_day)
        diag["request_tag"] = tag
        parsed = parse_ree_capacity_payload(payload, tag)
        diag["parsed_rows"] = int(len(parsed))
        diag["parsed_titles"] = ", ".join(sorted(parsed["raw_title"].dropna().astype(str).unique().tolist()))[:500] if not parsed.empty else ""
        diag_rows.append(diag)
        if not parsed.empty:
            frames.append(parsed)

    if not frames:
        return pd.DataFrame(columns=cols), pd.DataFrame(diag_rows)

    raw_live = pd.concat(frames, ignore_index=True)

    # Candidate windows overlap. Keep a single observation per month/title/value series;
    # do NOT sum duplicated months across windows.
    live = (
        raw_live.sort_values(["datetime", "raw_title", "request_tag"])
        .drop_duplicates(subset=["datetime", "raw_title"], keep="last")
        .reset_index(drop=True)
    )
    return live[cols], pd.DataFrame(diag_rows)


# =========================================================
# UTILITY-SCALE PV PROXY
# =========================================================
def choose_candidate_titles(df: pd.DataFrame, kind: str) -> list[str]:
    if df.empty:
        return []
    if kind == "pv":
        mask = df["title_norm"].str.contains(r"solar fotovolta|fotovolta|photovolta|solar pv", regex=True, na=False)
    elif kind == "autoconsumo":
        mask = df["title_norm"].str.contains(r"autocons|self.?consump", regex=True, na=False)
    else:
        mask = pd.Series(False, index=df.index)
    return sorted(df.loc[mask, "raw_title"].dropna().astype(str).unique().tolist())


def build_series_by_title(df: pd.DataFrame, raw_title: str | None, series_name: str) -> pd.DataFrame:
    cols = ["datetime", "series", "capacity_mw"]
    if df.empty or not raw_title:
        return pd.DataFrame(columns=cols)
    out = df[df["raw_title"] == raw_title].copy()
    if out.empty:
        return pd.DataFrame(columns=cols)
    out = (
        out.groupby("datetime", as_index=False)["capacity_mw"]
        .last()
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    out["series"] = series_name
    return out[cols]


def build_solar_total_from_combined(hist: pd.DataFrame, live: pd.DataFrame) -> pd.DataFrame:
    cols = ["datetime", "series", "capacity_mw"]
    frames = []
    hist_solar = hist[hist["technology"] == "Solar PV"].copy() if not hist.empty else pd.DataFrame()
    if not hist_solar.empty:
        h = hist_solar.groupby("datetime", as_index=False)["capacity_mw"].last()
        h["series"] = "Solar PV total capacity"
        frames.append(h[cols])
    if not live.empty:
        live_pv_titles = choose_candidate_titles(live, "pv")
        if live_pv_titles:
            # Prefer the simplest Solar PV title; if REE supplies only one, this picks it.
            chosen = sorted(live_pv_titles, key=lambda x: (len(x), x))[0]
            l = build_series_by_title(live, chosen, "Solar PV total capacity")
            if not l.empty:
                frames.append(l)
    if not frames:
        return pd.DataFrame(columns=cols)
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(subset=["datetime", "series"], keep="last").sort_values("datetime").reset_index(drop=True)


def build_utility_proxy(total_pv: pd.DataFrame, autoconsumo: pd.DataFrame) -> pd.DataFrame:
    cols = ["datetime", "series", "capacity_mw"]
    if total_pv.empty or autoconsumo.empty:
        return pd.DataFrame(columns=cols)
    total = total_pv.rename(columns={"capacity_mw": "pv_total_mw"})[["datetime", "pv_total_mw"]]
    auto = autoconsumo.rename(columns={"capacity_mw": "autoconsumo_mw"})[["datetime", "autoconsumo_mw"]]
    out = total.merge(auto, on="datetime", how="inner")
    out["capacity_mw"] = out["pv_total_mw"] - out["autoconsumo_mw"]
    out["series"] = "Utility-scale PV = total PV − autoconsumo"
    return out[cols].sort_values("datetime").reset_index(drop=True)


def comparison_table(total_pv: pd.DataFrame, autoconsumo: pd.DataFrame, utility: pd.DataFrame) -> pd.DataFrame:
    if total_pv.empty:
        return pd.DataFrame()
    out = total_pv.rename(columns={"capacity_mw": "Solar PV total MW"})[["datetime", "Solar PV total MW"]]
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
    return out[["Month", "Solar PV total MW", "Autoconsumo MW", "Utility-scale PV MW"]].reset_index(drop=True)


def capacity_chart(series_df: pd.DataFrame):
    if series_df.empty:
        return None
    order = [
        "Solar PV total capacity",
        "Autoconsumo PV",
        "Utility-scale PV = total PV − autoconsumo",
    ]
    colors = [GREY, RED, YELLOW_DARK]
    chart = alt.Chart(series_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("datetime:T", title="Month", axis=alt.Axis(format="%b-%Y", labelAngle=-35)),
        y=alt.Y("capacity_mw:Q", title="Installed capacity (MW)", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title="Series", sort=order, scale=alt.Scale(domain=order, range=colors)),
        tooltip=[
            alt.Tooltip("datetime:T", title="Month", format="%b-%Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("capacity_mw:Q", title="MW", format=",.0f"),
        ],
    ).properties(title="Solar PV installed capacity | historical + live REE 2026 test")
    return chart_style(chart, height=460)


# =========================================================
# PAGE
# =========================================================
section("Utility-scale Solar PV installed capacity — test bench")
st.caption(
    "This test reuses the Day Ahead REE endpoint logic for live 2026 capacity, but adds diagnostics "
    "and explicitly checks whether REE exposes an autoconsumo series that can be subtracted from Solar PV."
)

c1, c2 = st.columns([1, 1])
with c1:
    end_day = st.date_input("REE live end date", value=date.today(), min_value=date(2026, 1, 1), max_value=date.today())
with c2:
    st.info("Historical local installed capacity is read from `/data/installed_capacity_monthly.xlsx`; live REE capacity is tested only for 2026.")

hist = load_historical_installed_capacity()
with st.spinner("Fetching live 2026 installed-capacity windows from REE..."):
    live, diagnostics = load_live_2026_installed_capacity_with_diagnostics(end_day)

if live.empty:
    st.error(
        "REE returned no parsed live 2026 installed-capacity series for the tested windows. "
        "Open the diagnostics below: this now shows the API windows, HTTP result, and parsed-row count."
    )
else:
    st.success(f"Live REE 2026 capacity rows parsed: {len(live):,}.")

subsection("1) Live REE diagnostics")
with st.expander("Show REE request diagnostics", expanded=live.empty):
    st.dataframe(diagnostics, use_container_width=True, hide_index=True)

with st.expander("Show live REE titles received", expanded=False):
    if live.empty:
        st.info("No live titles were parsed.")
    else:
        title_table = (
            live.groupby(["raw_title", "title_norm"], as_index=False)
            .agg(months=("datetime", "nunique"), latest_month=("datetime", "max"), latest_mw=("capacity_mw", "last"))
            .sort_values("raw_title")
        )
        title_table["latest_month"] = pd.to_datetime(title_table["latest_month"]).dt.strftime("%b-%Y")
        st.dataframe(title_table, use_container_width=True, hide_index=True)

subsection("2) Choose PV and autoconsumo titles")
pv_candidates = choose_candidate_titles(live, "pv")
auto_candidates = choose_candidate_titles(live, "autoconsumo")
all_live_titles = sorted(live["raw_title"].dropna().astype(str).unique().tolist()) if not live.empty else []

left, right = st.columns(2)
with left:
    pv_title = st.selectbox(
        "REE title used as 2026 Solar PV total",
        options=[""] + pv_candidates + [x for x in all_live_titles if x not in pv_candidates],
        index=1 if pv_candidates else 0,
    )
with right:
    auto_title = st.selectbox(
        "REE title used as autoconsumo PV",
        options=[""] + auto_candidates + [x for x in all_live_titles if x not in auto_candidates],
        index=1 if auto_candidates else 0,
    )

if not auto_candidates:
    st.warning(
        "No obvious REE title containing 'autoconsumo' was detected in the live `potencia-instalada` payload. "
        "That would mean this endpoint alone cannot produce utility-scale PV by direct subtraction."
    )

subsection("3) Capacity series comparison")
solar_total = build_solar_total_from_combined(hist, live)
autoconsumo = build_series_by_title(live, auto_title or None, "Autoconsumo PV")
utility = build_utility_proxy(solar_total, autoconsumo)

series_df = pd.concat([solar_total, autoconsumo, utility], ignore_index=True)
chart = capacity_chart(series_df.dropna(subset=["capacity_mw"]))
if chart is not None:
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("There is not enough series data to draw the comparison chart yet.")

table = comparison_table(solar_total, autoconsumo, utility)
if not table.empty:
    latest = table.iloc[-1]
    m1, m2, m3 = st.columns(3)
    m1.metric("Latest Solar PV total", f"{latest['Solar PV total MW']:,.0f} MW" if pd.notna(latest["Solar PV total MW"]) else "—")
    m2.metric("Latest autoconsumo PV", f"{latest['Autoconsumo MW']:,.0f} MW" if pd.notna(latest["Autoconsumo MW"]) else "—")
    m3.metric("Latest utility-scale PV proxy", f"{latest['Utility-scale PV MW']:,.0f} MW" if pd.notna(latest["Utility-scale PV MW"]) else "—")

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

subsection("4) Raw rows")
tabs = st.tabs(["Historical local rows", "Live REE rows"])
with tabs[0]:
    st.dataframe(hist.sort_values(["datetime", "technology"]).reset_index(drop=True), use_container_width=True, hide_index=True)
with tabs[1]:
    st.dataframe(live.sort_values(["datetime", "raw_title"]).reset_index(drop=True), use_container_width=True, hide_index=True)

st.caption(
    "Next step after this test: confirm whether the REE live payload contains a dedicated autoconsumo PV series. "
    "If yes, we can wire the subtraction directly into the Day Ahead GW installed block; if not, we will need an external autoconsumo capacity source."
)
