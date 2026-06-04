from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# =========================================================
# IS2 Solarpark / UNITY SCADA + Day Ahead revenues
# =========================================================
# Generation source:
#   Solarpark/UNITY API using Cookie auth, same structure as the working test page.
# Price source:
#   Day Ahead workbook used by the Day Ahead tab:
#       data/hourly_avg_price_since2021.xlsx
#       sheet: prices_hourly_avg or prices_hourly
#       columns: datetime + price
# Revenue logic:
#   10-min SCADA power kW -> kWh per 10-min -> 15-min generation -> hourly generation -> hourly price join.
#   Revenue = hourly_generation_mwh * day_ahead_price_eur_mwh.
#
# Run:
#   streamlit run is2_solarpark_scada_revenues_corporate.py
#
# Required .env / Streamlit secrets:
#   SOLARPARK_COOKIE='IFMSCK=...'

# =========================================================
# Config
# =========================================================
BASE = "https://portal.solarpark-online.com"
MADRID_TZ = ZoneInfo("Europe/Madrid")
UTC_TZ = ZoneInfo("UTC")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
ENV_PATH = BASE_DIR / ".env"

if load_dotenv is not None:
    load_dotenv(dotenv_path=ENV_PATH, override=True)

# Confirmed / inferred source IDs from the current working SCADA test page.
POWER_SOURCE_IDS = {
    "Carmona Central 36": "d19246ea-065e-11f0-980e-42010afa015a",
    "Carmona Central 36.1": "cc211458-0892-11f0-9eeb-42010afa015a",
    "Palma del Condado Solar 555": "7113ab0e-2726-11f0-b9a2-42010afa015a",
    "Guarroman Solar 81": "e1e421b8-1382-11f0-85ad-42010afa015a",
}
ID_TO_SITE = {v: k for k, v in POWER_SOURCE_IDS.items()}

PRICE_WORKBOOK_CANDIDATES = [
    DATA_DIR / "hourly_avg_price_since2021.xlsx",
    DATA_DIR / "day_ahead_prices.xlsx",
    DATA_DIR / "omie_day_ahead_prices.xlsx",
    BASE_DIR / "hourly_avg_price_since2021.xlsx",
]

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_BLUE = "#1D4ED8"
CORP_GREY = "#4B5563"
CORP_BORDER = "#E5E7EB"
CORP_BG = "#F8FAFC"
CORP_RED = "#DC2626"
CORP_ORANGE = "#D97706"

# =========================================================
# Page + style
# =========================================================
st.set_page_config(page_title="IS2 SCADA Revenues", page_icon="⚡", layout="wide")

st.markdown(
    f"""
    <style>
    .main .block-container {{
        padding-top: 1.35rem;
        padding-bottom: 2rem;
        max-width: 1540px;
    }}
    h1 {{
        color:#111827 !important;
        font-size:2.05rem !important;
        font-weight:850 !important;
        letter-spacing:-0.02em;
    }}
    h2, h3 {{ color:#111827 !important; }}
    .corp-header {{
        background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 58%, #C7F0DD 100%);
        color: white;
        padding: 15px 20px;
        border-radius: 15px;
        font-weight: 850;
        font-size: 1.22rem;
        margin: 18px 0 14px 0;
        box-shadow: 0 3px 12px rgba(15,118,110,0.15);
    }}
    .status-ok {{background:#ECFDF5;color:#065F46;border:1px solid #BBF7D0;border-radius:14px;padding:13px 15px;margin:8px 0 16px 0;}}
    .status-warning {{background:#FFF8E6;color:#7A5200;border:1px solid #FFE1A6;border-radius:14px;padding:13px 15px;margin:8px 0 16px 0;}}
    .status-danger {{background:#FEF2F2;color:#991B1B;border:1px solid #FECACA;border-radius:14px;padding:13px 15px;margin:8px 0 16px 0;}}
    .pill {{display:inline-block;border-radius:999px;padding:6px 11px;font-size:0.85rem;font-weight:700;margin:4px 6px 10px 0;}}
    .pill-green {{background:#ECFDF5;border:1px solid #BBF7D0;color:#065F46;}}
    .pill-blue {{background:#EEF2FF;border:1px solid #C7D2FE;color:#3730A3;}}
    .pill-grey {{background:#F3F4F6;border:1px solid #E5E7EB;color:#374151;}}
    div[data-testid="metric-container"] {{
        background:white;
        border:1px solid {CORP_BORDER};
        border-radius:16px;
        padding:15px 17px;
        box-shadow:0 1px 3px rgba(15,23,42,0.05);
    }}
    div[data-testid="metric-container"] label {{color:#6B7280 !important;font-weight:650 !important;}}
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {{color:#111827 !important;font-weight:850 !important;}}
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# Display helpers
# =========================================================
def section_header(title: str) -> None:
    st.markdown(f'<div class="corp-header">{title}</div>', unsafe_allow_html=True)


def status_box(kind: str, html: str) -> None:
    st.markdown(f'<div class="status-{kind}">{html}</div>', unsafe_allow_html=True)


def pill(text: str, kind: str = "blue") -> None:
    st.markdown(f'<span class="pill pill-{kind}">{text}</span>', unsafe_allow_html=True)


def fmt_eur(x: float) -> str:
    return "—" if pd.isna(x) else f"€{x:,.0f}"


def fmt_mwh(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} MWh"


def fmt_price(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} €/MWh"


def chart_layout(title: str, subtitle: str = "", height: int = 470) -> dict:
    title_text = f"<b>{title}</b><br><sup>{subtitle}</sup>" if subtitle else f"<b>{title}</b>"
    return dict(
        title=dict(text=title_text, x=0.01, xanchor="left"),
        height=height,
        margin=dict(l=58, r=58, t=78, b=52),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, bgcolor="rgba(255,255,255,0.85)"),
        font=dict(size=12, color="#111827"),
    )


def styled_df(df: pd.DataFrame):
    return df.style.set_table_styles(
        [
            {"selector": "th", "props": [("background-color", CORP_GREY), ("color", "white"), ("font-weight", "750"), ("font-size", "13px"), ("text-align", "center"), ("padding", "9px 8px")]},
            {"selector": "td", "props": [("font-size", "12px"), ("padding", "7px 8px")]},
        ]
    )

# =========================================================
# Auth and SCADA helpers
# =========================================================
def get_cookie() -> str:
    """Cookie auth exactly as the working Solarpark/UNITY test page."""
    cookie = ""
    try:
        cookie = str(st.secrets.get("SOLARPARK_COOKIE", "") or "")
    except Exception:
        cookie = ""
    if not cookie:
        cookie = os.getenv("SOLARPARK_COOKIE", "")
    cookie = str(cookie).strip().strip('"').strip("'")
    if not cookie:
        status_box(
            "danger",
            "Missing <b>SOLARPARK_COOKIE</b>. Add it to local <code>.env</code> or Streamlit Secrets, e.g. "
            "<code>SOLARPARK_COOKIE='IFMSCK=...'</code>.",
        )
        st.stop()
    return cookie


def madrid_day_to_utc_str(d: date, end_of_day: bool = False) -> str:
    dt_local = datetime.combine(d, time(0, 0), tzinfo=MADRID_TZ)
    if end_of_day:
        dt_local = dt_local + timedelta(days=1)
    return dt_local.astimezone(UTC_TZ).strftime("%Y%m%dT%H%M%SZ")


@st.cache_data(show_spinner=False, ttl=900)
def fetch_power_values(start_utc: str, end_utc: str, cookie: str, source_ids: list[str]) -> dict | list:
    url = f"{BASE}/ifms/sources/values/v2"
    params = {"start_date": start_utc, "end_date": end_utc, "millis": "false", "lang": "en"}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Origin": BASE,
        "Referer": f"{BASE}/",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.post(url, params=params, json=source_ids, headers=headers, timeout=120)
    if not r.ok:
        raise RuntimeError(f"Solarpark request failed: HTTP {r.status_code}. URL={r.url}. Body preview={r.text[:500]}")
    return r.json()


def events_payload_to_power_df(payload: dict | list) -> pd.DataFrame:
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        events = payload[0].get("events", [])
    elif isinstance(payload, dict):
        events = payload.get("events", [])
    else:
        events = []

    if not events:
        return pd.DataFrame(columns=["datetime_utc", "datetime_madrid", "site", "source_id", "power_kw"])

    df = pd.DataFrame(events, columns=["datetime_utc", "value", "source_id"])
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], format="%Y%m%dT%H%M%SZ", utc=True, errors="coerce")
    df["datetime_madrid"] = df["datetime_utc"].dt.tz_convert("Europe/Madrid")
    df["power_kw"] = pd.to_numeric(df["value"], errors="coerce")
    df["site"] = df["source_id"].map(ID_TO_SITE)
    df = df.dropna(subset=["datetime_utc", "site", "power_kw"]).copy()
    df["power_kw"] = df["power_kw"].clip(lower=0)
    return df[["datetime_utc", "datetime_madrid", "site", "source_id", "power_kw"]].sort_values(["site", "datetime_madrid"])


def power_10min_to_generation_15min(power_df: pd.DataFrame, zero_night: bool = True, night_start: int = 22, night_end: int = 5) -> pd.DataFrame:
    """Convert 10-min power kW signals to 15-min generation kWh, with optional solar night sanity cleaning."""
    if power_df.empty:
        return pd.DataFrame(columns=["site", "datetime_madrid", "generation_kwh_15min"])

    tmp = power_df.copy()
    tmp["generation_kwh_10min"] = tmp["power_kw"] * (10.0 / 60.0)

    out = (
        tmp.set_index("datetime_madrid")
        .groupby("site")["generation_kwh_10min"]
        .resample("15min")
        .sum()
        .reset_index()
        .rename(columns={"generation_kwh_10min": "generation_kwh_15min_raw"})
    )

    hour_decimal = out["datetime_madrid"].dt.hour + out["datetime_madrid"].dt.minute / 60.0
    is_night = (hour_decimal >= night_start) | (hour_decimal < night_end)
    out["is_night_window"] = is_night
    out["night_generation_kwh_raw"] = np.where(is_night, out["generation_kwh_15min_raw"], 0.0)
    out["generation_kwh_15min"] = np.where(is_night, 0.0, out["generation_kwh_15min_raw"]) if zero_night else out["generation_kwh_15min_raw"]
    out["generation_mwh_15min"] = out["generation_kwh_15min"] / 1000.0
    out["hour_madrid"] = out["datetime_madrid"].dt.floor("h")
    out["month"] = out["datetime_madrid"].dt.strftime("%Y-%m")
    return out.sort_values(["site", "datetime_madrid"]).reset_index(drop=True)


def make_month_summary(gen15: pd.DataFrame) -> pd.DataFrame:
    if gen15.empty:
        return pd.DataFrame(columns=["site", "generation_mwh", "obs_15min"])
    out = gen15.groupby("site", as_index=False).agg(generation_mwh=("generation_mwh_15min", "sum"), obs_15min=("generation_mwh_15min", "count"))
    return out.sort_values("site")


def make_24h_average_profile(gen15: pd.DataFrame) -> pd.DataFrame:
    if gen15.empty:
        return pd.DataFrame(columns=["site", "time_of_day", "hour_decimal", "avg_generation_kwh_15min", "avg_power_kw"])
    tmp = gen15.copy()
    tmp["time_of_day"] = tmp["datetime_madrid"].dt.strftime("%H:%M")
    tmp["hour_decimal"] = tmp["datetime_madrid"].dt.hour + tmp["datetime_madrid"].dt.minute / 60.0
    profile = (
        tmp.groupby(["site", "time_of_day", "hour_decimal"], as_index=False)
        .agg(avg_generation_kwh_15min=("generation_kwh_15min", "mean"), obs=("generation_kwh_15min", "count"))
        .sort_values(["site", "hour_decimal"])
    )
    profile["avg_power_kw"] = profile["avg_generation_kwh_15min"] / 0.25
    return profile

# =========================================================
# Price helpers
# =========================================================
def _find_col(cols, candidates):
    norm = {str(c).strip().lower().replace(" ", "_"): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower().replace(" ", "_")
        if key in norm:
            return norm[key]
    for c in cols:
        lc = str(c).strip().lower()
        for cand in candidates:
            if cand.strip().lower() in lc:
                return c
    return None


def _to_numeric_price(s: pd.Series) -> pd.Series:
    txt = s.astype(str).str.strip()
    # Handles both 1.234,56 and 123,45 formats.
    mask_eu = txt.str.contains(",", regex=False) & txt.str.contains(".", regex=False)
    txt = txt.where(~mask_eu, txt.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    txt = txt.where(mask_eu, txt.str.replace(",", ".", regex=False))
    return pd.to_numeric(txt, errors="coerce")


def find_price_workbook() -> Path | None:
    for p in PRICE_WORKBOOK_CANDIDATES:
        if p.exists():
            return p
    return None


@st.cache_data(show_spinner=False)
def load_day_ahead_prices_from_workbook(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    sheet_candidates = ["prices_hourly_avg", "prices_hourly", 0]
    raw = None
    last_error = None
    for sh in sheet_candidates:
        try:
            raw = pd.read_excel(path, sheet_name=sh)
            break
        except Exception as exc:
            last_error = exc
    if raw is None:
        raise ValueError(f"Could not read price workbook {path.name}: {last_error}")

    dt_col = _find_col(raw.columns, ["datetime", "date_time", "timestamp", "datetime_madrid", "fecha_hora"])
    price_col = _find_col(raw.columns, ["price", "price_eur_mwh", "precio", "omie_price", "day_ahead_price", "eur_mwh", "€/mwh"])
    date_col = _find_col(raw.columns, ["date", "fecha", "day", "delivery_date"])
    hour_col = _find_col(raw.columns, ["hour", "hora", "period", "periodo", "he"])

    if price_col is None:
        raise ValueError(f"No price column found in {path.name}. Columns: {list(raw.columns)}")

    out = pd.DataFrame()
    if dt_col is not None:
        dt = pd.to_datetime(raw[dt_col], errors="coerce")
    elif date_col is not None and hour_col is not None:
        d = pd.to_datetime(raw[date_col], errors="coerce")
        h = pd.to_numeric(raw[hour_col], errors="coerce")
        h0 = (h - 1).where(h.between(1, 24), h).clip(lower=0, upper=23)
        dt = d + pd.to_timedelta(h0, unit="h")
    else:
        raise ValueError(f"No datetime or date+hour columns found in {path.name}. Columns: {list(raw.columns)}")

    if getattr(dt.dt, "tz", None) is None:
        out["hour_madrid"] = dt.dt.tz_localize("Europe/Madrid", ambiguous="infer", nonexistent="shift_forward").dt.floor("h")
    else:
        out["hour_madrid"] = dt.dt.tz_convert("Europe/Madrid").dt.floor("h")

    out["price_eur_mwh"] = _to_numeric_price(raw[price_col])
    out = out.dropna(subset=["hour_madrid", "price_eur_mwh"]).copy()

    # Strongly protect against duplicated rows or mixed geographies: average exact duplicate hours only.
    out = out.groupby("hour_madrid", as_index=False)["price_eur_mwh"].mean().sort_values("hour_madrid")
    out["price_source"] = f"Day Ahead workbook — {path.name}"
    return out[["hour_madrid", "price_eur_mwh", "price_source"]]


def price_quality_status(prices: pd.DataFrame) -> tuple[str, str]:
    if prices.empty:
        return "danger", "No hourly Day Ahead prices found for the selected SCADA period."
    p = prices["price_eur_mwh"].dropna()
    avg, med, mn, mx = p.mean(), p.median(), p.min(), p.max()
    missing_dup = prices["hour_madrid"].duplicated().sum()
    if avg > 160 or med > 150 or mx > 600 or mn < -300:
        return (
            "warning",
            f"<b>Price sanity warning:</b> baseload {avg:.1f} €/MWh, median {med:.1f}, min {mn:.1f}, max {mx:.1f}. "
            "This page is using the Day Ahead workbook only; check that the workbook contains the expected OMIE Spain hourly prices for this period.",
        )
    if missing_dup:
        return "warning", f"Price series loaded, but {missing_dup} duplicate hourly rows were detected before aggregation."
    return "ok", f"<b>Price sanity check passed:</b> baseload {avg:.1f} €/MWh, median {med:.1f}, min {mn:.1f}, max {mx:.1f}."

# =========================================================
# Revenue helpers
# =========================================================
def generation_15min_to_hourly_mwh(gen15: pd.DataFrame) -> pd.DataFrame:
    if gen15.empty:
        return pd.DataFrame(columns=["site", "hour_madrid", "generation_mwh"])
    tmp = gen15.copy()
    tmp["hour_madrid"] = tmp["datetime_madrid"].dt.floor("h")
    return tmp.groupby(["site", "hour_madrid"], as_index=False).agg(generation_mwh=("generation_mwh_15min", "sum"))


def calculate_revenues(gen15: pd.DataFrame, prices: pd.DataFrame):
    hourly = generation_15min_to_hourly_mwh(gen15)
    if hourly.empty or prices.empty:
        return hourly, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    joined = hourly.merge(prices[["hour_madrid", "price_eur_mwh", "price_source"]], on="hour_madrid", how="left")
    joined["revenue_eur"] = joined["generation_mwh"] * joined["price_eur_mwh"]
    joined["month"] = joined["hour_madrid"].dt.strftime("%Y-%m")
    joined["year"] = joined["hour_madrid"].dt.year
    joined["is_priced"] = joined["price_eur_mwh"].notna()

    price_month = prices.copy()
    price_month["month"] = price_month["hour_madrid"].dt.strftime("%Y-%m")
    baseload_month = price_month.groupby("month", as_index=False).agg(baseload_price_eur_mwh=("price_eur_mwh", "mean"), price_hours=("price_eur_mwh", "count"))

    def agg_site(g: pd.DataFrame) -> pd.Series:
        gen = g["generation_mwh"].sum()
        rev = g["revenue_eur"].sum(skipna=True)
        captured = rev / gen if gen else np.nan
        return pd.Series({
            "generation_mwh": gen,
            "revenue_eur": rev,
            "captured_price_eur_mwh": captured,
            "priced_hours": int(g["is_priced"].sum()),
            "missing_price_hours": int((~g["is_priced"]).sum()),
        })

    monthly = joined.groupby(["site", "month"], dropna=False).apply(agg_site).reset_index()
    monthly = monthly.merge(baseload_month, on="month", how="left")
    monthly["capture_factor_pct"] = monthly["captured_price_eur_mwh"] / monthly["baseload_price_eur_mwh"] * 100

    price_year = prices.copy()
    price_year["year"] = price_year["hour_madrid"].dt.year
    baseload_year = price_year.groupby("year", as_index=False).agg(baseload_price_eur_mwh=("price_eur_mwh", "mean"), price_hours=("price_eur_mwh", "count"))
    annual = joined.groupby(["site", "year"], dropna=False).apply(agg_site).reset_index()
    annual = annual.merge(baseload_year, on="year", how="left")
    annual["capture_factor_pct"] = annual["captured_price_eur_mwh"] / annual["baseload_price_eur_mwh"] * 100

    portfolio = joined.groupby("month", dropna=False).apply(agg_site).reset_index()
    portfolio = portfolio.merge(baseload_month, on="month", how="left")
    portfolio["capture_factor_pct"] = portfolio["captured_price_eur_mwh"] / portfolio["baseload_price_eur_mwh"] * 100
    return joined, monthly, annual, portfolio


def display_revenue_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No data to display.")
        return
    out = df.copy()
    rename = {
        "site": "Site",
        "month": "Month",
        "year": "Year",
        "generation_mwh": "Generation (MWh)",
        "revenue_eur": "Revenue (€)",
        "captured_price_eur_mwh": "Captured price (€/MWh)",
        "baseload_price_eur_mwh": "Baseload price (€/MWh)",
        "capture_factor_pct": "Capture factor (%)",
        "priced_hours": "Priced hours",
        "missing_price_hours": "Missing price hours",
        "price_hours": "Price hours in period",
    }
    out = out.rename(columns=rename)
    ordered = [v for v in rename.values() if v in out.columns]
    out = out[ordered]
    fmt = {
        "Generation (MWh)": "{:,.2f}",
        "Revenue (€)": "€{:,.0f}",
        "Captured price (€/MWh)": "{:,.2f}",
        "Baseload price (€/MWh)": "{:,.2f}",
        "Capture factor (%)": "{:,.1f}%",
        "Priced hours": "{:,.0f}",
        "Missing price hours": "{:,.0f}",
        "Price hours in period": "{:,.0f}",
    }
    st.dataframe(styled_df(out).format({k: v for k, v in fmt.items() if k in out.columns}), use_container_width=True, hide_index=True)

# =========================================================
# Sidebar
# =========================================================
st.sidebar.title("IS2 controls")

# Default previous full month, but user can change it.
today_madrid = datetime.now(MADRID_TZ).date()
default_end = today_madrid.replace(day=1) - timedelta(days=1)
default_start = default_end.replace(day=1)

start_day = st.sidebar.date_input("Start day", value=default_start)
end_day = st.sidebar.date_input("End day inclusive", value=default_end)

selected_sites = st.sidebar.multiselect("Sites", options=list(POWER_SOURCE_IDS.keys()), default=list(POWER_SOURCE_IDS.keys()))

st.sidebar.markdown("---")
st.sidebar.subheader("Solar cleaning")
zero_night = st.sidebar.checkbox("Set impossible night generation to zero", value=True)
nc1, nc2 = st.sidebar.columns(2)
night_start = nc1.number_input("Night starts", min_value=18, max_value=24, value=22, step=1)
night_end = nc2.number_input("Night ends", min_value=0, max_value=9, value=5, step=1)

st.sidebar.markdown("---")
st.sidebar.subheader("Price source")
price_workbook = find_price_workbook()
if price_workbook is not None:
    st.sidebar.success(f"Day Ahead workbook found: {price_workbook.name}")
else:
    st.sidebar.error("Day Ahead workbook not found in /data")

show_diagnostics = st.sidebar.checkbox("Show diagnostics", value=True)

# =========================================================
# Main
# =========================================================
st.title("IS2 SCADA generation & day-ahead revenues")
st.caption("Generation is pulled from Solarpark/UNITY SCADA with Cookie auth. Revenues use the same hourly Day Ahead price workbook as the Day Ahead tab.")

if end_day < start_day:
    status_box("danger", "End day must be >= start day.")
    st.stop()
if not selected_sites:
    status_box("warning", "Select at least one site.")
    st.stop()

selected_source_ids = [POWER_SOURCE_IDS[s] for s in selected_sites]
start_utc = madrid_day_to_utc_str(start_day)
end_utc = madrid_day_to_utc_str(end_day, end_of_day=True)

section_header("SCADA request")
pill(f"UTC window: {start_utc} → {end_utc}", "grey")
pill(f"Selected sites: {len(selected_sites)}", "green")
pill("Auth: SOLARPARK_COOKIE", "blue")

run = st.button("Fetch SCADA generation and calculate revenues", type="primary", use_container_width=True)
if not run:
    status_box("ok", "Ready. Select the dates/sites and click <b>Fetch SCADA generation and calculate revenues</b>.")
    st.stop()

cookie = get_cookie()

with st.spinner("Fetching Solarpark/UNITY SCADA values..."):
    try:
        payload = fetch_power_values(start_utc, end_utc, cookie, selected_source_ids)
    except Exception as exc:
        status_box("danger", f"Could not fetch Solarpark values: <code>{exc}</code>")
        st.stop()

power_df = events_payload_to_power_df(payload)
if power_df.empty:
    status_box("danger", "No SCADA power events returned for the configured source IDs and selected period.")
    with st.expander("Raw payload preview", expanded=False):
        st.json(payload if isinstance(payload, dict) else payload[:1])
    st.stop()

gen15 = power_10min_to_generation_15min(power_df, zero_night=zero_night, night_start=int(night_start), night_end=int(night_end))
month_summary = make_month_summary(gen15)
profile24 = make_24h_average_profile(gen15)

section_header("SCADA generation profile")
raw_night_kwh = gen15["night_generation_kwh_raw"].sum()
raw_total_kwh = gen15["generation_kwh_15min_raw"].sum()
clean_total_mwh = gen15["generation_mwh_15min"].sum()
raw_night_pct = raw_night_kwh / raw_total_kwh * 100 if raw_total_kwh else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Clean generation", fmt_mwh(clean_total_mwh))
c2.metric("Raw night generation", fmt_mwh(raw_night_kwh / 1000.0), f"{raw_night_pct:.2f}% raw")
c3.metric("SCADA points", f"{len(power_df):,}")
c4.metric("15-min intervals", f"{len(gen15):,}")

if raw_night_kwh > max(0.005 * raw_total_kwh, 10):
    status_box("warning", f"<b>Solar sanity warning:</b> raw SCADA has {raw_night_kwh/1000:,.2f} MWh in the configured night window ({raw_night_pct:.2f}% of raw generation). Cleaning applied: <b>{'night values set to zero' if zero_night else 'night values kept'}</b>.")
else:
    status_box("ok", "<b>Solar sanity check passed:</b> night generation is negligible.")

fig_gen = go.Figure()
for site in selected_sites:
    sub = gen15[gen15["site"] == site]
    fig_gen.add_trace(go.Scatter(
        x=sub["datetime_madrid"],
        y=sub["generation_kwh_15min"],
        mode="lines",
        name=site,
        line=dict(width=1.8),
        hovertemplate="%{x|%d-%b %H:%M}<br>%{y:,.1f} kWh/15-min<extra>" + site + "</extra>",
    ))
fig_gen.update_layout(**chart_layout("15-min SCADA generation", "10-min power values converted to 15-min generation; timestamps in Europe/Madrid", 500))
fig_gen.update_xaxes(title="Madrid date and hour", showgrid=False)
fig_gen.update_yaxes(title="Generation (kWh / 15-min)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8")
st.plotly_chart(fig_gen, use_container_width=True)

# 24h profile chart
fig_profile = go.Figure()
for site in selected_sites:
    sub = profile24[profile24["site"] == site]
    fig_profile.add_trace(go.Scatter(
        x=sub["hour_decimal"],
        y=sub["avg_power_kw"],
        mode="lines",
        name=site,
        line=dict(width=2.4),
        hovertemplate="Hour %{x:.2f}<br>Avg power %{y:,.0f} kW<extra>" + site + "</extra>",
    ))
fig_profile.update_layout(**chart_layout("Average 24h generation profile", "Average equivalent power across selected days", 430))
fig_profile.update_xaxes(title="Hour of day", range=[0, 24], dtick=2, showgrid=False)
fig_profile.update_yaxes(title="Equivalent average power (kW)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8")
st.plotly_chart(fig_profile, use_container_width=True)

# =========================================================
# Prices and revenues
# =========================================================
section_header("Day-ahead revenues — operational parks")

if price_workbook is None:
    status_box("danger", "Could not find the Day Ahead workbook. Expected <code>data/hourly_avg_price_since2021.xlsx</code>. Revenues have not been calculated.")
    st.stop()

try:
    prices_all = load_day_ahead_prices_from_workbook(str(price_workbook))
except Exception as exc:
    status_box("danger", f"Could not load Day Ahead prices from workbook: <code>{exc}</code>")
    st.stop()

prices = prices_all[
    (prices_all["hour_madrid"] >= gen15["hour_madrid"].min()) &
    (prices_all["hour_madrid"] <= gen15["hour_madrid"].max())
].copy()

pill(f"Price source: {price_workbook.name}", "blue")
pill(f"Price hours loaded: {len(prices):,}", "green")

status, message = price_quality_status(prices)
status_box(status, message)

if prices.empty:
    st.stop()

joined_rev, monthly_rev, annual_rev, portfolio_month = calculate_revenues(gen15, prices)

portfolio_gen = joined_rev["generation_mwh"].sum()
portfolio_rev = joined_rev["revenue_eur"].sum(skipna=True)
portfolio_captured = portfolio_rev / portfolio_gen if portfolio_gen else np.nan
baseload = prices["price_eur_mwh"].mean()
capture_factor = portfolio_captured / baseload * 100 if baseload and pd.notna(portfolio_captured) else np.nan
priced_pct = joined_rev["is_priced"].mean() * 100 if len(joined_rev) else 0.0

r1, r2, r3, r4 = st.columns(4)
r1.metric("Portfolio revenue", fmt_eur(portfolio_rev))
r2.metric("Captured price", fmt_price(portfolio_captured))
r3.metric("Baseload price", fmt_price(baseload))
r4.metric("Capture factor", f"{capture_factor:.1f}%" if pd.notna(capture_factor) else "—", f"{priced_pct:.1f}% priced hours")

hourly_portfolio = joined_rev.groupby("hour_madrid", as_index=False).agg(generation_mwh=("generation_mwh", "sum"), revenue_eur=("revenue_eur", "sum"))
hourly_portfolio = hourly_portfolio.merge(prices[["hour_madrid", "price_eur_mwh"]], on="hour_madrid", how="left")

fig_rev = go.Figure()
fig_rev.add_trace(go.Bar(
    x=hourly_portfolio["hour_madrid"],
    y=hourly_portfolio["generation_mwh"],
    name="SCADA generation",
    marker=dict(color=CORP_GREEN),
    opacity=0.82,
    yaxis="y",
    hovertemplate="%{x|%d-%b %H:%M}<br>Generation %{y:,.2f} MWh<extra></extra>",
))
fig_rev.add_trace(go.Scatter(
    x=hourly_portfolio["hour_madrid"],
    y=hourly_portfolio["price_eur_mwh"],
    name="Day-ahead price",
    mode="lines",
    line=dict(color=CORP_BLUE, width=2.7),
    yaxis="y2",
    hovertemplate="%{x|%d-%b %H:%M}<br>Price %{y:,.2f} €/MWh<extra></extra>",
))
fig_rev.update_layout(**chart_layout("Hourly SCADA generation and day-ahead price", "Generation aggregated from 15-min intervals; hourly price joined by Madrid hour", 470))
fig_rev.update_layout(
    yaxis=dict(title="Generation (MWh)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8"),
    yaxis2=dict(title="Price (€/MWh)", overlaying="y", side="right", showgrid=False),
    bargap=0.04,
)
fig_rev.update_xaxes(title="Madrid date and hour", showgrid=False)
st.plotly_chart(fig_rev, use_container_width=True)

st.markdown("#### Portfolio monthly summary")
display_revenue_table(portfolio_month.sort_values("month"))

tab_month, tab_annual, tab_detail = st.tabs(["Monthly by site", "Annual by site", "Hourly revenue detail"])
with tab_month:
    display_revenue_table(monthly_rev.sort_values(["site", "month"]))
with tab_annual:
    display_revenue_table(annual_rev.sort_values(["site", "year"]))
with tab_detail:
    detail = joined_rev[["site", "hour_madrid", "generation_mwh", "price_eur_mwh", "revenue_eur", "is_priced"]].rename(columns={
        "site": "Site",
        "hour_madrid": "Hour Madrid",
        "generation_mwh": "Generation (MWh)",
        "price_eur_mwh": "Price (€/MWh)",
        "revenue_eur": "Revenue (€)",
        "is_priced": "Priced",
    })
    st.dataframe(styled_df(detail.head(800)).format({"Generation (MWh)": "{:,.4f}", "Price (€/MWh)": "{:,.2f}", "Revenue (€)": "€{:,.2f}"}), use_container_width=True, hide_index=True)
    st.caption("Showing first 800 rows. Download the full detail below.")

# =========================================================
# Diagnostics and downloads
# =========================================================
if show_diagnostics:
    section_header("Diagnostics")
    with st.expander("SCADA raw sample", expanded=False):
        st.dataframe(power_df.head(200), use_container_width=True, hide_index=True)
    with st.expander("Monthly generation summary", expanded=False):
        st.dataframe(styled_df(month_summary).format({"generation_mwh": "{:,.2f}", "obs_15min": "{:,.0f}"}), use_container_width=True, hide_index=True)
    with st.expander("Price diagnostics", expanded=False):
        price_diag = prices["price_eur_mwh"].describe().to_frame("price_eur_mwh").T
        st.dataframe(styled_df(price_diag).format("{:,.2f}"), use_container_width=True)
        st.dataframe(prices.head(100), use_container_width=True, hide_index=True)
    with st.expander("Missing price hours", expanded=False):
        missing = joined_rev[joined_rev["price_eur_mwh"].isna()].copy()
        st.write(f"Missing hourly price rows after join: {len(missing):,}")
        if not missing.empty:
            st.dataframe(missing.head(200), use_container_width=True, hide_index=True)

section_header("Downloads")
d1, d2, d3 = st.columns(3)
d1.download_button("SCADA generation 15-min", gen15.to_csv(index=False).encode("utf-8"), f"is2_scada_generation_15min_{start_day}_{end_day}.csv", "text/csv", use_container_width=True)
d2.download_button("Hourly revenue detail", joined_rev.to_csv(index=False).encode("utf-8"), f"is2_hourly_revenue_detail_{start_day}_{end_day}.csv", "text/csv", use_container_width=True)
d3.download_button("Monthly revenue metrics", monthly_rev.to_csv(index=False).encode("utf-8"), f"is2_monthly_revenue_metrics_{start_day}_{end_day}.csv", "text/csv", use_container_width=True)
