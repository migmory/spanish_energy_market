from __future__ import annotations

import base64
import os
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pulp
import requests
import streamlit as st
from dotenv import load_dotenv
from matplotlib.backends.backend_pdf import PdfPages

# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

DATA_DIR = BASE_DIR / "data"
HISTORICAL_DIR = BASE_DIR / "historical_data"
OUTPUT_DIR = BASE_DIR / "reports"
OUTPUT_DIR.mkdir(exist_ok=True)

MADRID_TZ = ZoneInfo("Europe/Madrid")
LIVE_START_DATE = date(2026, 1, 1)

HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_SOLAR_FILE = DATA_DIR / "p48solar_since21.csv"
HIST_WORKBOOK_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_MIX_FILE = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"
HIST_INSTALLED_CAP_FILE = DATA_DIR / "installed_capacity_monthly.xlsx"
DEMAND_RAW_FILE = HISTORICAL_DIR / "demand_p48_total_10027_raw.csv"

LOGO_CANDIDATES = [
    DATA_DIR / "nexwell-power-.jpg",
    DATA_DIR / "nexwell-power.jpg",
    BASE_DIR / "Data" / "nexwell-power-.jpg",
    BASE_DIR / "Data" / "nexwell-power.jpg",
    Path("/mnt/data/nexwell-power-.jpg"),
    Path("/mnt/data/ghostwriter_images/context/6d7e5c15-89e2-5173-8381-e2b0a0d78467.jpg"),
]

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
DEMAND_INDICATOR_ID = 10027

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREY = "#4B5563"
LIGHT_GREY = "#F3F4F6"

YEAR_COLORS = ["#1D4ED8", "#059669", "#D97706", "#7C3AED", "#DC2626"]

LOCAL_MIX_TECH_MAP = {
    "Hidráulica": "Hydro",
    "Hidroeólica": "Other renewables",
    "Turbinación bombeo": "Pumped hydro",
    "Nuclear": "Nuclear",
    "Carbón": "Coal",
    "Fuel + Gas": "Fuel + Gas",
    "Turbina de vapor": "Steam turbine",
    "Ciclo combinado": "CCGT",
    "Eólica": "Wind",
    "Solar fotovoltaica": "Solar PV",
    "Solar térmica": "Solar thermal",
    "Otras renovables": "Other renewables",
    "Cogeneración": "CHP",
    "Residuos no renovables": "Other non-renewables",
    "Residuos renovables": "Biomass",
    "Biogás": "Biogas",
    "Biomasa": "Biomass",
}

TECH_ORDER = [
    "CCGT", "Hydro", "Pumped hydro", "Nuclear", "Solar PV", "Solar thermal",
    "Wind", "CHP", "Biomass", "Biogas", "Other renewables", "Coal",
    "Fuel + Gas", "Steam turbine", "Other non-renewables"
]

REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {"geo_trunc": "electric_system", "geo_limit": "peninsular", "geo_ids": "8741"}

# =========================================================
# STREAMLIT PAGE
# =========================================================
st.set_page_config(page_title="Monthly YTD Email Report", layout="wide")
st.markdown(
    """
    <style>
    h1 {font-size: 2rem !important;}
    h2, h3 {font-size: 1.2rem !important;}
    .stDataFrame td, .stDataFrame th {font-size: 13px !important;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Nexwell Power - Monthly YTD Email Report")

if "email_admin_password" in st.secrets:
    pwd = st.text_input("Password", type="password")
    if pwd != st.secrets["email_admin_password"]:
        st.stop()

# =========================================================
# HELPERS
# =========================================================
def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def parse_emails(raw: str) -> list[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def fmt_num(x, decimals=2, suffix="") -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.{decimals}f}{suffix}"


def fmt_pct(x, decimals=1) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:.{decimals}%}"


def pct_change(current, previous):
    if previous is None or pd.isna(previous) or previous == 0 or pd.isna(current):
        return pd.NA
    return current / previous - 1


def value_change(current, previous):
    if pd.isna(current) or pd.isna(previous):
        return pd.NA
    return current - previous


def period_filter(df: pd.DataFrame, start: date, end: date) -> pd.Series:
    """
    Robust date filter. Some loaders can return an empty frame or a frame whose
    datetime column is object/string typed. Convert locally so Streamlit does
    not crash with: Can only use .dt accessor with datetimelike values.
    """
    if df is None or df.empty or "datetime" not in df.columns:
        return pd.Series(False, index=df.index if df is not None else None)

    dt = pd.to_datetime(df["datetime"], errors="coerce")
    return (dt.dt.date >= start) & (dt.dt.date <= end)


def month_end_day(year: int, month: int) -> int:
    return int((pd.Timestamp(date(year, month, 1)) + pd.offsets.MonthEnd(0)).day)


def month_window(year: int, month: int, cutoff_month: int, cutoff_day: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month < cutoff_month:
        end = date(year, month, month_end_day(year, month))
    elif month == cutoff_month:
        end = date(year, month, min(cutoff_day, month_end_day(year, month)))
    else:
        end = date(year, month, month_end_day(year, month))
    return start, end


def find_logo_path() -> Path | None:
    for p in LOGO_CANDIDATES:
        if p.exists():
            return p
    return None


def image_to_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("utf-8")


def df_to_html_table(df: pd.DataFrame, pct_cols: list[str] | None = None) -> str:
    pct_cols = pct_cols or []
    out = df.copy()
    for c in out.columns:
        if c in pct_cols:
            out[c] = out[c].map(lambda v: "" if pd.isna(v) else f"{v:.1%}")
        elif pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].map(lambda v: "" if pd.isna(v) else f"{v:,.2f}")
    css = """
    <style>
    table.email-table{border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:12px;}
    table.email-table th{background:#4B5563;color:white;border:1px solid #d1d5db;padding:7px;text-align:center;}
    table.email-table td{border:1px solid #e5e7eb;padding:7px;text-align:center;}
    </style>
    """
    return css + out.to_html(index=False, classes="email-table", border=0, escape=False)


def metric_cards_html(items: list[tuple[str, str, str | None]]) -> str:
    cards = []
    for title, value, delta in items:
        delta_html = f'<div style="font-size:12px;color:#475569;margin-top:4px;">{delta}</div>' if delta else ""
        cards.append(f"""
        <div style="flex:1 1 180px;min-width:180px;border:1px solid #dbe4ea;border-top:4px solid {CORP_GREEN};border-radius:14px;padding:14px;background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);box-shadow:0 2px 8px rgba(15,23,42,0.05);">
          <div style="font-size:12px;color:#475569;margin-bottom:5px;font-weight:600;">{title}</div>
          <div style="font-size:20px;font-weight:700;color:#111827;">{value}</div>
          {delta_html}
        </div>
        """)
    return '<div style="display:flex;gap:10px;flex-wrap:wrap;">' + ''.join(cards) + '</div>'


def section_header_html(title: str, icon: str = "") -> str:
    icon_html = f'<span style="font-size:18px;margin-right:8px;">{icon}</span>' if icon else ''
    return (
        f'<div style="margin:20px 0 10px 0;padding:10px 14px;border-radius:12px;'
        f'background:linear-gradient(90deg,#ecfdf5 0%,#f8fafc 100%);border:1px solid #d1fae5;'
        f'display:flex;align-items:center;">{icon_html}'
        f'<span style="font-size:17px;font-weight:700;color:#0f172a;">{title}</span></div>'
    )


def fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def year_color_map(years: list[int]) -> dict[int, str]:
    years = sorted(set(years))
    return {y: YEAR_COLORS[i % len(YEAR_COLORS)] for i, y in enumerate(years)}


def _empty_df(columns: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(columns=columns)
    if "datetime" in columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df


# =========================================================
# HISTORICAL LOADERS
# =========================================================
def parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT
    try:
        ts = pd.to_datetime(s, utc=True, errors="raise")
        return ts.tz_convert("Europe/Madrid").tz_localize(None)
    except Exception:
        pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise")
    except Exception:
        return pd.NaT


@st.cache_data(show_spinner=False)
def load_historical_prices() -> pd.DataFrame:
    if not HIST_PRICES_FILE.exists():
        return _empty_df(["datetime", "price"])
    try:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name=0)
    if "price" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "price"})
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df.dropna(subset=["datetime", "price"])[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_historical_solar() -> pd.DataFrame:
    if HIST_WORKBOOK_FILE.exists():
        try:
            df = pd.read_excel(HIST_WORKBOOK_FILE, sheet_name="solar_hourly_best")
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
            if "solar_source" not in df.columns:
                df["solar_source"] = "Historical workbook"
            return df.dropna(subset=["datetime", "solar_best_mw"])[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)
        except Exception:
            pass
    if not HIST_SOLAR_FILE.exists():
        return _empty_df(["datetime", "solar_best_mw", "solar_source"])
    df = pd.read_csv(HIST_SOLAR_FILE)
    if "solar_best_mw" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "solar_best_mw"})
    if "solar_source" not in df.columns:
        df["solar_source"] = "Historical file"
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
    return df.dropna(subset=["datetime", "solar_best_mw"])[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_generation_mix_daily() -> pd.DataFrame:
    if not HIST_MIX_FILE.exists():
        return _empty_df(["datetime", "technology", "energy_mwh", "data_source"])
    raw = pd.read_excel(HIST_MIX_FILE, sheet_name="data", header=None)
    date_values = raw.iloc[4, 1:].tolist()
    dates = []
    for v in date_values:
        dt = pd.to_datetime(v, utc=True, errors="coerce")
        if pd.notna(dt):
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None).normalize()
        else:
            dt = parse_mixed_date(v)
            if pd.notna(dt):
                dt = pd.Timestamp(dt).normalize()
        dates.append(dt)
    records = []
    for _, row in raw.iloc[5:19, :].iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        if tech.lower().startswith("generación total") or tech.lower().startswith("generacion total"):
            continue
        vals = pd.to_numeric(row.iloc[1:], errors="coerce")
        for dt, val in zip(dates, vals):
            if pd.notna(dt) and pd.notna(val):
                records.append({
                    "datetime": pd.Timestamp(dt).normalize(),
                    "technology": tech,
                    "energy_mwh": float(val) * 1000.0,
                    "data_source": "Historical file",
                })
    out = pd.DataFrame(records)
    if out.empty:
        return _empty_df(["datetime", "technology", "energy_mwh", "data_source"])
    return out.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_installed_capacity_monthly() -> pd.DataFrame:
    if not HIST_INSTALLED_CAP_FILE.exists():
        return _empty_df(["datetime", "technology", "capacity_mw"])
    raw = pd.read_excel(HIST_INSTALLED_CAP_FILE, sheet_name="data", header=None)
    dates = [parse_mixed_date(v) for v in raw.iloc[4, 1:].tolist()]
    records = []
    for _, row in raw.iloc[5:19, :].iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        for col_idx, dt in enumerate(dates, start=1):
            if pd.isna(dt):
                continue
            val = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.notna(val):
                records.append({
                    "datetime": pd.Timestamp(dt).to_period("M").to_timestamp(),
                    "technology": tech,
                    "capacity_mw": float(val),
                })
    out = pd.DataFrame(records)
    if out.empty:
        return _empty_df(["datetime", "technology", "capacity_mw"])
    return out.groupby(["datetime", "technology"], as_index=False)["capacity_mw"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_demand_raw() -> pd.DataFrame:
    if not DEMAND_RAW_FILE.exists():
        return _empty_df(["datetime", "demand_mwh"])
    df = pd.read_csv(DEMAND_RAW_FILE)
    if "datetime" not in df.columns:
        return _empty_df(["datetime", "demand_mwh"])
    value_col = "value" if "value" in df.columns else ("demand_mw" if "demand_mw" in df.columns else None)
    if value_col is None:
        return _empty_df(["datetime", "demand_mwh"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["datetime", value_col]).sort_values("datetime")
    diffs = df["datetime"].diff().dt.total_seconds().div(3600).dropna()
    interval_h = 0.25 if not diffs.empty and diffs.median() <= 0.30 else 1.0
    df["demand_mwh"] = df[value_col] * interval_h
    df["datetime"] = df["datetime"].dt.floor("h")
    return df.groupby("datetime", as_index=False)["demand_mwh"].sum().sort_values("datetime").reset_index(drop=True)


# =========================================================
# ESIOS / REE LIVE HELPERS
# =========================================================
def require_esios_token() -> str | None:
    return (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip() or None


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def parse_esios_indicator(raw_json: dict, source_name: str) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return _empty_df(["datetime", "value", "source", "geo_name", "geo_id"])
    df = pd.DataFrame(values)
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None
    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    else:
        geo = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo == "españa").any():
            df = df[geo == "españa"].copy()
        elif (geo == "espana").any():
            df = df[geo == "espana"].copy()
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df["datetime"] = dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = source_name
    return df.dropna(subset=["datetime", "value"])[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(indicator_id: int, start_day: date, end_day: date, token: str) -> pd.DataFrame:
    if start_day > end_day:
        return _empty_df(["datetime", "value"])
    frames = []
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    chunk_start = start_day
    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=13))
        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(chunk_end + timedelta(days=1), tz="Europe/Madrid")
        params = {
            "start_date": start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time_trunc": "quarter_hour" if chunk_start >= date(2025, 10, 1) else "hour",
        }
        try:
            resp = requests.get(url, headers=build_headers(token), params=params, timeout=(15, 90))
            resp.raise_for_status()
            parsed = parse_esios_indicator(resp.json(), f"esios_{indicator_id}")
            if not parsed.empty:
                frames.append(parsed)
        except Exception:
            pass
        chunk_start = chunk_end + timedelta(days=1)
    if not frames:
        return _empty_df(["datetime", "value"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_ree_widget(category: str, widget: str, start_day: date, end_day: date, time_trunc: str = "day") -> dict:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": time_trunc,
        **REE_PENINSULAR_PARAMS,
    }
    url = f"{REE_API_BASE}/{category}/{widget}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_ree_included_series(payload: dict, value_field: str = "value") -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or item.get("id")
        for val in attrs.get("values", []) or []:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None)
            rows.append({
                "datetime": dt,
                "title": str(title).strip(),
                value_field: pd.to_numeric(val.get(value_field), errors="coerce"),
            })
    return pd.DataFrame(rows)


def normalize_ree_energy_to_mwh(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    max_abs = vals.abs().max(skipna=True) if not vals.empty else None
    if pd.notna(max_abs) and max_abs < 10000:
        return vals * 1000.0
    return vals


def _postprocess_ree_mix_df(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    if df.empty:
        return _empty_df(["datetime", "technology", "energy_mwh", "data_source"])
    if freq == "month":
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    else:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.normalize()
    df["technology"] = df["title"].map(lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip()))
    df["energy_mwh"] = normalize_ree_energy_to_mwh(df["value"])
    df["data_source"] = "REE API"
    df = df.dropna(subset=["datetime", "technology", "energy_mwh"]).copy()
    df = df.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum()
    return df.sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_mix_daily_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return _empty_df(["datetime", "technology", "energy_mwh", "data_source"])
    try:
        payload = fetch_ree_widget("generacion", "estructura-generacion", start_day, end_day, time_trunc="day")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return _empty_df(["datetime", "technology", "energy_mwh", "data_source"])
    return _postprocess_ree_mix_df(df, freq="day")


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_demand_monthly_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return _empty_df(["datetime", "demand_mwh"])
    try:
        payload = fetch_ree_widget("demanda", "ire-general", start_day, end_day, time_trunc="month")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return _empty_df(["datetime", "demand_mwh"])
    if df.empty:
        return _empty_df(["datetime", "demand_mwh"])
    df["title_norm"] = df["title"].astype(str).str.strip().str.lower()
    preferred = df[df["title_norm"].str.contains("real", na=False)].copy()
    if preferred.empty:
        preferred = df.copy()
    preferred["datetime"] = pd.to_datetime(preferred["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    preferred["demand_mwh"] = pd.to_numeric(preferred["value"], errors="coerce") * 1000.0
    preferred = preferred.dropna(subset=["datetime", "demand_mwh"]).copy()
    return preferred.groupby("datetime", as_index=False)["demand_mwh"].sum().sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_installed_capacity_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return _empty_df(["datetime", "technology", "capacity_mw"])
    try:
        payload = fetch_ree_widget("generacion", "potencia-instalada", start_day, end_day, time_trunc="month")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return _empty_df(["datetime", "technology", "capacity_mw"])
    if df.empty:
        return _empty_df(["datetime", "technology", "capacity_mw"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    df["technology"] = df["title"].map(lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip()))
    df["capacity_mw"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "technology", "capacity_mw"]).copy()
    return df.groupby(["datetime", "technology"], as_index=False)["capacity_mw"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


def append_live_price_solar_demand(
    price_hourly: pd.DataFrame,
    solar_hourly: pd.DataFrame,
    demand_hourly: pd.DataFrame,
    start_day: date,
    end_day: date,
    token: str | None,
):
    if not token or end_day < LIVE_START_DATE:
        return price_hourly, solar_hourly, demand_hourly
    live_start = max(start_day, LIVE_START_DATE)

    raw_price = fetch_esios_range(PRICE_INDICATOR_ID, live_start, end_day, token)
    if not raw_price.empty:
        lp = raw_price.copy()
        lp["datetime"] = lp["datetime"].dt.floor("h")
        lp = lp.groupby("datetime", as_index=False)["value"].mean().rename(columns={"value": "price"})
        price_hourly = pd.concat([price_hourly[~price_hourly["datetime"].isin(lp["datetime"])], lp], ignore_index=True)

    raw_solar = fetch_esios_range(SOLAR_P48_INDICATOR_ID, live_start, end_day, token)
    if not raw_solar.empty:
        ls = raw_solar.copy()
        ls["datetime"] = ls["datetime"].dt.floor("h")
        ls = ls.groupby("datetime", as_index=False)["value"].mean().rename(columns={"value": "solar_best_mw"})
        ls["solar_source"] = "ESIOS P48"
        solar_hourly = pd.concat([solar_hourly[~solar_hourly["datetime"].isin(ls["datetime"])], ls], ignore_index=True)

    raw_demand = fetch_esios_range(DEMAND_INDICATOR_ID, live_start, end_day, token)
    if not raw_demand.empty:
        ld = raw_demand.copy().sort_values("datetime")
        diffs = ld["datetime"].diff().dt.total_seconds().div(3600).dropna()
        interval_h = 0.25 if not diffs.empty and diffs.median() <= 0.30 else 1.0
        ld["demand_mwh"] = pd.to_numeric(ld["value"], errors="coerce") * interval_h
        ld["datetime"] = ld["datetime"].dt.floor("h")
        ld = ld.groupby("datetime", as_index=False)["demand_mwh"].sum()
        demand_hourly = pd.concat([demand_hourly[~demand_hourly["datetime"].isin(ld["datetime"])], ld], ignore_index=True)

    return (
        price_hourly.sort_values("datetime").reset_index(drop=True),
        solar_hourly.sort_values("datetime").reset_index(drop=True),
        demand_hourly.sort_values("datetime").reset_index(drop=True),
    )


def append_live_mix_capacity(mix_daily: pd.DataFrame, capacity_monthly: pd.DataFrame, current_window_start: date, current_window_end: date):
    live_mix = load_live_mix_daily_from_ree(current_window_start, current_window_end)
    if not live_mix.empty:
        mix_daily = pd.concat([
            mix_daily[~mix_daily["datetime"].isin(live_mix["datetime"].unique())],
            live_mix,
        ], ignore_index=True)
        mix_daily = mix_daily.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)

    live_capacity = load_live_installed_capacity_from_ree(current_window_start, current_window_end)
    if not live_capacity.empty:
        capacity_monthly = pd.concat([
            capacity_monthly[~capacity_monthly[["datetime", "technology"]].apply(tuple, axis=1).isin(live_capacity[["datetime", "technology"]].apply(tuple, axis=1))],
            live_capacity,
        ], ignore_index=True)
        capacity_monthly = capacity_monthly.groupby(["datetime", "technology"], as_index=False)["capacity_mw"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)

    live_demand_monthly = load_live_demand_monthly_from_ree(current_window_start, current_window_end)
    return mix_daily, capacity_monthly, live_demand_monthly


# =========================================================
# METRICS / TABLES
# =========================================================
@dataclass
class PeriodWindow:
    label: str
    year: int
    start: date
    end: date


def compute_price_solar_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, window: PeriodWindow) -> dict:
    p = price_hourly[period_filter(price_hourly, window.start, window.end)].copy()
    s = solar_hourly[period_filter(solar_hourly, window.start, window.end)].copy()
    merged = p.merge(s[["datetime", "solar_best_mw"]], on="datetime", how="left") if not p.empty else pd.DataFrame()
    if not merged.empty:
        merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
        solar_positive = merged[merged["solar_best_mw"] > 0].copy()
        solar_uncurtailed_mwh = merged["solar_best_mw"].sum()
        uncurtailed_value = (merged["price"] * merged["solar_best_mw"]).sum()
        curtailed_mw = merged["solar_best_mw"].where(merged["price"] > 0, 0.0)
        curtailed_mwh = curtailed_mw.sum()
        curtailed_value = (merged["price"].clip(lower=0) * curtailed_mw).sum()
    else:
        solar_positive = pd.DataFrame()
        solar_uncurtailed_mwh = curtailed_mwh = uncurtailed_value = curtailed_value = 0.0
    avg_price = p["price"].mean() if not p.empty else pd.NA
    captured_unc = uncurtailed_value / solar_uncurtailed_mwh if solar_uncurtailed_mwh > 0 else pd.NA
    captured_cur = curtailed_value / curtailed_mwh if curtailed_mwh > 0 else pd.NA
    return {
        "Period": window.label,
        "Avg spot (€/MWh)": avg_price,
        "Captured solar uncurtailed (€/MWh)": captured_unc,
        "Captured solar curtailed (€/MWh)": captured_cur,
        "Solar generation (GWh)": solar_uncurtailed_mwh / 1000.0,
        "Economic curtailment (GWh)": max(solar_uncurtailed_mwh - curtailed_mwh, 0) / 1000.0,
        "Economic curtailment (%)": (max(solar_uncurtailed_mwh - curtailed_mwh, 0) / solar_uncurtailed_mwh) if solar_uncurtailed_mwh > 0 else pd.NA,
        "Negative / zero hours": int((p["price"] <= 0).sum()) if not p.empty else 0,
        "Negative / zero hours (%)": ((p["price"] <= 0).mean()) if not p.empty else pd.NA,
        "Negative / zero solar hours": int(((merged["price"] <= 0) & (merged["solar_best_mw"] > 0)).sum()) if not merged.empty else 0,
        "Negative / zero solar hours (%)": (((merged["price"] <= 0) & (merged["solar_best_mw"] > 0)).mean()) if not merged.empty else pd.NA,
        "Capture rate uncurtailed (%)": captured_unc / avg_price if pd.notna(avg_price) and avg_price != 0 and pd.notna(captured_unc) else pd.NA,
        "Capture rate curtailed (%)": captured_cur / avg_price if pd.notna(avg_price) and avg_price != 0 and pd.notna(captured_cur) else pd.NA,
    }


def build_ytd_metrics_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    cur = compute_price_solar_metrics(price_hourly, solar_hourly, current)
    prev = compute_price_solar_metrics(price_hourly, solar_hourly, previous)
    metrics = [
        ("Avg spot", "Avg spot (€/MWh)", "€/MWh", "diff"),
        ("Captured solar uncurtailed", "Captured solar uncurtailed (€/MWh)", "€/MWh", "diff"),
        ("Captured solar curtailed", "Captured solar curtailed (€/MWh)", "€/MWh", "diff"),
        ("Solar generation", "Solar generation (GWh)", "GWh", "pct"),
        ("Economic curtailment", "Economic curtailment (GWh)", "GWh", "pct"),
        ("Economic curtailment", "Economic curtailment (%)", "%", "diff_pct_pts"),
        ("Negative / zero hours", "Negative / zero hours", "h", "diff"),
        ("Negative / zero hours", "Negative / zero hours (%)", "%", "diff_pct_pts"),
        ("Negative / zero solar hours", "Negative / zero solar hours", "h", "diff"),
        ("Negative / zero solar hours", "Negative / zero solar hours (%)", "%", "diff_pct_pts"),
        ("Capture rate uncurtailed", "Capture rate uncurtailed (%)", "%", "diff_pct_pts"),
        ("Capture rate curtailed", "Capture rate curtailed (%)", "%", "diff_pct_pts"),
    ]
    rows = []
    for label, key, unit, mode in metrics:
        cv, pv = cur.get(key), prev.get(key)
        rows.append({
            "Metric": label,
            f"YTD {current.year}": cv,
            f"YTD {previous.year}": pv,
            "Abs. change": value_change(cv, pv),
            "% change": pct_change(cv, pv) if mode == "pct" else pd.NA,
            "Unit": unit,
        })
    return pd.DataFrame(rows)


def _daily_agg(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    p = price_hourly[period_filter(price_hourly, start, end)].copy()
    s = solar_hourly[period_filter(solar_hourly, start, end)].copy()
    if p.empty:
        return _empty_df(["date", "spot", "captured_unc", "captured_cur", "solar_gwh"])
    merged = p.merge(s[["datetime", "solar_best_mw"]], on="datetime", how="left")
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
    merged["date"] = merged["datetime"].dt.date
    merged["positive_solar_mw"] = merged["solar_best_mw"].where(merged["price"] > 0, 0.0)

    def _calc(grp: pd.DataFrame) -> pd.Series:
        solar = grp["solar_best_mw"].sum()
        solar_pos = grp["positive_solar_mw"].sum()
        return pd.Series({
            "spot": grp["price"].mean(),
            "captured_unc": (grp["price"] * grp["solar_best_mw"]).sum() / solar if solar > 0 else pd.NA,
            "captured_cur": (grp["price"].clip(lower=0) * grp["positive_solar_mw"]).sum() / solar_pos if solar_pos > 0 else pd.NA,
            "solar_gwh": solar / 1000.0,
            "negative_hours": int((grp["price"] <= 0).sum()),
            "hours": int(len(grp)),
        })

    out = merged.groupby("date").apply(_calc).reset_index()
    out["year"] = pd.to_datetime(out["date"]).dt.year
    out["day_index"] = (pd.to_datetime(out["date"]) - pd.to_datetime(out["year"].astype(str) + "-01-01")).dt.days + 1
    out["plot_date"] = pd.to_datetime("2000-01-01") + pd.to_timedelta(out["day_index"] - 1, unit="D")
    return out


def _weekly_agg(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start: date, end: date, target_year: int | None = None) -> pd.DataFrame:
    """
    Weekly values aligned to the report year.

    Important: do not use Monday week_start.year as the year label, because the
    first days of January can have a week_start in the previous calendar year.
    That was creating an artificial 2024 legend entry and a long line from April
    to December/January. The week_index below is anchored to Jan 1 of target_year.
    """
    target_year = target_year or start.year

    daily = _daily_agg(price_hourly, solar_hourly, start, end)
    if daily.empty:
        return _empty_df(["week_start", "spot", "captured_unc", "captured_cur", "solar_gwh", "negative_hours", "hours", "year", "week_index", "plot_date"])

    daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
    daily = daily.dropna(subset=["date"]).copy()
    if daily.empty:
        return _empty_df(["week_start", "spot", "captured_unc", "captured_cur", "solar_gwh", "negative_hours", "hours", "year", "week_index", "plot_date"])

    jan1 = pd.Timestamp(date(target_year, 1, 1))
    daily["week_index"] = ((daily["date"] - jan1).dt.days // 7) + 1
    daily = daily[daily["week_index"] >= 1].copy()

    def _calc_week(grp: pd.DataFrame) -> pd.Series:
        solar = grp["solar_gwh"].sum()
        return pd.Series({
            "week_start": grp["date"].min(),
            "spot": grp["spot"].mean(),
            "captured_unc": (grp["captured_unc"] * grp["solar_gwh"]).sum() / solar if solar > 0 else pd.NA,
            "captured_cur": (grp["captured_cur"] * grp["solar_gwh"]).sum() / solar if solar > 0 else pd.NA,
            "solar_gwh": solar,
            "negative_hours": grp["negative_hours"].sum(),
            "hours": grp["hours"].sum(),
        })

    out = daily.groupby("week_index").apply(_calc_week).reset_index()
    out["year"] = target_year
    out["plot_date"] = pd.to_datetime("2000-01-01") + pd.to_timedelta((out["week_index"] - 1) * 7, unit="D")
    return out


def build_weekly_price_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    cur = _weekly_agg(price_hourly, solar_hourly, current.start, current.end, target_year=current.year)
    prev = _weekly_agg(price_hourly, solar_hourly, previous.start, previous.end, target_year=previous.year)
    out = pd.concat([cur, prev], ignore_index=True)
    if out.empty:
        return out
    out["Week start"] = pd.to_datetime(out["week_start"]).dt.strftime("%Y-%m-%d")
    out["Week"] = out["week_index"]
    return out[["Week start", "Week", "year", "spot", "captured_unc", "captured_cur", "solar_gwh", "negative_hours", "hours"]].rename(columns={
        "year": "Year",
        "spot": "Avg spot (€/MWh)",
        "captured_unc": "Captured uncurtailed (€/MWh)",
        "captured_cur": "Captured curtailed (€/MWh)",
        "solar_gwh": "Solar generation (GWh)",
        "negative_hours": "Negative / zero hours",
        "hours": "Hours",
    })


def build_monthly_price_solar_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, years: list[int], cutoff_month: int, cutoff_day: int) -> pd.DataFrame:
    rows = []
    for y in years:
        y_cutoff_day = cutoff_day if y == max(years) else min(cutoff_day, month_end_day(y, cutoff_month))
        for m in range(1, cutoff_month + 1):
            start, end = month_window(y, m, cutoff_month, y_cutoff_day)
            metrics = compute_price_solar_metrics(price_hourly, solar_hourly, PeriodWindow(f"{y}-{m:02d}", y, start, end))
            rows.append({
                "Year": y,
                "Month": pd.Timestamp(start).strftime("%b"),
                "Avg spot (€/MWh)": metrics["Avg spot (€/MWh)"],
                "Captured uncurtailed (€/MWh)": metrics["Captured solar uncurtailed (€/MWh)"],
                "Captured curtailed (€/MWh)": metrics["Captured solar curtailed (€/MWh)"],
                "Solar generation (GWh)": metrics["Solar generation (GWh)"],
                "Negative / zero hours": metrics["Negative / zero hours"],
                "Negative / zero hours (%)": metrics["Negative / zero hours (%)"],
                "Negative / zero solar hours": metrics["Negative / zero solar hours"],
                "Negative / zero solar hours (%)": metrics["Negative / zero solar hours (%)"],
                "Economic curtailment (%)": metrics["Economic curtailment (%)"],
            })
    return pd.DataFrame(rows)


def build_negative_hours_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, years: list[int], cutoff_month: int, cutoff_day: int) -> pd.DataFrame:
    monthly = build_monthly_price_solar_table(price_hourly, solar_hourly, years, cutoff_month, cutoff_day)
    if monthly.empty:
        return monthly
    return monthly[[
        "Year", "Month", "Negative / zero hours", "Negative / zero hours (%)",
        "Negative / zero solar hours", "Negative / zero solar hours (%)"
    ]].copy()


def build_mix_ytd_table(mix_daily: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    if mix_daily.empty:
        return pd.DataFrame()
    rows = []
    for tech in TECH_ORDER:
        cur = mix_daily[(mix_daily["technology"] == tech) & period_filter(mix_daily, current.start, current.end)]["energy_mwh"].sum() / 1000.0
        prev = mix_daily[(mix_daily["technology"] == tech) & period_filter(mix_daily, previous.start, previous.end)]["energy_mwh"].sum() / 1000.0
        if cur == 0 and prev == 0:
            continue
        rows.append({
            "Technology": tech,
            f"YTD {current.year} (GWh)": cur,
            f"YTD {previous.year} (GWh)": prev,
            "Abs. change (GWh)": cur - prev,
            "% change": pct_change(cur, prev),
        })
    out = pd.DataFrame(rows)
    return out.sort_values(f"YTD {current.year} (GWh)", ascending=False).reset_index(drop=True) if not out.empty else out


def build_demand_ytd_table(demand_hourly: pd.DataFrame, live_demand_monthly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    cur = demand_hourly[period_filter(demand_hourly, current.start, current.end)]["demand_mwh"].sum() / 1000.0 if not demand_hourly.empty else 0.0
    prev = demand_hourly[period_filter(demand_hourly, previous.start, previous.end)]["demand_mwh"].sum() / 1000.0 if not demand_hourly.empty else 0.0

    if current.year >= LIVE_START_DATE.year and not live_demand_monthly.empty:
        cur_monthly = live_demand_monthly[
            (live_demand_monthly["datetime"].dt.year == current.year) &
            (live_demand_monthly["datetime"].dt.month <= current.end.month)
        ]["demand_mwh"].sum() / 1000.0
        if cur_monthly > 0:
            cur = cur_monthly

    return pd.DataFrame([{
        "Metric": "Demand",
        f"YTD {current.year} (GWh)": cur,
        f"YTD {previous.year} (GWh)": prev,
        "Abs. change (GWh)": cur - prev,
        "% change": pct_change(cur, prev),
    }])


def build_capacity_table(capacity_monthly: pd.DataFrame, current_month: pd.Timestamp, previous_month: pd.Timestamp) -> pd.DataFrame:
    if capacity_monthly.empty:
        return pd.DataFrame()
    cur = capacity_monthly[capacity_monthly["datetime"] <= current_month.to_period("M").to_timestamp()].copy()
    prev = capacity_monthly[capacity_monthly["datetime"] <= previous_month.to_period("M").to_timestamp()].copy()
    if cur.empty or prev.empty:
        return pd.DataFrame()
    cur_latest = cur["datetime"].max()
    prev_latest = prev["datetime"].max()
    rows = []
    for tech in TECH_ORDER:
        cv = cur[(cur["datetime"] == cur_latest) & (cur["technology"] == tech)]["capacity_mw"].sum()
        pv = prev[(prev["datetime"] == prev_latest) & (prev["technology"] == tech)]["capacity_mw"].sum()
        if cv == 0 and pv == 0:
            continue
        rows.append({
            "Technology": tech,
            f"Latest {cur_latest.strftime('%b-%Y')} (MW)": cv,
            f"Latest {prev_latest.strftime('%b-%Y')} (MW)": pv,
            "Abs. change (MW)": cv - pv,
            "% change": pct_change(cv, pv),
        })
    return pd.DataFrame(rows).sort_values(f"Latest {cur_latest.strftime('%b-%Y')} (MW)", ascending=False)


def build_duck_curve_df(price_hourly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    rows = []
    for window in [previous, current]:
        p = price_hourly[period_filter(price_hourly, window.start, window.end)].copy()
        if p.empty:
            continue
        p["datetime"] = pd.to_datetime(p["datetime"], errors="coerce")
        p["price"] = pd.to_numeric(p["price"], errors="coerce")
        p = p.dropna(subset=["datetime", "price"]).copy()
        if p.empty:
            continue
        p["hour"] = p["datetime"].dt.hour
        prof = p.groupby("hour", as_index=False)["price"].mean().rename(columns={"price": "avg_price"})
        prof = pd.DataFrame({"hour": range(24)}).merge(prof, on="hour", how="left")
        prof["avg_price"] = prof["avg_price"].interpolate(limit_direction="both").fillna(0.0)
        prof["year"] = window.year
        rows.append(prof)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()



# =========================================================
# BESS REVENUE - aligned with BESS tab standalone logic
# =========================================================
def optimize_standalone_bess_day(
    prices: list[float],
    bess_mw: float,
    duration_h: float,
    eta_ch: float = 1.0,
    eta_dis: float = 0.85,
    cycle_limit_factor: float = 1.0,
) -> dict:
    """
    Standalone DA arbitrage optimizer aligned with the BESS tab logic:
    - generation = 0
    - demand = 0
    - omie_compra = omie_venta
    - revenue is divided by MWnom in the monthly/yearly report.
    """
    prices = [float(p) for p in prices if pd.notna(p)]
    n = len(prices)
    if n == 0 or bess_mw <= 0 or duration_h <= 0:
        return {
            "revenue_eur": 0.0,
            "charged_mwh": 0.0,
            "discharged_mwh": 0.0,
            "avg_buy_price": np.nan,
            "avg_sell_price": np.nan,
        }

    capacity_mwh = bess_mw * duration_h
    power_mw = bess_mw
    max_grid_flow = max(power_mw, 1e-9)

    model = pulp.LpProblem("monthly_report_standalone_bess", pulp.LpMaximize)
    grid_charge = pulp.LpVariable.dicts("grid_charge", range(n), lowBound=0)
    batt_for_sell = pulp.LpVariable.dicts("batt_for_sell", range(n), lowBound=0)
    soc = pulp.LpVariable.dicts("soc", range(n + 1), lowBound=0)
    is_charging = pulp.LpVariable.dicts("is_charging", range(n), cat="Binary")
    is_export = pulp.LpVariable.dicts("is_export", range(n), cat="Binary")

    model += soc[0] == 0.0
    model += soc[n] == 0.0

    for t in range(n):
        model += grid_charge[t] <= power_mw * is_charging[t]
        model += batt_for_sell[t] <= power_mw * (1 - is_charging[t])
        model += batt_for_sell[t] <= max_grid_flow * is_export[t]
        model += grid_charge[t] <= max_grid_flow * (1 - is_export[t])
        model += soc[t + 1] == soc[t] + eta_ch * grid_charge[t] - (1 / max(eta_dis, 1e-9)) * batt_for_sell[t]
        model += soc[t] <= capacity_mwh

    model += soc[n] <= capacity_mwh
    model += pulp.lpSum(grid_charge[t] for t in range(n)) <= cycle_limit_factor * capacity_mwh / max(eta_ch, 1e-9)
    model += pulp.lpSum(batt_for_sell[t] for t in range(n)) <= pulp.lpSum(grid_charge[t] for t in range(n))

    model += pulp.lpSum(
        batt_for_sell[t] * prices[t] - grid_charge[t] * prices[t]
        for t in range(n)
    )

    solver = pulp.PULP_CBC_CMD(msg=False)
    model.solve(solver)

    if pulp.LpStatus[model.status] != "Optimal":
        return {
            "revenue_eur": 0.0,
            "charged_mwh": 0.0,
            "discharged_mwh": 0.0,
            "avg_buy_price": np.nan,
            "avg_sell_price": np.nan,
        }

    charge = np.array([pulp.value(grid_charge[t]) or 0.0 for t in range(n)], dtype=float)
    discharge = np.array([pulp.value(batt_for_sell[t]) or 0.0 for t in range(n)], dtype=float)
    price_arr = np.array(prices, dtype=float)

    revenue = float((discharge * price_arr).sum() - (charge * price_arr).sum())
    charged_mwh = float(charge.sum())
    discharged_mwh = float(discharge.sum())
    buy_price = float((charge * price_arr).sum() / charged_mwh) if charged_mwh > 0 else np.nan
    sell_price = float((discharge * price_arr).sum() / discharged_mwh) if discharged_mwh > 0 else np.nan

    return {
        "revenue_eur": revenue,
        "charged_mwh": charged_mwh,
        "discharged_mwh": discharged_mwh,
        "avg_buy_price": buy_price,
        "avg_sell_price": sell_price,
    }


def build_bess_revenue_monthly_table(
    price_hourly: pd.DataFrame,
    years: list[int],
    cutoff_month: int,
    cutoff_day: int,
    bess_mw: float,
    duration_h: float,
    eta_ch: float,
    eta_dis: float,
    cycle_limit_factor: float,
) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame()

    p = price_hourly.copy()
    p["datetime"] = pd.to_datetime(p["datetime"], errors="coerce")
    p["price"] = pd.to_numeric(p["price"], errors="coerce")
    p = p.dropna(subset=["datetime", "price"]).copy()
    if p.empty:
        return pd.DataFrame()

    daily_rows = []
    for y in years:
        y_cutoff_day = cutoff_day if y == max(years) else min(cutoff_day, month_end_day(y, cutoff_month))
        for m in range(1, cutoff_month + 1):
            start, end = month_window(y, m, cutoff_month, y_cutoff_day)
            df_m = p[period_filter(p, start, end)].copy()
            if df_m.empty:
                continue
            df_m["day"] = df_m["datetime"].dt.date
            for day, grp in df_m.groupby("day"):
                res = optimize_standalone_bess_day(
                    prices=grp.sort_values("datetime")["price"].tolist(),
                    bess_mw=bess_mw,
                    duration_h=duration_h,
                    eta_ch=eta_ch,
                    eta_dis=eta_dis,
                    cycle_limit_factor=cycle_limit_factor,
                )
                daily_rows.append({
                    "Date": day,
                    "Year": y,
                    "month": pd.Timestamp(day).strftime("%Y-%m"),
                    "Revenue_BESS_EUR": res["revenue_eur"],
                    "Charge_MWh": res["charged_mwh"],
                    "Discharge_MWh": res["discharged_mwh"],
                    "Buy_Value_EUR": res["charged_mwh"] * (res["avg_buy_price"] if pd.notna(res["avg_buy_price"]) else 0.0),
                    "Sell_Value_EUR": res["discharged_mwh"] * (res["avg_sell_price"] if pd.notna(res["avg_sell_price"]) else 0.0),
                })

    daily = pd.DataFrame(daily_rows)
    if daily.empty:
        return pd.DataFrame()

    monthly = (
        daily.groupby(["Year", "month"], as_index=False)
        .agg(
            Revenue_BESS_EUR=("Revenue_BESS_EUR", "sum"),
            Charge_MWh=("Charge_MWh", "sum"),
            Discharge_MWh=("Discharge_MWh", "sum"),
            Buy_Value_EUR=("Buy_Value_EUR", "sum"),
            Sell_Value_EUR=("Sell_Value_EUR", "sum"),
            Days=("Date", "nunique"),
        )
    )
    monthly["Revenue BESS €/MW"] = np.where(bess_mw > 0, monthly["Revenue_BESS_EUR"] / bess_mw, np.nan)
    monthly["Avg buy price (€/MWh)"] = np.where(monthly["Charge_MWh"] > 0, monthly["Buy_Value_EUR"] / monthly["Charge_MWh"], np.nan)
    monthly["Avg sell price (€/MWh)"] = np.where(monthly["Discharge_MWh"] > 0, monthly["Sell_Value_EUR"] / monthly["Discharge_MWh"], np.nan)
    monthly["Captured spread (€/MWh)"] = monthly["Avg sell price (€/MWh)"] - monthly["Avg buy price (€/MWh)"]
    monthly["Cycles/day avg"] = np.where(
        (monthly["Days"] > 0) & (bess_mw * duration_h > 0),
        monthly["Discharge_MWh"] / max(eta_dis, 1e-9) / (bess_mw * duration_h) / monthly["Days"],
        np.nan,
    )

    total = (
        monthly.groupby("Year", as_index=False)
        .agg(
            Revenue_BESS_EUR=("Revenue_BESS_EUR", "sum"),
            Charge_MWh=("Charge_MWh", "sum"),
            Discharge_MWh=("Discharge_MWh", "sum"),
            Buy_Value_EUR=("Buy_Value_EUR", "sum"),
            Sell_Value_EUR=("Sell_Value_EUR", "sum"),
            Days=("Days", "sum"),
        )
    )
    total["month"] = "TOTAL"
    total["Revenue BESS €/MW"] = np.where(bess_mw > 0, total["Revenue_BESS_EUR"] / bess_mw, np.nan)
    total["Avg buy price (€/MWh)"] = np.where(total["Charge_MWh"] > 0, total["Buy_Value_EUR"] / total["Charge_MWh"], np.nan)
    total["Avg sell price (€/MWh)"] = np.where(total["Discharge_MWh"] > 0, total["Sell_Value_EUR"] / total["Discharge_MWh"], np.nan)
    total["Captured spread (€/MWh)"] = total["Avg sell price (€/MWh)"] - total["Avg buy price (€/MWh)"]
    total["Cycles/day avg"] = np.where(
        (total["Days"] > 0) & (bess_mw * duration_h > 0),
        total["Discharge_MWh"] / max(eta_dis, 1e-9) / (bess_mw * duration_h) / total["Days"],
        np.nan,
    )

    out = pd.concat([monthly, total], ignore_index=True, sort=False)
    return out[[
        "Year", "month", "Revenue BESS €/MW", "Revenue_BESS_EUR",
        "Cycles/day avg", "Charge_MWh", "Discharge_MWh",
        "Avg buy price (€/MWh)", "Avg sell price (€/MWh)", "Captured spread (€/MWh)",
    ]].sort_values(["Year", "month"]).reset_index(drop=True)


def build_bess_duration_comparison(
    price_hourly: pd.DataFrame,
    years: list[int],
    cutoff_month: int,
    cutoff_day: int,
    bess_mw: float,
    eta_ch: float,
    eta_dis: float,
    cycle_limit_factor: float,
) -> pd.DataFrame:
    rows = []
    for duration in [1.0, 2.0, 4.0]:
        tbl = build_bess_revenue_monthly_table(
            price_hourly=price_hourly,
            years=years,
            cutoff_month=cutoff_month,
            cutoff_day=cutoff_day,
            bess_mw=bess_mw,
            duration_h=duration,
            eta_ch=eta_ch,
            eta_dis=eta_dis,
            cycle_limit_factor=cycle_limit_factor,
        )
        if tbl.empty:
            continue
        totals = tbl[tbl["month"] == "TOTAL"].copy()
        totals["Duration"] = f"{int(duration)}h"
        rows.append(totals[["Year", "Duration", "Revenue BESS €/MW", "Cycles/day avg", "Captured spread (€/MWh)"]])
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def chart_bess_revenue(bess_table: pd.DataFrame, duration_label: str = "4h") -> str | None:
    if bess_table.empty:
        return None
    plot = bess_table[bess_table["month"] != "TOTAL"].copy()
    if plot.empty:
        return None
    plot["Month"] = pd.to_datetime(plot["month"], errors="coerce").dt.strftime("%b")
    cmap = year_color_map(plot["Year"].unique().tolist())
    fig, ax = plt.subplots(figsize=(10.8, 4.5))
    for y, grp in plot.groupby("Year"):
        grp = grp.copy()
        grp["month_num"] = pd.to_datetime(grp["month"], errors="coerce").dt.month
        grp = grp.sort_values("month_num")
        ax.bar(
            [f"{m}\n{y}" for m in grp["Month"]],
            grp["Revenue BESS €/MW"] / 1000.0,
            color=cmap[y],
            alpha=0.85,
            label=str(y),
        )
    ax.set_title(f"Monthly DA BESS revenue ({duration_label}, standalone)")
    ax.set_ylabel("k€/MWnom")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)


def chart_bess_spread_prices(bess_table: pd.DataFrame, duration_label: str = "4h") -> str | None:
    if bess_table.empty:
        return None
    plot = bess_table[bess_table["month"] != "TOTAL"].copy()
    if plot.empty:
        return None
    plot["month_dt"] = pd.to_datetime(plot["month"], errors="coerce")
    plot = plot.dropna(subset=["month_dt"]).copy()
    plot["Month"] = plot["month_dt"].dt.strftime("%b")
    plot["month_num"] = plot["month_dt"].dt.month
    years = sorted(plot["Year"].unique().tolist())
    months = sorted(plot["month_num"].unique().tolist())
    month_labels = [pd.Timestamp(2000, m, 1).strftime("%b") for m in months]
    cmap = year_color_map(years)

    fig, ax = plt.subplots(figsize=(11.2, 5.0))
    x = np.arange(len(months))
    width = 0.36 if len(years) == 2 else max(0.22, 0.8 / max(len(years), 1))
    offsets = np.linspace(-(len(years)-1)/2*width, (len(years)-1)/2*width, len(years)) if years else []

    # spread as bars
    for i, y in enumerate(years):
        grp = plot[plot["Year"] == y].copy()
        spread_vals = []
        for m in months:
            row = grp[grp["month_num"] == m]
            spread_vals.append(float(row["Captured spread (€/MWh)"].iloc[0]) if not row.empty and pd.notna(row["Captured spread (€/MWh)"].iloc[0]) else 0.0)
        ax.bar(x + offsets[i], spread_vals, width=width, color=cmap[y], alpha=0.25, edgecolor=cmap[y], linewidth=1.0, label=f"Spread {y}")

    # buy/sell as lines
    for y in years:
        grp = plot[plot["Year"] == y].sort_values("month_num")
        color = cmap[y]
        x_line = [months.index(m) for m in grp["month_num"]]
        ax.plot(x_line, grp["Avg buy price (€/MWh)"], color=color, linewidth=1.9, linestyle=":", marker="o", label=f"Buy {y}")
        ax.plot(x_line, grp["Avg sell price (€/MWh)"], color=color, linewidth=2.3, linestyle="-", marker="o", label=f"Sell {y}")

    ax.set_title(f"BESS buy / sell prices and captured spread ({duration_label})", fontweight='bold')
    ax.set_ylabel("€/MWh")
    ax.set_xticks(x)
    ax.set_xticklabels(month_labels)
    ax.grid(axis="y", alpha=0.22)
    ax.spines[['top','right']].set_visible(False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.20), ncol=3, fontsize=8, frameon=False)
    return fig_to_b64(fig)


def chart_bess_duration_totals(bess_duration_table: pd.DataFrame) -> str | None:
    if bess_duration_table.empty:
        return None
    fig, ax = plt.subplots(figsize=(10.5, 4.3))
    labels = []
    vals = []
    colors = []
    cmap = year_color_map(bess_duration_table["Year"].unique().tolist())
    for _, row in bess_duration_table.sort_values(["Year", "Duration"]).iterrows():
        labels.append(f"{row['Duration']}\n{int(row['Year'])}")
        vals.append(row["Revenue BESS €/MW"] / 1000.0)
        colors.append(cmap[int(row["Year"])])
    ax.bar(labels, vals, color=colors, alpha=0.85)
    ax.set_title("YTD DA BESS revenue by duration")
    ax.set_ylabel("k€/MWnom")
    ax.grid(axis="y", alpha=0.25)
    return fig_to_b64(fig)


# =========================================================
# CHARTS
# =========================================================
def chart_weekly_spot_capture(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> str | None:
    weekly = pd.concat([
        _weekly_agg(price_hourly, solar_hourly, previous.start, previous.end, target_year=previous.year),
        _weekly_agg(price_hourly, solar_hourly, current.start, current.end, target_year=current.year),
    ], ignore_index=True)
    if weekly.empty:
        return None

    years = sorted(weekly["year"].unique().tolist())
    cmap = year_color_map(years)

    fig, ax = plt.subplots(figsize=(11, 4.8))
    for y in years:
        grp = weekly[weekly["year"] == y].sort_values("plot_date")
        color = cmap[y]
        ax.plot(grp["plot_date"], grp["spot"], color=color, linewidth=2.3, label=f"Spot {y}")
        ax.plot(grp["plot_date"], grp["captured_unc"], color=color, linewidth=2.0, linestyle=":", label=f"Captured uncurtailed {y}")
        ax.plot(grp["plot_date"], grp["captured_cur"], color=color, linewidth=2.0, linestyle="--", label=f"Captured curtailed {y}")
    ax.set_title(f"Weekly spot vs captured prices (YTD to {current.end.strftime('%d-%b')})")
    ax.set_ylabel("€/MWh")
    ax.grid(axis="y", alpha=0.25)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3, fontsize=8)
    return fig_to_b64(fig)


def chart_monthly_negative_share(neg_table: pd.DataFrame) -> str | None:
    if neg_table.empty:
        return None
    cmap = year_color_map(neg_table["Year"].unique().tolist())
    fig, ax = plt.subplots(figsize=(10.5, 4.2))
    for y, grp in neg_table.groupby("Year"):
        grp = grp.copy()
        grp["Month_num"] = pd.to_datetime(grp["Month"], format="%b", errors="coerce").dt.month
        grp = grp.sort_values("Month_num")
        ax.plot(grp["Month"], grp["Negative / zero hours (%)"] * 100, marker="o", linewidth=2.2, color=cmap[y], label=f"{y}")
    ax.set_title("Monthly share of zero / negative price hours")
    ax.set_ylabel("% of hours")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)


def chart_solar_curtailment(monthly: pd.DataFrame) -> str | None:
    if monthly.empty:
        return None
    monthly = monthly.copy()
    years = sorted(monthly["Year"].unique().tolist())
    months = [m for m in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] if m in monthly["Month"].tolist()]
    cmap = year_color_map(years)
    fig, ax = plt.subplots(figsize=(10.8, 4.5))
    x = np.arange(len(months))
    width = 0.36 if len(years) == 2 else max(0.22, 0.8 / max(len(years), 1))
    offsets = np.linspace(-(len(years)-1)/2*width, (len(years)-1)/2*width, len(years)) if years else []
    for i, y in enumerate(years):
        grp = monthly[monthly["Year"] == y].copy()
        vals = []
        for m in months:
            row = grp[grp["Month"] == m]
            vals.append(float(row["Economic curtailment (%)"].iloc[0] * 100) if not row.empty and pd.notna(row["Economic curtailment (%)"].iloc[0]) else 0.0)
        ax.bar(x + offsets[i], vals, width=width, color=cmap[y], alpha=0.85, label=str(y))
    ax.set_title("Monthly economic curtailment based on P48")
    ax.set_ylabel("% of P48 production")
    ax.set_xticks(x)
    ax.set_xticklabels(months)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)


def chart_mix(mix_table: pd.DataFrame, current_year: int, previous_year: int) -> str | None:
    if mix_table.empty:
        return None
    cur_col = f"YTD {current_year} (GWh)"
    prev_col = f"YTD {previous_year} (GWh)"
    plot = mix_table.head(12).copy().sort_values(cur_col)
    y = range(len(plot))
    fig, ax = plt.subplots(figsize=(10.8, 5.2))
    ax.barh([i - 0.18 for i in y], plot[prev_col], height=0.35, label=str(previous_year), alpha=0.7)
    ax.barh([i + 0.18 for i in y], plot[cur_col], height=0.35, label=str(current_year), alpha=0.9)
    ax.set_yticks(list(y))
    ax.set_yticklabels(plot["Technology"])
    ax.set_xlabel("GWh")
    ax.set_title("YTD energy mix comparison")
    ax.grid(axis="x", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)


def chart_duck_curve(duck_df: pd.DataFrame) -> str | None:
    if duck_df.empty:
        return None
    years = sorted(duck_df["year"].unique().tolist())
    cmap = year_color_map(years)
    fig, ax = plt.subplots(figsize=(10.8, 4.8))
    for y in years:
        grp = duck_df[duck_df["year"] == y].sort_values("hour")
        color = cmap[y]
        ax.plot(grp["hour"], grp["avg_price"], color=color, linewidth=2.3, label=f"Avg hourly price {y}")
    ax.set_title("Duck curve style profile based on average hourly prices (YTD)")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("€/MWh")
    ax.set_xticks(range(0, 24, 2))
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)



def chart_price_heatmap(price_hourly: pd.DataFrame, year: int) -> str | None:
    """
    24 x 365 (or 366) heatmap of hourly OMIE prices for the selected year,
    styled closer to the reference shared by the user.
    Rows = hour of day, columns = day of year.
    """
    if price_hourly.empty:
        return None

    p = price_hourly.copy()
    p["datetime"] = pd.to_datetime(p["datetime"], errors="coerce")
    p["price"] = pd.to_numeric(p["price"], errors="coerce")
    p = p.dropna(subset=["datetime", "price"]).copy()
    p = p[p["datetime"].dt.year == year].copy()
    if p.empty:
        return None

    p["doy"] = p["datetime"].dt.dayofyear
    p["hour"] = p["datetime"].dt.hour

    n_days = 366 if pd.Timestamp(year=year, month=12, day=31).dayofyear == 366 else 365
    grid = np.full((24, n_days), np.nan)

    # One average hourly OMIE price per hour x day cell.
    agg = p.groupby(["hour", "doy"], as_index=False)["price"].mean()
    for _, row in agg.iterrows():
        h = int(row["hour"])
        d = int(row["doy"]) - 1
        if 0 <= h < 24 and 0 <= d < n_days:
            grid[h, d] = float(row["price"])

    if np.isfinite(grid).sum() == 0:
        return None

    # Robust scale so extreme spikes do not dominate the chart.
    finite_vals = grid[np.isfinite(grid)]
    vmin = np.nanpercentile(finite_vals, 3)
    vmax = np.nanpercentile(finite_vals, 97)
    if vmin == vmax:
        vmin = float(np.nanmin(finite_vals))
        vmax = float(np.nanmax(finite_vals)) + 1e-6

    fig, ax = plt.subplots(figsize=(12.4, 5.2))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')

    # Style intentionally similar to the sample: dark purple -> orange -> light cream.
    im = ax.imshow(
        grid,
        aspect='auto',
        origin='upper',
        cmap='magma',
        interpolation='nearest',
        vmin=vmin,
        vmax=vmax,
    )

    month_starts = [pd.Timestamp(year, m, 1).dayofyear - 1 for m in range(1, 13)]
    month_labels = [pd.Timestamp(year, m, 1).strftime('%b') for m in range(1, 13)]
    ax.set_xticks(month_starts)
    ax.set_xticklabels(month_labels)

    y_ticks = list(range(0, 24, 2))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([str(h) for h in y_ticks])

    ax.set_xlabel('Month')
    ax.set_ylabel('Time [hour]')
    ax.set_title(f'OMIE hourly price heatmap | {year} | 24 x {n_days}', fontweight='bold', pad=10)

    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)

    cbar = fig.colorbar(im, ax=ax, pad=0.02, fraction=0.03)
    cbar.set_label('Spot [€/MWh]')
    cbar.outline.set_visible(False)

    return fig_to_b64(fig)


# =========================================================
# PDF GENERATION
# =========================================================
def _format_df_for_pdf(df: pd.DataFrame, pct_cols: list[str] | None = None, max_rows: int = 30) -> pd.DataFrame:
    pct_cols = pct_cols or []
    tmp = df.head(max_rows).copy()
    for c in tmp.columns:
        if c in pct_cols:
            tmp[c] = tmp[c].map(lambda v: "" if pd.isna(v) else f"{v:.1%}")
        elif pd.api.types.is_numeric_dtype(tmp[c]):
            tmp[c] = tmp[c].map(lambda v: "" if pd.isna(v) else f"{v:,.2f}")
    return tmp.astype(str)


def _add_logo_stamp(fig, logo_path: Path | None):
    if not logo_path or not logo_path.exists():
        return
    try:
        img = plt.imread(str(logo_path))
        logo_ax = fig.add_axes([0.83, 0.88, 0.13, 0.09])
        logo_ax.imshow(img)
        logo_ax.axis("off")
    except Exception:
        pass


def _add_title_page(pdf: PdfPages, logo_path: Path | None, title: str, subtitle: str):
    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    _add_logo_stamp(fig, logo_path)
    if logo_path and logo_path.exists():
        try:
            img = plt.imread(str(logo_path))
            logo_ax = fig.add_axes([0.36, 0.60, 0.28, 0.24])
            logo_ax.imshow(img)
            logo_ax.axis("off")
        except Exception:
            pass
    ax.text(0.5, 0.50, title, ha="center", va="center", fontsize=22, fontweight="bold", color=CORP_GREEN_DARK, wrap=True)
    ax.text(0.5, 0.42, subtitle, ha="center", va="center", fontsize=12, color="#374151", wrap=True)
    ax.text(0.5, 0.08, f"Generated: {now_madrid().strftime('%Y-%m-%d %H:%M Madrid')}", ha="center", va="center", fontsize=9, color="#6B7280")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _add_chart_page(pdf: PdfPages, logo_path: Path | None, caption: str, img_b64: str):
    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    _add_logo_stamp(fig, logo_path)
    ax.text(0.5, 0.96, caption, ha="center", va="top", fontsize=16, fontweight="bold", color="#111827")
    raw = base64.b64decode(img_b64)
    img = plt.imread(BytesIO(raw), format="png")
    img_ax = fig.add_axes([0.06, 0.10, 0.88, 0.78])
    img_ax.imshow(img)
    img_ax.axis("off")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _add_table_page(pdf: PdfPages, logo_path: Path | None, title: str, df: pd.DataFrame, pct_cols: list[str] | None = None, max_rows: int = 30):
    fig, ax = plt.subplots(figsize=(11.69, 8.27))
    ax.axis("off")
    _add_logo_stamp(fig, logo_path)
    ax.set_title(title, fontsize=16, fontweight="bold", color="#111827", pad=18)
    if df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center", fontsize=12, color="#6B7280")
    else:
        tmp = _format_df_for_pdf(df, pct_cols=pct_cols, max_rows=max_rows)
        for c in tmp.columns:
            tmp[c] = tmp[c].map(lambda x: x[:35] + "…" if len(str(x)) > 36 else x)
        table = ax.table(cellText=tmp.values, colLabels=tmp.columns, cellLoc="center", loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(7.0)
        table.scale(1, 1.22)
        for (row, col), cell in table.get_celld().items():
            cell.set_edgecolor("#D1D5DB")
            cell.set_linewidth(0.4)
            if row == 0:
                cell.set_facecolor(CORP_GREY)
                cell.set_text_props(color="white", weight="bold")
            else:
                cell.set_facecolor("#FFFFFF" if row % 2 else "#F9FAFB")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def build_pdf_report(
    output_path: Path,
    logo_path: Path | None,
    title: str,
    kpi_df: pd.DataFrame,
    weekly_table: pd.DataFrame,
    monthly_table: pd.DataFrame,
    neg_hours_table: pd.DataFrame,
    mix_table: pd.DataFrame,
    demand_table: pd.DataFrame,
    capacity_table: pd.DataFrame,
    bess_table: pd.DataFrame,
    bess_duration_table: pd.DataFrame,
    charts: dict[str, str | None],
    current_year: int,
    previous_year: int,
) -> bytes:
    subtitle = f"Monthly Day Ahead report with YTD focus. Comparison: {current_year} YTD versus {previous_year} YTD, same calendar cut-off."
    with PdfPages(str(output_path)) as pdf:
        _add_title_page(pdf, logo_path, title, subtitle)
        for caption, img_b64 in charts.items():
            if img_b64:
                _add_chart_page(pdf, logo_path, caption, img_b64)
    return output_path.read_bytes()


# =========================================================
# EMAIL SENDERS
# =========================================================
def send_via_webhook(to_list: list[str], cc_list: list[str], subject: str, html_body: str, pdf_bytes: bytes, pdf_filename: str) -> tuple[bool, str]:
    url = st.secrets.get("mail_webhook_url", "")
    token = st.secrets.get("mail_webhook_token", "")
    if not url or not token:
        return False, "Webhook secrets not configured."
    payload = {
        "token": token,
        "to": to_list,
        "cc": cc_list,
        "subject": subject,
        "html_body": html_body,
        "attachments": [{
            "filename": pdf_filename,
            "content_base64": base64.b64encode(pdf_bytes).decode("utf-8"),
            "mime_type": "application/pdf",
        }],
    }
    resp = requests.post(url, json=payload, timeout=45)
    if 200 <= resp.status_code < 300:
        return True, "Email request sent successfully through webhook."
    return False, f"Webhook returned {resp.status_code}: {resp.text}"


def send_via_smtp(to_list: list[str], cc_list: list[str], subject: str, html_body: str, pdf_bytes: bytes, pdf_filename: str) -> tuple[bool, str]:
    host = st.secrets.get("smtp_host", "")
    user = st.secrets.get("smtp_user", "")
    password = st.secrets.get("smtp_password", "")
    sender = st.secrets.get("smtp_from", user)
    port = int(st.secrets.get("smtp_port", 587))
    if not host or not user or not password or not sender:
        return False, "SMTP secrets not configured."
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.set_content("Please view this email in HTML format. The PDF report is attached.")
    msg.add_alternative(html_body, subtype="html")
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename)
    with smtplib.SMTP(host, port, timeout=45) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)
    return True, "Email sent successfully through SMTP."


# =========================================================
# APP FLOW
# =========================================================
price_hourly = load_historical_prices()
solar_hourly = load_historical_solar()
mix_daily = load_generation_mix_daily()
demand_hourly = load_demand_raw()
capacity_monthly = load_installed_capacity_monthly()

if price_hourly.empty:
    st.error(f"No price data found. Expected file: {HIST_PRICES_FILE}")
    st.stop()

latest_price_day = price_hourly["datetime"].dt.date.max()
default_month = pd.Timestamp(latest_price_day).to_period("M").to_timestamp()

col_a, col_b = st.columns(2)
with col_a:
    to_emails_raw = st.text_area("To", value=st.secrets.get("default_to", ""), height=80)
with col_b:
    cc_emails_raw = st.text_area("Cc", value=st.secrets.get("default_cc", ""), height=80)

available_months = sorted(price_hourly["datetime"].dt.to_period("M").astype(str).unique().tolist())
month_str = st.selectbox(
    "Report month",
    available_months,
    index=available_months.index(default_month.strftime("%Y-%m")) if default_month.strftime("%Y-%m") in available_months else len(available_months) - 1,
)
report_month = pd.Timestamp(month_str)
report_year = report_month.year
previous_year = report_year - 1

heatmap_year_options = sorted(price_hourly["datetime"].dt.year.dropna().astype(int).unique().tolist()) if not price_hourly.empty else [report_year]
heatmap_year = st.selectbox("Heatmap year (OMIE hourly prices)", heatmap_year_options, index=heatmap_year_options.index(report_year) if report_year in heatmap_year_options else len(heatmap_year_options)-1)

month_end = (report_month + pd.offsets.MonthEnd(0)).date()
latest_in_month = price_hourly[price_hourly["datetime"].dt.to_period("M") == report_month.to_period("M")]["datetime"].dt.date.max()
cutoff_day = min(month_end, latest_in_month) if pd.notna(latest_in_month) else month_end

same_prev_day = min(cutoff_day.day, month_end_day(previous_year, report_month.month))
current_window = PeriodWindow(f"YTD {report_year}", report_year, date(report_year, 1, 1), cutoff_day)
previous_window = PeriodWindow(f"YTD {previous_year}", previous_year, date(previous_year, 1, 1), date(previous_year, report_month.month, same_prev_day))

# Append live current-year data when needed.
token = require_esios_token()
price_hourly, solar_hourly, demand_hourly = append_live_price_solar_demand(price_hourly, solar_hourly, demand_hourly, current_window.start, current_window.end, token)
mix_daily, capacity_monthly, live_demand_monthly = append_live_mix_capacity(mix_daily, capacity_monthly, current_window.start, current_window.end)

subject = st.text_input("Subject", value=f"Nexwell Power - Monthly Day Ahead YTD report - {report_month.strftime('%b-%Y')}")
intro_text = st.text_area(
    "Intro text",
    value=(
        "Hi all,\n\n"
        f"Please find below the monthly Day Ahead report for {report_month.strftime('%B %Y')}, "
        f"focused on YTD performance versus {previous_year}. The full PDF report is attached.\n"
    ),
    height=110,
)

st.subheader("BESS revenue settings")
bess_settings_cols = st.columns(5)
with bess_settings_cols[0]:
    bess_mw = st.number_input("BESS MWnom", min_value=0.1, value=float(st.secrets.get("default_bess_mw", 1.0)), step=0.1)
with bess_settings_cols[1]:
    bess_duration_h = st.number_input("Main duration (h)", min_value=0.5, value=float(st.secrets.get("default_bess_duration_h", 4.0)), step=0.5)
with bess_settings_cols[2]:
    bess_eta_ch = st.number_input("Charge efficiency", min_value=0.1, max_value=1.0, value=float(st.secrets.get("default_bess_eta_ch", 1.0)), step=0.01)
with bess_settings_cols[3]:
    bess_eta_dis = st.number_input("Discharge efficiency", min_value=0.1, max_value=1.0, value=float(st.secrets.get("default_bess_eta_dis", 0.85)), step=0.01)
with bess_settings_cols[4]:
    cycle_limit_factor = st.number_input("Cycles/day limit", min_value=0.1, max_value=3.0, value=float(st.secrets.get("default_bess_cycles_day", 1.0)), step=0.1)

kpi_df = build_ytd_metrics_table(price_hourly, solar_hourly, current_window, previous_window)
weekly_table = build_weekly_price_capture_table(price_hourly, solar_hourly, current_window, previous_window)
monthly_table = build_monthly_price_solar_table(price_hourly, solar_hourly, [previous_year, report_year], report_month.month, cutoff_day.day)
neg_hours_table = build_negative_hours_table(price_hourly, solar_hourly, [previous_year, report_year], report_month.month, cutoff_day.day)
mix_table = build_mix_ytd_table(mix_daily, current_window, previous_window)
demand_table = build_demand_ytd_table(demand_hourly, live_demand_monthly, current_window, previous_window)
capacity_table = build_capacity_table(capacity_monthly, report_month, pd.Timestamp(date(previous_year, report_month.month, 1)))
duck_df = build_duck_curve_df(price_hourly, current_window, previous_window)

bess_table = build_bess_revenue_monthly_table(
    price_hourly=price_hourly,
    years=[previous_year, report_year],
    cutoff_month=report_month.month,
    cutoff_day=cutoff_day.day,
    bess_mw=bess_mw,
    duration_h=bess_duration_h,
    eta_ch=bess_eta_ch,
    eta_dis=bess_eta_dis,
    cycle_limit_factor=cycle_limit_factor,
)
bess_duration_table = build_bess_duration_comparison(
    price_hourly=price_hourly,
    years=[previous_year, report_year],
    cutoff_month=report_month.month,
    cutoff_day=cutoff_day.day,
    bess_mw=bess_mw,
    eta_ch=bess_eta_ch,
    eta_dis=bess_eta_dis,
    cycle_limit_factor=cycle_limit_factor,
)

charts = {
    "Weekly spot and captured prices": chart_weekly_spot_capture(price_hourly, solar_hourly, current_window, previous_window),
    "OMIE hourly price heatmap (24x365)": chart_price_heatmap(price_hourly, heatmap_year),
    "Monthly negative / zero price share": chart_monthly_negative_share(neg_hours_table),
    "Solar economic curtailment based on P48": chart_solar_curtailment(monthly_table),
    "Duck curve": chart_duck_curve(duck_df),
    "Energy mix": chart_mix(mix_table, report_year, previous_year),
    "BESS monthly revenue": chart_bess_revenue(bess_table, duration_label=f"{bess_duration_h:g}h"),
    "BESS buy / sell prices and spread": chart_bess_spread_prices(bess_table, duration_label=f"{bess_duration_h:g}h"),
    "BESS revenue by duration": chart_bess_duration_totals(bess_duration_table),
}

logo_path = find_logo_path()
logo_html = ""
if logo_path:
    logo_html = f'<div style="margin-bottom:12px;"><img src="data:image/jpeg;base64,{image_to_b64(logo_path)}" alt="Nexwell Power" style="width:210px;height:auto;"></div>'

kpi_lookup = {row["Metric"] + "|" + row["Unit"]: row for _, row in kpi_df.iterrows()}
cards = []
for metric, unit, decimals in [
    ("Avg spot", "€/MWh", 2),
    ("Captured solar uncurtailed", "€/MWh", 2),
    ("Captured solar curtailed", "€/MWh", 2),
    ("Solar generation", "GWh", 1),
    ("Negative / zero hours", "%", 1),
    ("Economic curtailment", "%", 1),
]:
    key = metric + "|" + unit
    if key in kpi_lookup:
        row = kpi_lookup[key]
        cur_val = row.get(f"YTD {report_year}")
        prev_val = row.get(f"YTD {previous_year}")
        if unit == "%":
            value = fmt_pct(cur_val, decimals)
            delta = f"vs {previous_year}: {fmt_pct(prev_val, decimals)}"
        else:
            value = fmt_num(cur_val, decimals, f" {unit}")
            delta = f"vs {previous_year}: {fmt_num(prev_val, decimals, f' {unit}') }"
        cards.append((metric, value, delta))

if not bess_table.empty:
    bess_totals = bess_table[bess_table["month"] == "TOTAL"].copy()
    cur_bess = bess_totals[bess_totals["Year"] == report_year]["Revenue BESS €/MW"]
    prev_bess = bess_totals[bess_totals["Year"] == previous_year]["Revenue BESS €/MW"]
    if not cur_bess.empty:
        prev_label = fmt_num(prev_bess.iloc[0] / 1000.0, 1, " k€/MW") if not prev_bess.empty else "-"
        cards.append((f"BESS revenue {bess_duration_h:g}h", fmt_num(cur_bess.iloc[0] / 1000.0, 1, " k€/MW"), f"vs {previous_year}: {prev_label}"))

chart_html = ""
for title, img_b64 in charts.items():
    if img_b64:
        chart_html += f'<h3>{title}</h3><img src="data:image/png;base64,{img_b64}" style="max-width:100%;height:auto;border:1px solid #ddd;"><br><br>'

email_html = f"""
<html><body style="font-family:Arial,sans-serif;font-size:13px;color:#111827;background:#ffffff;">
<div style="max-width:1100px;margin:0 auto;">
{logo_html}
<div style="padding:14px 16px;border:1px solid #e5e7eb;border-radius:14px;background:linear-gradient(90deg,#ffffff 0%,#f8fafc 100%);margin-bottom:14px;">
<p style="margin:0 0 10px 0;">{intro_text.replace(chr(10), '<br>')}</p>
<p style="margin:0;"><strong>Cut-off:</strong> {current_window.end.strftime('%d-%b-%Y')}<br>
<strong>Report period:</strong> {current_window.start.strftime('%d-%b-%Y')} to {current_window.end.strftime('%d-%b-%Y')}<br>
<strong>Comparison period:</strong> {previous_window.start.strftime('%d-%b-%Y')} to {previous_window.end.strftime('%d-%b-%Y')}<br>
<strong>Comparison cut-off:</strong> {previous_window.end.strftime('%d-%b-%Y')}<br>
<strong>Heatmap year:</strong> {heatmap_year}</p>
</div>
{metric_cards_html(cards)}<br>
{section_header_html('Executive YTD KPIs', '📊')}{df_to_html_table(kpi_df, pct_cols=['% change'])}<br>
{section_header_html('Market and Day Ahead charts', '📈')}{chart_html}
{section_header_html('Weekly spot and captured prices', '📅')}{df_to_html_table(weekly_table)}<br>
{section_header_html('Solar captured prices and curtailment', '☀️')}{df_to_html_table(monthly_table, pct_cols=['Negative / zero hours (%)', 'Negative / zero solar hours (%)', 'Economic curtailment (%)'])}<br>
{section_header_html('Negative / zero price hours', '⚠️')}{df_to_html_table(neg_hours_table, pct_cols=['Negative / zero hours (%)', 'Negative / zero solar hours (%)'])}<br>
{section_header_html('Energy mix', '⚡')}{df_to_html_table(mix_table, pct_cols=['% change']) if not mix_table.empty else '<p>No energy mix data available.</p>'}<br>
{section_header_html('BESS revenue and spreads', '🔋')}{df_to_html_table(bess_table) if not bess_table.empty else '<p>No BESS revenue data available.</p>'}<br>
{section_header_html('BESS YTD duration comparison', '🔋')}{df_to_html_table(bess_duration_table) if not bess_duration_table.empty else '<p>No BESS duration comparison available.</p>'}<br>
{section_header_html('Demand', '🏭')}{df_to_html_table(demand_table, pct_cols=['% change']) if not demand_table.empty else '<p>No demand data available.</p>'}<br>
{section_header_html('Installed capacity', '🏗️')}{df_to_html_table(capacity_table, pct_cols=['% change']) if not capacity_table.empty else '<p>No installed capacity data available.</p>'}<br>
<p>Best regards,</p>
</div>
</body></html>
"""

pdf_filename = f"nexwell_power_monthly_ytd_report_{report_month.strftime('%Y_%m')}.pdf"
pdf_path = OUTPUT_DIR / pdf_filename
pdf_bytes = build_pdf_report(
    pdf_path,
    logo_path,
    subject,
    kpi_df,
    weekly_table,
    monthly_table,
    neg_hours_table,
    mix_table,
    demand_table,
    capacity_table,
    bess_table,
    bess_duration_table,
    charts,
    report_year,
    previous_year,
)

st.subheader("Preview")
st.markdown(email_html, unsafe_allow_html=True)

st.subheader("Quick tables")
col1, col2 = st.columns(2)
with col1:
    st.markdown("**YTD energy mix**")
    st.dataframe(mix_table, use_container_width=True)
with col2:
    st.markdown("**Negative / zero hours**")
    st.dataframe(neg_hours_table, use_container_width=True)

st.markdown("**BESS monthly revenue**")
st.dataframe(bess_table, use_container_width=True)
st.markdown("**BESS YTD duration comparison**")
st.dataframe(bess_duration_table, use_container_width=True)

st.subheader("PDF report")
st.download_button("Download PDF report", data=pdf_bytes, file_name=pdf_filename, mime="application/pdf")
st.download_button("Download email HTML", data=email_html, file_name=f"email_body_{report_month.strftime('%Y_%m')}.html", mime="text/html")

st.subheader("Recipients")
st.dataframe(pd.DataFrame({"To": [", ".join(parse_emails(to_emails_raw))], "Cc": [", ".join(parse_emails(cc_emails_raw))], "Subject": [subject]}), use_container_width=True)

send_method = st.radio("Send method", ["Webhook", "SMTP"], horizontal=True)
if st.button("Send monthly report now"):
    to_list = parse_emails(to_emails_raw)
    cc_list = parse_emails(cc_emails_raw)
    if not to_list:
        st.error("Add at least one recipient in To.")
    else:
        try:
            if send_method == "Webhook":
                ok, msg = send_via_webhook(to_list, cc_list, subject, email_html, pdf_bytes, pdf_filename)
            else:
                ok, msg = send_via_smtp(to_list, cc_list, subject, email_html, pdf_bytes, pdf_filename)
            if ok:
                st.success(msg)
            else:
                st.error(msg)
        except Exception as exc:
            st.error(f"Send failed: {exc}")

st.info(
    "This version adds a more corporate email layout, clearer BESS and solar sections with icons, a 24x365 OMIE hourly price heatmap for the selected year styled like the reference, negative-hours percentages, a duck-curve style hourly price profile, solar economic curtailment bars based on P48, and improved BESS visuals (monthly grouped revenue bars plus buy/sell/spread chart). The PDF remains charts-only and includes the Nexwell logo stamp."
)
