from __future__ import annotations

import calendar
import io
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from time import sleep
from typing import Iterable

import altair as alt
import numpy as np
import pandas as pd
import requests
import pulp
import streamlit as st
from dotenv import load_dotenv

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

# =========================================================
# PAGE / THEME
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
FORWARD_CURVES_DIRS = [
    BASE_DIR / "forward_curves",
    DATA_DIR / "forward_curves",
    Path(__file__).resolve().parent / "forward_curves",
]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Monthly Market Report", layout="wide")

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREEN_LIGHT = "#D1FAE5"
BLUE = "#1D4ED8"
BLUE_DARK = "#1E3A8A"
YELLOW = "#FBBF24"
YELLOW_DARK = "#D97706"
ORANGE = "#EA580C"
RED = "#DC2626"
PURPLE = "#7C3AED"
GREY = "#6B7280"
GRID = "#E5E7EB"
TEXT = "#111827"
WHITE = "#FFFFFF"

AURORA_COLOR = ORANGE
BARINGA_COLOR = BLUE_DARK

OMIP_BASE_URL = "https://www.omip.pt/en/dados-mercado"
OMIP_PRODUCTS = {"Power": "EL"}
OMIP_ZONES = {"Spain": "ES"}
OMIP_LIVE_INSTRUMENTS = {"Baseload": "FTB", "Solar": "FTS"}

st.markdown(
    f"""
    <style>
    html, body, [class*="css"] {{
        font-size: 100% !important;
    }}
    .stApp {{
        background: #FFFFFF;
    }}
    h1 {{
        font-size: 2.2rem !important;
        letter-spacing: -0.02em;
    }}
    h2 {{
        font-size: 1.4rem !important;
    }}
    h3 {{
        font-size: 1.15rem !important;
    }}
    .report-hero {{
        background: linear-gradient(115deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #B7EAD6 100%);
        border-radius: 18px;
        padding: 22px 24px;
        color: white;
        margin-bottom: 16px;
        box-shadow: 0 8px 24px rgba(15, 118, 110, 0.12);
    }}
    .report-hero-title {{
        font-size: 2rem;
        font-weight: 850;
        line-height: 1.1;
        margin-bottom: 6px;
    }}
    .report-hero-subtitle {{
        font-size: 1rem;
        opacity: 0.95;
    }}
    .section-banner {{
        background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 58%, #C7F0DD 100%);
        color: white;
        padding: 12px 16px;
        border-radius: 12px;
        font-weight: 800;
        font-size: 1.15rem;
        margin-top: 20px;
        margin-bottom: 12px;
        box-shadow: 0 2px 8px rgba(15,118,110,0.14);
    }}
    .mini-banner {{
        color: {TEXT};
        border-left: 5px solid {CORP_GREEN};
        padding: 7px 12px;
        font-weight: 800;
        font-size: 1.02rem;
        margin-top: 14px;
        margin-bottom: 8px;
        background: #F8FFFC;
        border-radius: 8px;
    }}
    .card-note {{
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 12px 14px;
        color: #334155;
        margin-bottom: 10px;
    }}
    .pill {{
        display: inline-block;
        background: #ECFDF5;
        border: 1px solid #A7F3D0;
        color: {CORP_GREEN_DARK};
        border-radius: 999px;
        padding: 4px 10px;
        font-size: 0.88rem;
        font-weight: 750;
        margin-right: 6px;
    }}
    .metric-caption {{
        color: #64748B;
        font-size: 0.82rem;
        font-weight: 600;
    }}
    div[data-testid="stMetricValue"] {{
        font-weight: 850;
        letter-spacing: -0.02em;
    }}
    div[data-testid="stMetricLabel"] {{
        font-weight: 720;
    }}
    .metric-footnote {{
        color: #64748B;
        font-size: 0.78rem;
        line-height: 1.35;
        margin-top: -0.35rem;
        margin-bottom: 0.55rem;
    }}
    .comparison-note {{
        color: #475569;
        font-size: 0.82rem;
        margin-top: 0.15rem;
        margin-bottom: 0.35rem;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

def section(title: str, icon: str = "") -> None:
    st.markdown(f'<div class="section-banner">{icon} {title}</div>', unsafe_allow_html=True)

def subsection(title: str) -> None:
    st.markdown(f'<div class="mini-banner">{title}</div>', unsafe_allow_html=True)

def card_note(text: str) -> None:
    st.markdown(f'<div class="card-note">{text}</div>', unsafe_allow_html=True)

def pills(items: list[str]) -> None:
    html = "".join(f'<span class="pill">{item}</span>' for item in items)
    st.markdown(html, unsafe_allow_html=True)

def apply_chart_style(chart, height: int = 340):
    return (
        chart.properties(height=height)
        .configure_view(stroke=GRID, fill=WHITE)
        .configure_axis(
            grid=True,
            gridColor=GRID,
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor=TEXT,
            titleColor=TEXT,
            labelFontSize=11,
            titleFontSize=13,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=11,
            titleFontSize=12,
            symbolStrokeWidth=3,
        )
        .configure_title(fontSize=14, anchor="start", color=TEXT)
    )

def fmt_eur(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{decimals}f} €/MWh"

def fmt_pct(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.{decimals}%}"

def fmt_mw_revenue(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.0f} €/MW".replace(",", ".")


def fmt_gwh(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{decimals}f} GWh"

def fmt_change_pct(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):+.{decimals}%}"

def fmt_pp(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value) * 100:+.{decimals}f} pp"

def month_label(ts: pd.Timestamp, mtd: bool = False) -> str:
    label = ts.strftime("%b %Y")
    return f"{label} (MTD)" if mtd else label

def month_start(d: date | pd.Timestamp) -> pd.Timestamp:
    t = pd.Timestamp(d)
    return pd.Timestamp(t.year, t.month, 1)

def month_end(ts: pd.Timestamp, current_day: date | None = None) -> pd.Timestamp:
    if current_day and ts.year == current_day.year and ts.month == current_day.month:
        return pd.Timestamp(current_day)
    last = calendar.monthrange(ts.year, ts.month)[1]
    return pd.Timestamp(ts.year, ts.month, last)

def previous_month(ts: pd.Timestamp) -> pd.Timestamp:
    return (ts - pd.offsets.MonthBegin(1)).normalize().replace(day=1)

def yoy_month(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts.year - 1, ts.month, 1)

def comparable_ytd_end(end_ts: pd.Timestamp, target_year: int) -> pd.Timestamp:
    """Return the same month/day cut-off in another year, with leap-day safety."""
    try:
        return pd.Timestamp(target_year, end_ts.month, end_ts.day)
    except ValueError:
        return pd.Timestamp(target_year, end_ts.month, 1) + pd.offsets.MonthEnd(0)

def values_equal_month(series: pd.Series, ts: pd.Timestamp) -> pd.Series:
    return (series.dt.year == ts.year) & (series.dt.month == ts.month)

# =========================================================
# FILES / CONFIG
# =========================================================
HIST_WORKBOOK_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_SOLAR_FILE = DATA_DIR / "p48solar_since21.csv"
HIST_MIX_FILE = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"
MIBGAS_LOCAL_PATTERN = "MIBGAS_Data_*.xlsx"
MIBGAS_CACHE_FILE = DATA_DIR / "mibgas_2026_cache.csv"
MIBGAS_TARGET_SHEET = "Trading Data PVB&VTP"
EEX_FORWARD_LOCAL_CSV = DATA_DIR / "eex_forward_market.csv"
EEX_FORWARD_LOCAL_XLSX = DATA_DIR / "eex_forward_market.xlsx"

BESS_MONTHLY_CANDIDATES = [
    DATA_DIR / "bess_monthly_metrics.xlsx",
    DATA_DIR / "bess_monthly_metrics.csv",
    DATA_DIR / "bess_report_monthly.xlsx",
    DATA_DIR / "bess_report_monthly.csv",
]
HYBRID_MONTHLY_CANDIDATES = [
    DATA_DIR / "hybrid_captured_price_monthly.xlsx",
    DATA_DIR / "hybrid_captured_price_monthly.csv",
    DATA_DIR / "pv_bess_hybrid_monthly.xlsx",
    DATA_DIR / "pv_bess_hybrid_monthly.csv",
]

BESS_REPORT_SOLAR_PROFILE_XLSX = DATA_DIR / "profile_production_1y_hourly.xlsx"
BESS_REPORT_DATA_XLSX = DATA_DIR / "data.xlsx"
BESS_REPORT_CAPACITY_MWH = 4.0
BESS_REPORT_C_RATE = 0.25
BESS_REPORT_POWER_MW = BESS_REPORT_CAPACITY_MWH * BESS_REPORT_C_RATE
BESS_REPORT_ETA_CH = 0.93
BESS_REPORT_ETA_DIS = 0.93
BESS_REPORT_CYCLE_LIMIT = 1.0

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
LIVE_START_DATE = date(2026, 1, 1)

REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {"geo_trunc": "electric_system", "geo_limit": "peninsular", "geo_ids": "8741"}

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

RENEWABLE_TECHS = {
    "Hydro", "Pumped hydro", "Wind", "Solar PV", "Solar thermal",
    "Other renewables", "Biomass", "Biogas",
}

# =========================================================
# HISTORICAL + LIVE DATA
# =========================================================
def build_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }

def _parse_datetime_esios(df: pd.DataFrame) -> pd.Series:
    for col in ["datetime", "datetime_utc", "datetime_local", "date"]:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", utc=True)
            if dt.notna().any():
                return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    return pd.to_datetime(pd.Series([pd.NaT] * len(df)))

def parse_esios_indicator(raw_json: dict, source_name: str) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    df = pd.DataFrame(values)
    if "geo_id" not in df.columns:
        df["geo_id"] = pd.NA
    if "geo_name" not in df.columns:
        df["geo_name"] = pd.NA
    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    df["datetime"] = _parse_datetime_esios(df)
    df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    df["source"] = source_name
    return df[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(indicator_id: int, start_day: date, end_day: date, token: str) -> pd.DataFrame:
    if not token or start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    frames: list[pd.DataFrame] = []
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
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=build_headers(token), params=params, timeout=(15, 120))
                resp.raise_for_status()
                parsed = parse_esios_indicator(resp.json(), f"esios_{indicator_id}")
                if not parsed.empty:
                    frames.append(parsed)
                break
            except requests.exceptions.RequestException:
                sleep(1.2 * (attempt + 1))
        chunk_start = chunk_end + timedelta(days=1)
    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )

@st.cache_data(show_spinner=False)
def load_historical_prices() -> pd.DataFrame:
    if not HIST_PRICES_FILE.exists():
        return pd.DataFrame(columns=["datetime", "price"])
    try:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name=0)
    if "price" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "price"})
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["datetime", "price"]).copy()
    return df[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_historical_solar() -> pd.DataFrame:
    if HIST_WORKBOOK_FILE.exists():
        try:
            df = pd.read_excel(HIST_WORKBOOK_FILE, sheet_name="solar_hourly_best")
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
            df = df.dropna(subset=["datetime", "solar_best_mw"]).copy()
            return df[["datetime", "solar_best_mw"]].sort_values("datetime").reset_index(drop=True)
        except Exception:
            pass
    if not HIST_SOLAR_FILE.exists():
        return pd.DataFrame(columns=["datetime", "solar_best_mw"])
    df = pd.read_csv(HIST_SOLAR_FILE)
    if "solar_best_mw" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "solar_best_mw"})
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
    df = df.dropna(subset=["datetime", "solar_best_mw"]).copy()
    return df[["datetime", "solar_best_mw"]].sort_values("datetime").reset_index(drop=True)

def build_best_solar_hourly(p48: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    p48 = p48.copy()
    fc = forecast.copy()
    if p48.empty and fc.empty:
        return pd.DataFrame(columns=["datetime", "solar_best_mw"])
    if p48.empty:
        out = fc.rename(columns={"solar_forecast_mw": "solar_best_mw"})[["datetime", "solar_best_mw"]]
        return out.sort_values("datetime").reset_index(drop=True)
    if fc.empty:
        out = p48.rename(columns={"solar_p48_mw": "solar_best_mw"})[["datetime", "solar_best_mw"]]
        return out.sort_values("datetime").reset_index(drop=True)
    out = p48.merge(fc, on="datetime", how="outer").sort_values("datetime")
    out["solar_best_mw"] = out["solar_p48_mw"].combine_first(out["solar_forecast_mw"])
    return out[["datetime", "solar_best_mw"]].dropna().sort_values("datetime").reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def load_live_prices(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token)
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])
    out = raw[["datetime", "value"]].rename(columns={"value": "price"})
    out["datetime"] = out["datetime"].dt.floor("h")
    return out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")

@st.cache_data(show_spinner=False, ttl=3600)
def load_live_solar(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw_p48 = fetch_esios_range(SOLAR_P48_INDICATOR_ID, start_day, end_day, token)
    raw_fc = fetch_esios_range(SOLAR_FORECAST_INDICATOR_ID, start_day, end_day, token)
    if not raw_p48.empty:
        p48 = raw_p48[["datetime", "value"]].rename(columns={"value": "solar_p48_mw"})
        p48["datetime"] = p48["datetime"].dt.floor("h")
        p48 = p48.groupby("datetime", as_index=False)["solar_p48_mw"].mean()
    else:
        p48 = pd.DataFrame(columns=["datetime", "solar_p48_mw"])
    if not raw_fc.empty:
        fc = raw_fc[["datetime", "value"]].rename(columns={"value": "solar_forecast_mw"})
        fc["datetime"] = fc["datetime"].dt.floor("h")
        fc = fc.groupby("datetime", as_index=False)["solar_forecast_mw"].mean()
    else:
        fc = pd.DataFrame(columns=["datetime", "solar_forecast_mw"])
    return build_best_solar_hourly(p48, fc)

def combine_hist_live(hist: pd.DataFrame, live: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    out = pd.concat([hist, live], ignore_index=True)
    if out.empty:
        return out
    return out.sort_values(subset).drop_duplicates(subset=subset, keep="last").reset_index(drop=True)


# =========================================================
# MIBGAS MONTHLY KPI + ELECTRICITY MIX SUMMARY
# =========================================================
def _clean_tabular_col(col) -> str:
    s = str(col).replace("\xa0", " ").replace("\n", " ").strip().lower()
    repl = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n", "[": "", "]": "", "(": "", ")": "", "%": "pct", "/": "_", "-": "_", ".": "_"}
    for a, b in repl.items():
        s = s.replace(a, b)
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _first_tabular_col(columns: list[str], candidates: list[str]) -> str | None:
    normalised = {_clean_tabular_col(c): c for c in columns}
    for cand in candidates:
        key = _clean_tabular_col(cand)
        if key in normalised:
            return normalised[key]
    return None


def _to_number_mibgas(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    s = series.astype(str).str.strip().str.replace("€", "", regex=False).str.replace(" ", "", regex=False).str.replace("\xa0", "", regex=False)
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _standardize_mibgas_raw(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["trading_day", "product", "area", "delivery_start", "delivery_end", "reference_price_eur_mwh"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    work = df.copy()
    work.columns = [_clean_tabular_col(c) for c in work.columns]
    colmap = {
        "trading_day": _first_tabular_col(work.columns.tolist(), ["trading_day", "trading day"]),
        "product": _first_tabular_col(work.columns.tolist(), ["product"]),
        "area": _first_tabular_col(work.columns.tolist(), ["area"]),
        "delivery_start": _first_tabular_col(work.columns.tolist(), ["first_day_delivery", "first day delivery", "delivery_start"]),
        "delivery_end": _first_tabular_col(work.columns.tolist(), ["last_day_delivery", "last day delivery", "delivery_end"]),
        "reference_price": _first_tabular_col(work.columns.tolist(), ["reference_price_eur_mwh", "reference price eur mwh", "daily_reference_price_eur_mwh", "daily reference price eur mwh"]),
    }
    if colmap["trading_day"] is None or colmap["product"] is None:
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame()
    out["trading_day"] = pd.to_datetime(work[colmap["trading_day"]], dayfirst=True, errors="coerce")
    out["product"] = work[colmap["product"]].astype(str).str.strip()
    out["area"] = work[colmap["area"]].astype(str).str.strip() if colmap["area"] else "ES"
    out["delivery_start"] = pd.to_datetime(work[colmap["delivery_start"]], dayfirst=True, errors="coerce") if colmap["delivery_start"] else pd.NaT
    out["delivery_end"] = pd.to_datetime(work[colmap["delivery_end"]], dayfirst=True, errors="coerce") if colmap["delivery_end"] else pd.NaT
    out["reference_price_eur_mwh"] = _to_number_mibgas(work[colmap["reference_price"]]) if colmap["reference_price"] else pd.NA
    out = out.dropna(subset=["trading_day", "product"]).copy()
    return out[cols].reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_mibgas_actuals_for_report() -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for path in sorted(DATA_DIR.glob(MIBGAS_LOCAL_PATTERN)):
        try:
            xls = pd.ExcelFile(path)
            sheet = MIBGAS_TARGET_SHEET if MIBGAS_TARGET_SHEET in xls.sheet_names else next((s for s in xls.sheet_names if "PVB" in str(s).upper() and "VTP" in str(s).upper()), None)
            if sheet is None:
                continue
            parsed = _standardize_mibgas_raw(pd.read_excel(path, sheet_name=sheet))
            if not parsed.empty:
                parts.append(parsed)
        except Exception:
            continue
    if MIBGAS_CACHE_FILE.exists():
        try:
            cache = pd.read_csv(MIBGAS_CACHE_FILE)
            parsed = _standardize_mibgas_raw(cache)
            if not parsed.empty:
                parts.append(parsed)
        except Exception:
            pass
    if not parts:
        return pd.DataFrame(columns=["trading_day", "price"])
    raw = pd.concat(parts, ignore_index=True)
    raw["reference_price_eur_mwh"] = pd.to_numeric(raw["reference_price_eur_mwh"], errors="coerce")
    actuals = raw[(raw["product"] == "GDAES_D+1") & (raw["area"].fillna("ES") == "ES")].copy()
    actuals["delivery_day"] = pd.to_datetime(actuals["delivery_start"], errors="coerce").combine_first(pd.to_datetime(actuals["trading_day"], errors="coerce"))
    actuals["price"] = actuals["reference_price_eur_mwh"]
    actuals = actuals.dropna(subset=["delivery_day", "price"]).copy()
    return actuals[["delivery_day", "price"]].drop_duplicates(subset=["delivery_day"], keep="last").sort_values("delivery_day").reset_index(drop=True)


def mibgas_monthly_mean(actuals: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> float | None:
    if actuals is None or actuals.empty:
        return None
    day = pd.to_datetime(actuals["delivery_day"], errors="coerce")
    mask = (day >= start_ts) & (day < end_ts + pd.Timedelta(days=1))
    values = pd.to_numeric(actuals.loc[mask, "price"], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _parse_mixed_date_for_mix(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT
    try:
        dt = pd.to_datetime(s, utc=True, errors="raise")
        return dt.tz_convert("Europe/Madrid").tz_localize(None).normalize()
    except Exception:
        pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise").normalize()
    except Exception:
        return pd.NaT


@st.cache_data(show_spinner=False)
def load_historical_generation_mix_daily_for_report() -> pd.DataFrame:
    cols = ["datetime", "technology", "energy_mwh", "data_source"]
    if not HIST_MIX_FILE.exists():
        return pd.DataFrame(columns=cols)
    try:
        raw = pd.read_excel(HIST_MIX_FILE, sheet_name="data", header=None)
    except Exception:
        return pd.DataFrame(columns=cols)
    if raw.shape[0] < 18 or raw.shape[1] < 2:
        return pd.DataFrame(columns=cols)
    dates = [_parse_mixed_date_for_mix(v) for v in raw.iloc[4, 1:].tolist()]
    records: list[dict] = []
    for _, row in raw.iloc[5:18, :].iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        values = pd.to_numeric(row.iloc[1:], errors="coerce")
        for dt, val in zip(dates, values):
            if pd.isna(dt) or pd.isna(val):
                continue
            records.append({"datetime": pd.Timestamp(dt).normalize(), "technology": tech, "energy_mwh": float(val) * 1000.0, "data_source": "Historical file"})
    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=cols)
    out = out[out["datetime"].dt.year.between(2022, 2025)].copy()
    hydro = out[out["technology"] == "Hydro"].groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
    hydro["technology"] = "Hydro"
    non_hydro = out[out["technology"] != "Hydro"].copy()
    out = pd.concat([non_hydro, hydro], ignore_index=True)
    return out.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_ree_widget_for_report(category: str, widget: str, start_day: date, end_day: date, time_trunc: str = "month") -> dict:
    params = {"start_date": f"{start_day.isoformat()}T00:00", "end_date": f"{end_day.isoformat()}T23:59", "time_trunc": time_trunc, **REE_PENINSULAR_PARAMS}
    resp = requests.get(f"{REE_API_BASE}/{category}/{widget}", params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_ree_included_series_for_report(payload: dict) -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or item.get("id")
        for val in attrs.get("values", []) or []:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            rows.append({"datetime": dt.tz_convert("Europe/Madrid").tz_localize(None), "title": str(title).strip(), "value": pd.to_numeric(val.get("value"), errors="coerce")})
    return pd.DataFrame(rows)


def _normalize_ree_energy_to_mwh(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    max_abs = vals.abs().max(skipna=True) if not vals.empty else None
    return vals * 1000.0 if pd.notna(max_abs) and max_abs < 10000 else vals


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_mix_monthly_for_report(start_day: date, end_day: date) -> pd.DataFrame:
    cols = ["datetime", "technology", "energy_mwh", "data_source"]
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=cols)
    try:
        payload = fetch_ree_widget_for_report("generacion", "estructura-generacion", start_day, end_day, time_trunc="month")
        df = parse_ree_included_series_for_report(payload)
    except Exception:
        return pd.DataFrame(columns=cols)
    if df.empty:
        return pd.DataFrame(columns=cols)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    df["technology"] = df["title"].map(lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip()))
    df["energy_mwh"] = _normalize_ree_energy_to_mwh(df["value"])
    df["data_source"] = "REE API"
    df = df.dropna(subset=["datetime", "technology", "energy_mwh"]).copy()
    hydro = df[df["technology"].isin(["Hydro", "Pumped hydro"])].groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
    hydro["technology"] = "Hydro"
    non_hydro = df[~df["technology"].isin(["Hydro", "Pumped hydro"])].copy()
    df = pd.concat([non_hydro[["datetime", "technology", "energy_mwh", "data_source"]], hydro], ignore_index=True)
    return df.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


def build_generation_month_metrics(mix_daily_hist: pd.DataFrame, mix_monthly_live: pd.DataFrame, target_month: pd.Timestamp) -> dict[str, float | None]:
    if target_month.year <= 2025:
        mix = mix_daily_hist.copy()
        if not mix.empty:
            mask = (mix["datetime"].dt.year == target_month.year) & (mix["datetime"].dt.month == target_month.month)
            mix = mix.loc[mask].copy()
    else:
        mix = mix_monthly_live.copy()
        if not mix.empty:
            mask = (mix["datetime"].dt.year == target_month.year) & (mix["datetime"].dt.month == target_month.month)
            mix = mix.loc[mask].copy()
    if mix.empty:
        return {"re_share": None, "solar_gwh": None, "wind_gwh": None, "hydro_gwh": None, "nuclear_gwh": None}
    grouped = mix.groupby("technology", as_index=False)["energy_mwh"].sum()
    energy_map = dict(zip(grouped["technology"], grouped["energy_mwh"]))
    total = float(grouped["energy_mwh"].sum()) if not grouped.empty else np.nan
    renewables = float(grouped[grouped["technology"].isin(RENEWABLE_TECHS)]["energy_mwh"].sum()) if not grouped.empty else np.nan
    solar_mwh = float(energy_map.get("Solar PV", 0.0) + energy_map.get("Solar thermal", 0.0))
    return {
        "re_share": None if not total or pd.isna(total) else renewables / total,
        "solar_gwh": solar_mwh / 1000.0,
        "wind_gwh": float(energy_map.get("Wind", 0.0)) / 1000.0,
        "hydro_gwh": float(energy_map.get("Hydro", 0.0)) / 1000.0,
        "nuclear_gwh": float(energy_map.get("Nuclear", 0.0)) / 1000.0,
    }


def build_generation_month_comparison_table(current: dict[str, float | None], previous: dict[str, float | None], current_label: str, previous_label: str) -> pd.DataFrame:
    specs = [
        ("Renewable generation share", "re_share", "share"),
        ("Solar injected", "solar_gwh", "gwh"),
        ("Wind injected", "wind_gwh", "gwh"),
        ("Hydro injected", "hydro_gwh", "gwh"),
        ("Nuclear injected", "nuclear_gwh", "gwh"),
    ]
    rows = []
    for label, key, kind in specs:
        curr = current.get(key)
        prev = previous.get(key)
        if kind == "share":
            curr_display, prev_display = fmt_pct(curr), fmt_pct(prev)
            diff_display = fmt_pp(None if curr is None or prev is None else curr - prev)
        else:
            curr_display, prev_display = fmt_gwh(curr), fmt_gwh(prev)
            diff = None if curr is None or prev in [None, 0] or pd.isna(prev) else (curr / prev) - 1
            diff_display = fmt_change_pct(diff)
        rows.append({"Metric": label, current_label: curr_display, previous_label: prev_display, "Diff vs prev.": diff_display})
    return pd.DataFrame(rows)


def style_generation_comparison_table(df: pd.DataFrame):
    return (
        df.style
        .set_properties(**{"font-size": "0.92rem", "padding": "8px 10px"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#0F766E"), ("color", "white"), ("font-weight", "800"), ("text-align", "center")]},
            {"selector": "tbody td:first-child", "props": [("font-weight", "800"), ("background-color", "#F8FFFC")]},
        ])
    )

# =========================================================
# FORWARD HOURLY SCENARIOS (AURORA / BARINGA)
# =========================================================
def _norm_col(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")

def _scenario_model_from_name(path: Path) -> str | None:
    name = path.name.lower()
    if "aurora" in name:
        return "Aurora"
    if "baringa" in name or "bringa" in name:
        return "Baringa"
    return None

def _forward_datetime_from_sheet(df: pd.DataFrame) -> pd.Series:
    cols = {_norm_col(c): c for c in df.columns}
    for key in ["datetime", "date_time", "timestamp", "cet", "cest", "local_datetime", "period_start"]:
        if key in cols:
            dt = pd.to_datetime(df[cols[key]], errors="coerce", dayfirst=True)
            if dt.notna().any():
                return dt
    day_col = next((cols[k] for k in ["day", "date", "fecha", "dia"] if k in cols), None)
    hour_col = next((cols[k] for k in ["hour", "period", "hora", "he"] if k in cols), None)
    if day_col is not None and hour_col is not None:
        day = pd.to_datetime(df[day_col], errors="coerce", dayfirst=True).dt.normalize()
        hour = pd.to_numeric(df[hour_col], errors="coerce")
        valid = hour.dropna()
        if not valid.empty and valid.min() >= 1 and valid.max() <= 24:
            hour = hour - 1
        return day + pd.to_timedelta(hour, unit="h")
    return pd.Series(pd.NaT, index=df.index)

def _forward_price_col(df: pd.DataFrame) -> str | None:
    cols = {_norm_col(c): c for c in df.columns}
    for key in ["omie_venta", "price", "spot_price", "market_price", "value", "eur_mwh"]:
        if key in cols:
            return cols[key]
    return None

def _normalise_forward_candidate(raw: pd.DataFrame, model: str, path: Path, sheet: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["datetime", "price", "model", "source_file", "source_sheet"])
    dt = _forward_datetime_from_sheet(raw)
    price_col = _forward_price_col(raw)
    if price_col is None:
        return pd.DataFrame(columns=["datetime", "price", "model", "source_file", "source_sheet"])
    out = pd.DataFrame({
        "datetime": pd.to_datetime(dt, errors="coerce"),
        "price": pd.to_numeric(raw[price_col], errors="coerce"),
        "model": model,
        "source_file": path.name,
        "source_sheet": sheet,
    }).dropna(subset=["datetime", "price"])
    return out.sort_values("datetime").reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_forward_hourly_scenarios() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    seen: set[Path] = set()
    for folder in FORWARD_CURVES_DIRS:
        if not folder.exists():
            continue
        for pattern in ("*.xlsx", "*.xls", "*.csv"):
            for path in folder.glob(pattern):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                model = _scenario_model_from_name(path)
                if model is None:
                    continue
                candidates: list[pd.DataFrame] = []
                try:
                    if path.suffix.lower() == ".csv":
                        candidates.append(_normalise_forward_candidate(pd.read_csv(path), model, path, "CSV"))
                    else:
                        xls = pd.ExcelFile(path)
                        for sheet in xls.sheet_names:
                            try:
                                candidates.append(_normalise_forward_candidate(pd.read_excel(path, sheet_name=sheet), model, path, sheet))
                            except Exception:
                                continue
                except Exception:
                    continue
                candidates = [c for c in candidates if not c.empty]
                if candidates:
                    frames.append(max(candidates, key=len))
    if not frames:
        return pd.DataFrame(columns=["datetime", "price", "model", "source_file", "source_sheet"])
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(subset=["datetime", "model"], keep="last").sort_values(["model", "datetime"]).reset_index(drop=True)

# =========================================================
# CAPTURE / CURTAILMENT / HEATMAPS
# =========================================================
def period_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> dict[str, float | None]:
    mask_p = (price_hourly["datetime"] >= start_ts) & (price_hourly["datetime"] < end_ts + pd.Timedelta(days=1))
    p = price_hourly.loc[mask_p, ["datetime", "price"]].copy()
    if p.empty:
        return {"avg_price": None, "captured_uncurtailed": None, "captured_curtailed": None, "capture_rate_uncurtailed": None, "capture_rate_curtailed": None}
    avg_price = float(p["price"].mean())
    mask_s = (solar_hourly["datetime"] >= start_ts) & (solar_hourly["datetime"] < end_ts + pd.Timedelta(days=1))
    s = solar_hourly.loc[mask_s, ["datetime", "solar_best_mw"]].copy()
    merged = p.merge(s, on="datetime", how="inner")
    merged = merged[pd.to_numeric(merged["solar_best_mw"], errors="coerce").fillna(0) > 0].copy()
    if merged.empty:
        return {"avg_price": avg_price, "captured_uncurtailed": None, "captured_curtailed": None, "capture_rate_uncurtailed": None, "capture_rate_curtailed": None}
    merged["weighted"] = merged["price"] * merged["solar_best_mw"]
    unc = merged["weighted"].sum() / merged["solar_best_mw"].sum()
    curtailed = merged[merged["price"] > 0].copy()
    cur = curtailed["weighted"].sum() / curtailed["solar_best_mw"].sum() if not curtailed.empty and curtailed["solar_best_mw"].sum() else np.nan
    return {
        "avg_price": float(avg_price),
        "captured_uncurtailed": float(unc),
        "captured_curtailed": None if pd.isna(cur) else float(cur),
        "capture_rate_uncurtailed": None if avg_price == 0 else float(unc / avg_price),
        "capture_rate_curtailed": None if avg_price == 0 or pd.isna(cur) else float(cur / avg_price),
    }

def monthly_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame(columns=["period", "avg_spot_price", "captured_solar_price_uncurtailed", "captured_solar_price_curtailed", "capture_pct_uncurtailed", "capture_pct_curtailed"])
    months = sorted(price_hourly["datetime"].dt.to_period("M").dropna().unique())
    rows = []
    for month in months:
        start = month.to_timestamp()
        end = month_end(start)
        m = period_metrics(price_hourly, solar_hourly, start, end)
        rows.append({
            "period": start,
            "avg_spot_price": m["avg_price"],
            "captured_solar_price_uncurtailed": m["captured_uncurtailed"],
            "captured_solar_price_curtailed": m["captured_curtailed"],
            "capture_pct_uncurtailed": m["capture_rate_uncurtailed"],
            "capture_pct_curtailed": m["capture_rate_curtailed"],
        })
    return pd.DataFrame(rows).sort_values("period").reset_index(drop=True)

def annual_capture_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp | None = None) -> dict[str, float | None]:
    start = pd.Timestamp(year, 1, 1)
    end = end_ts if end_ts is not None else pd.Timestamp(year, 12, 31)
    return period_metrics(price_hourly, solar_hourly, start, end)

def build_report_capture_table(
    monthly: pd.DataFrame,
    price_hourly: pd.DataFrame,
    solar_hourly: pd.DataFrame,
    forward_scenarios: pd.DataFrame,
    report_month: pd.Timestamp,
    report_end: pd.Timestamp,
) -> pd.DataFrame:
    month_names = [calendar.month_abbr[m] for m in range(1, 13)]
    rows = []
    monthly = monthly.copy()
    monthly["year"] = monthly["period"].dt.year
    monthly["month_num"] = monthly["period"].dt.month
    for m in range(1, 13):
        row = {"Month": month_names[m - 1]}
        for year in [2025, 2026]:
            hit = monthly[(monthly["year"] == year) & (monthly["month_num"] == m)]
            prefix = str(year)
            if not hit.empty:
                r = hit.iloc[-1]
                row[f"{prefix} Baseload"] = r["avg_spot_price"]
                row[f"{prefix} Solar unc."] = r["captured_solar_price_uncurtailed"]
                row[f"{prefix} Rate unc."] = r["capture_pct_uncurtailed"]
                row[f"{prefix} Solar curt."] = r["captured_solar_price_curtailed"]
                row[f"{prefix} Rate curt."] = r["capture_pct_curtailed"]
            else:
                for col in ["Baseload", "Solar unc.", "Rate unc.", "Solar curt.", "Rate curt."]:
                    row[f"{prefix} {col}"] = np.nan
        rows.append(row)

    # Annual / YTD actuals
    yr25 = annual_capture_metrics(price_hourly, solar_hourly, 2025)
    ytd26_end = report_end if report_month.year == 2026 else pd.Timestamp(2026, 12, 31)
    ytd26 = annual_capture_metrics(price_hourly, solar_hourly, 2026, end_ts=ytd26_end)
    final = {"Month": "YR / YTD"}
    for prefix, metrics in [("2025", yr25), ("2026", ytd26)]:
        final[f"{prefix} Baseload"] = metrics["avg_price"]
        final[f"{prefix} Solar unc."] = metrics["captured_uncurtailed"]
        final[f"{prefix} Rate unc."] = metrics["capture_rate_uncurtailed"]
        final[f"{prefix} Solar curt."] = metrics["captured_curtailed"]
        final[f"{prefix} Rate curt."] = metrics["capture_rate_curtailed"]
    rows.append(final)

    # 2026 YTD forecast rows for direct Aurora / Baringa comparison
    if report_month.year == 2026 and forward_scenarios is not None and not forward_scenarios.empty:
        for model in ["Aurora", "Baringa"]:
            f = forward_scenarios[
                (forward_scenarios["model"] == model)
                & (forward_scenarios["datetime"].dt.year == 2026)
                & (forward_scenarios["datetime"] <= ytd26_end + pd.Timedelta(days=1))
            ][["datetime", "price"]].copy()
            row = {"Month": f"YTD {model}"}
            for col in ["Baseload", "Solar unc.", "Rate unc.", "Solar curt.", "Rate curt."]:
                row[f"2025 {col}"] = np.nan
            metrics = annual_capture_metrics(f, solar_hourly, 2026, end_ts=ytd26_end) if not f.empty else {
                "avg_price": None,
                "captured_uncurtailed": None,
                "capture_rate_uncurtailed": None,
                "captured_curtailed": None,
                "capture_rate_curtailed": None,
            }
            row["2026 Baseload"] = metrics["avg_price"]
            row["2026 Solar unc."] = metrics["captured_uncurtailed"]
            row["2026 Rate unc."] = metrics["capture_rate_uncurtailed"]
            row["2026 Solar curt."] = metrics["captured_curtailed"]
            row["2026 Rate curt."] = metrics["capture_rate_curtailed"]
            rows.append(row)
    return pd.DataFrame(rows)

def format_capture_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    fmt = {}
    for col in df.columns:
        if "Rate" in col:
            fmt[col] = "{:.1%}"
        elif col != "Month":
            fmt[col] = "{:,.1f}"

    def _style_col(col: pd.Series) -> list[str]:
        name = str(col.name)
        if name.startswith("2025 "):
            base = "background-color: #F1F5F9;"
            if "Baseload" in name:
                base = "background-color: #DBEAFE; font-weight: 800; border-left: 2px solid #1D4ED8; border-right: 2px solid #1D4ED8;"
            return [base] * len(col)
        if name.startswith("2026 ") and "Baseload" in name:
            return ["font-weight: 800; border-left: 2px solid #1D4ED8; border-right: 2px solid #1D4ED8;"] * len(col)
        return [""] * len(col)

    def _style_rows(row: pd.Series) -> list[str]:
        label = str(row.get("Month", ""))
        if label == "YR / YTD":
            return ["background-color: #ECFDF5; font-weight: 900; border-top: 2px solid #10B981; border-bottom: 2px solid #10B981;"] * len(row)
        if label.startswith("YTD Aurora"):
            return ["background-color: #FFF7ED; font-weight: 800;"] * len(row)
        if label.startswith("YTD Baringa"):
            return ["background-color: #EFF6FF; font-weight: 800;"] * len(row)
        return [""] * len(row)

    return (
        df.style.format(fmt, na_rep="—")
        .apply(_style_col, axis=0)
        .apply(_style_rows, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "5px 7px")]},
        ])
    )


def _month_start_day_numbers(year: int) -> list[int]:
    return [pd.Timestamp(year, m, 1).dayofyear for m in range(1, 13)]


def _month_label_expr(year: int) -> str:
    month_starts = _month_start_day_numbers(year)
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    parts = [f"datum.value == {d} ? '{name}'" for d, name in zip(month_starts, month_names)]
    return " : ".join(parts) + " : ''"


def ytd_price_heatmap(price_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp):
    """24 x 365/366 hourly spot heatmap, matching the Day-Ahead page style.

    The full calendar grid is always drawn. 2025 therefore shows the full year,
    while 2026 keeps future / not-yet-available hours blank.
    """
    cols = ["datetime", "date", "date_label", "day_of_year", "hour", "price", "is_missing"]
    if price_hourly.empty:
        return None

    tmp = price_hourly.copy()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp["price"] = pd.to_numeric(tmp["price"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime", "price"])
    tmp = tmp[(tmp["datetime"].dt.year == year) & (tmp["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()

    full_days = pd.date_range(date(year, 1, 1), date(year, 12, 31), freq="D")
    full_grid = pd.MultiIndex.from_product([full_days, range(24)], names=["date", "hour"]).to_frame(index=False)
    full_grid["date"] = pd.to_datetime(full_grid["date"])
    full_grid["datetime"] = full_grid["date"] + pd.to_timedelta(full_grid["hour"], unit="h")
    full_grid["date_label"] = full_grid["date"].dt.strftime("%Y-%m-%d")
    full_grid["day_of_year"] = full_grid["date"].dt.dayofyear.astype(int)

    if tmp.empty:
        hourly = pd.DataFrame(columns=["date", "hour", "price"])
    else:
        tmp["date"] = tmp["datetime"].dt.normalize()
        tmp["hour"] = tmp["datetime"].dt.hour
        hourly = tmp.groupby(["date", "hour"], as_index=False)["price"].mean()

    plot = full_grid.merge(hourly, on=["date", "hour"], how="left")
    plot["is_missing"] = plot["price"].isna()
    valid = plot["price"].dropna()
    if valid.empty:
        return None

    p_low = float(min(valid.min(), valid.quantile(0.01), 0.0))
    p_high = float(max(valid.quantile(0.99), 120.0))
    if p_high <= p_low:
        p_high = p_low + 1.0
    p_mid_1 = p_low + (p_high - p_low) * 0.30
    p_mid_2 = p_low + (p_high - p_low) * 0.55
    p_mid_3 = p_low + (p_high - p_low) * 0.78

    month_starts = _month_start_day_numbers(year)
    label_expr = _month_label_expr(year)
    x_enc = alt.X(
        "day_of_year:O",
        title="Month",
        sort=list(range(1, int(plot["day_of_year"].max()) + 1)),
        axis=alt.Axis(values=month_starts, labelExpr=label_expr, labelAngle=0, grid=False, ticks=True, domain=True),
    )
    y_enc = alt.Y(
        "hour:O",
        title="Hour",
        sort=list(range(24)),
        axis=alt.Axis(values=list(range(0, 24, 2))),
    )
    base = alt.Chart(plot)

    rect_missing = base.transform_filter("datum.is_missing").mark_rect(
        fill="#F3F4F6",
        stroke="#F3F4F6",
    ).encode(
        x=x_enc,
        y=y_enc,
        tooltip=[
            alt.Tooltip("date_label:N", title="Date"),
            alt.Tooltip("hour:O", title="Hour"),
        ],
    )

    rect_prices = base.transform_filter("!datum.is_missing").mark_rect().encode(
        x=x_enc,
        y=y_enc,
        color=alt.Color(
            "price:Q",
            title="Spot [€/MWh]",
            scale=alt.Scale(
                domain=[p_low, p_mid_1, p_mid_2, p_mid_3, p_high],
                range=["#047857", "#86EFAC", "#FEF08A", "#FB923C", "#B91C1C"],
                clamp=True,
            ),
            legend=alt.Legend(orient="right", title="Spot [€/MWh]"),
        ),
        tooltip=[
            alt.Tooltip("datetime:T", title="Datetime", format="%Y-%m-%d %H:%M"),
            alt.Tooltip("price:Q", title="Spot €/MWh", format=",.2f"),
        ],
    )
    chart = alt.layer(rect_missing, rect_prices).properties(
        title=f"OMIE hourly price heatmap | {year} | 24 x {len(full_days)}"
    )
    return apply_chart_style(chart, height=455)


def pdf_hourly_spot_heatmap_segment(
    price_hourly: pd.DataFrame,
    year: int,
    month_start_num: int,
    month_end_num: int,
    end_ts: pd.Timestamp,
):
    """PDF-friendly hourly spot heatmap split into shorter calendar blocks.

    Full-year 24 x 365 heatmaps are excellent on the web, but become too flat in
    landscape A4 PDF. This helper keeps the same hourly granularity while splitting
    the year into multi-month segments so the rendered image remains legible.
    """
    if price_hourly.empty:
        return None

    seg_start = pd.Timestamp(year, month_start_num, 1)
    seg_end = pd.Timestamp(year, month_end_num, calendar.monthrange(year, month_end_num)[1])

    tmp = price_hourly.copy()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp["price"] = pd.to_numeric(tmp["price"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime", "price"])
    tmp = tmp[
        (tmp["datetime"] >= seg_start)
        & (tmp["datetime"] <= min(seg_end + pd.Timedelta(hours=23), end_ts + pd.Timedelta(days=1)))
    ].copy()

    full_days = pd.date_range(seg_start.normalize(), seg_end.normalize(), freq="D")
    if len(full_days) == 0:
        return None
    full_grid = pd.MultiIndex.from_product([full_days, range(24)], names=["date", "hour"]).to_frame(index=False)
    full_grid["date"] = pd.to_datetime(full_grid["date"])
    full_grid["datetime"] = full_grid["date"] + pd.to_timedelta(full_grid["hour"], unit="h")
    full_grid["date_label"] = full_grid["date"].dt.strftime("%Y-%m-%d")
    full_grid["segment_day"] = (full_grid["date"] - full_grid["date"].min()).dt.days + 1

    if tmp.empty:
        hourly = pd.DataFrame(columns=["date", "hour", "price"])
    else:
        tmp["date"] = tmp["datetime"].dt.normalize()
        tmp["hour"] = tmp["datetime"].dt.hour
        hourly = tmp.groupby(["date", "hour"], as_index=False)["price"].mean()

    plot = full_grid.merge(hourly, on=["date", "hour"], how="left")
    plot["is_missing"] = plot["price"].isna()
    valid = plot["price"].dropna()
    if valid.empty:
        return None

    p_low = float(min(valid.min(), valid.quantile(0.01), 0.0))
    p_high = float(max(valid.quantile(0.99), 120.0))
    if p_high <= p_low:
        p_high = p_low + 1.0
    p_mid_1 = p_low + (p_high - p_low) * 0.30
    p_mid_2 = p_low + (p_high - p_low) * 0.55
    p_mid_3 = p_low + (p_high - p_low) * 0.78

    month_ticks = []
    month_labels = []
    for m in range(month_start_num, month_end_num + 1):
        tick = int((pd.Timestamp(year, m, 1) - seg_start).days + 1)
        month_ticks.append(tick)
        month_labels.append(calendar.month_abbr[m])
    label_expr = " : ".join([f"datum.value == {tick} ? '{label}'" for tick, label in zip(month_ticks, month_labels)]) + " : ''"

    x_enc = alt.X(
        "segment_day:O",
        title="Month",
        sort=list(range(1, int(plot["segment_day"].max()) + 1)),
        axis=alt.Axis(values=month_ticks, labelExpr=label_expr, labelAngle=0, grid=False, ticks=True, domain=True),
    )
    y_enc = alt.Y(
        "hour:O",
        title="Hour",
        sort=list(range(24)),
        axis=alt.Axis(values=list(range(0, 24, 2))),
    )
    base = alt.Chart(plot)

    rect_missing = base.transform_filter("datum.is_missing").mark_rect(
        fill="#F3F4F6",
        stroke="#F3F4F6",
    ).encode(
        x=x_enc,
        y=y_enc,
        tooltip=[
            alt.Tooltip("date_label:N", title="Date"),
            alt.Tooltip("hour:O", title="Hour"),
        ],
    )

    rect_prices = base.transform_filter("!datum.is_missing").mark_rect().encode(
        x=x_enc,
        y=y_enc,
        color=alt.Color(
            "price:Q",
            title="Spot [€/MWh]",
            scale=alt.Scale(
                domain=[p_low, p_mid_1, p_mid_2, p_mid_3, p_high],
                range=["#047857", "#86EFAC", "#FEF08A", "#FB923C", "#B91C1C"],
                clamp=True,
            ),
            legend=alt.Legend(orient="right", title="Spot [€/MWh]"),
        ),
        tooltip=[
            alt.Tooltip("datetime:T", title="Datetime", format="%Y-%m-%d %H:%M"),
            alt.Tooltip("price:Q", title="Spot €/MWh", format=",.2f"),
        ],
    )
    chart = alt.layer(rect_missing, rect_prices).properties(
        title=f"OMIE hourly price heatmap | {year} | {calendar.month_abbr[month_start_num]}-{calendar.month_abbr[month_end_num]}"
    )
    return apply_chart_style(chart, height=510)


def _completed_month_numbers(year: int, end_ts: pd.Timestamp) -> list[int]:
    if year < end_ts.year:
        return list(range(1, 13))
    if year > end_ts.year:
        return []
    completed = []
    for month in range(1, 13):
        m_start = pd.Timestamp(year, month, 1)
        m_end = month_end(m_start)
        if m_end.normalize() <= end_ts.normalize():
            completed.append(month)
    return completed


def negative_frequency_heatmap(
    price_hourly: pd.DataFrame,
    year: int,
    end_ts: pd.Timestamp,
    *,
    full_calendar_year: bool = False,
):
    """Month x hour zero/negative-price frequency heatmap.

    2025 can be shown as the full calendar year. For 2026 the caller uses only
    completed months, so an MTD month is not mixed with closed-month frequencies.
    """
    if price_hourly.empty:
        return None

    month_numbers = list(range(1, 13)) if full_calendar_year else _completed_month_numbers(year, end_ts)
    if not month_numbers:
        return None

    p = price_hourly.copy()
    p["datetime"] = pd.to_datetime(p["datetime"], errors="coerce")
    p["price"] = pd.to_numeric(p["price"], errors="coerce")
    p = p.dropna(subset=["datetime", "price"])
    p = p[
        (p["datetime"].dt.year == year)
        & (p["datetime"].dt.month.isin(month_numbers))
    ].copy()

    full_grid = pd.MultiIndex.from_product([month_numbers, range(24)], names=["month_num", "hour"]).to_frame(index=False)
    full_grid["month"] = full_grid["month_num"].map(lambda m: calendar.month_abbr[int(m)])

    if p.empty:
        grouped = pd.DataFrame(columns=["month_num", "hour", "pct"])
    else:
        p["month_num"] = p["datetime"].dt.month
        p["hour"] = p["datetime"].dt.hour
        p["flag"] = (p["price"] <= 0).astype(float)
        grouped = p.groupby(["month_num", "hour"], as_index=False)["flag"].mean()
        grouped["pct"] = grouped["flag"] * 100.0
        grouped = grouped[["month_num", "hour", "pct"]]

    plot = full_grid.merge(grouped, on=["month_num", "hour"], how="left")
    plot["month"] = plot["month_num"].map(lambda m: calendar.month_abbr[int(m)])
    plot["label"] = plot["pct"].map(lambda x: f"{x:.0f}%" if pd.notna(x) and x > 0 else "")
    month_order = [calendar.month_abbr[m] for m in month_numbers]

    chart = alt.layer(
        alt.Chart(plot).mark_rect().encode(
            x=alt.X("hour:O", title="Hour", sort=list(range(24))),
            y=alt.Y("month:N", title="Month", sort=month_order),
            color=alt.Color(
                "pct:Q",
                title="% hours",
                scale=alt.Scale(domain=[0, 20, 40, 60, 80], range=["#FEF3C7", "#FDBA74", "#FB7185", "#EF4444", "#991B1B"], clamp=True),
            ),
            tooltip=[
                alt.Tooltip("month:N", title="Month"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("pct:Q", title="Zero/negative frequency", format=",.1f"),
            ],
        ),
        alt.Chart(plot).mark_text(fontSize=9).encode(
            x=alt.X("hour:O", sort=list(range(24))),
            y=alt.Y("month:N", sort=month_order),
            text="label:N",
            color=alt.condition("datum.pct >= 50", alt.value("white"), alt.value(TEXT)),
        ),
    ).properties(title=f"Zero / negative price frequency | {year}")
    return apply_chart_style(chart, height=max(260, 42 * len(month_numbers) + 120))



def _negative_event_monthly_counts(
    hourly: pd.DataFrame,
    *,
    year: int,
    end_ts: pd.Timestamp | None = None,
    model: str | None = None,
    source: str = "Actual",
) -> pd.DataFrame:
    """Monthly hours with negative and zero/negative prices."""
    cols = ["month_num", "Month", "Source", "Negative hours", "Zero / negative hours"]
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=cols)

    p = hourly.copy()
    p["datetime"] = pd.to_datetime(p["datetime"], errors="coerce")
    p["price"] = pd.to_numeric(p["price"], errors="coerce")
    p = p.dropna(subset=["datetime", "price"])
    p = p[p["datetime"].dt.year == year].copy()
    if model is not None and "model" in p.columns:
        p = p[p["model"].astype(str) == str(model)].copy()
    if end_ts is not None:
        p = p[p["datetime"] <= pd.Timestamp(end_ts) + pd.Timedelta(days=1)].copy()
    if p.empty:
        return pd.DataFrame(columns=cols)

    p["month_num"] = p["datetime"].dt.month
    p["negative_flag"] = (p["price"] < 0).astype(int)
    p["zero_negative_flag"] = (p["price"] <= 0).astype(int)
    grouped = (
        p.groupby("month_num", as_index=False)
        .agg(
            **{
                "Negative hours": ("negative_flag", "sum"),
                "Zero / negative hours": ("zero_negative_flag", "sum"),
            }
        )
        .sort_values("month_num")
        .reset_index(drop=True)
    )
    grouped["Month"] = grouped["month_num"].map(lambda m: calendar.month_abbr[int(m)])
    grouped["Source"] = source
    return grouped[cols]


def _negative_event_cumulative_chart_frame(
    hourly: pd.DataFrame,
    *,
    year: int,
    metric: str,
    end_ts: pd.Timestamp | None = None,
    model: str | None = None,
    series_label: str | None = None,
    curve_type: str = "Actual",
) -> pd.DataFrame:
    counts = _negative_event_monthly_counts(
        hourly,
        year=year,
        end_ts=end_ts,
        model=model,
        source=series_label or str(year),
    )
    if counts.empty:
        return pd.DataFrame(columns=["month_num", "Month", "Series", "Curve type", "cum_hours"])
    out = counts[["month_num", "Month", metric]].copy()
    out["Series"] = series_label or str(year)
    out["Curve type"] = curve_type
    out["cum_hours"] = pd.to_numeric(out[metric], errors="coerce").fillna(0).cumsum()
    return out[["month_num", "Month", "Series", "Curve type", "cum_hours"]]


def negative_zero_price_overlay_chart(
    price_hourly: pd.DataFrame,
    forward_scenarios: pd.DataFrame,
    report_end: pd.Timestamp,
    metric_label: str,
):
    """Overlay annual cumulative counts plus 2026 Aurora/Baringa forecasts."""
    metric = "Negative hours" if metric_label == "Only negative prices" else "Zero / negative hours"
    frames: list[pd.DataFrame] = []
    actual_years = sorted(
        [int(y) for y in pd.to_datetime(price_hourly.get("datetime"), errors="coerce").dt.year.dropna().unique().tolist()]
    ) if not price_hourly.empty else []
    actual_years = [y for y in actual_years if 2021 <= y <= 2026]
    for yr in actual_years:
        end = report_end if yr == 2026 else pd.Timestamp(yr, 12, 31)
        frame = _negative_event_cumulative_chart_frame(
            price_hourly,
            year=yr,
            metric=metric,
            end_ts=end,
            series_label=str(yr),
            curve_type="Actual",
        )
        if not frame.empty:
            frames.append(frame)

    if forward_scenarios is not None and not forward_scenarios.empty:
        for model in ["Aurora", "Baringa"]:
            frame = _negative_event_cumulative_chart_frame(
                forward_scenarios,
                year=2026,
                metric=metric,
                end_ts=None,
                model=model,
                series_label=model,
                curve_type="Forecast",
            )
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return None
    plot = pd.concat(frames, ignore_index=True)
    series_order = [str(y) for y in actual_years] + [m for m in ["Aurora", "Baringa"] if m in plot["Series"].astype(str).unique().tolist()]
    actual_palette = {
        "2021": "#2563EB",
        "2022": "#10B981",
        "2023": "#D97706",
        "2024": "#7C3AED",
        "2025": "#DC2626",
        "2026": "#0EA5E9",
    }
    palette = [actual_palette.get(s, GREY) if s not in {"Aurora", "Baringa"} else (AURORA_COLOR if s == "Aurora" else BARINGA_COLOR) for s in series_order]
    title = "Cumulative negative-price hours" if metric == "Negative hours" else "Cumulative zero / negative-price hours"
    chart = alt.Chart(plot).mark_line(point=alt.OverlayMarkDef(filled=True, size=54), strokeWidth=2.8).encode(
        x=alt.X(
            "month_num:O",
            sort=list(range(1, 13)),
            axis=alt.Axis(
                title=None,
                labelAngle=0,
                labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]",
            ),
        ),
        y=alt.Y("cum_hours:Q", title="Cumulative hours"),
        color=alt.Color(
            "Series:N",
            title="Series",
            scale=alt.Scale(domain=series_order, range=palette),
            legend=alt.Legend(symbolStrokeWidth=3),
        ),
        strokeDash=alt.StrokeDash(
            "Curve type:N",
            title="Line style",
            scale=alt.Scale(domain=["Actual", "Forecast"], range=[[1, 0], [7, 3]]),
        ),
        tooltip=[
            alt.Tooltip("Series:N", title="Series"),
            alt.Tooltip("Curve type:N", title="Type"),
            alt.Tooltip("Month:N", title="Month"),
            alt.Tooltip("cum_hours:Q", title="Cumulative hours", format=",.0f"),
        ],
    ).properties(title=title)
    return apply_chart_style(chart, height=360)


def negative_zero_summary_table(
    price_hourly: pd.DataFrame,
    forward_scenarios: pd.DataFrame,
    report_end: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    actual_2025 = _negative_event_monthly_counts(
        price_hourly,
        year=2025,
        end_ts=pd.Timestamp(2025, 12, 31),
        source="2025 actual",
    )
    if not actual_2025.empty:
        rows.append(actual_2025)
        rows.append(pd.DataFrame([{
            "month_num": 99,
            "Month": "TOTAL 2025",
            "Source": "2025 actual",
            "Negative hours": int(actual_2025["Negative hours"].sum()),
            "Zero / negative hours": int(actual_2025["Zero / negative hours"].sum()),
        }]))

    actual_2026 = _negative_event_monthly_counts(
        price_hourly,
        year=2026,
        end_ts=report_end,
        source="2026 actual",
    )
    if not actual_2026.empty:
        actual_2026 = actual_2026.copy()
        if report_end.year == 2026 and report_end.month in actual_2026["month_num"].tolist():
            actual_2026.loc[actual_2026["month_num"] == report_end.month, "Month"] = actual_2026.loc[
                actual_2026["month_num"] == report_end.month, "Month"
            ].astype(str) + " (MTD)"
        rows.append(actual_2026)
        rows.append(pd.DataFrame([{
            "month_num": 199,
            "Month": "YTD 2026",
            "Source": "2026 actual",
            "Negative hours": int(actual_2026["Negative hours"].sum()),
            "Zero / negative hours": int(actual_2026["Zero / negative hours"].sum()),
        }]))

    if forward_scenarios is not None and not forward_scenarios.empty:
        for model in ["Aurora", "Baringa"]:
            f = _negative_event_monthly_counts(
                forward_scenarios,
                year=2026,
                end_ts=report_end,
                model=model,
                source=f"YTD {model}",
            )
            if not f.empty:
                rows.append(pd.DataFrame([{
                    "month_num": 299 if model == "Aurora" else 300,
                    "Month": f"YTD {model}",
                    "Source": model,
                    "Negative hours": int(f["Negative hours"].sum()),
                    "Zero / negative hours": int(f["Zero / negative hours"].sum()),
                }]))

    if not rows:
        return pd.DataFrame(columns=["Month", "Source", "Negative hours", "Zero / negative hours"])
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["month_num", "Source"]).reset_index(drop=True)
    return out[["Month", "Source", "Negative hours", "Zero / negative hours"]]


def style_negative_zero_summary_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def _row_style(row: pd.Series) -> list[str]:
        src = str(row.get("Source", ""))
        month = str(row.get("Month", ""))
        if src == "Aurora":
            return ["background-color: #FFF7ED; font-weight: 800;"] * len(row)
        if src == "Baringa":
            return ["background-color: #EFF6FF; font-weight: 800;"] * len(row)
        if src == "2026 actual":
            base = "background-color: #F0F9FF;"
        elif src == "2025 actual":
            base = "background-color: #F8FAFC;"
        else:
            base = ""
        if "TOTAL" in month or "YTD" in month:
            base += " font-weight: 900; border-top: 2px solid #CBD5E1;"
        return [base] * len(row)

    return (
        df.style
        .format({
            "Negative hours": "{:,.0f}",
            "Zero / negative hours": "{:,.0f}",
        }, na_rep="—")
        .apply(_row_style, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "5px 7px")]},
        ])
    )


def negative_zero_summary_2025_table(
    price_hourly: pd.DataFrame,
) -> pd.DataFrame:
    """2025 monthly actual table plus full-year total."""
    actual = _negative_event_monthly_counts(
        price_hourly,
        year=2025,
        end_ts=pd.Timestamp(2025, 12, 31),
        source="2025 actual",
    )
    month_index = pd.DataFrame({
        "month_num": list(range(1, 13)),
        "Month": [calendar.month_abbr[m] for m in range(1, 13)],
    })
    if actual.empty:
        out = month_index.copy()
        out["Negative hours"] = 0
        out["Zero / negative hours"] = 0
    else:
        out = month_index.merge(
            actual[["month_num", "Negative hours", "Zero / negative hours"]],
            on="month_num",
            how="left",
        )
        out["Negative hours"] = pd.to_numeric(out["Negative hours"], errors="coerce").fillna(0).astype(int)
        out["Zero / negative hours"] = pd.to_numeric(out["Zero / negative hours"], errors="coerce").fillna(0).astype(int)
    total = pd.DataFrame([{
        "month_num": 99,
        "Month": "TOTAL 2025",
        "Negative hours": int(out["Negative hours"].sum()),
        "Zero / negative hours": int(out["Zero / negative hours"].sum()),
    }])
    out = pd.concat([out, total], ignore_index=True)
    return out[["Month", "Negative hours", "Zero / negative hours"]]


def negative_zero_summary_2026_scenario_table(
    price_hourly: pd.DataFrame,
    forward_scenarios: pd.DataFrame,
    report_end: pd.Timestamp,
) -> pd.DataFrame:
    """2026 monthly actual + Aurora + Baringa, with YTD benchmark footer."""
    report_end = pd.Timestamp(report_end)
    latest_month = int(report_end.month) if report_end.year == 2026 else 12
    month_nums = list(range(1, latest_month + 1))
    month_labels = [calendar.month_abbr[m] for m in month_nums]
    if report_end.year == 2026 and latest_month in month_nums:
        month_labels[-1] = f"{month_labels[-1]} (MTD)"

    base = pd.DataFrame({"month_num": month_nums, "Month": month_labels})

    def _metric_block(df: pd.DataFrame, source_name: str, model: str | None = None) -> pd.DataFrame:
        counts = _negative_event_monthly_counts(
            df,
            year=2026,
            end_ts=report_end,
            model=model,
            source=source_name,
        )
        if counts.empty:
            out = base[["month_num"]].copy()
            out["Negative hours"] = 0
            out["Zero / negative hours"] = 0
            return out
        out = base[["month_num"]].merge(
            counts[["month_num", "Negative hours", "Zero / negative hours"]],
            on="month_num",
            how="left",
        )
        out["Negative hours"] = pd.to_numeric(out["Negative hours"], errors="coerce").fillna(0).astype(int)
        out["Zero / negative hours"] = pd.to_numeric(out["Zero / negative hours"], errors="coerce").fillna(0).astype(int)
        return out

    actual = _metric_block(price_hourly, "2026 actual")
    aurora = _metric_block(forward_scenarios, "Aurora", model="Aurora") if forward_scenarios is not None else _metric_block(pd.DataFrame(), "Aurora", model="Aurora")
    baringa = _metric_block(forward_scenarios, "Baringa", model="Baringa") if forward_scenarios is not None else _metric_block(pd.DataFrame(), "Baringa", model="Baringa")

    out = base[["month_num", "Month"]].copy()
    out["Actual | Neg."] = actual["Negative hours"].astype(int)
    out["Actual | Zero/Neg."] = actual["Zero / negative hours"].astype(int)
    out["Aurora | Neg."] = aurora["Negative hours"].astype(int)
    out["Aurora | Zero/Neg."] = aurora["Zero / negative hours"].astype(int)
    out["Baringa | Neg."] = baringa["Negative hours"].astype(int)
    out["Baringa | Zero/Neg."] = baringa["Zero / negative hours"].astype(int)

    total = {
        "month_num": 199,
        "Month": "YTD 2026",
        "Actual | Neg.": int(out["Actual | Neg."].sum()),
        "Actual | Zero/Neg.": int(out["Actual | Zero/Neg."].sum()),
        "Aurora | Neg.": int(out["Aurora | Neg."].sum()),
        "Aurora | Zero/Neg.": int(out["Aurora | Zero/Neg."].sum()),
        "Baringa | Neg.": int(out["Baringa | Neg."].sum()),
        "Baringa | Zero/Neg.": int(out["Baringa | Zero/Neg."].sum()),
    }
    out = pd.concat([out, pd.DataFrame([total])], ignore_index=True)
    return out.drop(columns=["month_num"], errors="ignore")


def style_negative_zero_2025_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def _row_style(row: pd.Series) -> list[str]:
        total = "TOTAL" in str(row.get("Month", ""))
        css = "background-color: #F8FAFC;"
        if total:
            css += " font-weight: 900; border-top: 2px solid #94A3B8; background-color: #E2E8F0;"
        return [css] * len(row)

    return (
        df.style
        .format({
            "Negative hours": "{:,.0f}",
            "Zero / negative hours": "{:,.0f}",
        }, na_rep="—")
        .apply(_row_style, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "5px 7px")]},
        ])
    )


def style_negative_zero_2026_scenario_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    actual_cols = [c for c in df.columns if c.startswith("Actual |")]
    aurora_cols = [c for c in df.columns if c.startswith("Aurora |")]
    baringa_cols = [c for c in df.columns if c.startswith("Baringa |")]

    def _row_style(row: pd.Series) -> list[str]:
        is_total = "YTD" in str(row.get("Month", ""))
        base = "font-weight: 900; border-top: 2px solid #94A3B8;" if is_total else ""
        return [base] * len(row)

    styler = (
        df.style
        .format({c: "{:,.0f}" for c in df.columns if c != "Month"}, na_rep="—")
        .apply(_row_style, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_properties(subset=actual_cols, **{"background-color": "#F0F9FF"})
        .set_properties(subset=aurora_cols, **{"background-color": "#FFF7ED"})
        .set_properties(subset=baringa_cols, **{"background-color": "#EFF6FF"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center"), ("font-size", "0.88rem")]},
            {"selector": "td", "props": [("padding", "5px 6px"), ("font-size", "0.88rem")]},
        ])
    )
    return styler

def ytd_hourly_overlay(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, forward_scenarios: pd.DataFrame, year: int, end_ts: pd.Timestamp):
    p = price_hourly[(price_hourly["datetime"].dt.year == year) & (price_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    s = solar_hourly[(solar_hourly["datetime"].dt.year == year) & (solar_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    if p.empty:
        return None
    p["hour"] = p["datetime"].dt.hour
    p_avg = p.groupby("hour", as_index=False)["price"].mean()
    p_avg["series"] = f"Spot {year} YTD"
    p_avg["value"] = p_avg["price"]
    price_frames = [p_avg[["hour", "series", "value"]]]
    if year == 2026 and not forward_scenarios.empty:
        fs = forward_scenarios[(forward_scenarios["datetime"].dt.year == 2026) & (forward_scenarios["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
        for model in ["Aurora", "Baringa"]:
            m = fs[fs["model"] == model].copy()
            if m.empty:
                continue
            m["hour"] = m["datetime"].dt.hour
            avg = m.groupby("hour", as_index=False)["price"].mean()
            avg["series"] = model
            avg["value"] = avg["price"]
            price_frames.append(avg[["hour", "series", "value"]])
    price_plot = pd.concat(price_frames, ignore_index=True)
    if not s.empty:
        s["hour"] = s["datetime"].dt.hour
        solar_plot = s.groupby("hour", as_index=False)["solar_best_mw"].mean()
    else:
        solar_plot = pd.DataFrame(columns=["hour", "solar_best_mw"])
    price_color_scale = alt.Scale(
        domain=[f"Spot {year} YTD", "Aurora", "Baringa"],
        range=[BLUE, AURORA_COLOR, BARINGA_COLOR],
    )
    dash = alt.Scale(
        domain=[f"Spot {year} YTD", "Aurora", "Baringa"],
        range=[[1, 0], [7, 3], [7, 3]],
    )
    layers = []
    if not solar_plot.empty:
        layers.append(
            alt.Chart(solar_plot).mark_area(opacity=0.33, color=YELLOW).encode(
                x=alt.X("hour:O", title="Hour", sort=list(range(24))),
                y=alt.Y("solar_best_mw:Q", title="Solar generation (MW)"),
                tooltip=[
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("solar_best_mw:Q", title="Avg. solar generation", format=",.0f"),
                ],
            )
        )
    layers.append(
        alt.Chart(price_plot).mark_line(point=alt.OverlayMarkDef(filled=True, size=60), strokeWidth=3).encode(
            x=alt.X("hour:O", title="Hour", sort=list(range(24))),
            y=alt.Y("value:Q", title="Average price (€/MWh)"),
            color=alt.Color("series:N", title="Price series", scale=price_color_scale),
            strokeDash=alt.StrokeDash("series:N", title="Price series", scale=dash),
            detail="series:N",
            tooltip=[
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("value:Q", title="Average price", format=",.2f"),
            ],
        )
    )
    chart = alt.layer(*layers).resolve_scale(y="independent").properties(title=f"YTD 24h profile | {year}")
    return apply_chart_style(chart, height=360)

def monthly_economic_curtailment(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp) -> pd.DataFrame:
    p = price_hourly[(price_hourly["datetime"].dt.year == year) & (price_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    s = solar_hourly[(solar_hourly["datetime"].dt.year == year) & (solar_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    if p.empty or s.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "affected_mwh", "total_mwh", "pct_curtailment"])
    merged = p.merge(s, on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "affected_mwh", "total_mwh", "pct_curtailment"])
    merged["month_num"] = merged["datetime"].dt.month
    merged["month_name"] = merged["datetime"].dt.strftime("%b")
    total = merged.groupby(["month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "total_mwh"})
    affected = merged[merged["price"] <= 0].groupby(["month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "affected_mwh"})
    out = total.merge(affected, on=["month_num", "month_name"], how="left")
    out["affected_mwh"] = out["affected_mwh"].fillna(0.0)
    out["pct_curtailment"] = out["affected_mwh"] / out["total_mwh"].where(out["total_mwh"] != 0)
    out["year"] = year
    return out[["year", "month_num", "month_name", "affected_mwh", "total_mwh", "pct_curtailment"]]

def monthly_economic_curtailment_forward(forward_scenarios: pd.DataFrame, solar_hourly: pd.DataFrame, end_ts: pd.Timestamp) -> pd.DataFrame:
    if forward_scenarios.empty:
        return pd.DataFrame(columns=["model", "month_num", "month_name", "pct_curtailment"])
    frames = []
    for model in ["Aurora", "Baringa"]:
        f = forward_scenarios[(forward_scenarios["model"] == model) & (forward_scenarios["datetime"].dt.year == 2026) & (forward_scenarios["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
        if f.empty:
            continue
        tmp = monthly_economic_curtailment(f[["datetime", "price"]], solar_hourly, 2026, end_ts)
        if tmp.empty:
            continue
        tmp["model"] = model
        frames.append(tmp[["model", "month_num", "month_name", "pct_curtailment"]])
    if not frames:
        return pd.DataFrame(columns=["model", "month_num", "month_name", "pct_curtailment"])
    return pd.concat(frames, ignore_index=True)

def monthly_curtailment_chart(actual: pd.DataFrame, forward: pd.DataFrame, year: int):
    if actual.empty and forward.empty:
        return None
    month_order = [calendar.month_abbr[m] for m in range(1, 13)]
    layers = []
    if not actual.empty:
        layers.append(
            alt.Chart(actual).mark_bar(color=YELLOW_DARK, opacity=0.82).encode(
                x=alt.X("month_name:N", title=None, sort=month_order),
                y=alt.Y(
                    "pct_curtailment:Q",
                    title="Economic curtailment",
                    axis=alt.Axis(format=".0%"),
                    scale=alt.Scale(domain=[0, 1.0]),
                ),
                tooltip=[
                    alt.Tooltip("month_name:N", title="Month"),
                    alt.Tooltip("pct_curtailment:Q", title="Actual", format=".1%"),
                ],
            )
        )
    if not forward.empty and year == 2026:
        layers.append(
            alt.Chart(forward).mark_line(point=alt.OverlayMarkDef(filled=True, size=65), strokeWidth=3.2).encode(
                x=alt.X("month_name:N", title=None, sort=month_order),
                y=alt.Y(
                    "pct_curtailment:Q",
                    title="Economic curtailment",
                    axis=alt.Axis(format=".0%"),
                    scale=alt.Scale(domain=[0, 1.0]),
                ),
                color=alt.Color("model:N", title="2026 forecast", scale=alt.Scale(domain=["Aurora", "Baringa"], range=[AURORA_COLOR, BARINGA_COLOR])),
                strokeDash=alt.StrokeDash("model:N", title="2026 forecast", scale=alt.Scale(domain=["Aurora", "Baringa"], range=[[7, 3], [7, 3]])),
                detail="model:N",
                tooltip=[
                    alt.Tooltip("model:N", title="Model"),
                    alt.Tooltip("month_name:N", title="Month"),
                    alt.Tooltip("pct_curtailment:Q", title="Economic curtailment", format=".1%"),
                ],
            )
        )
    chart = alt.layer(*layers).resolve_scale(color="independent", strokeDash="independent").properties(title=f"Monthly economic curtailment | {year}")
    return apply_chart_style(chart, height=330)


def _clean_col_name(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")

def _first_existing(columns: list[str], candidates: list[str]) -> str | None:
    s = set(columns)
    return next((c for c in candidates if c in s), None)

def _contract_sort(value) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    direct = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(direct):
        return pd.Timestamp(direct).normalize()
    up = s.upper().replace("_", " ").replace("-", " ").replace("/", " ")
    q = re.search(r"Q([1-4]).*?(20\d{2})", up) or re.search(r"(20\d{2}).*?Q([1-4])", up)
    if q:
        a, b = q.groups()
        if a.startswith("20"):
            year, quarter = int(a), int(b)
        else:
            quarter, year = int(a), int(b)
        return pd.Timestamp(year, (quarter - 1) * 3 + 1, 1)
    y = re.search(r"(20\d{2})", up)
    if y:
        return pd.Timestamp(int(y.group(1)), 1, 1)
    return pd.NaT

def normalize_forward_market_df(raw: pd.DataFrame) -> pd.DataFrame:
    cols_out = ["as_of_date", "product", "market_area", "load_type", "contract", "contract_sort", "price", "currency", "source", "curve_family"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols_out)
    df = raw.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    columns = df.columns.tolist()
    date_col = _first_existing(columns, ["as_of_date", "trading_day", "trading_date", "trade_date", "business_date", "date", "settlement_date", "data_date", "timestamp"])
    product_col = _first_existing(columns, ["product", "product_name", "instrument", "instrument_name", "contract_name", "name", "commodity"])
    market_col = _first_existing(columns, ["market_area", "market", "area", "country", "zone", "delivery_area", "hub"])
    load_col = _first_existing(columns, ["load_type", "load", "profile", "base_peak", "baseload_peakload", "contract_type"])
    contract_col = _first_existing(columns, ["contract", "delivery_period", "maturity", "maturity_date", "delivery", "delivery_start", "period", "expiry", "expiration", "contract_month", "contract_year"])
    price_col = _first_existing(columns, ["settlement_price", "settlement", "settle", "settle_price", "final_settlement_price", "last_price", "last", "price", "close", "closing_price", "px_last"])
    currency_col = _first_existing(columns, ["currency", "ccy"])
    if contract_col is None or price_col is None:
        return pd.DataFrame(columns=cols_out)
    out = pd.DataFrame()
    out["as_of_date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT
    out["product"] = df[product_col].astype(str).str.strip() if product_col else "Forward"
    out["market_area"] = df[market_col].astype(str).str.strip() if market_col else ""
    out["load_type"] = df[load_col].astype(str).str.strip() if load_col else ""
    out["contract"] = df[contract_col].astype(str).str.strip()
    out["price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["currency"] = df[currency_col].astype(str).str.strip() if currency_col else "EUR/MWh"
    out["source"] = "EEX local"
    combo = (out["product"].fillna("") + " " + out["load_type"].fillna("") + " " + out["contract"].fillna("")).str.lower()
    out["curve_family"] = np.where(combo.str.contains("solar|pv", na=False), "Solar", "Baseload")
    out.loc[out["load_type"].eq("") & combo.str.contains("base|baseload", na=False), "load_type"] = "Baseload"
    out["contract_sort"] = out["contract"].map(_contract_sort)
    out = out.dropna(subset=["price", "contract_sort"]).copy()
    return out[cols_out].sort_values(["as_of_date", "curve_family", "contract_sort"]).reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_forward_market_history() -> pd.DataFrame:
    if EEX_FORWARD_LOCAL_XLSX.exists():
        try:
            return normalize_forward_market_df(pd.read_excel(EEX_FORWARD_LOCAL_XLSX))
        except Exception:
            pass
    if EEX_FORWARD_LOCAL_CSV.exists():
        try:
            return normalize_forward_market_df(pd.read_csv(EEX_FORWARD_LOCAL_CSV))
        except Exception:
            pass
    return pd.DataFrame(columns=["as_of_date", "product", "market_area", "load_type", "contract", "contract_sort", "price", "currency", "source", "curve_family"])

def classify_contracts(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty:
        return pd.DataFrame(columns=["curve_family", "period", "contract", "price"])
    rows = []
    for family, fam in latest.groupby("curve_family"):
        fam = fam.dropna(subset=["contract_sort"]).copy()
        fam["contract_text"] = fam["contract"].astype(str).str.upper()
        quarters = fam[fam["contract_text"].str.contains(r"\bQ[1-4]\b", regex=True)].sort_values("contract_sort")
        years = fam[~fam["contract_text"].str.contains(r"\bQ[1-4]\b", regex=True)].sort_values("contract_sort")
        for idx, (_, row) in enumerate(quarters.head(2).iterrows(), start=1):
            rows.append({"curve_family": family, "period": f"Q+{idx}", "contract": row["contract"], "price": row["price"], "contract_sort": row["contract_sort"]})
        for idx, (_, row) in enumerate(years.head(2).iterrows(), start=1):
            rows.append({"curve_family": family, "period": f"Y+{idx}", "contract": row["contract"], "price": row["price"], "contract_sort": row["contract_sort"]})
    return pd.DataFrame(rows)

def forward_snapshot_and_monthly_change(history: pd.DataFrame) -> pd.DataFrame:
    cols = ["curve_family", "period", "contract", "latest_price", "m_minus_1_price", "monthly_change_pct", "as_of_date"]
    if history.empty or history["as_of_date"].dropna().empty:
        return pd.DataFrame(columns=cols)
    latest_date = history["as_of_date"].dropna().max()
    latest = history[history["as_of_date"] == latest_date].copy()
    latest_classified = classify_contracts(latest)
    month_ago_date = latest_date - pd.DateOffset(months=1)
    previous_candidates = history[history["as_of_date"] <= month_ago_date].copy()
    if previous_candidates.empty:
        previous_date = history["as_of_date"].dropna().min()
    else:
        previous_date = previous_candidates["as_of_date"].max()
    previous = classify_contracts(history[history["as_of_date"] == previous_date].copy())
    out = latest_classified.merge(
        previous[["curve_family", "period", "price"]].rename(columns={"price": "m_minus_1_price"}),
        on=["curve_family", "period"],
        how="left",
    )
    out = out.rename(columns={"price": "latest_price"})
    out["monthly_change_pct"] = (out["latest_price"] / out["m_minus_1_price"] - 1.0)
    out["as_of_date"] = latest_date
    return out[cols].sort_values(["period", "curve_family"]).reset_index(drop=True)

def style_forward_snapshot_table(df: pd.DataFrame):
    def shade_curve(row: pd.Series):
        curve = str(row.get("Curve", "")).strip().lower()
        if curve == "baseload":
            base = "background-color: #EAF2FF;"
        elif curve == "solar":
            base = "background-color: #FFF4CC;"
        else:
            base = ""
        styles = [base for _ in row.index]
        for i, col in enumerate(row.index):
            if col in {"Latest quote", "Quote one month ago"}:
                if curve == "baseload":
                    styles[i] = base + " font-weight: 700; color: #1D4ED8;"
                elif curve == "solar":
                    styles[i] = base + " font-weight: 700; color: #9A6700;"
        return styles

    return (
        df.style
        .format({
            "Latest quote": "{:,.2f}",
            "Quote one month ago": "{:,.2f}",
            "M-1 change": "{:.1%}",
        }, na_rep="—")
        .apply(shade_curve, axis=1)
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "6px 8px")]},
        ])
    )


def forward_snapshot_chart(snapshot: pd.DataFrame):
    if snapshot.empty:
        return None
    period_order = ["Q+1", "Q+2", "Y+1", "Y+2"]
    chart = alt.Chart(snapshot).mark_bar().encode(
        x=alt.X("period:N", title=None, sort=period_order),
        xOffset=alt.XOffset("curve_family:N"),
        y=alt.Y("latest_price:Q", title="Latest quote (€/MWh)"),
        color=alt.Color("curve_family:N", title="Curve", scale=alt.Scale(domain=["Baseload", "Solar"], range=[BLUE, YELLOW])),
        tooltip=[
            alt.Tooltip("curve_family:N", title="Curve"),
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("latest_price:Q", title="Latest quote", format=",.2f"),
            alt.Tooltip("monthly_change_pct:Q", title="M-1 change", format=".1%"),
        ],
    ).properties(title="Latest forward snapshot | Baseload vs Solar")
    return apply_chart_style(chart, height=330)

def forward_history_line_chart(history: pd.DataFrame, snapshot: pd.DataFrame):
    """Historical forward quote lines for the latest selected Q+1/Q+2/Y+1/Y+2 contracts."""
    if history.empty or snapshot.empty:
        return None
    h = history.copy()
    h["as_of_date"] = pd.to_datetime(h["as_of_date"], errors="coerce")
    h["price"] = pd.to_numeric(h["price"], errors="coerce")
    h = h.dropna(subset=["as_of_date", "price", "curve_family", "contract"])
    selected = snapshot[["curve_family", "period", "contract"]].drop_duplicates().copy()
    plot = h.merge(selected, on=["curve_family", "contract"], how="inner")
    if plot.empty:
        return None
    plot["series"] = plot["curve_family"].astype(str) + " " + plot["period"].astype(str)
    series_order = []
    for period in ["Q+1", "Q+2", "Y+1", "Y+2"]:
        for fam in ["Baseload", "Solar"]:
            series = f"{fam} {period}"
            if series in plot["series"].unique().tolist():
                series_order.append(series)
    if not series_order:
        series_order = sorted(plot["series"].dropna().unique().tolist())
    palette = [
        BLUE, "#3B82F6", BLUE_DARK, "#2563EB",
        YELLOW_DARK, "#F59E0B", "#B45309", "#FBBF24",
    ]
    color_range = [palette[i % len(palette)] for i in range(len(series_order))]
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=2.8).encode(
        x=alt.X("as_of_date:T", title="Quote date", axis=alt.Axis(format="%b-%y", labelAngle=-35)),
        y=alt.Y("price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title="Forward series", scale=alt.Scale(domain=series_order, range=color_range)),
        strokeDash=alt.StrokeDash(
            "curve_family:N",
            title="Curve type",
            scale=alt.Scale(domain=["Baseload", "Solar"], range=[[1, 0], [7, 3]]),
        ),
        detail="series:N",
        tooltip=[
            alt.Tooltip("as_of_date:T", title="Quote date", format="%Y-%m-%d"),
            alt.Tooltip("curve_family:N", title="Curve"),
            alt.Tooltip("period:N", title="Bucket"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("price:Q", title="Price", format=",.2f"),
        ],
    ).properties(title="Forward quote history | Selected Q+1 / Q+2 / Y+1 / Y+2")
    return apply_chart_style(chart, height=380)



# =========================================================
# LIVE OMIP FORWARD SNAPSHOT — aligned with Forward Market page pull
# =========================================================
def _omip_url(asof: date, instrument: str) -> str:
    params = {
        "date": asof.strftime("%Y-%m-%d"),
        "product": "EL",
        "zone": "ES",
        "instrument": instrument,
    }
    return f"{OMIP_BASE_URL}?{urlencode(params)}"


def _omip_normalize_str(value) -> str:
    if pd.isna(value):
        return ""
    s = str(value)
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace("\ufeff", "")
    return re.sub(r"\s+", " ", s).strip()


def _omip_parse_num(value) -> float | None:
    s = _omip_normalize_str(value)
    if not s or s.lower() in {"n.a.", "na", "nan", "none", "-", ""}:
        return None
    s = s.replace("€", "").replace("/MWh", "").replace("MWh", "").replace(" ", "")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _omip_extract_contract(raw_text: str, instrument: str) -> str | None:
    text = _omip_normalize_str(raw_text)
    m = re.search(rf"({re.escape(instrument)}\s+[A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)?)\s*$", text)
    if m:
        return _omip_normalize_str(m.group(1))
    m = re.search(rf"({re.escape(instrument)}\s+.+)$", text)
    if m:
        tail = _omip_normalize_str(m.group(1))
        tail = tail.split(" Transparency")[0].strip()
        return tail[:80]
    return None


def _omip_contract_sort_key(contract: str) -> int:
    c = _omip_normalize_str(contract)
    m = re.search(r"YR-(\d{2})", c)
    if m:
        return (2000 + int(m.group(1))) * 10000
    m = re.search(r"Q([1-4])-(\d{2})", c)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1)) * 1000
    m = re.search(r"M\s+([A-Za-z]{3})-(\d{2})", c)
    if m:
        months = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
        return (2000 + int(m.group(2))) * 10000 + months.get(m.group(1).title(), 99) * 100
    return 99999999


@st.cache_data(show_spinner=False, ttl=1800)
def _fetch_omip_tables_live(asof_iso: str, instrument: str) -> tuple[list[pd.DataFrame], str]:
    asof = pd.Timestamp(asof_iso).date()
    url = _omip_url(asof, instrument)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-GB,en;q=0.9,es;q=0.8",
        "Referer": "https://www.omip.pt/",
    }
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text))
    return tables, url


def _parse_omip_live_contracts(tables: list[pd.DataFrame], asof: date, curve_family: str, instrument: str) -> pd.DataFrame:
    rows: list[dict] = []
    for table_id, table in enumerate(tables, start=1):
        if table is None or table.empty:
            continue
        df = table.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" | ".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns]
        df.columns = [str(c).strip() for c in df.columns]
        for _, row in df.iterrows():
            values = list(row.values)
            texts = [_omip_normalize_str(v) for v in values]
            contract = None
            for txt in texts:
                contract = _omip_extract_contract(txt, instrument)
                if contract:
                    break
            if not contract:
                continue

            def value_at(pos: int):
                return values[pos] if pos < len(values) else None

            best_bid = _omip_parse_num(value_at(3))
            best_ask = _omip_parse_num(value_at(4))
            last_price = _omip_parse_num(value_at(7))
            d_price = _omip_parse_num(value_at(15))
            d_minus_1 = _omip_parse_num(value_at(16))
            price = d_price
            if price is None:
                price = last_price
            if price is None and best_bid is not None and best_ask is not None:
                price = (best_bid + best_ask) / 2.0
            if price is None:
                price = d_minus_1
            if price is None:
                continue
            rows.append(
                {
                    "as_of_date": pd.Timestamp(asof),
                    "curve_family": curve_family,
                    "contract": contract,
                    "contract_sort": _omip_contract_sort_key(contract),
                    "price": price,
                    "source": "OMIP live pull",
                }
            )
    if not rows:
        return pd.DataFrame(columns=["as_of_date", "curve_family", "contract", "contract_sort", "price", "source"])
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["as_of_date", "curve_family", "contract"], keep="last")
    return out.sort_values(["curve_family", "contract_sort", "contract"]).reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=1800)
def load_live_omip_forward_contracts(asof_iso: str) -> pd.DataFrame:
    asof = pd.Timestamp(asof_iso).date()
    parts = []
    for curve_family, instrument in OMIP_LIVE_INSTRUMENTS.items():
        try:
            tables, _ = _fetch_omip_tables_live(asof.isoformat(), instrument)
            parsed = _parse_omip_live_contracts(tables, asof, curve_family, instrument)
            if not parsed.empty:
                parts.append(parsed)
        except Exception:
            continue
    if not parts:
        return pd.DataFrame(columns=["as_of_date", "curve_family", "contract", "contract_sort", "price", "source"])
    return pd.concat(parts, ignore_index=True)


def live_omip_forward_snapshot(asof: date) -> pd.DataFrame:
    cols = ["curve_family", "period", "contract", "latest_price", "m_minus_1_price", "monthly_change_pct", "as_of_date"]
    today_live = load_live_omip_forward_contracts(asof.isoformat())
    if today_live.empty:
        return pd.DataFrame(columns=cols)

    month_ago = asof - timedelta(days=30)
    prior_live = load_live_omip_forward_contracts(month_ago.isoformat())
    rows = []
    for family, fam in today_live.groupby("curve_family"):
        fam = fam.copy()
        fam["text"] = fam["contract"].astype(str).str.upper()
        quarters = fam[fam["text"].str.contains(r"\bQ[1-4]-\d{2}\b", regex=True)].sort_values("contract_sort")
        years = fam[fam["text"].str.contains(r"\bYR-\d{2}\b", regex=True)].sort_values("contract_sort")
        selected = []
        selected += [(f"Q+{idx}", row) for idx, (_, row) in enumerate(quarters.head(2).iterrows(), start=1)]
        selected += [(f"Y+{idx}", row) for idx, (_, row) in enumerate(years.head(2).iterrows(), start=1)]
        for period, row in selected:
            prev_match = prior_live[
                (prior_live["curve_family"] == family)
                & (prior_live["contract"] == row["contract"])
            ]
            prev_price = prev_match["price"].iloc[-1] if not prev_match.empty else np.nan
            rows.append(
                {
                    "curve_family": family,
                    "period": period,
                    "contract": row["contract"],
                    "latest_price": row["price"],
                    "m_minus_1_price": prev_price,
                    "monthly_change_pct": (row["price"] / prev_price - 1.0) if pd.notna(prev_price) and prev_price != 0 else np.nan,
                    "as_of_date": pd.Timestamp(asof),
                }
            )
    return pd.DataFrame(rows, columns=cols).sort_values(["period", "curve_family"]).reset_index(drop=True)

# =========================================================
# BESS SECTION
# =========================================================
def top_bottom_spread_daily(price_hourly: pd.DataFrame, duration_h: int) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame(columns=["date", "spread"])
    p = price_hourly.copy()
    p["date"] = p["datetime"].dt.normalize()
    rows = []
    for d, g in p.groupby("date"):
        prices = pd.to_numeric(g["price"], errors="coerce").dropna().sort_values()
        if len(prices) < duration_h * 2:
            continue
        bottom = prices.head(duration_h).mean()
        top = prices.tail(duration_h).mean()
        rows.append({"date": d, "spread": float(top - bottom)})
    return pd.DataFrame(rows)

def top_bottom_summary(price_hourly: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> dict[str, float | None]:
    p = price_hourly[(price_hourly["datetime"] >= start_ts) & (price_hourly["datetime"] < end_ts + pd.Timedelta(days=1))].copy()
    out = {}
    for h in [1, 2, 4]:
        daily = top_bottom_spread_daily(p, h)
        out[f"TB{h}"] = None if daily.empty else float(daily["spread"].mean())
    return out

def bess_monthly_proxy(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    cols = ["period", "tb1", "tb2", "tb4", "revenue_wo_demand_eur_mw", "revenue_w_demand_1c_eur_mw", "revenue_w_demand_1_4c_eur_mw"]
    if price_hourly.empty:
        return pd.DataFrame(columns=cols)
    p = price_hourly.copy()
    p["period"] = p["datetime"].dt.to_period("M").dt.to_timestamp()
    solar = solar_hourly.copy()
    rows = []
    rte = 0.85
    for period, g in p.groupby("period"):
        s = solar[(solar["datetime"].dt.to_period("M").dt.to_timestamp() == period)].copy()
        tb = top_bottom_summary(g, period, month_end(period))
        g["date"] = g["datetime"].dt.normalize()
        day_revenues_grid = []
        day_revenues_solar = []
        for d, day in g.groupby("date"):
            day_prices = day[["datetime", "price"]].dropna().sort_values("price")
            if len(day_prices) < 8:
                continue
            charge_grid = day_prices.head(4)["price"].sum() / rte
            discharge = day_prices.tail(4)["price"].sum()
            grid_rev = float(discharge - charge_grid)
            day_revenues_grid.append(grid_rev)
            solar_day = s[s["datetime"].dt.normalize() == d].merge(day[["datetime", "price"]], on="datetime", how="inner")
            solar_day = solar_day[solar_day["solar_best_mw"] > 0].sort_values("price")
            if len(solar_day) >= 4:
                charge_pv = solar_day.head(4)["price"].sum() / rte
                solar_rev = float(discharge - charge_pv)
                day_revenues_solar.append(solar_rev)
        days = max(len(day_revenues_grid), 1)
        grid_1c = float(np.nansum(day_revenues_grid))
        solar_wo = float(np.nansum(day_revenues_solar))
        rows.append({
            "period": period,
            "tb1": tb["TB1"],
            "tb2": tb["TB2"],
            "tb4": tb["TB4"],
            "revenue_wo_demand_eur_mw": solar_wo,
            "revenue_w_demand_1c_eur_mw": grid_1c,
            "revenue_w_demand_1_4c_eur_mw": grid_1c * 1.4,
        })
    return pd.DataFrame(rows, columns=cols).sort_values("period").reset_index(drop=True)

def _load_optional_table(paths: list[Path]) -> pd.DataFrame:
    for path in paths:
        if not path.exists():
            continue
        try:
            raw = pd.read_excel(path) if path.suffix.lower() in {".xlsx", ".xls"} else pd.read_csv(path)
            if not raw.empty:
                return raw
        except Exception:
            continue
    return pd.DataFrame()

def normalize_bess_table(raw: pd.DataFrame) -> pd.DataFrame:
    proxy_cols = ["period", "tb1", "tb2", "tb4", "revenue_wo_demand_eur_mw", "revenue_w_demand_1c_eur_mw", "revenue_w_demand_1_4c_eur_mw"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=proxy_cols)
    df = raw.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    period_col = _first_existing(df.columns.tolist(), ["period", "month", "date", "datetime"])
    if period_col is None:
        return pd.DataFrame(columns=proxy_cols)
    out = pd.DataFrame()
    out["period"] = pd.to_datetime(df[period_col], errors="coerce").dt.to_period("M").dt.to_timestamp()
    mapping = {
        "tb1": ["tb1", "top_bottom_1h", "spread_1h"],
        "tb2": ["tb2", "top_bottom_2h", "spread_2h"],
        "tb4": ["tb4", "top_bottom_4h", "spread_4h"],
        "revenue_wo_demand_eur_mw": ["revenue_wo_demand_eur_mw", "wo_demand_eur_mw", "revenue_without_demand"],
        "revenue_w_demand_1c_eur_mw": ["revenue_w_demand_1c_eur_mw", "w_demand_1c_eur_mw", "revenue_with_demand_1c"],
        "revenue_w_demand_1_4c_eur_mw": ["revenue_w_demand_1_4c_eur_mw", "w_demand_1_4c_eur_mw", "revenue_with_demand_1_4c"],
    }
    for target, candidates in mapping.items():
        col = _first_existing(df.columns.tolist(), candidates)
        out[target] = pd.to_numeric(df[col], errors="coerce") if col else np.nan
    return out.dropna(subset=["period"]).sort_values("period").reset_index(drop=True)

def load_or_build_bess_monthly(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    raw = _load_optional_table(BESS_MONTHLY_CANDIDATES)
    normalized = normalize_bess_table(raw)
    if not normalized.empty:
        return normalized, "file"
    return bess_monthly_proxy(price_hourly, solar_hourly), "proxy"

def bess_summary_table(bess: pd.DataFrame, report_month: pd.Timestamp, report_end: pd.Timestamp) -> pd.DataFrame:
    cols = ["Metric", "Selected month", "YTD", "Annualized", "Previous year"]
    if bess.empty:
        return pd.DataFrame(columns=cols)
    b = bess.copy()
    b["year"] = b["period"].dt.year
    b["month"] = b["period"].dt.month
    selected = b[(b["year"] == report_month.year) & (b["month"] == report_month.month)]
    ytd = b[(b["year"] == report_month.year) & (b["period"] <= report_month)]
    prev = b[b["year"] == report_month.year - 1]
    metrics = [
        ("TB4", "tb4", "eur"),
        ("TB2", "tb2", "eur"),
        ("TB1", "tb1", "eur"),
        ("Revenue w/o demand", "revenue_wo_demand_eur_mw", "rev"),
        ("Revenue w. demand 1.0c", "revenue_w_demand_1c_eur_mw", "rev"),
        ("Revenue w. demand 1.4c", "revenue_w_demand_1_4c_eur_mw", "rev"),
    ]
    days_elapsed = max((report_end - pd.Timestamp(report_month.year, 1, 1)).days + 1, 1)
    rows = []
    for label, col, kind in metrics:
        sel_val = selected[col].mean() if not selected.empty else np.nan
        ytd_val = ytd[col].sum() if kind == "rev" else ytd[col].mean()
        annualized = ytd_val * 365 / days_elapsed if kind == "rev" and pd.notna(ytd_val) else ytd_val
        prev_val = prev[col].sum() if kind == "rev" else prev[col].mean()
        rows.append({
            "Metric": label,
            "Selected month": sel_val,
            "YTD": ytd_val,
            "Annualized": annualized,
            "Previous year": prev_val,
        })
    return pd.DataFrame(rows, columns=cols)

def format_bess_summary(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def _fmt(metric: str, value):
        if pd.isna(value):
            return "—"
        if "Revenue" in metric:
            return fmt_mw_revenue(value)
        return fmt_eur(value)

    display = df.copy()
    for col in ["Selected month", "YTD", "Annualized", "Previous year"]:
        display[col] = display.apply(lambda r: _fmt(r["Metric"], r[col]), axis=1)

    def _row_style(row: pd.Series) -> list[str]:
        metric = str(row.get("Metric", ""))
        if metric.startswith("TB"):
            return ["background-color: #EFF6FF; font-weight: 750;"] * len(row)
        if metric.startswith("Revenue"):
            return ["background-color: #ECFDF5; font-weight: 750;"] * len(row)
        return [""] * len(row)

    return (
        display.style
        .apply(_row_style, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "6px 8px")]},
        ])
    )

def _load_hybrid_monthly_file() -> pd.DataFrame:
    """Optional dedicated monthly hybrid capture file."""
    raw = _load_optional_table(HYBRID_MONTHLY_CANDIDATES)
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    period_col = _first_existing(df.columns.tolist(), ["period", "month", "date", "datetime"])
    wo_col = _first_existing(df.columns.tolist(), ["hybrid_wo_demand", "captured_hybrid_wo_demand", "captured_hybrid_without_demand"])
    w_col = _first_existing(df.columns.tolist(), ["hybrid_w_demand", "captured_hybrid_w_demand", "captured_hybrid_with_demand"])
    if period_col is None or wo_col is None or w_col is None:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["period"] = pd.to_datetime(df[period_col], errors="coerce").dt.to_period("M").dt.to_timestamp()
    out["hybrid_wo_demand"] = pd.to_numeric(df[wo_col], errors="coerce")
    out["hybrid_w_demand"] = pd.to_numeric(df[w_col], errors="coerce")
    return out.dropna(subset=["period"]).sort_values("period").reset_index(drop=True)


def _load_bess_report_solar_profile() -> pd.DataFrame:
    if not BESS_REPORT_SOLAR_PROFILE_XLSX.exists():
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    try:
        df = pd.read_excel(BESS_REPORT_SOLAR_PROFILE_XLSX)
    except Exception:
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    if df.empty:
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    norm = {_clean_col_name(c): c for c in df.columns}
    key = _first_existing(list(norm.keys()), ["generation", "generacion", "gen"])
    if key is None:
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    out = pd.DataFrame({"generation": pd.to_numeric(df[norm[key]], errors="coerce").fillna(0.0)})
    out["hour_of_year"] = np.arange(1, len(out) + 1)
    return out[["hour_of_year", "generation"]]


def _load_bess_report_consumption_profile() -> pd.DataFrame:
    if not BESS_REPORT_DATA_XLSX.exists():
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    try:
        df = pd.read_excel(BESS_REPORT_DATA_XLSX)
    except Exception:
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    if df.empty:
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    cols = list(df.columns)
    if len(cols) >= 6:
        df = df.rename(columns={cols[5]: "consumption"})
    if "consumption" not in df.columns:
        norm = {_clean_col_name(c): c for c in df.columns}
        key = _first_existing(list(norm.keys()), ["consumption", "consumo", "load"])
        if key is None:
            return pd.DataFrame(columns=["hour_of_year", "consumption"])
        df = df.rename(columns={norm[key]: "consumption"})
    out = pd.DataFrame({"consumption": pd.to_numeric(df["consumption"], errors="coerce").fillna(0.0)})
    out["hour_of_year"] = np.arange(1, len(out) + 1)
    return out[["hour_of_year", "consumption"]]


def _bess_report_dataset(price_hourly: pd.DataFrame, mode: str) -> pd.DataFrame:
    p = price_hourly.copy()
    p["datetime"] = pd.to_datetime(p["datetime"], errors="coerce")
    p["price"] = pd.to_numeric(p["price"], errors="coerce")
    p = p.dropna(subset=["datetime", "price"])
    p = p[p["datetime"].dt.year >= 2025].sort_values("datetime").copy()
    if p.empty:
        return pd.DataFrame(columns=["timestamp", "dia", "hora", "year", "omie_venta", "omie_compra", "generacion", "consumo"])

    p["year"] = p["datetime"].dt.year
    p["hour_of_year"] = (p["datetime"].dt.dayofyear - 1) * 24 + p["datetime"].dt.hour + 1
    p["timestamp"] = p["datetime"]
    p["dia"] = p["datetime"].dt.date
    p["hora"] = p["datetime"].dt.hour + 1
    p["omie_venta"] = p["price"]

    solar = _load_bess_report_solar_profile()
    if solar.empty:
        return pd.DataFrame(columns=["timestamp", "dia", "hora", "year", "omie_venta", "omie_compra", "generacion", "consumo"])
    p = p.merge(solar, on="hour_of_year", how="left")
    p["generacion"] = pd.to_numeric(p["generation"], errors="coerce").fillna(0.0) * BESS_REPORT_POWER_MW

    if mode == "with_demand":
        consumption = _load_bess_report_consumption_profile()
        if consumption.empty:
            p["consumo"] = 0.0
        else:
            p = p.merge(consumption, on="hour_of_year", how="left")
            p["consumo"] = pd.to_numeric(p["consumption"], errors="coerce").fillna(0.0)
        p["omie_compra"] = p["omie_venta"]
    else:
        p["consumo"] = 0.0
        p["omie_compra"] = 1000.0

    return p[["timestamp", "dia", "hora", "year", "omie_venta", "omie_compra", "generacion", "consumo"]].copy()


def _optimize_bess_report_day(df_day: pd.DataFrame) -> pd.DataFrame:
    """Daily LP based on the BESS-tab logic, fixed to Monthly Report assumptions."""
    df_day = df_day.sort_values("hora").reset_index(drop=True).copy()
    n = len(df_day)
    if n == 0:
        return pd.DataFrame()

    sell = df_day["omie_venta"].astype(float).tolist()
    buy = df_day["omie_compra"].astype(float).tolist()
    generation = df_day["generacion"].astype(float).tolist()
    consumption = df_day["consumo"].astype(float).tolist()

    max_power = BESS_REPORT_POWER_MW
    max_grid_flow = max(max_power, 1e-9)

    model = pulp.LpProblem("monthly_report_bess_daily_optimization", pulp.LpMaximize)
    g_to_grid = pulp.LpVariable.dicts("g_to_grid", range(n), lowBound=0)
    g_to_batt = pulp.LpVariable.dicts("g_to_batt", range(n), lowBound=0)
    g_to_self = pulp.LpVariable.dicts("g_to_self", range(n), lowBound=0)
    grid_charge = pulp.LpVariable.dicts("grid_charge", range(n), lowBound=0)
    batt_for_load = pulp.LpVariable.dicts("batt_for_load", range(n), lowBound=0)
    batt_for_sell = pulp.LpVariable.dicts("batt_for_sell", range(n), lowBound=0)
    grid_purchase = pulp.LpVariable.dicts("grid_purchase", range(n), lowBound=0)
    soc = pulp.LpVariable.dicts("soc", range(n + 1), lowBound=0)
    is_charging = pulp.LpVariable.dicts("is_charging", range(n), cat="Binary")
    is_export = pulp.LpVariable.dicts("is_export", range(n), cat="Binary")

    model += soc[0] == 0.0
    model += soc[n] == 0.0

    for t in range(n):
        model += g_to_batt[t] + grid_charge[t] <= max_power * is_charging[t]
        model += batt_for_load[t] + batt_for_sell[t] <= max_power * (1 - is_charging[t])
        model += g_to_grid[t] + batt_for_sell[t] <= max_grid_flow * is_export[t]
        model += grid_purchase[t] + grid_charge[t] <= max_grid_flow * (1 - is_export[t])
        model += g_to_grid[t] + g_to_batt[t] + g_to_self[t] == generation[t]
        model += consumption[t] - g_to_self[t] == batt_for_load[t] + grid_purchase[t]
        model += soc[t + 1] == (
            soc[t]
            + BESS_REPORT_ETA_CH * (g_to_batt[t] + grid_charge[t])
            - (1 / BESS_REPORT_ETA_DIS) * (batt_for_load[t] + batt_for_sell[t])
        )
        model += soc[t] <= BESS_REPORT_CAPACITY_MWH

    model += soc[n] <= BESS_REPORT_CAPACITY_MWH
    model += pulp.lpSum(g_to_batt[t] + grid_charge[t] for t in range(n)) <= BESS_REPORT_CYCLE_LIMIT * BESS_REPORT_CAPACITY_MWH / max(BESS_REPORT_ETA_CH, 1e-9)
    model += pulp.lpSum(batt_for_load[t] + batt_for_sell[t] for t in range(n)) <= pulp.lpSum(g_to_batt[t] + grid_charge[t] for t in range(n))
    model += pulp.lpSum(
        g_to_grid[t] * sell[t]
        + batt_for_sell[t] * sell[t]
        - grid_purchase[t] * buy[t]
        - grid_charge[t] * buy[t]
        for t in range(n)
    )

    model.solve(pulp.PULP_CBC_CMD(msg=False))

    def vals(var_dict):
        return [pulp.value(var_dict[i]) if pulp.value(var_dict[i]) is not None else 0.0 for i in range(n)]

    res = pd.DataFrame(
        {
            "datetime": pd.to_datetime(df_day["timestamp"]).values,
            "hour": pd.to_numeric(df_day["hora"], errors="coerce").astype(int).values,
            "omie_venta": sell,
            "generacion": generation,
            "consumo": consumption,
            "g_to_grid": vals(g_to_grid),
            "g_to_batt": vals(g_to_batt),
            "g_to_self": vals(g_to_self),
            "grid_charge": vals(grid_charge),
            "batt_for_load": vals(batt_for_load),
            "batt_for_sell": vals(batt_for_sell),
            "grid_purchase": vals(grid_purchase),
            "soc": [pulp.value(soc[i + 1]) if pulp.value(soc[i + 1]) is not None else 0.0 for i in range(n)],
        }
    )
    res["charge_from_pv_mwh"] = res["g_to_batt"]
    res["charge_from_grid_mwh"] = res["grid_charge"]
    res["discharge_to_load_mwh"] = res["batt_for_load"]
    res["discharge_to_market_mwh"] = res["batt_for_sell"]
    res["hybrid_profile_mwh"] = res["g_to_grid"] - res["grid_charge"] + res["batt_for_sell"]
    res["hybrid_revenue_eur"] = res["hybrid_profile_mwh"] * res["omie_venta"]
    return res


@st.cache_data(show_spinner=False, ttl=86400)
def _compute_bess_hybrid_capture_from_model(price_hourly: pd.DataFrame) -> pd.DataFrame:
    """Return monthly captured hybrid prices for w/o-demand and with-demand report scenarios."""
    monthly_parts = []
    for mode, target in [("without_demand", "hybrid_wo_demand"), ("with_demand", "hybrid_w_demand")]:
        data = _bess_report_dataset(price_hourly, mode)
        if data.empty:
            continue
        dispatch_parts = []
        for _, day in data.groupby(pd.to_datetime(data["dia"])):
            result = _optimize_bess_report_day(day)
            if not result.empty:
                dispatch_parts.append(result)
        if not dispatch_parts:
            continue
        dispatch = pd.concat(dispatch_parts, ignore_index=True)
        dispatch["period"] = pd.to_datetime(dispatch["datetime"]).dt.to_period("M").dt.to_timestamp()
        monthly = (
            dispatch.groupby("period", as_index=False)
            .agg(
                hybrid_profile_mwh=("hybrid_profile_mwh", "sum"),
                hybrid_revenue_eur=("hybrid_revenue_eur", "sum"),
            )
        )
        monthly[target] = np.where(
            monthly["hybrid_profile_mwh"] != 0,
            monthly["hybrid_revenue_eur"] / monthly["hybrid_profile_mwh"],
            np.nan,
        )
        monthly_parts.append(monthly[["period", target]])

    if not monthly_parts:
        return pd.DataFrame(columns=["period", "hybrid_wo_demand", "hybrid_w_demand"])
    out = monthly_parts[0]
    for part in monthly_parts[1:]:
        out = out.merge(part, on="period", how="outer")
    return out.sort_values("period").reset_index(drop=True)


def load_or_build_hybrid_capture_monthly(price_hourly: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    dedicated = _load_hybrid_monthly_file()
    if not dedicated.empty:
        return dedicated, "file"
    modeled = _compute_bess_hybrid_capture_from_model(price_hourly)
    if not modeled.empty:
        return modeled, "model"
    return pd.DataFrame(columns=["period", "hybrid_wo_demand", "hybrid_w_demand"]), "unavailable"


def hybrid_actual_monthly(monthly_capture: pd.DataFrame, hybrid_capture: pd.DataFrame) -> pd.DataFrame:
    cols = ["period", "baseload", "hybrid_wo_demand", "hybrid_w_demand"]
    if monthly_capture.empty or hybrid_capture.empty:
        return pd.DataFrame(columns=cols)
    out = monthly_capture[["period", "avg_spot_price"]].copy().rename(columns={"avg_spot_price": "baseload"})
    out = out.merge(hybrid_capture, on="period", how="left")
    return out[cols].dropna(subset=["period"]).sort_values("period").reset_index(drop=True)


def hybrid_chart(hybrid: pd.DataFrame):
    if hybrid.empty:
        return None
    h = hybrid[hybrid["period"] >= pd.Timestamp(2025, 1, 1)].copy()
    if h.empty:
        return None

    h["period"] = pd.to_datetime(h["period"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    h = h.dropna(subset=["period"]).drop_duplicates(subset=["period"], keep="last").sort_values("period")
    h["period_label"] = h["period"].dt.strftime("%b-%y")
    period_order = h["period_label"].tolist()

    long = h.melt(
        id_vars=["period", "period_label"],
        value_vars=["baseload", "hybrid_wo_demand", "hybrid_w_demand"],
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])

    names = {
        "baseload": "Baseload",
        "hybrid_wo_demand": "Hybrid w/o demand",
        "hybrid_w_demand": "Hybrid w. demand",
    }
    long["series"] = long["series"].map(names)

    chart = alt.Chart(long).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X(
            "period_label:N",
            title=None,
            sort=period_order,
            axis=alt.Axis(labelAngle=-35),
        ),
        y=alt.Y("value:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "series:N",
            title="Series",
            scale=alt.Scale(domain=list(names.values()), range=[BLUE, YELLOW_DARK, CORP_GREEN]),
            legend=alt.Legend(orient="top", direction="horizontal", labelLimit=260, titleLimit=260, symbolLimit=260),
        ),
        strokeDash=alt.StrokeDash(
            "series:N",
            title="Series",
            scale=alt.Scale(domain=list(names.values()), range=[[1, 0], [6, 3], [3, 2]]),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("period:T", title="Month", format="%b %Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="Captured price", format=",.2f"),
        ],
    ).properties(title="Monthly baseload vs BESS-model captured hybrid price")
    return apply_chart_style(chart, height=360)


def _hybrid_summary_values(hybrid: pd.DataFrame, year: int) -> dict[str, float | None]:
    """Return summary price levels for the BESS chart cards."""
    if hybrid is None or hybrid.empty:
        return {"baseload": None, "hybrid_wo_demand": None, "hybrid_w_demand": None}
    h = hybrid.copy()
    h["period"] = pd.to_datetime(h["period"], errors="coerce")
    h = h.dropna(subset=["period"])
    h = h[h["period"].dt.year == year].copy()
    if h.empty:
        return {"baseload": None, "hybrid_wo_demand": None, "hybrid_w_demand": None}
    return {
        "baseload": float(pd.to_numeric(h["baseload"], errors="coerce").mean()),
        "hybrid_wo_demand": float(pd.to_numeric(h["hybrid_wo_demand"], errors="coerce").mean()),
        "hybrid_w_demand": float(pd.to_numeric(h["hybrid_w_demand"], errors="coerce").mean()),
    }


def _fmt_hybrid_card_value(value: float | None) -> str:
    return "—" if value is None or pd.isna(value) else f"{value:,.1f} €/MWh"


def render_hybrid_summary_cards(hybrid: pd.DataFrame) -> None:
    fy_2025 = _hybrid_summary_values(hybrid, 2025)
    ytd_2026 = _hybrid_summary_values(hybrid, 2026)

    def card_html(title: str, values: dict[str, float | None]) -> str:
        return f"""
        <div style="
            border:1px solid #D9E7F3;
            border-radius:16px;
            padding:14px 16px;
            background:linear-gradient(180deg, #FFFFFF 0%, #F8FBFF 100%);
            box-shadow:0 4px 14px rgba(15, 23, 42, 0.04);
            min-height:122px;
        ">
            <div style="font-weight:800; font-size:1.02rem; color:#0F172A; margin-bottom:10px;">{title}</div>
            <div style="display:grid; grid-template-columns:repeat(3, 1fr); gap:10px;">
                <div style="border-left:4px solid {BLUE}; padding-left:10px;">
                    <div style="font-size:0.78rem; color:#64748B; font-weight:700;">Baseload</div>
                    <div style="font-size:1.18rem; font-weight:850; color:#0F172A;">{_fmt_hybrid_card_value(values["baseload"])}</div>
                </div>
                <div style="border-left:4px solid {YELLOW_DARK}; padding-left:10px;">
                    <div style="font-size:0.78rem; color:#64748B; font-weight:700;">Hybrid w/o demand</div>
                    <div style="font-size:1.18rem; font-weight:850; color:#0F172A;">{_fmt_hybrid_card_value(values["hybrid_wo_demand"])}</div>
                </div>
                <div style="border-left:4px solid {CORP_GREEN}; padding-left:10px;">
                    <div style="font-size:0.78rem; color:#64748B; font-weight:700;">Hybrid w. demand</div>
                    <div style="font-size:1.18rem; font-weight:850; color:#0F172A;">{_fmt_hybrid_card_value(values["hybrid_w_demand"])}</div>
                </div>
            </div>
        </div>
        """

    left, right = st.columns(2)
    with left:
        st.markdown(card_html("2025 full-year summary", fy_2025), unsafe_allow_html=True)
    with right:
        st.markdown(card_html("2026 YTD summary", ytd_2026), unsafe_allow_html=True)
    st.caption("Summary cards show the average of the monthly values displayed in the chart.")




def build_bess_with_demand_daily_strategy_chart(dispatch: pd.DataFrame):
    if dispatch is None or dispatch.empty:
        return None
    d = dispatch.copy().sort_values("hour")
    d["hour_label"] = d["hour"].map(lambda h: f"{int(h):02d}:00")
    hours = [f"{h:02d}:00" for h in range(1, 25)]

    bars = pd.concat([
        d[["hour_label", "charge_from_pv_mwh"]].rename(columns={"charge_from_pv_mwh": "flow_mwh"}).assign(series="Charge from PV", flow_mwh=lambda x: -x["flow_mwh"]),
        d[["hour_label", "charge_from_grid_mwh"]].rename(columns={"charge_from_grid_mwh": "flow_mwh"}).assign(series="Charge from grid", flow_mwh=lambda x: -x["flow_mwh"]),
        d[["hour_label", "discharge_to_load_mwh"]].rename(columns={"discharge_to_load_mwh": "flow_mwh"}).assign(series="Discharge to demand"),
        d[["hour_label", "discharge_to_market_mwh"]].rename(columns={"discharge_to_market_mwh": "flow_mwh"}).assign(series="Discharge to market"),
    ], ignore_index=True)

    flow_colors = alt.Scale(
        domain=["Charge from PV", "Charge from grid", "Discharge to demand", "Discharge to market"],
        range=[RED, "#FCA5A5", "#0EA5E9", CORP_GREEN],
    )
    bars_chart = alt.Chart(bars).mark_bar(opacity=0.9).encode(
        x=alt.X("hour_label:N", title="Hour", sort=hours, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("flow_mwh:Q", title="Battery flow (MWh): discharge + / charge −"),
        color=alt.Color("series:N", title="BESS flow", scale=flow_colors),
        tooltip=[alt.Tooltip("hour_label:N", title="Hour"), alt.Tooltip("series:N", title="Flow"), alt.Tooltip("flow_mwh:Q", title="MWh", format=",.3f")],
    )

    profile_long = pd.concat([
        d[["hour_label", "generacion"]].rename(columns={"generacion": "value"}).assign(series="Solar generation"),
        d[["hour_label", "consumo"]].rename(columns={"consumo": "value"}).assign(series="Demand"),
    ], ignore_index=True)
    profile_line = alt.Chart(profile_long).mark_line(point=False, strokeWidth=2.2, strokeDash=[5, 3]).encode(
        x=alt.X("hour_label:N", sort=hours, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("value:Q", title="Solar / demand profile (MWh)"),
        color=alt.Color("series:N", title="Profile", scale=alt.Scale(domain=["Solar generation", "Demand"], range=[YELLOW_DARK, GREY])),
        tooltip=[alt.Tooltip("hour_label:N", title="Hour"), alt.Tooltip("series:N", title="Profile"), alt.Tooltip("value:Q", title="MWh", format=",.3f")],
    )

    price_line = alt.Chart(d).mark_line(point=True, strokeWidth=2.6, color=BLUE_DARK).encode(
        x=alt.X("hour_label:N", sort=hours, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("omie_venta:Q", title="OMIE sell price (€/MWh)"),
        tooltip=[alt.Tooltip("hour_label:N", title="Hour"), alt.Tooltip("omie_venta:Q", title="Spot price", format=",.2f")],
    )

    chart = alt.layer(bars_chart, profile_line, price_line).resolve_scale(y="independent", color="independent").properties(
        title="Selected-day BESS with demand strategy | 24h charge / discharge example",
        height=390,
    )
    return apply_chart_style(chart, height=390)


def available_bess_days_for_month(price_hourly: pd.DataFrame, selected_month: pd.Timestamp, report_end: pd.Timestamp) -> list[date]:
    if price_hourly is None or price_hourly.empty:
        return []
    dt = pd.to_datetime(price_hourly["datetime"], errors="coerce")
    mask = (dt.dt.year == selected_month.year) & (dt.dt.month == selected_month.month) & (dt <= report_end + pd.Timedelta(days=1))
    days = sorted(dt.loc[mask].dt.date.dropna().unique().tolist())
    return days


# =========================================================
# PDF EXPORT
# =========================================================
def _chart_to_png_bytes(chart, *, title: str = "") -> bytes | None:
    """Render Altair charts for PDF with a controlled landscape aspect ratio.

    Streamlit can display very wide charts cleanly, but inserting those same specs
    in a landscape-A4 PDF makes the image collapse into a thin strip. For PDF
    export we force balanced pixel dimensions before rasterising.
    """
    if chart is None:
        return None
    try:
        import vl_convert as vlc

        title_l = (title or "").lower()
        if "hourly spot market heatmap" in title_l:
            pdf_width, pdf_height, scale = 1120, 520, 3.2
        elif "zero / negative price frequency heatmap" in title_l:
            pdf_width, pdf_height, scale = 1050, 470, 3.2
        else:
            pdf_width, pdf_height, scale = 1020, 500, 3.0

        chart_pdf = chart.properties(width=pdf_width, height=pdf_height)
        spec_json = chart_pdf.to_json()
        return vlc.vegalite_to_png(spec_json, scale=scale)
    except Exception:
        return None


def _pdf_table_data(df: pd.DataFrame, *, max_rows: int | None = None) -> list[list[str]]:
    if df is None or df.empty:
        return [["No data available"]]
    work = df.copy()
    if max_rows is not None:
        work = work.head(max_rows)
    rows = [list(map(str, work.columns.tolist()))]
    for _, row in work.iterrows():
        vals = []
        for val in row.tolist():
            if pd.isna(val):
                vals.append("—")
            elif isinstance(val, (float, np.floating)):
                vals.append(f"{float(val):,.2f}")
            elif isinstance(val, (pd.Timestamp, datetime, date)):
                vals.append(pd.Timestamp(val).strftime("%d %b %Y"))
            else:
                vals.append(str(val))
        rows.append(vals)
    return rows


def build_pdf_report_bytes(
    *,
    report_label: str,
    report_end: pd.Timestamp,
    selected_metrics: dict,
    prev_metrics: dict,
    yoy_metrics: dict,
    capture_report: pd.DataFrame,
    negative_summary: pd.DataFrame,
    forward_snapshot: pd.DataFrame,
    bess_summary: pd.DataFrame,
    charts: list[tuple[str, object]],
) -> bytes:
    from io import BytesIO
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        Image as RLImage,
        KeepTogether,
        PageBreak,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )

    output = BytesIO()
    doc = SimpleDocTemplate(
        output,
        pagesize=landscape(A4),
        leftMargin=0.75 * cm,
        rightMargin=0.75 * cm,
        topMargin=0.8 * cm,
        bottomMargin=0.9 * cm,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=20,
        leading=24,
        textColor=colors.HexColor(CORP_GREEN_DARK),
        alignment=TA_LEFT,
        spaceAfter=8,
    )
    h_style = ParagraphStyle(
        "SectionHeader",
        parent=styles["Heading2"],
        fontSize=13,
        leading=16,
        textColor=colors.white,
        backColor=colors.HexColor(CORP_GREEN_DARK),
        borderPadding=6,
        spaceBefore=10,
        spaceAfter=7,
    )
    sub_style = ParagraphStyle(
        "SubHeader",
        parent=styles["Heading3"],
        fontSize=11,
        leading=14,
        textColor=colors.HexColor(TEXT),
        spaceBefore=7,
        spaceAfter=5,
    )
    body_style = ParagraphStyle(
        "Body",
        parent=styles["BodyText"],
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor(TEXT),
        spaceAfter=4,
    )
    small_style = ParagraphStyle(
        "Small",
        parent=body_style,
        fontSize=7,
        leading=9,
        textColor=colors.HexColor(GREY),
    )

    logo_path = DATA_DIR / "nexwell-power-.jpg"
    story = []
    if logo_path.exists():
        try:
            logo = RLImage(str(logo_path), width=3.2 * cm, height=1.0 * cm)
            story.append(logo)
            story.append(Spacer(1, 0.12 * cm))
        except Exception:
            pass
    story.append(Paragraph("Monthly Market Report", title_style))
    story.append(Paragraph(f"<b>Report month:</b> {report_label} &nbsp;&nbsp; <b>Data cut-off:</b> {report_end:%d %b %Y}", body_style))
    story.append(Paragraph("NEXWELLPOWER | Corporate market dashboard", small_style))

    # Executive KPI table
    story.append(Paragraph("Day-Ahead KPI panel", h_style))
    kpi_rows = [
        ["Metric", report_label, "Previous month", "Same month LY"],
        ["Baseload", fmt_eur(selected_metrics.get("avg_price")), fmt_eur(prev_metrics.get("avg_price")), fmt_eur(yoy_metrics.get("avg_price"))],
        ["Solar captured unc.", fmt_eur(selected_metrics.get("captured_uncurtailed")), fmt_eur(prev_metrics.get("captured_uncurtailed")), fmt_eur(yoy_metrics.get("captured_uncurtailed"))],
        ["Solar captured curt.", fmt_eur(selected_metrics.get("captured_curtailed")), fmt_eur(prev_metrics.get("captured_curtailed")), fmt_eur(yoy_metrics.get("captured_curtailed"))],
        ["Solar capture rate curtailed", fmt_pct(selected_metrics.get("capture_rate_curtailed")), fmt_pct(prev_metrics.get("capture_rate_curtailed")), fmt_pct(yoy_metrics.get("capture_rate_curtailed"))],
    ]
    kpi_table = Table(kpi_rows, repeatRows=1, colWidths=[5.0 * cm, 4.5 * cm, 4.5 * cm, 4.5 * cm])
    kpi_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(CORP_GREEN_DARK)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#F8FAFC")),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CBD5E1")),
        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(kpi_table)

    # Capture table
    story.append(Paragraph("Monthly baseload vs Solar PV capture table", h_style))
    cap_data = _pdf_table_data(capture_report)
    cap_col_widths = [2.1 * cm] + [2.08 * cm] * max(0, len(cap_data[0]) - 1)
    cap_table = Table(cap_data, repeatRows=1, colWidths=cap_col_widths)
    cap_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#475569")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (1, 1), (5, -1), colors.HexColor("#F1F5F9")),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#ECFDF5")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.18, colors.HexColor("#CBD5E1")),
        ("FONTSIZE", (0, 0), (-1, -1), 5.9),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(cap_table)

    # Negative / zero-price summary
    story.append(Paragraph("Negative and zero-price hours", h_style))
    negative_data = _pdf_table_data(negative_summary)
    negative_table = Table(negative_data, repeatRows=1)
    negative_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#475569")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#F8FAFC")),
        ("GRID", (0, 0), (-1, -1), 0.18, colors.HexColor("#CBD5E1")),
        ("FONTSIZE", (0, 0), (-1, -1), 6.2),
        ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(negative_table)

    # Charts
    for title, chart in charts:
        png = _chart_to_png_bytes(chart, title=title)
        if png is None:
            continue
        story.append(PageBreak())
        story.append(Paragraph(title, h_style))
        img = RLImage(BytesIO(png))
        img._restrictSize(27.0 * cm, 15.8 * cm)
        story.append(img)

    # Forward
    story.append(PageBreak())
    story.append(Paragraph("Forward Market", h_style))
    story.append(Paragraph("Snapshot based on the latest available live / cached curve dataset shown in the web report.", body_style))
    fwd_data = _pdf_table_data(forward_snapshot)
    fwd_table = Table(fwd_data, repeatRows=1)
    fwd_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(CORP_GREEN_DARK)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CBD5E1")),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(fwd_table)

    # BESS
    story.append(Paragraph("BESS", h_style))
    story.append(Paragraph("Optimization window: daily. Hybrid captured-price chart assumptions: maximum 1 cycle/day, 4h BESS (4.0 MWh / 1.0 MW), and separate w/o demand / w. demand model runs.", body_style))
    bess_data = _pdf_table_data(bess_summary)
    bess_table = Table(bess_data, repeatRows=1)
    bess_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#475569")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 1), (-1, 3), colors.HexColor("#EFF6FF")),
        ("BACKGROUND", (0, 4), (-1, -1), colors.HexColor("#ECFDF5")),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CBD5E1")),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(bess_table)

    def _draw_stamp(canvas, doc):
        canvas.saveState()
        width, height = landscape(A4)

        # Clear, visible corporate seal in the upper-right corner.
        badge_x = width - 6.30 * cm
        badge_y = height - 1.18 * cm
        badge_w = 5.55 * cm
        badge_h = 0.64 * cm
        canvas.setFillColor(colors.HexColor("#D1FAE5"))
        canvas.setStrokeColor(colors.HexColor(CORP_GREEN_DARK))
        canvas.setLineWidth(0.7)
        canvas.roundRect(badge_x, badge_y, badge_w, badge_h, 0.14 * cm, fill=1, stroke=1)
        canvas.setFillColor(colors.HexColor(CORP_GREEN_DARK))
        canvas.setFont("Helvetica-Bold", 9.5)
        canvas.drawCentredString(badge_x + badge_w / 2, badge_y + 0.22 * cm, "NEXWELL POWER")

        # Compact footer metadata.
        canvas.setFillColor(colors.HexColor("#64748B"))
        canvas.setFont("Helvetica-Bold", 7)
        canvas.drawRightString(width - 0.75 * cm, 0.42 * cm, "Monthly Market Report")
        canvas.setFont("Helvetica", 7)
        canvas.drawString(0.75 * cm, 0.42 * cm, f"Generated from the dashboard | {datetime.now():%Y-%m-%d %H:%M}")
        canvas.restoreState()

    doc.build(story, onFirstPage=_draw_stamp, onLaterPages=_draw_stamp)
    return output.getvalue()


# =========================================================
# MAIN LOAD
# =========================================================
st.markdown(
    """
    <div class="report-hero">
      <div class="report-hero-title">📊 Monthly Market Report</div>
      <div class="report-hero-subtitle">Corporate dashboard for Day-Ahead, Forward Market and BESS. The current month is always shown as MTD.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

token = os.getenv("ESIOS_API_TOKEN") or os.getenv("ESIOS_TOKEN") or ""
today = date.today()
live_end = today
live_start = LIVE_START_DATE

with st.spinner("Loading market data and refreshing live 2026 figures..."):
    historical_prices = load_historical_prices()
    historical_solar = load_historical_solar()
    live_prices = load_live_prices(token, live_start, live_end) if token and today >= LIVE_START_DATE else pd.DataFrame(columns=["datetime", "price"])
    live_solar = load_live_solar(token, live_start, live_end) if token and today >= LIVE_START_DATE else pd.DataFrame(columns=["datetime", "solar_best_mw"])
    price_hourly = combine_hist_live(historical_prices, live_prices, ["datetime"])
    solar_hourly = combine_hist_live(historical_solar, live_solar, ["datetime"])
    forward_hourly = load_forward_hourly_scenarios()
    forward_history = load_forward_market_history()

if price_hourly.empty:
    st.error("No hourly price data is available. The report cannot be generated.")
    st.stop()

latest_data_ts = price_hourly["datetime"].max()
current_month_start = pd.Timestamp(today.year, today.month, 1)
min_month = max(pd.Timestamp(2025, 1, 1), price_hourly["datetime"].min().to_period("M").to_timestamp())
max_month = current_month_start
month_starts = pd.date_range(min_month, max_month, freq="MS").tolist()
option_labels = [month_label(m, m.year == today.year and m.month == today.month) for m in month_starts]
selected_label = st.selectbox("📅 Month to display", options=option_labels, index=len(option_labels) - 1)
selected_month = month_starts[option_labels.index(selected_label)]
is_current_mtd = selected_month.year == today.year and selected_month.month == today.month
report_end = month_end(selected_month, today if is_current_mtd else None)
report_end = min(report_end, pd.Timestamp(latest_data_ts.date()))
comparison_2025_end = comparable_ytd_end(report_end, 2025)

pills([
    f"Report month: {selected_label}",
    f"Data cut-off: {report_end:%d %b %Y}",
    "Current month = MTD" if is_current_mtd else "Closed month",
])

if is_current_mtd:
    card_note("The selected month is the current month. All same-month values are explicitly MTD; YTD metrics are calculated through the latest available market day.")
if not token and today.year >= 2026:
    st.info("No ESIOS token was found in the environment, so live 2026 refresh may rely only on files already stored in /data.")

monthly_capture = monthly_capture_table(price_hourly, solar_hourly)
mibgas_actuals = load_mibgas_actuals_for_report()
historical_mix_daily = load_historical_generation_mix_daily_for_report()
live_mix_monthly = load_live_2026_mix_monthly_for_report(date(2026, 1, 1), today)

# =========================================================
# SECTION 1 — DAY AHEAD
# =========================================================
section("Day-Ahead Market", "⚡")

subsection("Quick read | selected month KPI panel")
selected_metrics = period_metrics(price_hourly, solar_hourly, selected_month, report_end)
prev_month = previous_month(selected_month)
prev_metrics = period_metrics(price_hourly, solar_hourly, prev_month, month_end(prev_month))
yoy = yoy_month(selected_month)
yoy_metrics = period_metrics(price_hourly, solar_hourly, yoy, month_end(yoy))
mibgas_selected = mibgas_monthly_mean(mibgas_actuals, selected_month, report_end)
mibgas_prev = mibgas_monthly_mean(mibgas_actuals, prev_month, month_end(prev_month))
mibgas_yoy = mibgas_monthly_mean(mibgas_actuals, yoy, month_end(yoy))

q1, q2, q3, q4, q5 = st.columns(5)
with q1:
    st.metric(
        f"Baseload | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(selected_metrics["avg_price"]),
        help="Simple average of hourly day-ahead prices.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_eur(prev_metrics["avg_price"])}</b><br>Same month LY: <b>{fmt_eur(yoy_metrics["avg_price"])}</b></div>',
        unsafe_allow_html=True,
    )
with q2:
    st.metric(
        f"Solar captured unc. | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(selected_metrics["captured_uncurtailed"]),
        help="Generation-weighted solar captured price including zero/negative hours.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_eur(prev_metrics["captured_uncurtailed"])}</b><br>Same month LY: <b>{fmt_eur(yoy_metrics["captured_uncurtailed"])}</b></div>',
        unsafe_allow_html=True,
    )
with q3:
    st.metric(
        f"Solar captured curt. | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(selected_metrics["captured_curtailed"]),
        help="Generation-weighted solar captured price excluding zero/negative price hours.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_eur(prev_metrics["captured_curtailed"])}</b><br>Same month LY: <b>{fmt_eur(yoy_metrics["captured_curtailed"])}</b></div>',
        unsafe_allow_html=True,
    )
with q4:
    st.metric(
        "Solar capture rate | curtailed",
        fmt_pct(selected_metrics["capture_rate_curtailed"]),
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_pct(prev_metrics["capture_rate_curtailed"])}</b><br>Same month LY: <b>{fmt_pct(yoy_metrics["capture_rate_curtailed"])}</b></div>',
        unsafe_allow_html=True,
    )
with q5:
    st.metric(
        f"MIBGAS D+1 | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(mibgas_selected),
        help="Monthly average GDAES D+1 MIBGAS reference price by delivery day, using the MIBGAS files/cache available to the app.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_eur(mibgas_prev)}</b><br>Same month LY: <b>{fmt_eur(mibgas_yoy)}</b></div>',
        unsafe_allow_html=True,
    )

subsection("Monthly baseload vs Solar PV capture table | 2025 history and 2026 YTD")
capture_report = build_report_capture_table(monthly_capture, price_hourly, solar_hourly, forward_hourly, selected_month, report_end)
st.dataframe(format_capture_table(capture_report), use_container_width=True, height=520)

subsection("Hourly spot market heatmap | 2026 available data vs full-year 2025")
spot_heatmap_2026 = ytd_price_heatmap(price_hourly, 2026, pd.Timestamp(latest_data_ts.date()))
if spot_heatmap_2026 is not None:
    st.markdown('<div class="comparison-note">2026 hourly spot map — future / not-yet-available hours remain blank</div>', unsafe_allow_html=True)
    st.altair_chart(spot_heatmap_2026, use_container_width=True)
spot_heatmap_2025 = ytd_price_heatmap(price_hourly, 2025, pd.Timestamp(2025, 12, 31))
if spot_heatmap_2025 is not None:
    st.markdown('<div class="comparison-note">2025 full-year hourly spot map</div>', unsafe_allow_html=True)
    st.altair_chart(spot_heatmap_2025, use_container_width=True)

subsection("Zero / negative price frequency heatmap | 2026 completed months vs full-year 2025")
neg_heatmap_2026 = negative_frequency_heatmap(
    price_hourly,
    2026,
    pd.Timestamp(latest_data_ts.date()),
    full_calendar_year=False,
)
if neg_heatmap_2026 is not None:
    st.markdown('<div class="comparison-note">2026 — closed months only; an open MTD month is intentionally excluded</div>', unsafe_allow_html=True)
    st.altair_chart(neg_heatmap_2026, use_container_width=True)
neg_heatmap_2025 = negative_frequency_heatmap(
    price_hourly,
    2025,
    pd.Timestamp(2025, 12, 31),
    full_calendar_year=True,
)
if neg_heatmap_2025 is not None:
    st.markdown('<div class="comparison-note">2025 — full-year monthly frequency heatmap</div>', unsafe_allow_html=True)
    st.altair_chart(neg_heatmap_2025, use_container_width=True)


subsection("Negative and zero-price hours | annual overlap and 2026 scenario benchmark")
negative_metric_choice = st.radio(
    "Negative-price line metric",
    ["Only negative prices", "Zero and negative prices"],
    horizontal=True,
    index=0,
    key="monthly_report_negative_price_metric",
)
negative_overlay_chart = negative_zero_price_overlay_chart(
    price_hourly,
    forward_hourly,
    pd.Timestamp(latest_data_ts.date()),
    negative_metric_choice,
)
negative_summary = negative_zero_summary_table(
    price_hourly,
    forward_hourly,
    pd.Timestamp(latest_data_ts.date()),
)
negative_summary_2025 = negative_zero_summary_2025_table(price_hourly)
negative_summary_2026 = negative_zero_summary_2026_scenario_table(
    price_hourly,
    forward_hourly,
    pd.Timestamp(latest_data_ts.date()),
)

if negative_overlay_chart is not None:
    st.altair_chart(negative_overlay_chart, use_container_width=True)

neg_2025_col, neg_2026_col = st.columns([1.0, 1.65])
with neg_2025_col:
    st.markdown('<div class="comparison-note">2025 actual | monthly counts and full-year total</div>', unsafe_allow_html=True)
    if not negative_summary_2025.empty:
        st.dataframe(style_negative_zero_2025_table(negative_summary_2025), use_container_width=True, height=520)
    else:
        st.info("No 2025 negative-price summary could be calculated.")
with neg_2026_col:
    st.markdown('<div class="comparison-note">2026 actual + Aurora + Baringa | monthly counts and YTD benchmark</div>', unsafe_allow_html=True)
    if not negative_summary_2026.empty:
        st.dataframe(style_negative_zero_2026_scenario_table(negative_summary_2026), use_container_width=True, height=520)
    else:
        st.info("No 2026 scenario negative-price summary could be calculated.")

subsection("YTD 24h average market profile vs solar generation")
hourly_overlay = ytd_hourly_overlay(price_hourly, solar_hourly, forward_hourly, selected_month.year, report_end)
if hourly_overlay is not None:
    st.altair_chart(hourly_overlay, use_container_width=True)

subsection("Monthly economic curtailment | 2026 actual vs Aurora / Baringa and 2025 full-year")
actual_curt_2026 = monthly_economic_curtailment(price_hourly, solar_hourly, 2026, pd.Timestamp(latest_data_ts.date()))
forward_curt_2026 = monthly_economic_curtailment_forward(forward_hourly, solar_hourly, pd.Timestamp(latest_data_ts.date()))
curt_chart_2026 = monthly_curtailment_chart(actual_curt_2026, forward_curt_2026, 2026)
if curt_chart_2026 is not None:
    st.altair_chart(curt_chart_2026, use_container_width=True)
actual_curt_2025 = monthly_economic_curtailment(price_hourly, solar_hourly, 2025, pd.Timestamp(2025, 12, 31))
curt_chart_2025 = monthly_curtailment_chart(actual_curt_2025, pd.DataFrame(), 2025)
if curt_chart_2025 is not None:
    st.markdown('<div class="comparison-note">2025 full-year actual economic curtailment</div>', unsafe_allow_html=True)
    st.altair_chart(curt_chart_2025, use_container_width=True)

subsection("Monthly generation injection and renewable share | selected month vs previous month")
selected_generation_metrics = build_generation_month_metrics(historical_mix_daily, live_mix_monthly, selected_month)
previous_generation_metrics = build_generation_month_metrics(historical_mix_daily, live_mix_monthly, prev_month)
generation_comparison = build_generation_month_comparison_table(
    selected_generation_metrics,
    previous_generation_metrics,
    month_label(selected_month, is_current_mtd),
    month_label(prev_month, False),
)
st.dataframe(style_generation_comparison_table(generation_comparison), use_container_width=True, hide_index=True)
st.caption("Solar = Solar PV + Solar thermal. Renewable share is calculated over the total generation mix available for the month.")


# =========================================================
# SECTION 2 — FORWARD MARKET
# =========================================================
section("Forward Market", "📈")

forward_snapshot_live = live_omip_forward_snapshot(today)
forward_snapshot_local = forward_snapshot_and_monthly_change(forward_history)
forward_snapshot = forward_snapshot_live if not forward_snapshot_live.empty else forward_snapshot_local
forward_source_label = "OMIP live pull for access date" if not forward_snapshot_live.empty else "Local normalized forward-history fallback"

if forward_snapshot.empty:
    st.info("No forward snapshot could be populated. The page now attempts the same OMIP live pull used by the Forward Market tab for today's access date, and falls back to a normalized local forward-history file when available.")
    forward_chart = None
else:
    as_of = pd.to_datetime(forward_snapshot["as_of_date"].iloc[0]).date()
    pills([f"Forward quote date: {as_of:%d %b %Y}", forward_source_label, "Q+1 / Q+2", "Y+1 / Y+2", "Baseload + Solar"])
    forward_history_chart = forward_history_line_chart(forward_history, forward_snapshot)
    forward_chart = forward_history_chart if forward_history_chart is not None else forward_snapshot_chart(forward_snapshot)
    if forward_history_chart is not None:
        st.caption("Historical quote lines use the normalized forward-history dataset where available; the table below keeps the latest quote and M-1 variation.")
    if forward_chart is not None:
        st.altair_chart(forward_chart, use_container_width=True)
    forward_display = forward_snapshot.copy()
    forward_display["M-1 change"] = forward_display["monthly_change_pct"]
    forward_display = forward_display[["curve_family", "period", "contract", "latest_price", "m_minus_1_price", "M-1 change"]].rename(columns={
        "curve_family": "Curve",
        "period": "Bucket",
        "contract": "Contract",
        "latest_price": "Latest quote",
        "m_minus_1_price": "Quote one month ago",
    })
    st.dataframe(
        style_forward_snapshot_table(forward_display),
        use_container_width=True,
    )

# =========================================================
# SECTION 3 — BESS
# =========================================================
section("BESS", "🔋")

bess_monthly, bess_source = load_or_build_bess_monthly(price_hourly, solar_hourly)
# Keep the BESS section clean in the report view.
# Source/methodology details are already reflected in the tables, chart caption and PDF content.

subsection("Top-Bottom spreads and BESS revenue overview")
bess_summary = bess_summary_table(bess_monthly, selected_month, report_end)
if bess_summary.empty:
    st.info("No BESS metrics are available.")
else:
    st.dataframe(format_bess_summary(bess_summary), use_container_width=True)

subsection("Monthly BESS-model captured hybrid price vs monthly baseload")
hybrid_capture_monthly, hybrid_source = load_or_build_hybrid_capture_monthly(price_hourly)
hybrid = hybrid_actual_monthly(monthly_capture, hybrid_capture_monthly)
if not hybrid.empty:
    render_hybrid_summary_cards(hybrid)
hybrid_plot = hybrid_chart(hybrid)
if hybrid_plot is not None:
    st.altair_chart(hybrid_plot, use_container_width=True)
    source_note = "dedicated monthly hybrid capture input file" if hybrid_source == "file" else "daily BESS optimisation model embedded in this report"
    st.caption(
        f"Hybrid captured price source: {source_note}. Assumptions: daily optimization window, maximum 1 cycle/day, "
        f"4h BESS (4.0 MWh / 1.0 MW), ηch={BESS_REPORT_ETA_CH:.0%}, ηdis={BESS_REPORT_ETA_DIS:.0%}. "
        "The w/o-demand and w.-demand series are calculated independently with the BESS captured-price logic."
    )
else:
    st.info("Hybrid captured-price series could not be generated. Add a dedicated hybrid monthly file in /data or ensure the default BESS profile files are available.")

subsection("Selected-day BESS with demand strategy | 24h charge / discharge example")
available_bess_days = available_bess_days_for_month(price_hourly, selected_month, report_end)
if not available_bess_days:
    st.info("No hourly market days are available for the selected report month.")
else:
    selected_bess_day = st.selectbox(
        "Choose a day within the selected report month",
        options=available_bess_days,
        index=len(available_bess_days) - 1,
        format_func=lambda d: pd.Timestamp(d).strftime("%d %b %Y"),
        key="monthly_report_bess_selected_day",
    )
    bess_with_demand_data = _bess_report_dataset(price_hourly, "with_demand")
    day_df = bess_with_demand_data[pd.to_datetime(bess_with_demand_data["dia"]).dt.date == selected_bess_day].copy() if not bess_with_demand_data.empty else pd.DataFrame()
    if day_df.empty:
        st.info("The BESS with-demand strategy could not be built for the selected day.")
    else:
        bess_day_dispatch = _optimize_bess_report_day(day_df)
        bess_day_chart = build_bess_with_demand_daily_strategy_chart(bess_day_dispatch)
        if bess_day_chart is not None:
            st.altair_chart(bess_day_chart, use_container_width=True)
        st.caption("Daily BESS strategy example: with-demand model, daily optimization window, max 1 cycle/day, 4h BESS (4.0 MWh / 1.0 MW).")

st.caption("BESS footnote: optimization window is daily; monthly hybrid captured-price comparison uses max 1 cycle/day.")
st.caption("Monthly Market Report | Corporate dashboard based on the app's hourly market data, forward curve files and BESS monthly inputs/proxies.")

# =========================================================
# PDF DOWNLOAD
# =========================================================
section("Download full report", "📄")
pdf_spot_heatmaps = []
for _year, _end in [(2026, pd.Timestamp(latest_data_ts.date())), (2025, pd.Timestamp(2025, 12, 31))]:
    for _m0, _m1 in [(1, 3), (4, 6), (7, 9), (10, 12)]:
        _seg = pdf_hourly_spot_heatmap_segment(price_hourly, _year, _m0, _m1, _end)
        if _seg is not None:
            pdf_spot_heatmaps.append((f"{_year} hourly spot market heatmap | {calendar.month_abbr[_m0]}-{calendar.month_abbr[_m1]}", _seg))

pdf_charts = pdf_spot_heatmaps + [
    ("2026 zero / negative price frequency heatmap", neg_heatmap_2026),
    ("2025 zero / negative price frequency heatmap", neg_heatmap_2025),
    ("Negative / zero-price cumulative hours | scenario overlay", negative_overlay_chart),
    ("YTD 24h average market profile vs solar generation", hourly_overlay),
    ("2026 monthly economic curtailment", curt_chart_2026),
    ("2025 full-year economic curtailment", curt_chart_2025),
    ("Forward market history / latest snapshot", forward_chart),
    ("BESS-model hybrid captured price", hybrid_plot),
]
pdf_bytes = build_pdf_report_bytes(
    report_label=selected_label,
    report_end=report_end,
    selected_metrics=selected_metrics,
    prev_metrics=prev_metrics,
    yoy_metrics=yoy_metrics,
    capture_report=capture_report,
    negative_summary=negative_summary,
    forward_snapshot=forward_snapshot,
    bess_summary=bess_summary,
    charts=pdf_charts,
)
st.download_button(
    "⬇️ Download Monthly Market Report PDF",
    data=pdf_bytes,
    file_name=f"nexwellpower_monthly_market_report_{selected_month:%Y_%m}.pdf",
    mime="application/pdf",
    use_container_width=True,
)
