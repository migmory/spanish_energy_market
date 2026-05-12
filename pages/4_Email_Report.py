from __future__ import annotations

import os
import base64
import re
from datetime import date, datetime, time, timedelta
from io import BytesIO, StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt

# Altair default limit is 5,000 rows. Full-year hourly heatmaps need 8,760/8,784 rows.
try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import pandas as pd
import pulp
import requests
import streamlit as st
from dotenv import load_dotenv

try:
    from scipy.optimize import linprog
    SCIPY_LINPROG_AVAILABLE = True
except Exception:
    linprog = None
    SCIPY_LINPROG_AVAILABLE = False

try:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image as RLImage, PageBreak, KeepTogether
    )
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

HISTORICAL_DIR = BASE_DIR / "historical_data"
HISTORICAL_DIR.mkdir(exist_ok=True)
DATA_DIR = BASE_DIR / "data"
NEXWELL_LOGO_PATH = DATA_DIR / "nexwell-power.jpg"
BESS_DEFAULT_DATA_PATH = DATA_DIR / "data.xlsx"
BESS_DEFAULT_SOLAR_PROFILE_PATH = DATA_DIR / "profile_production_1y_hourly.xlsx"

PRICE_RAW_CSV_PATH = HISTORICAL_DIR / "day_ahead_spain_spot_600_raw.csv"
SOLAR_P48_RAW_CSV_PATH = HISTORICAL_DIR / "solar_p48_spain_84_raw.csv"
SOLAR_FORECAST_RAW_CSV_PATH = HISTORICAL_DIR / "solar_forecast_spain_542_raw.csv"
DEMAND_RAW_CSV_PATH = HISTORICAL_DIR / "demand_p48_total_10027_raw.csv"
REE_MIX_DAILY_CSV_PATH = HISTORICAL_DIR / "ree_generation_structure_daily_peninsular.csv"

HIST_PRICES_XLSX_PATH = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_WORKBOOK_XLSX_PATH = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_SOLAR_CSV_PATH = DATA_DIR / "p48solar_since21.csv"
HIST_MIX_XLSX_PATH = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"

MADRID_TZ = ZoneInfo("Europe/Madrid")

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027

ENERGY_MIX_INDICATORS_OFFICIAL = {
    "Nuclear": 74,
    "CCGT": 79,
    "Wind": 10010,
    "Solar PV": 84,
    "Solar thermal": 85,
    "Hydro UGH": 71,
    "Hydro non-UGH": 72,
    "Pumped hydro": 73,
    "CHP": 10011,
    "Biomass": 91,
    "Biogas": 92,
    "Other renewables": 10013,
}

ENERGY_MIX_INDICATORS_FORECAST = {
    "Nuclear": None,
    "CCGT": None,
    "Wind": None,
    "Solar PV": 542,
    "Solar thermal": 543,
    "Hydro UGH": None,
    "Hydro non-UGH": None,
    "Pumped hydro": None,
    "CHP": None,
    "Biomass": None,
    "Biogas": None,
    "Other renewables": None,
}

DAILY_REE_TECH_MAP = {
    "Ciclo combinado": "CCGT",
    "Hidráulica": "Hydro",
    "Nuclear": "Nuclear",
    "Solar fotovoltaica": "Solar PV",
    "Solar térmica": "Solar thermal",
    "Eólica": "Wind",
    "Cogeneración": "CHP",
    "Biomasa": "Biomass",
    "Biogás": "Biogas",
    "Otras renovables": "Other renewables",
    "Turbinación bombeo": "Hydro",
    "Hidroeólica": "Other renewables",
    "Residuos renovables": "Other renewables",
    "Residuos no renovables": "Other renewables",
}

DISPLAY_TECH_MAP = {
    "Hydro UGH": "Hydro",
    "Hydro non-UGH": "Hydro",
    "Pumped hydro": "Hydro",
}

TECH_ORDER = [
    "CCGT",
    "Hydro",
    "Nuclear",
    "Solar PV",
    "Solar thermal",
    "Wind",
    "CHP",
    "Biomass",
    "Biogas",
    "Other renewables",
]

TECH_COLORS = {
    "CCGT": "#9CA3AF",
    "Hydro": "#60A5FA",
    "Nuclear": "#C084FC",
    "Solar PV": "#FACC15",
    "Solar thermal": "#FCA5A5",
    "Wind": "#2563EB",
    "CHP": "#F97316",
    "Biomass": "#16A34A",
    "Biogas": "#22C55E",
    "Other renewables": "#14B8A6",
    "Demand": "#111827",
}

TECH_COLOR_SCALE = alt.Scale(
    domain=TECH_ORDER,
    range=[TECH_COLORS[t] for t in TECH_ORDER],
)

PRICE_LOW_GREEN = "#16A34A"
PRICE_MID_YELLOW = "#FDE047"
PRICE_MID_ORANGE = "#F97316"
PRICE_HIGH_RED = "#DC2626"
MISSING_GREY = "#F3F4F6"



REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {"geo_trunc": "electric_system", "geo_limit": "peninsular", "geo_ids": "8741"}
LIVE_MIX_START_DATE = date(2026, 1, 1)

LOCAL_REE_MIX_TECH_MAP = {
    "Hidráulica": "Hydro",
    "Hidroeólica": "Other renewables",
    "Turbinación bombeo": "Hydro",
    "Nuclear": "Nuclear",
    "Carbón": "Other non-renewables",
    "Fuel + Gas": "Other non-renewables",
    "Turbina de vapor": "Other non-renewables",
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
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Hydro",
    "Biomass",
    "Biogas",
    "Other renewables",
}

st.set_page_config(page_title="Email Report", layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-size: 100% !important;
    }
    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 100% !important;
    }
    h1 {
        font-size: 1.9rem !important;
    }
    h2, h3 {
        font-size: 1.15rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Email Report")

if "email_admin_password" not in st.secrets:
    st.error("Missing secret: email_admin_password")
    st.stop()

pwd = st.text_input("Password", type="password")
if pwd != st.secrets["email_admin_password"]:
    st.stop()

report_granularity = st.selectbox("Report granularity", ["Daily", "Weekly", "Monthly", "Quarterly", "Annual"], index=1)
negative_price_mode = st.selectbox("Negative-price metric", ["Zero and negative prices", "Only negative prices"], index=0)


# =========================================================
# TIME / TOKEN
# =========================================================
def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def today_madrid() -> date:
    return now_madrid().date()


def allow_next_day_refresh() -> bool:
    return now_madrid().time() >= time(15, 0)


def max_refresh_day_from_clock() -> date:
    return today_madrid() + timedelta(days=1) if allow_next_day_refresh() else today_madrid()


def require_esios_token() -> str | None:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    return token or None


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def resolve_time_trunc(day: date) -> str:
    return "hour" if day < date(2025, 10, 1) else "quarter_hour"


# =========================================================
# HELPERS
# =========================================================
def ensure_datetime_col(df: pd.DataFrame, col: str = "datetime") -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = pd.to_datetime(out[col], errors="coerce")
        out = out.dropna(subset=[col]).copy()
    return out


def parse_emails(raw: str) -> list[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def format_preview_df(df: pd.DataFrame, pct_cols: list[str] | None = None) -> pd.DataFrame:
    pct_cols = pct_cols or []
    out = df.copy()

    for col in out.columns:
        if col in pct_cols:
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:.2%}")
        elif pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:,.2f}")

    return out


def df_to_html_table(df: pd.DataFrame, pct_cols: list[str] | None = None, small: bool = False) -> str:
    pct_cols = pct_cols or []
    tmp = format_preview_df(df, pct_cols=pct_cols)

    font_size = "12px" if small else "13px"
    styles = f"""
    <style>
    table.email-table {{
        border-collapse: collapse;
        width: 100%;
        font-family: Arial, sans-serif;
        font-size: {font_size};
    }}
    table.email-table th {{
        background: #d1d5db;
        color: #111111;
        border: 1px solid #c7ccd4;
        padding: 8px;
        text-align: center;
        font-weight: 700;
    }}
    table.email-table td {{
        border: 1px solid #e5e7eb;
        padding: 8px;
        text-align: center;
    }}
    </style>
    """
    html = tmp.to_html(index=False, classes="email-table", border=0, escape=False)
    return styles + html


def make_kpi_cards_html(items: list[tuple[str, str]]) -> str:
    cards = []
    for title, value in items:
        cards.append(
            f"""
            <div style="
                flex: 1 1 180px;
                min-width: 180px;
                border: 1px solid #e5e7eb;
                border-radius: 10px;
                padding: 10px 12px;
                background: #f8fafc;">
                <div style="font-size:12px; color:#475569; margin-bottom:4px;">{title}</div>
                <div style="font-size:18px; font-weight:700; color:#111827;">{value}</div>
            </div>
            """
        )
    return f'<div style="display:flex; gap:10px; flex-wrap:wrap;">{"".join(cards)}</div>'


# =========================================================
# LOAD / PARSE LOCAL DATA
# =========================================================
def load_raw_history(csv_path: Path, source_name: str | None = None) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame()

    if csv_path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(csv_path)
    else:
        df = pd.read_csv(csv_path)

    if df.empty:
        return pd.DataFrame()

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if source_name is not None:
        if "source" not in df.columns:
            df["source"] = source_name
        if "geo_name" not in df.columns:
            df["geo_name"] = None
        if "geo_id" not in df.columns:
            df["geo_id"] = None

    return df



def load_price_history_fallback() -> pd.DataFrame:
    if PRICE_RAW_CSV_PATH.exists():
        return load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")

    if not HIST_PRICES_XLSX_PATH.exists():
        return pd.DataFrame()

    try:
        df = pd.read_excel(HIST_PRICES_XLSX_PATH, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_XLSX_PATH)

    if df.empty:
        return pd.DataFrame()

    if "price" in df.columns and "value" not in df.columns:
        df = df.rename(columns={"price": "value"})
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "historical_prices_xlsx"
    df["geo_name"] = None
    df["geo_id"] = None
    return df

def load_solar_p48_history_fallback() -> pd.DataFrame:
    """
    Historical solar loader aligned with the Day Ahead page.

    Priority:
    1) Full workbook export /data/hourly_avg_price_since2021.xlsx, sheet solar_hourly_best
    2) /data/p48solar_since21.csv
    3) historical_data/solar_p48_spain_84_raw.csv

    This avoids incomplete raw slices hiding 2025 months in the Email Report
    economic-curtailment charts.
    """
    if HIST_WORKBOOK_XLSX_PATH.exists():
        try:
            df = pd.read_excel(HIST_WORKBOOK_XLSX_PATH, sheet_name="solar_hourly_best")
            if not df.empty:
                if "solar_best_mw" in df.columns and "value" not in df.columns:
                    df = df.rename(columns={"solar_best_mw": "value"})
                if "datetime" in df.columns:
                    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                if "value" in df.columns:
                    df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df = df.dropna(subset=["datetime", "value"]).copy()
                df["source"] = "historical_workbook_solar_hourly_best"
                df["geo_name"] = None
                df["geo_id"] = None
                return df
        except Exception:
            pass

    if HIST_SOLAR_CSV_PATH.exists():
        try:
            df = pd.read_csv(HIST_SOLAR_CSV_PATH)
            if not df.empty:
                if "solar_best_mw" in df.columns and "value" not in df.columns:
                    df = df.rename(columns={"solar_best_mw": "value"})
                if "datetime" in df.columns:
                    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                if "value" in df.columns:
                    df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df = df.dropna(subset=["datetime", "value"]).copy()
                df["source"] = "historical_solar_csv"
                df["geo_name"] = None
                df["geo_id"] = None
                return df
        except Exception:
            pass

    if SOLAR_P48_RAW_CSV_PATH.exists():
        return load_raw_history(SOLAR_P48_RAW_CSV_PATH, "esios_84")

    return pd.DataFrame()


def infer_interval_hours(df: pd.DataFrame) -> pd.Series:
    if df.empty or "datetime" not in df.columns:
        return pd.Series(dtype=float)

    out = ensure_datetime_col(df, "datetime")
    if out.empty:
        return pd.Series(dtype=float)

    diffs = out["datetime"].diff().dt.total_seconds().div(3600)
    if diffs.dropna().empty:
        interval = 1.0
    else:
        median_diff = diffs.dropna().median()
        interval = 0.25 if median_diff <= 0.30 else 1.0

    return pd.Series(interval, index=out.index)


def to_hourly_mean(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])

    out = ensure_datetime_col(df, "datetime")
    if out.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])

    out["datetime_hour"] = out["datetime"].dt.floor("h")

    out = (
        out.groupby("datetime_hour", as_index=False)
        .agg(
            value=("value", "mean"),
            source=("source", "first"),
            geo_name=("geo_name", "first"),
            geo_id=("geo_id", "first"),
        )
        .rename(columns={"datetime_hour": "datetime", "value": value_col_name})
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    return out


def to_energy_intervals(df: pd.DataFrame, value_col_name: str, energy_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"])

    out = ensure_datetime_col(df, "datetime")
    if out.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"])

    out[value_col_name] = pd.to_numeric(out["value"], errors="coerce")
    out["interval_h"] = infer_interval_hours(out)
    out[energy_col_name] = out[value_col_name] * out["interval_h"]

    out = out[["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def to_hourly_energy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = ensure_datetime_col(df, "datetime")
    if out.empty:
        return out

    out["datetime_hour"] = out["datetime"].dt.floor("h")

    agg_dict = {"energy_mwh": "sum"}
    if "mw" in out.columns:
        agg_dict["mw"] = "mean"
    for c in ["source", "geo_name", "geo_id", "technology", "data_source"]:
        if c in out.columns:
            agg_dict[c] = "first"

    out = (
        out.groupby("datetime_hour", as_index=False)
        .agg(agg_dict)
        .rename(columns={"datetime_hour": "datetime"})
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    return out


# =========================================================
# ESIOS FETCH
# =========================================================
def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    raise ValueError("No datetime column found")


def parse_esios_indicator(
    raw_json: dict,
    source_name: str,
    filter_date: date | None = None,
) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df = pd.DataFrame(values)

    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    else:
        geo_series = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo_series == "españa").any():
            df = df[geo_series == "españa"].copy()
        elif (geo_series == "espana").any():
            df = df[geo_series == "espana"].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df["datetime"] = parse_datetime_label(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()

    if filter_date is not None:
        df = df[df["datetime"].dt.date == filter_date].copy()

    df["source"] = source_name
    df = df[["datetime", "value", "source", "geo_name", "geo_id"]].copy()
    return df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")


def fetch_esios_day(indicator_id: int, day: date, token: str) -> dict:
    start_local = pd.Timestamp(day, tz="Europe/Madrid")
    end_local = start_local + pd.Timedelta(days=1)

    start_utc = start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "time_trunc": resolve_time_trunc(day),
    }

    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    resp = requests.get(url, headers=build_headers(token), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_hourly_energy_for_indicator(indicator_id: int | None, day: date, token: str | None) -> pd.DataFrame:
    if indicator_id is None or not token:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh"])

    try:
        raw = fetch_esios_day(indicator_id, day, token)
        df = parse_esios_indicator(raw, source_name=f"esios_{indicator_id}", filter_date=day)
        if df.empty:
            return pd.DataFrame(columns=["datetime", "mw", "energy_mwh"])
        energy = to_energy_intervals(df, value_col_name="mw", energy_col_name="energy_mwh")
        return to_hourly_energy(energy)
    except Exception:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh"])

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_live_hourly_prices_for_day(day: date, token: str | None) -> pd.DataFrame:
    if not token:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])
    try:
        raw = fetch_esios_day(PRICE_INDICATOR_ID, day, token)
        df = parse_esios_indicator(raw, source_name=f"esios_{PRICE_INDICATOR_ID}", filter_date=day)
        if df.empty:
            return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])
        out = to_hourly_mean(df, value_col_name="price")
        return out
    except Exception:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])

def fetch_live_hourly_mean_for_indicator(indicator_id: int, day: date, token: str | None, value_col_name: str) -> pd.DataFrame:
    if not token:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])
    try:
        raw = fetch_esios_day(indicator_id, day, token)
        df = parse_esios_indicator(raw, source_name=f"esios_{indicator_id}", filter_date=day)
        if df.empty:
            return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])
        return to_hourly_mean(df, value_col_name=value_col_name)
    except Exception:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])


# =========================================================
# SOLAR
# =========================================================
def build_best_solar_hourly(
    solar_p48_hourly: pd.DataFrame,
    solar_forecast_hourly: pd.DataFrame,
) -> pd.DataFrame:
    base_cols = ["datetime", "source", "geo_name", "geo_id"]
    p48 = solar_p48_hourly.copy()
    fc = solar_forecast_hourly.copy()

    if p48.empty and fc.empty:
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

    if p48.empty:
        out = fc.rename(columns={"solar_forecast_mw": "solar_best_mw"}).copy()
        out["solar_source"] = "Forecast"
        return out[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]]

    if fc.empty:
        out = p48.rename(columns={"solar_p48_mw": "solar_best_mw"}).copy()
        out["solar_source"] = "P48"
        return out[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]]

    merged = p48[base_cols + ["solar_p48_mw"]].merge(
        fc[base_cols + ["solar_forecast_mw"]],
        on="datetime",
        how="outer",
        suffixes=("_p48", "_fc"),
    )

    merged["solar_best_mw"] = merged["solar_p48_mw"].combine_first(merged["solar_forecast_mw"])
    merged["solar_source"] = merged["solar_p48_mw"].apply(lambda x: "P48" if pd.notna(x) else None)
    merged.loc[merged["solar_source"].isna() & merged["solar_forecast_mw"].notna(), "solar_source"] = "Forecast"

    merged["source"] = "best_solar"
    merged["geo_name"] = merged.get("geo_name_p48", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_name_fc", pd.Series([None] * len(merged)))
    )
    merged["geo_id"] = merged.get("geo_id_p48", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_id_fc", pd.Series([None] * len(merged)))
    )

    out = merged[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]].copy()
    out = ensure_datetime_col(out, "datetime")
    return out.sort_values("datetime").reset_index(drop=True)


def build_solar_profile_for_report_day(
    solar_p48_hourly: pd.DataFrame,
    solar_forecast_hourly: pd.DataFrame,
    report_day: date,
    token: str | None,
) -> pd.DataFrame:
    """
    Mirror the Day Ahead logic: for the selected day, combine hourly P48 and forecast
    and prioritize P48 when available, falling back to forecast only where needed.
    """
    tomorrow = today_madrid() + timedelta(days=1)

    solar_p48_hourly = ensure_datetime_col(solar_p48_hourly, "datetime")
    solar_forecast_hourly = ensure_datetime_col(solar_forecast_hourly, "datetime")

    # Try live data for the selected day first, using the same priority as Day Ahead:
    # P48 first, then forecast as fallback.
    live_p48 = fetch_live_hourly_mean_for_indicator(SOLAR_P48_INDICATOR_ID, report_day, token, "solar_p48_mw")
    live_fc = fetch_live_hourly_mean_for_indicator(SOLAR_FORECAST_INDICATOR_ID, report_day, token, "solar_forecast_mw")
    if not live_p48.empty or not live_fc.empty:
        live_best = build_best_solar_hourly(live_p48, live_fc)
        if not live_best.empty:
            return live_best[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)

    # For tomorrow, if there is no live day-ahead profile, shift the previous day's reliable profile.
    if report_day == tomorrow:
        prev_day = report_day - timedelta(days=1)
        prev_p48 = solar_p48_hourly[solar_p48_hourly["datetime"].dt.date == prev_day].copy()
        prev_fc = solar_forecast_hourly[solar_forecast_hourly["datetime"].dt.date == prev_day].copy()
        shifted_best = build_best_solar_hourly(prev_p48, prev_fc)
        if not shifted_best.empty:
            shifted_best["datetime"] = shifted_best["datetime"] + pd.Timedelta(days=1)
            return shifted_best[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)

    # Historical exact day fallback.
    best = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
    out = best[best["datetime"].dt.date == report_day].copy()
    return out[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)

    if report_day == tomorrow:
        prev_day = report_day - timedelta(days=1)

        prev_fc = solar_forecast_hourly[solar_forecast_hourly["datetime"].dt.date == prev_day].copy()
        prev_p48 = solar_p48_hourly[solar_p48_hourly["datetime"].dt.date == prev_day].copy()

        if not prev_fc.empty:
            out = prev_fc.rename(columns={"solar_forecast_mw": "solar_best_mw"}).copy()
            out["solar_source"] = "Forecast"
        elif not prev_p48.empty:
            out = prev_p48.rename(columns={"solar_p48_mw": "solar_best_mw"}).copy()
            out["solar_source"] = "Forecast"
        else:
            return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

        out["datetime"] = out["datetime"] + pd.Timedelta(days=1)
        return out[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)

    best = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
    out = best[best["datetime"].dt.date == report_day].copy()
    return out[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)


def build_hourly_weights_from_energy(df_hourly: pd.DataFrame, report_day: date, value_col: str = "energy_mwh") -> pd.DataFrame:
    hours = pd.date_range(
        start=pd.Timestamp(report_day),
        end=pd.Timestamp(report_day) + pd.Timedelta(hours=23),
        freq="h",
    )
    base = pd.DataFrame({"datetime": hours})
    base["Hour"] = base["datetime"].dt.strftime("%H:%M")

    if df_hourly.empty:
        base["weight"] = 0.0
        return base

    tmp = ensure_datetime_col(df_hourly, "datetime")
    if tmp.empty:
        base["weight"] = 0.0
        return base

    tmp = tmp.groupby("datetime", as_index=False)[value_col].sum()
    base = base.merge(tmp, on="datetime", how="left")
    base[value_col] = base[value_col].fillna(0.0)

    total = base[value_col].sum()
    base["weight"] = base[value_col] / total if total > 0 else 0.0
    return base[["datetime", "Hour", "weight"]]


# =========================================================
# MIX
# =========================================================
def parse_mixed_date(value):
    dt = pd.to_datetime(value, errors="coerce")
    return dt if pd.notna(dt) else pd.NaT


def load_historical_generation_mix_daily_for_report() -> pd.DataFrame:
    """
    Historical generation mix aligned with the Day Ahead page:
    load /data/generation_mix_daily_2021_2025.xlsx when available.
    Output is kept in GWh to match the Email Report tables.
    """
    if not HIST_MIX_XLSX_PATH.exists():
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

    try:
        raw = pd.read_excel(HIST_MIX_XLSX_PATH, sheet_name="data", header=None)
    except Exception:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

    if raw.empty or raw.shape[0] < 6:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

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

    tech_rows = raw.iloc[5:18, :].copy()
    records = []
    for _, row in tech_rows.iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_REE_MIX_TECH_MAP.get(tech_raw, DAILY_REE_TECH_MAP.get(tech_raw, tech_raw))
        if tech not in TECH_ORDER:
            continue
        values = pd.to_numeric(row.iloc[1:], errors="coerce")
        for dt, val in zip(dates, values):
            if pd.isna(dt) or pd.isna(val):
                continue
            records.append(
                {
                    "datetime": pd.Timestamp(dt).normalize(),
                    "technology": tech,
                    "value_gwh": float(val),
                    "data_source": "Historical workbook",
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

    out = (
        out.groupby(["datetime", "technology", "data_source"], as_index=False)["value_gwh"]
        .sum()
        .sort_values(["datetime", "technology", "data_source"])
        .reset_index(drop=True)
    )
    return out


def load_ree_mix_daily() -> pd.DataFrame:
    """
    Primary historical source: generation_mix_daily_2021_2025.xlsx.
    Fallback source: historical_data/ree_generation_structure_daily_peninsular.csv.
    """
    hist = load_historical_generation_mix_daily_for_report()
    if not hist.empty:
        return hist

    df = load_raw_history(REE_MIX_DAILY_CSV_PATH)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

    expected_cols = ["datetime", "technology", "value_gwh", "data_source"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value_gwh"] = pd.to_numeric(df["value_gwh"], errors="coerce")
    df = df.dropna(subset=["datetime", "value_gwh", "technology"]).copy()
    df["technology"] = df["technology"].replace(DAILY_REE_TECH_MAP)
    df = df[df["technology"].isin(TECH_ORDER)].copy()
    df = (
        df.groupby(["datetime", "technology"], as_index=False)
        .agg(value_gwh=("value_gwh", "sum"), data_source=("data_source", "first"))
        .sort_values(["datetime", "technology"])
        .reset_index(drop=True)
    )
    return df


def parse_ree_included_series_for_report(payload: dict, value_field: str = "value") -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or item.get("id")
        for val in attrs.get("values", []) or []:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None)
            rows.append(
                {
                    "datetime": dt,
                    "title": str(title).strip(),
                    value_field: pd.to_numeric(val.get(value_field), errors="coerce"),
                }
            )
    return pd.DataFrame(rows)


def normalize_ree_energy_to_gwh(series: pd.Series) -> pd.Series:
    """
    Daily REE widget data may arrive in GWh-scale values or MWh-scale values
    depending on endpoint behaviour. Normalize to GWh for the Email Report.
    """
    vals = pd.to_numeric(series, errors="coerce")
    max_abs = vals.abs().max(skipna=True) if not vals.empty else None
    if pd.notna(max_abs) and max_abs > 10000:
        return vals / 1000.0
    return vals


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_ree_widget_for_report(category: str, widget: str, start_day: date, end_day: date, time_trunc: str = "day") -> dict:
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


def load_live_2026_mix_daily_from_ree_for_report(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_MIX_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])
    try:
        payload = fetch_ree_widget_for_report("generacion", "estructura-generacion", start_day, end_day, time_trunc="day")
        df = parse_ree_included_series_for_report(payload, value_field="value")
    except Exception:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "data_source"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.normalize()
    df["technology"] = df["title"].map(lambda x: LOCAL_REE_MIX_TECH_MAP.get(str(x).strip(), DAILY_REE_TECH_MAP.get(str(x).strip(), str(x).strip())))
    df["value_gwh"] = normalize_ree_energy_to_gwh(df["value"])
    df["data_source"] = "REE API"
    df = df.dropna(subset=["datetime", "technology", "value_gwh"]).copy()
    df = df[df["technology"].isin(TECH_ORDER)].copy()

    out = (
        df.groupby(["datetime", "technology", "data_source"], as_index=False)["value_gwh"]
        .sum()
        .sort_values(["datetime", "technology", "data_source"])
        .reset_index(drop=True)
    )
    return out


def enrich_mix_with_live_2026(ree_daily_df: pd.DataFrame, period_specs: list[tuple[str, date, date]]) -> pd.DataFrame:
    base = ensure_datetime_col(ree_daily_df, "datetime")
    needed = [(s, e) for _, s, e in period_specs if e >= LIVE_MIX_START_DATE]
    if not needed:
        return base
    start_d = max(min(s for s, _ in needed), LIVE_MIX_START_DATE)
    end_d = max(e for _, e in needed)
    live = load_live_2026_mix_daily_from_ree_for_report(start_d, end_d)
    if live.empty:
        return base
    out = pd.concat([base, live], ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.normalize()
    out = (
        out.dropna(subset=["datetime", "technology", "value_gwh"])
        .sort_values(["datetime", "technology", "data_source"])
        .drop_duplicates(subset=["datetime", "technology"], keep="last")
        .reset_index(drop=True)
    )
    return out


def get_daily_mix_totals(report_day: date, ree_daily_df: pd.DataFrame) -> pd.DataFrame:
    if ree_daily_df.empty:
        return pd.DataFrame(columns=["technology", "value_gwh", "data_source"])

    ree_daily_df = ensure_datetime_col(ree_daily_df, "datetime")
    if ree_daily_df.empty:
        return pd.DataFrame(columns=["technology", "value_gwh", "data_source"])

    tomorrow = today_madrid() + timedelta(days=1)
    source_day = report_day - timedelta(days=1) if report_day == tomorrow else report_day

    out = ree_daily_df[ree_daily_df["datetime"].dt.date == source_day].copy()
    if out.empty:
        return pd.DataFrame(columns=["technology", "value_gwh", "data_source"])

    if report_day == tomorrow:
        out["data_source"] = "Forecast"

    return out[["technology", "value_gwh", "data_source"]].copy()


def shift_previous_day_to_target(best_hourly: pd.DataFrame, target_day: date) -> pd.DataFrame:
    if best_hourly.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "technology", "data_source"])

    best_hourly = ensure_datetime_col(best_hourly, "datetime")
    if best_hourly.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "technology", "data_source"])

    prev_day = target_day - timedelta(days=1)
    prev_df = best_hourly[best_hourly["datetime"].dt.date == prev_day].copy()
    if prev_df.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "technology", "data_source"])

    prev_df["datetime"] = prev_df["datetime"] + pd.Timedelta(days=1)
    if "data_source" in prev_df.columns:
        prev_df["data_source"] = "Forecast"
    return prev_df


def fetch_best_mix_shape_for_original_tech(original_tech: str, report_day: date, token: str | None) -> pd.DataFrame:
    official_id = ENERGY_MIX_INDICATORS_OFFICIAL.get(original_tech)
    forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(original_tech)
    tomorrow = today_madrid() + timedelta(days=1)

    # Intento 1: si mañana y existe forecast horario real, usarlo
    if report_day == tomorrow and token and forecast_id is not None:
        fc = fetch_hourly_energy_for_indicator(forecast_id, report_day, token)
        if not fc.empty:
            fc["technology"] = original_tech
            fc["data_source"] = "Forecast"
            return fc

    # Intento 2: oficial del día
    if token and official_id is not None and report_day != tomorrow:
        off = fetch_hourly_energy_for_indicator(official_id, report_day, token)
        if not off.empty:
            off["technology"] = original_tech
            off["data_source"] = "Official"
            return off

    # Intento 3: mañana -> oficial del día anterior desplazado
    if token and official_id is not None and report_day == tomorrow:
        off_prev = fetch_hourly_energy_for_indicator(official_id, report_day - timedelta(days=1), token)
        if not off_prev.empty:
            off_prev["datetime"] = pd.to_datetime(off_prev["datetime"], errors="coerce") + pd.Timedelta(days=1)
            off_prev["technology"] = original_tech
            off_prev["data_source"] = "Forecast"
            return off_prev

    # Fallback a histórico local cacheado
    local = load_mix_best_energy(original_tech, official_id, forecast_id)
    local = ensure_datetime_col(local, "datetime")
    if local.empty:
        return pd.DataFrame(columns=["datetime", "energy_mwh"])

    if report_day == tomorrow:
        local = shift_previous_day_to_target(local, report_day)
    else:
        local = local[local["datetime"].dt.date == report_day].copy()

    return local


def load_mix_best_energy(tech_name: str, official_id: int | None, forecast_id: int | None) -> pd.DataFrame:
    def get_mix_indicator_csv_path_variant(name: str, indicator_id: int | None, variant: str) -> Path:
        safe_name = name.lower().replace(" ", "_").replace("/", "_")
        suffix = "none" if indicator_id is None else str(indicator_id)
        return DATA_DIR / f"mix_{variant}_{suffix}_{safe_name}.csv"

    official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if official_id is not None:
        official_df = load_raw_history(
            get_mix_indicator_csv_path_variant(tech_name, official_id, "official"),
            f"esios_{official_id}",
        )

    if forecast_id is not None:
        forecast_df = load_raw_history(
            get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"),
            f"esios_{forecast_id}",
        )

    official_energy = to_energy_intervals(official_df, value_col_name="mw", energy_col_name="energy_mwh")
    forecast_energy = to_energy_intervals(forecast_df, value_col_name="mw", energy_col_name="energy_mwh")

    if official_energy.empty and forecast_energy.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "technology", "data_source"])

    if official_energy.empty:
        out = forecast_energy.copy()
        out["technology"] = tech_name
        out["data_source"] = "Forecast"
        return to_hourly_energy(out)

    if forecast_energy.empty:
        out = official_energy.copy()
        out["technology"] = tech_name
        out["data_source"] = "Official"
        return to_hourly_energy(out)

    off = official_energy[["datetime", "mw", "energy_mwh"]].copy().rename(
        columns={"mw": "mw_official", "energy_mwh": "energy_mwh_official"}
    )
    fc = forecast_energy[["datetime", "mw", "energy_mwh"]].copy().rename(
        columns={"mw": "mw_forecast", "energy_mwh": "energy_mwh_forecast"}
    )

    merged = off.merge(fc, on="datetime", how="outer")
    merged["mw"] = merged["mw_official"].combine_first(merged["mw_forecast"])
    merged["energy_mwh"] = merged["energy_mwh_official"].combine_first(merged["energy_mwh_forecast"])
    merged["data_source"] = merged["mw_official"].apply(lambda x: "Official" if pd.notna(x) else None)
    merged.loc[merged["data_source"].isna() & merged["mw_forecast"].notna(), "data_source"] = "Forecast"
    merged["technology"] = tech_name

    out = merged[["datetime", "mw", "energy_mwh", "technology", "data_source"]].copy()
    return to_hourly_energy(out)


def get_hourly_shape_for_tech(
    tech_name: str,
    best_hourly: pd.DataFrame,
    report_day: date,
    solar_profile_day: pd.DataFrame,
) -> pd.DataFrame:
    best_hourly = ensure_datetime_col(best_hourly, "datetime")
    solar_profile_day = ensure_datetime_col(solar_profile_day, "datetime")

    if not best_hourly.empty:
        return build_hourly_weights_from_energy(best_hourly, report_day, value_col="energy_mwh")

    if tech_name in {"Solar PV", "Solar thermal"} and not solar_profile_day.empty:
        proxy = solar_profile_day.copy().rename(columns={"solar_best_mw": "energy_mwh"})
        return build_hourly_weights_from_energy(proxy, report_day, value_col="energy_mwh")

    hours = pd.date_range(
        start=pd.Timestamp(report_day),
        end=pd.Timestamp(report_day) + pd.Timedelta(hours=23),
        freq="h",
    )
    flat = pd.DataFrame({"datetime": hours})
    flat["Hour"] = flat["datetime"].dt.strftime("%H:%M")
    flat["weight"] = 1 / 24
    return flat[["datetime", "Hour", "weight"]]


def build_all_mix_hourly_for_day(report_day: date, ree_daily_df: pd.DataFrame, solar_profile_day: pd.DataFrame, token: str | None) -> pd.DataFrame:
    daily_totals = get_daily_mix_totals(report_day, ree_daily_df)
    if daily_totals.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "technology", "energy_mwh", "data_source"])

    rows = []

    for tech_name in TECH_ORDER:
        tech_daily = daily_totals[daily_totals["technology"] == tech_name].copy()
        if tech_daily.empty:
            continue

        daily_total_mwh = float(tech_daily["value_gwh"].sum()) * 1000.0
        data_source = tech_daily["data_source"].iloc[0] if not tech_daily.empty else "Official"

        if tech_name == "Hydro":
            original_techs = ["Hydro UGH", "Hydro non-UGH", "Pumped hydro"]
        else:
            original_techs = [tech_name]

        hourly_candidates = []
        for original_tech in original_techs:
            best = fetch_best_mix_shape_for_original_tech(original_tech, report_day, token)
            best = ensure_datetime_col(best, "datetime")
            if not best.empty and "energy_mwh" in best.columns:
                hourly_candidates.append(best[["datetime", "energy_mwh"]].copy())

        if hourly_candidates:
            tech_hourly = (
                pd.concat(hourly_candidates, ignore_index=True)
                .groupby("datetime", as_index=False)
                .agg(energy_mwh=("energy_mwh", "sum"))
                .sort_values("datetime")
                .reset_index(drop=True)
            )
        else:
            tech_hourly = pd.DataFrame(columns=["datetime", "energy_mwh"])

        weights = get_hourly_shape_for_tech(
            tech_name=tech_name,
            best_hourly=tech_hourly,
            report_day=report_day,
            solar_profile_day=solar_profile_day,
        )

        if weights["weight"].sum() <= 0:
            weights["weight"] = 1 / 24

        weights["technology"] = tech_name
        weights["energy_mwh"] = daily_total_mwh * weights["weight"]
        weights["data_source"] = data_source

        rows.append(weights[["datetime", "Hour", "technology", "energy_mwh", "data_source"]])

    if not rows:
        return pd.DataFrame(columns=["datetime", "Hour", "technology", "energy_mwh", "data_source"])

    out = pd.concat(rows, ignore_index=True)
    out = ensure_datetime_col(out, "datetime")
    out = out.sort_values(["datetime", "technology"]).reset_index(drop=True)
    return out


# =========================================================
# DEMAND
# =========================================================
def build_hourly_demand_for_day(demand_raw: pd.DataFrame, report_day: date) -> pd.DataFrame:
    if demand_raw.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "demand_mwh"])

    demand_hourly = to_hourly_mean(demand_raw, "demand_mw")
    if demand_hourly.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "demand_mwh"])

    demand_hourly = ensure_datetime_col(demand_hourly, "datetime")
    demand_hourly["energy_mwh"] = pd.to_numeric(demand_hourly["demand_mw"], errors="coerce")
    tomorrow = today_madrid() + timedelta(days=1)

    if report_day == tomorrow:
        prev_day = report_day - timedelta(days=1)
        day_df = demand_hourly[demand_hourly["datetime"].dt.date == prev_day].copy()
        if not day_df.empty:
            day_df["datetime"] = day_df["datetime"] + pd.Timedelta(days=1)
    else:
        day_df = demand_hourly[demand_hourly["datetime"].dt.date == report_day].copy()

    if day_df.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "demand_mwh"])

    day_df["Hour"] = day_df["datetime"].dt.strftime("%H:%M")
    day_df = day_df.rename(columns={"energy_mwh": "demand_mwh"})
    return day_df[["datetime", "Hour", "demand_mwh"]].sort_values("datetime").reset_index(drop=True)


# =========================================================
# METRICS / CAPTURE
# =========================================================
def compute_period_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_d: date, end_d: date) -> dict:
    price_hourly = ensure_datetime_col(price_hourly, "datetime")
    solar_hourly = ensure_datetime_col(solar_hourly, "datetime")

    period_price = price_hourly[
        (price_hourly["datetime"].dt.date >= start_d)
        & (price_hourly["datetime"].dt.date <= end_d)
    ].copy()

    period_solar = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d)
        & (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None

    merged = period_price.merge(period_solar[["datetime", "solar_best_mw"]], on="datetime", how="left")
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
    merged = merged[merged["solar_best_mw"] > 0].copy()

    captured_uncurtailed = None
    captured_curtailed = None
    if not merged.empty and merged["solar_best_mw"].sum() > 0:
        captured_uncurtailed = (merged["price"] * merged["solar_best_mw"]).sum() / merged["solar_best_mw"].sum()
        positive = merged[merged["price"] > 0].copy()
        if not positive.empty and positive["solar_best_mw"].sum() > 0:
            captured_curtailed = (positive["price"] * positive["solar_best_mw"]).sum() / positive["solar_best_mw"].sum()

    return {
        "avg_price": avg_price,
        "captured": captured_uncurtailed,
        "captured_uncurtailed": captured_uncurtailed,
        "captured_curtailed": captured_curtailed,
        "capture_pct": (captured_uncurtailed / avg_price) if (captured_uncurtailed is not None and avg_price not in [None, 0]) else None,
        "capture_pct_uncurtailed": (captured_uncurtailed / avg_price) if (captured_uncurtailed is not None and avg_price not in [None, 0]) else None,
        "capture_pct_curtailed": (captured_curtailed / avg_price) if (captured_curtailed is not None and avg_price not in [None, 0]) else None,
    }


def make_metrics_df(
    price_hourly: pd.DataFrame,
    historical_best_solar: pd.DataFrame,
    solar_profile_day: pd.DataFrame,
    report_day: date,
) -> pd.DataFrame:
    day_metrics = compute_period_metrics(price_hourly, solar_profile_day, report_day, report_day)

    metric_day = min(report_day, price_hourly["datetime"].dt.date.max())
    month_start = metric_day.replace(day=1)
    ytd_start = metric_day.replace(month=1, day=1)

    mtd_metrics = compute_period_metrics(price_hourly, historical_best_solar, month_start, metric_day)
    ytd_metrics = compute_period_metrics(price_hourly, historical_best_solar, ytd_start, metric_day)

    return pd.DataFrame(
        [
            {
                "Period": "Day",
                "Average price (€/MWh)": day_metrics["avg_price"],
                "Captured solar uncurtailed (€/MWh)": day_metrics["captured_uncurtailed"],
                "Captured solar curtailed (€/MWh)": day_metrics["captured_curtailed"],
                "Solar capture rate uncurtailed (%)": day_metrics["capture_pct_uncurtailed"],
                "Solar capture rate curtailed (%)": day_metrics["capture_pct_curtailed"],
            },
            {
                "Period": "MTD",
                "Average price (€/MWh)": mtd_metrics["avg_price"],
                "Captured solar uncurtailed (€/MWh)": mtd_metrics["captured_uncurtailed"],
                "Captured solar curtailed (€/MWh)": mtd_metrics["captured_curtailed"],
                "Solar capture rate uncurtailed (%)": mtd_metrics["capture_pct_uncurtailed"],
                "Solar capture rate curtailed (%)": mtd_metrics["capture_pct_curtailed"],
            },
            {
                "Period": "YTD",
                "Average price (€/MWh)": ytd_metrics["avg_price"],
                "Captured solar uncurtailed (€/MWh)": ytd_metrics["captured_uncurtailed"],
                "Captured solar curtailed (€/MWh)": ytd_metrics["captured_curtailed"],
                "Solar capture rate uncurtailed (%)": ytd_metrics["capture_pct_uncurtailed"],
                "Solar capture rate curtailed (%)": ytd_metrics["capture_pct_curtailed"],
            },
        ]
    )


def build_daily_dataset(price_hourly: pd.DataFrame, solar_profile_day: pd.DataFrame, report_day: date):
    price_hourly = ensure_datetime_col(price_hourly, "datetime")
    solar_profile_day = ensure_datetime_col(solar_profile_day, "datetime")

    day_price = price_hourly[price_hourly["datetime"].dt.date == report_day].copy()
    day_solar = solar_profile_day.copy()

    merged = day_price.merge(
        day_solar[["datetime", "solar_best_mw", "solar_source"]],
        on="datetime",
        how="left",
    )
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
    merged["solar_source"] = merged["solar_source"].fillna("No data")

    positive_solar = merged[merged["solar_best_mw"] > 0].copy()
    capture_price = None
    if not positive_solar.empty:
        capture_price = (positive_solar["price"] * positive_solar["solar_best_mw"]).sum() / positive_solar["solar_best_mw"].sum()

    merged["Hour"] = merged["datetime"].dt.hour + 1
    merged = merged.rename(
        columns={
            "price": "Price (€/MWh)",
            "solar_best_mw": "Solar (MW)",
            "solar_source": "Solar source",
        }
    )

    return merged[["datetime", "Hour", "Price (€/MWh)", "Solar (MW)", "Solar source"]].copy(), capture_price


def compute_energy_mix_kpis(day_mix_hourly: pd.DataFrame, hourly_df: pd.DataFrame) -> tuple[float | None, int]:
    negative_hours = int((hourly_df["Price (€/MWh)"] <= 0).sum()) if not hourly_df.empty else 0
    if day_mix_hourly.empty:
        return None, negative_hours

    total = float(day_mix_hourly["energy_mwh"].sum())
    if total <= 0:
        return None, negative_hours

    renew = float(day_mix_hourly[day_mix_hourly["technology"].isin(RENEWABLE_TECHS)]["energy_mwh"].sum())
    return renew / total, negative_hours


# =========================================================
# BESS TB SPREADS
# =========================================================
def compute_bess_tb_spread(
    hourly_df: pd.DataFrame,
    capacity_mwh: float = 1.0,
    c_rate: float = 0.25,
    eta_ch: float = 1.0,
    eta_dis: float = 1.0,
) -> float | None:
    """
    Deterministic TB spread proxy without external solver binaries.

    For a 1-cycle standalone battery with equal buy/sell spot prices:
    - TB4 (c-rate 0.25) = weighted sell price of the 4 highest-price hours
      minus weighted buy price of the 4 lowest-price hours.
    - TB2 (c-rate 0.50) = same logic over 2 charging/discharging hours.

    This avoids CBC/PuLP executable issues on Streamlit Cloud while keeping the
    standard economic interpretation of TB spreads.
    """
    if hourly_df.empty or "Price (€/MWh)" not in hourly_df.columns:
        return None

    prices = pd.to_numeric(hourly_df["Price (€/MWh)"], errors="coerce").dropna().tolist()
    if not prices:
        return None

    capacity_mwh = float(capacity_mwh)
    c_rate = float(c_rate)
    eta_ch = max(float(eta_ch), 1e-9)
    eta_dis = max(float(eta_dis), 1e-9)
    power_mwh_per_hour = capacity_mwh * c_rate
    if capacity_mwh <= 0 or power_mwh_per_hour <= 0:
        return None

    remaining_charge = capacity_mwh / eta_ch
    remaining_discharge = capacity_mwh * eta_dis
    buy_cost = 0.0
    sell_revenue = 0.0

    for p in sorted(prices):
        q = min(power_mwh_per_hour, remaining_charge)
        if q <= 0:
            break
        buy_cost += q * float(p)
        remaining_charge -= q

    for p in sorted(prices, reverse=True):
        q = min(power_mwh_per_hour, remaining_discharge)
        if q <= 0:
            break
        sell_revenue += q * float(p)
        remaining_discharge -= q

    if remaining_charge > 1e-6 or remaining_discharge > 1e-6:
        return None

    return (sell_revenue - buy_cost) / capacity_mwh


def make_tb_spreads_table(hourly_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for label, c_rate in [("TB4", 0.25), ("TB2", 0.50), ("TB1", 1.00)]:
        spread = compute_bess_tb_spread(
            hourly_df=hourly_df,
            capacity_mwh=1.0,
            c_rate=c_rate,
            eta_ch=1.0,
            eta_dis=1.0,
        )
        rows.append(
            {
                "Product": label,
                "Battery size (MWh)": 1.0,
                "C-rate": c_rate,
                "Efficiency": "100% / 100%",
                "Spread (€/MWh)": spread,
            }
        )
    return pd.DataFrame(rows)


# =========================================================
# CHARTS
# =========================================================
def build_overlay_chart(hourly_df: pd.DataFrame):
    if hourly_df.empty:
        return None

    hour_order = sorted(hourly_df["Hour"].dropna().unique().tolist())

    price_line = (
        alt.Chart(hourly_df)
        .mark_line(point=True, color="#0f766e", strokeWidth=3)
        .encode(
            x=alt.X("Hour:O", sort=hour_order, axis=alt.Axis(title="Hour", labelAngle=0)),
            y=alt.Y("Price (€/MWh):Q", title="Price (€/MWh)"),
            tooltip=[
                alt.Tooltip("Hour:O", title="Hour"),
                alt.Tooltip("Price (€/MWh):Q", title="Price", format=".2f"),
                alt.Tooltip("Solar (MW):Q", title="Solar", format=".2f"),
                alt.Tooltip("Solar source:N", title="Solar source"),
            ],
        )
    )

    solar_area = (
        alt.Chart(hourly_df)
        .mark_area(opacity=0.25, color="#FACC15")
        .encode(
            x=alt.X("Hour:O", sort=hour_order, axis=alt.Axis(title="Hour", labelAngle=0)),
            y=alt.Y("Solar (MW):Q", title="Solar (MW)"),
        )
    )

    return alt.layer(price_line, solar_area).resolve_scale(y="independent").properties(height=360)


def build_mix_with_demand_chart(day_mix_hourly: pd.DataFrame, demand_hourly_day: pd.DataFrame):
    if day_mix_hourly.empty and demand_hourly_day.empty:
        return None

    layers = []

    if not day_mix_hourly.empty:
        order_list = day_mix_hourly["Hour"].drop_duplicates().tolist()
        bars = (
            alt.Chart(day_mix_hourly)
            .mark_bar()
            .encode(
                x=alt.X("Hour:N", sort=order_list, axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("energy_mwh:Q", title="Generation & demand (MWh)", stack=True),
                color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
                order=alt.Order("technology:N", sort="ascending"),
                tooltip=[
                    alt.Tooltip("Hour:N", title="Hour"),
                    alt.Tooltip("technology:N", title="Technology"),
                    alt.Tooltip("energy_mwh:Q", title="Generation (MWh)", format=",.2f"),
                    alt.Tooltip("data_source:N", title="Data source"),
                ],
            )
        )
        layers.append(bars)

    if not demand_hourly_day.empty:
        demand_line = (
            alt.Chart(demand_hourly_day)
            .mark_line(point=True, color=TECH_COLORS["Demand"], strokeWidth=2.5)
            .encode(
                x=alt.X("Hour:N", sort=demand_hourly_day["Hour"].drop_duplicates().tolist(), axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("demand_mwh:Q", title="Generation & demand (MWh)"),
                tooltip=[
                    alt.Tooltip("Hour:N", title="Hour"),
                    alt.Tooltip("demand_mwh:Q", title="Demand (MWh)", format=",.2f"),
                ],
            )
        )
        layers.append(demand_line)

    return alt.layer(*layers).properties(height=400)


def build_mix_matrix_table(day_mix_hourly: pd.DataFrame, demand_hourly_day: pd.DataFrame) -> pd.DataFrame:
    if day_mix_hourly.empty and demand_hourly_day.empty:
        return pd.DataFrame()

    mix_pivot = pd.DataFrame()
    if not day_mix_hourly.empty:
        mix_pivot = (
            day_mix_hourly.pivot_table(
                index="technology",
                columns="Hour",
                values="energy_mwh",
                aggfunc="sum",
                fill_value=0.0,
            )
            .reindex(TECH_ORDER, fill_value=0.0)
        )
        mix_pivot = mix_pivot.loc[(mix_pivot.sum(axis=1) > 0)]
        mix_pivot = mix_pivot.reindex(sorted(mix_pivot.columns), axis=1)
        mix_pivot.columns = [str(c) for c in mix_pivot.columns]
        mix_pivot = mix_pivot.reset_index().rename(columns={"technology": "Technology"})

    demand_row = pd.DataFrame()
    if not demand_hourly_day.empty:
        tmp = demand_hourly_day.copy().sort_values("Hour")
        demand_row = pd.DataFrame([{"Technology": "Demand", **{str(r["Hour"]): r["demand_mwh"] for _, r in tmp.iterrows()}}])

    if mix_pivot.empty:
        return demand_row
    if demand_row.empty:
        return mix_pivot

    return pd.concat([mix_pivot, demand_row], ignore_index=True)


def chart_to_base64_png(chart) -> str | None:
    if chart is None:
        return None
    try:
        png_bytes = chart.to_image(format="png")
        return base64.b64encode(png_bytes).decode("utf-8")
    except Exception:
        return None


def line_area_png_base64(hourly_df: pd.DataFrame) -> str | None:
    if hourly_df.empty:
        return None
    try:
        fig, ax1 = plt.subplots(figsize=(10, 4.2), dpi=140)
        ax2 = ax1.twinx()

        ax1.plot(hourly_df["datetime"], hourly_df["Price (€/MWh)"], color="#0f766e", linewidth=2.2, marker="o", markersize=3)
        ax2.fill_between(hourly_df["datetime"], hourly_df["Solar (MW)"], color="#FACC15", alpha=0.28)

        ax1.set_ylabel("Price (€/MWh)")
        ax2.set_ylabel("Solar (MW)")
        ax1.grid(alpha=0.25)
        ax1.set_facecolor("#f8fafc")
        fig.patch.set_facecolor("white")
        fig.autofmt_xdate(rotation=0)

        buffer = BytesIO()
        fig.tight_layout()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        plt.close(fig)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    except Exception:
        return None


def mix_with_demand_png_base64(day_mix_hourly: pd.DataFrame, demand_hourly_day: pd.DataFrame) -> str | None:
    if day_mix_hourly.empty and demand_hourly_day.empty:
        return None

    try:
        mix_pivot = pd.DataFrame()
        if not day_mix_hourly.empty:
            mix_pivot = (
                day_mix_hourly.pivot_table(
                    index="Hour",
                    columns="technology",
                    values="energy_mwh",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .reindex(columns=[c for c in TECH_ORDER if c in day_mix_hourly["technology"].unique()])
                .sort_index()
            )

        fig, ax = plt.subplots(figsize=(11, 5), dpi=140)

        if not mix_pivot.empty:
            bottom = None
            for tech in mix_pivot.columns:
                vals = mix_pivot[tech].values
                ax.bar(
                    mix_pivot.index,
                    vals,
                    bottom=bottom,
                    label=tech,
                    color=TECH_COLORS.get(tech, "#9CA3AF"),
                )
                bottom = vals if bottom is None else bottom + vals

        if not demand_hourly_day.empty:
            demand_plot = demand_hourly_day.sort_values("Hour")
            ax.plot(
                demand_plot["Hour"],
                demand_plot["demand_mwh"],
                color=TECH_COLORS["Demand"],
                linewidth=2.5,
                marker="o",
                markersize=3,
                label="Demand",
            )

        ax.set_ylabel("Generation & demand (MWh)")
        ax.set_xlabel("Hour")
        ax.grid(axis="y", alpha=0.22)
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8)
        ax.set_facecolor("#f8fafc")

        buffer = BytesIO()
        fig.tight_layout()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        plt.close(fig)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    except Exception:
        return None


# =========================================================
# MONTHLY COMPARISON HELPERS
# =========================================================
def month_start_end(period_ts: pd.Timestamp) -> tuple[date, date]:
    start = period_ts.to_period("M").to_timestamp().date()
    end = (period_ts.to_period("M").to_timestamp() + pd.offsets.MonthEnd(0)).date()
    return start, end


def build_period_metric_row(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, label: str, start_d: date, end_d: date) -> dict:
    metrics = compute_period_metrics(price_hourly, solar_hourly, start_d, end_d)
    period_price = price_hourly[
        (price_hourly["datetime"].dt.date >= start_d)
        & (price_hourly["datetime"].dt.date <= end_d)
    ].copy()
    period_solar = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d)
        & (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()
    merged = period_price.merge(period_solar[["datetime", "solar_best_mw"]], on="datetime", how="left")
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
    zero_neg_hours = int((period_price["price"] <= 0).sum()) if not period_price.empty else 0
    solar_zero_neg_hours = int(((merged["price"] <= 0) & (merged["solar_best_mw"] > 0)).sum()) if not merged.empty else 0
    return {
        "Period": label,
        "Average price (€/MWh)": metrics.get("avg_price"),
        "Captured solar uncurtailed (€/MWh)": metrics.get("captured_uncurtailed"),
        "Captured solar curtailed (€/MWh)": metrics.get("captured_curtailed"),
        "Solar capture rate uncurtailed (%)": metrics.get("capture_pct_uncurtailed"),
        "Solar capture rate curtailed (%)": metrics.get("capture_pct_curtailed"),
        "Zero / negative hours": zero_neg_hours,
        "Zero / negative hours during solar": solar_zero_neg_hours,
    }


def build_monthly_mtd_ytd_comparison_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, primary_month: pd.Timestamp, compare_month: pd.Timestamp) -> pd.DataFrame:
    rows = []

    p_start, p_end = month_start_end(primary_month)
    c_start, c_end = month_start_end(compare_month)

    primary_cutoff = p_end
    latest_day = price_hourly["datetime"].dt.date.max() if not price_hourly.empty else p_end
    if primary_month.to_period("M") == pd.Timestamp(latest_day).to_period("M"):
        primary_cutoff = min(latest_day, p_end)

    compare_cutoff_day = min(primary_cutoff.day, c_end.day)
    compare_cutoff = c_start.replace(day=compare_cutoff_day)

    rows.append(build_period_metric_row(price_hourly, solar_hourly, f"MTD {primary_month.strftime('%b-%Y')}", p_start, primary_cutoff))
    rows.append(build_period_metric_row(price_hourly, solar_hourly, f"MTD {compare_month.strftime('%b-%Y')}", c_start, compare_cutoff))

    p_ytd_start = date(primary_month.year, 1, 1)
    c_ytd_start = date(compare_month.year, 1, 1)
    compare_ytd_cutoff = date(compare_month.year, compare_month.month, compare_cutoff_day)
    rows.append(build_period_metric_row(price_hourly, solar_hourly, f"YTD {primary_month.year}", p_ytd_start, primary_cutoff))
    rows.append(build_period_metric_row(price_hourly, solar_hourly, f"YTD {compare_month.year}", c_ytd_start, compare_ytd_cutoff))

    return pd.DataFrame(rows)


def price_event_mask(series: pd.Series, mode: str) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    if mode == "Only negative prices":
        return vals < 0
    return vals <= 0


def price_event_label(mode: str) -> str:
    return "negative price" if mode == "Only negative prices" else "zero / negative price"


def build_omie_hourly_price_heatmap_png_base64(price_hourly: pd.DataFrame, year_sel: int) -> str | None:
    """Full-year OMIE hourly heatmap: x = day of year, y = hour.

    Low prices are green; medium prices yellow/orange; high prices red.
    Missing/future hours are light grey.
    """
    if price_hourly.empty:
        return None
    try:
        tmp = ensure_datetime_col(price_hourly, "datetime")
        tmp = tmp[tmp["datetime"].dt.year == int(year_sel)].copy()
        if tmp.empty:
            return None

        start = pd.Timestamp(year=int(year_sel), month=1, day=1)
        end = pd.Timestamp(year=int(year_sel), month=12, day=31)
        all_days = pd.date_range(start, end, freq="D")
        n_days = len(all_days)
        grid = pd.DataFrame({"datetime_day": all_days})
        grid["key"] = 1
        hours = pd.DataFrame({"hour": list(range(24)), "key": 1})
        full = grid.merge(hours, on="key").drop(columns="key")
        full["datetime"] = full["datetime_day"] + pd.to_timedelta(full["hour"], unit="h")

        t = tmp.copy()
        t["datetime"] = t["datetime"].dt.floor("h")
        t = t.groupby("datetime", as_index=False)["price"].mean()
        full = full.merge(t, on="datetime", how="left")

        matrix = full.pivot(index="hour", columns="datetime_day", values="price").reindex(index=range(24), columns=all_days)
        data = matrix.to_numpy(dtype=float)
        masked = pd.DataFrame(data).mask(pd.isna(data)).to_numpy()

        cmap = LinearSegmentedColormap.from_list(
            "omie_low_green_high_red",
            [PRICE_LOW_GREEN, PRICE_MID_YELLOW, PRICE_MID_ORANGE, PRICE_HIGH_RED],
        )
        cmap.set_bad(MISSING_GREY)

        valid = pd.Series(data.ravel()).dropna()
        vmax = float(max(150.0, valid.quantile(0.98))) if not valid.empty else 150.0
        vmin = float(min(0.0, valid.quantile(0.02))) if not valid.empty else 0.0

        fig, ax = plt.subplots(figsize=(12.5, 4.8), dpi=140)
        im = ax.imshow(masked, aspect="auto", interpolation="nearest", cmap=cmap, vmin=vmin, vmax=vmax)

        month_starts = pd.date_range(start, end, freq="MS")
        month_positions = [(m - start).days for m in month_starts]
        ax.set_xticks(month_positions)
        ax.set_xticklabels([m.strftime("%b") for m in month_starts], fontsize=8)
        ax.set_yticks(list(range(0, 24, 2)))
        ax.set_yticklabels([str(h) for h in range(0, 24, 2)], fontsize=8)
        ax.set_xlabel("Month")
        ax.set_ylabel("Time [hour]")
        ax.set_title(f"OMIE hourly price heatmap | {year_sel} | 24 x 365", fontsize=11, fontweight="bold")
        ax.grid(False)
        ax.set_facecolor(MISSING_GREY)

        cbar = fig.colorbar(im, ax=ax, fraction=0.022, pad=0.02)
        cbar.set_label("Spot [€/MWh]")
        cbar.ax.tick_params(labelsize=8)

        fig.text(
            0.01,
            0.01,
            "Color scale: green = lower spot price; yellow/orange = medium price; red = higher spot price. Missing/future hours are light grey.",
            fontsize=8,
            color="#6B7280",
        )
        fig.tight_layout(rect=[0, 0.04, 1, 1])
        buffer = BytesIO()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        plt.close(fig)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    except Exception:
        return None


def build_zero_negative_hour_table(price_hourly: pd.DataFrame, year_sel: int, mode: str = "Zero and negative prices") -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame()
    tmp = ensure_datetime_col(price_hourly, "datetime")
    tmp = tmp[tmp["datetime"].dt.year == int(year_sel)].copy()
    if tmp.empty:
        return pd.DataFrame()
    tmp["Month"] = tmp["datetime"].dt.strftime("%b")
    tmp["Hour"] = tmp["datetime"].dt.strftime("H%H")
    tmp["is_event"] = price_event_mask(tmp["price"], mode)
    total = tmp.groupby(["Month", "Hour"], as_index=False)["price"].count().rename(columns={"price": "total_hours"})
    neg = tmp[tmp["is_event"]].groupby(["Month", "Hour"], as_index=False)["price"].count().rename(columns={"price": "event_hours"})
    out = total.merge(neg, on=["Month", "Hour"], how="left")
    out["event_hours"] = out["event_hours"].fillna(0)
    out["pct_event"] = out["event_hours"] / out["total_hours"]
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    hour_order = [f"H{i:02d}" for i in range(24)]
    pivot = out.pivot(index="Month", columns="Hour", values="pct_event").reindex(index=month_order, columns=hour_order)
    return pivot.reset_index()


def build_zero_negative_heatmap(price_hourly: pd.DataFrame, year_sel: int, mode: str = "Zero and negative prices"):
    table = build_zero_negative_hour_table(price_hourly, year_sel, mode=mode)
    if table.empty:
        return None
    plot = table.melt(id_vars="Month", var_name="Hour", value_name="pct_event")
    plot["pct_label"] = plot["pct_event"].map(lambda x: "" if pd.isna(x) else f"{x:.0%}")
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    label = price_event_label(mode)
    rect = alt.Chart(plot).mark_rect().encode(
        x=alt.X("Hour:N", title="Hour"),
        y=alt.Y("Month:N", sort=month_order, title="Month"),
        color=alt.Color("pct_event:Q", title=f"% {label} hours", scale=alt.Scale(scheme="teals")),
        tooltip=["Month", "Hour", alt.Tooltip("pct_event:Q", title=f"% {label} hours", format=".1%")],
    )
    txt = alt.Chart(plot).mark_text(fontSize=8).encode(
        x="Hour:N",
        y=alt.Y("Month:N", sort=month_order),
        text="pct_label:N",
        color=alt.condition("datum.pct_event >= 0.5", alt.value("white"), alt.value("#111827")),
    )
    return alt.layer(rect, txt).properties(height=380, title=f"12x24 {label} frequency ({year_sel})")


def build_curtailment_pct_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, years: list[int], mode: str = "Zero and negative prices") -> pd.DataFrame:
    _, plot = build_monthly_curtailment_chart(price_hourly, solar_hourly, years, mode=mode)
    if plot.empty:
        return pd.DataFrame()
    out = plot[["Year", "month_num", "month_name", "pct_curtailment"]].copy()
    out["Month"] = out["month_name"]
    return out.sort_values(["Year", "month_num"])[["Year", "Month", "pct_curtailment"]]


def build_spot_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, year: int) -> pd.DataFrame:
    _, plot = build_spot_capture_evolution_chart(price_hourly, solar_hourly, year)
    if plot.empty:
        return pd.DataFrame()
    pivot = plot.pivot(index="Month", columns="Series", values="value").reset_index()
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    pivot["month_num"] = pivot["Month"].map({m:i for i,m in enumerate(month_order,1)})
    pivot = pivot.sort_values("month_num").drop(columns=["month_num"])
    return pivot


def build_monthly_negative_pct_table(price_hourly: pd.DataFrame, years: list[int], mode: str = "Zero and negative prices") -> pd.DataFrame:
    rows = []
    for year in years:
        tmp = ensure_datetime_col(price_hourly, "datetime")
        tmp = tmp[tmp["datetime"].dt.year == int(year)].copy()
        if tmp.empty:
            continue
        tmp["month_num"] = tmp["datetime"].dt.month
        tmp["month_name"] = tmp["datetime"].dt.strftime("%b")
        tmp["is_event"] = price_event_mask(tmp["price"], mode)
        total = tmp.groupby(["month_num", "month_name"], as_index=False)["price"].count().rename(columns={"price": "total_hours"})
        neg = tmp[tmp["is_event"]].groupby(["month_num", "month_name"], as_index=False)["price"].count().rename(columns={"price": "event_hours"})
        out = total.merge(neg, on=["month_num", "month_name"], how="left")
        out["event_hours"] = out["event_hours"].fillna(0)
        out["pct_event"] = out["event_hours"] / out["total_hours"]
        out["Year"] = year
        rows.append(out)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["Month"] = out["month_name"]
    return out[["Year", "Month", "month_num", "event_hours", "total_hours", "pct_event"]].sort_values(["Year", "month_num"])


def build_monthly_negative_chart(price_hourly: pd.DataFrame, years: list[int], mode: str = "Zero and negative prices"):
    table = build_monthly_negative_pct_table(price_hourly, years, mode=mode)
    if table.empty:
        return None
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    label = price_event_label(mode)
    chart = alt.Chart(table).mark_bar().encode(
        x=alt.X("Month:N", sort=month_order, title=None),
        y=alt.Y("pct_event:Q", title=f"% {label} hours", axis=alt.Axis(format=".0%")),
        color=alt.Color("Year:N", title="Year"),
        xOffset="Year:N",
        tooltip=[
            "Year", "Month",
            alt.Tooltip("event_hours:Q", title=f"{label.title()} hours", format=",.0f"),
            alt.Tooltip("total_hours:Q", title="Total hours", format=",.0f"),
            alt.Tooltip("pct_event:Q", title=f"% {label} hours", format=".1%"),
        ],
    ).properties(height=320, title=f"Monthly {label} frequency")
    return chart


def build_monthly_curtailment_chart(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, years: list[int], mode: str = "Zero and negative prices"):
    rows = []
    for year in years:
        tmp_p = ensure_datetime_col(price_hourly, "datetime")
        tmp_s = ensure_datetime_col(solar_hourly, "datetime")
        tmp_p = tmp_p[tmp_p["datetime"].dt.year == int(year)].copy()
        tmp_s = tmp_s[tmp_s["datetime"].dt.year == int(year)].copy()
        if tmp_p.empty or tmp_s.empty:
            continue
        tmp = tmp_p.merge(tmp_s[["datetime", "solar_best_mw"]], on="datetime", how="left")
        tmp["solar_best_mw"] = tmp["solar_best_mw"].fillna(0.0)
        tmp = tmp[tmp["solar_best_mw"] > 0].copy()
        if tmp.empty:
            continue
        tmp["month_name"] = tmp["datetime"].dt.strftime("%b")
        tmp["month_num"] = tmp["datetime"].dt.month
        tmp["is_event"] = price_event_mask(tmp["price"], mode)
        total = tmp.groupby(["month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "total_p48"})
        aff = tmp[tmp["is_event"]].groupby(["month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "affected_p48"})
        out = total.merge(aff, on=["month_num", "month_name"], how="left")
        out["affected_p48"] = out["affected_p48"].fillna(0.0)
        out["pct_curtailment"] = out["affected_p48"] / out["total_p48"]
        out["Year"] = year
        rows.append(out)
    if not rows:
        return None, pd.DataFrame()
    plot = pd.concat(rows, ignore_index=True)
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    label = price_event_label(mode)
    chart = alt.Chart(plot).mark_bar().encode(
        x=alt.X("month_name:N", sort=month_order, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("pct_curtailment:Q", title="Economic curtailment", axis=alt.Axis(format=".0%")),
        color=alt.Color("Year:N", title="Year"),
        xOffset="Year:N",
        tooltip=[
            "Year", alt.Tooltip("month_name:N", title="Month"),
            alt.Tooltip("affected_p48:Q", title=f"Solar P48 at {label} hours", format=",.0f"),
            alt.Tooltip("total_p48:Q", title="Total solar P48", format=",.0f"),
            alt.Tooltip("pct_curtailment:Q", title="Economic curtailment", format=".1%"),
        ],
    ).properties(height=320, title=f"Monthly economic curtailment based on {label} hours")
    return chart, plot


def build_spot_capture_evolution_chart(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, year: int):
    tmp_p = price_hourly[price_hourly["datetime"].dt.year == year].copy()
    if tmp_p.empty:
        return None, pd.DataFrame()
    rows = []
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    for m in range(1, 13):
        d0 = date(year, m, 1)
        d1 = (pd.Timestamp(d0) + pd.offsets.MonthEnd(0)).date()
        metrics = compute_period_metrics(price_hourly, solar_hourly, d0, d1)
        rows.append({"Month": pd.Timestamp(d0).strftime("%b"), "Series": "Average spot price", "value": metrics.get("avg_price")})
        rows.append({"Month": pd.Timestamp(d0).strftime("%b"), "Series": "Captured solar (uncurtailed)", "value": metrics.get("captured_uncurtailed")})
        rows.append({"Month": pd.Timestamp(d0).strftime("%b"), "Series": "Captured solar (curtailed)", "value": metrics.get("captured_curtailed")})
    plot = pd.DataFrame(rows).dropna(subset=["value"])
    if plot.empty:
        return None, pd.DataFrame()
    color_scale = alt.Scale(
        domain=["Average spot price", "Captured solar (uncurtailed)", "Captured solar (curtailed)"],
        range=["#2563EB", "#FBBF24", "#D97706"],
    )
    dash_scale = alt.Scale(
        domain=["Average spot price", "Captured solar (uncurtailed)", "Captured solar (curtailed)"],
        range=[[1,0], [2,2], [6,4]],
    )
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("Month:N", sort=month_order, title=None),
        y=alt.Y("value:Q", title="€/MWh"),
        color=alt.Color("Series:N", title=None, scale=color_scale),
        strokeDash=alt.StrokeDash("Series:N", title=None, scale=dash_scale),
        tooltip=["Series", "Month", alt.Tooltip("value:Q", format=".2f")],
    ).properties(height=320, title=f"Spot and solar capture evolution ({year})")
    return chart, plot


def build_monthly_comparison_table(price_hourly: pd.DataFrame, historical_best_solar: pd.DataFrame, primary_month: pd.Timestamp, compare_month: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for label, month_ts in [("Primary", primary_month), ("Comparison", compare_month)]:
        d0, d1 = month_start_end(month_ts)
        metrics = compute_period_metrics(price_hourly, historical_best_solar, d0, d1)
        p = price_hourly[(price_hourly["datetime"].dt.date >= d0) & (price_hourly["datetime"].dt.date <= d1)].copy()
        s = historical_best_solar[(historical_best_solar["datetime"].dt.date >= d0) & (historical_best_solar["datetime"].dt.date <= d1)].copy()
        merged = p.merge(s[["datetime", "solar_best_mw"]], on="datetime", how="left")
        merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
        zero_neg_hours = int((p["price"] <= 0).sum()) if not p.empty else 0
        solar_zero_neg_hours = int(((merged["price"] <= 0) & (merged["solar_best_mw"] > 0)).sum()) if not merged.empty else 0
        rows.append({
            "Label": label,
            "Month": month_ts.strftime("%b-%Y"),
            "Average price (€/MWh)": metrics.get("avg_price"),
            "Captured solar (€/MWh)": metrics.get("captured"),
            "Solar capture rate (%)": metrics.get("capture_pct"),
            "Zero / negative hours": zero_neg_hours,
            "Zero / negative hours during solar": solar_zero_neg_hours,
        })
    return pd.DataFrame(rows)


def build_monthly_avg_profile_chart(price_hourly: pd.DataFrame, primary_month: pd.Timestamp, compare_month: pd.Timestamp):
    rows = []
    for label, month_ts in [(primary_month.strftime("%b-%Y"), primary_month), (compare_month.strftime("%b-%Y"), compare_month)]:
        d0, d1 = month_start_end(month_ts)
        p = price_hourly[(price_hourly["datetime"].dt.date >= d0) & (price_hourly["datetime"].dt.date <= d1)].copy()
        if p.empty:
            continue
        p["Hour"] = p["datetime"].dt.hour
        g = p.groupby("Hour", as_index=False)["price"].mean()
        g["Series"] = label
        rows.append(g)
    if not rows:
        return None
    plot = pd.concat(rows, ignore_index=True)
    return alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("Hour:O", title="Hour"),
        y=alt.Y("price:Q", title="Average price (€/MWh)"),
        color=alt.Color("Series:N", title=None),
        tooltip=["Series", "Hour", alt.Tooltip("price:Q", format=".2f")],
    ).properties(height=320)


def build_monthly_mix_compare_chart(ree_daily_df: pd.DataFrame, primary_month: pd.Timestamp, compare_month: pd.Timestamp):
    if ree_daily_df.empty:
        return None, pd.DataFrame()
    rows = []
    for label, month_ts in [(primary_month.strftime("%b-%Y"), primary_month), (compare_month.strftime("%b-%Y"), compare_month)]:
        d0, d1 = month_start_end(month_ts)
        tmp = ree_daily_df[(ree_daily_df["datetime"].dt.date >= d0) & (ree_daily_df["datetime"].dt.date <= d1)].copy()
        if tmp.empty:
            continue
        g = tmp.groupby("technology", as_index=False)["value_gwh"].sum()
        g["MonthLabel"] = label
        rows.append(g)
    if not rows:
        return None, pd.DataFrame()
    plot = pd.concat(rows, ignore_index=True)
    chart = alt.Chart(plot).mark_bar().encode(
        x=alt.X("MonthLabel:N", title=None),
        y=alt.Y("value_gwh:Q", title="Generation (GWh)"),
        color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
        tooltip=["MonthLabel", "technology", alt.Tooltip("value_gwh:Q", title="Generation (GWh)", format=",.1f")],
    ).properties(height=340)
    return chart, plot


# =========================================================
# MULTI-PERIOD REPORT HELPERS
# =========================================================
def fmt_eur(value) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{float(value):,.2f} €/MWh"


def fmt_num(value, suffix: str = "", decimals: int = 2) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{float(value):,.{decimals}f}{suffix}"


def pct_change(current, previous):
    if current is None or previous is None or pd.isna(current) or pd.isna(previous) or float(previous) == 0:
        return None
    return (float(current) / float(previous)) - 1.0


def period_label(granularity: str, start_d: date, end_d: date) -> str:
    if granularity == "Daily":
        return pd.Timestamp(start_d).strftime("%d-%b-%Y")
    if granularity == "Weekly":
        iso = pd.Timestamp(start_d).isocalendar()
        return f"{int(iso.year)}-W{int(iso.week):02d} ({pd.Timestamp(start_d).strftime('%d-%b')} to {pd.Timestamp(end_d).strftime('%d-%b')})"
    if granularity == "Monthly":
        return pd.Timestamp(start_d).strftime("%b-%Y")
    if granularity == "Quarterly":
        q = ((start_d.month - 1) // 3) + 1
        return f"Q{q}-{start_d.year}"
    return str(start_d.year)


def start_end_from_anchor(anchor: date, granularity: str) -> tuple[date, date]:
    ts = pd.Timestamp(anchor)
    if granularity == "Daily":
        return ts.date(), ts.date()
    if granularity == "Weekly":
        start = (ts - pd.Timedelta(days=int(ts.weekday()))).date()
        end = start + timedelta(days=6)
        return start, end
    if granularity == "Monthly":
        start = ts.to_period("M").to_timestamp().date()
        end = (ts.to_period("M").to_timestamp() + pd.offsets.MonthEnd(0)).date()
        return start, end
    if granularity == "Quarterly":
        start = ts.to_period("Q").to_timestamp().date()
        end = ts.to_period("Q").end_time.date()
        return start, end
    start = date(ts.year, 1, 1)
    end = date(ts.year, 12, 31)
    return start, end


def prior_year_same_period(start_d: date, end_d: date, granularity: str) -> tuple[date, date]:
    if granularity == "Weekly":
        iso = pd.Timestamp(start_d).isocalendar()
        target_year = int(iso.year) - 1
        week = int(iso.week)
        try:
            start = date.fromisocalendar(target_year, week, 1)
        except ValueError:
            # Some ISO years do not have week 53. Fall back to the last available week.
            start = date.fromisocalendar(target_year, 52, 1)
        return start, start + timedelta(days=6)

    if granularity == "Daily":
        try:
            start = start_d.replace(year=start_d.year - 1)
        except ValueError:
            start = start_d.replace(year=start_d.year - 1, day=28)
        return start, start

    if granularity == "Monthly":
        start = date(start_d.year - 1, start_d.month, 1)
        end = (pd.Timestamp(start) + pd.offsets.MonthEnd(0)).date()
        return start, end

    if granularity == "Quarterly":
        start = date(start_d.year - 1, start_d.month, 1)
        return start_end_from_anchor(start, "Quarterly")

    return date(start_d.year - 1, 1, 1), date(start_d.year - 1, 12, 31)


def previous_period(start_d: date, end_d: date, granularity: str) -> tuple[date, date]:
    if granularity == "Daily":
        prev = start_d - timedelta(days=1)
        return prev, prev
    if granularity == "Weekly":
        start = start_d - timedelta(days=7)
        return start, start + timedelta(days=6)
    if granularity == "Monthly":
        start = (pd.Timestamp(start_d) - pd.offsets.MonthBegin(1)).date()
        end = (pd.Timestamp(start) + pd.offsets.MonthEnd(0)).date()
        return start, end
    if granularity == "Quarterly":
        start = (pd.Timestamp(start_d) - pd.offsets.QuarterBegin(startingMonth=1)).date()
        return start_end_from_anchor(start, "Quarterly")
    return date(start_d.year - 1, 1, 1), date(start_d.year - 1, 12, 31)


def build_available_period_options(price_hourly: pd.DataFrame, granularity: str) -> list[dict]:
    if price_hourly.empty:
        return []
    tmp = ensure_datetime_col(price_hourly, "datetime")
    if tmp.empty:
        return []
    dates = tmp["datetime"].dt.date.drop_duplicates().sort_values().tolist()
    anchors = []
    seen = set()
    for d in dates:
        start_d, end_d = start_end_from_anchor(d, granularity)
        key = (start_d, end_d)
        if key not in seen:
            seen.add(key)
            anchors.append({"start": start_d, "end": end_d, "label": period_label(granularity, start_d, end_d)})
    return anchors


def clip_period_to_available(price_hourly: pd.DataFrame, start_d: date, end_d: date) -> tuple[date, date]:
    if price_hourly.empty:
        return start_d, end_d
    min_d = price_hourly["datetime"].dt.date.min()
    max_d = price_hourly["datetime"].dt.date.max()
    return max(start_d, min_d), min(end_d, max_d)


def ensure_recent_price_days(price_hourly: pd.DataFrame, start_d: date, end_d: date, token: str | None) -> pd.DataFrame:
    """Append missing recent day-ahead prices on demand for selected/reporting periods."""
    if not token:
        return price_hourly
    out = ensure_datetime_col(price_hourly, "datetime")
    max_live = max_refresh_day_from_clock()
    d = start_d
    added = []
    while d <= end_d and d <= max_live:
        has_day = not out[out["datetime"].dt.date == d].empty
        if not has_day:
            live = fetch_live_hourly_prices_for_day(d, token)
            if not live.empty:
                added.append(live)
        d += timedelta(days=1)
    if not added:
        return out
    out = pd.concat([out] + added, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime").reset_index(drop=True)
    return out


def extend_price_history_to_latest(price_hourly: pd.DataFrame, token: str | None) -> pd.DataFrame:
    """Preload recent live day-ahead prices so Weekly/Monthly selectors include the latest periods."""
    out = ensure_datetime_col(price_hourly, "datetime")
    if out.empty or not token:
        return out
    start_d = out["datetime"].dt.date.max() + timedelta(days=1)
    end_d = max_refresh_day_from_clock()
    if start_d > end_d:
        return out
    return ensure_recent_price_days(out, start_d, end_d, token)


def ensure_recent_best_solar_days(
    solar_p48_hourly: pd.DataFrame,
    solar_forecast_hourly: pd.DataFrame,
    historical_best_solar: pd.DataFrame,
    start_d: date,
    end_d: date,
    token: str | None,
) -> pd.DataFrame:
    """Append best-available solar profiles for recent missing days so capture metrics work beyond the local cache."""
    out = ensure_datetime_col(historical_best_solar, "datetime")
    solar_p48_hourly = ensure_datetime_col(solar_p48_hourly, "datetime")
    solar_forecast_hourly = ensure_datetime_col(solar_forecast_hourly, "datetime")
    d = start_d
    extra = []
    while d <= end_d:
        has_day = not out[out["datetime"].dt.date == d].empty
        if not has_day:
            live_best = build_solar_profile_for_report_day(solar_p48_hourly, solar_forecast_hourly, d, token)
            if not live_best.empty:
                extra.append(live_best[["datetime", "solar_best_mw", "solar_source"]].copy())
        d += timedelta(days=1)
    if not extra:
        return out
    out = pd.concat([out] + extra, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime").reset_index(drop=True)
    return out


def build_capture_period_chart(day_ahead_df: pd.DataFrame):
    if day_ahead_df.empty:
        return None
    cols = [
        "Captured solar uncurtailed (€/MWh)",
        "Captured solar curtailed (€/MWh)",
        "Average spot (€/MWh)",
    ]
    plot = day_ahead_df[["Period"] + [c for c in cols if c in day_ahead_df.columns]].copy()
    plot = plot.melt(id_vars=["Period"], var_name="Metric", value_name="€/MWh").dropna()
    if plot.empty:
        return None
    return alt.Chart(plot).mark_bar().encode(
        x=alt.X("Period:N", title=None, sort=day_ahead_df["Period"].tolist()),
        y=alt.Y("€/MWh:Q", title="€/MWh"),
        xOffset="Metric:N",
        color=alt.Color("Metric:N", title=None),
        tooltip=["Period", "Metric", alt.Tooltip("€/MWh:Q", format=",.2f")],
    ).properties(height=300, title="Spot and captured-price comparison")


def build_bess_revenue_comparison_chart(bess_df: pd.DataFrame):
    if bess_df.empty:
        return None
    plot = bess_df.dropna(subset=["Revenue BESS (€)"]).copy()
    if plot.empty:
        return None
    return alt.Chart(plot).mark_bar().encode(
        x=alt.X("Mode:N", title=None, sort=["Standalone BESS", "BESS with demand", "BESS without demand"]),
        y=alt.Y("Revenue BESS (€):Q", title="Revenue (€)"),
        color=alt.Color("Period:N", title=None),
        xOffset="Period:N",
        tooltip=["Mode", "Period", alt.Tooltip("Revenue BESS (€):Q", format=",.2f"), alt.Tooltip("Revenue BESS (€/MW):Q", format=",.2f")],
    ).properties(height=330, title="BESS revenue comparison by mode and period")


def build_period_day_ahead_row(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, label: str, start_d: date, end_d: date, mode: str) -> dict:
    metrics = compute_period_metrics(price_hourly, solar_hourly, start_d, end_d)
    tmp_p = price_hourly[
        (price_hourly["datetime"].dt.date >= start_d)
        & (price_hourly["datetime"].dt.date <= end_d)
    ].copy()
    tmp_s = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d)
        & (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()
    merged = tmp_p.merge(tmp_s[["datetime", "solar_best_mw"]], on="datetime", how="left")
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0) if not merged.empty else pd.Series(dtype=float)
    event_hours = int(price_event_mask(tmp_p["price"], mode).sum()) if not tmp_p.empty else 0
    event_hours_solar = int((price_event_mask(merged["price"], mode) & (merged["solar_best_mw"] > 0)).sum()) if not merged.empty else 0
    return {
        "Period": label,
        "Start": start_d,
        "End": end_d,
        "Average spot (€/MWh)": metrics.get("avg_price"),
        "Captured solar uncurtailed (€/MWh)": metrics.get("captured_uncurtailed"),
        "Captured solar curtailed (€/MWh)": metrics.get("captured_curtailed"),
        "Solar capture rate uncurtailed (%)": metrics.get("capture_pct_uncurtailed"),
        "Solar capture rate curtailed (%)": metrics.get("capture_pct_curtailed"),
        f"{price_event_label(mode).title()} hours": event_hours,
        f"{price_event_label(mode).title()} hours during solar": event_hours_solar,
    }


def build_period_day_ahead_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, period_specs: list[tuple[str, date, date]], mode: str) -> pd.DataFrame:
    return pd.DataFrame([
        build_period_day_ahead_row(price_hourly, solar_hourly, label, s, e, mode)
        for label, s, e in period_specs
    ])


def build_ytd_negative_table(price_hourly: pd.DataFrame, selected_end: date, mode: str) -> pd.DataFrame:
    selected_year = selected_end.year
    prev_year = selected_year - 1
    selected_start = date(selected_year, 1, 1)
    try:
        prev_cut = selected_end.replace(year=prev_year)
    except ValueError:
        prev_cut = selected_end.replace(year=prev_year, day=28)
    rows = []
    for label, s, e in [
        (f"YTD {selected_year}", selected_start, selected_end),
        (f"YTD {prev_year} to same date", date(prev_year, 1, 1), prev_cut),
    ]:
        tmp = price_hourly[
            (price_hourly["datetime"].dt.date >= s)
            & (price_hourly["datetime"].dt.date <= e)
        ].copy()
        event_hours = int(price_event_mask(tmp["price"], mode).sum()) if not tmp.empty else 0
        total_hours = int(tmp["price"].count()) if not tmp.empty else 0
        rows.append({
            "Period": label,
            "Start": s,
            "End": e,
            f"{price_event_label(mode).title()} hours": event_hours,
            "Total hours": total_hours,
            f"% {price_event_label(mode)} hours": (event_hours / total_hours) if total_hours else None,
        })
    return pd.DataFrame(rows)


def build_period_mix_table(ree_daily_df: pd.DataFrame, period_specs: list[tuple[str, date, date]]) -> pd.DataFrame:
    if ree_daily_df.empty:
        return pd.DataFrame()
    tmp = ensure_datetime_col(ree_daily_df, "datetime")
    rows = []
    for label, s, e in period_specs:
        df = tmp[(tmp["datetime"].dt.date >= s) & (tmp["datetime"].dt.date <= e)].copy()
        if df.empty:
            rows.append({"Period": label, "Solar PV (GWh)": None, "Wind (GWh)": None, "Hydro (GWh)": None, "Nuclear (GWh)": None, "Renewables share (%)": None})
            continue
        g = df.groupby("technology", as_index=False)["value_gwh"].sum()
        vals = {r["technology"]: float(r["value_gwh"]) for _, r in g.iterrows()}
        total = float(g["value_gwh"].sum())
        renew = float(g[g["technology"].isin(RENEWABLE_TECHS)]["value_gwh"].sum())
        rows.append({
            "Period": label,
            "Solar PV (GWh)": vals.get("Solar PV"),
            "Wind (GWh)": vals.get("Wind"),
            "Hydro (GWh)": vals.get("Hydro"),
            "Nuclear (GWh)": vals.get("Nuclear"),
            "Renewables share (%)": (renew / total) if total > 0 else None,
        })
    return pd.DataFrame(rows)


def build_period_mix_chart(mix_table: pd.DataFrame):
    if mix_table.empty:
        return None
    value_cols = ["Solar PV (GWh)", "Wind (GWh)", "Hydro (GWh)", "Nuclear (GWh)"]
    available = [c for c in value_cols if c in mix_table.columns]
    plot = mix_table.melt(id_vars=["Period"], value_vars=available, var_name="Technology", value_name="Generation (GWh)").dropna()
    if plot.empty:
        return None
    return alt.Chart(plot).mark_bar().encode(
        x=alt.X("Period:N", title=None, sort=mix_table["Period"].tolist()),
        y=alt.Y("Generation (GWh):Q", title="Generation (GWh)"),
        color=alt.Color("Technology:N", title=None),
        xOffset="Technology:N",
        tooltip=["Period", "Technology", alt.Tooltip("Generation (GWh):Q", format=",.1f")],
    ).properties(height=320, title="Generation comparison for selected technologies")


def build_period_mix_delta_table(mix_table: pd.DataFrame) -> pd.DataFrame:
    """
    Build explicit deltas for the selected period versus:
    - previous period
    - same period prior year

    The existing absolute mix table is useful, but the user asked for a true difference view.
    """
    if mix_table.empty or len(mix_table) < 2:
        return pd.DataFrame()

    selected = mix_table.iloc[0]
    comparisons = []
    value_cols = [
        "Solar PV (GWh)",
        "Wind (GWh)",
        "Hydro (GWh)",
        "Nuclear (GWh)",
        "Renewables share (%)",
    ]

    for idx in range(1, len(mix_table)):
        comp = mix_table.iloc[idx]
        comp_label = str(comp.get("Period", f"Comparison {idx}"))
        for col in value_cols:
            cur = selected.get(col)
            ref = comp.get(col)
            delta = None if pd.isna(cur) or pd.isna(ref) else float(cur) - float(ref)
            rel = None
            if pd.notna(cur) and pd.notna(ref) and float(ref) != 0:
                rel = float(cur) / float(ref) - 1.0
            comparisons.append(
                {
                    "Comparison": comp_label,
                    "Metric": col,
                    "Selected value": cur,
                    "Reference value": ref,
                    "Δ absolute": delta,
                    "Δ %": rel,
                }
            )
    return pd.DataFrame(comparisons)


def build_period_mix_delta_chart(mix_delta_table: pd.DataFrame):
    if mix_delta_table.empty:
        return None
    plot = mix_delta_table.copy()
    plot = plot[plot["Metric"] != "Renewables share (%)"].dropna(subset=["Δ absolute"])
    if plot.empty:
        return None
    return (
        alt.Chart(plot)
        .mark_bar()
        .encode(
            x=alt.X("Metric:N", title=None),
            y=alt.Y("Δ absolute:Q", title="Δ GWh"),
            color=alt.Color("Comparison:N", title=None),
            xOffset="Comparison:N",
            tooltip=[
                "Comparison",
                "Metric",
                alt.Tooltip("Selected value:Q", format=",.1f"),
                alt.Tooltip("Reference value:Q", format=",.1f"),
                alt.Tooltip("Δ absolute:Q", format="+,.1f"),
                alt.Tooltip("Δ %:Q", format="+.1%"),
            ],
        )
        .properties(height=320, title="Energy mix differences vs previous period and prior year")
    )


def build_renewables_share_delta_chart(mix_delta_table: pd.DataFrame):
    if mix_delta_table.empty:
        return None
    plot = mix_delta_table[mix_delta_table["Metric"] == "Renewables share (%)"].copy()
    plot = plot.dropna(subset=["Δ absolute"])
    if plot.empty:
        return None
    # Share is stored as decimal: show percentage-point delta.
    plot["Δ p.p."] = plot["Δ absolute"] * 100.0
    return (
        alt.Chart(plot)
        .mark_bar()
        .encode(
            x=alt.X("Comparison:N", title=None),
            y=alt.Y("Δ p.p.:Q", title="Δ renewable share (p.p.)"),
            color=alt.Color("Comparison:N", legend=None),
            tooltip=[
                "Comparison",
                alt.Tooltip("Selected value:Q", title="Selected RE share", format=".1%"),
                alt.Tooltip("Reference value:Q", title="Reference RE share", format=".1%"),
                alt.Tooltip("Δ p.p.:Q", title="Δ p.p.", format="+.1f"),
            ],
        )
        .properties(height=260, title="Renewables share delta")
    )


def _tb_hourly_for_day(price_hourly: pd.DataFrame, d: date) -> pd.DataFrame:
    tmp = price_hourly[price_hourly["datetime"].dt.date == d].copy()
    if tmp.empty:
        return pd.DataFrame()
    tmp = tmp.rename(columns={"price": "Price (€/MWh)"})
    return tmp[["datetime", "Price (€/MWh)"]].copy()


@st.cache_data(show_spinner=False, ttl=3600)
def build_daily_tb_cache(price_csv_key: str, price_hourly_json: str) -> pd.DataFrame:
    """Compute daily TB4/TB2 spread cache for the price history supplied by the app."""
    price_hourly = pd.read_json(StringIO(price_hourly_json), orient="split")
    price_hourly["datetime"] = pd.to_datetime(price_hourly["datetime"], errors="coerce")
    rows = []
    for d in sorted(price_hourly["datetime"].dt.date.dropna().unique().tolist()):
        h = _tb_hourly_for_day(price_hourly, d)
        rows.append({
            "Date": d,
            "TB4 spread (€/MWh)": compute_bess_tb_spread(h, capacity_mwh=1.0, c_rate=0.25, eta_ch=1.0, eta_dis=1.0),
            "TB2 spread (€/MWh)": compute_bess_tb_spread(h, capacity_mwh=1.0, c_rate=0.50, eta_ch=1.0, eta_dis=1.0),
        })
    return pd.DataFrame(rows)


def build_period_tb_table(price_hourly: pd.DataFrame, period_specs: list[tuple[str, date, date]]) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame()
    tb_daily = build_daily_tb_cache(
        str(price_hourly["datetime"].max()),
        price_hourly[["datetime", "price"]].to_json(orient="split", date_format="iso"),
    )
    if tb_daily.empty:
        return pd.DataFrame()
    tb_daily["Date"] = pd.to_datetime(tb_daily["Date"], errors="coerce").dt.date
    rows = []
    for label, s, e in period_specs:
        tmp = tb_daily[(tb_daily["Date"] >= s) & (tb_daily["Date"] <= e)].copy()
        rows.append({
            "Period": label,
            "TB4 average spread (€/MWh)": tmp["TB4 spread (€/MWh)"].mean() if not tmp.empty else None,
            "TB2 average spread (€/MWh)": tmp["TB2 spread (€/MWh)"].mean() if not tmp.empty else None,
            "Days with spread data": int(tmp.dropna(subset=["TB4 spread (€/MWh)", "TB2 spread (€/MWh)"], how="all").shape[0]) if not tmp.empty else 0,
        })
    return pd.DataFrame(rows)


def load_omip_cache_for_report() -> pd.DataFrame:
    path = DATA_DIR / "omip_ES_EL_date_range2025_20260511.xlsx"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(path, sheet_name="All curves")
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    if "market_date" not in df.columns or "curve_price" not in df.columns:
        return pd.DataFrame()
    df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce").dt.date
    df["curve_price"] = pd.to_numeric(df["curve_price"], errors="coerce")
    if "maturity" in df.columns:
        year_df = df[df["maturity"].astype(str).str.upper().eq("YR")].copy()
        if not year_df.empty:
            df = year_df
    if "sheet" not in df.columns:
        df["sheet"] = "Forward basket"
    return df.dropna(subset=["market_date", "curve_price"]).copy()


def build_forward_summary_table(forward_df: pd.DataFrame, selected_spec: tuple[str, date, date], previous_spec: tuple[str, date, date]) -> pd.DataFrame:
    if forward_df.empty:
        return pd.DataFrame()
    sel_label, s0, e0 = selected_spec
    prev_label, s1, e1 = previous_spec
    rows = []
    sheets = sorted(forward_df["sheet"].dropna().astype(str).unique().tolist()) if "sheet" in forward_df.columns else ["Forward basket"]
    for sheet in sheets:
        x = forward_df[forward_df["sheet"].astype(str) == sheet] if "sheet" in forward_df.columns else forward_df
        a = x[(x["market_date"] >= s0) & (x["market_date"] <= e0)]["curve_price"].mean()
        b = x[(x["market_date"] >= s1) & (x["market_date"] <= e1)]["curve_price"].mean()
        rows.append({
            "Curve basket": sheet,
            f"{sel_label} avg (€/MWh)": a,
            f"{prev_label} avg (€/MWh)": b,
            "Δ €/MWh": (a - b) if pd.notna(a) and pd.notna(b) else None,
            "Δ %": pct_change(a, b),
            "Direction": "Up" if pd.notna(a) and pd.notna(b) and a > b else "Down" if pd.notna(a) and pd.notna(b) and a < b else "Flat / n.a.",
        })
    return pd.DataFrame(rows)


def normalise_mibgas_col(col: str) -> str:
    s = str(col).strip().lower().replace("\xa0", " ").replace("\n", " ")
    repl = {"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","[":"","]":"","(":"",")":"","%":"pct","/":"_","-":"_",".":"_"}
    for a, b in repl.items():
        s = s.replace(a, b)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def to_mibgas_number(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    s = series.astype(str).str.strip().str.replace("€", "", regex=False).str.replace(" ", "", regex=False)
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def first_existing_col(df: pd.DataFrame, names: list[str]) -> str | None:
    cols = set(df.columns)
    for name in names:
        n = normalise_mibgas_col(name)
        if n in cols:
            return n
    return None


def standardise_mibgas_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [normalise_mibgas_col(c) for c in out.columns]
    trading = first_existing_col(out, ["Trading day", "trading_day"])
    product = first_existing_col(out, ["Product", "product"])
    area = first_existing_col(out, ["Area", "area"])
    delivery = first_existing_col(out, ["First Day Delivery", "first_day_delivery", "delivery_start"])
    ref = first_existing_col(out, ["Reference Price [EUR/MWh]", "Daily Reference Price [EUR/MWh]", "reference_price_eur_mwh"])
    if trading is None or product is None or ref is None:
        return pd.DataFrame()
    ret = pd.DataFrame()
    ret["trading_day"] = pd.to_datetime(out[trading], dayfirst=True, errors="coerce")
    ret["product"] = out[product].astype(str).str.strip()
    ret["area"] = out[area].astype(str).str.strip() if area else "ES"
    ret["delivery_day"] = pd.to_datetime(out[delivery], dayfirst=True, errors="coerce") if delivery else ret["trading_day"]
    ret["price"] = to_mibgas_number(out[ref])
    ret = ret.dropna(subset=["trading_day", "price"])
    ret = ret[(ret["product"] == "GDAES_D+1") & (ret["area"].fillna("ES") == "ES")].copy()
    ret["date"] = ret["delivery_day"].combine_first(ret["trading_day"]).dt.date
    return ret[["date", "price"]].dropna().drop_duplicates(subset=["date"], keep="last")


def load_mibgas_actuals_for_report() -> pd.DataFrame:
    parts = []
    for path in sorted(DATA_DIR.glob("MIBGAS_Data_*.xlsx")):
        try:
            raw = pd.read_excel(path, sheet_name="Trading Data PVB&VTP")
            std = standardise_mibgas_frame(raw)
            if not std.empty:
                parts.append(std)
        except Exception:
            continue
    cache = DATA_DIR / "mibgas_2026_cache.csv"
    if cache.exists():
        try:
            cached = pd.read_csv(cache)
            # The MIBGAS tab stores a normalised cache. Re-map directly when available.
            if {"product", "area", "trading_day", "reference_price_eur_mwh"}.issubset(set(cached.columns)):
                c = cached.copy()
                c["trading_day"] = pd.to_datetime(c["trading_day"], errors="coerce")
                c["delivery_start"] = pd.to_datetime(c.get("delivery_start"), errors="coerce")
                c["date"] = c["delivery_start"].combine_first(c["trading_day"]).dt.date
                c["price"] = pd.to_numeric(c["reference_price_eur_mwh"], errors="coerce")
                c = c[(c["product"] == "GDAES_D+1") & (c["area"].fillna("ES") == "ES")].copy()
                parts.append(c[["date", "price"]].dropna())
        except Exception:
            pass
    if not parts:
        return pd.DataFrame(columns=["date", "price"])
    out = pd.concat(parts, ignore_index=True)
    return out.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)


def build_mibgas_summary_table(mibgas_df: pd.DataFrame, period_specs: list[tuple[str, date, date]]) -> pd.DataFrame:
    rows = []
    for label, s, e in period_specs:
        tmp = mibgas_df[(mibgas_df["date"] >= s) & (mibgas_df["date"] <= e)].copy() if not mibgas_df.empty else pd.DataFrame()
        rows.append({
            "Period": label,
            "MIBGAS GDAES D+1 average (€/MWh)": tmp["price"].mean() if not tmp.empty else None,
            "Days with gas data": int(tmp.shape[0]) if not tmp.empty else 0,
        })
    return pd.DataFrame(rows)


def make_metric_kpis(
    day_ahead_df: pd.DataFrame,
    mibgas_df: pd.DataFrame,
    tb_df: pd.DataFrame,
    forward_df: pd.DataFrame,
    mix_df: pd.DataFrame | None = None,
    bess_revenue_df: pd.DataFrame | None = None,
) -> list[tuple[str, str]]:
    items = []
    if not day_ahead_df.empty:
        sel = day_ahead_df.iloc[0]
        prev = day_ahead_df.iloc[1] if len(day_ahead_df) > 1 else None
        yoy = day_ahead_df.iloc[2] if len(day_ahead_df) > 2 else None

        items.append(("⚡ Baseload avg", fmt_eur(sel.get("Average spot (€/MWh)"))))
        if prev is not None:
            ch = pct_change(sel.get("Average spot (€/MWh)"), prev.get("Average spot (€/MWh)"))
            items.append(("↔ Spot vs previous", "n/a" if ch is None else f"{ch:+.1%}"))
        if yoy is not None:
            ch = pct_change(sel.get("Average spot (€/MWh)"), yoy.get("Average spot (€/MWh)"))
            items.append(("🕘 Spot vs YoY", "n/a" if ch is None else f"{ch:+.1%}"))

        items.append(("☀️ Captured uncurtailed", fmt_eur(sel.get("Captured solar uncurtailed (€/MWh)"))))
        items.append(("☀️ Captured curtailed", fmt_eur(sel.get("Captured solar curtailed (€/MWh)"))))

    if not mibgas_df.empty:
        sel = mibgas_df.iloc[0]
        items.append(("🔥 MIBGAS avg", fmt_eur(sel.get("MIBGAS GDAES D+1 average (€/MWh)"))))
        if len(mibgas_df) > 1:
            ch = pct_change(sel.get("MIBGAS GDAES D+1 average (€/MWh)"), mibgas_df.iloc[1].get("MIBGAS GDAES D+1 average (€/MWh)"))
            items.append(("↔ MIBGAS vs previous", "n/a" if ch is None else f"{ch:+.1%}"))

    if not tb_df.empty:
        items.append(("🔋 TB4 spread", fmt_eur(tb_df.iloc[0].get("TB4 average spread (€/MWh)"))))
        items.append(("🔋 TB2 spread", fmt_eur(tb_df.iloc[0].get("TB2 average spread (€/MWh)"))))

    if forward_df is not None and not forward_df.empty:
        dirs = sorted(forward_df["Direction"].dropna().astype(str).unique().tolist())
        items.append(("📈 Forward curve", ", ".join(dirs) if dirs else "n/a"))
        delta = forward_df["Δ €/MWh"].mean() if "Δ €/MWh" in forward_df.columns else None
        if pd.notna(delta):
            items.append(("📊 Forward avg move", f"{float(delta):+,.2f} €/MWh"))

    if mix_df is not None and not mix_df.empty:
        items.append(("🌿 Renewable share", fmt_num(100 * mix_df.iloc[0].get("Renewables share (%)"), suffix="%", decimals=1)))
        items.append(("💨 Wind output", fmt_num(mix_df.iloc[0].get("Wind (GWh)"), suffix=" GWh", decimals=1)))

    if bess_revenue_df is not None and not bess_revenue_df.empty:
        st_row = bess_revenue_df[bess_revenue_df["Mode"].eq("Standalone BESS")].head(1)
        wd_row = bess_revenue_df[bess_revenue_df["Mode"].eq("BESS with demand")].head(1)
        if not st_row.empty:
            items.append(("💰 Standalone BESS", fmt_num(st_row.iloc[0].get("Revenue BESS (€)"), suffix=" €", decimals=0)))
        if not wd_row.empty:
            items.append(("🏭 BESS with demand", fmt_num(wd_row.iloc[0].get("Revenue BESS (€)"), suffix=" €", decimals=0)))
    return items



# =========================================================
# BESS PERIOD REVENUES (REPORT VERSION)
# =========================================================
def load_bess_default_solar_profile_for_report() -> pd.DataFrame:
    if not BESS_DEFAULT_SOLAR_PROFILE_PATH.exists():
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    try:
        df = pd.read_excel(BESS_DEFAULT_SOLAR_PROFILE_PATH)
    except Exception:
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    if df.empty:
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    col_map = {str(c).lower().strip(): c for c in df.columns}
    gen_col = next((col_map[c] for c in ["generation", "generacion", "gen"] if c in col_map), None)
    if gen_col is None:
        return pd.DataFrame(columns=["hour_of_year", "generation"])
    out = pd.DataFrame({"generation": pd.to_numeric(df[gen_col], errors="coerce").fillna(0.0)})
    out["hour_of_year"] = np.arange(1, len(out) + 1)
    return out[["hour_of_year", "generation"]]


def load_bess_default_demand_profile_for_report() -> pd.DataFrame:
    if not BESS_DEFAULT_DATA_PATH.exists():
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    try:
        df = pd.read_excel(BESS_DEFAULT_DATA_PATH, sheet_name=0)
    except Exception:
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    if df.empty:
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    cols = list(df.columns)
    if len(cols) < 6:
        return pd.DataFrame(columns=["hour_of_year", "consumption"])
    tmp = df.copy()
    tmp["consumption"] = pd.to_numeric(tmp[cols[5]], errors="coerce").fillna(0.0)
    tmp["hour_of_year"] = np.arange(1, len(tmp) + 1)
    return tmp[["hour_of_year", "consumption"]]


def make_period_hourly_bess_inputs(
    price_hourly: pd.DataFrame,
    start_d: date,
    end_d: date,
    mode: str,
    bess_power_mw: float = 1.0,
) -> pd.DataFrame:
    tmp = ensure_datetime_col(price_hourly, "datetime")
    tmp = tmp[
        (tmp["datetime"].dt.date >= start_d)
        & (tmp["datetime"].dt.date <= end_d)
    ][["datetime", "price"]].copy()
    if tmp.empty:
        return pd.DataFrame()

    tmp["date"] = tmp["datetime"].dt.date
    tmp["hour"] = tmp["datetime"].dt.hour + 1
    tmp["year"] = tmp["datetime"].dt.year
    tmp["hour_of_year"] = tmp["datetime"].dt.dayofyear.sub(1).mul(24).add(tmp["datetime"].dt.hour + 1)

    solar_profile = load_bess_default_solar_profile_for_report()
    demand_profile = load_bess_default_demand_profile_for_report()

    tmp = tmp.merge(solar_profile, on="hour_of_year", how="left")
    tmp = tmp.merge(demand_profile, on="hour_of_year", how="left")
    tmp["generation"] = tmp["generation"].fillna(0.0) * float(bess_power_mw)
    tmp["consumption"] = tmp["consumption"].fillna(0.0)

    if mode == "Standalone BESS":
        tmp["generation"] = 0.0
        tmp["consumption"] = 0.0
        tmp["buy_price"] = tmp["price"]
    elif mode == "BESS with demand":
        tmp["buy_price"] = tmp["price"]
    elif mode == "BESS without demand":
        tmp["buy_price"] = 1000.0
        tmp["consumption"] = 0.0
    else:
        raise ValueError(f"Unknown BESS mode: {mode}")

    tmp = tmp.rename(columns={"price": "sell_price"})
    return tmp[["datetime", "date", "hour", "sell_price", "buy_price", "generation", "consumption"]].copy()


def _linprog_bess_day_revenue(
    df_day: pd.DataFrame,
    capacity_mwh: float = 4.0,
    power_mw: float = 1.0,
    eta_ch: float = 0.93,
    eta_dis: float = 0.93,
    cycle_limit_factor: float = 1.0,
) -> dict:
    """
    Continuous-LP report replica of the BESS logic.
    It keeps the same economic objective and operational constraints as the BESS tab,
    but drops binary import/export switches so it can run with scipy.linprog
    without relying on an external CBC executable.
    """
    if not SCIPY_LINPROG_AVAILABLE or df_day.empty:
        return {
            "Revenue BESS (€)": None,
            "Charged (MWh)": None,
            "Discharged (MWh)": None,
            "Solver": "Unavailable",
        }

    d = df_day.sort_values("hour").reset_index(drop=True).copy()
    sell = pd.to_numeric(d["sell_price"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    buy = pd.to_numeric(d["buy_price"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    gen = pd.to_numeric(d["generation"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    load = pd.to_numeric(d["consumption"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    n = len(d)
    if n == 0:
        return {
            "Revenue BESS (€)": None,
            "Charged (MWh)": None,
            "Discharged (MWh)": None,
            "Solver": "No data",
        }

    # Variable slices
    ofs = {}
    k = 0
    for name in ["g_to_grid", "g_to_batt", "g_to_self", "grid_charge", "batt_for_load", "batt_for_sell", "grid_purchase"]:
        ofs[name] = slice(k, k + n)
        k += n
    ofs["soc"] = slice(k, k + n + 1)
    total_vars = k + n + 1

    # Objective: linprog minimizes, so use negative of project cash flow objective.
    c = np.zeros(total_vars)
    c[ofs["g_to_grid"]] = -sell
    c[ofs["batt_for_sell"]] = -sell
    c[ofs["grid_purchase"]] = buy
    c[ofs["grid_charge"]] = buy

    A_eq = []
    b_eq = []

    # g_to_grid + g_to_batt + g_to_self == generation
    for t in range(n):
        row = np.zeros(total_vars)
        row[ofs["g_to_grid"].start + t] = 1.0
        row[ofs["g_to_batt"].start + t] = 1.0
        row[ofs["g_to_self"].start + t] = 1.0
        A_eq.append(row)
        b_eq.append(gen[t])

    # g_to_self + batt_for_load + grid_purchase == demand
    for t in range(n):
        row = np.zeros(total_vars)
        row[ofs["g_to_self"].start + t] = 1.0
        row[ofs["batt_for_load"].start + t] = 1.0
        row[ofs["grid_purchase"].start + t] = 1.0
        A_eq.append(row)
        b_eq.append(load[t])

    # SOC transitions
    eta_ch = max(float(eta_ch), 1e-9)
    eta_dis = max(float(eta_dis), 1e-9)
    for t in range(n):
        row = np.zeros(total_vars)
        row[ofs["soc"].start + t + 1] = 1.0
        row[ofs["soc"].start + t] = -1.0
        row[ofs["g_to_batt"].start + t] = -eta_ch
        row[ofs["grid_charge"].start + t] = -eta_ch
        row[ofs["batt_for_load"].start + t] = 1.0 / eta_dis
        row[ofs["batt_for_sell"].start + t] = 1.0 / eta_dis
        A_eq.append(row)
        b_eq.append(0.0)

    # SOC start/end = 0
    row = np.zeros(total_vars)
    row[ofs["soc"].start] = 1.0
    A_eq.append(row)
    b_eq.append(0.0)

    row = np.zeros(total_vars)
    row[ofs["soc"].stop - 1] = 1.0
    A_eq.append(row)
    b_eq.append(0.0)

    A_ub = []
    b_ub = []
    max_power = max(float(power_mw), 1e-9)
    max_grid_flow = max_power

    # charge/discharge/export/import power caps
    for t in range(n):
        row = np.zeros(total_vars)
        row[ofs["g_to_batt"].start + t] = 1.0
        row[ofs["grid_charge"].start + t] = 1.0
        A_ub.append(row)
        b_ub.append(max_power)

        row = np.zeros(total_vars)
        row[ofs["batt_for_load"].start + t] = 1.0
        row[ofs["batt_for_sell"].start + t] = 1.0
        A_ub.append(row)
        b_ub.append(max_power)

        row = np.zeros(total_vars)
        row[ofs["g_to_grid"].start + t] = 1.0
        row[ofs["batt_for_sell"].start + t] = 1.0
        A_ub.append(row)
        b_ub.append(max_grid_flow)

        row = np.zeros(total_vars)
        row[ofs["grid_purchase"].start + t] = 1.0
        row[ofs["grid_charge"].start + t] = 1.0
        A_ub.append(row)
        b_ub.append(max_grid_flow)

    # 1 cycle/day charging limit
    row = np.zeros(total_vars)
    row[ofs["g_to_batt"]] = 1.0
    row[ofs["grid_charge"]] = 1.0
    A_ub.append(row)
    b_ub.append(float(cycle_limit_factor) * float(capacity_mwh) / eta_ch)

    # Discharge <= charge
    row = np.zeros(total_vars)
    row[ofs["batt_for_load"]] = 1.0
    row[ofs["batt_for_sell"]] = 1.0
    row[ofs["g_to_batt"]] = -1.0
    row[ofs["grid_charge"]] = -1.0
    A_ub.append(row)
    b_ub.append(0.0)

    bounds = [(0.0, None)] * total_vars
    for i in range(ofs["soc"].start, ofs["soc"].stop):
        bounds[i] = (0.0, float(capacity_mwh))

    try:
        res = linprog(
            c,
            A_ub=np.asarray(A_ub),
            b_ub=np.asarray(b_ub),
            A_eq=np.asarray(A_eq),
            b_eq=np.asarray(b_eq),
            bounds=bounds,
            method="highs",
        )
    except Exception:
        return {
            "Revenue BESS (€)": None,
            "Charged (MWh)": None,
            "Discharged (MWh)": None,
            "Solver": "Error",
        }

    if not res.success or res.x is None:
        return {
            "Revenue BESS (€)": None,
            "Charged (MWh)": None,
            "Discharged (MWh)": None,
            "Solver": "No optimum",
        }

    x = res.x
    g_to_batt = x[ofs["g_to_batt"]]
    grid_charge = x[ofs["grid_charge"]]
    batt_for_load = x[ofs["batt_for_load"]]
    batt_for_sell = x[ofs["batt_for_sell"]]

    revenue_bess = float(
        (-g_to_batt * sell).sum()
        - (grid_charge * buy).sum()
        + (batt_for_sell * sell).sum()
    )
    return {
        "Revenue BESS (€)": revenue_bess,
        "Charged (MWh)": float((g_to_batt + grid_charge).sum()),
        "Discharged (MWh)": float((batt_for_load + batt_for_sell).sum()),
        "Solver": "SciPy LP",
    }


def build_bess_revenue_period_table(
    price_hourly: pd.DataFrame,
    period_specs: list[tuple[str, date, date]],
    capacity_mwh: float = 4.0,
    c_rate: float = 0.25,
    eta_ch: float = 0.93,
    eta_dis: float = 0.93,
) -> pd.DataFrame:
    modes = ["Standalone BESS", "BESS with demand", "BESS without demand"]
    power_mw = float(capacity_mwh) * float(c_rate)
    rows = []
    for mode in modes:
        for label, s, e in period_specs:
            data = make_period_hourly_bess_inputs(price_hourly, s, e, mode, bess_power_mw=power_mw)
            daily_rows = []
            if not data.empty:
                for _day, day_df in data.groupby("date"):
                    daily_rows.append(
                        _linprog_bess_day_revenue(
                            day_df,
                            capacity_mwh=capacity_mwh,
                            power_mw=power_mw,
                            eta_ch=eta_ch,
                            eta_dis=eta_dis,
                            cycle_limit_factor=1.0,
                        )
                    )
            tmp = pd.DataFrame(daily_rows)
            revenue = tmp["Revenue BESS (€)"].sum(min_count=1) if not tmp.empty else None
            charged = tmp["Charged (MWh)"].sum(min_count=1) if not tmp.empty else None
            discharged = tmp["Discharged (MWh)"].sum(min_count=1) if not tmp.empty else None
            valid_days = int(tmp["Revenue BESS (€)"].notna().sum()) if not tmp.empty and "Revenue BESS (€)" in tmp.columns else 0
            rows.append({
                "Mode": mode,
                "Period": label,
                "Revenue BESS (€)": revenue,
                "Revenue BESS (€/MW)": (revenue / power_mw) if revenue is not None and pd.notna(revenue) and power_mw > 0 else None,
                "Charged (MWh)": charged,
                "Discharged (MWh)": discharged,
                "Solved days": valid_days,
                "Assumptions": f"{capacity_mwh:.1f} MWh / {power_mw:.1f} MW, ηch={eta_ch:.0%}, ηdis={eta_dis:.0%}, 1 cycle/day",
            })
    return pd.DataFrame(rows)


# =========================================================
# CORPORATE PDF EXPORT
# =========================================================
def _pdf_safe_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (float, np.floating)):
        return f"{float(value):,.2f}"
    return str(value)


def _pdf_df_table(df: pd.DataFrame, max_rows: int = 18):
    if not REPORTLAB_AVAILABLE or df is None or df.empty:
        return Paragraph("<i>No data available.</i>", getSampleStyleSheet()["BodyText"])
    work = df.head(max_rows).copy()
    data = [[_pdf_safe_text(c) for c in work.columns]]
    for _, row in work.iterrows():
        data.append([_pdf_safe_text(v) for v in row.tolist()])
    tbl = Table(data, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0F766E")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D1D5DB")),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#F8FAFC")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return tbl


def _pdf_img_from_b64(image_b64: str | None, width_cm: float = 24.0):
    if not REPORTLAB_AVAILABLE or not image_b64:
        return None
    try:
        bio = BytesIO(base64.b64decode(image_b64))
        img = RLImage(bio)
        ratio = img.imageHeight / max(img.imageWidth, 1)
        img.drawWidth = width_cm * cm
        img.drawHeight = img.drawWidth * ratio
        return img
    except Exception:
        return None


def build_corporate_pdf_bytes(
    subject: str,
    report_granularity: str,
    selected_period_label: str,
    kpi_items: list[tuple[str, str]],
    day_ahead_df: pd.DataFrame,
    forward_df: pd.DataFrame,
    mibgas_df: pd.DataFrame,
    tb_df: pd.DataFrame,
    bess_revenue_df: pd.DataFrame,
    mix_df: pd.DataFrame,
    mix_delta_df: pd.DataFrame,
    ytd_negative_df: pd.DataFrame,
    capture_period_b64: str | None,
    mix_delta_b64: str | None,
    mix_re_share_delta_b64: str | None,
    bess_revenue_chart_b64: str | None,
    omie_heatmap_b64: str | None,
    mix_chart_b64: str | None,
    neg_chart_b64: str | None,
    zero_neg_b64: str | None,
    curt_b64: str | None,
    spot_b64: str | None,
) -> bytes | None:
    if not REPORTLAB_AVAILABLE:
        return None

    output = BytesIO()
    doc = SimpleDocTemplate(
        output,
        pagesize=landscape(A4),
        rightMargin=0.8 * cm,
        leftMargin=0.8 * cm,
        topMargin=1.25 * cm,
        bottomMargin=1.0 * cm,
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="NexwellTitle",
        parent=styles["Title"],
        fontSize=19,
        leading=22,
        textColor=colors.HexColor("#0F766E"),
        alignment=TA_LEFT,
        spaceAfter=8,
    ))
    styles.add(ParagraphStyle(
        name="NexwellH2",
        parent=styles["Heading2"],
        fontSize=11,
        leading=13,
        textColor=colors.HexColor("#0F766E"),
        spaceBefore=8,
        spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        name="NexwellBody",
        parent=styles["BodyText"],
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#1F2937"),
    ))
    styles.add(ParagraphStyle(
        name="NexwellSmall",
        parent=styles["BodyText"],
        fontSize=7,
        leading=9,
        textColor=colors.HexColor("#475569"),
    ))

    def _header_footer(canvas, _doc):
        canvas.saveState()
        page_w, page_h = landscape(A4)
        canvas.setFillColor(colors.HexColor("#0F766E"))
        canvas.rect(0, page_h - 0.55 * cm, page_w, 0.55 * cm, fill=1, stroke=0)
        if NEXWELL_LOGO_PATH.exists():
            try:
                canvas.drawImage(
                    ImageReader(str(NEXWELL_LOGO_PATH)),
                    0.9 * cm,
                    page_h - 0.46 * cm,
                    width=1.0 * cm,
                    height=0.32 * cm,
                    preserveAspectRatio=True,
                    mask="auto",
                )
            except Exception:
                pass
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 7)
        canvas.drawRightString(page_w - 0.9 * cm, page_h - 0.34 * cm, "Nexwell Power | Energy Markets")
        canvas.setFillColor(colors.HexColor("#64748B"))
        canvas.setFont("Helvetica", 6.5)
        canvas.drawString(0.9 * cm, 0.45 * cm, f"{subject} | {selected_period_label}")
        canvas.drawRightString(page_w - 0.9 * cm, 0.45 * cm, f"Page {canvas.getPageNumber()}")
        canvas.restoreState()

    story = []
    story.append(Paragraph(subject, styles["NexwellTitle"]))
    story.append(Paragraph(
        f"<b>Report type:</b> {report_granularity} &nbsp;&nbsp; "
        f"<b>Selected period:</b> {selected_period_label}",
        styles["NexwellBody"],
    ))
    story.append(Spacer(1, 0.18 * cm))

    if NEXWELL_LOGO_PATH.exists():
        try:
            logo = RLImage(str(NEXWELL_LOGO_PATH))
            logo.drawHeight = 0.72 * cm
            logo.drawWidth = 2.2 * cm
            story.append(logo)
            story.append(Spacer(1, 0.15 * cm))
        except Exception:
            pass

    if kpi_items:
        kpi_df = pd.DataFrame(kpi_items, columns=["Quick metric", "Value"])
        story.append(Paragraph("Executive quick metrics", styles["NexwellH2"]))
        story.append(_pdf_df_table(kpi_df, max_rows=20))
        story.append(Spacer(1, 0.15 * cm))
        story.append(Paragraph(
            "This corporate market digest consolidates spot, captured prices, forward direction, gas, BESS value, "
            "generation mix, curtailment and negative-price diagnostics in one period-comparison pack. "
            "The first comparison is always versus the previous equivalent period; the second is versus the same period one year earlier when available.",
            styles["NexwellBody"],
        ))

    sections = [
        ("Day-ahead spot and solar captured prices", day_ahead_df),
        ("Forward market summary", forward_df),
        ("MIBGAS GDAES D+1 summary", mibgas_df),
        ("BESS TB spreads", tb_df),
        ("BESS revenue comparison", bess_revenue_df),
        ("Generation mix comparison", mix_df),
        ("Generation mix deltas", mix_delta_df),
        ("Negative prices YTD", ytd_negative_df),
    ]
    for title, df in sections:
        story.append(Spacer(1, 0.15 * cm))
        story.append(Paragraph(title, styles["NexwellH2"]))
        story.append(_pdf_df_table(df, max_rows=18))

    chart_sections = [
        ("Spot and captured-price comparison", capture_period_b64),
        ("BESS revenue comparison", bess_revenue_chart_b64),
        ("Energy mix differences", mix_delta_b64),
        ("Renewables share delta", mix_re_share_delta_b64),
        ("OMIE hourly price heatmap", omie_heatmap_b64),
        ("Generation mix chart", mix_chart_b64),
        ("Monthly negative-price frequency", neg_chart_b64),
        ("12x24 negative-price frequency", zero_neg_b64),
        ("Monthly economic curtailment", curt_b64),
        ("Spot and solar capture evolution", spot_b64),
    ]
    for title, img_b64 in chart_sections:
        img = _pdf_img_from_b64(img_b64)
        if img is not None:
            story.append(PageBreak())
            story.append(Paragraph(title, styles["NexwellH2"]))
            story.append(img)

    story.append(Spacer(1, 0.2 * cm))
    story.append(Paragraph(
        "Method note: BESS revenue comparison uses the Email Report's continuous LP replica "
        "of the BESS-tab dispatch logic with default assumptions of 4.0 MWh, 1.0 MW, "
        "93% charging/discharging efficiency and 1 cycle/day. "
        "If the default demand file is unavailable, the 'BESS with demand' case uses a zero-demand fallback.",
        styles["NexwellSmall"],
    ))

    doc.build(story, onFirstPage=_header_footer, onLaterPages=_header_footer)
    output.seek(0)
    return output.getvalue()


# =========================================================
# LOAD DATA
# =========================================================
token = require_esios_token()

price_raw = load_price_history_fallback()
solar_p48_raw = load_solar_p48_history_fallback()
solar_forecast_raw = load_raw_history(SOLAR_FORECAST_RAW_CSV_PATH, "esios_542")
demand_raw = load_raw_history(DEMAND_RAW_CSV_PATH, "esios_10027")
ree_daily_raw = load_ree_mix_daily()

if price_raw.empty:
    st.error("No price history found. The app looked first in /historical_data and then in /data/hourly_avg_price_since2021.xlsx.")
    st.stop()

price_hourly = to_hourly_mean(price_raw, value_col_name="price")
solar_p48_hourly = to_hourly_mean(solar_p48_raw, value_col_name="solar_p48_mw")
solar_forecast_hourly = to_hourly_mean(solar_forecast_raw, value_col_name="solar_forecast_mw")


latest_available_day = price_hourly["datetime"].dt.date.max()
tomorrow_allowed = max_refresh_day_from_clock()

historical_best_solar = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
price_hourly = extend_price_history_to_latest(price_hourly, token)

# Period selector
period_options = build_available_period_options(price_hourly, report_granularity)
if not period_options:
    st.error("No periods available for the selected granularity.")
    st.stop()

default_idx = len(period_options) - 1
selected_label = st.selectbox(
    "Reporting period",
    [p["label"] for p in period_options],
    index=default_idx,
    key=f"email_report_period_{report_granularity.lower()}",
)
selected_option = next(p for p in period_options if p["label"] == selected_label)
selected_start, selected_end = selected_option["start"], selected_option["end"]

# Enrich recent price history when the selected period reaches beyond local history.
price_hourly = ensure_recent_price_days(price_hourly, selected_start, selected_end, token)
historical_best_solar = ensure_recent_best_solar_days(
    solar_p48_hourly,
    solar_forecast_hourly,
    historical_best_solar,
    selected_start,
    selected_end,
    token,
)

previous_start, previous_end = previous_period(selected_start, selected_end, report_granularity)
prior_start, prior_end = prior_year_same_period(selected_start, selected_end, report_granularity)

selected_label_long = f"Selected: {period_label(report_granularity, selected_start, selected_end)}"
previous_label_long = f"Previous period: {period_label(report_granularity, previous_start, previous_end)}"
prior_label_long = f"Same period prior year: {period_label(report_granularity, prior_start, prior_end)}"

if report_granularity == "Annual":
    period_specs = [
        (selected_label_long, selected_start, selected_end),
        ("Previous year", previous_start, previous_end),
    ]
else:
    period_specs = [
        (selected_label_long, selected_start, selected_end),
        (previous_label_long, previous_start, previous_end),
        (prior_label_long, prior_start, prior_end),
    ]

historical_best_solar = ensure_recent_best_solar_days(
    solar_p48_hourly,
    solar_forecast_hourly,
    historical_best_solar,
    min([x[1] for x in period_specs]),
    max([x[2] for x in period_specs]),
    token,
)

# Mail config
col1, col2 = st.columns(2)
with col1:
    to_emails_raw = st.text_area(
        "To",
        value=st.secrets.get("default_to", ""),
        placeholder="name1@company.com; name2@company.com",
        height=90,
    )
with col2:
    cc_emails_raw = st.text_area(
        "Cc",
        value=st.secrets.get("default_cc", ""),
        placeholder="optional@company.com; optional2@company.com",
        height=90,
    )

subject = st.text_input(
    "Subject",
    value=f"Energy market report - {period_label(report_granularity, selected_start, selected_end)}",
)
intro_text = st.text_area(
    "Intro text",
    value=(
        "Hi all,\n\n"
        f"Please find below the {report_granularity.lower()} energy-market update for "
        f"{period_label(report_granularity, selected_start, selected_end)}.\n"
    ),
    height=120,
)

# Core cross-page summaries
day_ahead_period_df = build_period_day_ahead_table(
    price_hourly,
    historical_best_solar,
    period_specs,
    negative_price_mode,
)
capture_period_chart = build_capture_period_chart(day_ahead_period_df)
ytd_negative_df = build_ytd_negative_table(price_hourly, selected_end, negative_price_mode)
ree_daily_reporting = enrich_mix_with_live_2026(ree_daily_raw, period_specs)
mix_period_df = build_period_mix_table(ree_daily_reporting, period_specs)
mix_period_chart = build_period_mix_chart(mix_period_df)
mix_delta_df = build_period_mix_delta_table(mix_period_df)
mix_delta_chart = build_period_mix_delta_chart(mix_delta_df)
mix_re_share_delta_chart = build_renewables_share_delta_chart(mix_delta_df)
tb_period_df = build_period_tb_table(price_hourly, period_specs)
bess_revenue_period_df = build_bess_revenue_period_table(
    price_hourly,
    period_specs,
    capacity_mwh=4.0,
    c_rate=0.25,
    eta_ch=0.93,
    eta_dis=0.93,
)
bess_revenue_chart = build_bess_revenue_comparison_chart(bess_revenue_period_df)

mibgas_actuals_df = load_mibgas_actuals_for_report()
mibgas_period_df = build_mibgas_summary_table(mibgas_actuals_df, period_specs)

forward_cache_df = load_omip_cache_for_report()
forward_summary_df = build_forward_summary_table(
    forward_cache_df,
    period_specs[0],
    period_specs[1] if len(period_specs) > 1 else period_specs[0],
)

selected_year = selected_start.year
compare_years = sorted({selected_year, selected_year - 1})
omie_heatmap_b64 = build_omie_hourly_price_heatmap_png_base64(price_hourly, selected_year)

neg_pct_chart = build_monthly_negative_chart(price_hourly, compare_years, mode=negative_price_mode)
neg_pct_table = build_monthly_negative_pct_table(price_hourly, compare_years, mode=negative_price_mode)
zero_neg_heatmap = build_zero_negative_heatmap(price_hourly, selected_year, mode=negative_price_mode)
zero_neg_heatmap_table = build_zero_negative_hour_table(price_hourly, selected_year, mode=negative_price_mode)
curt_chart, _curt_plot = build_monthly_curtailment_chart(price_hourly, historical_best_solar, compare_years, mode=negative_price_mode)
curt_pct_table = build_curtailment_pct_table(price_hourly, historical_best_solar, compare_years, mode=negative_price_mode)
spot_capture_chart, _spot_capture_plot = build_spot_capture_evolution_chart(price_hourly, historical_best_solar, selected_year)
spot_capture_pivot = build_spot_capture_table(price_hourly, historical_best_solar, selected_year)

# Preview: fast metrics
st.subheader("📌 Executive dashboard")
kpi_cards_preview = make_metric_kpis(day_ahead_period_df, mibgas_period_df, tb_period_df, forward_summary_df, mix_df=mix_period_df, bess_revenue_df=bess_revenue_period_df)
if kpi_cards_preview:
    st.markdown(make_kpi_cards_html(kpi_cards_preview), unsafe_allow_html=True)

st.markdown(
    f"""
    <div style="padding:14px 16px;border:1px solid #d9e4e1;border-radius:14px;background:#f5fbfa;margin:12px 0 18px 0;">
      <div style="font-weight:700;color:#0f766e;font-size:15px;margin-bottom:4px;">Market digest</div>
      <div style="color:#334155;font-size:13px;">
        Selected period: <b>{period_label(report_granularity, selected_start, selected_end)}</b>.
        The dashboard compares it against the prior equivalent period and the same period last year,
        and rolls together spot, solar capture, forward direction, MIBGAS, storage value and generation mix.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("⚡ Day-ahead spot and solar captured prices")
st.dataframe(day_ahead_period_df, use_container_width=True)
if capture_period_chart is not None:
    st.altair_chart(capture_period_chart, use_container_width=True)
st.caption("Captured prices are computed from the best available solar profile (P48 first, forecast as fallback) matched against OMIE spot prices.")

st.subheader("📈 Forward market during the selected period")
if forward_summary_df.empty:
    st.info("No OMIP forward cache was available for this period.")
else:
    st.dataframe(forward_summary_df, use_container_width=True)
    st.caption("Forward summary uses the average of available yearly OMIP curve rows in the local cache, split by curve basket.")

st.subheader("🔥 MIBGAS GDAES D+1")
if mibgas_period_df.empty:
    st.info("No MIBGAS actuals available. The report reads local MIBGAS files plus the 2026 cache created by the MIBGAS tab.")
else:
    st.dataframe(mibgas_period_df, use_container_width=True)

st.subheader("🔋 BESS TB spreads")
if tb_period_df.empty:
    st.info("No TB spread data available.")
else:
    st.dataframe(tb_period_df, use_container_width=True)
    st.caption("TB4 and TB2 are averaged from daily standalone arbitrage spreads without relying on CBC/PuLP solver binaries.")

st.subheader("💰 BESS revenue comparison")
if bess_revenue_period_df.empty:
    st.info("No BESS revenue comparison available.")
else:
    st.dataframe(bess_revenue_period_df, use_container_width=True)
    if bess_revenue_chart is not None:
        st.altair_chart(bess_revenue_chart, use_container_width=True)
    st.caption(
        "Revenue comparison uses 4.0 MWh / 1.0 MW, 93% charging and discharging efficiency, and 1 cycle/day. "
        "The three scenarios follow the BESS tab naming: Standalone BESS, BESS with demand, and BESS without demand."
    )

st.subheader("🌍 Generation mix comparison")
if mix_period_df.empty:
    st.info("No period generation mix data available. Historical 2022–2025 comes from the local workbook; 2026 is fetched live from REE's generation-structure endpoint.")
else:
    st.dataframe(mix_period_df, use_container_width=True)
    if mix_period_chart is not None:
        st.altair_chart(mix_period_chart, use_container_width=True)

    st.markdown("#### Mix deltas vs previous period and prior year")
    if mix_delta_df.empty:
        st.info("No generation-mix differences available for the selected comparison periods.")
    else:
        st.dataframe(mix_delta_df, use_container_width=True)
        if mix_delta_chart is not None:
            st.altair_chart(mix_delta_chart, use_container_width=True)
        if mix_re_share_delta_chart is not None:
            st.altair_chart(mix_re_share_delta_chart, use_container_width=True)

st.subheader("🧊 Negative prices YTD")
st.dataframe(ytd_negative_df, use_container_width=True)

st.subheader("🗺️ OMIE hourly price heatmap")
if omie_heatmap_b64:
    st.image(BytesIO(base64.b64decode(omie_heatmap_b64)), use_container_width=True)

st.subheader(f"📉 Monthly {price_event_label(negative_price_mode)} frequency")
if neg_pct_chart is not None:
    st.altair_chart(neg_pct_chart, use_container_width=True)
if not neg_pct_table.empty:
    st.dataframe(neg_pct_table, use_container_width=True)

st.subheader(f"🕒 12x24 {price_event_label(negative_price_mode)} frequency ({selected_year})")
if zero_neg_heatmap is not None:
    st.altair_chart(zero_neg_heatmap, use_container_width=True)
if not zero_neg_heatmap_table.empty:
    st.dataframe(zero_neg_heatmap_table, use_container_width=True)

st.subheader("✂️ Monthly economic curtailment")
if curt_chart is not None:
    st.altair_chart(curt_chart, use_container_width=True)
if not curt_pct_table.empty:
    st.dataframe(curt_pct_table, use_container_width=True)

st.subheader("☀️ Spot and capture evolution")
if spot_capture_chart is not None:
    st.altair_chart(spot_capture_chart, use_container_width=True)
if not spot_capture_pivot.empty:
    st.dataframe(spot_capture_pivot, use_container_width=True)

# Email HTML
day_ahead_html = df_to_html_table(
    day_ahead_period_df,
    pct_cols=[
        "Solar capture rate uncurtailed (%)",
        "Solar capture rate curtailed (%)",
    ],
)
forward_html = df_to_html_table(forward_summary_df, pct_cols=["Δ %"]) if not forward_summary_df.empty else "<p>No OMIP forward cache available for the selected period.</p>"
mibgas_html = df_to_html_table(mibgas_period_df) if not mibgas_period_df.empty else "<p>No MIBGAS period summary available.</p>"
tb_html = df_to_html_table(tb_period_df) if not tb_period_df.empty else "<p>No TB spread summary available.</p>"
bess_revenue_html = df_to_html_table(bess_revenue_period_df) if not bess_revenue_period_df.empty else "<p>No BESS revenue comparison available.</p>"
mix_html = df_to_html_table(mix_period_df, pct_cols=["Renewables share (%)"]) if not mix_period_df.empty else "<p>No generation mix period summary available.</p>"
mix_delta_html = df_to_html_table(mix_delta_df, pct_cols=["Δ %"]) if not mix_delta_df.empty else "<p>No generation mix delta summary available.</p>"
ytd_negative_html = df_to_html_table(ytd_negative_df, pct_cols=[f"% {price_event_label(negative_price_mode)} hours"])
neg_pct_html = df_to_html_table(neg_pct_table.drop(columns=["month_num"], errors="ignore"), pct_cols=["pct_event"]) if not neg_pct_table.empty else "<p>No monthly negative-price frequency table available.</p>"
zero_neg_table_html = df_to_html_table(zero_neg_heatmap_table, pct_cols=[c for c in zero_neg_heatmap_table.columns if c.startswith("H")]) if not zero_neg_heatmap_table.empty else "<p>No 12x24 negative-price frequency table available.</p>"
curt_html = df_to_html_table(curt_pct_table, pct_cols=["pct_curtailment"]) if not curt_pct_table.empty else "<p>No economic curtailment table available.</p>"
spot_capture_html = df_to_html_table(spot_capture_pivot) if not spot_capture_pivot.empty else "<p>No spot/capture evolution table available.</p>"

kpi_cards_html = make_kpi_cards_html(kpi_cards_preview)

omie_heatmap_html = ""
if omie_heatmap_b64:
    omie_heatmap_html = f"""
    <h3>OMIE hourly price heatmap ({selected_year})</h3>
    <img src="data:image/png;base64,{omie_heatmap_b64}" alt="OMIE hourly heatmap" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

mix_chart_html = ""
mix_b64 = chart_to_base64_png(mix_period_chart)
if mix_b64:
    mix_chart_html = f"""
    <h3>Generation mix comparison</h3>
    <img src="data:image/png;base64,{mix_b64}" alt="Generation mix comparison" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

mix_delta_chart_html = ""
mix_delta_b64 = chart_to_base64_png(mix_delta_chart)
mix_re_share_delta_b64 = chart_to_base64_png(mix_re_share_delta_chart)
if mix_delta_b64:
    mix_delta_chart_html += f"""
    <h3>Energy mix differences</h3>
    <img src="data:image/png;base64,{mix_delta_b64}" alt="Energy mix differences" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """
if mix_re_share_delta_b64:
    mix_delta_chart_html += f"""
    <h3>Renewables share delta</h3>
    <img src="data:image/png;base64,{mix_re_share_delta_b64}" alt="Renewables share delta" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

neg_chart_html = ""
neg_b64 = chart_to_base64_png(neg_pct_chart)
if neg_b64:
    neg_chart_html = f"""
    <h3>Monthly {price_event_label(negative_price_mode)} frequency</h3>
    <img src="data:image/png;base64,{neg_b64}" alt="Negative price frequency" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

zero_neg_heatmap_html = ""
zn_b64 = chart_to_base64_png(zero_neg_heatmap)
if zn_b64:
    zero_neg_heatmap_html = f"""
    <h3>12x24 {price_event_label(negative_price_mode)} frequency ({selected_year})</h3>
    <img src="data:image/png;base64,{zn_b64}" alt="12x24 negative price frequency" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

curt_chart_html = ""
curt_b64 = chart_to_base64_png(curt_chart)
if curt_b64:
    curt_chart_html = f"""
    <h3>Monthly economic curtailment</h3>
    <img src="data:image/png;base64,{curt_b64}" alt="Economic curtailment" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

capture_period_chart_html = ""
capture_period_b64 = chart_to_base64_png(capture_period_chart)
if capture_period_b64:
    capture_period_chart_html = f"""
    <h3>Spot and captured-price comparison</h3>
    <img src="data:image/png;base64,{capture_period_b64}" alt="Spot and captured price comparison" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

spot_capture_chart_html = ""
spot_b64 = chart_to_base64_png(spot_capture_chart)
if spot_b64:
    spot_capture_chart_html = f"""
    <h3>Spot and solar capture evolution ({selected_year})</h3>
    <img src="data:image/png;base64,{spot_b64}" alt="Spot and capture evolution" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

email_html = f"""
<html><body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
  <p>{intro_text.replace(chr(10), '<br>')}</p>
  <p>
    <strong>Granularity:</strong> {report_granularity}<br>
    <strong>Selected period:</strong> {period_label(report_granularity, selected_start, selected_end)}<br>
    <strong>Comparison periods:</strong> {", ".join([x[0] for x in period_specs[1:]])}
  </p>
  {kpi_cards_html}<br><br>
  <h3>Day-ahead spot and solar captured prices</h3>{day_ahead_html}<br>
  {capture_period_chart_html}
  <h3>Forward market summary</h3>{forward_html}<br>
  <h3>MIBGAS GDAES D+1 summary</h3>{mibgas_html}<br>
  <h3>BESS TB spreads</h3>{tb_html}<br>
  <h3>BESS revenue comparison</h3>{bess_revenue_html}<br>
  <h3>Generation mix comparison</h3>{mix_html}<br>
  {mix_chart_html}
  <h3>Generation mix deltas vs previous period and prior year</h3>{mix_delta_html}<br>
  {mix_delta_chart_html}
  <h3>Negative prices YTD</h3>{ytd_negative_html}<br>
  {omie_heatmap_html}
  {neg_chart_html}
  <h3>Monthly negative-price frequency table</h3>{neg_pct_html}<br>
  {zero_neg_heatmap_html}
  <h3>12x24 negative-price frequency table ({selected_year})</h3>{zero_neg_table_html}<br>
  {curt_chart_html}
  <h3>Economic curtailment table</h3>{curt_html}<br>
  {spot_capture_chart_html}
  <h3>Spot and capture evolution table</h3>{spot_capture_html}<br>
  <p>Best regards,</p>
</body></html>
"""

report_day = selected_start
mix_chart_b64 = chart_to_base64_png(mix_period_chart)
mix_delta_b64 = chart_to_base64_png(mix_delta_chart)
mix_re_share_delta_b64 = chart_to_base64_png(mix_re_share_delta_chart)
capture_period_b64 = chart_to_base64_png(capture_period_chart)
bess_revenue_chart_b64 = chart_to_base64_png(bess_revenue_chart)
neg_chart_b64 = chart_to_base64_png(neg_pct_chart)
zero_neg_b64 = chart_to_base64_png(zero_neg_heatmap)
curt_b64 = chart_to_base64_png(curt_chart)
spot_b64 = chart_to_base64_png(spot_capture_chart)

pdf_bytes = build_corporate_pdf_bytes(
    subject=subject,
    report_granularity=report_granularity,
    selected_period_label=period_label(report_granularity, selected_start, selected_end),
    kpi_items=kpi_cards_preview,
    day_ahead_df=day_ahead_period_df,
    forward_df=forward_summary_df,
    mibgas_df=mibgas_period_df,
    tb_df=tb_period_df,
    bess_revenue_df=bess_revenue_period_df,
    mix_df=mix_period_df,
    mix_delta_df=mix_delta_df,
    ytd_negative_df=ytd_negative_df,
    capture_period_b64=capture_period_b64,
    mix_delta_b64=mix_delta_b64,
    mix_re_share_delta_b64=mix_re_share_delta_b64,
    bess_revenue_chart_b64=bess_revenue_chart_b64,
    omie_heatmap_b64=omie_heatmap_b64,
    mix_chart_b64=mix_chart_b64,
    neg_chart_b64=neg_chart_b64,
    zero_neg_b64=zero_neg_b64,
    curt_b64=curt_b64,
    spot_b64=spot_b64,
)

st.subheader("Corporate PDF export")
if pdf_bytes:
    st.download_button(
        label="Download corporate PDF",
        data=pdf_bytes,
        file_name=f"nexwell_energy_market_report_{period_label(report_granularity, selected_start, selected_end).replace(' ', '_')}.pdf",
        mime="application/pdf",
    )
else:
    st.info("Corporate PDF export is unavailable. Add 'reportlab' to requirements.txt if it is not installed.")

st.subheader("Email HTML preview")
st.code(email_html, language="html")

st.download_button(
    label="Download email HTML",
    data=email_html,
    file_name=f"email_report_{report_day.isoformat()}.html",
    mime="text/html",
)

st.subheader("Recipients preview")
recipients_preview = pd.DataFrame(
    {
        "To": [", ".join(parse_emails(to_emails_raw))],
        "Cc": [", ".join(parse_emails(cc_emails_raw))],
        "Subject": [subject],
    }
)
st.dataframe(recipients_preview, use_container_width=True)

send_enabled = "mail_webhook_url" in st.secrets and "mail_webhook_token" in st.secrets

if not send_enabled:
    st.warning("Manual send button is not enabled yet. Add mail_webhook_url and mail_webhook_token to Secrets.")
else:
    if st.button("Send now"):
        to_list = parse_emails(to_emails_raw)
        cc_list = parse_emails(cc_emails_raw)

        if not to_list:
            st.error("Add at least one recipient in To.")
        else:
            payload = {
                "token": st.secrets["mail_webhook_token"],
                "to": to_list,
                "cc": cc_list,
                "subject": subject,
                "html_body": email_html,
                "report_day": report_day.isoformat(),
            }

            try:
                resp = requests.post(
                    st.secrets["mail_webhook_url"],
                    json=payload,
                    timeout=30,
                )
                if 200 <= resp.status_code < 300:
                    st.success("Email request sent successfully.")
                else:
                    st.error(f"Webhook returned status {resp.status_code}: {resp.text}")
            except Exception as e:
                st.error(f"Send failed: {e}")

st.info(
    "This report supports Daily, Weekly, Monthly, Quarterly and Annual snapshots. "
    "Historical solar now follows the same source priority as Day Ahead, so the economic-curtailment chart remains aligned (including Apr-2025). "
    "Generation mix now uses the historical 2022–2025 workbook plus live REE 2026 generation-structure data for selected comparison periods. "
    "Recent OMIE and solar days are preloaded so weekly and monthly selectors can reach the latest available May periods. "
    "TB4/TB2 now use a deterministic spread method that avoids CBC/PuLP executable failures on Streamlit Cloud. "
    "The report also adds BESS revenue comparison for Standalone BESS, BESS with demand, and BESS without demand, "
    "plus a branded Nexwell corporate PDF export. "
    "MIBGAS 2026 values are read from the cache generated by the MIBGAS tab when available."
)
