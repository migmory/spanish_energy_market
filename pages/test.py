from __future__ import annotations

import os
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from time import sleep
from typing import Any
from zoneinfo import ZoneInfo

import altair as alt
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from dateutil.easter import easter

try:
    from sklearn.ensemble import HistGradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except Exception:
    HistGradientBoostingRegressor = None
    SKLEARN_AVAILABLE = False

# =========================================================
# TEST — PBF - bilaterales: Hueco térmico + precio spot
# =========================================================
#
# Objetivo:
#   Replicar el gráfico "Hueco Térmico y Precio":
#     - Barras naranjas: hueco térmico horario, una barra por hora
#     - Línea negra: precio spot day-ahead horario
#     - Eje Y izquierdo: Hueco Térmico (MWh)
#     - Eje Y derecho: Precio (€/MWh)
#     - Eje X: hora local Madrid
#
# Precio:
#   Usa exactamente la lógica base del Day Ahead:
#     - data/hourly_avg_price_since2021.xlsx, sheet prices_hourly_avg
#     - ESIOS indicator 600 para live/current 2026
#     - timestamps ESIOS: UTC -> Europe/Madrid -> timezone-naive
#
# .env / Streamlit secrets:
#   ESIOS_TOKEN=...
# =========================================================

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="Hueco térmico PBF + REE demand profile", layout="wide")

st.markdown(
    """
    <style>
    div[data-testid="stVegaLiteChart"] {
        max-width: 100% !important;
        overflow-x: hidden !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <style>
    .formula-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 14px;
        padding: 14px 16px;
        margin: 10px 0 16px 0;
        color: #0F172A;
    }
    .formula-card b { color: #0F172A; }
    .formula-main {
        font-size: 1.02rem;
        line-height: 1.55;
        font-weight: 650;
    }
    .formula-note {
        color: #475569;
        font-size: 0.92rem;
        line-height: 1.45;
        margin-top: 8px;
    }
    .downward-card {
        background: #FFF7FA;
        border: 1px solid #FBCFE8;
        border-left: 6px solid #C81046;
        border-radius: 12px;
        padding: 12px 14px;
        margin: 8px 0 12px 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Same path convention as the main app pages.
BASE_DIR = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
ENV_PATH = BASE_DIR / ".env"
DATA_DIR = BASE_DIR / "data"
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
LIVE_START_DATE = date(2026, 1, 1)
MADRID_TZ = ZoneInfo("Europe/Madrid")

load_dotenv(dotenv_path=ENV_PATH, override=True)

BASE = "https://api.esios.ree.es"
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

# Chart style inspired by the monthly report page
BLUE = "#1D4ED8"
GREY_DARK = "#475569"
TEXT = "#0F172A"
GRID = "#E2E8F0"
WHITE = "#FFFFFF"

def apply_monthly_report_chart_style(chart, height: int = 340):
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

# ---------------------------------------------------------
# IDs
# ---------------------------------------------------------
PRICE_INDICATOR_ID = 600
DEMAND_PBF_ID = 10141

# Balancing-energy graph IDs.
# Goal: replicate the slide/chart: upward and downward balancing energy should not be blended.
# Energy indicators are summed over the selected period and converted MWh -> GWh.
# Reserve indicators are averaged over the selected period and shown in MW.
# Keep the mapping explicit so it is easy to adjust if ESIOS changes/renames an indicator.
BALANCING_ENERGY_SECTIONS = {
    "Day-ahead constraints Phase I": {"up": [701], "down": [702]},
    "Day-ahead constraints Phase II": {"up": [703], "down": [704]},
    "Real-time constraints": {"up": [1806, 1808, 1810, 1814], "down": [1807, 1809, 1811, 1815]},
    "Tertiary / mFRR": {"up": [10395, 10396], "down": [10394, 10397]},
    "Secondary energy": {"up": [680], "down": [681]},
}
BALANCING_SECONDARY_RESERVE_IDS = {"Downward": [633], "Upward": [632]}
# Official ESIOS secondary-reserve band capacity prices by direction.
# The public ESIOS page shows 10388 compared with 10463 for upward/downward reserve price.
# In this app they are displayed as hourly-equivalent capacity prices:
# €/MW/h = published €/MW/15min × 4.
BALANCING_SECONDARY_RESERVE_PRICE_IDS = {"Downward": [10463], "Upward": [10388]}

# Hourly average price profiles shown below the main balancing chart.
# aFRR capacity prices are converted from published €/MW/15min to hourly-equivalent €/MW/h.
BALANCING_HOURLY_PRICE_PROFILE_SECTIONS = {
    "aFRR capacity price": {
        "unit": "€/MW/h",
        "published_unit": "€/MW/15min",
        "multiplier": 4.0,
        "series": {"Downward": [10463], "Upward": [10388]},
    },
    "Day-ahead constraints Phase I price": {
        "unit": "€/MWh",
        "published_unit": "€/MWh",
        "multiplier": 1.0,
        "series": {"Downward": [706], "Upward": [705]},
    },
    "Day-ahead constraints Phase II price": {
        "unit": "€/MWh",
        "published_unit": "€/MWh",
        "multiplier": 1.0,
        "series": {"Downward": [708], "Upward": [707]},
    },
}

# Official ESIOS average-price indicators used in the hover tooltip.
# Real-time constraints are intentionally left empty because the CT/RTD/EST/RSI buckets
# do not have one clean public price indicator that is directly comparable to the
# aggregated volume bucket used in this chart.
BALANCING_PRICE_SECTIONS = {
    "Day-ahead constraints Phase I": {"up": [705], "down": [706]},
    "Day-ahead constraints Phase II": {"up": [707], "down": [708]},
    "Real-time constraints": {"up": [], "down": []},
    "Tertiary / mFRR": {"up": [10386], "down": [10387]},
    "Secondary energy": {"up": [682], "down": [683]},
}

# PBF gross generation.
# IMPORTANT:
# Avoid non-working aggregated IDs such as 10167/10077/10086 where ESIOS returns no rows.
# Use base PBF technology IDs and aggregate manually.
PBF_GROSS_COMPONENTS = {
    "Hydro UGH": 1,
    "Hydro non-UGH": 2,
    "Nuclear": 4,
    "Coal sub-bituminous": 7,
    "Coal anthracite": 8,
    "Combined cycle GT": 9,
    "Fuel": 10,
    "Natural gas": 11,
    "Wind onshore": 12,
    "Wind offshore": 13,
    "Solar PV": 14,
    "Solar thermal": 15,
    "Cogeneration": 17,
    # These may or may not exist depending on ESIOS/range; handled as optional.
    "Other renewables": 10074,
    "Non-renewable waste": 10095,
}

# Bilateral PBF indicators from the info you provided.
PBF_BILATERAL_COMPONENTS = {
    "Hydro UGH": 421,
    "Hydro non-UGH": 422,
    "Nuclear": 424,
    "Coal sub-bituminous": 426,
    "Coal anthracite": 427,
    "Combined cycle GT": 429,
    "Wind onshore": 432,
    "Wind offshore": 433,
    "Other renewables": 10234,
}
PBF_BILATERAL_TOTAL_SALES_ID = 10235

# PBF programmed interconnection balance indicators.
# Positive saldo is treated as net exports from the Spanish system, so it increases
# the domestic generation requirement. Negative saldo is treated as net imports.
PBF_INTERCONNECTION_COMPONENTS = {
    "PBF balance France": 10104,
    "PBF balance Portugal": 10113,
    "PBF balance Morocco": 10122,
    "PBF balance Andorra": 10131,
}

# Aggregations for the thermal gap formula.
NON_THERMAL_TECHS_DEFAULT = [
    "Hydro UGH",
    "Hydro non-UGH",
    "Nuclear",
    "Wind onshore",
    "Wind offshore",
    "Solar PV",
    "Solar thermal",
    "Other renewables",
]

CONVENTIONAL_TECHS_DEFAULT = [
    "Coal sub-bituminous",
    "Coal anthracite",
    "Combined cycle GT",
    "Fuel",
    "Natural gas",
    "Cogeneration",
    "Non-renewable waste",
]


# =========================================================
# Shared helpers copied/adapted from Day Ahead
# =========================================================
def require_esios_token() -> str:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    try:
        token = str(st.secrets.get("ESIOS_TOKEN", "") or st.secrets.get("ESIOS_API_TOKEN", "") or token).strip()
    except Exception:
        pass

    token = token.strip('"').strip("'")
    if not token:
        raise ValueError(f"No token found in {ENV_PATH}. Expected ESIOS_TOKEN.")
    return token


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    """
    Day Ahead convention:
      ESIOS UTC timestamp -> Europe/Madrid -> timezone-naive local timestamp.

    This avoids the browser/Altair applying another timezone conversion.
    """
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    raise ValueError("No datetime column found")


def parse_esios_indicator(raw_json: dict, source_name: str) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df = pd.DataFrame(values)
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    # Spain / national scope, like Day Ahead.
    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    else:
        geo_series = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo_series == "españa").any():
            df = df[geo_series == "españa"].copy()
        elif (geo_series == "espana").any():
            df = df[geo_series == "espana"].copy()

    df["datetime"] = parse_datetime_label(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    df["source"] = source_name
    return df[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
    time_trunc: str = "hour",
    time_agg: str | None = None,
) -> pd.DataFrame:
    """
    Day Ahead style chunked fetch using Madrid local date boundaries converted to UTC.
    """
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    url = f"{BASE}/indicators/{indicator_id}"
    frames = []
    chunk_start = start_day
    chunk_days = 31

    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=chunk_days - 1))

        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(chunk_end + timedelta(days=1), tz="Europe/Madrid")
        start_utc = start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        end_utc = end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

        last_error = None
        for attempt in range(3):
            try:
                resp = requests.get(
                    url,
                    headers=build_headers(token),
                    params={
                        "start_date": start_utc,
                        "end_date": end_utc,
                        "time_trunc": time_trunc,
                        **({"time_agg": time_agg} if time_agg else {}),
                    },
                    timeout=(15, 120),
                )
                resp.raise_for_status()
                parsed = parse_esios_indicator(resp.json(), source_name=f"esios_{indicator_id}")
                if not parsed.empty:
                    frames.append(parsed)
                last_error = None
                break
            except requests.exceptions.RequestException as exc:
                last_error = exc
                sleep(1.5 * (attempt + 1))

        if last_error is not None:
            # Warning only for core data; optional indicators are handled higher up.
            pass

        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["datetime", "geo_id", "source"], keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False)
def load_historical_prices() -> pd.DataFrame:
    """
    Exactly the same source as Day Ahead for historical spot prices.
    """
    if not HIST_PRICES_FILE.exists():
        return pd.DataFrame(columns=["datetime", "price"])

    try:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name=0)
        if "price" not in df.columns and "value" in df.columns:
            df = df.rename(columns={"value": "price"})

    if "datetime" not in df.columns or "price" not in df.columns:
        return pd.DataFrame(columns=["datetime", "price"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["datetime", "price"]).copy()
    df["datetime"] = df["datetime"].dt.floor("h")

    df["price_source"] = f"Day Ahead workbook: {HIST_PRICES_FILE.name}"
    return df[["datetime", "price", "price_source"]].sort_values("datetime").reset_index(drop=True)


def maybe_fix_suspicious_price_scale(prices: pd.Series) -> tuple[pd.Series, str]:
    """
    ESIOS indicator 600 should be €/MWh. In some extraction paths it can arrive 10x higher
    than the Day Ahead page profile for the same day. This guard only applies when the
    whole live series is clearly implausible for the selected range.
    """
    clean = pd.to_numeric(prices, errors="coerce")
    if clean.dropna().empty:
        return clean, "empty"

    median_price = clean.median()
    mean_price = clean.mean()
    max_price = clean.max()

    # A March Spanish DA profile with median > 250 €/MWh and max > 400 €/MWh is almost certainly
    # a scale issue for this workflow, not a normal price profile. Divide by 10.
    if median_price > 250 or (mean_price > 180 and max_price > 350):
        return clean / 10.0, "divided_by_10"

    return clean, "unchanged"


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_prices(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token, time_trunc="hour")
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])

    out = raw[["datetime", "value"]].rename(columns={"value": "price"}).copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["datetime", "price"])

    # Price is €/MWh. Average duplicates; never sum prices.
    out = out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")
    out["price"], scale_status = maybe_fix_suspicious_price_scale(out["price"])
    out["price_source"] = f"ESIOS indicator 600 ({scale_status})"
    return out


def load_prices_like_day_ahead(
    token: str,
    start_day: date,
    end_day: date,
    mode: str,
    auto_fix_scale: bool,
) -> pd.DataFrame:
    """
    Load hourly spot prices with Madrid-local naive timestamps.

    The Day Ahead page builds price_hourly from:
      1) historical workbook
      2) ESIOS indicator 600 for 2026 live data

    This test allows choosing priority because the live ESIOS series can occasionally
    arrive at an unexpected scale in this isolated test page.
    """
    hist = load_historical_prices()
    live_start = max(start_day, LIVE_START_DATE)
    live = pd.DataFrame(columns=["datetime", "price", "price_source"])

    if live_start <= end_day and mode != "Day Ahead workbook only":
        live = load_live_2026_prices(token, live_start, end_day)
        if not auto_fix_scale and not live.empty:
            # Re-fetch without applying the scale guard is not needed here; the guard is conservative.
            # This branch only keeps the UI option explicit.
            pass

    if mode == "Day Ahead workbook only":
        combined = hist.copy()
    elif mode == "ESIOS only":
        combined = live.copy()
    elif mode == "ESIOS first, fill gaps with Day Ahead workbook":
        # Keep ESIOS where overlapping.
        combined = pd.concat([hist, live], ignore_index=True)
        combined["_priority"] = combined["price_source"].astype(str).str.contains("ESIOS", case=False, na=False).astype(int)
        combined = combined.sort_values(["datetime", "_priority"]).drop_duplicates("datetime", keep="last")
        combined = combined.drop(columns=["_priority"])
    else:
        # Default: keep workbook where overlapping, use ESIOS only for missing hours.
        combined = pd.concat([live, hist], ignore_index=True)
        combined["_priority"] = combined["price_source"].astype(str).str.contains("Day Ahead workbook", case=False, na=False).astype(int)
        combined = combined.sort_values(["datetime", "_priority"]).drop_duplicates("datetime", keep="last")
        combined = combined.drop(columns=["_priority"])

    if combined.empty:
        return pd.DataFrame(columns=["datetime", "price", "price_source"])

    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce").dt.floor("h")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    combined = combined.dropna(subset=["datetime", "price"])

    mask = (combined["datetime"].dt.date >= start_day) & (combined["datetime"].dt.date <= end_day)
    return combined.loc[mask, ["datetime", "price", "price_source"]].sort_values("datetime").reset_index(drop=True)


# =========================================================
# PBF fetch/calculation
# =========================================================
def fetch_named_indicators(
    indicators: dict[str, int],
    start_day: date,
    end_day: date,
    token: str,
    *,
    warn_missing: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    frames = []
    missing = []

    progress = st.progress(0, text="Fetching ESIOS PBF indicators...")
    items = list(indicators.items())

    for i, (name, indicator_id) in enumerate(items, start=1):
        try:
            raw = fetch_esios_range(indicator_id, start_day, end_day, token, time_trunc="hour", time_agg="sum")
            if raw.empty:
                missing.append(f"{name} ({indicator_id})")
            else:
                temp = raw[["datetime", "value"]].copy()
                temp["datetime"] = pd.to_datetime(temp["datetime"], errors="coerce").dt.floor("h")
                temp["value"] = pd.to_numeric(temp["value"], errors="coerce")
                temp = temp.dropna(subset=["datetime", "value"])

                # Generation/demand are hourly energy/program values. Sum duplicates.
                temp = temp.groupby("datetime", as_index=False)["value"].sum()
                temp["series"] = name
                temp["indicator_id"] = indicator_id
                frames.append(temp)
        except Exception as exc:
            missing.append(f"{name} ({indicator_id}) error: {exc}")

        progress.progress(i / len(items), text=f"Fetched {i}/{len(items)} PBF indicators")

    progress.empty()

    if warn_missing and missing:
        with st.expander("Missing indicators returned by ESIOS", expanded=False):
            st.write(missing)

    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "series", "indicator_id"]), missing

    return pd.concat(frames, ignore_index=True), missing


def build_wide(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    wide = (
        raw.pivot_table(index="datetime", columns="series", values="value", aggfunc="sum")
        .reset_index()
        .sort_values("datetime")
    )
    wide.columns.name = None
    return wide


def apply_bilateral_netting(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    net PBF tech = gross PBF tech - bilateral PBF tech

    If a bilateral indicator returns no data, it is treated as zero.
    """
    out = wide.copy()
    diag = []

    for tech, gross_id in PBF_GROSS_COMPONENTS.items():
        if tech not in out.columns:
            out[tech] = 0.0

        bilat_col_name = f"{tech} bilateral PBF"
        net_col_name = f"{tech} net PBF"

        bilat_indicator_name = f"Programa bilateral PBF {tech}"
        # Actual column names are nicer Spanish labels below; get them from mapping reverse.
        matching_bilat_cols = []
        for bilat_name, bilat_id in BILATERAL_FETCH_NAMES.items():
            if BILATERAL_TO_GROSS_TECH.get(bilat_name) == tech:
                matching_bilat_cols.append(bilat_name)

        for col in matching_bilat_cols:
            if col not in out.columns:
                out[col] = 0.0

        out[bilat_col_name] = out[matching_bilat_cols].sum(axis=1) if matching_bilat_cols else 0.0
        out[net_col_name] = out[tech] - out[bilat_col_name]

        # For the "PBF - bilaterals" view we do not want tiny negative values due to mismatched revisions.
        # Keep the main thermal gap itself unclipped later.
        out[net_col_name] = out[net_col_name].clip(lower=0)

        gross_sum = out[tech].sum()
        bilat_sum = out[bilat_col_name].sum()
        diag.append(
            {
                "technology": tech,
                "gross_id": gross_id,
                "gross_mwh": gross_sum,
                "bilateral_mwh": bilat_sum,
                "net_mwh": out[net_col_name].sum(),
                "bilateral_share_pct": (bilat_sum / gross_sum * 100) if gross_sum else pd.NA,
                "bilateral_columns": ", ".join(matching_bilat_cols) if matching_bilat_cols else "None / assumed 0",
            }
        )

    return out, pd.DataFrame(diag)


def calculate_thermal_gap(
    wide: pd.DataFrame,
    non_thermal_techs: list[str],
    *,
    subtract_conventional_bilaterals: bool = True,
    include_interconnections: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate a pool / price-setting thermal gap.

    price-setting demand = PBF demand
                          - conventional bilateral PBF programmes
                          + optional PBF net export balance

    thermal gap = price-setting demand - net non-thermal PBF
    """
    netted, diag = apply_bilateral_netting(wide)
    out = netted.copy()

    if "Total scheduled demand PBF" not in out.columns:
        out["Total scheduled demand PBF"] = 0.0

    net_cols = []
    for tech in non_thermal_techs:
        col = f"{tech} net PBF"
        if col not in out.columns:
            out[col] = 0.0
        net_cols.append(col)

    conventional_bilateral_cols = []
    for tech in CONVENTIONAL_TECHS_DEFAULT:
        col = f"{tech} bilateral PBF"
        if col not in out.columns:
            out[col] = 0.0
        conventional_bilateral_cols.append(col)

    out["conventional_bilateral_pbf_mwh"] = (
        out[conventional_bilateral_cols].sum(axis=1) if conventional_bilateral_cols else 0.0
    )
    if not subtract_conventional_bilaterals:
        out["conventional_bilateral_pbf_mwh"] = 0.0

    interconnection_cols = []
    for name in globals().get("PBF_INTERCONNECTION_COMPONENTS", {}).keys():
        if name not in out.columns:
            out[name] = 0.0
        interconnection_cols.append(name)

    out["net_exports_pbf_mwh"] = out[interconnection_cols].sum(axis=1) if interconnection_cols else 0.0
    if not include_interconnections:
        out["net_exports_pbf_mwh"] = 0.0

    out["non_thermal_net_pbf_mwh"] = out[net_cols].sum(axis=1) if net_cols else 0.0
    out["price_setting_demand_pbf_mwh"] = (
        out["Total scheduled demand PBF"]
        - out["conventional_bilateral_pbf_mwh"]
        + out["net_exports_pbf_mwh"]
    )
    out["raw_thermal_gap_mwh"] = out["price_setting_demand_pbf_mwh"] - out["non_thermal_net_pbf_mwh"]

    out["date_madrid"] = out["datetime"].dt.date
    out["hour_madrid"] = out["datetime"].dt.hour
    return out, diag

def calculate_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["month"] = tmp["datetime"].dt.strftime("%Y-%m")

    rows = []
    for month, g in tmp.groupby("month"):
        avg_price = g["price"].mean() if "price" in g.columns else pd.NA
        gap_sum = g["raw_thermal_gap_mwh"].sum()
        weighted_price = (
            (g["price"] * g["raw_thermal_gap_mwh"]).sum() / gap_sum
            if "price" in g.columns and gap_sum != 0
            else pd.NA
        )

        rows.append(
            {
                "month": month,
                "avg_spot_price_eur_mwh": avg_price,
                "avg_thermal_gap_mwh": g["raw_thermal_gap_mwh"].mean(),
                "max_thermal_gap_mwh": g["raw_thermal_gap_mwh"].max(),
                "min_thermal_gap_mwh": g["raw_thermal_gap_mwh"].min(),
                "thermal_gap_mwh_sum": gap_sum,
                "price_weighted_by_gap_eur_mwh": weighted_price,
                "demand_pbf_mwh": g["Total scheduled demand PBF"].sum(),
                "conventional_bilateral_pbf_mwh": g.get("conventional_bilateral_pbf_mwh", pd.Series(0.0, index=g.index)).sum(),
                "net_exports_pbf_mwh": g.get("net_exports_pbf_mwh", pd.Series(0.0, index=g.index)).sum(),
                "price_setting_demand_pbf_mwh": g.get("price_setting_demand_pbf_mwh", g["Total scheduled demand PBF"]).sum(),
                "non_thermal_net_pbf_mwh": g["non_thermal_net_pbf_mwh"].sum(),
                "missing_price_hours": int(g["price"].isna().sum()) if "price" in g.columns else len(g),
            }
        )

    return pd.DataFrame(rows)


# =========================================================
# Balancing energy: upward vs downward
# =========================================================
def _sum_esios_indicators_over_period_gwh(
    indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[float, list[str]]:
    """
    Fetch one or more hourly ESIOS indicators, sum their values over the selected
    period, and convert MWh -> GWh. Values are absolute for volume reporting so
    downward energy is not netted against upward energy.
    """
    total_mwh = 0.0
    missing: list[str] = []

    for indicator_id in indicator_ids:
        raw = fetch_esios_range(
            indicator_id,
            start_day,
            end_day,
            token,
            time_trunc="hour",
            time_agg="sum",
        )
        if raw.empty:
            missing.append(str(indicator_id))
            continue
        vals = pd.to_numeric(raw["value"], errors="coerce").dropna()
        total_mwh += float(vals.abs().sum())

    return total_mwh / 1000.0, missing


def _fetch_hourly_series_for_indicator(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
    *,
    time_agg: str,
) -> pd.DataFrame:
    raw = fetch_esios_range(
        indicator_id,
        start_day,
        end_day,
        token,
        time_trunc="hour",
        time_agg=time_agg,
    )
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "value", "indicator_id"])
    out = raw[["datetime", "value"]].copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["datetime", "value"])
    out = out.groupby("datetime", as_index=False)["value"].mean()
    out["indicator_id"] = indicator_id
    return out


def _build_hourly_volume_mwh(
    indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[pd.DataFrame, list[str]]:
    frames = []
    missing: list[str] = []
    for indicator_id in indicator_ids:
        raw = _fetch_hourly_series_for_indicator(indicator_id, start_day, end_day, token, time_agg="sum")
        if raw.empty:
            missing.append(str(indicator_id))
            continue
        frames.append(raw[["datetime", "value"]].rename(columns={"value": "volume_mwh"}))

    if not frames:
        return pd.DataFrame(columns=["datetime", "volume_mwh"]), missing

    out = pd.concat(frames, ignore_index=True)
    out["volume_mwh"] = pd.to_numeric(out["volume_mwh"], errors="coerce").abs()
    out = out.groupby("datetime", as_index=False)["volume_mwh"].sum()
    return out, missing


def _build_hourly_price_eur_mwh(
    indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[pd.DataFrame, list[str]]:
    frames = []
    missing: list[str] = []
    for indicator_id in indicator_ids:
        raw = _fetch_hourly_series_for_indicator(indicator_id, start_day, end_day, token, time_agg="average")
        if raw.empty:
            missing.append(str(indicator_id))
            continue
        frames.append(raw[["datetime", "value"]].rename(columns={"value": "price_eur_mwh"}))

    if not frames:
        return pd.DataFrame(columns=["datetime", "price_eur_mwh"]), missing

    out = pd.concat(frames, ignore_index=True)
    out["price_eur_mwh"] = pd.to_numeric(out["price_eur_mwh"], errors="coerce")
    # If multiple official price indicators exist for a side, average them by timestamp.
    # For the current mapping this is mainly a robust fallback; most sides use one official price indicator.
    out = out.groupby("datetime", as_index=False)["price_eur_mwh"].mean()
    return out, missing


def _weighted_avg_price_over_period(
    volume_indicator_ids: list[int],
    price_indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[float | None, float | None, list[str]]:
    """Return volume-weighted average €/MWh and implied cost M€.

    If price and volume timestamps do not align, fall back to the simple average of
    the official price series and still estimate cost using total volume.
    """
    if not price_indicator_ids:
        return None, None, []

    volume, miss_vol = _build_hourly_volume_mwh(volume_indicator_ids, start_day, end_day, token)
    price, miss_price = _build_hourly_price_eur_mwh(price_indicator_ids, start_day, end_day, token)
    missing = [f"volume {x}" for x in miss_vol] + [f"price {x}" for x in miss_price]

    if price.empty:
        return None, None, missing

    total_volume_mwh = float(volume["volume_mwh"].sum()) if not volume.empty else 0.0
    merged = volume.merge(price, on="datetime", how="inner") if not volume.empty else pd.DataFrame()
    merged = merged.dropna(subset=["volume_mwh", "price_eur_mwh"]) if not merged.empty else merged

    if not merged.empty and merged["volume_mwh"].sum() > 0:
        avg_price = float((merged["volume_mwh"] * merged["price_eur_mwh"]).sum() / merged["volume_mwh"].sum())
    else:
        avg_price = float(price["price_eur_mwh"].dropna().mean())

    cost_meur = (total_volume_mwh * avg_price) / 1_000_000.0 if total_volume_mwh else None
    return avg_price, cost_meur, missing


def _avg_esios_indicators_over_period_mw(
    indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[float | None, list[str]]:
    """Fetch one or more hourly ESIOS indicators and return their average MW."""
    frames = []
    missing: list[str] = []

    for indicator_id in indicator_ids:
        raw = fetch_esios_range(
            indicator_id,
            start_day,
            end_day,
            token,
            time_trunc="hour",
            time_agg="average",
        )
        if raw.empty:
            missing.append(str(indicator_id))
            continue
        frames.append(pd.to_numeric(raw["value"], errors="coerce"))

    if not frames:
        return None, missing

    vals = pd.concat(frames, ignore_index=True).dropna()
    if vals.empty:
        return None, missing
    return float(vals.mean()), missing


@st.cache_data(show_spinner=False, ttl=3600)
def build_hourly_price_profile(
    profile_name: str,
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[pd.DataFrame, dict[str, Any], list[str]]:
    """Return H1-H24 average price profile for an official ESIOS price profile section."""
    config = globals().get("BALANCING_HOURLY_PRICE_PROFILE_SECTIONS", {}).get(profile_name, {})
    unit = str(config.get("unit", ""))
    published_unit = str(config.get("published_unit", unit))
    multiplier = float(config.get("multiplier", 1.0) or 1.0)
    series_map = config.get("series", {}) or {}

    rows: list[dict[str, Any]] = []
    missing: list[str] = []

    for direction, indicator_ids in series_map.items():
        frames = []
        for indicator_id in indicator_ids:
            raw = fetch_esios_range(
                int(indicator_id),
                start_day,
                end_day,
                token,
                time_trunc="hour",
                time_agg="average",
            )
            if raw.empty:
                missing.append(f"{profile_name} {direction}: {indicator_id}")
                continue
            tmp = raw[["datetime", "value"]].copy()
            tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
            tmp["published_value"] = pd.to_numeric(tmp["value"], errors="coerce")
            tmp = tmp.dropna(subset=["datetime", "published_value"])
            tmp["indicator_id"] = str(indicator_id)
            frames.append(tmp[["datetime", "published_value", "indicator_id"]])

        if not frames:
            continue

        temp = pd.concat(frames, ignore_index=True)
        temp["hour"] = temp["datetime"].dt.hour + 1
        temp["value"] = temp["published_value"] * multiplier
        profile = (
            temp.groupby("hour", as_index=False)
            .agg(
                value=("value", "mean"),
                published_value=("published_value", "mean"),
                obs=("value", "count"),
            )
        )
        ids_label = ", ".join(map(str, indicator_ids))
        for rec in profile.to_dict("records"):
            rows.append(
                {
                    "profile": profile_name,
                    "direction": direction,
                    "hour": int(rec["hour"]),
                    "hour_label": f"H{int(rec['hour']):02d}",
                    "value": float(rec["value"]),
                    "published_value": float(rec["published_value"]),
                    "unit": unit,
                    "published_unit": published_unit,
                    "obs": int(rec["obs"]),
                    "indicator_ids": ids_label,
                }
            )

    out = pd.DataFrame(rows)
    meta = {
        "profile": profile_name,
        "unit": unit,
        "published_unit": published_unit,
        "multiplier": multiplier,
        "series": series_map,
    }
    return out, meta, missing


def plot_hourly_price_profile_altair(profile_df: pd.DataFrame, meta: dict[str, Any]) -> alt.Chart:
    """Static H1-H24 line chart sized to fit inside the Streamlit content column."""
    title = str(meta.get("profile", "Hourly price profile"))
    unit = str(meta.get("unit", ""))
    published_unit = str(meta.get("published_unit", unit))
    multiplier = float(meta.get("multiplier", 1.0) or 1.0)

    if profile_df.empty:
        return alt.Chart(pd.DataFrame({"hour": [], "value": []})).mark_line()

    plot = profile_df.copy()
    order = [s for s in ["Upward", "Downward"] if s in plot["direction"].dropna().astype(str).unique().tolist()]
    colors = [BLUE, GREY_DARK][: len(order)] if order else [BLUE, GREY_DARK]
    dashes = [[1, 0], [5, 3]][: len(order)] if order else [[1, 0], [5, 3]]

    y_title = f"Average price ({unit})" if unit else "Average price"
    subtitle = "Average by delivery hour over the selected period"
    if abs(multiplier - 1.0) > 1e-9:
        subtitle = f"Average by delivery hour over the selected period | published {published_unit} × {multiplier:g} = {unit}"

    chart = alt.Chart(plot).mark_line(
        point=alt.OverlayMarkDef(filled=True, size=42),
        strokeWidth=2.6,
    ).encode(
        x=alt.X(
            "hour:Q",
            title="Hour",
            scale=alt.Scale(domain=[1, 24], nice=False),
            axis=alt.Axis(values=list(range(1, 25)), labelAngle=0, labelFontSize=10),
        ),
        y=alt.Y(
            "value:Q",
            title=y_title,
            scale=alt.Scale(zero=False),
        ),
        color=alt.Color(
            "direction:N",
            title="Direction",
            scale=alt.Scale(domain=order, range=colors),
            legend=alt.Legend(orient="top", direction="horizontal", columns=3, labelLimit=420, titleLimit=420),
        ),
        strokeDash=alt.StrokeDash(
            "direction:N",
            title="Direction",
            scale=alt.Scale(domain=order, range=dashes),
            legend=None,
        ),
    ).properties(
        title=f"{title} | H1-H24",
        width=980,
    )

    return apply_monthly_report_chart_style(chart, height=330)


def build_hourly_price_profile_table(profile_df: pd.DataFrame, meta: dict[str, Any]) -> pd.DataFrame:
    """Compact visible table under each static chart so the page does not create horizontal overflow."""
    if profile_df is None or profile_df.empty:
        return pd.DataFrame()

    unit = str(meta.get("unit", ""))

    tmp = profile_df.copy()
    tmp["value"] = pd.to_numeric(tmp["value"], errors="coerce")
    tmp["obs"] = pd.to_numeric(tmp["obs"], errors="coerce")

    value_wide = tmp.pivot(index="hour", columns="direction", values="value") if not tmp.empty else pd.DataFrame()
    obs_wide = tmp.pivot(index="hour", columns="direction", values="obs") if not tmp.empty else pd.DataFrame()

    hours = list(range(1, 25))
    rows = []
    for h in hours:
        rows.append({
            "Hour": f"H{h:02d}",
            f"Upward avg ({unit})": value_wide.loc[h, "Upward"] if (not value_wide.empty and "Upward" in value_wide.columns and h in value_wide.index) else pd.NA,
            f"Downward avg ({unit})": value_wide.loc[h, "Downward"] if (not value_wide.empty and "Downward" in value_wide.columns and h in value_wide.index) else pd.NA,
            "Upward obs": obs_wide.loc[h, "Upward"] if (not obs_wide.empty and "Upward" in obs_wide.columns and h in obs_wide.index) else pd.NA,
            "Downward obs": obs_wide.loc[h, "Downward"] if (not obs_wide.empty and "Downward" in obs_wide.columns and h in obs_wide.index) else pd.NA,
        })

    out = pd.DataFrame(rows)
    for col in out.columns:
        if col.startswith("Upward avg") or col.startswith("Downward avg"):
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)
        if col.endswith("obs"):
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    return out


@st.cache_data(show_spinner=False, ttl=3600)
def build_balancing_energy_summary(
    start_day: date,
    end_day: date,
    token: str,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Build the summary tables needed to reproduce the balancing-energy chart.
    Upward and downward are kept as separate positive volumes; only the plot draws
    downward below zero.
    """
    rows = []
    missing: list[str] = []

    for category, sides in BALANCING_ENERGY_SECTIONS.items():
        up_ids = sides.get("up", [])
        down_ids = sides.get("down", [])
        price_sides = globals().get("BALANCING_PRICE_SECTIONS", {}).get(category, {"up": [], "down": []})

        up_gwh, miss_up = _sum_esios_indicators_over_period_gwh(up_ids, start_day, end_day, token)
        down_gwh, miss_down = _sum_esios_indicators_over_period_gwh(down_ids, start_day, end_day, token)
        up_avg_price, up_cost_meur, miss_up_price = _weighted_avg_price_over_period(
            up_ids, price_sides.get("up", []), start_day, end_day, token
        )
        down_avg_price, down_cost_meur, miss_down_price = _weighted_avg_price_over_period(
            down_ids, price_sides.get("down", []), start_day, end_day, token
        )

        missing.extend([f"{category} upward volume: {x}" for x in miss_up])
        missing.extend([f"{category} downward volume: {x}" for x in miss_down])
        missing.extend([f"{category} upward price: {x}" for x in miss_up_price])
        missing.extend([f"{category} downward price: {x}" for x in miss_down_price])
        rows.append(
            {
                "category": category,
                "upward_gwh": up_gwh,
                "downward_gwh": down_gwh,
                "upward_avg_price_eur_mwh": up_avg_price,
                "downward_avg_price_eur_mwh": down_avg_price,
                "upward_cost_meur": up_cost_meur,
                "downward_cost_meur": down_cost_meur,
                "upward_price_ids": ", ".join(map(str, price_sides.get("up", []))) or "n/a",
                "downward_price_ids": ", ".join(map(str, price_sides.get("down", []))) or "n/a",
            }
        )

    reserve_rows = []
    reserve_price_map = globals().get("BALANCING_SECONDARY_RESERVE_PRICE_IDS", {})
    for direction, ids in BALANCING_SECONDARY_RESERVE_IDS.items():
        avg_mw, miss = _avg_esios_indicators_over_period_mw(ids, start_day, end_day, token)
        avg_price_eur_mw, miss_price = _avg_esios_indicators_over_period_mw(
            reserve_price_map.get(direction, []), start_day, end_day, token
        )
        missing.extend([f"Secondary reserve {direction}: {x}" for x in miss])
        missing.extend([f"Secondary reserve price {direction}: {x}" for x in miss_price])
        reserve_rows.append(
            {
                "direction": direction,
                "avg_mw": avg_mw,
                "avg_price_eur_mw_15min": avg_price_eur_mw,
                "avg_price_eur_mw_h": (avg_price_eur_mw * 4.0) if avg_price_eur_mw is not None else None,
                "price_ids": ", ".join(map(str, reserve_price_map.get(direction, []))) or "n/a",
            }
        )

    energy = pd.DataFrame(rows)
    reserve = pd.DataFrame(reserve_rows)
    return energy, reserve, missing


def build_balancing_indicator_breakdown(
    start_day: date,
    end_day: date,
    token: str,
) -> pd.DataFrame:
    """Detailed per-indicator breakdown to debug methodology differences."""
    rows: list[dict[str, Any]] = []

    for category, sides in BALANCING_ENERGY_SECTIONS.items():
        for direction_key, ids in [("Upward", sides.get("up", [])), ("Downward", sides.get("down", []))]:
            for indicator_id in ids:
                raw = fetch_esios_range(
                    indicator_id,
                    start_day,
                    end_day,
                    token,
                    time_trunc="hour",
                    time_agg="sum",
                )
                vals = pd.to_numeric(raw["value"], errors="coerce").dropna() if not raw.empty else pd.Series(dtype=float)
                rows.append(
                    {
                        "table": "energy",
                        "category": category,
                        "direction": direction_key,
                        "indicator_id": indicator_id,
                        "value": float(vals.abs().sum()) / 1000.0 if not vals.empty else pd.NA,
                        "unit": "GWh",
                        "rows": int(len(vals)),
                    }
                )

        price_sides = globals().get("BALANCING_PRICE_SECTIONS", {}).get(category, {"up": [], "down": []})
        for direction_key, ids in [("Upward", price_sides.get("up", [])), ("Downward", price_sides.get("down", []))]:
            for indicator_id in ids:
                raw = fetch_esios_range(
                    indicator_id,
                    start_day,
                    end_day,
                    token,
                    time_trunc="hour",
                    time_agg="average",
                )
                vals = pd.to_numeric(raw["value"], errors="coerce").dropna() if not raw.empty else pd.Series(dtype=float)
                rows.append(
                    {
                        "table": "price",
                        "category": category,
                        "direction": direction_key,
                        "indicator_id": indicator_id,
                        "value": float(vals.mean()) if not vals.empty else pd.NA,
                        "unit": "€/MWh",
                        "rows": int(len(vals)),
                    }
                )

    for direction_key, ids in BALANCING_SECONDARY_RESERVE_IDS.items():
        for indicator_id in ids:
            raw = fetch_esios_range(
                indicator_id,
                start_day,
                end_day,
                token,
                time_trunc="hour",
                time_agg="average",
            )
            vals = pd.to_numeric(raw["value"], errors="coerce").dropna() if not raw.empty else pd.Series(dtype=float)
            rows.append(
                {
                    "table": "secondary_reserve",
                    "category": "Average secondary reserve",
                    "direction": direction_key,
                    "indicator_id": indicator_id,
                    "value": float(vals.mean()) if not vals.empty else pd.NA,
                    "unit": "MW",
                    "rows": int(len(vals)),
                }
            )

    for direction_key, ids in globals().get("BALANCING_SECONDARY_RESERVE_PRICE_IDS", {}).items():
        for indicator_id in ids:
            raw = fetch_esios_range(
                indicator_id,
                start_day,
                end_day,
                token,
                time_trunc="hour",
                time_agg="average",
            )
            vals = pd.to_numeric(raw["value"], errors="coerce").dropna() if not raw.empty else pd.Series(dtype=float)
            rows.append(
                {
                    "table": "secondary_reserve_price",
                    "category": "Average secondary reserve band price",
                    "direction": direction_key,
                    "indicator_id": indicator_id,
                    "value": float(vals.mean()) if not vals.empty else pd.NA,
                    "unit": "€/MW/15min",
                    "rows": int(len(vals)),
                }
            )

    return pd.DataFrame(rows)


def plot_balancing_energy_summary_altair(energy: pd.DataFrame, reserve: pd.DataFrame, title_suffix: str = "") -> alt.Chart:
    """Interactive Altair version with hover tooltips."""
    energy_plot = energy.copy()
    energy_plot["upward_gwh"] = pd.to_numeric(energy_plot["upward_gwh"], errors="coerce")
    energy_plot["downward_gwh"] = pd.to_numeric(energy_plot["downward_gwh"], errors="coerce")

    long_rows = []
    for _, row in energy_plot.iterrows():
        up_gwh = pd.to_numeric(row.get("upward_gwh"), errors="coerce")
        down_gwh = pd.to_numeric(row.get("downward_gwh"), errors="coerce")
        up_price = pd.to_numeric(row.get("upward_avg_price_eur_mwh"), errors="coerce")
        down_price = pd.to_numeric(row.get("downward_avg_price_eur_mwh"), errors="coerce")
        up_cost = pd.to_numeric(row.get("upward_cost_meur"), errors="coerce")
        down_cost = pd.to_numeric(row.get("downward_cost_meur"), errors="coerce")

        long_rows.append({
            "category": row.get("category"),
            "direction": "Upward",
            "value_gwh": float(up_gwh) if pd.notna(up_gwh) else 0.0,
            "plot_value": float(up_gwh) if pd.notna(up_gwh) else 0.0,
            "avg_price_eur_mwh": float(up_price) if pd.notna(up_price) else None,
            "cost_meur": float(up_cost) if pd.notna(up_cost) else None,
            "price_ids": row.get("upward_price_ids", "n/a"),
            "label": (f"{float(up_gwh):,.0f} GWh | {float(up_price):,.0f} €/MWh" if pd.notna(up_gwh) and pd.notna(up_price) else (f"{float(up_gwh):,.0f} GWh" if pd.notna(up_gwh) else "")),
        })
        long_rows.append({
            "category": row.get("category"),
            "direction": "Downward",
            "value_gwh": float(down_gwh) if pd.notna(down_gwh) else 0.0,
            "plot_value": -float(down_gwh) if pd.notna(down_gwh) else 0.0,
            "avg_price_eur_mwh": float(down_price) if pd.notna(down_price) else None,
            "cost_meur": float(down_cost) if pd.notna(down_cost) else None,
            "price_ids": row.get("downward_price_ids", "n/a"),
            "label": (f"{float(down_gwh):,.0f} GWh | {float(down_price):,.0f} €/MWh" if pd.notna(down_gwh) and pd.notna(down_price) else (f"{float(down_gwh):,.0f} GWh" if pd.notna(down_gwh) else "")),
        })

    energy_long = pd.DataFrame(long_rows)
    energy_long["label_y"] = energy_long.apply(
        lambda r: r["plot_value"] + max(abs(r["plot_value"]) * 0.045, 35.0) if r["plot_value"] >= 0 else r["plot_value"] - max(abs(r["plot_value"]) * 0.055, 45.0),
        axis=1,
    )
    max_energy = max(energy_long["value_gwh"].max() if not energy_long.empty else 0.0, 1.0)
    energy_domain = [-max_energy * 1.22, max_energy * 1.18]

    color_scale = alt.Scale(domain=["Upward", "Downward"], range=["#0F7F73", "#C81046"])
    x_sort = energy["category"].tolist()

    energy_base = alt.Chart(energy_long).encode(
        x=alt.X("category:N", sort=x_sort, axis=alt.Axis(title=None, labelAngle=-18, labelLimit=260, labelFontSize=13)),
        color=alt.Color("direction:N", scale=color_scale, legend=alt.Legend(orient="bottom", direction="horizontal", title=None)),
    )

    energy_bars = energy_base.mark_bar().encode(
        y=alt.Y(
            "plot_value:Q",
            title="GWh (+ upward / - downward)",
            scale=alt.Scale(domain=energy_domain),
        ),
        tooltip=[
            alt.Tooltip("category:N", title="Category"),
            alt.Tooltip("direction:N", title="Direction"),
            alt.Tooltip("value_gwh:Q", title="Volume (GWh)", format=",.0f"),
            alt.Tooltip("avg_price_eur_mwh:Q", title="Avg price (€/MWh)", format=",.2f"),
            alt.Tooltip("cost_meur:Q", title="Implied cost (M€)", format=",.1f"),
            alt.Tooltip("price_ids:N", title="Price indicator IDs"),
        ],
    )

    # Labels are split by direction so downward volume/price labels sit clearly below the red bars.
    energy_text_up = energy_base.transform_filter(
        alt.datum.direction == "Upward"
    ).mark_text(fontSize=12, color="#0F7F73", fontWeight="bold", dy=-4).encode(
        y=alt.Y("label_y:Q", scale=alt.Scale(domain=energy_domain)),
        text="label:N",
    )
    energy_text_down = energy_base.transform_filter(
        alt.datum.direction == "Downward"
    ).mark_text(fontSize=12, color="#C81046", fontWeight="bold", dy=8).encode(
        y=alt.Y("label_y:Q", scale=alt.Scale(domain=energy_domain)),
        text="label:N",
    )

    zero_rule = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="#AEB6BF").encode(y="y:Q")

    energy_chart = (
        (zero_rule + energy_bars + energy_text_up + energy_text_down)
        .properties(
            width=980,
            height=560,
            title=alt.TitleParams(
                text=f"Upward and downward balancing energy should not be blended{title_suffix}",
                subtitle=["Balancing energy volume: upward vs downward"],
                anchor="start",
                fontSize=20,
                subtitleFontSize=15,
            ),
        )
        .interactive()
    )

    reserve_plot = reserve.copy()
    reserve_plot["avg_mw"] = pd.to_numeric(reserve_plot["avg_mw"], errors="coerce")
    # ESIOS/balancing-services-style capacity price can be understood as €/MW/15min.
    # Display the hourly equivalent in the chart label: €/MW/h = €/MW/15min × 4.
    if "avg_price_eur_mw_h" not in reserve_plot.columns:
        if "avg_price_eur_mw_15min" in reserve_plot.columns:
            reserve_plot["avg_price_eur_mw_h"] = pd.to_numeric(reserve_plot["avg_price_eur_mw_15min"], errors="coerce") * 4.0
        elif "avg_price_eur_mw" in reserve_plot.columns:
            reserve_plot["avg_price_eur_mw_h"] = pd.to_numeric(reserve_plot["avg_price_eur_mw"], errors="coerce") * 4.0
        else:
            reserve_plot["avg_price_eur_mw_h"] = pd.NA
    reserve_plot["avg_price_eur_mw_h"] = pd.to_numeric(reserve_plot["avg_price_eur_mw_h"], errors="coerce")
    if "avg_price_eur_mw_15min" in reserve_plot.columns:
        reserve_plot["avg_price_eur_mw_15min"] = pd.to_numeric(reserve_plot["avg_price_eur_mw_15min"], errors="coerce")
    else:
        reserve_plot["avg_price_eur_mw_15min"] = reserve_plot["avg_price_eur_mw_h"] / 4.0
    reserve_plot["label"] = reserve_plot.apply(
        lambda r: (
            f"{r['avg_mw']:,.0f} MW | {r['avg_price_eur_mw_h']:,.2f} €/MW/h"
            if pd.notna(r.get("avg_mw")) and pd.notna(r.get("avg_price_eur_mw_h"))
            else (f"{r['avg_mw']:,.0f} MW" if pd.notna(r.get("avg_mw")) else "")
        ),
        axis=1,
    )
    reserve_plot["label_y"] = reserve_plot["avg_mw"].fillna(0) + reserve_plot["avg_mw"].fillna(0).clip(lower=1) * 0.035
    reserve_max = max(reserve_plot["avg_mw"].max(skipna=True) if not reserve_plot.empty else 0.0, 1.0)

    reserve_base = alt.Chart(reserve_plot).encode(
        x=alt.X("direction:N", sort=["Downward", "Upward"], axis=alt.Axis(title=None, labelAngle=0, labelFontSize=13)),
        color=alt.Color(
            "direction:N",
            scale=alt.Scale(domain=["Downward", "Upward"], range=["#1CA7DF", "#0B6FA4"]),
            legend=None,
        ),
    )
    reserve_bars = reserve_base.mark_bar().encode(
        y=alt.Y("avg_mw:Q", title="MW", scale=alt.Scale(domain=[0, reserve_max * 1.12])),
        tooltip=[
            alt.Tooltip("direction:N", title="Direction"),
            alt.Tooltip("avg_mw:Q", title="Average reserve (MW)", format=",.0f"),
            alt.Tooltip("avg_price_eur_mw_h:Q", title="Avg band price hourly eq. (€/MW/h)", format=",.2f"),
            alt.Tooltip("avg_price_eur_mw_15min:Q", title="Published / 15-min price (€/MW/15min)", format=",.2f"),
            alt.Tooltip("price_ids:N", title="Price indicator IDs"),
        ],
    )
    reserve_text = reserve_base.mark_text(fontSize=13, color="#222222", fontWeight="bold", dy=-4).encode(
        y=alt.Y("label_y:Q", scale=alt.Scale(domain=[0, reserve_max * 1.12])),
        text="label:N",
    )
    reserve_chart = (
        (reserve_bars + reserve_text)
        .properties(
            width=430,
            height=560,
            title=alt.TitleParams(text="Average secondary reserve and directional band prices", anchor="middle", fontSize=16),
        )
        .interactive()
    )

    return (
        alt.hconcat(energy_chart, reserve_chart, spacing=35)
        .resolve_scale(color="independent")
        .configure_axis(labelFontSize=12, titleFontSize=14)
        .configure_legend(labelFontSize=13, titleFontSize=13)
        .configure_view(strokeWidth=0)
    )


def plot_balancing_energy_summary(energy: pd.DataFrame, reserve: pd.DataFrame, title_suffix: str = "") -> plt.Figure:
    """Matplotlib version of the slide-style balancing chart."""
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(15, 5.6),
        gridspec_kw={"width_ratios": [2.35, 1]},
        constrained_layout=True,
    )

    fig.suptitle(
        f"Upward and downward balancing energy should not be blended{title_suffix}",
        x=0.02,
        ha="left",
        fontsize=16,
    )

    x = list(range(len(energy)))
    up = pd.to_numeric(energy["upward_gwh"], errors="coerce").fillna(0.0)
    down = pd.to_numeric(energy["downward_gwh"], errors="coerce").fillna(0.0)

    axes[0].bar(x, up, color="#0F7F73", label="Upward")
    axes[0].bar(x, -down, color="#C81046", label="Downward")
    axes[0].axhline(0, color="#AEB6BF", linewidth=1)
    axes[0].set_title("Balancing energy volume: upward vs downward")
    axes[0].set_ylabel("GWh (+ upward / - downward)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(energy["category"].tolist(), rotation=13, ha="right")
    axes[0].grid(axis="y", alpha=0.22)
    axes[0].spines[["top", "right"]].set_visible(False)
    axes[0].legend(loc="lower center", bbox_to_anchor=(0.73, -0.28), ncol=2, frameon=False)

    y_span = max(float(up.max() if len(up) else 0), float(down.max() if len(down) else 0), 1.0)
    for i, val in enumerate(up):
        if abs(val) > 1e-9:
            axes[0].text(i, val + y_span * 0.025, f"{val:,.0f} GWh", ha="center", va="bottom", fontsize=9)
    for i, val in enumerate(down):
        if abs(val) > 1e-9:
            axes[0].text(i, -val - y_span * 0.025, f"{val:,.0f} GWh", ha="center", va="top", fontsize=9)

    reserve_plot = reserve.copy()
    reserve_plot["avg_mw"] = pd.to_numeric(reserve_plot["avg_mw"], errors="coerce")
    axes[1].bar(reserve_plot["direction"], reserve_plot["avg_mw"], color=["#1CA7DF", "#0B6FA4"])
    axes[1].set_title("Average secondary reserve")
    axes[1].set_ylabel("MW")
    axes[1].grid(axis="y", alpha=0.22)
    axes[1].spines[["top", "right"]].set_visible(False)

    reserve_max = reserve_plot["avg_mw"].max(skipna=True)
    reserve_span = float(reserve_max) if pd.notna(reserve_max) and reserve_max else 1.0
    for i, val in enumerate(reserve_plot["avg_mw"]):
        if pd.notna(val):
            axes[1].text(i, val + reserve_span * 0.015, f"{val:,.0f} MW", ha="center", va="bottom", fontsize=9)

    return fig


# Nice bilingual names for bilaterals. These columns are fetched and mapped back to gross techs.
BILATERAL_FETCH_NAMES = {
    "Programa bilateral PBF Hidráulica UGH": 421,
    "Programa bilateral PBF Hidráulica no UGH": 422,
    "Programa bilateral PBF Nuclear": 424,
    "Programa bilateral PBF Hulla sub-bituminosa": 426,
    "Programa bilateral PBF Hulla antracita": 427,
    "Programa bilateral PBF Ciclo combinado": 429,
    "Programa bilateral PBF Eólica terrestre": 432,
    "Programa bilateral PBF Eólica marina": 433,
    "Programa bilateral PBF Otras renovables": 10234,
}
BILATERAL_TO_GROSS_TECH = {
    "Programa bilateral PBF Hidráulica UGH": "Hydro UGH",
    "Programa bilateral PBF Hidráulica no UGH": "Hydro non-UGH",
    "Programa bilateral PBF Nuclear": "Nuclear",
    "Programa bilateral PBF Hulla sub-bituminosa": "Coal sub-bituminous",
    "Programa bilateral PBF Hulla antracita": "Coal anthracite",
    "Programa bilateral PBF Ciclo combinado": "Combined cycle GT",
    "Programa bilateral PBF Eólica terrestre": "Wind onshore",
    "Programa bilateral PBF Eólica marina": "Wind offshore",
    "Programa bilateral PBF Otras renovables": "Other renewables",
}




# =========================================================
# Official restrictions detail technology breakdown
# =========================================================
def _norm_col_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def _read_tabular_upload(uploaded_file: Any) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame()
    name = str(getattr(uploaded_file, "name", "")).lower()
    data = uploaded_file.getvalue()
    try:
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(BytesIO(data), sheet_name=0)
    except Exception:
        pass

    # ESIOS downloads are commonly ; separated and can use utf-8-sig or latin1.
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        for sep in (";", ",", "\t", "|"):
            try:
                df = pd.read_csv(BytesIO(data), sep=sep, encoding=enc)
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue
    return pd.DataFrame()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_norm_col_name(c) for c in out.columns]
    return out


def _find_col(columns: list[str], include_any: list[str], include_all: list[str] | None = None, exclude_any: list[str] | None = None) -> str | None:
    include_all = include_all or []
    exclude_any = exclude_any or []
    for col in columns:
        if include_any and not any(token in col for token in include_any):
            continue
        if include_all and not all(token in col for token in include_all):
            continue
        if exclude_any and any(token in col for token in exclude_any):
            continue
        return col
    return None


def _parse_energy_mwh(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").abs()


def _parse_price(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _direction_matches(series: pd.Series, direction: str) -> pd.Series:
    direction = direction.lower()
    text = series.astype(str).map(_norm_col_name)
    if direction == "upward":
        return text.str.contains("subir|upward|up", regex=True, na=False)
    return text.str.contains("bajar|downward|down", regex=True, na=False)


def _phase_matches(series: pd.Series, category: str) -> pd.Series:
    text = series.astype(str).map(_norm_col_name)
    if "Phase I" in category:
        return text.str.contains(r"fase_?i$|fase_?1|phase_?i$|phase_?1", regex=True, na=False)
    if "Phase II" in category:
        return text.str.contains(r"fase_?ii|fase_?2|phase_?ii|phase_?2", regex=True, na=False)
    return pd.Series(True, index=series.index)


def build_technology_breakdown_from_official_files(
    restrictions_df: pd.DataFrame,
    mapping_df: pd.DataFrame | None,
    category: str,
    direction: str,
) -> tuple[pd.DataFrame, list[str]]:
    """Aggregate official detailed restrictions rows by technology.

    This does not infer technologies from aggregate ESIOS indicators. It uses a detailed
    official restrictions file and, when needed, an official structural UP/UF mapping file.
    """
    notes: list[str] = []
    if restrictions_df is None or restrictions_df.empty:
        return pd.DataFrame(), ["No restrictions detail file loaded."]

    df = _normalize_columns(restrictions_df)
    cols = df.columns.tolist()

    energy_col = _find_col(cols, ["energia", "energy", "mwh"], exclude_any=["precio", "price"])
    price_col = _find_col(cols, ["precio", "price", "eur", "euro"])
    tech_col = _find_col(cols, ["tecnologia", "technology", "tipo_produccion", "combustible", "fuel"])
    unit_col = _find_col(cols, ["unidad_programacion", "up", "unidad", "programming_unit", "codigo_unidad"], exclude_any=["fisica", "physical"])
    direction_col = _find_col(cols, ["sentido", "direction", "tipo_asignacion", "tipo"])
    phase_col = _find_col(cols, ["fase", "phase", "proceso", "mercado"])

    if energy_col is None:
        return pd.DataFrame(), [f"Could not detect an energy/MWh column. Columns found: {cols}"]

    if direction_col is not None:
        mask = _direction_matches(df[direction_col], direction)
        if mask.any():
            df = df.loc[mask].copy()
        else:
            notes.append(f"Direction column '{direction_col}' found but no rows matched {direction}; no direction filter applied.")
    else:
        notes.append("No direction/sentido column found; using all rows.")

    if phase_col is not None and ("Phase I" in category or "Phase II" in category):
        mask = _phase_matches(df[phase_col], category)
        if mask.any():
            df = df.loc[mask].copy()
        else:
            notes.append(f"Phase column '{phase_col}' found but no rows matched {category}; no phase filter applied.")
    elif "Phase" in category:
        notes.append("No phase/fase column found; using all rows in the uploaded file. Upload a file already filtered to the phase if needed.")

    # Add technology by mapping file if not present directly.
    if tech_col is None and mapping_df is not None and not mapping_df.empty and unit_col is not None:
        map_df = _normalize_columns(mapping_df)
        mcols = map_df.columns.tolist()
        map_unit_col = _find_col(mcols, ["unidad_programacion", "up", "unidad", "programming_unit", "codigo_unidad"], exclude_any=["fisica", "physical"])
        map_tech_col = _find_col(mcols, ["tecnologia", "technology", "tipo_produccion", "combustible", "fuel"])
        if map_unit_col is not None and map_tech_col is not None:
            left = df.copy()
            left["_unit_key"] = left[unit_col].astype(str).str.strip().str.upper()
            right = map_df[[map_unit_col, map_tech_col]].copy()
            right["_unit_key"] = right[map_unit_col].astype(str).str.strip().str.upper()
            right = right.drop_duplicates("_unit_key")
            df = left.merge(right[["_unit_key", map_tech_col]], on="_unit_key", how="left")
            tech_col = map_tech_col
            notes.append("Technology taken from uploaded official structural mapping file.")
        else:
            notes.append("Mapping file loaded, but unit/technology columns could not be detected.")

    if tech_col is None:
        if unit_col is not None:
            tech_col = unit_col
            notes.append("No technology column/mapping found; grouping by programming unit instead.")
        else:
            df["technology"] = "Unknown / unmapped"
            tech_col = "technology"
            notes.append("No technology or unit column found; rows grouped as Unknown / unmapped.")

    df["_energy_mwh"] = _parse_energy_mwh(df[energy_col])
    df["_price_eur_mwh"] = _parse_price(df[price_col]) if price_col is not None else pd.NA
    df["_technology"] = df[tech_col].astype(str).replace({"nan": "Unknown / unmapped", "None": "Unknown / unmapped"}).fillna("Unknown / unmapped")
    df = df.dropna(subset=["_energy_mwh"]).copy()

    if df.empty:
        return pd.DataFrame(), notes + ["No rows with numeric energy after filters."]

    rows = []
    for tech, g in df.groupby("_technology", dropna=False):
        energy_mwh = float(g["_energy_mwh"].sum())
        price_vals = pd.to_numeric(g["_price_eur_mwh"], errors="coerce")
        if price_vals.notna().any() and energy_mwh > 0:
            weighted_price = float((g["_energy_mwh"] * price_vals.fillna(0)).sum() / g.loc[price_vals.notna(), "_energy_mwh"].sum()) if g.loc[price_vals.notna(), "_energy_mwh"].sum() > 0 else pd.NA
        else:
            weighted_price = pd.NA
        rows.append({
            "technology": str(tech),
            "energy_gwh": energy_mwh / 1000.0,
            "avg_price_eur_mwh": weighted_price,
            "cost_meur": (energy_mwh * weighted_price / 1_000_000.0) if pd.notna(weighted_price) else pd.NA,
            "rows": int(len(g)),
        })

    out = pd.DataFrame(rows).sort_values("energy_gwh", ascending=False).reset_index(drop=True)
    return out, notes


def plot_technology_breakdown_altair(tech_df: pd.DataFrame, category: str, direction: str) -> alt.Chart:
    data = tech_df.copy()
    data["energy_gwh"] = pd.to_numeric(data["energy_gwh"], errors="coerce")
    data["avg_price_eur_mwh"] = pd.to_numeric(data["avg_price_eur_mwh"], errors="coerce")
    data["cost_meur"] = pd.to_numeric(data["cost_meur"], errors="coerce")
    return alt.Chart(data).mark_bar().encode(
        y=alt.Y("technology:N", sort="-x", title=None),
        x=alt.X("energy_gwh:Q", title="Energy (GWh)"),
        tooltip=[
            alt.Tooltip("technology:N", title="Technology / unit"),
            alt.Tooltip("energy_gwh:Q", title="Energy (GWh)", format=",.1f"),
            alt.Tooltip("avg_price_eur_mwh:Q", title="Avg price (€/MWh)", format=",.2f"),
            alt.Tooltip("cost_meur:Q", title="Cost estimate (M€)", format=",.2f"),
            alt.Tooltip("rows:Q", title="Rows", format=",d"),
        ],
    ).properties(
        height=max(260, min(720, 28 * len(data))),
        title=alt.TitleParams(
            text=f"Technology breakdown — {category} {direction.lower()}",
            subtitle="Built from official detailed restrictions file, not from aggregate indicator IDs",
            anchor="start",
        ),
    ).interactive()

# =========================================================
# REE public demand profile helpers
# Adapted from the working Embalses + demand test.
# =========================================================
def safe_json_response(resp: requests.Response) -> dict | list | str:
    try:
        return resp.json()
    except Exception:
        return (resp.text or "")[:2000]


def parse_ree_public_included_series(payload: dict, value_field: str = "value") -> pd.DataFrame:
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


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_ree_demand_evolution_public(
    start_day: date,
    end_day: date,
    time_trunc: str = "hour",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": time_trunc,
        **REE_PENINSULAR_PARAMS,
    }
    url = f"{REE_API_BASE}/demanda/evolucion"
    try:
        resp = requests.get(url, params=params, timeout=60)
    except Exception as exc:
        return pd.DataFrame(), {"http": "ERROR", "url": url, "rows": 0, "error": str(exc)[:500]}

    payload = safe_json_response(resp)
    if not resp.ok or not isinstance(payload, dict):
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}

    df = parse_ree_public_included_series(payload, value_field="value")
    if df.empty:
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}

    if df["title"].nunique() > 1:
        demand_like = df[df["title"].astype(str).str.contains("demanda", case=False, na=False)].copy()
        if not demand_like.empty:
            df = demand_like

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()

    if time_trunc == "hour":
        df["hourly_avg_mw"] = df["value"]
        df["hourly_avg_gw"] = df["hourly_avg_mw"] / 1000.0
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df["weekday"] = df["datetime"].dt.day_name()
        df["is_weekend"] = df["datetime"].dt.weekday >= 5
    elif time_trunc == "month":
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["demand_mwh"] = df["value"]
        df["demand_gwh"] = df["demand_mwh"] / 1000.0
        df["avg_demand_gw"] = df["demand_gwh"] / (df["month"].dt.days_in_month * 24)

    info = {
        "http": resp.status_code,
        "url": resp.url,
        "rows": int(len(df)),
        "title_values": ", ".join(sorted(df["title"].dropna().astype(str).unique().tolist())[:5]),
        "payload_preview": None,
    }
    return df.sort_values("datetime").reset_index(drop=True), info


def public_month_bounds(d: date) -> tuple[date, date]:
    start = date(d.year, d.month, 1)
    end = date(d.year, 12, 31) if d.month == 12 else date(d.year, d.month + 1, 1) - timedelta(days=1)
    return start, end


def public_previous_month_bounds(d: date) -> tuple[date, date]:
    first = date(d.year, d.month, 1)
    return public_month_bounds(first - timedelta(days=1))


def build_demand_monthly_summary(hourly: pd.DataFrame) -> dict[str, Any]:
    summary = {
        "demand_gwh": None,
        "avg_demand_gw": None,
        "max_hourly_gw": None,
        "min_hourly_gw": None,
        "peak_hour": None,
        "days": None,
        "load_factor": None,
    }
    if hourly is not None and not hourly.empty:
        summary["days"] = hourly["date"].nunique()
        summary["avg_demand_gw"] = hourly["hourly_avg_gw"].mean()
        summary["demand_gwh"] = hourly["hourly_avg_gw"].sum()
        summary["max_hourly_gw"] = hourly["hourly_avg_gw"].max()
        summary["min_hourly_gw"] = hourly["hourly_avg_gw"].min()
        idx = hourly["hourly_avg_gw"].idxmax()
        summary["peak_hour"] = hourly.loc[idx, "datetime"]
        if summary["max_hourly_gw"] not in [None, 0]:
            summary["load_factor"] = summary["avg_demand_gw"] / summary["max_hourly_gw"]
    return summary


def build_demand_hourly_profile(hourly: pd.DataFrame, label: str) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "avg_gw", "min_gw", "max_gw", "obs", "label"])
    out = hourly.groupby("hour", as_index=False).agg(
        avg_gw=("hourly_avg_gw", "mean"),
        min_gw=("hourly_avg_gw", "min"),
        max_gw=("hourly_avg_gw", "max"),
        obs=("hourly_avg_gw", "count"),
    )
    out["label"] = label
    return out


def build_demand_daily_avg_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "avg_gw", "max_gw", "min_gw"])
    return hourly.groupby("date", as_index=False).agg(
        avg_gw=("hourly_avg_gw", "mean"),
        max_gw=("hourly_avg_gw", "max"),
        min_gw=("hourly_avg_gw", "min"),
    )


def build_demand_weekday_hourly_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "day_type", "avg_gw", "obs"])
    tmp = hourly.copy()
    tmp["day_type"] = tmp["is_weekend"].map({True: "Weekend", False: "Weekday"})
    return tmp.groupby(["day_type", "hour"], as_index=False).agg(
        avg_gw=("hourly_avg_gw", "mean"),
        obs=("hourly_avg_gw", "count"),
    )


def pct_delta(cur: Any, prev: Any) -> float | None:
    if cur is None or prev in [None, 0] or pd.isna(cur) or pd.isna(prev):
        return None
    return float(cur) / float(prev) - 1


def delta_text(value: float | None, suffix: str = "", decimals: int = 1, good_when_up: bool = True) -> str:
    if value is None or pd.isna(value):
        return "→ n/a"
    positive = value >= 0
    arrow = "↑" if positive else "↓"
    return f"{arrow} {value:+,.{decimals}f}{suffix}"


def padded_zero_domain(values: pd.Series, pad: float = 0.08) -> list[float]:
    """Return a y-domain that includes zero and adds light padding."""
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return [-1.0, 1.0]
    vmin = min(float(clean.min()), 0.0)
    vmax = max(float(clean.max()), 0.0)
    if abs(vmax - vmin) < 1e-9:
        span = max(abs(vmax), 1.0)
        return [-span, span]
    span = vmax - vmin
    return [vmin - span * pad, vmax + span * pad]


def align_second_axis_zero_domain(primary_domain: list[float], secondary_values: pd.Series, pad: float = 0.08) -> list[float]:
    """
    Build a secondary-axis domain whose zero is at the same relative height as
    zero in primary_domain. This keeps both Y axes visually zero-aligned.
    """
    clean = pd.to_numeric(secondary_values, errors="coerce").dropna()
    if clean.empty:
        return [-1.0, 1.0]

    left_min, left_max = float(primary_domain[0]), float(primary_domain[1])
    if abs(left_max - left_min) < 1e-9 or not (left_min < 0 < left_max):
        return padded_zero_domain(clean, pad=pad)

    zero_frac = (0.0 - left_min) / (left_max - left_min)
    if not (0.0 < zero_frac < 1.0):
        return padded_zero_domain(clean, pad=pad)

    pmin = min(float(clean.min()), 0.0)
    pmax = max(float(clean.max()), 0.0)
    pspan = max(pmax - pmin, 1.0)
    pmin -= pspan * pad
    pmax += pspan * pad

    required_range = max(
        pmax / (1.0 - zero_frac) if pmax > 0 else 0.0,
        (-pmin) / zero_frac if pmin < 0 else 0.0,
        1.0,
    )
    return [-zero_frac * required_range, (1.0 - zero_frac) * required_range]



# =========================================================
# Readability helpers
# =========================================================
def _fmt_num(value: Any, decimals: int = 0, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{decimals}f}{suffix}"


def _thermal_gap_formula_html(
    non_thermal_techs: list[str],
    *,
    subtract_conventional_bilaterals: bool = True,
    include_interconnections: bool = False,
) -> str:
    hydro_included = any(t in non_thermal_techs for t in ["Hydro UGH", "Hydro non-UGH"])
    hydro_piece = " − Net hydro PBF" if hydro_included else ""
    conventional_piece = " − Conventional bilateral PBF" if subtract_conventional_bilaterals else ""
    interconnection_piece = " + Net export balance PBF" if include_interconnections else ""
    hydro_note = (
        "Hydro dispatchable is currently deducted from demand, so hydro lowers the thermal gap."
        if hydro_included
        else "Hydro dispatchable is currently not deducted, so hydro remains inside the residual thermal gap."
    )
    interconnection_note = (
        " Net exports are added and net imports reduce the gap."
        if include_interconnections
        else " Interconnections are not included in this run."
    )
    return (
        '<div class="formula-card">'
        '<div class="formula-main">'
        '<b>Thermal gap formula used in this run</b><br>'
        f'Thermal gap = PBF demand{conventional_piece}{interconnection_piece} − Net nuclear PBF − Net wind PBF − Net solar PBF − Net other renewables PBF{hydro_piece}'
        '</div>'
        '<div class="formula-note">'
        '<b>Net PBF</b> means gross PBF minus bilateral PBF for each technology deducted in the formula. '
        '<b>Conventional bilateral PBF</b> is deducted from demand because those conventional volumes are already contracted outside the pool. '
        f'{hydro_note}{interconnection_note}<br>'
        'If you remove Hydro UGH / Hydro non-UGH from the selector, the formula removes the hydro term and hydro appears inside the residual technology stack.'
        '</div></div>'
    )

def _thermal_gap_formula_table(df: pd.DataFrame, non_thermal_techs: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rows = []
    demand_avg = pd.to_numeric(df.get("Total scheduled demand PBF"), errors="coerce").mean()
    conv_bilat_avg = pd.to_numeric(df.get("conventional_bilateral_pbf_mwh", pd.Series(0.0, index=df.index)), errors="coerce").mean()
    net_exports_avg = pd.to_numeric(df.get("net_exports_pbf_mwh", pd.Series(0.0, index=df.index)), errors="coerce").mean()
    price_setting_avg = pd.to_numeric(df.get("price_setting_demand_pbf_mwh", df.get("Total scheduled demand PBF")), errors="coerce").mean()
    nonthermal_avg = pd.to_numeric(df.get("non_thermal_net_pbf_mwh"), errors="coerce").mean()
    gap_avg = pd.to_numeric(df.get("raw_thermal_gap_mwh"), errors="coerce").mean()
    rows.append({"Formula block": "PBF demand", "Included": "Always", "Average MWh/h": demand_avg})
    rows.append({"Formula block": "Conventional bilateral PBF", "Included": "Deducted if enabled", "Average MWh/h": -conv_bilat_avg})
    rows.append({"Formula block": "Net export balance PBF", "Included": "Added if enabled", "Average MWh/h": net_exports_avg})
    rows.append({"Formula block": "Price-setting demand basis", "Included": "Intermediate", "Average MWh/h": price_setting_avg})
    for tech in non_thermal_techs:
        col = f"{tech} net PBF"
        if col in df.columns:
            rows.append({"Formula block": f"Net {tech} PBF", "Included": "Deducted", "Average MWh/h": -pd.to_numeric(df[col], errors="coerce").mean()})
    rows.append({"Formula block": "Total net non-thermal PBF", "Included": "Deducted subtotal", "Average MWh/h": -nonthermal_avg})
    rows.append({"Formula block": "Thermal gap result", "Included": "Result", "Average MWh/h": gap_avg})
    out = pd.DataFrame(rows)
    if not out.empty:
        out["Average MWh/h"] = pd.to_numeric(out["Average MWh/h"], errors="coerce").round(0)
    return out



def _balancing_downward_focus_table(energy: pd.DataFrame) -> pd.DataFrame:
    if energy is None or energy.empty:
        return pd.DataFrame()
    out = energy[[c for c in ["category", "downward_gwh", "downward_avg_price_eur_mwh", "downward_cost_meur", "downward_price_ids"] if c in energy.columns]].copy()
    out = out.rename(columns={
        "category": "Product",
        "downward_gwh": "Downward volume (GWh)",
        "downward_avg_price_eur_mwh": "Downward avg price (€/MWh)",
        "downward_cost_meur": "Downward cost estimate (M€)",
        "downward_price_ids": "Price IDs",
    })
    for col in ["Downward volume (GWh)", "Downward avg price (€/MWh)", "Downward cost estimate (M€)"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2 if "price" in col.lower() else 1)
    return out



# =========================================================
# THREE-STEP DAY-AHEAD FORECAST
# 1) demand  2) non-thermal generation / thermal gap  3) DA price
# =========================================================
DA_OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DA_OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
DA_REDATA_DEMAND_URL = f"{REE_API_BASE}/demanda/evolucion"

DA_FORECAST_GREEN = "#16A34A"
DA_FORECAST_ORANGE = "#F97316"
DA_FORECAST_BLUE = "#2563EB"
DA_FORECAST_GREY = "#64748B"
DA_FORECAST_RED = "#DC2626"

DA_DEFAULT_NON_THERMAL_TECHS = [
    "Nuclear",
    "Wind onshore",
    "Wind offshore",
    "Solar PV",
    "Solar thermal",
    "Hydro non-UGH",
    "Other renewables",
]

DA_GENERATION_COLORS = {
    "Nuclear": "#A855F7",
    "Wind onshore": "#2563EB",
    "Wind offshore": "#60A5FA",
    "Solar PV": "#FACC15",
    "Solar thermal": "#FB923C",
    "Hydro non-UGH": "#06B6D4",
    "Other renewables": "#10B981",
    "Hydro UGH": "#0EA5E9",
}

# Demand / weather proxy points. Weights are normalized hourly.
DA_SPAIN_WEATHER_POINTS = [
    {"city": "Madrid", "latitude": 40.4168, "longitude": -3.7038, "weight": 0.220},
    {"city": "Barcelona", "latitude": 41.3874, "longitude": 2.1686, "weight": 0.150},
    {"city": "Valencia", "latitude": 39.4699, "longitude": -0.3763, "weight": 0.085},
    {"city": "Sevilla", "latitude": 37.3891, "longitude": -5.9845, "weight": 0.070},
    {"city": "Málaga", "latitude": 36.7213, "longitude": -4.4214, "weight": 0.050},
    {"city": "Zaragoza", "latitude": 41.6488, "longitude": -0.8891, "weight": 0.045},
    {"city": "Murcia", "latitude": 37.9922, "longitude": -1.1307, "weight": 0.040},
    {"city": "Bilbao", "latitude": 43.2630, "longitude": -2.9350, "weight": 0.040},
    {"city": "Alicante", "latitude": 38.3452, "longitude": -0.4810, "weight": 0.035},
    {"city": "Valladolid", "latitude": 41.6523, "longitude": -4.7245, "weight": 0.030},
    {"city": "A Coruña", "latitude": 43.3623, "longitude": -8.4115, "weight": 0.028},
    {"city": "Vigo", "latitude": 42.2406, "longitude": -8.7207, "weight": 0.028},
    {"city": "Córdoba", "latitude": 37.8882, "longitude": -4.7794, "weight": 0.025},
    {"city": "Granada", "latitude": 37.1773, "longitude": -3.5986, "weight": 0.025},
    {"city": "Oviedo", "latitude": 43.3619, "longitude": -5.8494, "weight": 0.025},
    {"city": "Pamplona", "latitude": 42.8125, "longitude": -1.6458, "weight": 0.018},
    {"city": "Badajoz", "latitude": 38.8794, "longitude": -6.9707, "weight": 0.017},
    {"city": "Santander", "latitude": 43.4623, "longitude": -3.8099, "weight": 0.015},
    {"city": "Logroño", "latitude": 42.4627, "longitude": -2.4449, "weight": 0.009},
]

DA_WEATHER_VARIABLES = [
    "temperature_2m",
    "shortwave_radiation",
    "cloud_cover",
    "wind_speed_100m",
    "precipitation",
]


def da_section_header(step: str, title: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
        <div style="
            border-left:6px solid #0F766E;
            background:linear-gradient(90deg,#ECFDF5 0%,#F8FAFC 100%);
            border-radius:10px;
            padding:11px 15px;
            margin:14px 0 10px 0;
        ">
          <div style="font-weight:850;color:#0F172A;font-size:1.06rem;">
            {step} · {title}
          </div>
          <div style="color:#64748B;font-size:0.88rem;margin-top:2px;">
            {subtitle}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def da_configure_chart(chart, height: int = 390):
    return (
        chart.properties(height=height)
        .configure_view(stroke="#E2E8F0", fill="white")
        .configure_axis(
            grid=True,
            gridColor="#E2E8F0",
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor="#0F172A",
            titleColor="#0F172A",
            labelFontSize=11,
            titleFontSize=12,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=11,
            titleFontSize=11,
            symbolStrokeWidth=3,
        )
    )


def da_national_holidays(years: list[int]) -> set[date]:
    result: set[date] = set()
    for year in years:
        for month, day in [
            (1, 1), (1, 6), (5, 1), (8, 15),
            (10, 12), (11, 1), (12, 6), (12, 8), (12, 25),
        ]:
            result.add(date(year, month, day))
        result.add(easter(year) - timedelta(days=2))
    return result


# ---------------------------------------------------------
# Weather
# ---------------------------------------------------------
def da_weather_points(mode: str) -> list[dict]:
    if mode == "Madrid":
        return [
            {
                "city": "Madrid",
                "latitude": 40.4168,
                "longitude": -3.7038,
                "weight": 1.0,
            }
        ]
    return DA_SPAIN_WEATHER_POINTS


def da_parse_weighted_weather(payload, points: list[dict]) -> pd.DataFrame:
    if isinstance(payload, dict):
        payload = [payload]

    frames = []
    for idx, item in enumerate(payload):
        if idx >= len(points):
            continue
        hourly = item.get("hourly", {}) or {}
        times = hourly.get("time", []) or []
        if not times:
            continue

        n = len(times)
        frame = pd.DataFrame({"datetime": pd.to_datetime(times, errors="coerce")})
        for variable in DA_WEATHER_VARIABLES:
            vals = hourly.get(variable, []) or []
            if len(vals) == n:
                frame[variable] = pd.to_numeric(pd.Series(vals), errors="coerce")
            else:
                frame[variable] = np.nan
        frame["weight"] = float(points[idx]["weight"])
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["datetime"] + DA_WEATHER_VARIABLES)

    long = pd.concat(frames, ignore_index=True)
    rows = []
    for variable in DA_WEATHER_VARIABLES:
        temp = long[["datetime", "weight", variable]].dropna(subset=["datetime", variable]).copy()
        if temp.empty:
            continue
        temp["weighted"] = temp[variable] * temp["weight"]
        agg = temp.groupby("datetime", as_index=False).agg(
            weighted=("weighted", "sum"),
            available_weight=("weight", "sum"),
        )
        agg[variable] = agg["weighted"] / agg["available_weight"]
        rows.append(agg[["datetime", variable]])

    if not rows:
        return pd.DataFrame(columns=["datetime"] + DA_WEATHER_VARIABLES)

    out = rows[0]
    for frame in rows[1:]:
        out = out.merge(frame, on="datetime", how="outer")
    return out.sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=86400)
def da_load_weather_history(
    start_day: date,
    end_day: date,
    mode: str,
) -> pd.DataFrame:
    points = da_weather_points(mode)
    safe_end = min(end_day, date.today() - timedelta(days=5))
    if start_day > safe_end:
        return pd.DataFrame(columns=["datetime"] + DA_WEATHER_VARIABLES)

    params = {
        "latitude": ",".join(str(p["latitude"]) for p in points),
        "longitude": ",".join(str(p["longitude"]) for p in points),
        "start_date": start_day.isoformat(),
        "end_date": safe_end.isoformat(),
        "hourly": ",".join(DA_WEATHER_VARIABLES),
        "timezone": "Europe/Madrid",
        "models": "era5_land",
    }
    response = requests.get(
        DA_OPEN_METEO_ARCHIVE_URL,
        params=params,
        timeout=180,
    )
    response.raise_for_status()
    return da_parse_weighted_weather(response.json(), points)


@st.cache_data(show_spinner=False, ttl=1800)
def da_load_weather_forecast(
    target_day: date,
    mode: str,
) -> pd.DataFrame:
    points = da_weather_points(mode)
    params = {
        "latitude": ",".join(str(p["latitude"]) for p in points),
        "longitude": ",".join(str(p["longitude"]) for p in points),
        "hourly": ",".join(DA_WEATHER_VARIABLES),
        "timezone": "Europe/Madrid",
        "forecast_days": min(max((target_day - date.today()).days + 1, 1), 16),
    }
    response = requests.get(
        DA_OPEN_METEO_FORECAST_URL,
        params=params,
        timeout=120,
    )
    response.raise_for_status()
    out = da_parse_weighted_weather(response.json(), points)
    return out[out["datetime"].dt.date == target_day].reset_index(drop=True)


# ---------------------------------------------------------
# Demand forecast
# ---------------------------------------------------------
def da_parse_redata_hourly(payload: dict) -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = str(attrs.get("title") or item.get("id") or "").strip()
        for value in attrs.get("values", []) or []:
            dt = pd.to_datetime(value.get("datetime"), utc=True, errors="coerce")
            val = pd.to_numeric(value.get("value"), errors="coerce")
            if pd.isna(dt) or pd.isna(val):
                continue
            rows.append(
                {
                    "datetime": dt.tz_convert("Europe/Madrid").tz_localize(None),
                    "title": title,
                    "value": float(val),
                }
            )
    return pd.DataFrame(rows)


def da_fetch_demand_chunk(start_day: date, end_day: date) -> pd.DataFrame:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "hour",
        **REE_PENINSULAR_PARAMS,
    }
    response = requests.get(
        DA_REDATA_DEMAND_URL,
        params=params,
        timeout=90,
    )
    response.raise_for_status()

    raw = da_parse_redata_hourly(response.json())
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "demand_mw"])

    demand_like = raw[
        raw["title"].str.contains(
            "demanda|demand",
            case=False,
            regex=True,
            na=False,
        )
    ]
    if not demand_like.empty:
        raw = demand_like

    totals = (
        raw.groupby("title", as_index=False)["value"]
        .sum()
        .sort_values("value", ascending=False)
    )
    if not totals.empty:
        raw = raw[raw["title"] == totals.iloc[0]["title"]]

    return (
        raw.groupby("datetime", as_index=False)["value"]
        .mean()
        .rename(columns={"value": "demand_mw"})
        .sort_values("datetime")
    )


@st.cache_data(show_spinner=False, ttl=3600)
def da_load_hourly_demand(start_day: date, end_day: date) -> pd.DataFrame:
    chunks = []
    current = start_day
    while current <= end_day:
        chunk_end = min(end_day, current + timedelta(days=6))
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    frames = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(da_fetch_demand_chunk, s, e): (s, e)
            for s, e in chunks
        }
        for future in as_completed(futures):
            try:
                frame = future.result()
            except Exception:
                frame = pd.DataFrame()
            if frame is not None and not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["datetime", "demand_mw"])

    out = pd.concat(frames, ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out["demand_mw"] = pd.to_numeric(out["demand_mw"], errors="coerce")
    return (
        out.dropna(subset=["datetime", "demand_mw"])
        .groupby("datetime", as_index=False)["demand_mw"]
        .mean()
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def da_calendar_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out["date"] = out["datetime"].dt.date
    out["hour"] = out["datetime"].dt.hour
    out["date_ts"] = pd.to_datetime(out["date"].astype(str))
    out["dow"] = out["date_ts"].dt.dayofweek
    out["doy"] = out["date_ts"].dt.dayofyear
    out["month"] = out["date_ts"].dt.month
    out["is_weekend"] = (out["dow"] >= 5).astype(int)

    holidays = da_national_holidays(
        sorted(out["date_ts"].dt.year.unique().tolist())
    )
    out["is_holiday"] = out["date"].isin(holidays).astype(int)
    out["is_pre_holiday"] = out["date"].map(
        lambda d: int(d + timedelta(days=1) in holidays)
    )
    out["is_post_holiday"] = out["date"].map(
        lambda d: int(d - timedelta(days=1) in holidays)
    )

    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24.0)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24.0)
    out["dow_sin"] = np.sin(2 * np.pi * out["dow"] / 7.0)
    out["dow_cos"] = np.cos(2 * np.pi * out["dow"] / 7.0)
    out["doy_sin"] = np.sin(2 * np.pi * out["doy"] / 365.25)
    out["doy_cos"] = np.cos(2 * np.pi * out["doy"] / 365.25)
    return out


def da_add_weather_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in DA_WEATHER_VARIABLES:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["heating_degree"] = (16.0 - out["temperature_2m"]).clip(lower=0)
    out["cooling_degree"] = (out["temperature_2m"] - 22.0).clip(lower=0)
    out["heating_degree_sq"] = out["heating_degree"] ** 2
    out["cooling_degree_sq"] = out["cooling_degree"] ** 2

    daily = (
        out.groupby("date", as_index=False)
        .agg(
            daily_temp_mean=("temperature_2m", "mean"),
            daily_temp_min=("temperature_2m", "min"),
            daily_temp_max=("temperature_2m", "max"),
            daily_radiation=("shortwave_radiation", "mean"),
            daily_wind_100m=("wind_speed_100m", "mean"),
            daily_precipitation=("precipitation", "sum"),
        )
    )
    return out.merge(daily, on="date", how="left")


def da_add_demand_lags(
    frame: pd.DataFrame,
    demand_lookup: dict,
    trend_alpha: float,
) -> pd.DataFrame:
    out = frame.copy()
    for lag in [2, 3, 7, 9, 10, 14, 21, 28]:
        out[f"lag_{lag}d"] = [
            demand_lookup.get((d - timedelta(days=lag), int(h)), np.nan)
            for d, h in zip(out["date"], out["hour"])
        ]

    out["weekly_change_d2_vs_d9"] = out["lag_2d"] - out["lag_9d"]
    out["weekly_change_d3_vs_d10"] = out["lag_3d"] - out["lag_10d"]
    out["recent_weekly_trend_mw"] = (
        0.65 * out["weekly_change_d2_vs_d9"]
        + 0.35 * out["weekly_change_d3_vs_d10"]
    ).clip(lower=-3500, upper=3500)
    out["trend_adjusted_lag_7d"] = (
        out["lag_7d"]
        + float(trend_alpha) * out["recent_weekly_trend_mw"]
    )
    out["same_hour_4w_mean"] = out[
        ["lag_7d", "lag_14d", "lag_21d", "lag_28d"]
    ].mean(axis=1)
    out["recent_level_mean"] = out[
        ["lag_2d", "lag_3d", "lag_7d"]
    ].mean(axis=1)
    return out


DA_DEMAND_FEATURES = [
    "hour", "month", "dow", "is_weekend",
    "is_holiday", "is_pre_holiday", "is_post_holiday",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
    "temperature_2m", "daily_temp_mean", "daily_temp_min", "daily_temp_max",
    "heating_degree", "cooling_degree", "heating_degree_sq", "cooling_degree_sq",
    "lag_2d", "lag_3d", "lag_7d", "lag_9d", "lag_10d",
    "lag_14d", "lag_21d", "lag_28d",
    "weekly_change_d2_vs_d9", "weekly_change_d3_vs_d10",
    "recent_weekly_trend_mw", "trend_adjusted_lag_7d",
    "same_hour_4w_mean", "recent_level_mean",
]


def da_metrics(actual, predicted) -> dict[str, float]:
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    valid = np.isfinite(actual) & np.isfinite(predicted)
    actual = actual[valid]
    predicted = predicted[valid]
    if len(actual) == 0:
        return {"mae": np.nan, "rmse": np.nan, "mape": np.nan}
    error = actual - predicted
    nonzero = np.abs(actual) > 1e-9
    return {
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(np.sqrt(np.mean(error ** 2))),
        "mape": (
            float(np.mean(np.abs(error[nonzero] / actual[nonzero])) * 100)
            if nonzero.any()
            else np.nan
        ),
    }


def da_similar_day_prediction(
    history: pd.DataFrame,
    target: pd.DataFrame,
    target_col: str,
) -> np.ndarray:
    predictions = []
    for row in target.itertuples(index=False):
        candidates = history[
            (history["dow"] == row.dow)
            & (history["hour"] == row.hour)
            & (history["date"] < row.date)
        ].copy()
        if candidates.empty:
            fallback = getattr(row, "same_hour_4w_mean", np.nan)
            predictions.append(float(fallback) if pd.notna(fallback) else 0.0)
            continue
        candidates["temp_distance"] = (
            candidates["daily_temp_mean"] - row.daily_temp_mean
        ).abs()
        candidates["days_ago"] = candidates["date"].map(
            lambda d: max((row.date - d).days, 1)
        )
        candidates = candidates.nsmallest(12, ["temp_distance", "days_ago"])
        weights = (
            np.exp(-candidates["temp_distance"] / 3.0)
            * np.exp(-candidates["days_ago"] / 240.0)
        )
        predictions.append(
            float(np.average(candidates[target_col], weights=weights))
        )
    return np.asarray(predictions)


@st.cache_data(show_spinner=False, ttl=1800)
def da_generate_demand_forecast(
    target_day: date,
    lookback_days: int,
    weather_mode: str,
    trend_alpha: float,
) -> dict:
    history_end = target_day - timedelta(days=2)
    history_start = history_end - timedelta(days=int(lookback_days))

    demand = da_load_hourly_demand(history_start, history_end)
    weather = da_load_weather_history(history_start, history_end, weather_mode)
    target_weather = da_load_weather_forecast(target_day, weather_mode)

    if demand.empty or weather.empty or target_weather.empty:
        raise ValueError("Demand or weather history/forecast is unavailable.")

    demand["date"] = demand["datetime"].dt.date
    demand["hour"] = demand["datetime"].dt.hour
    demand_grid = (
        demand.groupby(["date", "hour"], as_index=False)["demand_mw"]
        .mean()
    )
    demand_lookup = {
        (r.date, int(r.hour)): float(r.demand_mw)
        for r in demand_grid.itertuples(index=False)
    }

    weather["date"] = weather["datetime"].dt.date
    weather["hour"] = weather["datetime"].dt.hour
    weather_grid = (
        weather.groupby(["date", "hour"], as_index=False)[DA_WEATHER_VARIABLES]
        .mean()
    )

    history = demand_grid.merge(
        weather_grid,
        on=["date", "hour"],
        how="inner",
    )
    history["datetime"] = (
        pd.to_datetime(history["date"].astype(str))
        + pd.to_timedelta(history["hour"], unit="h")
    )
    history = da_calendar_features(history)
    history = da_add_weather_features(history)
    history = da_add_demand_lags(
        history,
        demand_lookup,
        trend_alpha,
    )

    target = target_weather.copy()
    target["date"] = target["datetime"].dt.date
    target["hour"] = target["datetime"].dt.hour
    target = (
        target.groupby(["date", "hour"], as_index=False)[DA_WEATHER_VARIABLES]
        .mean()
    )
    target["datetime"] = (
        pd.to_datetime(target["date"].astype(str))
        + pd.to_timedelta(target["hour"], unit="h")
    )
    target = da_calendar_features(target)
    target = da_add_weather_features(target)
    target = da_add_demand_lags(
        target,
        demand_lookup,
        trend_alpha,
    )

    model_data = history.dropna(
        subset=["demand_mw"] + DA_DEMAND_FEATURES
    ).copy()
    target_data = target.dropna(subset=DA_DEMAND_FEATURES).copy()

    if len(model_data) < 24 * 160 or len(target_data) < 23:
        raise ValueError("Insufficient complete data after demand lag construction.")

    validation_start = (
        model_data["date_ts"].max() - pd.Timedelta(days=41)
    )
    train = model_data[model_data["date_ts"] < validation_start]
    validation = model_data[model_data["date_ts"] >= validation_start]

    if SKLEARN_AVAILABLE:
        validation_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.055,
            max_iter=300,
            max_leaf_nodes=31,
            min_samples_leaf=30,
            l2_regularization=8.0,
            random_state=42,
        )
        validation_model.fit(
            train[DA_DEMAND_FEATURES],
            train["demand_mw"],
        )
        validation_prediction = validation_model.predict(
            validation[DA_DEMAND_FEATURES]
        )

        final_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.055,
            max_iter=300,
            max_leaf_nodes=31,
            min_samples_leaf=30,
            l2_regularization=8.0,
            random_state=42,
        )
        final_model.fit(
            model_data[DA_DEMAND_FEATURES],
            model_data["demand_mw"],
        )
        target_prediction = final_model.predict(
            target_data[DA_DEMAND_FEATURES]
        )
        model_name = "Histogram gradient boosting"
    else:
        validation_prediction = da_similar_day_prediction(
            train,
            validation,
            "demand_mw",
        )
        target_prediction = da_similar_day_prediction(
            model_data,
            target_data,
            "demand_mw",
        )
        model_name = "Weighted similar-day fallback"

    model_stats = da_metrics(
        validation["demand_mw"],
        validation_prediction,
    )
    trend_stats = da_metrics(
        validation["demand_mw"],
        validation["trend_adjusted_lag_7d"],
    )
    plain_d7_stats = da_metrics(
        validation["demand_mw"],
        validation["lag_7d"],
    )

    residual = (
        validation["demand_mw"].to_numpy()
        - np.asarray(validation_prediction)
    )
    residual_frame = validation[["hour"]].copy()
    residual_frame["residual"] = residual
    quantiles = (
        residual_frame.groupby("hour")["residual"]
        .quantile([0.10, 0.90])
        .unstack()
    )
    quantiles.columns = ["residual_p10", "residual_p90"]

    forecast = target_data[
        [
            "datetime", "date", "hour",
            "temperature_2m", "shortwave_radiation",
            "cloud_cover", "wind_speed_100m", "precipitation",
            "lag_2d", "lag_7d", "lag_9d",
            "recent_weekly_trend_mw",
            "trend_adjusted_lag_7d",
        ]
    ].copy()
    forecast["model_forecast_mw"] = np.maximum(target_prediction, 0)
    forecast = forecast.merge(
        quantiles,
        left_on="hour",
        right_index=True,
        how="left",
    )
    forecast["residual_p10"] = forecast["residual_p10"].fillna(
        np.quantile(residual, 0.10)
    )
    forecast["residual_p90"] = forecast["residual_p90"].fillna(
        np.quantile(residual, 0.90)
    )
    forecast["p10_mw"] = (
        forecast["model_forecast_mw"] + forecast["residual_p10"]
    ).clip(lower=0)
    forecast["p90_mw"] = (
        forecast["model_forecast_mw"] + forecast["residual_p90"]
    ).clip(lower=0)

    return {
        "forecast": forecast.sort_values("datetime").reset_index(drop=True),
        "model_stats": model_stats,
        "trend_stats": trend_stats,
        "plain_d7_stats": plain_d7_stats,
        "model_name": model_name,
        "history_start": history_start,
        "history_end": history_end,
        "training_rows": len(model_data),
        "demand_history": demand[
            ["datetime", "demand_mw"]
        ].sort_values("datetime").reset_index(drop=True),
    }


def da_demand_chart(forecast: pd.DataFrame):
    band = alt.Chart(forecast).mark_area(
        opacity=0.15,
        color=DA_FORECAST_GREEN,
    ).encode(
        x=alt.X(
            "datetime:T",
            title=None,
            axis=alt.Axis(format="%H:%M", labelAngle=0),
        ),
        y=alt.Y(
            "p10_mw:Q",
            title="Demand (MW)",
            scale=alt.Scale(zero=False),
        ),
        y2="p90_mw:Q",
    )

    long = pd.concat(
        [
            forecast[["datetime", "model_forecast_mw"]]
            .rename(columns={"model_forecast_mw": "value"})
            .assign(series="Model forecast"),
            forecast[["datetime", "trend_adjusted_lag_7d"]]
            .rename(columns={"trend_adjusted_lag_7d": "value"})
            .assign(series="Previous week + recent trend"),
            forecast[["datetime", "lag_7d"]]
            .rename(columns={"lag_7d": "value"})
            .assign(series="Same weekday previous week"),
            forecast[["datetime", "lag_2d"]]
            .rename(columns={"lag_2d": "value"})
            .assign(series="D-2 actual reference"),
        ],
        ignore_index=True,
    )

    domain = [
        "Model forecast",
        "Previous week + recent trend",
        "Same weekday previous week",
        "D-2 actual reference",
    ]
    line = alt.Chart(long).mark_line(strokeWidth=3).encode(
        x=alt.X(
            "datetime:T",
            title=None,
            axis=alt.Axis(format="%H:%M", labelAngle=0),
        ),
        y=alt.Y(
            "value:Q",
            title="Demand (MW)",
            scale=alt.Scale(zero=False),
        ),
        color=alt.Color(
            "series:N",
            title="Demand series",
            scale=alt.Scale(
                domain=domain,
                range=[
                    DA_FORECAST_GREEN,
                    DA_FORECAST_ORANGE,
                    DA_FORECAST_GREY,
                    "#93C5FD",
                ],
            ),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                columns=4,
                labelLimit=340,
            ),
        ),
        strokeDash=alt.StrokeDash(
            "series:N",
            legend=None,
            scale=alt.Scale(
                domain=domain,
                range=[[1, 0], [8, 3], [5, 3], [2, 2]],
            ),
        ),
        tooltip=[
            alt.Tooltip(
                "datetime:T",
                title="Hour",
                format="%d-%m-%Y %H:%M",
            ),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="Demand MW", format=",.0f"),
        ],
    )
    return da_configure_chart(alt.layer(band, line), height=410)


# ---------------------------------------------------------
# Historical PBF / non-thermal training frame
# ---------------------------------------------------------
def da_forecast_indicator_map(
    selected_techs: list[str],
    include_interconnections: bool,
) -> dict[str, int]:
    indicator_map = {"Total scheduled demand PBF": DEMAND_PBF_ID}

    required_gross = set(selected_techs) | {
        "Coal sub-bituminous",
        "Coal anthracite",
        "Combined cycle GT",
        "Fuel",
        "Natural gas",
        "Cogeneration",
        "Non-renewable waste",
    }
    for tech in required_gross:
        if tech in PBF_GROSS_COMPONENTS:
            indicator_map[tech] = PBF_GROSS_COMPONENTS[tech]

    for bilateral_name, indicator_id in BILATERAL_FETCH_NAMES.items():
        tech = BILATERAL_TO_GROSS_TECH.get(bilateral_name)
        if tech in required_gross or tech in selected_techs:
            indicator_map[bilateral_name] = indicator_id

    if include_interconnections:
        indicator_map.update(PBF_INTERCONNECTION_COMPONENTS)

    return indicator_map


@st.cache_data(show_spinner=False, ttl=3600)
def da_load_pbf_history(
    start_day: date,
    end_day: date,
    selected_techs_tuple: tuple[str, ...],
    include_interconnections: bool,
    _token: str,
) -> tuple[pd.DataFrame, list[str]]:
    indicator_map = da_forecast_indicator_map(
        list(selected_techs_tuple),
        include_interconnections,
    )

    frames = []
    missing = []

    def _fetch(name: str, indicator_id: int):
        raw = fetch_esios_range(
            indicator_id,
            start_day,
            end_day,
            _token,
            time_trunc="hour",
            time_agg="sum",
        )
        if raw.empty:
            return name, indicator_id, pd.DataFrame()
        temp = raw[["datetime", "value"]].copy()
        temp["datetime"] = pd.to_datetime(
            temp["datetime"],
            errors="coerce",
        ).dt.floor("h")
        temp["value"] = pd.to_numeric(temp["value"], errors="coerce")
        temp = temp.dropna(subset=["datetime", "value"])
        temp = temp.groupby("datetime", as_index=False)["value"].sum()
        temp["series"] = name
        temp["indicator_id"] = indicator_id
        return name, indicator_id, temp

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_fetch, name, indicator_id): (name, indicator_id)
            for name, indicator_id in indicator_map.items()
        }
        for future in as_completed(futures):
            name, indicator_id = futures[future]
            try:
                _, _, frame = future.result()
            except Exception:
                frame = pd.DataFrame()
            if frame.empty:
                missing.append(f"{name} ({indicator_id})")
            else:
                frames.append(frame)

    if not frames:
        return pd.DataFrame(), missing

    raw = pd.concat(frames, ignore_index=True)
    wide = build_wide(raw)
    return wide, missing


def da_series_lookup(frame: pd.DataFrame, value_col: str) -> dict:
    temp = frame[["datetime", value_col]].copy()
    temp["datetime"] = pd.to_datetime(temp["datetime"], errors="coerce")
    temp[value_col] = pd.to_numeric(temp[value_col], errors="coerce")
    temp["date"] = temp["datetime"].dt.date
    temp["hour"] = temp["datetime"].dt.hour

    lookup = {}
    for day_value, hour_value, numeric_value in temp[
        ["date", "hour", value_col]
    ].itertuples(index=False, name=None):
        if pd.notna(numeric_value):
            lookup[(day_value, int(hour_value))] = float(numeric_value)
    return lookup


def da_add_generation_lags(
    frame: pd.DataFrame,
    lookup: dict,
) -> pd.DataFrame:
    out = frame.copy()
    for lag in [2, 3, 7, 9, 10, 14, 21, 28]:
        out[f"gen_lag_{lag}d"] = [
            lookup.get((d - timedelta(days=lag), int(h)), np.nan)
            for d, h in zip(out["date"], out["hour"])
        ]
    out["gen_weekly_change_d2_d9"] = (
        out["gen_lag_2d"] - out["gen_lag_9d"]
    )
    out["gen_weekly_change_d3_d10"] = (
        out["gen_lag_3d"] - out["gen_lag_10d"]
    )
    out["gen_trend_adjusted_d7"] = (
        out["gen_lag_7d"]
        + 0.65 * out["gen_weekly_change_d2_d9"]
        + 0.35 * out["gen_weekly_change_d3_d10"]
    )
    out["gen_same_hour_4w"] = out[
        ["gen_lag_7d", "gen_lag_14d", "gen_lag_21d", "gen_lag_28d"]
    ].mean(axis=1)
    return out


DA_GENERATION_FEATURES = [
    "hour", "month", "dow", "is_weekend",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
    "temperature_2m", "shortwave_radiation", "cloud_cover",
    "wind_speed_100m", "precipitation",
    "daily_temp_mean", "daily_radiation", "daily_wind_100m",
    "daily_precipitation",
    "gen_lag_2d", "gen_lag_3d", "gen_lag_7d",
    "gen_lag_9d", "gen_lag_10d",
    "gen_lag_14d", "gen_lag_21d", "gen_lag_28d",
    "gen_weekly_change_d2_d9", "gen_weekly_change_d3_d10",
    "gen_trend_adjusted_d7", "gen_same_hour_4w",
]


def da_generation_fallback(
    tech: str,
    target: pd.DataFrame,
) -> np.ndarray:
    base = target["gen_trend_adjusted_d7"].copy()
    base = base.fillna(target["gen_same_hour_4w"])
    base = base.fillna(target["gen_lag_2d"]).fillna(0.0)

    if tech in {"Solar PV", "Solar thermal"}:
        radiation = target["shortwave_radiation"].fillna(0)
        base = np.where(radiation <= 2.0, 0.0, base)
    elif tech in {"Wind onshore", "Wind offshore"}:
        wind_factor = (
            target["wind_speed_100m"].fillna(
                target["daily_wind_100m"]
            )
            / target["daily_wind_100m"].replace(0, np.nan)
        ).clip(0.55, 1.45)
        base = base * wind_factor.fillna(1.0)

    return np.maximum(np.asarray(base, dtype=float), 0.0)


def da_forecast_one_generation_tech(
    history_netted: pd.DataFrame,
    weather_history: pd.DataFrame,
    target_weather: pd.DataFrame,
    tech: str,
) -> tuple[pd.DataFrame, dict]:
    target_col = f"{tech} net PBF"
    if target_col not in history_netted.columns:
        return pd.DataFrame(), {
            "technology": tech,
            "mae_mwh": np.nan,
            "mape_pct": np.nan,
            "model": "missing historical target",
        }

    series = history_netted[["datetime", target_col]].copy()
    series[target_col] = pd.to_numeric(series[target_col], errors="coerce")
    lookup = da_series_lookup(series, target_col)

    weather = weather_history.copy()
    weather["datetime"] = pd.to_datetime(weather["datetime"], errors="coerce")
    history = series.merge(weather, on="datetime", how="inner")
    history = da_calendar_features(history)
    history = da_add_weather_features(history)
    history = da_add_generation_lags(history, lookup)

    target = target_weather.copy()
    target = da_calendar_features(target)
    target = da_add_weather_features(target)
    target = da_add_generation_lags(target, lookup)

    model_data = history.dropna(
        subset=[target_col] + DA_GENERATION_FEATURES
    ).copy()
    target_data = target.dropna(subset=DA_GENERATION_FEATURES).copy()

    if len(model_data) < 24 * 120 or len(target_data) < 23:
        prediction = da_generation_fallback(tech, target)
        out = target[
            [
                "datetime", "date", "hour",
                "shortwave_radiation", "wind_speed_100m",
                "gen_lag_2d", "gen_lag_7d",
                "gen_trend_adjusted_d7",
            ]
        ].copy()
        out["forecast_mwh"] = prediction
        out["technology"] = tech
        return out, {
            "technology": tech,
            "mae_mwh": np.nan,
            "mape_pct": np.nan,
            "model": "lag/weather fallback",
        }

    validation_start = model_data["date_ts"].max() - pd.Timedelta(days=27)
    train = model_data[model_data["date_ts"] < validation_start]
    validation = model_data[model_data["date_ts"] >= validation_start]

    if SKLEARN_AVAILABLE and not train.empty and not validation.empty:
        validation_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.055,
            max_iter=260,
            max_leaf_nodes=25,
            min_samples_leaf=24,
            l2_regularization=6.0,
            random_state=42,
        )
        validation_model.fit(
            train[DA_GENERATION_FEATURES],
            train[target_col],
        )
        validation_prediction = validation_model.predict(
            validation[DA_GENERATION_FEATURES]
        )

        final_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.055,
            max_iter=260,
            max_leaf_nodes=25,
            min_samples_leaf=24,
            l2_regularization=6.0,
            random_state=42,
        )
        final_model.fit(
            model_data[DA_GENERATION_FEATURES],
            model_data[target_col],
        )
        prediction = final_model.predict(
            target_data[DA_GENERATION_FEATURES]
        )
        stats = da_metrics(
            validation[target_col],
            validation_prediction,
        )
        model_name = "gradient boosting"
    else:
        prediction = da_generation_fallback(tech, target_data)
        stats = {"mae": np.nan, "mape": np.nan}
        model_name = "lag/weather fallback"

    recent_cap = (
        pd.to_numeric(model_data[target_col], errors="coerce")
        .tail(24 * 90)
        .quantile(0.998)
    )
    if pd.notna(recent_cap) and recent_cap > 0:
        prediction = np.minimum(prediction, recent_cap * 1.12)
    prediction = np.maximum(prediction, 0.0)

    if tech in {"Solar PV", "Solar thermal"}:
        radiation = target_data["shortwave_radiation"].fillna(0).to_numpy()
        prediction = np.where(radiation <= 2.0, 0.0, prediction)

    out = target_data[
        [
            "datetime", "date", "hour",
            "shortwave_radiation", "wind_speed_100m",
            "gen_lag_2d", "gen_lag_7d",
            "gen_trend_adjusted_d7",
        ]
    ].copy()
    out["forecast_mwh"] = prediction
    out["technology"] = tech

    return out, {
        "technology": tech,
        "mae_mwh": stats.get("mae"),
        "mape_pct": stats.get("mape"),
        "model": model_name,
    }


def da_forecast_pattern_series(
    history: pd.DataFrame,
    target_day: date,
    value_col: str,
    allow_negative: bool = False,
) -> pd.Series:
    lookup = da_series_lookup(history[["datetime", value_col]], value_col)
    values = []
    for hour in range(24):
        lag2 = lookup.get((target_day - timedelta(days=2), hour), np.nan)
        lag3 = lookup.get((target_day - timedelta(days=3), hour), np.nan)
        lag7 = lookup.get((target_day - timedelta(days=7), hour), np.nan)
        lag9 = lookup.get((target_day - timedelta(days=9), hour), np.nan)
        lag10 = lookup.get((target_day - timedelta(days=10), hour), np.nan)
        recent = [x for x in [lag2, lag3, lag7] if pd.notna(x)]

        adjusted = np.nan
        if all(pd.notna(x) for x in [lag2, lag3, lag7, lag9, lag10]):
            adjusted = (
                lag7
                + 0.65 * (lag2 - lag9)
                + 0.35 * (lag3 - lag10)
            )

        if pd.notna(adjusted):
            value = adjusted
        elif recent:
            value = float(np.mean(recent))
        else:
            value = 0.0
        values.append(
            float(value)
            if allow_negative
            else max(float(value), 0.0)
        )
    return pd.Series(values, dtype=float)


def da_calibrate_demand_to_pbf(
    selected_demand: pd.DataFrame,
    actual_demand_history: pd.DataFrame,
    pbf_history: pd.DataFrame,
    target_day: date,
) -> pd.DataFrame:
    actual = actual_demand_history.copy()
    actual["datetime"] = pd.to_datetime(actual["datetime"], errors="coerce").dt.floor("h")

    pbf = pbf_history[["datetime", "Total scheduled demand PBF"]].copy()
    pbf["datetime"] = pd.to_datetime(pbf["datetime"], errors="coerce").dt.floor("h")
    pbf["Total scheduled demand PBF"] = pd.to_numeric(
        pbf["Total scheduled demand PBF"],
        errors="coerce",
    )

    merged = actual.merge(pbf, on="datetime", how="inner")
    merged["ratio"] = (
        merged["Total scheduled demand PBF"]
        / merged["demand_mw"].replace(0, np.nan)
    )
    merged["ratio"] = merged["ratio"].clip(0.90, 1.10)
    merged["hour"] = merged["datetime"].dt.hour
    merged["is_weekend"] = (
        merged["datetime"].dt.dayofweek >= 5
    ).astype(int)

    recent_cut = merged["datetime"].max() - pd.Timedelta(days=60)
    recent = merged[merged["datetime"] >= recent_cut].copy()
    target_weekend = int(target_day.weekday() >= 5)

    factor_by_hour = (
        recent[recent["is_weekend"] == target_weekend]
        .groupby("hour")["ratio"]
        .median()
    )
    global_factor = float(recent["ratio"].median()) if not recent.empty else 1.0

    out = selected_demand.copy()
    out["pbf_calibration_factor"] = out["hour"].map(factor_by_hour).fillna(global_factor)
    out["pbf_demand_forecast_mwh"] = (
        out["selected_demand_mw"] * out["pbf_calibration_factor"]
    )
    return out


@st.cache_data(show_spinner=False, ttl=1800)
def da_generate_thermal_gap_forecast(
    target_day: date,
    lookback_days: int,
    weather_mode: str,
    selected_techs_tuple: tuple[str, ...],
    include_interconnections: bool,
    selected_demand_records: tuple,
    actual_demand_records: tuple,
    _token: str,
) -> dict:
    history_end = target_day - timedelta(days=2)
    history_start = history_end - timedelta(days=int(lookback_days))

    selected_techs = list(selected_techs_tuple)

    pbf_wide, missing = da_load_pbf_history(
        history_start,
        history_end,
        selected_techs_tuple,
        include_interconnections,
        _token,
    )
    if pbf_wide.empty:
        raise ValueError("No historical PBF data returned for generation training.")

    historical_thermal, bilateral_diag = calculate_thermal_gap(
        pbf_wide,
        selected_techs,
        subtract_conventional_bilaterals=True,
        include_interconnections=include_interconnections,
    )

    weather_history = da_load_weather_history(
        history_start,
        history_end,
        weather_mode,
    )
    target_weather = da_load_weather_forecast(
        target_day,
        weather_mode,
    )
    if weather_history.empty or target_weather.empty:
        raise ValueError("Weather data unavailable for generation forecast.")

    generation_frames = []
    generation_stats = []
    for tech in selected_techs:
        forecast, stats = da_forecast_one_generation_tech(
            historical_thermal,
            weather_history,
            target_weather,
            tech,
        )
        if not forecast.empty:
            generation_frames.append(forecast)
        generation_stats.append(stats)

    if not generation_frames:
        raise ValueError("No non-thermal generation forecast could be built.")

    generation_long = pd.concat(generation_frames, ignore_index=True)
    generation_long["forecast_mwh"] = pd.to_numeric(
        generation_long["forecast_mwh"],
        errors="coerce",
    ).fillna(0.0).clip(lower=0)

    generation_wide = (
        generation_long.pivot_table(
            index=["datetime", "date", "hour"],
            columns="technology",
            values="forecast_mwh",
            aggfunc="sum",
        )
        .reset_index()
        .sort_values("datetime")
    )
    generation_wide.columns.name = None
    generation_wide["non_thermal_forecast_mwh"] = generation_wide[
        [c for c in selected_techs if c in generation_wide.columns]
    ].sum(axis=1)

    selected_demand = pd.DataFrame(
        list(selected_demand_records),
        columns=["datetime", "hour", "selected_demand_mw"],
    )
    selected_demand["datetime"] = pd.to_datetime(
        selected_demand["datetime"],
        errors="coerce",
    )
    selected_demand["date"] = selected_demand["datetime"].dt.date

    actual_demand = pd.DataFrame(
        list(actual_demand_records),
        columns=["datetime", "demand_mw"],
    )
    actual_demand["datetime"] = pd.to_datetime(
        actual_demand["datetime"],
        errors="coerce",
    )

    calibrated_demand = da_calibrate_demand_to_pbf(
        selected_demand,
        actual_demand,
        historical_thermal,
        target_day,
    )

    target = generation_wide.merge(
        calibrated_demand[
            [
                "datetime",
                "selected_demand_mw",
                "pbf_calibration_factor",
                "pbf_demand_forecast_mwh",
            ]
        ],
        on="datetime",
        how="inner",
    )

    conv_history = historical_thermal[
        ["datetime", "conventional_bilateral_pbf_mwh"]
    ].copy()
    target["conventional_bilateral_forecast_mwh"] = da_forecast_pattern_series(
        conv_history,
        target_day,
        "conventional_bilateral_pbf_mwh",
    ).to_numpy()[: len(target)]

    if include_interconnections:
        inter_history = historical_thermal[
            ["datetime", "net_exports_pbf_mwh"]
        ].copy()
        target["net_exports_forecast_mwh"] = da_forecast_pattern_series(
            inter_history,
            target_day,
            "net_exports_pbf_mwh",
            allow_negative=True,
        ).to_numpy()[: len(target)]
    else:
        target["net_exports_forecast_mwh"] = 0.0

    target["price_setting_demand_forecast_mwh"] = (
        target["pbf_demand_forecast_mwh"]
        - target["conventional_bilateral_forecast_mwh"]
        + target["net_exports_forecast_mwh"]
    )
    target["thermal_gap_forecast_mwh"] = (
        target["price_setting_demand_forecast_mwh"]
        - target["non_thermal_forecast_mwh"]
    )

    return {
        "forecast": target.sort_values("datetime").reset_index(drop=True),
        "generation_long": generation_long.sort_values(
            ["datetime", "technology"]
        ).reset_index(drop=True),
        "generation_stats": pd.DataFrame(generation_stats),
        "historical_thermal": historical_thermal,
        "bilateral_diag": bilateral_diag,
        "missing": missing,
    }


def da_generation_demand_chart(
    generation_long: pd.DataFrame,
    thermal_forecast: pd.DataFrame,
):
    gen = generation_long.copy()
    techs = [
        tech for tech in DA_DEFAULT_NON_THERMAL_TECHS + ["Hydro UGH"]
        if tech in gen["technology"].unique()
    ]
    colors = [
        DA_GENERATION_COLORS.get(tech, "#94A3B8")
        for tech in techs
    ]

    area = alt.Chart(gen).mark_area(opacity=0.88).encode(
        x=alt.X(
            "datetime:T",
            title=None,
            axis=alt.Axis(format="%H:%M", labelAngle=0),
        ),
        y=alt.Y(
            "sum(forecast_mwh):Q",
            title="Forecast non-thermal generation (MWh/h)",
            stack="zero",
        ),
        color=alt.Color(
            "technology:N",
            title="Forecast technology",
            scale=alt.Scale(domain=techs, range=colors),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                columns=4,
                labelLimit=250,
            ),
        ),
        order=alt.Order("technology:N", sort="ascending"),
        tooltip=[
            alt.Tooltip(
                "datetime:T",
                title="Hour",
                format="%d-%m-%Y %H:%M",
            ),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip(
                "forecast_mwh:Q",
                title="Forecast MWh",
                format=",.0f",
            ),
        ],
    )

    demand_line = alt.Chart(thermal_forecast).mark_line(
        color="#111827",
        strokeWidth=3.2,
    ).encode(
        x=alt.X("datetime:T"),
        y=alt.Y(
            "pbf_demand_forecast_mwh:Q",
            title="Demand / generation (MWh/h)",
        ),
        tooltip=[
            alt.Tooltip(
                "datetime:T",
                title="Hour",
                format="%d-%m-%Y %H:%M",
            ),
            alt.Tooltip(
                "pbf_demand_forecast_mwh:Q",
                title="PBF demand forecast",
                format=",.0f",
            ),
            alt.Tooltip(
                "non_thermal_forecast_mwh:Q",
                title="Non-thermal forecast",
                format=",.0f",
            ),
        ],
    )

    return da_configure_chart(
        alt.layer(area, demand_line).resolve_scale(y="shared"),
        height=390,
    )


def da_thermal_gap_chart(thermal_forecast: pd.DataFrame):
    plot = thermal_forecast.copy()
    plot["gap_sign"] = np.where(
        plot["thermal_gap_forecast_mwh"] >= 0,
        "Positive thermal gap",
        "Negative / excess non-thermal",
    )
    bars = alt.Chart(plot).mark_bar(opacity=0.9).encode(
        x=alt.X(
            "datetime:T",
            title=None,
            axis=alt.Axis(format="%H:%M", labelAngle=0),
        ),
        y=alt.Y(
            "thermal_gap_forecast_mwh:Q",
            title="Forecast thermal gap (MWh/h)",
        ),
        color=alt.Color(
            "gap_sign:N",
            title=None,
            scale=alt.Scale(
                domain=[
                    "Positive thermal gap",
                    "Negative / excess non-thermal",
                ],
                range=[DA_FORECAST_BLUE, DA_FORECAST_RED],
            ),
        ),
        tooltip=[
            alt.Tooltip(
                "datetime:T",
                title="Hour",
                format="%d-%m-%Y %H:%M",
            ),
            alt.Tooltip(
                "pbf_demand_forecast_mwh:Q",
                title="PBF demand forecast",
                format=",.0f",
            ),
            alt.Tooltip(
                "non_thermal_forecast_mwh:Q",
                title="Non-thermal forecast",
                format=",.0f",
            ),
            alt.Tooltip(
                "conventional_bilateral_forecast_mwh:Q",
                title="Conventional bilateral estimate",
                format=",.0f",
            ),
            alt.Tooltip(
                "thermal_gap_forecast_mwh:Q",
                title="Thermal gap",
                format=",.0f",
            ),
        ],
    )
    zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(
        color="#0F172A",
        strokeWidth=1.2,
    ).encode(y="y:Q")
    return da_configure_chart(alt.layer(zero, bars), height=320)


# ---------------------------------------------------------
# Price forecast
# ---------------------------------------------------------
def da_add_price_features(
    frame: pd.DataFrame,
    price_lookup: dict,
    gap_lookup: dict,
) -> pd.DataFrame:
    out = da_calendar_features(frame)
    out["thermal_gap_mwh"] = pd.to_numeric(
        out["thermal_gap_mwh"],
        errors="coerce",
    )
    out["positive_gap_mwh"] = out["thermal_gap_mwh"].clip(lower=0)
    out["negative_gap_flag"] = (
        out["thermal_gap_mwh"] <= 0
    ).astype(int)
    out["gap_sq_scaled"] = (
        out["positive_gap_mwh"] / 10_000.0
    ) ** 2

    for lag in [1, 2, 7, 14, 21, 28]:
        out[f"price_lag_{lag}d"] = [
            price_lookup.get((d - timedelta(days=lag), int(h)), np.nan)
            for d, h in zip(out["date"], out["hour"])
        ]
    for lag in [1, 2, 7]:
        out[f"gap_lag_{lag}d"] = [
            gap_lookup.get((d - timedelta(days=lag), int(h)), np.nan)
            for d, h in zip(out["date"], out["hour"])
        ]

    # At a D-1 publication cut-off, the realised/PBF thermal gap for D-1 may
    # not yet be available. Use D-2 as the latest complete fallback.
    out["gap_lag_1d"] = out["gap_lag_1d"].fillna(out["gap_lag_2d"])
    out["price_lag_1d"] = out["price_lag_1d"].fillna(out["price_lag_2d"])

    out["same_hour_price_4w"] = out[
        [
            "price_lag_7d",
            "price_lag_14d",
            "price_lag_21d",
            "price_lag_28d",
        ]
    ].median(axis=1)
    out["price_anchor"] = (
        0.50 * out["price_lag_7d"]
        + 0.30 * out["price_lag_1d"]
        + 0.20 * out["same_hour_price_4w"]
    )
    out["price_anchor"] = out["price_anchor"].fillna(
        0.65 * out["price_lag_7d"]
        + 0.35 * out["same_hour_price_4w"]
    )
    out["gap_change_vs_d7"] = (
        out["thermal_gap_mwh"] - out["gap_lag_7d"]
    )
    out["gap_change_vs_d1"] = (
        out["thermal_gap_mwh"] - out["gap_lag_1d"]
    )
    return out


DA_PRICE_FEATURES = [
    "hour", "month", "dow", "is_weekend",
    "is_holiday", "is_pre_holiday", "is_post_holiday",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
    "thermal_gap_mwh", "positive_gap_mwh", "negative_gap_flag",
    "gap_sq_scaled", "gap_change_vs_d7", "gap_change_vs_d1",
    "price_lag_1d", "price_lag_2d", "price_lag_7d",
    "price_lag_14d", "price_lag_21d", "price_lag_28d",
    "same_hour_price_4w", "price_anchor",
    "gap_lag_1d", "gap_lag_2d", "gap_lag_7d",
]


def da_price_fallback(
    model_data: pd.DataFrame,
    target_data: pd.DataFrame,
) -> np.ndarray:
    positive = model_data[
        (model_data["thermal_gap_mwh"] > 0)
        & model_data["price"].notna()
    ].copy()
    if len(positive) >= 50:
        x = positive["thermal_gap_mwh"].to_numpy(dtype=float)
        y = (
            positive["price"] - positive["price_anchor"]
        ).to_numpy(dtype=float)
        valid = np.isfinite(x) & np.isfinite(y)
        if valid.sum() >= 20:
            slope, intercept = np.polyfit(x[valid], y[valid], 1)
        else:
            slope, intercept = 0.0, 0.0
    else:
        slope, intercept = 0.0, 0.0

    return (
        target_data["price_anchor"].fillna(0).to_numpy(dtype=float)
        + intercept
        + slope * target_data["thermal_gap_mwh"].to_numpy(dtype=float)
    )


@st.cache_data(show_spinner=False, ttl=1800)
def da_generate_price_forecast(
    target_day: date,
    historical_records: tuple,
    target_records: tuple,
    price_source_mode: str,
    auto_fix_scale: bool,
    _token: str,
) -> dict:
    historical = pd.DataFrame(
        list(historical_records),
        columns=[
            "datetime",
            "thermal_gap_mwh",
            "pbf_demand_mwh",
            "non_thermal_mwh",
        ],
    )
    historical["datetime"] = pd.to_datetime(
        historical["datetime"],
        errors="coerce",
    )

    price_start = historical["datetime"].min().date()
    price_end = target_day - timedelta(days=1)
    prices = load_prices_like_day_ahead(
        _token,
        price_start,
        price_end,
        price_source_mode,
        auto_fix_scale,
    )
    if prices.empty:
        raise ValueError("No historical spot-price data available.")

    hist = historical.merge(
        prices[["datetime", "price"]],
        on="datetime",
        how="inner",
    )
    hist["date"] = hist["datetime"].dt.date
    hist["hour"] = hist["datetime"].dt.hour

    target = pd.DataFrame(
        list(target_records),
        columns=[
            "datetime",
            "thermal_gap_mwh",
            "pbf_demand_mwh",
            "non_thermal_mwh",
        ],
    )
    target["datetime"] = pd.to_datetime(target["datetime"], errors="coerce")
    target["date"] = target["datetime"].dt.date
    target["hour"] = target["datetime"].dt.hour

    price_lookup_frame = prices[["datetime", "price"]].copy()
    price_lookup_frame["date"] = price_lookup_frame["datetime"].dt.date
    price_lookup_frame["hour"] = price_lookup_frame["datetime"].dt.hour
    price_lookup = {
        (r.date, int(r.hour)): float(r.price)
        for r in price_lookup_frame.itertuples(index=False)
        if pd.notna(r.price)
    }

    gap_lookup = {
        (r.date, int(r.hour)): float(r.thermal_gap_mwh)
        for r in historical[
            ["datetime", "thermal_gap_mwh"]
        ].assign(
            date=lambda x: x["datetime"].dt.date,
            hour=lambda x: x["datetime"].dt.hour,
        ).itertuples(index=False)
        if pd.notna(r.thermal_gap_mwh)
    }

    hist = da_add_price_features(hist, price_lookup, gap_lookup)
    target = da_add_price_features(target, price_lookup, gap_lookup)

    hist["price_residual"] = hist["price"] - hist["price_anchor"]
    model_data = hist.dropna(
        subset=["price", "price_residual"] + DA_PRICE_FEATURES
    ).copy()
    target_data = target.dropna(subset=DA_PRICE_FEATURES).copy()

    if len(model_data) < 24 * 120 or len(target_data) < 23:
        raise ValueError("Insufficient complete price / thermal-gap training rows.")

    validation_start = model_data["date_ts"].max() - pd.Timedelta(days=41)
    train = model_data[model_data["date_ts"] < validation_start]
    validation = model_data[model_data["date_ts"] >= validation_start]

    if SKLEARN_AVAILABLE and not train.empty and not validation.empty:
        validation_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.05,
            max_iter=320,
            max_leaf_nodes=27,
            min_samples_leaf=28,
            l2_regularization=10.0,
            random_state=42,
        )
        validation_model.fit(
            train[DA_PRICE_FEATURES],
            train["price_residual"],
        )
        validation_residual = validation_model.predict(
            validation[DA_PRICE_FEATURES]
        )
        validation_price = (
            validation["price_anchor"].to_numpy()
            + validation_residual
        )

        final_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.05,
            max_iter=320,
            max_leaf_nodes=27,
            min_samples_leaf=28,
            l2_regularization=10.0,
            random_state=42,
        )
        final_model.fit(
            model_data[DA_PRICE_FEATURES],
            model_data["price_residual"],
        )
        target_residual = final_model.predict(
            target_data[DA_PRICE_FEATURES]
        )
        raw_prediction = (
            target_data["price_anchor"].to_numpy()
            + target_residual
        )
        model_name = "Residual gradient boosting around D-1 / D-7 anchor"
    else:
        validation_price = da_price_fallback(train, validation)
        raw_prediction = da_price_fallback(model_data, target_data)
        model_name = "Anchored linear thermal-gap fallback"

    # User-requested structural rule.
    final_prediction = np.where(
        target_data["thermal_gap_mwh"].to_numpy() <= 0,
        0.0,
        np.maximum(raw_prediction, 0.0),
    )

    historical_cap = model_data["price"].quantile(0.997)
    if pd.notna(historical_cap):
        final_prediction = np.minimum(
            final_prediction,
            max(float(historical_cap) * 1.20, 250.0),
        )

    validation_final = np.where(
        validation["thermal_gap_mwh"].to_numpy() <= 0,
        0.0,
        np.maximum(validation_price, 0.0),
    )
    model_stats = da_metrics(
        validation["price"],
        validation_final,
    )
    anchor_stats = da_metrics(
        validation["price"],
        validation["price_anchor"],
    )

    output = target_data[
        [
            "datetime", "date", "hour",
            "thermal_gap_mwh",
            "price_anchor",
            "price_lag_1d",
            "price_lag_7d",
            "same_hour_price_4w",
            "gap_change_vs_d1",
            "gap_change_vs_d7",
        ]
    ].copy()
    output["forecast_price_eur_mwh"] = final_prediction
    output["raw_model_price_eur_mwh"] = raw_prediction
    output["zero_price_rule"] = output["thermal_gap_mwh"] <= 0

    return {
        "forecast": output.sort_values("datetime").reset_index(drop=True),
        "model_stats": model_stats,
        "anchor_stats": anchor_stats,
        "model_name": model_name,
        "training_rows": len(model_data),
    }


def da_price_chart(price_forecast: pd.DataFrame):
    long = pd.concat(
        [
            price_forecast[
                ["datetime", "forecast_price_eur_mwh"]
            ]
            .rename(columns={"forecast_price_eur_mwh": "price"})
            .assign(series="Forecast DA price"),
            price_forecast[["datetime", "price_lag_1d"]]
            .rename(columns={"price_lag_1d": "price"})
            .assign(series="D-1 price"),
            price_forecast[["datetime", "price_lag_7d"]]
            .rename(columns={"price_lag_7d": "price"})
            .assign(series="Same weekday previous week"),
            price_forecast[["datetime", "price_anchor"]]
            .rename(columns={"price_anchor": "price"})
            .assign(series="D-1 / D-7 anchor"),
        ],
        ignore_index=True,
    )

    domain = [
        "Forecast DA price",
        "D-1 price",
        "Same weekday previous week",
        "D-1 / D-7 anchor",
    ]
    chart = alt.Chart(long).mark_line(
        point=True,
        strokeWidth=3,
    ).encode(
        x=alt.X(
            "datetime:T",
            title=None,
            axis=alt.Axis(format="%H:%M", labelAngle=0),
        ),
        y=alt.Y(
            "price:Q",
            title="Price (€/MWh)",
            scale=alt.Scale(zero=True),
        ),
        color=alt.Color(
            "series:N",
            title="Price series",
            scale=alt.Scale(
                domain=domain,
                range=[
                    "#111827",
                    DA_FORECAST_ORANGE,
                    "#60A5FA",
                    DA_FORECAST_GREY,
                ],
            ),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                columns=4,
                labelLimit=300,
            ),
        ),
        strokeDash=alt.StrokeDash(
            "series:N",
            legend=None,
            scale=alt.Scale(
                domain=domain,
                range=[[1, 0], [5, 3], [2, 2], [8, 3]],
            ),
        ),
        tooltip=[
            alt.Tooltip(
                "datetime:T",
                title="Hour",
                format="%d-%m-%Y %H:%M",
            ),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("price:Q", title="€/MWh", format=",.2f"),
        ],
    )

    zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(
        color="#DC2626",
        strokeWidth=1,
    ).encode(y="y:Q")

    return da_configure_chart(alt.layer(zero, chart), height=370)


def da_tb4_unconstrained(prices: pd.Series) -> float:
    clean = pd.to_numeric(prices, errors="coerce").dropna().sort_values()
    if len(clean) < 8:
        return np.nan
    return float(clean.tail(4).mean() - clean.head(4).mean())


# =========================================================
# THREE-STEP FORECAST UI
# =========================================================
def render_three_step_day_ahead_forecast() -> None:
    st.title("Day-ahead demand → thermal gap → spot-price forecast")
    st.caption(
        "Operational prototype for tomorrow's peninsular hourly curve. "
        "The historical thermal-gap analysis remains available below."
    )

    target_day = date.today() + timedelta(days=1)

    c1, c2, c3, c4 = st.columns([1.0, 1.0, 1.15, 1.35])
    with c1:
        st.metric("Forecast delivery day", f"{target_day:%d/%m/%Y}")
    with c2:
        lookback_days = st.select_slider(
            "Training history",
            options=[270, 365, 450, 550],
            value=365,
            format_func=lambda x: f"{x} days",
            key="da3_training_history",
        )
    with c3:
        weather_mode = st.selectbox(
            "Weather input",
            ["Spain weighted proxy", "Madrid"],
            index=0,
            key="da3_weather_mode",
        )
    with c4:
        demand_source = st.selectbox(
            "Demand curve used downstream",
            [
                "Model forecast (green)",
                "Previous week + recent trend (orange)",
            ],
            index=0,
            key="da3_demand_source",
        )

    c5, c6, c7 = st.columns([1.0, 1.7, 1.1])
    with c5:
        trend_alpha = st.slider(
            "Weekly-trend correction α",
            min_value=0.0,
            max_value=1.5,
            value=1.0,
            step=0.1,
            key="da3_trend_alpha",
        )
    with c6:
        selected_techs = st.multiselect(
            "Non-thermal generation deducted from demand",
            options=[
                "Nuclear",
                "Wind onshore",
                "Wind offshore",
                "Solar PV",
                "Solar thermal",
                "Hydro non-UGH",
                "Other renewables",
                "Hydro UGH",
            ],
            default=DA_DEFAULT_NON_THERMAL_TECHS,
            key="da3_nonthermal_techs",
        )
    with c7:
        include_interconnections_forecast = st.checkbox(
            "Estimate interconnections",
            value=False,
            key="da3_interconnections",
        )

    st.markdown(
        """
        <div class="formula-card">
          <b>Three-step logic</b><br>
          1. Forecast demand for tomorrow.<br>
          2. Forecast non-thermal PBF generation and calculate the thermal gap.<br>
          3. Forecast DA price from the thermal gap and recent price anchors.
          A negative thermal gap is forced to <b>0 €/MWh</b>, as requested.
        </div>
        """,
        unsafe_allow_html=True,
    )

    run_forecast = st.button(
        "Run three-step day-ahead forecast",
        type="primary",
        use_container_width=True,
        key="da3_run_forecast",
    )

    if run_forecast:
        if not selected_techs:
            st.error("Select at least one non-thermal technology.")
            return

        try:
            token = require_esios_token()
        except Exception as exc:
            st.error(str(exc))
            return

        try:
            with st.spinner("Step 1/3 — forecasting tomorrow's hourly demand..."):
                demand_result = da_generate_demand_forecast(
                    target_day,
                    int(lookback_days),
                    weather_mode,
                    float(trend_alpha),
                )

            demand_forecast = demand_result["forecast"].copy()
            if demand_source.startswith("Model"):
                demand_forecast["selected_demand_mw"] = (
                    demand_forecast["model_forecast_mw"]
                )
                selected_demand_label = "Model forecast"
            else:
                demand_forecast["selected_demand_mw"] = (
                    demand_forecast["trend_adjusted_lag_7d"]
                )
                selected_demand_label = "Previous week + recent trend"

            selected_records = tuple(
                demand_forecast[
                    ["datetime", "hour", "selected_demand_mw"]
                ].itertuples(index=False, name=None)
            )
            demand_history_records = tuple(
                demand_result["demand_history"][
                    ["datetime", "demand_mw"]
                ].itertuples(index=False, name=None)
            )

            with st.spinner(
                "Step 2/3 — forecasting renewable / nuclear / run-of-river generation and thermal gap..."
            ):
                thermal_result = da_generate_thermal_gap_forecast(
                    target_day,
                    int(lookback_days),
                    weather_mode,
                    tuple(selected_techs),
                    bool(include_interconnections_forecast),
                    selected_records,
                    demand_history_records,
                    token,
                )

            thermal_forecast = thermal_result["forecast"].copy()
            historical_thermal = thermal_result["historical_thermal"].copy()

            historical_records = tuple(
                historical_thermal[
                    [
                        "datetime",
                        "raw_thermal_gap_mwh",
                        "Total scheduled demand PBF",
                        "non_thermal_net_pbf_mwh",
                    ]
                ]
                .rename(
                    columns={
                        "raw_thermal_gap_mwh": "thermal_gap_mwh",
                        "Total scheduled demand PBF": "pbf_demand_mwh",
                        "non_thermal_net_pbf_mwh": "non_thermal_mwh",
                    }
                )
                .itertuples(index=False, name=None)
            )

            target_records = tuple(
                thermal_forecast[
                    [
                        "datetime",
                        "thermal_gap_forecast_mwh",
                        "pbf_demand_forecast_mwh",
                        "non_thermal_forecast_mwh",
                    ]
                ]
                .rename(
                    columns={
                        "thermal_gap_forecast_mwh": "thermal_gap_mwh",
                        "pbf_demand_forecast_mwh": "pbf_demand_mwh",
                        "non_thermal_forecast_mwh": "non_thermal_mwh",
                    }
                )
                .itertuples(index=False, name=None)
            )

            with st.spinner("Step 3/3 — forecasting tomorrow's DA spot-price curve..."):
                price_result = da_generate_price_forecast(
                    target_day,
                    historical_records,
                    target_records,
                    "Day Ahead workbook first, fill gaps with ESIOS",
                    True,
                    token,
                )

            st.session_state["da3_result"] = {
                "target_day": target_day,
                "demand_result": demand_result,
                "demand_forecast": demand_forecast,
                "selected_demand_label": selected_demand_label,
                "thermal_result": thermal_result,
                "price_result": price_result,
            }
        except Exception as exc:
            st.error(f"Three-step forecast failed: {exc}")

    result = st.session_state.get("da3_result")
    if not result:
        st.info(
            "Press the button to generate tomorrow's demand, thermal-gap and "
            "day-ahead price curves. The first execution can take several "
            "minutes while historical ESIOS data are cached."
        )
        st.divider()
        return

    demand_result = result["demand_result"]
    demand_forecast = result["demand_forecast"]
    thermal_result = result["thermal_result"]
    thermal_forecast = thermal_result["forecast"]
    price_result = result["price_result"]
    price_forecast = price_result["forecast"]

    # -----------------------------------------------------
    # Step 1
    # -----------------------------------------------------
    da_section_header(
        "STEP 1",
        "Tomorrow's hourly demand",
        f"Downstream curve selected: {result['selected_demand_label']}.",
    )

    d1, d2, d3, d4, d5 = st.columns(5)
    selected_series = demand_forecast["selected_demand_mw"]
    peak_idx = selected_series.idxmax()
    min_idx = selected_series.idxmin()

    d1.metric(
        "Selected forecast average",
        f"{selected_series.mean():,.0f} MW",
    )
    d2.metric(
        "Selected forecast peak",
        f"{selected_series.max():,.0f} MW",
        delta=f"{int(demand_forecast.loc[peak_idx, 'hour']):02d}:00",
        delta_color="off",
    )
    d3.metric(
        "Selected forecast minimum",
        f"{selected_series.min():,.0f} MW",
        delta=f"{int(demand_forecast.loc[min_idx, 'hour']):02d}:00",
        delta_color="off",
    )
    d4.metric(
        "Model backtest MAPE",
        f"{demand_result['model_stats']['mape']:,.2f}%",
    )
    d5.metric(
        "Trend-adjusted D-7 MAPE",
        f"{demand_result['trend_stats']['mape']:,.2f}%",
    )

    st.altair_chart(
        da_demand_chart(demand_forecast),
        use_container_width=True,
    )
    st.caption(
        "The selected curve is the one passed into Step 2. The model uses "
        "calendar effects, forecast temperature and demand lags; the orange "
        "alternative explicitly corrects D-7 using D-2 versus D-9 and "
        "D-3 versus D-10."
    )

    # -----------------------------------------------------
    # Step 2
    # -----------------------------------------------------
    da_section_header(
        "STEP 2",
        "Non-thermal generation and thermal gap",
        "Forecast technologies are shown as a stack; the black line is the PBF-calibrated demand forecast.",
    )

    st.altair_chart(
        da_generation_demand_chart(
            thermal_result["generation_long"],
            thermal_forecast,
        ),
        use_container_width=True,
    )

    g1, g2, g3, g4, g5 = st.columns(5)
    gap = thermal_forecast["thermal_gap_forecast_mwh"]
    g1.metric(
        "Average PBF demand",
        f"{thermal_forecast['pbf_demand_forecast_mwh'].mean():,.0f} MW",
    )
    g2.metric(
        "Average non-thermal generation",
        f"{thermal_forecast['non_thermal_forecast_mwh'].mean():,.0f} MW",
    )
    g3.metric(
        "Average thermal gap",
        f"{gap.mean():,.0f} MW",
    )
    g4.metric(
        "Peak thermal gap",
        f"{gap.max():,.0f} MW",
    )
    g5.metric(
        "Negative-gap hours",
        f"{int((gap <= 0).sum())} h",
    )

    st.altair_chart(
        da_thermal_gap_chart(thermal_forecast),
        use_container_width=True,
    )

    with st.expander("Generation forecast quality and methodology"):
        stats = thermal_result["generation_stats"].copy()
        if not stats.empty:
            st.dataframe(
                stats,
                use_container_width=True,
                hide_index=True,
            )
        st.markdown(
            """
            - **Solar PV / solar thermal:** radiation, cloud cover, seasonality and recent PBF lags.
            - **Wind:** 100 m wind speed, calendar effects and recent PBF lags.
            - **Run-of-river:** Hydro non-UGH, recent dispatch and precipitation variables.
            - **Nuclear / other renewables:** recent hourly programme, seasonality and availability patterns.
            - Gross PBF is converted to **net PBF** using historical bilateral programmes.
            - Conventional bilateral programmes are forecast separately and deducted from the demand basis.
            """
        )
        if thermal_result["missing"]:
            st.markdown("**Missing optional ESIOS indicators**")
            st.write(thermal_result["missing"])

    # -----------------------------------------------------
    # Step 3
    # -----------------------------------------------------
    da_section_header(
        "STEP 3",
        "Tomorrow's DA spot-price forecast",
        "Absolute prices are anchored in D-1, D-7 and the recent same-hour profile; the model adjusts that anchor using the forecast thermal gap.",
    )

    p1, p2, p3, p4, p5 = st.columns(5)
    forecast_price = price_forecast["forecast_price_eur_mwh"]
    p1.metric(
        "Forecast baseload",
        f"{forecast_price.mean():,.2f} €/MWh",
    )
    p2.metric(
        "Forecast minimum",
        f"{forecast_price.min():,.2f} €/MWh",
    )
    p3.metric(
        "Forecast maximum",
        f"{forecast_price.max():,.2f} €/MWh",
    )
    p4.metric(
        "Forecast zero-price hours",
        f"{int((forecast_price == 0).sum())} h",
    )
    p5.metric(
        "Forecast TB4",
        f"{da_tb4_unconstrained(forecast_price):,.2f} €/MWh",
    )

    st.altair_chart(
        da_price_chart(price_forecast),
        use_container_width=True,
    )

    st.caption(
        f"Price model: {price_result['model_name']}. "
        f"Backtest MAPE: {price_result['model_stats']['mape']:,.2f}% "
        f"versus {price_result['anchor_stats']['mape']:,.2f}% for the "
        "unadjusted D-1 / D-7 anchor. Structural rule: thermal gap ≤ 0 "
        "implies 0 €/MWh."
    )

    # -----------------------------------------------------
    # Combined hourly output
    # -----------------------------------------------------
    combined = thermal_forecast.merge(
        price_forecast[
            [
                "datetime",
                "forecast_price_eur_mwh",
                "price_anchor",
                "price_lag_1d",
                "price_lag_7d",
                "zero_price_rule",
            ]
        ],
        on="datetime",
        how="left",
    )
    combined = combined.merge(
        demand_forecast[
            [
                "datetime",
                "model_forecast_mw",
                "trend_adjusted_lag_7d",
                "selected_demand_mw",
                "temperature_2m",
                "shortwave_radiation",
                "wind_speed_100m",
            ]
        ],
        on="datetime",
        how="left",
    )

    tech_wide = (
        thermal_result["generation_long"]
        .pivot_table(
            index="datetime",
            columns="technology",
            values="forecast_mwh",
            aggfunc="sum",
        )
        .reset_index()
    )
    tech_wide.columns.name = None
    combined = combined.merge(tech_wide, on="datetime", how="left")

    with st.expander("Hourly demand → generation → thermal gap → price output", expanded=True):
        display_cols = [
            "datetime",
            "selected_demand_mw",
            "pbf_demand_forecast_mwh",
            *[tech for tech in selected_techs if tech in combined.columns],
            "non_thermal_forecast_mwh",
            "conventional_bilateral_forecast_mwh",
            "thermal_gap_forecast_mwh",
            "forecast_price_eur_mwh",
            "price_anchor",
            "price_lag_1d",
            "price_lag_7d",
        ]
        display_cols = [c for c in display_cols if c in combined.columns]
        st.dataframe(
            combined[display_cols],
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download complete three-step forecast CSV",
            data=combined.to_csv(index=False).encode("utf-8"),
            file_name=f"day_ahead_3step_forecast_{target_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.warning(
        "This is an analytical forecast, not the official REE or OMIE forecast. "
        "The price rule that maps negative thermal gap to exactly zero is a "
        "deliberate modelling assumption requested for this prototype."
    )
    st.divider()


render_three_step_day_ahead_forecast()


# =========================================================
# UI
# =========================================================
st.title("Historical thermal gap and realised spot price")
st.success("Version loaded: OFFICIAL balancing v14 — price-setting gap with conventional bilaterals and optional interconnections")
st.caption(
    "PBF minus bilateral schedules. Prices are loaded with the same logic as the Day Ahead page. "
    "Everything is displayed in Madrid local time."
)

with st.expander("Indicator IDs", expanded=False):
    st.markdown("**Core**")
    st.write({"Spot price": PRICE_INDICATOR_ID, "Demand PBF": DEMAND_PBF_ID})
    st.markdown("**Gross PBF components**")
    st.json(PBF_GROSS_COMPONENTS)
    st.markdown("**Bilateral PBF components**")
    st.json(BILATERAL_FETCH_NAMES)

col1, col2 = st.columns(2)
with col1:
    start_day = st.date_input("Start day", value=date.today() - timedelta(days=8), key="historical_gap_start")
with col2:
    end_day = st.date_input("End day inclusive", value=date.today() - timedelta(days=2), key="historical_gap_end")

non_thermal_techs = st.multiselect(
    "Non-thermal net PBF components deducted from demand",
    options=list(PBF_GROSS_COMPONENTS.keys()),
    default=NON_THERMAL_TECHS_DEFAULT,
)

subtract_conventional_bilaterals = st.checkbox(
    "Deduct conventional bilateral programmes from PBF demand",
    value=True,
    help="Use this for a pool / price-setting gap: conventional bilateral volumes are already contracted outside the pool, so they should not be counted as conventional price-setting output.",
)
include_interconnections = st.checkbox(
    "Include PBF interconnection balance",
    value=False,
    help="Adds PBF net export balance to the gap. Positive saldo is treated as exports from Spain and increases the domestic generation requirement; negative saldo is treated as imports and reduces it.",
)

st.markdown(
    _thermal_gap_formula_html(
        non_thermal_techs,
        subtract_conventional_bilaterals=subtract_conventional_bilaterals,
        include_interconnections=include_interconnections,
    ),
    unsafe_allow_html=True,
)

show_diagnostics = st.checkbox("Show diagnostics and extra tables", value=False)
align_zero_axes = st.checkbox(
    "Align zero on both Y-axes",
    value=True,
    help="Forces the right price axis to place 0 €/MWh at the same vertical height as 0 MWh on the left thermal-gap axis.",
)
x_axis_style = st.selectbox(
    "X-axis style",
    ["Day label + hourly numbers 1-24", "Datetime labels"],
    index=0,
)
price_source_mode = st.selectbox(
    "Spot price source",
    [
        "Day Ahead workbook first, fill gaps with ESIOS",
        "ESIOS first, fill gaps with Day Ahead workbook",
        "Day Ahead workbook only",
        "ESIOS only",
    ],
    index=0,
    help=(
        "Use the same hourly price convention as the Day Ahead page: Madrid-local hourly timestamps. "
        "Workbook first is safer when the ESIOS live indicator returns values with an unexpected scale."
    ),
)
auto_fix_price_scale = st.checkbox(
    "Auto-fix suspicious ESIOS price scale",
    value=True,
    help="If live ESIOS prices look 10x too high versus normal Spanish spot prices, divide that live series by 10 before merging.",
)
show_balancing_chart = st.checkbox(
    "Show balancing energy upward/downward chart",
    value=True,
    help="Adds the monthly-style ESIOS balancing chart: upward and downward energy volumes plus average secondary reserve.",
)

if end_day < start_day:
    st.error("End day must be >= start day.")
    st.stop()

run = st.button("Fetch and plot", type="primary", use_container_width=True)

if run:
    try:
        token = require_esios_token()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    indicator_map = {"Total scheduled demand PBF": DEMAND_PBF_ID}
    indicator_map.update(PBF_GROSS_COMPONENTS)
    indicator_map.update(BILATERAL_FETCH_NAMES)
    indicator_map["Programa bilateral PBF Total Ventas"] = PBF_BILATERAL_TOTAL_SALES_ID
    if include_interconnections:
        indicator_map.update(PBF_INTERCONNECTION_COMPONENTS)

    raw, missing = fetch_named_indicators(indicator_map, start_day, end_day, token, warn_missing=show_diagnostics)

    if raw.empty:
        st.warning("No PBF data returned.")
        st.stop()

    wide = build_wide(raw)
    thermal, bilat_diag = calculate_thermal_gap(
        wide,
        non_thermal_techs,
        subtract_conventional_bilaterals=subtract_conventional_bilaterals,
        include_interconnections=include_interconnections,
    )

    prices = load_prices_like_day_ahead(token, start_day, end_day, price_source_mode, auto_fix_price_scale)

    thermal["datetime"] = pd.to_datetime(thermal["datetime"], errors="coerce").dt.floor("h")
    prices["datetime"] = pd.to_datetime(prices["datetime"], errors="coerce").dt.floor("h")

    df = thermal.merge(prices, on="datetime", how="left").sort_values("datetime")

    monthly = calculate_monthly_stats(df)

    # Always show minimal status so the page does not look like "nothing happens".
    st.info(
        f"Fetched {len(raw):,} raw rows | {len(df):,} hourly chart rows | "
        f"Price rows matched: {int(df['price'].notna().sum()) if 'price' in df.columns else 0:,} | "
        f"Missing price hours: {int(df['price'].isna().sum()) if 'price' in df.columns else len(df):,}"
    )
    if "price" in df.columns and df["price"].notna().any():
        price_sources = sorted(df["price_source"].dropna().astype(str).unique().tolist()) if "price_source" in df.columns else []
        st.caption(
            f"Spot price average for selected range: {df['price'].mean():,.2f} €/MWh. "
            f"Price source(s): {', '.join(price_sources) if price_sources else 'unknown'}."
        )

    # Confirm bilateral netting is happening.
    total_bilat = float(bilat_diag["bilateral_mwh"].sum()) if not bilat_diag.empty and "bilateral_mwh" in bilat_diag.columns else 0.0
    total_gross = float(bilat_diag["gross_mwh"].sum()) if not bilat_diag.empty and "gross_mwh" in bilat_diag.columns else 0.0
    st.caption(
        f"Bilateral schedules deducted: {total_bilat:,.0f} MWh deducted from {total_gross:,.0f} MWh gross PBF "
        f"({(total_bilat / total_gross * 100) if total_gross else 0:,.1f}%)."
    )

    formula_table = _thermal_gap_formula_table(df, non_thermal_techs)
    if not formula_table.empty:
        st.markdown("**Thermal gap formula — average contribution over selected period**")
        st.dataframe(formula_table, use_container_width=True, hide_index=True)

    if "raw_thermal_gap_mwh" in df.columns:
        st.caption(
            f"Thermal gap range: {df['raw_thermal_gap_mwh'].min():,.0f} to {df['raw_thermal_gap_mwh'].max():,.0f} MWh/h. "
            f"Price range: {df['price'].min():,.2f} to {df['price'].max():,.2f} €/MWh."
            if "price" in df.columns and df["price"].notna().any()
            else f"Thermal gap range: {df['raw_thermal_gap_mwh'].min():,.0f} to {df['raw_thermal_gap_mwh'].max():,.0f} MWh/h."
        )

    # If all thermal gap values are missing/zero, show the key input table.
    if df.empty or "raw_thermal_gap_mwh" not in df.columns or df["raw_thermal_gap_mwh"].dropna().empty:
        st.error("No thermal gap data available after merge/calculation.")
        st.dataframe(wide.head(50), use_container_width=True, hide_index=True)
        st.stop()

    if df["raw_thermal_gap_mwh"].abs().sum() == 0:
        st.warning("Thermal gap is all zero. Check demand/non-thermal indicator IDs or ESIOS returned empty series.")
        st.dataframe(df.head(50), use_container_width=True, hide_index=True)

    # -----------------------------------------------------
    # Main interactive chart + thermal-gap composition
    # -----------------------------------------------------
    st.subheader("Thermal Gap and Price")
    st.caption(
        "Columns: hourly thermal gap. Line: hourly spot price. "
        "The formula table above shows how the thermal gap changes when hydro is included or removed. "
        "X-axis is Europe/Madrid local time. The top chart only shows thermal gap on the left axis and spot price on the right axis."
    )

    plot_df = df.copy()
    plot_df["datetime"] = pd.to_datetime(plot_df["datetime"], errors="coerce")
    plot_df["raw_thermal_gap_mwh"] = pd.to_numeric(plot_df["raw_thermal_gap_mwh"], errors="coerce")
    plot_df["price"] = pd.to_numeric(plot_df["price"], errors="coerce") if "price" in plot_df.columns else pd.NA
    plot_df = plot_df.dropna(subset=["datetime", "raw_thermal_gap_mwh"]).sort_values("datetime").reset_index(drop=True)

    if plot_df.empty:
        st.error("Chart data is empty after datetime/thermal-gap cleaning.")
        st.dataframe(df.head(100), use_container_width=True, hide_index=True)
        st.stop()

    plot_df["datetime_label"] = plot_df["datetime"].dt.strftime("%Y-%m-%d %H:%M")
    plot_df["hour_1_24"] = plot_df["datetime"].dt.hour + 1
    plot_df["date_label"] = plot_df["datetime"].dt.strftime("%Y-%m-%d")

    thermal_y_domain = padded_zero_domain(plot_df["raw_thermal_gap_mwh"])
    price_y_domain = None
    if "price" in plot_df.columns and plot_df["price"].notna().any():
        if align_zero_axes:
            price_y_domain = align_second_axis_zero_domain(thermal_y_domain, plot_df["price"])
        else:
            price_y_domain = padded_zero_domain(plot_df["price"])

    x_axis = alt.Axis(
        title="Madrid local time",
        format="%H" if x_axis_style == "Day label + hourly numbers 1-24" else "%d-%b %H:%M",
        labelAngle=-90,
    )

    thermal_bars = (
        alt.Chart(plot_df)
        .mark_bar(opacity=0.88, color="#2F73C8")
        .encode(
            x=alt.X("datetime:T", title="Madrid local time", axis=x_axis),
            y=alt.Y(
                "raw_thermal_gap_mwh:Q",
                title="Thermal Gap (MWh)",
                axis=alt.Axis(format="~s", orient="left", titleColor="#2F73C8", labelColor="#2F73C8"),
                scale=alt.Scale(domain=thermal_y_domain),
            ),
            tooltip=[
                alt.Tooltip("datetime_label:N", title="Madrid time"),
                alt.Tooltip("date_label:N", title="Date"),
                alt.Tooltip("hour_1_24:Q", title="Hour", format=".0f"),
                alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap (MWh)", format=",.0f"),
                alt.Tooltip("Total scheduled demand PBF:Q", title="Demand PBF (MWh)", format=",.0f"),
                alt.Tooltip("non_thermal_net_pbf_mwh:Q", title="Non-thermal net PBF (MWh)", format=",.0f"),
            ],
        )
    )

    if "price" in plot_df.columns and plot_df["price"].notna().any():
        price_source_in_tooltip = [alt.Tooltip("price_source:N", title="Price source")] if "price_source" in plot_df.columns else []
        price_line = (
            alt.Chart(plot_df.dropna(subset=["price"]))
            .mark_line(strokeWidth=2.5, color="black")
            .encode(
                x=alt.X("datetime:T", title="Madrid local time", axis=x_axis),
                y=alt.Y(
                    "price:Q",
                    title="Spot price (€/MWh)",
                    axis=alt.Axis(format=".0f", orient="right", titleColor="black", labelColor="black"),
                    scale=alt.Scale(domain=price_y_domain),
                ),
                tooltip=[
                    alt.Tooltip("datetime_label:N", title="Madrid time"),
                    alt.Tooltip("price:Q", title="Spot price (€/MWh)", format=",.2f"),
                    *price_source_in_tooltip,
                    alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap (MWh)", format=",.0f"),
                ],
            )
        )
        price_points = (
            alt.Chart(plot_df.dropna(subset=["price"]))
            .mark_point(size=40, opacity=0.001, filled=True)
            .encode(
                x="datetime:T",
                y=alt.Y("price:Q", axis=None, scale=alt.Scale(domain=price_y_domain)),
                tooltip=[
                    alt.Tooltip("datetime_label:N", title="Madrid time"),
                    alt.Tooltip("price:Q", title="Spot price (€/MWh)", format=",.2f"),
                    alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap (MWh)", format=",.0f"),
                ],
            )
        )
        main_chart = alt.layer(thermal_bars, price_line, price_points).resolve_scale(y="independent")
    else:
        st.warning("No price data matched the hourly thermal-gap data. Showing only thermal-gap bars.")
        main_chart = alt.layer(thermal_bars)

    st.altair_chart(
        main_chart.properties(height=470).interactive(bind_y=False),
        use_container_width=True,
    )

    if align_zero_axes:
        st.caption("Zero alignment enabled: the thermal-gap and spot-price axes are scaled so 0 appears at the same vertical height.")

    st.markdown(
        "- **Left Y-axis**: hourly thermal gap, MWh/h\n"
        "- **Right Y-axis**: hourly spot price, €/MWh\n"
        "- **X-axis**: Madrid local time\n"
        "- **Bilateral schedules**: deducted before calculating the thermal gap"
    )

    # -----------------------------------------------------
    # Thermal-gap composition by technology
    # -----------------------------------------------------
    st.subheader("Thermal Gap composition by technology")
    st.caption(
        "The bottom chart shows the net PBF technologies that remain inside the price-setting thermal gap after the selected deductions. "
        "Conventional bilateral volumes are deducted from the demand basis, so the stack represents net conventional volumes that can still help set the day-ahead price. "
        "If Hydro UGH / Hydro non-UGH is removed from the formula selector, it appears here as part of the residual stack. "
        "When the thermal gap is negative, positive technologies are not forced below zero; the negative value is shown as a separate 'negative residual' bucket."
    )

    # Technologies included in the residual stack are all net PBF technologies NOT deducted in the formula.
    # This means Hydro non-UGH / Hydro UGH automatically move into the bottom composition chart when removed
    # from the non-thermal selector above.
    residual_techs = []
    for tech in PBF_GROSS_COMPONENTS.keys():
        if tech in non_thermal_techs:
            continue
        col = f"{tech} net PBF"
        if col in plot_df.columns:
            residual_techs.append((tech, col))

    if not residual_techs:
        st.warning("No residual net PBF technology columns are available for the composition chart.")
    else:
        comp_base_cols = ["datetime", "datetime_label", "hour_1_24", "raw_thermal_gap_mwh"] + [c for _, c in residual_techs]
        comp_wide = plot_df[comp_base_cols].copy()
        value_cols = [c for _, c in residual_techs]
        comp_wide[value_cols] = comp_wide[value_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
        comp_wide["listed_residual_stack_mwh"] = comp_wide[value_cols].sum(axis=1)
        comp_wide["unallocated_gap_mwh"] = comp_wide["raw_thermal_gap_mwh"] - comp_wide["listed_residual_stack_mwh"]

        comp_alloc = comp_wide.copy()
        positive_gap_mask = comp_alloc["raw_thermal_gap_mwh"] > 0
        valid_positive_stack = positive_gap_mask & (comp_alloc["listed_residual_stack_mwh"].abs() > 1e-9)

        # For positive thermal gap hours: scale only positive residual technologies proportionally so the
        # stack matches the top chart. For negative gap hours: do NOT assign negative values to technologies
        # like cogeneration; show the negative residual as its own explanatory bucket instead.
        for col in value_cols:
            original = comp_wide[col].copy()
            comp_alloc[col] = 0.0
            comp_alloc.loc[valid_positive_stack, col] = (
                original.loc[valid_positive_stack]
                / comp_wide.loc[valid_positive_stack, "listed_residual_stack_mwh"]
                * comp_wide.loc[valid_positive_stack, "raw_thermal_gap_mwh"]
            )
        comp_alloc["allocated_total_mwh"] = comp_alloc[value_cols].sum(axis=1)

        tech_rename = {col: tech for tech, col in residual_techs}
        stack_df = comp_alloc.melt(
            id_vars=["datetime", "datetime_label", "hour_1_24", "raw_thermal_gap_mwh", "allocated_total_mwh"],
            value_vars=value_cols,
            var_name="technology_col",
            value_name="mwh",
        )
        stack_df["technology"] = stack_df["technology_col"].map(tech_rename).fillna(stack_df["technology_col"])
        stack_df["mwh"] = pd.to_numeric(stack_df["mwh"], errors="coerce").fillna(0.0)
        stack_df = stack_df[stack_df["mwh"].abs() > 1e-9].copy()

        negative_rows = comp_alloc.loc[comp_alloc["raw_thermal_gap_mwh"] < 0, ["datetime", "datetime_label", "hour_1_24", "raw_thermal_gap_mwh", "allocated_total_mwh"]].copy()
        if not negative_rows.empty:
            negative_rows["technology_col"] = "negative_residual"
            negative_rows["technology"] = "Negative residual / excess deducted"
            negative_rows["mwh"] = negative_rows["raw_thermal_gap_mwh"]
            stack_df = pd.concat([stack_df, negative_rows], ignore_index=True, sort=False)

        if stack_df.empty:
            st.warning("All residual technology contributions are zero for the selected period.")
        else:
            comp_y_domain = padded_zero_domain(
                pd.concat([stack_df["mwh"], comp_alloc["raw_thermal_gap_mwh"], comp_alloc["allocated_total_mwh"]], ignore_index=True)
            )

            stacked_bars = (
                alt.Chart(stack_df)
                .mark_bar(opacity=0.97)
                .encode(
                    x=alt.X("datetime:T", title="Madrid local time", axis=x_axis),
                    y=alt.Y(
                        "mwh:Q",
                        stack="zero",
                        title="Residual thermal gap by technology (MWh)",
                        axis=alt.Axis(format="~s", orient="left", titleColor="#2F73C8", labelColor="#2F73C8"),
                        scale=alt.Scale(domain=comp_y_domain),
                    ),
                    color=alt.Color(
                        "technology:N",
                        title="Technology",
                        legend=alt.Legend(orient="top", direction="horizontal", labelLimit=280, columns=3),
                    ),
                    tooltip=[
                        alt.Tooltip("datetime_label:N", title="Madrid time"),
                        alt.Tooltip("hour_1_24:Q", title="Hour", format=".0f"),
                        alt.Tooltip("technology:N", title="Technology"),
                        alt.Tooltip("mwh:Q", title="Displayed contribution (MWh)", format=",.0f"),
                        alt.Tooltip("allocated_total_mwh:Q", title="Positive stack total (MWh)", format=",.0f"),
                        alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap total (MWh)", format=",.0f"),
                    ],
                )
            )

            composition_chart = alt.layer(stacked_bars).properties(height=520).interactive(bind_y=False)
            st.altair_chart(composition_chart, use_container_width=True)

            neg_hours = int((comp_wide["raw_thermal_gap_mwh"] < 0).sum())
            if neg_hours:
                st.info(
                    f"There are {neg_hours:,} hours with negative thermal gap. In those hours the selected deductions exceed PBF demand, "
                    "so the chart shows a separate negative residual instead of assigning negative MWh to positive technologies such as cogeneration."
                )

            unallocated_abs = float(comp_wide["unallocated_gap_mwh"].abs().sum())
            thermal_abs = float(comp_wide["raw_thermal_gap_mwh"].abs().sum())
            if unallocated_abs > max(1e-6, 0.01 * thermal_abs):
                st.info(
                    "For positive thermal-gap hours, listed residual technologies are scaled proportionally to match the top thermal-gap total hour by hour. "
                    "The original difference between listed residual technologies and the thermal-gap formula is shown in diagnostics."
                )

            with st.expander("Thermal-gap composition data", expanded=False):
                st.dataframe(
                    stack_df[["datetime_label", "technology", "mwh", "allocated_total_mwh", "raw_thermal_gap_mwh"]]
                    .rename(columns={
                        "datetime_label": "Madrid time",
                        "mwh": "Displayed contribution (MWh)",
                        "allocated_total_mwh": "Positive stack total (MWh)",
                        "raw_thermal_gap_mwh": "Thermal gap total (MWh)",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

            with st.expander("Thermal-gap reconciliation diagnostics", expanded=False):
                st.dataframe(
                    comp_wide[["datetime_label", "raw_thermal_gap_mwh", "listed_residual_stack_mwh", "unallocated_gap_mwh"]].rename(columns={
                        "datetime_label": "Madrid time",
                        "raw_thermal_gap_mwh": "Thermal gap total (MWh)",
                        "listed_residual_stack_mwh": "Original listed residual techs (MWh)",
                        "unallocated_gap_mwh": "Original difference vs thermal gap (MWh)",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )


    # -----------------------------------------------------
    # Balancing energy: upward vs downward
    # -----------------------------------------------------
    if show_balancing_chart:
        st.subheader("Balancing energy — upward vs downward")
        st.caption(
            "ESIOS indicators are summed over the selected range for balancing energy volumes. "
            "Upward and downward are kept separate; downward is only plotted below zero for visual comparison. "
            "Secondary reserve is averaged in MW and its band price is displayed as hourly-equivalent €/MW/h (= published €/MW/15min × 4) when available."
        )

        with st.spinner("Fetching ESIOS balancing-energy indicators..."):
            balancing_energy, balancing_reserve, balancing_missing = build_balancing_energy_summary(start_day, end_day, token)

        if balancing_energy.empty:
            st.warning("No balancing-energy rows could be built for the selected period.")
        else:
            title_suffix = f" | {start_day:%d-%b-%Y} to {end_day:%d-%b-%Y}"
            balancing_chart = plot_balancing_energy_summary_altair(balancing_energy, balancing_reserve, title_suffix=title_suffix)
            st.altair_chart(balancing_chart, use_container_width=True)
            st.caption("Volume and price are also shown in the tables below so the downward bars do not depend on reading labels inside the chart.")

            downward_focus = _balancing_downward_focus_table(balancing_energy)
            if not downward_focus.empty:
                st.markdown('<div class="downward-card"><b>Downward balancing — volume and average price</b><br>Dedicated table for the red/downward bars in the chart.</div>', unsafe_allow_html=True)
                st.dataframe(downward_focus, use_container_width=True, hide_index=True)

            st.subheader("Average hourly price profiles — H1 to H24")
            st.caption("Charts are shown full-width, one below another, in static photo mode. Exact values are displayed in visible tables underneath each chart, so no hover is needed.")
            hourly_profile_rows = []
            hourly_profile_missing: list[str] = []
            for profile_name in [
                "aFRR capacity price",
                "Day-ahead constraints Phase I price",
                "Day-ahead constraints Phase II price",
            ]:
                with st.spinner(f"Fetching {profile_name} hourly profile..."):
                    profile_df, profile_meta, profile_missing = build_hourly_price_profile(profile_name, start_day, end_day, token)
                hourly_profile_missing.extend(profile_missing)
                if profile_df.empty:
                    st.warning(f"No data returned for {profile_name}.")
                    continue
                hourly_profile_rows.append(profile_df)
                st.altair_chart(plot_hourly_price_profile_altair(profile_df, profile_meta), use_container_width=False)
                st.caption("Static chart mode: exact values are shown in the table below.")
                profile_table = build_hourly_price_profile_table(profile_df, profile_meta)
                if not profile_table.empty:
                    st.dataframe(profile_table, use_container_width=True, hide_index=True)

            if hourly_profile_rows:
                hourly_profiles_all = pd.concat(hourly_profile_rows, ignore_index=True)
                with st.expander("Hourly price profile data", expanded=False):
                    st.dataframe(hourly_profiles_all, use_container_width=True, hide_index=True)
                    if hourly_profile_missing:
                        st.markdown("**Missing/empty hourly price profile indicators**")
                        st.write(hourly_profile_missing)
                    st.download_button(
                        "Download hourly price profiles CSV",
                        hourly_profiles_all.to_csv(index=False).encode("utf-8"),
                        file_name=f"balancing_hourly_price_profiles_{start_day}_{end_day}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

            price_view_cols = [
                "category",
                "upward_gwh",
                "upward_avg_price_eur_mwh",
                "upward_cost_meur",
                "downward_gwh",
                "downward_avg_price_eur_mwh",
                "downward_cost_meur",
            ]
            price_view_cols = [c for c in price_view_cols if c in balancing_energy.columns]
            st.markdown("**Visible volume / average price table**")
            st.dataframe(
                balancing_energy[price_view_cols].rename(columns={
                    "category": "Category",
                    "upward_gwh": "Upward volume (GWh)",
                    "upward_avg_price_eur_mwh": "Upward avg price (€/MWh)",
                    "upward_cost_meur": "Upward cost estimate (M€)",
                    "downward_gwh": "Downward volume (GWh)",
                    "downward_avg_price_eur_mwh": "Downward avg price (€/MWh)",
                    "downward_cost_meur": "Downward cost estimate (M€)",
                }),
                use_container_width=True,
                hide_index=True,
            )
            reserve_view_cols = [c for c in ["direction", "avg_mw", "avg_price_eur_mw_h", "avg_price_eur_mw_15min", "price_ids"] if c in balancing_reserve.columns]
            if reserve_view_cols:
                st.markdown("**Visible secondary reserve / average band price table**")
                st.dataframe(
                    balancing_reserve[reserve_view_cols].rename(columns={
                        "direction": "Direction",
                        "avg_mw": "Average reserve (MW)",
                        "avg_price_eur_mw_h": "Average band price hourly eq. (€/MW/h)",
                        "avg_price_eur_mw_15min": "Published band price (€/MW/15min)",
                        "price_ids": "Price indicator IDs",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Upward balancing energy", f"{balancing_energy['upward_gwh'].sum():,.0f} GWh")
            with c2:
                st.metric("Downward balancing energy", f"{balancing_energy['downward_gwh'].sum():,.0f} GWh")
            with c3:
                net_gwh = balancing_energy["upward_gwh"].sum() - balancing_energy["downward_gwh"].sum()
                st.metric("Upward minus downward", f"{net_gwh:,.0f} GWh")

            with st.expander("Balancing-energy data and indicator diagnostics", expanded=False):
                st.markdown("**Energy volumes by product**")
                st.dataframe(balancing_energy, use_container_width=True, hide_index=True)
                st.markdown("**Average secondary reserve and directional band prices**")
                st.dataframe(balancing_reserve, use_container_width=True, hide_index=True)
                st.markdown("**Indicator IDs used**")
                st.json({
                    "energy_sections": BALANCING_ENERGY_SECTIONS,
                    "price_sections": globals().get("BALANCING_PRICE_SECTIONS", {}),
                    "secondary_reserve": BALANCING_SECONDARY_RESERVE_IDS,
                    "secondary_reserve_price_directional": globals().get("BALANCING_SECONDARY_RESERVE_PRICE_IDS", {}),
                    "secondary_reserve_hourly_aggregation": "average",
                    "price_hourly_aggregation": "average, then volume-weighted when hourly volume alignment is available",
                })
                breakdown = build_balancing_indicator_breakdown(start_day, end_day, token)
                if not breakdown.empty:
                    st.markdown("**Per-indicator contribution breakdown**")
                    st.dataframe(breakdown, use_container_width=True, hide_index=True)
                if balancing_missing:
                    st.markdown("**Missing/empty indicators**")
                    st.write(balancing_missing)

            with st.expander("Technology breakdown from official REE/e·sios detail files", expanded=False):
                st.markdown(
                    "This part cannot be derived from aggregate indicators like 701/702. "
                    "Upload the official detailed restrictions file from e·sios downloads, and optionally the official structural UP/UF mapping file. "
                    "If the restrictions file already contains a technology column, the mapping file is not needed."
                )
                st.markdown(
                    "Official sources: [e·sios downloads](https://www.esios.ree.es/es/descargas) and "
                    "[Unidades de programación](https://www.esios.ree.es/es/unidades-de-programacion)."
                )
                tech_col1, tech_col2 = st.columns(2)
                with tech_col1:
                    tech_category = st.selectbox(
                        "Balancing bucket to break down",
                        options=list(BALANCING_ENERGY_SECTIONS.keys()),
                        index=0,
                        key="balancing_tech_category",
                    )
                with tech_col2:
                    tech_direction = st.selectbox(
                        "Direction",
                        options=["Upward", "Downward"],
                        index=0,
                        key="balancing_tech_direction",
                    )

                restrictions_upload = st.file_uploader(
                    "Official restrictions detail file CSV/XLSX",
                    type=["csv", "txt", "xlsx", "xls"],
                    key="official_restrictions_detail_upload",
                )
                mapping_upload = st.file_uploader(
                    "Optional official UP/UF technology mapping CSV/XLSX",
                    type=["csv", "txt", "xlsx", "xls"],
                    key="official_unit_technology_mapping_upload",
                )

                if restrictions_upload is not None:
                    detail_df = _read_tabular_upload(restrictions_upload)
                    mapping_df = _read_tabular_upload(mapping_upload) if mapping_upload is not None else pd.DataFrame()
                    tech_breakdown, tech_notes = build_technology_breakdown_from_official_files(
                        detail_df,
                        mapping_df,
                        tech_category,
                        tech_direction,
                    )
                    if tech_notes:
                        for note in tech_notes:
                            st.caption(note)
                    if tech_breakdown.empty:
                        st.warning("Could not build a technology breakdown from the uploaded file. Open the preview below and check column names.")
                    else:
                        st.altair_chart(
                            plot_technology_breakdown_altair(tech_breakdown, tech_category, tech_direction),
                            use_container_width=True,
                        )
                        st.dataframe(tech_breakdown, use_container_width=True, hide_index=True)
                        st.download_button(
                            "Download technology breakdown CSV",
                            tech_breakdown.to_csv(index=False).encode("utf-8"),
                            file_name=f"technology_breakdown_{tech_category}_{tech_direction}_{start_day}_{end_day}.csv".replace(" ", "_"),
                            mime="text/csv",
                            use_container_width=True,
                        )
                    with st.expander("Uploaded restrictions file preview", expanded=False):
                        st.write({"columns": list(detail_df.columns), "rows": int(len(detail_df))})
                        st.dataframe(detail_df.head(50), use_container_width=True, hide_index=True)
                else:
                    st.info("Upload the official detailed restrictions file to calculate the technology split.")

            csv_buf = pd.concat(
                [
                    balancing_energy.assign(table="energy_gwh"),
                    balancing_reserve.rename(columns={"direction": "category", "avg_mw": "upward_gwh"}).assign(
                        downward_gwh=pd.NA,
                        table="secondary_reserve_avg_mw",
                    ),
                ],
                ignore_index=True,
                sort=False,
            )
            st.download_button(
                "Download balancing chart data CSV",
                csv_buf.to_csv(index=False).encode("utf-8"),
                file_name=f"balancing_energy_up_down_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )


    # -----------------------------------------------------
    # REE demand average 24h profile — from the working demand test
    # -----------------------------------------------------
    st.subheader("REE Península demand — monthly average hourly shape")
    st.caption(
        "Public REE demanda/evolucion hourly pull. Values are converted to Europe/Madrid local time and averaged by hour of day."
    )

    selected_month_start, selected_month_natural_end = public_month_bounds(start_day)
    selected_month_end = min(end_day, selected_month_natural_end)
    if selected_month_end < selected_month_start:
        selected_month_end = selected_month_natural_end

    prev_month_start, prev_month_end = public_previous_month_bounds(selected_month_start)

    with st.spinner("Pulling REE demanda/evolucion hourly demand data..."):
        selected_hourly, sel_info = fetch_ree_demand_evolution_public(
            selected_month_start,
            selected_month_end,
            time_trunc="hour",
        )
        prev_hourly, prev_info = fetch_ree_demand_evolution_public(
            prev_month_start,
            prev_month_end,
            time_trunc="hour",
        )

    if selected_hourly.empty:
        st.warning("No hourly demand rows returned from REE demanda/evolucion for the selected month.")
        with st.expander("REE demand diagnostics", expanded=False):
            st.json({"selected": sel_info, "previous": prev_info})
    else:
        sel_label = (
            f"{selected_month_start:%b-%Y}"
            if selected_month_end == selected_month_natural_end
            else f"{selected_month_start:%b-%Y} MTD to {selected_month_end:%d-%b}"
        )
        prev_label = f"{prev_month_start:%b-%Y}"

        sel_summary = build_demand_monthly_summary(selected_hourly)
        prev_summary = build_demand_monthly_summary(prev_hourly) if not prev_hourly.empty else {}

        demand_delta = pct_delta(sel_summary.get("demand_gwh"), prev_summary.get("demand_gwh"))
        avg_delta = pct_delta(sel_summary.get("avg_demand_gw"), prev_summary.get("avg_demand_gw"))
        peak_delta = pct_delta(sel_summary.get("max_hourly_gw"), prev_summary.get("max_hourly_gw"))
        lf_delta = pct_delta(sel_summary.get("load_factor"), prev_summary.get("load_factor"))

        d1, d2, d3, d4, d5 = st.columns(5)
        with d1:
            st.metric(f"Demand total | {sel_label}", f"{sel_summary.get('demand_gwh'):,.1f} GWh" if sel_summary.get("demand_gwh") is not None else "—")
            st.caption(delta_text(None if demand_delta is None else demand_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False))
        with d2:
            st.metric("Average demand", f"{sel_summary.get('avg_demand_gw'):,.2f} GW" if sel_summary.get("avg_demand_gw") is not None else "—")
            st.caption(delta_text(None if avg_delta is None else avg_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False))
        with d3:
            st.metric("Max hourly demand", f"{sel_summary.get('max_hourly_gw'):,.2f} GW" if sel_summary.get("max_hourly_gw") is not None else "—")
            st.caption(delta_text(None if peak_delta is None else peak_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False))
        with d4:
            lf = sel_summary.get("load_factor")
            st.metric("Load factor", f"{lf * 100:,.1f}%" if lf is not None else "—")
            st.caption(delta_text(None if lf_delta is None else lf_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=True))
        with d5:
            st.metric("Days included", f"{sel_summary.get('days'):,.0f} d" if sel_summary.get("days") is not None else "—")
            if sel_summary.get("peak_hour") is not None:
                st.caption(f"Peak hour: {pd.Timestamp(sel_summary['peak_hour']):%d-%b %H:%M}")

        profiles = [build_demand_hourly_profile(selected_hourly, sel_label)]
        if not prev_hourly.empty:
            profiles.append(build_demand_hourly_profile(prev_hourly, prev_label))

        profile_df = pd.concat([p for p in profiles if p is not None and not p.empty], ignore_index=True)
        if not profile_df.empty:
            profile_chart = alt.Chart(profile_df).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("hour:O", title="Hour of day", sort=list(range(24))),
                y=alt.Y("avg_gw:Q", title="Average hourly demand (GW)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "label:N",
                    title="Month",
                    legend=alt.Legend(orient="top", direction="horizontal", labelLimit=360, titleLimit=360),
                ),
                strokeDash=alt.StrokeDash("label:N", legend=None),
                tooltip=[
                    alt.Tooltip("label:N", title="Month"),
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f"),
                    alt.Tooltip("min_gw:Q", title="Min GW", format=".2f"),
                    alt.Tooltip("max_gw:Q", title="Max GW", format=".2f"),
                    alt.Tooltip("obs:Q", title="Obs", format=",d"),
                ],
            ).properties(height=380)
            st.altair_chart(profile_chart, use_container_width=True)

        wd = build_demand_weekday_hourly_profile(selected_hourly)
        if not wd.empty:
            wd_chart = alt.Chart(wd).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("hour:O", title="Hour of day", sort=list(range(24))),
                y=alt.Y("avg_gw:Q", title="Average demand (GW)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "day_type:N",
                    title="Day type",
                    legend=alt.Legend(orient="top", direction="horizontal"),
                ),
                tooltip=[
                    alt.Tooltip("day_type:N", title="Day type"),
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f"),
                    alt.Tooltip("obs:Q", title="Obs", format=",d"),
                ],
            ).properties(height=330)
            st.altair_chart(wd_chart, use_container_width=True)

        daily = build_demand_daily_avg_profile(selected_hourly)
        if not daily.empty:
            daily_chart = alt.Chart(daily).mark_bar().encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("avg_gw:Q", title="Daily average demand (GW)", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"),
                    alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f"),
                    alt.Tooltip("max_gw:Q", title="Max GW", format=".2f"),
                    alt.Tooltip("min_gw:Q", title="Min GW", format=".2f"),
                ],
            ).properties(height=300)
            st.altair_chart(daily_chart, use_container_width=True)

        with st.expander("REE demand diagnostics", expanded=False):
            st.json({"selected": sel_info, "previous": prev_info})
            st.dataframe(selected_hourly.head(200), use_container_width=True, hide_index=True)


    if show_diagnostics:
        st.subheader("Diagnostics")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("First hour", str(df["datetime"].min()))
        with c2:
            st.metric("Last hour", str(df["datetime"].max()))
        with c3:
            st.metric("Missing price hours", int(df["price"].isna().sum()))
        with c4:
            st.metric("Missing optional indicators", len(missing))

        st.markdown("**Monthly stats**")
        st.dataframe(monthly, use_container_width=True, hide_index=True)

        with st.expander("Price diagnostics", expanded=False):
            if df["price"].notna().any():
                st.write({
                    "min": float(df["price"].min()),
                    "avg": float(df["price"].mean()),
                    "max": float(df["price"].max()),
                })
                st.dataframe(
                    df[["datetime", "price"]].dropna().sort_values("price", ascending=False).head(25),
                    use_container_width=True,
                    hide_index=True,
                )

        with st.expander("Bilateral netting diagnostics", expanded=False):
            st.dataframe(bilat_diag, use_container_width=True, hide_index=True)

        with st.expander("Missing indicators", expanded=False):
            st.write(missing)

        with st.expander("Chart data", expanded=False):
            cols = [
                "datetime",
                "raw_thermal_gap_mwh",
                "price",
                "price_source",
                "Total scheduled demand PBF",
                "non_thermal_net_pbf_mwh",
            ]
            cols = [c for c in cols if c in df.columns]
            st.dataframe(df[cols], use_container_width=True, hide_index=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "Download chart data CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name=f"hueco_termico_precio_pbf_net_bilat_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "Download bilateral diagnostics CSV",
                bilat_diag.to_csv(index=False).encode("utf-8"),
                file_name=f"bilateral_diagnostics_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c3:
            st.download_button(
                "Download raw indicators CSV",
                raw.to_csv(index=False).encode("utf-8"),
                file_name=f"raw_pbf_bilaterals_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
