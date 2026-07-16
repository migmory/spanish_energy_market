import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

import altair as alt
import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import HistGradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except Exception:
    HistGradientBoostingRegressor = None
    SKLEARN_AVAILABLE = False

from dateutil.easter import easter
import requests
import streamlit as st
from dotenv import load_dotenv


# =========================================================
# PAGE / ENV
# =========================================================
st.set_page_config(
    page_title="Test - Demand, temperature and PBF",
    layout="wide",
)

CURRENT_FILE = Path(__file__).resolve()

ENV_CANDIDATES = [
    CURRENT_FILE.parent / ".env",
    CURRENT_FILE.parent.parent / ".env",
]

for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_dotenv(env_path, override=True)
        break

MADRID_TZ = ZoneInfo("Europe/Madrid")

ESIOS_API_BASE = "https://api.esios.ree.es/indicators"
REDATA_DEMAND_URL = "https://apidatos.ree.es/es/datos/demanda/evolucion"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
DEMAND_BLUE = "#1D4ED8"
TEMPERATURE_ORANGE = "#EA580C"

MAX_RANGE_DAYS = 124

FORECAST_VALIDATION_DAYS = 42

# =========================================================
# MIBGAS D-1 INPUT
# =========================================================
# This reads the same GDAES_D+1 Reference Price dataset used by the MIBGAS
# Streamlit page. The feature used for electricity delivery day D is the gas
# price for gas delivery day D-1, which is a conservative no-leakage input.
MIBGAS_TARGET_SHEET = "Trading Data PVB&VTP"
MIBGAS_LOCAL_FILE_PATTERN = "MIBGAS_Data_*.xlsx"
MIBGAS_CACHE_FILENAME = "mibgas_2026_cache.csv"


# =========================================================
# PBF INDICATORS
# =========================================================
# The composite indicators are used where available so that the chart resembles
# the technology groups displayed in the ESIOS programmed-generation balance.
#
# PBF values are requested with:
#   time_trunc=day
#   time_agg=sum
# so each returned value is treated as daily programmed energy in MWh.
PBF_TECH_INDICATORS = {
    "Hydro": [1, 2],
    "Pumped hydro": [3],
    "Nuclear": [4],
    "Coal": [10167],
    "CCGT": [9],
    "Fuel + Gas": [10077],
    "Wind": [10073],
    "Solar PV": [14],
    "Solar thermal": [15],
    "CHP": [10086],
    "Biomass": [21],
    "Biogas": [22],
    "Other renewables": [10074],
    "Other non-renewables": [10095],
}

PBF_TECH_ORDER = [
    "Hydro",
    "Pumped hydro",
    "Nuclear",
    "Coal",
    "CCGT",
    "Fuel + Gas",
    "Wind",
    "Solar PV",
    "Solar thermal",
    "CHP",
    "Biomass",
    "Biogas",
    "Other renewables",
    "Other non-renewables",
]

PBF_COLOR_DOMAIN = PBF_TECH_ORDER
PBF_COLOR_RANGE = [
    "#60A5FA",
    "#0284C7",
    "#C084FC",
    "#374151",
    "#9CA3AF",
    "#6B7280",
    "#2563EB",
    "#FACC15",
    "#FCA5A5",
    "#F97316",
    "#16A34A",
    "#22C55E",
    "#14B8A6",
    "#7C2D12",
]



# =========================================================
# DOWNSTREAM MARKET FORECAST
# =========================================================
PRICE_INDICATOR_ID = 600
PBF_DEMAND_INDICATOR_ID = 10141

# Gross PBF technology indicators.
FORECAST_NON_THERMAL_INDICATORS = {
    "Hydro UGH": [1],
    "Wind": [12, 13],
    "Solar PV": [14],
    "Solar thermal": [15],
    "Run-of-river": [2],  # Hydro non-UGH
    "Nuclear": [4],
    "Other renewables": [10074],
}

# Bilateral programmes must be deducted from gross PBF to obtain the net
# programme that is comparable with the price-setting thermal-gap definition.
FORECAST_BILATERAL_INDICATORS = {
    "Hydro UGH": [421],
    "Wind": [432, 433],
    "Solar PV": [],
    "Solar thermal": [],
    "Run-of-river": [422],
    "Nuclear": [424],
    "Other renewables": [10234],
}

FORECAST_NON_THERMAL_DEFAULT = [
    "Hydro UGH",
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Run-of-river",
    "Nuclear",
    "Other renewables",
]

FORECAST_TECH_COLORS = {
    "Hydro UGH": "#0EA5E9",
    "Wind": "#2563EB",
    "Solar PV": "#FACC15",
    "Solar thermal": "#FB923C",
    "Run-of-river": "#06B6D4",
    "Nuclear": "#A855F7",
    "Other renewables": "#10B981",
}

# Explicit D-1 calibration supplied by the user. It is used as a fallback for
# the 17-Jul-2026 forecast and as a validation reference. On later dates the
# app automatically downloads the corresponding D-1 PBF demand, gross
# generation and bilateral programmes from ESIOS.
EMBEDDED_D1_CALIBRATION_DATE = date(2026, 7, 16)
EMBEDDED_D1_CALIBRATION_RECORDS = [{'hour': 0,
  'pbf_demand_mwh': 29688.5,
  'Hydro UGH': 3970.0,
  'Run-of-river': 449.7,
  'Nuclear': 2111.1,
  'Wind': 4876.7,
  'Solar PV': 0.4,
  'Solar thermal': 452.8,
  'Other renewables': 578.1,
  'actual_thermal_gap_mwh': 17249.7,
  'price_eur_mwh': 67.842},
 {'hour': 1,
  'pbf_demand_mwh': 27744.8,
  'Hydro UGH': 3052.4,
  'Run-of-river': 447.3,
  'Nuclear': 2109.0,
  'Wind': 4753.6,
  'Solar PV': 5.8,
  'Solar thermal': 430.2,
  'Other renewables': 579.9,
  'actual_thermal_gap_mwh': 16366.6,
  'price_eur_mwh': 66.589},
 {'hour': 2,
  'pbf_demand_mwh': 26598.5,
  'Hydro UGH': 2803.4,
  'Run-of-river': 433.7,
  'Nuclear': 2108.8,
  'Wind': 4495.8,
  'Solar PV': 5.8,
  'Solar thermal': 386.6,
  'Other renewables': 580.0,
  'actual_thermal_gap_mwh': 15784.4,
  'price_eur_mwh': 66.574},
 {'hour': 3,
  'pbf_demand_mwh': 25986.2,
  'Hydro UGH': 2705.9,
  'Run-of-river': 432.3,
  'Nuclear': 2109.0,
  'Wind': 4315.5,
  'Solar PV': 0.0,
  'Solar thermal': 338.9,
  'Other renewables': 578.4,
  'actual_thermal_gap_mwh': 15506.2,
  'price_eur_mwh': 66.304},
 {'hour': 4,
  'pbf_demand_mwh': 25586.3,
  'Hydro UGH': 2620.6,
  'Run-of-river': 430.2,
  'Nuclear': 2108.8,
  'Wind': 4028.6,
  'Solar PV': 0.0,
  'Solar thermal': 256.1,
  'Other renewables': 578.4,
  'actual_thermal_gap_mwh': 15563.6,
  'price_eur_mwh': 66.26},
 {'hour': 5,
  'pbf_demand_mwh': 25622.8,
  'Hydro UGH': 2846.4,
  'Run-of-river': 427.7,
  'Nuclear': 2109.2,
  'Wind': 3586.0,
  'Solar PV': 0.2,
  'Solar thermal': 239.6,
  'Other renewables': 579.4,
  'actual_thermal_gap_mwh': 15834.3,
  'price_eur_mwh': 66.574},
 {'hour': 6,
  'pbf_demand_mwh': 26975.3,
  'Hydro UGH': 3643.9,
  'Run-of-river': 425.1,
  'Nuclear': 2106.8,
  'Wind': 3449.4,
  'Solar PV': 134.8,
  'Solar thermal': 149.3,
  'Other renewables': 577.9,
  'actual_thermal_gap_mwh': 16488.1,
  'price_eur_mwh': 67.3},
 {'hour': 7,
  'pbf_demand_mwh': 29242.7,
  'Hydro UGH': 3973.8,
  'Run-of-river': 424.1,
  'Nuclear': 2104.9,
  'Wind': 3175.1,
  'Solar PV': 2443.1,
  'Solar thermal': 241.2,
  'Other renewables': 573.8,
  'actual_thermal_gap_mwh': 16306.7,
  'price_eur_mwh': 68.218},
 {'hour': 8,
  'pbf_demand_mwh': 31194.3,
  'Hydro UGH': 2707.3,
  'Run-of-river': 422.6,
  'Nuclear': 2101.2,
  'Wind': 2741.5,
  'Solar PV': 10609.8,
  'Solar thermal': 383.3,
  'Other renewables': 572.3,
  'actual_thermal_gap_mwh': 11656.3,
  'price_eur_mwh': 64.82},
 {'hour': 9,
  'pbf_demand_mwh': 33024.5,
  'Hydro UGH': 1030.2,
  'Run-of-river': 424.9,
  'Nuclear': 2101.1,
  'Wind': 1643.6,
  'Solar PV': 20008.7,
  'Solar thermal': 924.6,
  'Other renewables': 572.8,
  'actual_thermal_gap_mwh': 6318.6,
  'price_eur_mwh': 54.556},
 {'hour': 10,
  'pbf_demand_mwh': 34430.6,
  'Hydro UGH': 897.9,
  'Run-of-river': 423.2,
  'Nuclear': 2098.9,
  'Wind': 939.8,
  'Solar PV': 25910.6,
  'Solar thermal': 1609.2,
  'Other renewables': 483.9,
  'actual_thermal_gap_mwh': 2067.1,
  'price_eur_mwh': 47.241},
 {'hour': 11,
  'pbf_demand_mwh': 36127.9,
  'Hydro UGH': 864.0,
  'Run-of-river': 336.8,
  'Nuclear': 2099.0,
  'Wind': 1073.3,
  'Solar PV': 28035.3,
  'Solar thermal': 1833.6,
  'Other renewables': 468.9,
  'actual_thermal_gap_mwh': 1417.0,
  'price_eur_mwh': 39.886},
 {'hour': 12,
  'pbf_demand_mwh': 37274.6,
  'Hydro UGH': 861.7,
  'Run-of-river': 311.8,
  'Nuclear': 2095.0,
  'Wind': 1407.9,
  'Solar PV': 29312.6,
  'Solar thermal': 1950.7,
  'Other renewables': 466.9,
  'actual_thermal_gap_mwh': 868.0,
  'price_eur_mwh': 39.25},
 {'hour': 13,
  'pbf_demand_mwh': 38294.8,
  'Hydro UGH': 823.2,
  'Run-of-river': 316.1,
  'Nuclear': 2091.2,
  'Wind': 1979.4,
  'Solar PV': 30061.0,
  'Solar thermal': 1974.7,
  'Other renewables': 466.3,
  'actual_thermal_gap_mwh': 582.9,
  'price_eur_mwh': 37.817},
 {'hour': 14,
  'pbf_demand_mwh': 38840.0,
  'Hydro UGH': 354.9,
  'Run-of-river': 297.7,
  'Nuclear': 2090.8,
  'Wind': 2709.8,
  'Solar PV': 30157.3,
  'Solar thermal': 1983.0,
  'Other renewables': 465.2,
  'actual_thermal_gap_mwh': 781.3,
  'price_eur_mwh': 35.725},
 {'hour': 15,
  'pbf_demand_mwh': 38727.1,
  'Hydro UGH': 90.2,
  'Run-of-river': 297.7,
  'Nuclear': 2089.2,
  'Wind': 3463.3,
  'Solar PV': 29821.2,
  'Solar thermal': 1985.1,
  'Other renewables': 455.9,
  'actual_thermal_gap_mwh': 524.5,
  'price_eur_mwh': 31.698},
 {'hour': 16,
  'pbf_demand_mwh': 38699.0,
  'Hydro UGH': 89.7,
  'Run-of-river': 298.2,
  'Nuclear': 2091.2,
  'Wind': 4175.4,
  'Solar PV': 28806.7,
  'Solar thermal': 1979.7,
  'Other renewables': 450.7,
  'actual_thermal_gap_mwh': 807.4,
  'price_eur_mwh': 30.316},
 {'hour': 17,
  'pbf_demand_mwh': 38866.1,
  'Hydro UGH': 91.2,
  'Run-of-river': 302.8,
  'Nuclear': 2091.1,
  'Wind': 4818.4,
  'Solar PV': 27368.1,
  'Solar thermal': 1971.7,
  'Other renewables': 461.1,
  'actual_thermal_gap_mwh': 1761.7,
  'price_eur_mwh': 31.924},
 {'hour': 18,
  'pbf_demand_mwh': 38463.4,
  'Hydro UGH': 319.7,
  'Run-of-river': 335.6,
  'Nuclear': 2089.0,
  'Wind': 5545.9,
  'Solar PV': 25138.8,
  'Solar thermal': 2027.1,
  'Other renewables': 473.0,
  'actual_thermal_gap_mwh': 2534.3,
  'price_eur_mwh': 33.401},
 {'hour': 19,
  'pbf_demand_mwh': 37624.3,
  'Hydro UGH': 966.9,
  'Run-of-river': 423.7,
  'Nuclear': 2096.8,
  'Wind': 7055.8,
  'Solar PV': 19005.2,
  'Solar thermal': 1947.2,
  'Other renewables': 485.0,
  'actual_thermal_gap_mwh': 5643.7,
  'price_eur_mwh': 40.922},
 {'hour': 20,
  'pbf_demand_mwh': 36057.3,
  'Hydro UGH': 2650.7,
  'Run-of-river': 436.7,
  'Nuclear': 2101.2,
  'Wind': 8955.1,
  'Solar PV': 9664.7,
  'Solar thermal': 1621.9,
  'Other renewables': 575.5,
  'actual_thermal_gap_mwh': 10051.5,
  'price_eur_mwh': 58.566},
 {'hour': 21,
  'pbf_demand_mwh': 33876.9,
  'Hydro UGH': 3987.6,
  'Run-of-river': 479.0,
  'Nuclear': 2112.8,
  'Wind': 8932.2,
  'Solar PV': 1877.2,
  'Solar thermal': 1129.2,
  'Other renewables': 577.4,
  'actual_thermal_gap_mwh': 14781.5,
  'price_eur_mwh': 71.865},
 {'hour': 22,
  'pbf_demand_mwh': 32050.7,
  'Hydro UGH': 4123.8,
  'Run-of-river': 493.8,
  'Nuclear': 2113.0,
  'Wind': 8993.1,
  'Solar PV': 19.1,
  'Solar thermal': 792.7,
  'Other renewables': 576.8,
  'actual_thermal_gap_mwh': 14938.4,
  'price_eur_mwh': 74.338},
 {'hour': 23,
  'pbf_demand_mwh': 29836.6,
  'Hydro UGH': 4078.8,
  'Run-of-river': 493.3,
  'Nuclear': 2103.0,
  'Wind': 8712.2,
  'Solar PV': 1.2,
  'Solar thermal': 710.3,
  'Other renewables': 578.6,
  'actual_thermal_gap_mwh': 13159.2,
  'price_eur_mwh': 70.573}]

# The forecast remains driven by the model, but is moderately anchored in the
# latest complete PBF day to avoid abrupt, implausible level shifts.
D1_DEMAND_BLEND_WEIGHT = 0.25
D1_GENERATION_BLEND_WEIGHT = 0.20


GENERATION_WEATHER_VARIABLES = [
    "temperature_2m",
    "shortwave_radiation",
    "cloud_cover",
    "wind_speed_100m",
    "precipitation",
]

GENERATION_WEATHER_POINTS = [
    {"latitude": 42.9, "longitude": -8.1, "weight": 1.0},
    {"latitude": 43.2, "longitude": -5.8, "weight": 1.0},
    {"latitude": 42.7, "longitude": -1.6, "weight": 1.0},
    {"latitude": 41.2, "longitude": -0.8, "weight": 1.0},
    {"latitude": 41.8, "longitude": -4.5, "weight": 1.0},
    {"latitude": 39.4, "longitude": -2.5, "weight": 1.0},
    {"latitude": 39.0, "longitude": -6.1, "weight": 1.0},
    {"latitude": 37.4, "longitude": -6.1, "weight": 1.0},
    {"latitude": 37.2, "longitude": -3.5, "weight": 1.0},
    {"latitude": 38.0, "longitude": -1.2, "weight": 1.0},
    {"latitude": 39.4, "longitude": -0.8, "weight": 1.0},
    {"latitude": 41.7, "longitude": 1.5, "weight": 1.0},
]


# =========================================================
# NATIONAL TEMPERATURE PROXY
# =========================================================
# Population-weighted proxy based on representative peninsular cities.
# The weights are deliberately simple and are normalised in the calculation.
# This is useful as an electricity-demand explanatory variable, but it is not
# an official AEMET national temperature index.
SPAIN_TEMPERATURE_POINTS = [
    {"city": "Madrid",      "latitude": 40.4168, "longitude": -3.7038, "weight": 0.220},
    {"city": "Barcelona",   "latitude": 41.3874, "longitude":  2.1686, "weight": 0.150},
    {"city": "Valencia",    "latitude": 39.4699, "longitude": -0.3763, "weight": 0.085},
    {"city": "Sevilla",     "latitude": 37.3891, "longitude": -5.9845, "weight": 0.070},
    {"city": "Málaga",      "latitude": 36.7213, "longitude": -4.4214, "weight": 0.050},
    {"city": "Zaragoza",    "latitude": 41.6488, "longitude": -0.8891, "weight": 0.045},
    {"city": "Murcia",      "latitude": 37.9922, "longitude": -1.1307, "weight": 0.040},
    {"city": "Bilbao",      "latitude": 43.2630, "longitude": -2.9350, "weight": 0.040},
    {"city": "Alicante",    "latitude": 38.3452, "longitude": -0.4810, "weight": 0.035},
    {"city": "Valladolid",  "latitude": 41.6523, "longitude": -4.7245, "weight": 0.030},
    {"city": "A Coruña",    "latitude": 43.3623, "longitude": -8.4115, "weight": 0.028},
    {"city": "Vigo",        "latitude": 42.2406, "longitude": -8.7207, "weight": 0.028},
    {"city": "Córdoba",     "latitude": 37.8882, "longitude": -4.7794, "weight": 0.025},
    {"city": "Granada",     "latitude": 37.1773, "longitude": -3.5986, "weight": 0.025},
    {"city": "Oviedo",      "latitude": 43.3619, "longitude": -5.8494, "weight": 0.025},
    {"city": "Pamplona",    "latitude": 42.8125, "longitude": -1.6458, "weight": 0.018},
    {"city": "Badajoz",     "latitude": 38.8794, "longitude": -6.9707, "weight": 0.017},
    {"city": "Santander",   "latitude": 43.4623, "longitude": -3.8099, "weight": 0.015},
    {"city": "Logroño",     "latitude": 42.4627, "longitude": -2.4449, "weight": 0.009},
]


# =========================================================
# DISPLAY HELPERS
# =========================================================
def section_header(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(
                90deg,
                {CORP_GREEN_DARK} 0%,
                {CORP_GREEN} 55%,
                #C7F0DD 100%
            );
            color: white;
            padding: 12px 18px;
            border-radius: 12px;
            font-weight: 800;
            font-size: 1.20rem;
            margin-top: 12px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">
            {title}
        </div>
        """,
        unsafe_allow_html=True,
    )


def configure_chart(chart: alt.Chart, height: int = 360):
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
            labelFontSize=11,
            titleFontSize=12,
        )
    )


def require_esios_token() -> str:
    token = (
        os.getenv("ESIOS_TOKEN")
        or os.getenv("ESIOS_API_TOKEN")
        or ""
    ).strip()

    if not token:
        st.error(
            "No se ha encontrado ESIOS_TOKEN ni ESIOS_API_TOKEN en el archivo .env."
        )
        st.stop()

    return token


def esios_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


# =========================================================
# DAILY DEMAND — REData
# =========================================================
def parse_redata_included(payload: dict) -> pd.DataFrame:
    rows = []

    for item in payload.get("included", []) or []:
        attributes = item.get("attributes", {}) or {}
        title = str(attributes.get("title") or item.get("id") or "").strip()

        for value in attributes.get("values", []) or []:
            dt = pd.to_datetime(
                value.get("datetime"),
                utc=True,
                errors="coerce",
            )
            numeric_value = pd.to_numeric(
                value.get("value"),
                errors="coerce",
            )

            if pd.isna(dt) or pd.isna(numeric_value):
                continue

            rows.append(
                {
                    "datetime": (
                        dt.tz_convert("Europe/Madrid")
                        .tz_localize(None)
                        .normalize()
                    ),
                    "title": title,
                    "value": float(numeric_value),
                }
            )

    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=3600)
def load_daily_peninsular_demand(
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "day",
        "geo_trunc": "electric_system",
        "geo_limit": "peninsular",
        "geo_ids": "8741",
    }

    response = requests.get(
        REDATA_DEMAND_URL,
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    raw = parse_redata_included(response.json())
    if raw.empty:
        return pd.DataFrame(columns=["date", "demand_gwh"])

    # Prefer the demand series where the widget contains several indicators.
    demand_like = raw[
        raw["title"].str.contains(
            "demanda|demand",
            case=False,
            regex=True,
            na=False,
        )
    ].copy()

    if not demand_like.empty:
        raw = demand_like

    # If several series remain, keep the one with the largest accumulated
    # positive energy, which normally corresponds to total demand.
    title_totals = (
        raw.groupby("title", as_index=False)["value"]
        .sum()
        .sort_values("value", ascending=False)
    )

    if not title_totals.empty:
        selected_title = title_totals.iloc[0]["title"]
        raw = raw[raw["title"] == selected_title].copy()

    daily = (
        raw.groupby("datetime", as_index=False)["value"]
        .sum()
        .rename(columns={"datetime": "date"})
        .sort_values("date")
    )

    median_value = daily["value"].median()

    # Typical peninsular daily demand is approximately hundreds of GWh.
    # Convert MWh to GWh when the returned magnitude is clearly in MWh.
    if pd.notna(median_value) and median_value > 5_000:
        daily["demand_gwh"] = daily["value"] / 1_000.0
    else:
        daily["demand_gwh"] = daily["value"]

    return daily[["date", "demand_gwh"]].reset_index(drop=True)


# =========================================================
# DAILY TEMPERATURE — OPEN-METEO
# =========================================================
@st.cache_data(show_spinner=False, ttl=86400)
def load_spain_daily_temperature(
    start_day: date,
    end_day: date,
    mode: str,
) -> pd.DataFrame:
    if mode == "Madrid":
        points = [
            {
                "city": "Madrid",
                "latitude": 40.4168,
                "longitude": -3.7038,
                "weight": 1.0,
            }
        ]
    else:
        points = SPAIN_TEMPERATURE_POINTS

    params = {
        "latitude": ",".join(str(point["latitude"]) for point in points),
        "longitude": ",".join(str(point["longitude"]) for point in points),
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "daily": "temperature_2m_mean",
        "timezone": "Europe/Madrid",
        "models": "era5_land",
    }

    response = requests.get(
        OPEN_METEO_ARCHIVE_URL,
        params=params,
        timeout=90,
    )
    response.raise_for_status()

    payload = response.json()
    if isinstance(payload, dict):
        payload = [payload]

    frames = []

    for idx, location_payload in enumerate(payload):
        if idx >= len(points):
            continue

        daily = location_payload.get("daily", {}) or {}
        times = daily.get("time", []) or []
        values = daily.get("temperature_2m_mean", []) or []

        if not times or not values:
            continue

        point = points[idx]

        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(times, errors="coerce"),
                "temperature_c": pd.to_numeric(
                    pd.Series(values),
                    errors="coerce",
                ),
            }
        )
        frame["city"] = point["city"]
        frame["weight"] = float(point["weight"])
        frame = frame.dropna(subset=["date", "temperature_c"])
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["date", "temperature_c"])

    long_df = pd.concat(frames, ignore_index=True)
    long_df["weighted_temperature"] = (
        long_df["temperature_c"] * long_df["weight"]
    )

    # Re-normalise the weights per day if one location is temporarily missing.
    national = (
        long_df.groupby("date", as_index=False)
        .agg(
            weighted_temperature=("weighted_temperature", "sum"),
            available_weight=("weight", "sum"),
        )
    )
    national["temperature_c"] = (
        national["weighted_temperature"]
        / national["available_weight"]
    )

    return national[["date", "temperature_c"]].sort_values("date")



# =========================================================
# DAY-AHEAD HOURLY DEMAND FORECAST
# =========================================================
def parse_redata_hourly(payload: dict) -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = str(attrs.get("title") or item.get("id") or "").strip()
        for value in attrs.get("values", []) or []:
            dt = pd.to_datetime(value.get("datetime"), utc=True, errors="coerce")
            val = pd.to_numeric(value.get("value"), errors="coerce")
            if pd.isna(dt) or pd.isna(val):
                continue
            rows.append({
                "datetime": dt.tz_convert("Europe/Madrid").tz_localize(None),
                "title": title,
                "value": float(val),
            })
    return pd.DataFrame(rows)


def _fetch_hourly_demand_chunk(start_day: date, end_day: date) -> pd.DataFrame:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "hour",
        "geo_trunc": "electric_system",
        "geo_limit": "peninsular",
        "geo_ids": "8741",
    }
    response = requests.get(REDATA_DEMAND_URL, params=params, timeout=90)
    response.raise_for_status()
    raw = parse_redata_hourly(response.json())
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "demand_mw"])

    demand_like = raw[raw["title"].str.contains("demanda|demand", case=False, regex=True, na=False)]
    if not demand_like.empty:
        raw = demand_like

    totals = raw.groupby("title", as_index=False)["value"].sum().sort_values("value", ascending=False)
    if not totals.empty:
        raw = raw[raw["title"] == totals.iloc[0]["title"]]

    return (
        raw.groupby("datetime", as_index=False)["value"]
        .mean()
        .rename(columns={"value": "demand_mw"})
        .sort_values("datetime")
    )


@st.cache_data(show_spinner=False, ttl=3600)
def load_hourly_peninsular_demand(start_day: date, end_day: date) -> pd.DataFrame:
    chunks = []
    current = start_day
    while current <= end_day:
        chunk_end = min(end_day, current + timedelta(days=6))
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    frames = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_fetch_hourly_demand_chunk, s, e): (s, e) for s, e in chunks}
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
        .groupby("datetime", as_index=False)["demand_mw"].mean()
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def _weighted_hourly_temperature(
    payload,
    points: list[dict],
    source_variable: str = "temperature_2m",
) -> pd.DataFrame:
    """Population-weight a live or archived D-1 temperature forecast."""
    if isinstance(payload, dict):
        payload = [payload]

    frames = []
    for idx, item in enumerate(payload):
        if idx >= len(points):
            continue

        hourly = item.get("hourly", {}) or {}
        times = hourly.get("time", []) or []
        values = hourly.get(source_variable, []) or []

        if not times or not values:
            continue

        frame = pd.DataFrame(
            {
                "datetime": pd.to_datetime(times, errors="coerce"),
                "temperature_c": pd.to_numeric(
                    pd.Series(values),
                    errors="coerce",
                ),
            }
        )
        frame["weight"] = float(points[idx]["weight"])
        frames.append(
            frame.dropna(subset=["datetime", "temperature_c"])
        )

    if not frames:
        return pd.DataFrame(
            columns=["datetime", "temperature_c"]
        )

    long = pd.concat(frames, ignore_index=True)
    long["weighted"] = (
        long["temperature_c"] * long["weight"]
    )
    out = (
        long.groupby("datetime", as_index=False)
        .agg(
            weighted=("weighted", "sum"),
            weight=("weight", "sum"),
        )
    )
    out["temperature_c"] = (
        out["weighted"] / out["weight"]
    )

    return out[
        ["datetime", "temperature_c"]
    ].sort_values("datetime")


def _temperature_points(mode: str) -> list[dict]:
    if mode == "Madrid":
        return [{"city": "Madrid", "latitude": 40.4168, "longitude": -3.7038, "weight": 1.0}]
    return SPAIN_TEMPERATURE_POINTS


@st.cache_data(show_spinner=False, ttl=86400)
def load_hourly_temperature_history(start_day: date, end_day: date, mode: str) -> pd.DataFrame:
    points = _temperature_points(mode)
    safe_end = min(end_day, date.today() - timedelta(days=5))
    if start_day > safe_end:
        return pd.DataFrame(columns=["datetime", "temperature_c"])
    params = {
        "latitude": ",".join(str(p["latitude"]) for p in points),
        "longitude": ",".join(str(p["longitude"]) for p in points),
        "start_date": start_day.isoformat(),
        "end_date": safe_end.isoformat(),
        "hourly": "temperature_2m",
        "timezone": "Europe/Madrid",
        "models": "era5_land",
    }
    response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=180)
    response.raise_for_status()
    return _weighted_hourly_temperature(response.json(), points)


@st.cache_data(show_spinner=False, ttl=1800)
def load_hourly_temperature_forecast(
    target_day: date,
    mode: str,
) -> pd.DataFrame:
    """
    Weather input available at the day-ahead decision point.

    Historical/today targets use Open-Meteo Previous Runs
    temperature_2m_previous_day1. Tomorrow uses the latest live forecast.
    """
    points = _temperature_points(mode)

    common = {
        "latitude": ",".join(
            str(point["latitude"])
            for point in points
        ),
        "longitude": ",".join(
            str(point["longitude"])
            for point in points
        ),
        "timezone": "Europe/Madrid",
    }

    if target_day <= date.today():
        source_variable = "temperature_2m_previous_day1"
        params = {
            **common,
            "start_date": target_day.isoformat(),
            "end_date": target_day.isoformat(),
            "hourly": source_variable,
        }
        endpoint = OPEN_METEO_PREVIOUS_RUNS_URL
    else:
        source_variable = "temperature_2m"
        params = {
            **common,
            "hourly": source_variable,
            "forecast_days": min(
                max(
                    (target_day - date.today()).days + 1,
                    1,
                ),
                16,
            ),
        }
        endpoint = OPEN_METEO_FORECAST_URL

    response = requests.get(
        endpoint,
        params=params,
        timeout=120,
    )
    response.raise_for_status()

    out = _weighted_hourly_temperature(
        response.json(),
        points,
        source_variable=source_variable,
    )
    out = out[
        out["datetime"].dt.date == target_day
    ].copy()

    expected = pd.DataFrame(
        {
            "datetime": pd.date_range(
                start=pd.Timestamp(target_day),
                periods=24,
                freq="h",
            )
        }
    )
    out = (
        expected.merge(out, on="datetime", how="left")
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    out["temperature_c"] = (
        pd.to_numeric(
            out["temperature_c"],
            errors="coerce",
        )
        .interpolate(limit_direction="both")
    )

    return out


def national_holidays(years: list[int]) -> set[date]:
    result = set()
    for year in years:
        for month, day in [(1, 1), (1, 6), (5, 1), (8, 15), (10, 12), (11, 1), (12, 6), (12, 8), (12, 25)]:
            result.add(date(year, month, day))
        result.add(easter(year) - timedelta(days=2))
    return result


def build_forecast_features(frame: pd.DataFrame, demand_lookup: dict, trend_alpha: float = 1.0) -> pd.DataFrame:
    out = frame.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out["date"] = out["datetime"].dt.date
    out["hour"] = out["datetime"].dt.hour
    out["date_ts"] = pd.to_datetime(out["date"].astype(str))
    out["dow"] = out["date_ts"].dt.dayofweek
    out["doy"] = out["date_ts"].dt.dayofyear
    out["month"] = out["date_ts"].dt.month
    out["is_weekend"] = (out["dow"] >= 5).astype(int)

    holidays = national_holidays(sorted(out["date_ts"].dt.year.unique().tolist()))
    out["is_holiday"] = out["date"].isin(holidays).astype(int)
    out["is_pre_holiday"] = out["date"].map(lambda d: int(d + timedelta(days=1) in holidays))
    out["is_post_holiday"] = out["date"].map(lambda d: int(d - timedelta(days=1) in holidays))

    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["dow"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["dow"] / 7)
    out["doy_sin"] = np.sin(2 * np.pi * out["doy"] / 365.25)
    out["doy_cos"] = np.cos(2 * np.pi * out["doy"] / 365.25)

    out["heating_degree"] = (16 - out["temperature_c"]).clip(lower=0)
    out["cooling_degree"] = (out["temperature_c"] - 22).clip(lower=0)
    out["heating_degree_sq"] = out["heating_degree"] ** 2
    out["cooling_degree_sq"] = out["cooling_degree"] ** 2

    daily_temp = out.groupby("date", as_index=False)["temperature_c"].agg(["mean", "min", "max"]).reset_index()
    daily_temp = daily_temp.rename(columns={"mean": "daily_temp_mean", "min": "daily_temp_min", "max": "daily_temp_max"})
    out = out.merge(daily_temp, on="date", how="left")

    # Historical demand lags available at a D-1 publication cut-off.
    # D-2 and D-9 are the same weekday one week apart; likewise D-3 and D-10.
    # Their differences provide an explicit measure of the most recent weekly
    # level change, which is useful during heatwaves, cold spells or abrupt
    # changes in economic/activity conditions.
    for lag in [1, 2, 3, 7, 9, 10, 14, 21, 28]:
        out[f"lag_{lag}d"] = [
            demand_lookup.get(
                (d - timedelta(days=lag), int(h)),
                np.nan,
            )
            for d, h in zip(out["date"], out["hour"])
        ]

    out["weekly_change_d2_vs_d9"] = out["lag_2d"] - out["lag_9d"]
    out["weekly_change_d3_vs_d10"] = out["lag_3d"] - out["lag_10d"]

    # Give slightly more relevance to D-2, while reducing the risk that a
    # single anomalous day completely shifts the target curve.
    out["recent_weekly_trend_raw_mw"] = (
        0.65 * out["weekly_change_d2_vs_d9"]
        + 0.35 * out["weekly_change_d3_vs_d10"]
    )

    # Cap extreme one-week movements before applying them to D-7. The raw
    # differences remain available to the model as separate features.
    out["recent_weekly_trend_mw"] = out[
        "recent_weekly_trend_raw_mw"
    ].clip(lower=-3500.0, upper=3500.0)

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


FORECAST_FEATURES = [
    "hour", "month", "dow", "is_weekend", "is_holiday", "is_pre_holiday", "is_post_holiday",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
    "temperature_c", "daily_temp_mean", "daily_temp_min", "daily_temp_max",
    "heating_degree", "cooling_degree", "heating_degree_sq", "cooling_degree_sq",
    "lag_2d", "lag_3d", "lag_7d", "lag_9d", "lag_10d",
    "lag_14d", "lag_21d", "lag_28d",
    "weekly_change_d2_vs_d9", "weekly_change_d3_vs_d10",
    "recent_weekly_trend_raw_mw", "recent_weekly_trend_mw",
    "trend_adjusted_lag_7d",
    "same_hour_4w_mean", "recent_level_mean",
]


def forecast_metrics(actual, predicted) -> dict:
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    valid = np.isfinite(actual) & np.isfinite(predicted)
    actual, predicted = actual[valid], predicted[valid]
    if len(actual) == 0:
        return {"mae": np.nan, "rmse": np.nan, "mape": np.nan}
    error = actual - predicted
    return {
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(np.sqrt(np.mean(error ** 2))),
        "mape": float(np.mean(np.abs(error / actual)) * 100),
    }


def similar_day_prediction(history: pd.DataFrame, target: pd.DataFrame) -> np.ndarray:
    predictions = []
    for row in target.itertuples(index=False):
        candidates = history[(history["dow"] == row.dow) & (history["date"] < row.date)].copy()
        candidates = candidates[candidates["hour"] == row.hour]
        if candidates.empty:
            predictions.append(float(row.same_hour_4w_mean))
            continue
        candidates["temp_distance"] = (candidates["daily_temp_mean"] - row.daily_temp_mean).abs()
        candidates["days_ago"] = candidates["date"].map(lambda d: (row.date - d).days)
        candidates = candidates.nsmallest(10, ["temp_distance", "days_ago"])
        weights = np.exp(-candidates["temp_distance"] / 3) * np.exp(-candidates["days_ago"] / 240)
        predictions.append(float(np.average(candidates["demand_mw"], weights=weights)))
    return np.asarray(predictions)


@st.cache_data(show_spinner=False, ttl=1800)
def generate_day_ahead_forecast(target_day: date, lookback_days: int, temperature_mode: str, trend_alpha: float = 1.0) -> dict:
    history_end = target_day - timedelta(days=2)
    history_start = history_end - timedelta(days=lookback_days)

    demand = load_hourly_peninsular_demand(history_start, history_end)
    weather = load_hourly_temperature_history(history_start, history_end, temperature_mode)
    target_weather = load_hourly_temperature_forecast(target_day, temperature_mode)
    if demand.empty or weather.empty or target_weather.empty:
        raise ValueError("Hourly demand or weather data is unavailable.")

    demand["date"] = demand["datetime"].dt.date
    demand["hour"] = demand["datetime"].dt.hour
    demand_grid = demand.groupby(["date", "hour"], as_index=False)["demand_mw"].mean()
    demand_lookup = {(r.date, int(r.hour)): float(r.demand_mw) for r in demand_grid.itertuples(index=False)}

    weather["date"] = weather["datetime"].dt.date
    weather["hour"] = weather["datetime"].dt.hour
    weather_grid = weather.groupby(["date", "hour"], as_index=False)["temperature_c"].mean()

    history = demand_grid.merge(weather_grid, on=["date", "hour"], how="inner")
    history["datetime"] = pd.to_datetime(history["date"].astype(str)) + pd.to_timedelta(history["hour"], unit="h")
    history = build_forecast_features(history, demand_lookup, trend_alpha=trend_alpha)

    target = target_weather.copy()
    target["date"] = target["datetime"].dt.date
    target["hour"] = target["datetime"].dt.hour
    target = target.groupby(["date", "hour"], as_index=False)["temperature_c"].mean()
    target["datetime"] = pd.to_datetime(target["date"].astype(str)) + pd.to_timedelta(target["hour"], unit="h")
    target = build_forecast_features(target, demand_lookup, trend_alpha=trend_alpha)

    model_data = history.dropna(subset=["demand_mw"] + FORECAST_FEATURES).copy()
    target_data = target.dropna(subset=FORECAST_FEATURES).copy()
    if len(model_data) < 24 * 180 or len(target_data) < 23:
        raise ValueError("Insufficient complete observations after creating lag features.")

    validation_start = model_data["date_ts"].max() - pd.Timedelta(days=FORECAST_VALIDATION_DAYS - 1)
    train = model_data[model_data["date_ts"] < validation_start]
    validation = model_data[model_data["date_ts"] >= validation_start]

    if SKLEARN_AVAILABLE:
        validation_model = HistGradientBoostingRegressor(
            loss="absolute_error", learning_rate=0.055, max_iter=300,
            max_leaf_nodes=31, min_samples_leaf=30, l2_regularization=8, random_state=42,
        )
        validation_model.fit(train[FORECAST_FEATURES], train["demand_mw"])
        validation_prediction = validation_model.predict(validation[FORECAST_FEATURES])

        final_model = HistGradientBoostingRegressor(
            loss="absolute_error", learning_rate=0.055, max_iter=300,
            max_leaf_nodes=31, min_samples_leaf=30, l2_regularization=8, random_state=42,
        )
        final_model.fit(model_data[FORECAST_FEATURES], model_data["demand_mw"])
        target_prediction = final_model.predict(target_data[FORECAST_FEATURES])
        model_name = "Histogram gradient boosting"
    else:
        validation_prediction = similar_day_prediction(train, validation)
        target_prediction = similar_day_prediction(model_data, target_data)
        model_name = "Weighted similar-day fallback"

    model_stats = forecast_metrics(
        validation["demand_mw"],
        validation_prediction,
    )
    baseline_stats = forecast_metrics(
        validation["demand_mw"],
        validation["lag_7d"],
    )
    trend_baseline_stats = forecast_metrics(
        validation["demand_mw"],
        validation["trend_adjusted_lag_7d"],
    )

    residuals = validation[["hour"]].copy()
    residuals["residual"] = validation["demand_mw"].to_numpy() - validation_prediction
    quantiles = residuals.groupby("hour")["residual"].quantile([0.10, 0.90]).unstack()
    quantiles.columns = ["residual_p10", "residual_p90"]

    forecast = target_data[
        [
            "datetime",
            "date",
            "hour",
            "temperature_c",
            "lag_2d",
            "lag_7d",
            "lag_9d",
            "weekly_change_d2_vs_d9",
            "weekly_change_d3_vs_d10",
            "recent_weekly_trend_mw",
            "trend_adjusted_lag_7d",
        ]
    ].copy()
    forecast["forecast_mw"] = np.maximum(target_prediction, 0)
    forecast = forecast.merge(quantiles, left_on="hour", right_index=True, how="left")
    forecast["residual_p10"] = forecast["residual_p10"].fillna(residuals["residual"].quantile(0.10))
    forecast["residual_p90"] = forecast["residual_p90"].fillna(residuals["residual"].quantile(0.90))
    forecast["p10_mw"] = (forecast["forecast_mw"] + forecast["residual_p10"]).clip(lower=0)
    forecast["p90_mw"] = (forecast["forecast_mw"] + forecast["residual_p90"]).clip(lower=0)

    backtest = validation[
        [
            "datetime",
            "date",
            "hour",
            "demand_mw",
            "lag_7d",
            "trend_adjusted_lag_7d",
            "recent_weekly_trend_mw",
        ]
    ].copy()
    backtest["model_forecast_mw"] = validation_prediction

    return {
        "forecast": forecast.sort_values("datetime").reset_index(drop=True),
        "backtest": backtest.sort_values("datetime").reset_index(drop=True),
        "model_stats": model_stats,
        "baseline_stats": baseline_stats,
        "trend_baseline_stats": trend_baseline_stats,
        "trend_alpha": float(trend_alpha),
        "model_name": model_name,
        "history_start": history_start,
        "history_end": history_end,
        "training_rows": len(model_data),
    }


def build_day_ahead_chart(forecast: pd.DataFrame):
    band = alt.Chart(forecast).mark_area(opacity=0.16, color=CORP_GREEN).encode(
        x=alt.X("datetime:T", title=None, axis=alt.Axis(format="%H:%M", labelAngle=0)),
        y=alt.Y("p10_mw:Q", title="Demand (MW)", scale=alt.Scale(zero=False)),
        y2="p90_mw:Q",
    )

    lines = pd.concat([
        forecast[["datetime", "forecast_mw"]]
        .rename(columns={"forecast_mw": "value"})
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
    ], ignore_index=True)

    chart = alt.Chart(lines).mark_line(strokeWidth=3).encode(
        x=alt.X("datetime:T", title=None, axis=alt.Axis(format="%H:%M", labelAngle=0)),
        y=alt.Y("value:Q", title="Demand (MW)", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "series:N",
            title="Forecast series",
            scale=alt.Scale(
                domain=[
                    "Model forecast",
                    "Previous week + recent trend",
                    "Same weekday previous week",
                    "D-2 actual reference",
                ],
                range=[
                    "#22C55E",
                    "#F97316",
                    "#64748B",
                    "#93C5FD",
                ],
            ),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                columns=4,
                labelLimit=360,
                titleLimit=240,
                symbolLimit=360,
            ),
        ),
        strokeDash=alt.StrokeDash(
            "series:N", legend=None,
            scale=alt.Scale(
                domain=[
                    "Model forecast",
                    "Previous week + recent trend",
                    "Same weekday previous week",
                    "D-2 actual reference",
                ],
                range=[
                    [1, 0],
                    [8, 3],
                    [5, 3],
                    [2, 2],
                ],
            ),
        ),
        tooltip=[
            alt.Tooltip("datetime:T", title="Hour", format="%d-%m-%Y %H:%M"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="Demand", format=",.0f"),
        ],
    )
    return configure_chart(alt.layer(band, chart), height=430)


def build_backtest_chart(backtest: pd.DataFrame):
    bt = backtest.copy()
    safe_actual = bt["demand_mw"].replace(0, np.nan)

    bt["model_ape"] = (
        bt["demand_mw"] - bt["model_forecast_mw"]
    ).abs() / safe_actual
    bt["baseline_ape"] = (
        bt["demand_mw"] - bt["lag_7d"]
    ).abs() / safe_actual
    bt["trend_baseline_ape"] = (
        bt["demand_mw"] - bt["trend_adjusted_lag_7d"]
    ).abs() / safe_actual

    daily = bt.groupby("date", as_index=False).agg(
        model=("model_ape", "mean"),
        trend_baseline=("trend_baseline_ape", "mean"),
        baseline=("baseline_ape", "mean"),
    )

    long = daily.melt(
        id_vars="date",
        var_name="series",
        value_name="mape",
    )
    long["series"] = long["series"].map(
        {
            "model": "Model",
            "trend_baseline": "Previous week + recent trend",
            "baseline": "Previous-week baseline",
        }
    )

    chart = alt.Chart(long).mark_line(
        point=True,
        strokeWidth=2.3,
    ).encode(
        x=alt.X(
            "date:T",
            title=None,
            axis=alt.Axis(format="%d-%b"),
        ),
        y=alt.Y(
            "mape:Q",
            title="Daily MAPE",
            axis=alt.Axis(format=".1%"),
        ),
        color=alt.Color(
            "series:N",
            title="Backtest",
            scale=alt.Scale(
                domain=[
                    "Model",
                    "Previous week + recent trend",
                    "Previous-week baseline",
                ],
                range=[
                    CORP_GREEN,
                    "#F97316",
                    "#64748B",
                ],
            ),
        ),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("mape:Q", title="MAPE", format=".2%"),
        ],
    )
    return configure_chart(chart, height=290)


# =========================================================
# PBF DAILY MIX — ESIOS
# =========================================================
def parse_esios_values(payload: dict) -> pd.DataFrame:
    values = payload.get("indicator", {}).get("values", []) or []
    if not values:
        return pd.DataFrame(
            columns=["datetime", "value", "geo_id", "geo_name"]
        )

    frame = pd.DataFrame(values)

    for column in ["geo_id", "geo_name"]:
        if column not in frame.columns:
            frame[column] = pd.NA

    if "datetime_utc" in frame.columns:
        dt = pd.to_datetime(
            frame["datetime_utc"],
            utc=True,
            errors="coerce",
        )
    elif "datetime" in frame.columns:
        dt = pd.to_datetime(
            frame["datetime"],
            utc=True,
            errors="coerce",
        )
    else:
        return pd.DataFrame(
            columns=["datetime", "value", "geo_id", "geo_name"]
        )

    frame["datetime"] = (
        dt.dt.tz_convert("Europe/Madrid")
        .dt.tz_localize(None)
        .dt.floor("h")
    )
    frame["value"] = pd.to_numeric(
        frame.get("value"),
        errors="coerce",
    )

    frame = frame.dropna(subset=["datetime", "value"]).copy()
    if frame.empty:
        return frame

    # Prefer the Spanish / peninsular aggregate when multiple geographies appear.
    geo_id_numeric = pd.to_numeric(frame["geo_id"], errors="coerce")

    if geo_id_numeric.eq(3).any():
        frame = frame[geo_id_numeric.eq(3)].copy()
    else:
        geo_text = (
            frame["geo_name"]
            .astype(str)
            .str.lower()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )

        preferred = frame[
            geo_text.str.contains(
                "espana|peninsula|peninsular",
                regex=True,
                na=False,
            )
        ].copy()

        if not preferred.empty:
            frame = preferred

    return frame[["datetime", "value", "geo_id", "geo_name"]]


def fetch_one_pbf_indicator_hourly(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
) -> pd.DataFrame:
    """
    Fetch one PBF indicator at hourly resolution.

    time_agg=sum ensures that, after the quarter-hour market transition, the
    four quarter-hour programmed-energy values are summed into the hourly MWh.
    """
    frames = []
    chunk_start = start_day

    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=13))

        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(
            chunk_end + timedelta(days=1),
            tz="Europe/Madrid",
        )

        params = {
            "start_date": start_local.tz_convert("UTC").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "end_date": end_local.tz_convert("UTC").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "time_trunc": "hour",
            "time_agg": "sum",
        }

        for attempt in range(3):
            try:
                response = requests.get(
                    f"{ESIOS_API_BASE}/{indicator_id}",
                    headers=esios_headers(token),
                    params=params,
                    timeout=(15, 120),
                )
                response.raise_for_status()

                parsed = parse_esios_values(response.json())
                if not parsed.empty:
                    frames.append(parsed)
                break

            except requests.RequestException:
                sleep(1.5 * (attempt + 1))

        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["datetime", "energy_mwh"])

    out = pd.concat(frames, ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["datetime", "value"])

    return (
        out.groupby("datetime", as_index=False)["value"]
        .sum()
        .rename(columns={"value": "energy_mwh"})
        .sort_values("datetime")
        .drop_duplicates(subset=["datetime"], keep="last")
        .reset_index(drop=True)
    )


def load_one_pbf_technology_hourly(
    technology: str,
    indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> pd.DataFrame:
    frames = []

    for indicator_id in indicator_ids:
        indicator_df = fetch_one_pbf_indicator_hourly(
            indicator_id=indicator_id,
            start_day=start_day,
            end_day=end_day,
            token=token,
        )
        if not indicator_df.empty:
            frames.append(indicator_df)

    if not frames:
        return pd.DataFrame(
            columns=["datetime", "technology", "energy_mwh"]
        )

    combined = pd.concat(frames, ignore_index=True)
    combined = (
        combined.groupby("datetime", as_index=False)["energy_mwh"]
        .sum()
    )
    combined["technology"] = technology

    return combined[["datetime", "technology", "energy_mwh"]]


@st.cache_data(show_spinner=False, ttl=3600)
def load_pbf_hourly_mix(
    start_day: date,
    end_day: date,
    _token: str,
) -> pd.DataFrame:
    results = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(
                load_one_pbf_technology_hourly,
                technology,
                indicator_ids,
                start_day,
                end_day,
                _token,
            ): technology
            for technology, indicator_ids in PBF_TECH_INDICATORS.items()
        }

        for future in as_completed(futures):
            try:
                frame = future.result()
            except Exception:
                frame = pd.DataFrame(
                    columns=["datetime", "technology", "energy_mwh"]
                )

            if not frame.empty:
                results.append(frame)

    if not results:
        return pd.DataFrame(
            columns=["datetime", "technology", "energy_mwh"]
        )

    out = pd.concat(results, ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["energy_mwh"] = pd.to_numeric(out["energy_mwh"], errors="coerce")
    out = out.dropna(
        subset=["datetime", "technology", "energy_mwh"]
    )

    return (
        out.groupby(
            ["datetime", "technology"],
            as_index=False,
        )["energy_mwh"]
        .sum()
        .sort_values(["datetime", "technology"])
        .reset_index(drop=True)
    )


# =========================================================
# CHARTS
# =========================================================
def build_demand_temperature_chart(
    combined: pd.DataFrame,
    temperature_label: str,
    selected_series: list[str],
    rolling_days: int,
):
    if combined.empty or not selected_series:
        return None

    plot = combined.copy().sort_values("date")
    plot["avg_demand_gw"] = plot["demand_gwh"] / 24.0
    plot["avg_demand_rolling_gw"] = (
        plot["avg_demand_gw"]
        .rolling(rolling_days, min_periods=1)
        .mean()
    )
    plot["temperature_rolling_c"] = (
        plot["temperature_c"]
        .rolling(rolling_days, min_periods=1)
        .mean()
    )

    demand_frames = []
    temperature_frames = []
    legend_order = []
    legend_colors = []
    legend_dashes = []

    if "Demand daily" in selected_series:
        demand_frames.append(
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["avg_demand_gw"],
                    "series": "Demand daily",
                }
            )
        )
        legend_order.append("Demand daily")
        legend_colors.append("#A5B4FC")
        legend_dashes.append([4, 2])

    if "Demand rolling average" in selected_series:
        demand_rolling_label = f"Demand {rolling_days}d avg"
        demand_frames.append(
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["avg_demand_rolling_gw"],
                    "series": demand_rolling_label,
                }
            )
        )
        legend_order.append(demand_rolling_label)
        legend_colors.append(DEMAND_BLUE)
        legend_dashes.append([1, 0])

    if "Temperature daily" in selected_series:
        temperature_frames.append(
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["temperature_c"],
                    "series": "Temperature daily",
                }
            )
        )
        legend_order.append("Temperature daily")
        legend_colors.append("#FDBA74")
        legend_dashes.append([4, 2])

    if "Temperature rolling average" in selected_series:
        temperature_rolling_label = f"Temperature {rolling_days}d avg"
        temperature_frames.append(
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["temperature_rolling_c"],
                    "series": temperature_rolling_label,
                }
            )
        )
        legend_order.append(temperature_rolling_label)
        legend_colors.append(TEMPERATURE_ORANGE)
        legend_dashes.append([1, 0])

    color_scale = alt.Scale(
        domain=legend_order,
        range=legend_colors,
    )
    dash_scale = alt.Scale(
        domain=legend_order,
        range=legend_dashes,
    )

    layers = []

    if demand_frames:
        demand_long = pd.concat(demand_frames, ignore_index=True)
        demand_chart = (
            alt.Chart(demand_long)
            .mark_line(strokeWidth=2.6)
            .encode(
                x=alt.X(
                    "date:T",
                    title=None,
                    axis=alt.Axis(format="%d-%b", labelAngle=0),
                ),
                y=alt.Y(
                    "value:Q",
                    title="Average daily demand (GW)",
                    axis=alt.Axis(
                        orient="left",
                        titlePadding=12,
                        labelPadding=8,
                    ),
                    scale=alt.Scale(zero=False),
                ),
                color=alt.Color(
                    "series:N",
                    title="Series",
                    scale=color_scale,
                    sort=legend_order,
                ),
                strokeDash=alt.StrokeDash(
                    "series:N",
                    scale=dash_scale,
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip(
                        "value:Q",
                        title="Average daily demand (GW)",
                        format=",.2f",
                    ),
                ],
            )
        )
        layers.append(demand_chart)

    if temperature_frames:
        temperature_long = pd.concat(temperature_frames, ignore_index=True)
        temperature_chart = (
            alt.Chart(temperature_long)
            .mark_line(strokeWidth=2.6)
            .encode(
                x=alt.X(
                    "date:T",
                    title=None,
                    axis=alt.Axis(format="%d-%b", labelAngle=0),
                ),
                y=alt.Y(
                    "value:Q",
                    title=f"{temperature_label} (°C)",
                    axis=alt.Axis(
                        orient="right",
                        titlePadding=12,
                        labelPadding=8,
                    ),
                    scale=alt.Scale(zero=False),
                ),
                color=alt.Color(
                    "series:N",
                    title="Series",
                    scale=color_scale,
                    sort=legend_order,
                ),
                strokeDash=alt.StrokeDash(
                    "series:N",
                    scale=dash_scale,
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip(
                        "value:Q",
                        title="Temperature (°C)",
                        format=",.1f",
                    ),
                ],
            )
        )
        layers.append(temperature_chart)

    if not layers:
        return None

    if len(layers) == 1:
        chart = layers[0]
    else:
        chart = alt.layer(*layers).resolve_scale(y="independent")

    return configure_chart(chart, height=420)


def build_pbf_hourly_area_chart(pbf_hourly: pd.DataFrame):
    if pbf_hourly.empty:
        return None

    plot = pbf_hourly.copy()
    # Hourly MWh divided by one hour is average MW; /1,000 displays GW.
    plot["average_power_gw"] = plot["energy_mwh"] / 1_000.0

    order = [
        technology
        for technology in PBF_TECH_ORDER
        if technology in plot["technology"].unique()
    ]

    chart = (
        alt.Chart(plot)
        .mark_area()
        .encode(
            x=alt.X(
                "datetime:T",
                title=None,
                axis=alt.Axis(
                    format="%d-%b %H:%M",
                    labelAngle=-35,
                    labelOverlap="greedy",
                ),
            ),
            y=alt.Y(
                "sum(average_power_gw):Q",
                title="Hourly PBF programmed generation (GW)",
                stack="zero",
            ),
            color=alt.Color(
                "technology:N",
                title="Technology",
                sort=order,
                scale=alt.Scale(
                    domain=PBF_COLOR_DOMAIN,
                    range=PBF_COLOR_RANGE,
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                    columns=5,
                    labelLimit=220,
                    symbolLimit=220,
                ),
            ),
            order=alt.Order(
                "technology:N",
                sort="ascending",
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%Y-%m-%d %H:%M",
                ),
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip(
                    "average_power_gw:Q",
                    title="Programmed power (GW)",
                    format=",.2f",
                ),
                alt.Tooltip(
                    "energy_mwh:Q",
                    title="Programmed energy (MWh)",
                    format=",.0f",
                ),
            ],
        )
    )

    return configure_chart(chart, height=420)


def build_pbf_average_chart(summary: pd.DataFrame):
    if summary.empty:
        return None

    order = summary["technology"].tolist()

    bars = (
        alt.Chart(summary)
        .mark_bar(cornerRadiusEnd=3)
        .encode(
            y=alt.Y(
                "technology:N",
                sort=order,
                title=None,
            ),
            x=alt.X(
                "average_programmed_gw:Q",
                title="Average programmed power (GW)",
            ),
            color=alt.Color(
                "technology:N",
                legend=None,
                scale=alt.Scale(
                    domain=PBF_COLOR_DOMAIN,
                    range=PBF_COLOR_RANGE,
                ),
            ),
            tooltip=[
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip(
                    "average_programmed_gw:Q",
                    title="Average programmed power (GW)",
                    format=",.2f",
                ),
                alt.Tooltip(
                    "period_total_gwh:Q",
                    title="Period total (GWh)",
                    format=",.1f",
                ),
                alt.Tooltip(
                    "share_pct:Q",
                    title="Energy share",
                    format=".1%",
                ),
            ],
        )
    )

    labels = (
        alt.Chart(summary)
        .mark_text(
            align="left",
            baseline="middle",
            dx=5,
            fontWeight="bold",
        )
        .encode(
            y=alt.Y("technology:N", sort=order),
            x=alt.X("average_programmed_gw:Q"),
            text=alt.Text(
                "average_programmed_gw:Q",
                format=",.1f",
            ),
        )
    )

    return configure_chart(
        alt.layer(bars, labels),
        height=max(340, 31 * len(summary)),
    )



# =========================================================
# STEP 2 — FORECAST PBF GENERATION AND THERMAL GAP
# =========================================================
def _weighted_generation_weather(
    payload,
    variable_suffix: str = "",
) -> pd.DataFrame:
    """
    Geographically average live variables or archived variables ending in
    _previous_day1. Returned columns keep the canonical variable names.
    """
    if isinstance(payload, dict):
        payload = [payload]

    frames = []
    for idx, item in enumerate(payload):
        if idx >= len(GENERATION_WEATHER_POINTS):
            continue

        hourly = item.get("hourly", {}) or {}
        times = hourly.get("time", []) or []
        if not times:
            continue

        frame = pd.DataFrame(
            {
                "datetime": pd.to_datetime(
                    times,
                    errors="coerce",
                )
            }
        )

        for variable in GENERATION_WEATHER_VARIABLES:
            source_variable = (
                f"{variable}{variable_suffix}"
            )
            values = hourly.get(source_variable, []) or []
            frame[variable] = (
                pd.to_numeric(
                    pd.Series(values),
                    errors="coerce",
                )
                if len(values) == len(times)
                else np.nan
            )

        frame["weight"] = float(
            GENERATION_WEATHER_POINTS[idx]["weight"]
        )
        frames.append(frame)

    if not frames:
        return pd.DataFrame(
            columns=[
                "datetime",
                *GENERATION_WEATHER_VARIABLES,
            ]
        )

    long = pd.concat(frames, ignore_index=True)
    result = None

    for variable in GENERATION_WEATHER_VARIABLES:
        temp = long[
            ["datetime", "weight", variable]
        ].dropna(
            subset=["datetime", variable]
        ).copy()

        if temp.empty:
            continue

        temp["weighted"] = (
            temp[variable] * temp["weight"]
        )
        temp = (
            temp.groupby("datetime", as_index=False)
            .agg(
                weighted=("weighted", "sum"),
                available_weight=("weight", "sum"),
            )
        )
        temp[variable] = (
            temp["weighted"]
            / temp["available_weight"]
        )
        temp = temp[["datetime", variable]]

        result = (
            temp
            if result is None
            else result.merge(
                temp,
                on="datetime",
                how="outer",
            )
        )

    if result is None:
        return pd.DataFrame(
            columns=[
                "datetime",
                *GENERATION_WEATHER_VARIABLES,
            ]
        )

    return (
        result.sort_values("datetime")
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False, ttl=86400)
def load_generation_weather_history(
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    safe_end = min(end_day, date.today() - timedelta(days=5))
    if start_day > safe_end:
        return pd.DataFrame(
            columns=["datetime"] + GENERATION_WEATHER_VARIABLES
        )

    params = {
        "latitude": ",".join(
            str(point["latitude"])
            for point in GENERATION_WEATHER_POINTS
        ),
        "longitude": ",".join(
            str(point["longitude"])
            for point in GENERATION_WEATHER_POINTS
        ),
        "start_date": start_day.isoformat(),
        "end_date": safe_end.isoformat(),
        "hourly": ",".join(GENERATION_WEATHER_VARIABLES),
        "timezone": "Europe/Madrid",
        "models": "era5",
    }
    response = requests.get(
        OPEN_METEO_ARCHIVE_URL,
        params=params,
        timeout=180,
    )
    response.raise_for_status()
    return _weighted_generation_weather(response.json())


@st.cache_data(show_spinner=False, ttl=1800)
def load_generation_weather_forecast(
    target_day: date,
) -> pd.DataFrame:
    """
    Retrieve the weather information available at D-1.

    Historical/today targets use Open-Meteo Previous Runs variables ending
    in _previous_day1. Tomorrow uses the current live Best Match forecast.
    """
    common = {
        "latitude": ",".join(
            str(point["latitude"])
            for point in GENERATION_WEATHER_POINTS
        ),
        "longitude": ",".join(
            str(point["longitude"])
            for point in GENERATION_WEATHER_POINTS
        ),
        "timezone": "Europe/Madrid",
    }

    if target_day <= date.today():
        variable_suffix = "_previous_day1"
        hourly_variables = [
            f"{variable}{variable_suffix}"
            for variable in GENERATION_WEATHER_VARIABLES
        ]
        params = {
            **common,
            "start_date": target_day.isoformat(),
            "end_date": target_day.isoformat(),
            "hourly": ",".join(hourly_variables),
        }
        endpoint = OPEN_METEO_PREVIOUS_RUNS_URL
    else:
        variable_suffix = ""
        params = {
            **common,
            "hourly": ",".join(
                GENERATION_WEATHER_VARIABLES
            ),
            "forecast_days": min(
                max(
                    (target_day - date.today()).days + 1,
                    1,
                ),
                16,
            ),
        }
        endpoint = OPEN_METEO_FORECAST_URL

    response = requests.get(
        endpoint,
        params=params,
        timeout=120,
    )
    response.raise_for_status()

    forecast = _weighted_generation_weather(
        response.json(),
        variable_suffix=variable_suffix,
    )
    forecast = forecast[
        forecast["datetime"].dt.date == target_day
    ].copy()

    expected = pd.DataFrame(
        {
            "datetime": pd.date_range(
                start=pd.Timestamp(target_day),
                periods=24,
                freq="h",
            )
        }
    )
    forecast = (
        expected.merge(
            forecast,
            on="datetime",
            how="left",
        )
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    for variable in GENERATION_WEATHER_VARIABLES:
        if variable not in forecast.columns:
            forecast[variable] = np.nan

        forecast[variable] = pd.to_numeric(
            forecast[variable],
            errors="coerce",
        ).interpolate(limit_direction="both")

    return forecast


def _market_calendar(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["datetime"] = pd.to_datetime(
        out["datetime"],
        errors="coerce",
    )
    out["date"] = out["datetime"].dt.date
    out["hour"] = out["datetime"].dt.hour
    out["date_ts"] = pd.to_datetime(out["date"].astype(str))
    out["dow"] = out["date_ts"].dt.dayofweek
    out["doy"] = out["date_ts"].dt.dayofyear
    out["month"] = out["date_ts"].dt.month
    out["is_weekend"] = (out["dow"] >= 5).astype(int)

    holidays = national_holidays(
        sorted(out["date_ts"].dt.year.unique().tolist())
    )
    out["is_holiday"] = out["date"].isin(holidays).astype(int)
    out["is_pre_holiday"] = out["date"].map(
        lambda d: int(d + timedelta(days=1) in holidays)
    )
    out["is_post_holiday"] = out["date"].map(
        lambda d: int(d - timedelta(days=1) in holidays)
    )

    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["dow"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["dow"] / 7)
    out["doy_sin"] = np.sin(2 * np.pi * out["doy"] / 365.25)
    out["doy_cos"] = np.cos(2 * np.pi * out["doy"] / 365.25)
    return out


def _generation_weather_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    out = frame.copy()
    for variable in GENERATION_WEATHER_VARIABLES:
        if variable not in out.columns:
            out[variable] = np.nan
        out[variable] = pd.to_numeric(
            out[variable],
            errors="coerce",
        )

    daily = (
        out.groupby("date", as_index=False)
        .agg(
            daily_temperature=("temperature_2m", "mean"),
            daily_radiation=("shortwave_radiation", "mean"),
            daily_cloud=("cloud_cover", "mean"),
            daily_wind=("wind_speed_100m", "mean"),
            daily_precipitation=("precipitation", "sum"),
        )
    )
    return out.merge(daily, on="date", how="left")


def _hourly_lookup(
    frame: pd.DataFrame,
    value_column: str,
) -> dict:
    temp = frame[["datetime", value_column]].copy()
    temp["datetime"] = pd.to_datetime(
        temp["datetime"],
        errors="coerce",
    )
    temp[value_column] = pd.to_numeric(
        temp[value_column],
        errors="coerce",
    )
    temp["date"] = temp["datetime"].dt.date
    temp["hour"] = temp["datetime"].dt.hour

    lookup = {}
    for day_value, hour_value, numeric_value in temp[
        ["date", "hour", value_column]
    ].itertuples(index=False, name=None):
        if pd.notna(numeric_value):
            lookup[(day_value, int(hour_value))] = float(
                numeric_value
            )
    return lookup


def _generation_lags(
    frame: pd.DataFrame,
    lookup: dict,
) -> pd.DataFrame:
    out = frame.copy()

    for lag in [2, 3, 7, 9, 10, 14, 21, 28]:
        out[f"gen_lag_{lag}d"] = [
            lookup.get(
                (d - timedelta(days=lag), int(h)),
                np.nan,
            )
            for d, h in zip(out["date"], out["hour"])
        ]

    out["gen_change_d2_d9"] = (
        out["gen_lag_2d"] - out["gen_lag_9d"]
    )
    out["gen_change_d3_d10"] = (
        out["gen_lag_3d"] - out["gen_lag_10d"]
    )
    out["gen_adjusted_d7"] = (
        out["gen_lag_7d"]
        + 0.65 * out["gen_change_d2_d9"]
        + 0.35 * out["gen_change_d3_d10"]
    )
    out["gen_same_hour_4w"] = out[
        [
            "gen_lag_7d",
            "gen_lag_14d",
            "gen_lag_21d",
            "gen_lag_28d",
        ]
    ].mean(axis=1)
    return out


GENERATION_FEATURES = [
    "hour", "month", "dow", "is_weekend", "is_holiday",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "doy_sin", "doy_cos",
    "temperature_2m", "shortwave_radiation", "cloud_cover",
    "wind_speed_100m", "precipitation",
    "daily_temperature", "daily_radiation", "daily_cloud",
    "daily_wind", "daily_precipitation",
    "gen_lag_1d", "gen_lag_2d", "gen_lag_3d", "gen_lag_7d",
    "gen_lag_9d", "gen_lag_10d",
    "gen_lag_14d", "gen_lag_21d", "gen_lag_28d",
    "gen_change_d2_d9", "gen_change_d3_d10",
    "gen_adjusted_d7", "gen_same_hour_4w",
]


@st.cache_data(show_spinner=False, ttl=3600)
def load_forecast_pbf_history(
    start_day: date,
    end_day: date,
    technologies_tuple: tuple[str, ...],
    _token: str,
) -> pd.DataFrame:
    """
    Load NET PBF generation by technology:

        net PBF = gross PBF - bilateral PBF

    This is the same perimeter used by the user's historical thermal-gap
    export. Using gross PBF would materially overstate nuclear and wind
    generation available to the market and can create artificial negative gaps.
    """
    results = []

    def _fetch_technology(technology: str) -> pd.DataFrame:
        gross = load_one_pbf_technology_hourly(
            technology=technology,
            indicator_ids=FORECAST_NON_THERMAL_INDICATORS[technology],
            start_day=start_day,
            end_day=end_day,
            token=_token,
        )
        if gross.empty:
            return pd.DataFrame()

        gross = gross.rename(
            columns={"energy_mwh": "gross_energy_mwh"}
        )[["datetime", "technology", "gross_energy_mwh"]]

        bilateral_ids = FORECAST_BILATERAL_INDICATORS.get(
            technology,
            [],
        )
        bilateral_frames = []
        for indicator_id in bilateral_ids:
            frame = fetch_one_pbf_indicator_hourly(
                indicator_id=indicator_id,
                start_day=start_day,
                end_day=end_day,
                token=_token,
            )
            if not frame.empty:
                bilateral_frames.append(frame)

        if bilateral_frames:
            bilateral = pd.concat(
                bilateral_frames,
                ignore_index=True,
            )
            bilateral = (
                bilateral.groupby("datetime", as_index=False)[
                    "energy_mwh"
                ]
                .sum()
                .rename(
                    columns={
                        "energy_mwh": "bilateral_energy_mwh"
                    }
                )
            )
        else:
            bilateral = gross[["datetime"]].copy()
            bilateral["bilateral_energy_mwh"] = 0.0

        out = gross.merge(
            bilateral,
            on="datetime",
            how="left",
        )
        out["bilateral_energy_mwh"] = (
            pd.to_numeric(
                out["bilateral_energy_mwh"],
                errors="coerce",
            )
            .fillna(0.0)
            .clip(lower=0.0)
        )
        out["gross_energy_mwh"] = pd.to_numeric(
            out["gross_energy_mwh"],
            errors="coerce",
        )
        out["energy_mwh"] = (
            out["gross_energy_mwh"]
            - out["bilateral_energy_mwh"]
        ).clip(lower=0.0)
        out["net_to_gross_ratio"] = (
            out["energy_mwh"]
            / out["gross_energy_mwh"].replace(0, np.nan)
        ).clip(lower=0.0, upper=1.10)

        return out[
            [
                "datetime",
                "technology",
                "energy_mwh",
                "gross_energy_mwh",
                "bilateral_energy_mwh",
                "net_to_gross_ratio",
            ]
        ]

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(
                _fetch_technology,
                technology,
            ): technology
            for technology in technologies_tuple
        }
        for future in as_completed(futures):
            try:
                frame = future.result()
            except Exception:
                frame = pd.DataFrame()
            if frame is not None and not frame.empty:
                results.append(frame)

    if not results:
        return pd.DataFrame(
            columns=[
                "datetime",
                "technology",
                "energy_mwh",
                "gross_energy_mwh",
                "bilateral_energy_mwh",
                "net_to_gross_ratio",
            ]
        )

    history = pd.concat(results, ignore_index=True)
    history["datetime"] = pd.to_datetime(
        history["datetime"],
        errors="coerce",
    ).dt.floor("h")

    numeric_columns = [
        "energy_mwh",
        "gross_energy_mwh",
        "bilateral_energy_mwh",
        "net_to_gross_ratio",
    ]
    for column in numeric_columns:
        history[column] = pd.to_numeric(
            history[column],
            errors="coerce",
        )

    return (
        history.dropna(
            subset=["datetime", "technology", "energy_mwh"]
        )
        .sort_values(["datetime", "technology"])
        .drop_duplicates(
            subset=["datetime", "technology"],
            keep="last",
        )
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False, ttl=3600)
def load_pbf_demand_history(
    start_day: date,
    end_day: date,
    _token: str,
) -> pd.DataFrame:
    demand = fetch_one_pbf_indicator_hourly(
        indicator_id=PBF_DEMAND_INDICATOR_ID,
        start_day=start_day,
        end_day=end_day,
        token=_token,
    )
    if demand.empty:
        return pd.DataFrame(
            columns=["datetime", "pbf_demand_mwh"]
        )

    return (
        demand.rename(
            columns={"energy_mwh": "pbf_demand_mwh"}
        )[["datetime", "pbf_demand_mwh"]]
        .sort_values("datetime")
        .drop_duplicates("datetime", keep="last")
        .reset_index(drop=True)
    )


def embedded_d1_calibration(
    target_day: date,
) -> pd.DataFrame:
    calibration_day = target_day - timedelta(days=1)
    if calibration_day != EMBEDDED_D1_CALIBRATION_DATE:
        return pd.DataFrame()

    out = pd.DataFrame(
        EMBEDDED_D1_CALIBRATION_RECORDS
    )
    out["date"] = calibration_day
    out["datetime"] = (
        pd.to_datetime(out["date"].astype(str))
        + pd.to_timedelta(out["hour"], unit="h")
    )

    generation_columns = [
        tech
        for tech in FORECAST_NON_THERMAL_INDICATORS
        if tech in out.columns
    ]
    out["non_thermal_net_mwh"] = out[
        generation_columns
    ].sum(axis=1)

    return out.sort_values("datetime").reset_index(drop=True)


def build_previous_day_calibration(
    target_day: date,
    technologies: list[str],
    pbf_history: pd.DataFrame,
    pbf_demand_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the D-1 calibration profile from ESIOS, falling back to the supplied
    16-Jul-2026 export when required.
    """
    calibration_day = target_day - timedelta(days=1)

    generation = pbf_history[
        pbf_history["datetime"].dt.date == calibration_day
    ].copy()
    demand = pbf_demand_history[
        pbf_demand_history["datetime"].dt.date == calibration_day
    ].copy()

    if not generation.empty and not demand.empty:
        generation_wide = (
            generation.pivot_table(
                index="datetime",
                columns="technology",
                values="energy_mwh",
                aggfunc="sum",
            )
            .reset_index()
        )
        generation_wide.columns.name = None
        out = demand.merge(
            generation_wide,
            on="datetime",
            how="inner",
        )
        out["hour"] = out["datetime"].dt.hour

        available = [
            tech
            for tech in technologies
            if tech in out.columns
        ]
        out["non_thermal_net_mwh"] = out[
            available
        ].sum(axis=1)
        out["actual_thermal_gap_mwh"] = (
            out["pbf_demand_mwh"]
            - out["non_thermal_net_mwh"]
        )
        out["calibration_source"] = "ESIOS D-1 net PBF"
        return out.sort_values("datetime").reset_index(drop=True)

    fallback = embedded_d1_calibration(target_day)
    if fallback.empty:
        return fallback

    keep_columns = [
        "datetime",
        "hour",
        "pbf_demand_mwh",
        *[
            tech
            for tech in technologies
            if tech in fallback.columns
        ],
        "non_thermal_net_mwh",
        "actual_thermal_gap_mwh",
        "price_eur_mwh",
    ]
    fallback = fallback[keep_columns].copy()
    fallback["calibration_source"] = (
        "User supplied 16-Jul-2026 PBF export"
    )
    return fallback


def _generation_fallback(
    technology: str,
    target: pd.DataFrame,
) -> np.ndarray:
    recent_anchor = (
        0.60 * target["gen_lag_1d"]
        + 0.40 * target["gen_adjusted_d7"]
    )
    prediction = (
        recent_anchor
        .fillna(target["gen_lag_1d"])
        .fillna(target["gen_adjusted_d7"])
        .fillna(target["gen_same_hour_4w"])
        .fillna(target["gen_lag_2d"])
        .fillna(0.0)
        .to_numpy(dtype=float)
    )

    if technology in {"Solar PV", "Solar thermal"}:
        radiation = (
            target["shortwave_radiation"]
            .fillna(0)
            .to_numpy(dtype=float)
        )
        prediction = np.where(
            radiation <= 2,
            0,
            prediction,
        )

    return np.maximum(prediction, 0)


def _complete_generation_model_inputs(
    model_data: pd.DataFrame,
    target_data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Complete model features without dropping target hours.

    Priority for missing target values:
    1. same-hour median from the training history;
    2. global training median;
    3. zero only when the feature is unavailable throughout history.

    HistGradientBoosting can technically accept NaNs, but explicit imputation
    keeps the fallback model and post-processing deterministic as well.
    """
    train = model_data.copy()
    target = target_data.copy()
    imputed_cells = 0

    # Reconstruct the lag family before generic imputation. This preserves the
    # most recent physical generation pattern whenever an isolated ESIOS hour
    # is missing.
    lag_columns = [
        "gen_lag_2d", "gen_lag_3d", "gen_lag_7d",
        "gen_lag_9d", "gen_lag_10d", "gen_lag_14d",
        "gen_lag_21d", "gen_lag_28d",
    ]
    available_lags = [c for c in lag_columns if c in target.columns]
    if available_lags:
        target_lag_mean = target[available_lags].mean(axis=1)
        for column in available_lags:
            missing_before = int(target[column].isna().sum())
            target[column] = target[column].fillna(target_lag_mean)
            imputed_cells += missing_before - int(target[column].isna().sum())

    if "gen_same_hour_4w" in target.columns:
        recent_mean = target[
            [
                c for c in [
                    "gen_lag_7d", "gen_lag_14d",
                    "gen_lag_21d", "gen_lag_28d",
                ]
                if c in target.columns
            ]
        ].mean(axis=1)
        missing_before = int(target["gen_same_hour_4w"].isna().sum())
        target["gen_same_hour_4w"] = target[
            "gen_same_hour_4w"
        ].fillna(recent_mean)
        imputed_cells += missing_before - int(
            target["gen_same_hour_4w"].isna().sum()
        )

    for column in ["gen_change_d2_d9", "gen_change_d3_d10"]:
        if column in target.columns:
            missing_before = int(target[column].isna().sum())
            target[column] = target[column].fillna(0.0)
            imputed_cells += missing_before

    if "gen_adjusted_d7" in target.columns:
        reconstructed = (
            target.get("gen_lag_7d", pd.Series(index=target.index, dtype=float))
            + 0.65 * target.get(
                "gen_change_d2_d9",
                pd.Series(0.0, index=target.index),
            )
            + 0.35 * target.get(
                "gen_change_d3_d10",
                pd.Series(0.0, index=target.index),
            )
        )
        missing_before = int(target["gen_adjusted_d7"].isna().sum())
        target["gen_adjusted_d7"] = target[
            "gen_adjusted_d7"
        ].fillna(reconstructed)
        imputed_cells += missing_before - int(
            target["gen_adjusted_d7"].isna().sum()
        )

    for feature in GENERATION_FEATURES:
        if feature not in train.columns:
            train[feature] = np.nan
        if feature not in target.columns:
            target[feature] = np.nan

        train[feature] = pd.to_numeric(train[feature], errors="coerce")
        target[feature] = pd.to_numeric(target[feature], errors="coerce")

        # Same-hour medians are especially important for radiation, wind and
        # generation lags because a single daily median would flatten profiles.
        hourly_median = train.groupby("hour")[feature].median()
        hourly_fill = target["hour"].map(hourly_median)

        missing_before = int(target[feature].isna().sum())
        target[feature] = target[feature].fillna(hourly_fill)

        global_median = train[feature].median()
        if pd.isna(global_median):
            global_median = 0.0

        target[feature] = target[feature].fillna(float(global_median))
        train[feature] = train[feature].fillna(float(global_median))
        imputed_cells += missing_before

    return train, target, imputed_cells


def forecast_pbf_technology(
    technology: str,
    history: pd.DataFrame,
    weather_history: pd.DataFrame,
    target_weather: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    technology_history = history[
        history["technology"] == technology
    ][["datetime", "energy_mwh"]].copy()

    lookup = _hourly_lookup(
        technology_history,
        "energy_mwh",
    )

    training = technology_history.merge(
        weather_history,
        on="datetime",
        how="inner",
    )
    training = _market_calendar(training)
    training = _generation_weather_features(training)
    training = _generation_lags(training, lookup)

    target = _market_calendar(target_weather.copy())
    target = _generation_weather_features(target)
    target = _generation_lags(target, lookup)

    # Keep every valid generation observation. Missing explanatory variables
    # are completed below instead of discarding the entire hour.
    model_data = training.dropna(
        subset=["energy_mwh", "datetime", "hour"]
    ).copy()
    target_data = target.dropna(
        subset=["datetime", "hour"]
    ).copy()

    model_data, target_data, imputed_cells = (
        _complete_generation_model_inputs(
            model_data,
            target_data,
        )
    )

    if len(target_data) < 23:
        raise ValueError(
            f"Only {len(target_data)} target hours are available for "
            f"{technology}; expected at least 23."
        )

    stats = {"mae": np.nan, "mape": np.nan}
    model_name = "Lag/weather fallback"

    if SKLEARN_AVAILABLE and len(model_data) >= 24 * 120:
        validation_start = (
            model_data["date_ts"].max()
            - pd.Timedelta(days=27)
        )
        train = model_data[
            model_data["date_ts"] < validation_start
        ]
        validation = model_data[
            model_data["date_ts"] >= validation_start
        ]

        if not train.empty and not validation.empty:
            validation_model = HistGradientBoostingRegressor(
                loss="absolute_error",
                learning_rate=0.055,
                max_iter=260,
                max_leaf_nodes=25,
                min_samples_leaf=24,
                l2_regularization=6,
                random_state=42,
            )
            validation_model.fit(
                train[GENERATION_FEATURES],
                train["energy_mwh"],
            )
            validation_prediction = (
                validation_model.predict(
                    validation[GENERATION_FEATURES]
                )
            )
            stats = forecast_metrics(
                validation["energy_mwh"],
                validation_prediction,
            )

            final_model = HistGradientBoostingRegressor(
                loss="absolute_error",
                learning_rate=0.055,
                max_iter=260,
                max_leaf_nodes=25,
                min_samples_leaf=24,
                l2_regularization=6,
                random_state=42,
            )
            final_model.fit(
                model_data[GENERATION_FEATURES],
                model_data["energy_mwh"],
            )
            prediction = final_model.predict(
                target_data[GENERATION_FEATURES]
            )
            model_name = "Gradient boosting"
        else:
            prediction = _generation_fallback(
                technology,
                target_data,
            )
    else:
        prediction = _generation_fallback(
            technology,
            target_data,
        )

    prediction = np.maximum(prediction, 0)

    if technology in {"Solar PV", "Solar thermal"}:
        radiation = target_data[
            "shortwave_radiation"
        ].fillna(0).to_numpy(dtype=float)
        prediction = np.where(
            radiation <= 2,
            0,
            prediction,
        )

    recent_cap = model_data["energy_mwh"].tail(
        24 * 90
    ).quantile(0.998)
    if pd.notna(recent_cap) and recent_cap > 0:
        prediction = np.minimum(
            prediction,
            float(recent_cap) * 1.12,
        )

    output = target_data[
        [
            "datetime",
            "date",
            "hour",
            "shortwave_radiation",
            "wind_speed_100m",
            "gen_lag_1d",
            "gen_lag_2d",
            "gen_lag_7d",
            "gen_adjusted_d7",
        ]
    ].copy()
    output["technology"] = technology
    output["model_net_forecast_mwh"] = prediction

    # D-1 is known from the PBF programme before tomorrow's DA gate closure.
    # Keep the model dominant, but anchor 20% in the latest complete net-PBF
    # profile to prevent implausible jumps in solar hours.
    d1_anchor = pd.to_numeric(
        output["gen_lag_1d"],
        errors="coerce",
    )
    output["forecast_mwh"] = (
        (1.0 - D1_GENERATION_BLEND_WEIGHT)
        * output["model_net_forecast_mwh"]
        + D1_GENERATION_BLEND_WEIGHT
        * d1_anchor.fillna(output["model_net_forecast_mwh"])
    ).clip(lower=0.0)

    return output, {
        "Technology": technology,
        "Model": model_name,
        "Target feature values imputed": int(imputed_cells),
        "Backtest MAE (MW)": stats.get("mae"),
        "Backtest MAPE (%)": stats.get("mape"),
    }


def generate_thermal_gap_forecast(
    target_day: date,
    lookback_days: int,
    selected_demand: pd.DataFrame,
    technologies: list[str],
    token: str,
) -> dict:
    # Demand model uses completed realised demand through D-2. PBF generation,
    # bilateral programmes and PBF demand for D-1 are already known and are
    # therefore included as calibration / lag information.
    pbf_history_end = target_day - timedelta(days=1)
    history_start = (
        target_day
        - timedelta(days=int(lookback_days))
        - timedelta(days=2)
    )

    pbf_history = load_forecast_pbf_history(
        history_start,
        pbf_history_end,
        tuple(technologies),
        token,
    )
    pbf_demand_history = load_pbf_demand_history(
        history_start,
        pbf_history_end,
        token,
    )
    weather_history = load_generation_weather_history(
        history_start,
        target_day - timedelta(days=2),
    )
    target_weather = load_generation_weather_forecast(
        target_day,
    )

    if pbf_history.empty:
        raise ValueError("No historical net PBF generation data.")
    if pbf_demand_history.empty:
        raise ValueError("No historical PBF demand data.")
    if weather_history.empty or target_weather.empty:
        raise ValueError("Generation weather data unavailable.")

    d1_calibration = build_previous_day_calibration(
        target_day,
        technologies,
        pbf_history,
        pbf_demand_history,
    )

    forecast_frames = []
    stats_rows = []

    for technology in technologies:
        if pbf_history[
            pbf_history["technology"] == technology
        ].empty:
            stats_rows.append(
                {
                    "Technology": technology,
                    "Model": "No ESIOS net-PBF history",
                    "Target feature values imputed": np.nan,
                    "Backtest MAE (MW)": np.nan,
                    "Backtest MAPE (%)": np.nan,
                }
            )
            continue

        forecast, stats = forecast_pbf_technology(
            technology,
            pbf_history,
            weather_history,
            target_weather,
        )
        forecast_frames.append(forecast)
        stats_rows.append(stats)

    if not forecast_frames:
        raise ValueError(
            "No net PBF generation forecast could be produced."
        )

    generation_long = pd.concat(
        forecast_frames,
        ignore_index=True,
    )

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

    available_technologies = [
        tech
        for tech in technologies
        if tech in generation_wide.columns
    ]
    generation_wide["non_thermal_forecast_mwh"] = (
        generation_wide[available_technologies].sum(axis=1)
    )

    demand = selected_demand[
        ["datetime", "selected_demand_mw"]
    ].copy()
    demand["datetime"] = pd.to_datetime(
        demand["datetime"],
        errors="coerce",
    ).dt.floor("h")
    demand["hour"] = demand["datetime"].dt.hour

    forecast = generation_wide.merge(
        demand,
        on=["datetime", "hour"],
        how="inner",
    )

    # Align the total-demand model to the latest known PBF demand perimeter.
    if not d1_calibration.empty:
        d1_demand = d1_calibration[
            ["hour", "pbf_demand_mwh"]
        ].drop_duplicates("hour")
        forecast = forecast.merge(
            d1_demand.rename(
                columns={
                    "pbf_demand_mwh": "d1_pbf_demand_mwh"
                }
            ),
            on="hour",
            how="left",
        )
    else:
        forecast["d1_pbf_demand_mwh"] = np.nan

    forecast["pbf_demand_forecast_mwh"] = (
        (1.0 - D1_DEMAND_BLEND_WEIGHT)
        * forecast["selected_demand_mw"]
        + D1_DEMAND_BLEND_WEIGHT
        * forecast["d1_pbf_demand_mwh"].fillna(
            forecast["selected_demand_mw"]
        )
    )

    forecast["thermal_gap_forecast_mwh"] = (
        forecast["pbf_demand_forecast_mwh"]
        - forecast["non_thermal_forecast_mwh"]
    )

    if not d1_calibration.empty:
        d1_reference = d1_calibration[
            [
                "hour",
                "actual_thermal_gap_mwh",
                "non_thermal_net_mwh",
                "calibration_source",
            ]
        ].drop_duplicates("hour")
        forecast = forecast.merge(
            d1_reference.rename(
                columns={
                    "actual_thermal_gap_mwh": (
                        "d1_actual_thermal_gap_mwh"
                    ),
                    "non_thermal_net_mwh": (
                        "d1_non_thermal_net_mwh"
                    ),
                }
            ),
            on="hour",
            how="left",
        )
    else:
        forecast["d1_actual_thermal_gap_mwh"] = np.nan
        forecast["d1_non_thermal_net_mwh"] = np.nan
        forecast["calibration_source"] = "No D-1 calibration"

    # Historical price training uses the same PBF-demand / NET-generation
    # perimeter as the target forecast.
    pbf_history_wide = (
        pbf_history.pivot_table(
            index="datetime",
            columns="technology",
            values="energy_mwh",
            aggfunc="sum",
        )
        .reset_index()
    )
    pbf_history_wide.columns.name = None
    historical_techs = [
        tech
        for tech in technologies
        if tech in pbf_history_wide.columns
    ]
    pbf_history_wide["non_thermal_mwh"] = (
        pbf_history_wide[historical_techs].sum(axis=1)
    )

    historical = pbf_demand_history.merge(
        pbf_history_wide[
            ["datetime", "non_thermal_mwh"]
        ],
        on="datetime",
        how="inner",
    )
    historical["thermal_gap_mwh"] = (
        historical["pbf_demand_mwh"]
        - historical["non_thermal_mwh"]
    )

    return {
        "forecast": forecast.sort_values(
            "datetime"
        ).reset_index(drop=True),
        "generation_long": generation_long.sort_values(
            ["datetime", "technology"]
        ).reset_index(drop=True),
        "generation_stats": pd.DataFrame(stats_rows),
        "historical": historical.sort_values(
            "datetime"
        ).reset_index(drop=True),
        "d1_calibration": d1_calibration,
    }


def build_generation_forecast_chart(
    generation_long: pd.DataFrame,
    forecast: pd.DataFrame,
):
    technologies = [
        tech
        for tech in FORECAST_NON_THERMAL_DEFAULT
        if tech in generation_long["technology"].unique()
    ]
    colors = [
        FORECAST_TECH_COLORS.get(tech, "#94A3B8")
        for tech in technologies
    ]

    area = (
        alt.Chart(generation_long)
        .mark_area(opacity=0.88)
        .encode(
            x=alt.X(
                "datetime:T",
                title=None,
                axis=alt.Axis(format="%H:%M", labelAngle=0),
            ),
            y=alt.Y(
                "sum(forecast_mwh):Q",
                title="Demand / PBF generation forecast (MW)",
                stack="zero",
            ),
            color=alt.Color(
                "technology:N",
                title="PBF technology",
                scale=alt.Scale(
                    domain=technologies,
                    range=colors,
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                    columns=4,
                    labelLimit=240,
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip(
                    "forecast_mwh:Q",
                    title="Forecast MW",
                    format=",.0f",
                ),
            ],
        )
    )

    demand_line = (
        alt.Chart(forecast)
        .mark_line(color="#111827", strokeWidth=3.2)
        .encode(
            x="datetime:T",
            y=alt.Y(
                "pbf_demand_forecast_mwh:Q",
                title="PBF demand / net PBF generation forecast (MW)",
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
                    title="Non-thermal PBF",
                    format=",.0f",
                ),
            ],
        )
    )

    return configure_chart(
        alt.layer(area, demand_line).resolve_scale(y="shared"),
        height=360,
    )


def build_thermal_gap_forecast_chart(
    forecast: pd.DataFrame,
):
    plot = forecast.copy()
    plot["gap_type"] = np.where(
        plot["thermal_gap_forecast_mwh"] > 0,
        "Positive thermal gap",
        "Negative thermal gap",
    )

    bars = (
        alt.Chart(plot)
        .mark_bar(opacity=0.9)
        .encode(
            x=alt.X(
                "datetime:T",
                title=None,
                axis=alt.Axis(format="%H:%M", labelAngle=0),
            ),
            y=alt.Y(
                "thermal_gap_forecast_mwh:Q",
                title="Forecast thermal gap (MW)",
            ),
            color=alt.Color(
                "gap_type:N",
                title=None,
                scale=alt.Scale(
                    domain=[
                        "Positive thermal gap",
                        "Negative thermal gap",
                    ],
                    range=["#2563EB", "#DC2626"],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip(
                    "selected_demand_mw:Q",
                    title="Demand forecast",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "non_thermal_forecast_mwh:Q",
                    title="Non-thermal PBF",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "thermal_gap_forecast_mwh:Q",
                    title="Thermal gap",
                    format=",.0f",
                ),
            ],
        )
    )

    d1_line = (
        alt.Chart(plot)
        .mark_line(
            color="#F97316",
            strokeWidth=2.5,
            strokeDash=[6, 3],
            point=True,
        )
        .encode(
            x=alt.X("datetime:T"),
            y=alt.Y(
                "d1_actual_thermal_gap_mwh:Q",
                title="Forecast thermal gap (MW)",
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip(
                    "d1_actual_thermal_gap_mwh:Q",
                    title="D-1 actual net-PBF gap",
                    format=",.0f",
                ),
            ],
        )
    )

    zero = (
        alt.Chart(pd.DataFrame({"zero": [0]}))
        .mark_rule(color="#0F172A")
        .encode(y="zero:Q")
    )
    return configure_chart(
        alt.layer(zero, bars, d1_line),
        height=290,
    )



# =========================================================
# MIBGAS GDAES D+1 — DAILY GAS INPUT
# =========================================================
def _mibgas_normalize_column(column) -> str:
    if pd.isna(column):
        return ""
    value = (
        str(column)
        .replace("\xa0", " ")
        .replace("\n", " ")
        .strip()
        .lower()
    )
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
        "[": "",
        "]": "",
        "(": "",
        ")": "",
        "%": "pct",
        "/": "_",
        "-": "_",
        ".": "_",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    value = re.sub(r"\s+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def _mibgas_to_number(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace("€", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("\xa0", "", regex=False)
    )
    cleaned = (
        cleaned.str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _mibgas_first_column(
    columns: list[str],
    candidates: list[str],
) -> str | None:
    column_set = set(columns)
    for candidate in candidates:
        normalized = _mibgas_normalize_column(candidate)
        if normalized in column_set:
            return normalized
    return None


def _mibgas_data_directories() -> list[Path]:
    candidates = [
        CURRENT_FILE.parent / "data",
        CURRENT_FILE.parent.parent / "data",
    ]
    output = []
    for candidate in candidates:
        if candidate not in output:
            output.append(candidate)
    return output


def _standardize_mibgas_actuals(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Return GDAES_D+1 reference prices indexed by gas delivery day.

    This follows the MIBGAS page convention:
      market_trading_day = original Trading day
      gas_delivery_day   = First Day Delivery
    """
    if raw is None or raw.empty:
        return pd.DataFrame(
            columns=[
                "gas_delivery_day",
                "market_trading_day",
                "gas_price_eur_mwh",
                "gas_source",
            ]
        )

    frame = raw.copy()
    frame.columns = [
        _mibgas_normalize_column(column)
        for column in frame.columns
    ]

    trading_column = _mibgas_first_column(
        frame.columns.tolist(),
        ["Trading day", "trading_day"],
    )
    product_column = _mibgas_first_column(
        frame.columns.tolist(),
        ["Product", "product"],
    )
    area_column = _mibgas_first_column(
        frame.columns.tolist(),
        ["Area", "area"],
    )
    delivery_column = _mibgas_first_column(
        frame.columns.tolist(),
        ["First Day Delivery", "first_day_delivery", "delivery_start"],
    )
    price_column = _mibgas_first_column(
        frame.columns.tolist(),
        [
            "Reference Price [EUR/MWh]",
            "Daily Reference Price [EUR/MWh]",
            "reference_price_eur_mwh",
            "daily_reference_price_eur_mwh",
        ],
    )

    if (
        trading_column is None
        or product_column is None
        or price_column is None
    ):
        return pd.DataFrame(
            columns=[
                "gas_delivery_day",
                "market_trading_day",
                "gas_price_eur_mwh",
                "gas_source",
            ]
        )

    product = frame[product_column].astype(str).str.strip()
    area = (
        frame[area_column].astype(str).str.strip()
        if area_column is not None
        else pd.Series("ES", index=frame.index)
    )

    mask = (
        product.eq("GDAES_D+1")
        & area.fillna("ES").eq("ES")
    )
    selected = frame.loc[mask].copy()
    if selected.empty:
        return pd.DataFrame(
            columns=[
                "gas_delivery_day",
                "market_trading_day",
                "gas_price_eur_mwh",
                "gas_source",
            ]
        )

    output = pd.DataFrame(index=selected.index)
    output["market_trading_day"] = pd.to_datetime(
        selected[trading_column],
        dayfirst=True,
        errors="coerce",
    )
    output["gas_delivery_day"] = (
        pd.to_datetime(
            selected[delivery_column],
            dayfirst=True,
            errors="coerce",
        )
        if delivery_column is not None
        else pd.NaT
    )
    output["gas_delivery_day"] = (
        output["gas_delivery_day"]
        .combine_first(output["market_trading_day"])
        .dt.normalize()
    )
    output["gas_price_eur_mwh"] = _mibgas_to_number(
        selected[price_column]
    )
    output["gas_source"] = (
        selected["source_file"].astype(str)
        if "source_file" in selected.columns
        else "MIBGAS"
    )

    return (
        output.dropna(
            subset=[
                "gas_delivery_day",
                "gas_price_eur_mwh",
            ]
        )
        .sort_values(
            [
                "gas_delivery_day",
                "market_trading_day",
            ]
        )
        .drop_duplicates(
            subset=["gas_delivery_day"],
            keep="last",
        )
        .reset_index(drop=True)
    )


def _read_mibgas_excel_actuals(path: Path) -> pd.DataFrame:
    try:
        workbook = pd.ExcelFile(path)
    except Exception:
        return pd.DataFrame()

    sheet = None
    if MIBGAS_TARGET_SHEET in workbook.sheet_names:
        sheet = MIBGAS_TARGET_SHEET
    else:
        for candidate in workbook.sheet_names:
            candidate_text = str(candidate).upper()
            if "PVB" in candidate_text and "VTP" in candidate_text:
                sheet = candidate
                break

    if sheet is None:
        return pd.DataFrame()

    try:
        frame = pd.read_excel(path, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()

    frame["source_file"] = f"{path.name}/{sheet}"
    return _standardize_mibgas_actuals(frame)


@st.cache_data(show_spinner=False, ttl=1800)
def load_mibgas_gdaes_actuals() -> tuple[pd.DataFrame, str]:
    """
    Load the same gas-price source used by the MIBGAS page.

    Historical years come from data/MIBGAS_Data_*.xlsx. The current-year cache
    comes from data/mibgas_2026_cache.csv, which is refreshed by the MIBGAS
    Streamlit page. The forecast remains operational when the gas source is
    unavailable; gas features then receive a zero availability flag.
    """
    frames = []
    sources = []

    for data_directory in _mibgas_data_directories():
        if not data_directory.exists():
            continue

        for path in sorted(
            data_directory.glob(MIBGAS_LOCAL_FILE_PATTERN)
        ):
            actuals = _read_mibgas_excel_actuals(path)
            if not actuals.empty:
                frames.append(actuals)
                sources.append(path.name)

        cache_path = data_directory / MIBGAS_CACHE_FILENAME
        if cache_path.exists():
            try:
                cached = pd.read_csv(cache_path)
                cached["source_file"] = cache_path.name
                actuals = _standardize_mibgas_actuals(cached)
                if not actuals.empty:
                    frames.append(actuals)
                    sources.append(cache_path.name)
            except Exception:
                pass

    if not frames:
        return (
            pd.DataFrame(
                columns=[
                    "gas_delivery_day",
                    "market_trading_day",
                    "gas_price_eur_mwh",
                    "gas_source",
                ]
            ),
            (
                "No MIBGAS GDAES_D+1 data found. Open the MIBGAS page and "
                "refresh its SFTP cache, or add MIBGAS_Data_*.xlsx to /data."
            ),
        )

    combined = pd.concat(frames, ignore_index=True)
    combined["gas_delivery_day"] = pd.to_datetime(
        combined["gas_delivery_day"],
        errors="coerce",
    ).dt.normalize()
    combined["market_trading_day"] = pd.to_datetime(
        combined["market_trading_day"],
        errors="coerce",
    )
    combined["gas_price_eur_mwh"] = pd.to_numeric(
        combined["gas_price_eur_mwh"],
        errors="coerce",
    )

    combined = (
        combined.dropna(
            subset=[
                "gas_delivery_day",
                "gas_price_eur_mwh",
            ]
        )
        .sort_values(
            [
                "gas_delivery_day",
                "market_trading_day",
            ]
        )
        .drop_duplicates(
            subset=["gas_delivery_day"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    message = (
        f"{len(combined):,} GDAES_D+1 delivery-day prices loaded from "
        + ", ".join(sorted(set(sources)))
    )
    return combined, message


def build_mibgas_daily_lookup(
    gas_actuals: pd.DataFrame,
    start_day: date,
    end_day: date,
) -> tuple[dict, dict]:
    """
    Build a daily as-of lookup.

    Short gaps such as weekends/holidays are forward-filled for at most four
    days. Long gaps are left unavailable rather than carrying stale gas prices.
    """
    if gas_actuals is None or gas_actuals.empty:
        return {}, {}

    daily = gas_actuals[
        [
            "gas_delivery_day",
            "gas_price_eur_mwh",
            "gas_source",
        ]
    ].copy()
    daily["gas_delivery_day"] = pd.to_datetime(
        daily["gas_delivery_day"],
        errors="coerce",
    ).dt.normalize()
    daily = daily.dropna(
        subset=[
            "gas_delivery_day",
            "gas_price_eur_mwh",
        ]
    )

    full_index = pd.date_range(
        start=pd.Timestamp(start_day),
        end=pd.Timestamp(end_day),
        freq="D",
    )
    indexed = (
        daily.set_index("gas_delivery_day")
        .sort_index()
        .reindex(full_index)
    )
    indexed["gas_price_eur_mwh"] = (
        pd.to_numeric(
            indexed["gas_price_eur_mwh"],
            errors="coerce",
        )
        .ffill(limit=4)
    )
    indexed["gas_source"] = indexed["gas_source"].ffill(
        limit=4
    )

    price_lookup = {
        timestamp.date(): float(value)
        for timestamp, value in indexed[
            "gas_price_eur_mwh"
        ].items()
        if pd.notna(value)
    }
    source_lookup = {
        timestamp.date(): str(value)
        for timestamp, value in indexed[
            "gas_source"
        ].items()
        if pd.notna(value)
    }
    return price_lookup, source_lookup


def add_mibgas_features(
    frame: pd.DataFrame,
    gas_price_lookup: dict,
) -> pd.DataFrame:
    """
    Add no-leakage gas features to every electricity delivery date D.

    The primary variable is GDAES_D+1 for gas delivery D-1. We also add recent
    changes and a gas × thermal-gap interaction for CCGT price-setting hours.
    """
    out = frame.copy()

    def _gas(day_value, lag_days: int):
        return gas_price_lookup.get(
            day_value - timedelta(days=lag_days),
            np.nan,
        )

    out["mibgas_d1_eur_mwh"] = [
        _gas(day_value, 1)
        for day_value in out["date"]
    ]
    out["mibgas_d2_eur_mwh"] = [
        _gas(day_value, 2)
        for day_value in out["date"]
    ]
    out["mibgas_d7_eur_mwh"] = [
        _gas(day_value, 7)
        for day_value in out["date"]
    ]

    seven_day_values = []
    for day_value in out["date"]:
        values = [
            _gas(day_value, lag)
            for lag in range(1, 8)
        ]
        valid = [
            value
            for value in values
            if pd.notna(value)
        ]
        seven_day_values.append(
            float(np.mean(valid))
            if valid
            else np.nan
        )

    out["mibgas_7d_avg_eur_mwh"] = seven_day_values
    out["mibgas_change_d1_eur_mwh"] = (
        out["mibgas_d1_eur_mwh"]
        - out["mibgas_d2_eur_mwh"]
    )
    out["mibgas_change_vs_7d_eur_mwh"] = (
        out["mibgas_d1_eur_mwh"]
        - out["mibgas_7d_avg_eur_mwh"]
    )
    out["mibgas_data_available"] = (
        out["mibgas_d1_eur_mwh"].notna().astype(int)
    )

    # Preserve functionality when the gas page/cache is unavailable. The
    # availability flag lets the model distinguish an absent input from a true
    # zero gas price.
    gas_numeric_columns = [
        "mibgas_d1_eur_mwh",
        "mibgas_d2_eur_mwh",
        "mibgas_d7_eur_mwh",
        "mibgas_7d_avg_eur_mwh",
        "mibgas_change_d1_eur_mwh",
        "mibgas_change_vs_7d_eur_mwh",
    ]
    for column in gas_numeric_columns:
        out[column] = pd.to_numeric(
            out[column],
            errors="coerce",
        ).fillna(0.0)

    out["mibgas_x_positive_gap"] = (
        out["mibgas_d1_eur_mwh"]
        * out["positive_gap"]
        / 10_000.0
    )
    return out


# =========================================================
# STEP 3 — FORECAST DA SPOT PRICE
# =========================================================
@st.cache_data(show_spinner=False, ttl=3600)
def load_esios_price_history(
    start_day: date,
    end_day: date,
    _token: str,
) -> pd.DataFrame:
    frames = []
    current = start_day

    while current <= end_day:
        chunk_end = min(
            end_day,
            current + timedelta(days=13),
        )
        start_local = pd.Timestamp(
            current,
            tz="Europe/Madrid",
        )
        end_local = pd.Timestamp(
            chunk_end + timedelta(days=1),
            tz="Europe/Madrid",
        )
        params = {
            "start_date": start_local.tz_convert("UTC").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "end_date": end_local.tz_convert("UTC").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "time_trunc": "hour",
            "time_agg": "average",
        }

        for attempt in range(3):
            try:
                response = requests.get(
                    f"{ESIOS_API_BASE}/{PRICE_INDICATOR_ID}",
                    headers=esios_headers(_token),
                    params=params,
                    timeout=(15, 120),
                )
                response.raise_for_status()
                parsed = parse_esios_values(response.json())
                if not parsed.empty:
                    frames.append(parsed)
                break
            except requests.RequestException:
                sleep(1.5 * (attempt + 1))

        current = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(
            columns=["datetime", "price_eur_mwh"]
        )

    output = pd.concat(frames, ignore_index=True)
    output["datetime"] = pd.to_datetime(
        output["datetime"],
        errors="coerce",
    ).dt.floor("h")
    output["value"] = pd.to_numeric(
        output["value"],
        errors="coerce",
    )

    return (
        output.dropna(subset=["datetime", "value"])
        .groupby("datetime", as_index=False)["value"]
        .mean()
        .rename(columns={"value": "price_eur_mwh"})
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def _price_features(
    frame: pd.DataFrame,
    price_lookup: dict,
    gap_lookup: dict,
    gas_price_lookup: dict,
) -> pd.DataFrame:
    out = _market_calendar(frame)

    for lag in [1, 2, 7, 14, 21, 28]:
        out[f"price_lag_{lag}d"] = [
            price_lookup.get(
                (d - timedelta(days=lag), int(h)),
                np.nan,
            )
            for d, h in zip(out["date"], out["hour"])
        ]

    for lag in [1, 2, 7]:
        out[f"gap_lag_{lag}d"] = [
            gap_lookup.get(
                (d - timedelta(days=lag), int(h)),
                np.nan,
            )
            for d, h in zip(out["date"], out["hour"])
        ]

    out["price_lag_1d"] = out["price_lag_1d"].fillna(
        out["price_lag_2d"]
    )
    out["gap_lag_1d"] = out["gap_lag_1d"].fillna(
        out["gap_lag_2d"]
    )

    out["same_hour_price_4w"] = out[
        [
            "price_lag_7d",
            "price_lag_14d",
            "price_lag_21d",
            "price_lag_28d",
        ]
    ].median(axis=1)

    out["price_anchor"] = (
        0.45 * out["price_lag_7d"]
        + 0.35 * out["price_lag_1d"]
        + 0.20 * out["same_hour_price_4w"]
    )
    out["price_anchor"] = out["price_anchor"].fillna(
        0.65 * out["price_lag_7d"]
        + 0.35 * out["same_hour_price_4w"]
    )

    out["positive_gap"] = out[
        "thermal_gap_mwh"
    ].clip(lower=0)
    out["gap_sq_scaled"] = (
        out["positive_gap"] / 10_000
    ) ** 2
    out["gap_change_d1"] = (
        out["thermal_gap_mwh"] - out["gap_lag_1d"]
    )
    out["gap_change_d7"] = (
        out["thermal_gap_mwh"] - out["gap_lag_7d"]
    )

    out = add_mibgas_features(
        out,
        gas_price_lookup,
    )
    return out


PRICE_FEATURES = [
    "hour", "month", "dow", "is_weekend",
    "is_holiday", "is_pre_holiday", "is_post_holiday",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "doy_sin", "doy_cos",
    "thermal_gap_mwh", "positive_gap", "gap_sq_scaled",
    "gap_change_d1", "gap_change_d7",
    "gap_lag_1d", "gap_lag_2d", "gap_lag_7d",
    "price_lag_1d", "price_lag_2d", "price_lag_7d",
    "price_lag_14d", "price_lag_21d", "price_lag_28d",
    "same_hour_price_4w", "price_anchor",
    "mibgas_d1_eur_mwh", "mibgas_d2_eur_mwh",
    "mibgas_d7_eur_mwh", "mibgas_7d_avg_eur_mwh",
    "mibgas_change_d1_eur_mwh",
    "mibgas_change_vs_7d_eur_mwh",
    "mibgas_data_available", "mibgas_x_positive_gap",
]



def empirical_similar_gap_price_reference(
    historical_data: pd.DataFrame,
    target_data: pd.DataFrame,
    max_candidates: int = 120,
) -> pd.DataFrame:
    """
    Build an empirical price reference for each target hour using historical
    observations with a similar thermal gap and nearby hour of day.

    This avoids the incorrect structural assumption that a negative thermal gap
    must imply an exact 0 EUR/MWh price. The historical sample captures other
    balancing mechanisms that are not fully represented in the simplified gap:
    exports, pumping, storage charging, curtailment and market constraints.
    """
    required = {
        "thermal_gap_mwh",
        "price_eur_mwh",
        "hour",
        "is_weekend",
    }
    if historical_data is None or historical_data.empty:
        return pd.DataFrame(index=target_data.index)

    history = historical_data.copy()
    missing = required.difference(history.columns)
    if missing:
        return pd.DataFrame(index=target_data.index)

    history["thermal_gap_mwh"] = pd.to_numeric(
        history["thermal_gap_mwh"],
        errors="coerce",
    )
    history["price_eur_mwh"] = pd.to_numeric(
        history["price_eur_mwh"],
        errors="coerce",
    )
    history = history.dropna(
        subset=["thermal_gap_mwh", "price_eur_mwh", "hour"]
    ).copy()

    if history.empty:
        return pd.DataFrame(index=target_data.index)

    gap_scale = max(
        float(history["thermal_gap_mwh"].std()),
        2_500.0,
    )

    output_rows = []

    for row in target_data.itertuples():
        target_gap = float(row.thermal_gap_mwh)
        target_hour = int(row.hour)
        target_weekend = int(row.is_weekend)

        candidates = history[
            history["is_weekend"] == target_weekend
        ].copy()

        if len(candidates) < 50:
            candidates = history.copy()

        hour_distance = (
            candidates["hour"] - target_hour
        ).abs()
        candidates["hour_distance"] = np.minimum(
            hour_distance,
            24 - hour_distance,
        )
        candidates["gap_distance"] = (
            candidates["thermal_gap_mwh"] - target_gap
        ).abs()

        # Prefer observations within +/-2 hours. Relax automatically when
        # the sample is too small.
        local = candidates[
            candidates["hour_distance"] <= 2
        ].copy()
        if len(local) >= 30:
            candidates = local

        candidates["distance_score"] = (
            candidates["gap_distance"] / gap_scale
            + candidates["hour_distance"] / 3.0
        )

        # Where available, prefer historical observations with a similar D-1
        # MIBGAS level. A 10 €/MWh gas difference has roughly the same distance
        # weight as one unit of the thermal-gap scale.
        if (
            "mibgas_d1_eur_mwh" in candidates.columns
            and hasattr(row, "mibgas_d1_eur_mwh")
            and float(getattr(row, "mibgas_data_available", 0)) > 0
        ):
            target_gas = float(row.mibgas_d1_eur_mwh)
            candidates["gas_distance"] = (
                pd.to_numeric(
                    candidates["mibgas_d1_eur_mwh"],
                    errors="coerce",
                )
                - target_gas
            ).abs()
            candidates["distance_score"] = (
                candidates["distance_score"]
                + candidates["gas_distance"].fillna(0.0) / 10.0
            )
        candidates = candidates.nsmallest(
            max_candidates,
            "distance_score",
        )

        if candidates.empty:
            output_rows.append(
                {
                    "conditional_price_median_eur_mwh": np.nan,
                    "conditional_price_p25_eur_mwh": np.nan,
                    "conditional_price_p75_eur_mwh": np.nan,
                    "probability_price_le_zero_pct": np.nan,
                    "probability_price_below_5_pct": np.nan,
                    "similar_gap_observations": 0,
                }
            )
            continue

        prices = candidates["price_eur_mwh"]

        output_rows.append(
            {
                "conditional_price_median_eur_mwh": float(
                    prices.median()
                ),
                "conditional_price_p25_eur_mwh": float(
                    prices.quantile(0.25)
                ),
                "conditional_price_p75_eur_mwh": float(
                    prices.quantile(0.75)
                ),
                "probability_price_le_zero_pct": float(
                    (prices <= 0.0).mean() * 100.0
                ),
                "probability_price_below_5_pct": float(
                    (prices <= 5.0).mean() * 100.0
                ),
                "similar_gap_observations": int(len(prices)),
            }
        )

    result = pd.DataFrame(
        output_rows,
        index=target_data.index,
    )
    return result


def blend_model_with_empirical_low_gap_reference(
    raw_prediction,
    model_data: pd.DataFrame,
    target_data: pd.DataFrame,
) -> tuple[np.ndarray, pd.DataFrame]:
    """
    Blend the model forecast with an empirical similar-gap price reference.

    The empirical weight rises only when the forecast gap is unusually low
    relative to history. There is no hard zero-price override.
    """
    empirical = empirical_similar_gap_price_reference(
        model_data,
        target_data,
    )

    raw = np.maximum(
        np.asarray(raw_prediction, dtype=float),
        0.0,
    )

    if empirical.empty:
        return raw, empirical

    historical_gap = pd.to_numeric(
        model_data["thermal_gap_mwh"],
        errors="coerce",
    ).dropna()

    if historical_gap.empty:
        empirical["low_gap_blend_weight"] = 0.0
        return raw, empirical

    low_gap_threshold = float(
        historical_gap.quantile(0.15)
    )
    extreme_low_gap = float(
        historical_gap.quantile(0.01)
    )

    denominator = max(
        low_gap_threshold - extreme_low_gap,
        1_000.0,
    )

    severity = np.clip(
        (
            low_gap_threshold
            - target_data["thermal_gap_mwh"].to_numpy(dtype=float)
        )
        / denominator,
        0.0,
        1.0,
    )

    # Normal hours remain almost entirely model-driven. Very low-gap hours
    # receive up to 45% empirical calibration.
    blend_weight = 0.05 + 0.40 * severity

    empirical_median = pd.to_numeric(
        empirical["conditional_price_median_eur_mwh"],
        errors="coerce",
    ).to_numpy(dtype=float)

    valid_empirical = np.isfinite(empirical_median)
    blend_weight = np.where(
        valid_empirical,
        blend_weight,
        0.0,
    )
    empirical_median = np.where(
        valid_empirical,
        np.maximum(empirical_median, 0.0),
        raw,
    )

    blended = (
        (1.0 - blend_weight) * raw
        + blend_weight * empirical_median
    )

    empirical["low_gap_blend_weight"] = (
        blend_weight * 100.0
    )
    empirical["historical_low_gap_threshold_mw"] = (
        low_gap_threshold
    )

    return np.maximum(blended, 0.0), empirical



def recent_price_guardrails(
    raw_prediction,
    target_data: pd.DataFrame,
    empirical_reference: pd.DataFrame | None,
    hourly_bias_correction: pd.Series | None = None,
) -> tuple[np.ndarray, pd.DataFrame]:
    """
    Prevent the residual price model from moving too far away from all recent
    hourly references without a sufficiently strong thermal-gap justification.

    The model is still allowed to exceed D-1 / D-7 when tomorrow's thermal gap
    is materially higher, but otherwise the positive residual is shrunk and
    capped by a recent-price envelope.
    """
    target = target_data.copy()
    raw = np.asarray(raw_prediction, dtype=float)

    anchor = pd.to_numeric(
        target["price_anchor"],
        errors="coerce",
    ).to_numpy(dtype=float)

    # Correct systematic recent hourly bias estimated out of sample.
    if hourly_bias_correction is not None and len(hourly_bias_correction):
        bias = (
            target["hour"]
            .map(hourly_bias_correction)
            .fillna(0.0)
            .clip(lower=-15.0, upper=15.0)
            .to_numpy(dtype=float)
        )
        raw = raw - 0.65 * bias
    else:
        bias = np.zeros(len(target), dtype=float)

    model_residual = raw - anchor

    gap_reference = pd.concat(
        [
            pd.to_numeric(target["gap_lag_1d"], errors="coerce"),
            pd.to_numeric(target["gap_lag_7d"], errors="coerce"),
        ],
        axis=1,
    ).max(axis=1)

    gap_shock = (
        pd.to_numeric(target["thermal_gap_mwh"], errors="coerce")
        - gap_reference
    ).fillna(0.0).to_numpy(dtype=float)

    # Only a clear positive thermal-gap shock should justify a material uplift
    # above all recent price references.
    shock_strength = np.clip(gap_shock / 4_000.0, 0.0, 1.0)

    hours = target["hour"].to_numpy(dtype=int)
    evening_or_night = (hours >= 18) | (hours <= 6)

    # Positive residuals are shrunk more aggressively overnight/evening.
    positive_shrink = np.where(
        evening_or_night,
        0.30 + 0.40 * shock_strength,
        0.48 + 0.32 * shock_strength,
    )
    negative_shrink = np.where(
        evening_or_night,
        0.72,
        0.82,
    )

    residual_shrink = np.where(
        model_residual >= 0,
        positive_shrink,
        negative_shrink,
    )

    shrunk = anchor + residual_shrink * model_residual

    reference_frame = pd.DataFrame(
        {
            "price_lag_1d": pd.to_numeric(
                target["price_lag_1d"],
                errors="coerce",
            ),
            "price_lag_7d": pd.to_numeric(
                target["price_lag_7d"],
                errors="coerce",
            ),
            "same_hour_price_4w": pd.to_numeric(
                target["same_hour_price_4w"],
                errors="coerce",
            ),
            "price_anchor": pd.to_numeric(
                target["price_anchor"],
                errors="coerce",
            ),
        },
        index=target.index,
    )

    if (
        empirical_reference is not None
        and not empirical_reference.empty
        and "conditional_price_p75_eur_mwh" in empirical_reference.columns
    ):
        reference_frame["similar_gap_p75"] = pd.to_numeric(
            empirical_reference["conditional_price_p75_eur_mwh"],
            errors="coerce",
        ).to_numpy()

    recent_upper = reference_frame.max(axis=1, skipna=True).to_numpy(
        dtype=float
    )
    recent_lower = reference_frame.min(axis=1, skipna=True).to_numpy(
        dtype=float
    )

    # Allow only a modest buffer above recent references unless tomorrow's
    # thermal gap is genuinely higher. 3 EUR/MWh per additional GW, capped.
    gap_uplift_allowance = np.clip(
        np.maximum(gap_shock, 0.0) * 0.003,
        0.0,
        22.0,
    )

    # A higher D-1 MIBGAS price can justify a higher CCGT-linked electricity
    # price even when the thermal-gap shape is unchanged. The allowance uses a
    # conservative 1.8x heat-rate proxy and is capped to avoid overreaction.
    gas_change_vs_7d = pd.to_numeric(
        target.get(
            "mibgas_change_vs_7d_eur_mwh",
            pd.Series(0.0, index=target.index),
        ),
        errors="coerce",
    ).fillna(0.0).to_numpy(dtype=float)
    gas_uplift_allowance = np.clip(
        np.maximum(gas_change_vs_7d, 0.0) * 1.8,
        0.0,
        20.0,
    )

    base_buffer = np.where(evening_or_night, 4.0, 6.0)
    upper_guardrail = (
        recent_upper
        + base_buffer
        + gap_uplift_allowance
        + gas_uplift_allowance
    )

    # Do not over-constrain downward moves. The lower guardrail is deliberately
    # loose and exists only to avoid pathological model output.
    lower_guardrail = np.maximum(
        0.0,
        recent_lower - np.where(evening_or_night, 20.0, 30.0),
    )

    guarded = np.clip(
        shrunk,
        lower_guardrail,
        upper_guardrail,
    )

    diagnostics = pd.DataFrame(
        {
            "hourly_bias_correction_eur_mwh": bias,
            "model_residual_before_guardrail_eur_mwh": model_residual,
            "residual_shrink_factor": residual_shrink,
            "gap_shock_vs_recent_mw": gap_shock,
            "gap_uplift_allowance_eur_mwh": gap_uplift_allowance,
            "gas_uplift_allowance_eur_mwh": gas_uplift_allowance,
            "recent_reference_upper_eur_mwh": recent_upper,
            "recent_reference_lower_eur_mwh": recent_lower,
            "price_upper_guardrail_eur_mwh": upper_guardrail,
            "price_lower_guardrail_eur_mwh": lower_guardrail,
            "guardrail_reduction_eur_mwh": np.maximum(
                shrunk - guarded,
                0.0,
            ),
            "guardrail_increase_eur_mwh": np.maximum(
                guarded - shrunk,
                0.0,
            ),
            "guardrail_applied": np.abs(guarded - shrunk) > 1e-6,
        },
        index=target.index,
    )

    return np.maximum(guarded, 0.0), diagnostics



def apply_negative_gap_near_zero_calibration(
    forecast_prices,
    target_data: pd.DataFrame,
) -> tuple[np.ndarray, pd.DataFrame]:
    """
    Strongly compress forecast prices when the simplified thermal gap is
    negative, without imposing an exact zero-price rule.

    Soft upper cap:
        TG =     0 MW -> 8.0 EUR/MWh
        TG = -1000 MW -> 6.25 EUR/MWh
        TG = -2000 MW -> 4.50 EUR/MWh
        TG = -3000 MW -> 2.75 EUR/MWh
        TG <=-4000 MW -> 1.00 EUR/MWh

    A small positive gap receives no structural cap.
    """
    prices = np.maximum(
        np.asarray(forecast_prices, dtype=float),
        0.0,
    )

    gap = pd.to_numeric(
        target_data["thermal_gap_mwh"],
        errors="coerce",
    ).fillna(0.0).to_numpy(dtype=float)

    negative_mask = gap < 0.0

    negative_severity = np.clip(
        np.maximum(-gap, 0.0) / 4_000.0,
        0.0,
        1.0,
    )

    # Continuous cap from 8 EUR/MWh at a just-negative gap to 1 EUR/MWh
    # for a gap at or below -4 GW.
    near_zero_cap = (
        8.0 - 7.0 * negative_severity
    )

    calibrated = prices.copy()
    calibrated[negative_mask] = np.minimum(
        prices[negative_mask],
        near_zero_cap[negative_mask],
    )

    diagnostics = pd.DataFrame(
        {
            "negative_gap_near_zero_cap_eur_mwh": np.where(
                negative_mask,
                near_zero_cap,
                np.nan,
            ),
            "negative_gap_price_before_calibration_eur_mwh": prices,
            "negative_gap_price_compression_eur_mwh": np.where(
                negative_mask,
                np.maximum(prices - calibrated, 0.0),
                0.0,
            ),
            "negative_gap_near_zero_applied": (
                negative_mask
                & (calibrated < prices - 1e-9)
            ),
        },
        index=target_data.index,
    )

    return np.maximum(calibrated, 0.0), diagnostics


def generate_price_forecast(
    target_day: date,
    historical_gap: pd.DataFrame,
    forecast_gap: pd.DataFrame,
    token: str,
) -> dict:
    historical = historical_gap[
        ["datetime", "thermal_gap_mwh"]
    ].copy()
    prices = load_esios_price_history(
        historical["datetime"].min().date(),
        target_day - timedelta(days=1),
        token,
    )
    if prices.empty:
        raise ValueError("No spot-price history returned by ESIOS.")

    gas_actuals, gas_data_message = load_mibgas_gdaes_actuals()
    gas_price_lookup, gas_source_lookup = build_mibgas_daily_lookup(
        gas_actuals,
        historical["datetime"].min().date() - timedelta(days=35),
        target_day - timedelta(days=1),
    )

    training = historical.merge(
        prices,
        on="datetime",
        how="inner",
    )
    training["date"] = training["datetime"].dt.date
    training["hour"] = training["datetime"].dt.hour

    price_frame = prices.copy()
    price_frame["date"] = price_frame["datetime"].dt.date
    price_frame["hour"] = price_frame["datetime"].dt.hour
    price_lookup = {
        (r.date, int(r.hour)): float(r.price_eur_mwh)
        for r in price_frame.itertuples(index=False)
        if pd.notna(r.price_eur_mwh)
    }

    gap_frame = historical.copy()
    gap_frame["date"] = gap_frame["datetime"].dt.date
    gap_frame["hour"] = gap_frame["datetime"].dt.hour
    gap_lookup = {
        (r.date, int(r.hour)): float(r.thermal_gap_mwh)
        for r in gap_frame.itertuples(index=False)
        if pd.notna(r.thermal_gap_mwh)
    }

    training = _price_features(
        training,
        price_lookup,
        gap_lookup,
        gas_price_lookup,
    )
    training["price_residual"] = (
        training["price_eur_mwh"]
        - training["price_anchor"]
    )

    target = forecast_gap[
        ["datetime", "thermal_gap_forecast_mwh"]
    ].rename(
        columns={
            "thermal_gap_forecast_mwh": "thermal_gap_mwh"
        }
    )
    target = _price_features(
        target,
        price_lookup,
        gap_lookup,
        gas_price_lookup,
    )

    model_data = training.dropna(
        subset=["price_eur_mwh", "price_residual"] + PRICE_FEATURES
    ).copy()
    target_data = target.dropna(
        subset=PRICE_FEATURES
    ).copy()

    if len(model_data) < 24 * 120:
        raise ValueError("Insufficient complete price history.")
    if len(target_data) < 23:
        raise ValueError("Incomplete target price anchors.")

    validation_start = (
        model_data["date_ts"].max()
        - pd.Timedelta(days=41)
    )
    train = model_data[
        model_data["date_ts"] < validation_start
    ]
    validation = model_data[
        model_data["date_ts"] >= validation_start
    ]

    if (
        SKLEARN_AVAILABLE
        and not train.empty
        and not validation.empty
    ):
        validation_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.05,
            max_iter=320,
            max_leaf_nodes=27,
            min_samples_leaf=28,
            l2_regularization=10,
            random_state=42,
        )
        validation_model.fit(
            train[PRICE_FEATURES],
            train["price_residual"],
        )
        validation_raw = (
            validation["price_anchor"].to_numpy()
            + validation_model.predict(
                validation[PRICE_FEATURES]
            )
        )

        final_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            learning_rate=0.05,
            max_iter=320,
            max_leaf_nodes=27,
            min_samples_leaf=28,
            l2_regularization=10,
            random_state=42,
        )
        final_model.fit(
            model_data[PRICE_FEATURES],
            model_data["price_residual"],
        )
        target_raw = (
            target_data["price_anchor"].to_numpy()
            + final_model.predict(
                target_data[PRICE_FEATURES]
            )
        )
        model_name = (
            "Thermal-gap residual model around D-1 / D-7 anchor"
        )
    else:
        validation_raw = validation[
            "price_anchor"
        ].to_numpy()
        target_raw = target_data[
            "price_anchor"
        ].to_numpy()
        model_name = "D-1 / D-7 anchor fallback"

    # Estimate systematic recent hourly model bias from the chronological
    # validation period. Positive values mean the model overpredicted.
    validation_error = pd.Series(
        validation_raw - validation["price_eur_mwh"].to_numpy(dtype=float),
        index=validation.index,
    )
    hourly_bias_correction = (
        pd.DataFrame(
            {
                "hour": validation["hour"].to_numpy(),
                "error": validation_error.to_numpy(),
            }
        )
        .groupby("hour")["error"]
        .median()
    )

    # First calibrate against historically similar thermal-gap observations.
    validation_empirical_blend, validation_empirical = (
        blend_model_with_empirical_low_gap_reference(
            validation_raw,
            train if not train.empty else model_data,
            validation,
        )
    )
    target_empirical_blend, target_empirical = (
        blend_model_with_empirical_low_gap_reference(
            target_raw,
            model_data,
            target_data,
        )
    )

    # Then constrain unjustified deviations from D-1, D-7 and the recent
    # same-hour profile. This specifically controls evening/night overshoots.
    validation_final, validation_guardrails = recent_price_guardrails(
        validation_empirical_blend,
        validation,
        validation_empirical,
        hourly_bias_correction=None,
    )
    target_final, target_guardrails = recent_price_guardrails(
        target_empirical_blend,
        target_data,
        target_empirical,
        hourly_bias_correction=hourly_bias_correction,
    )

    # Thermal gap below zero is a very strong low-price signal. Apply this
    # after all gas, anchor and residual adjustments so no later uplift can
    # override the near-zero calibration.
    validation_final, validation_negative_gap = (
        apply_negative_gap_near_zero_calibration(
            validation_final,
            validation,
        )
    )
    target_final, target_negative_gap = (
        apply_negative_gap_near_zero_calibration(
            target_final,
            target_data,
        )
    )

    cap = model_data["price_eur_mwh"].quantile(0.997)
    if pd.notna(cap):
        target_final = np.minimum(
            target_final,
            max(float(cap) * 1.20, 250),
        )

    output = target_data[
        [
            "datetime",
            "date",
            "hour",
            "thermal_gap_mwh",
            "price_anchor",
            "price_lag_1d",
            "price_lag_7d",
            "same_hour_price_4w",
            "mibgas_d1_eur_mwh",
            "mibgas_d2_eur_mwh",
            "mibgas_d7_eur_mwh",
            "mibgas_7d_avg_eur_mwh",
            "mibgas_change_d1_eur_mwh",
            "mibgas_change_vs_7d_eur_mwh",
            "mibgas_data_available",
            "mibgas_x_positive_gap",
        ]
    ].copy()
    output["forecast_price_eur_mwh"] = target_final
    output["raw_model_price_eur_mwh"] = target_raw
    output["negative_thermal_gap_flag"] = (
        output["thermal_gap_mwh"] <= 0
    )

    if target_empirical is not None and not target_empirical.empty:
        for column in target_empirical.columns:
            output[column] = target_empirical[column].to_numpy()

    if target_guardrails is not None and not target_guardrails.empty:
        for column in target_guardrails.columns:
            output[column] = target_guardrails[column].to_numpy()

    if (
        target_negative_gap is not None
        and not target_negative_gap.empty
    ):
        for column in target_negative_gap.columns:
            output[column] = (
                target_negative_gap[column].to_numpy()
            )

    return {
        "forecast": output.sort_values(
            "datetime"
        ).reset_index(drop=True),
        "model_stats": forecast_metrics(
            validation["price_eur_mwh"],
            validation_final,
        ),
        "anchor_stats": forecast_metrics(
            validation["price_eur_mwh"],
            validation["price_anchor"],
        ),
        "model_name": (
            model_name
            + " + MIBGAS GDAES D-1"
            if int(target_data["mibgas_data_available"].max()) > 0
            else model_name + " (MIBGAS unavailable)"
        ),
        "training_rows": len(model_data),
        "gas_data_message": gas_data_message,
        "gas_delivery_day_used": target_day - timedelta(days=1),
        "gas_price_d1_eur_mwh": (
            float(target_data["mibgas_d1_eur_mwh"].iloc[0])
            if int(target_data["mibgas_data_available"].max()) > 0
            else np.nan
        ),
        "gas_price_7d_avg_eur_mwh": (
            float(target_data["mibgas_7d_avg_eur_mwh"].iloc[0])
            if int(target_data["mibgas_data_available"].max()) > 0
            else np.nan
        ),
        "gas_price_change_d1_eur_mwh": (
            float(target_data["mibgas_change_d1_eur_mwh"].iloc[0])
            if int(target_data["mibgas_data_available"].max()) > 0
            else np.nan
        ),
        "gas_source": gas_source_lookup.get(
            target_day - timedelta(days=1),
            "Unavailable",
        ),
    }


def build_price_forecast_chart(
    price_forecast: pd.DataFrame,
):
    long = pd.concat(
        [
            price_forecast[
                ["datetime", "forecast_price_eur_mwh"]
            ]
            .rename(
                columns={
                    "forecast_price_eur_mwh": "price"
                }
            )
            .assign(series="DA price forecast"),
            price_forecast[
                ["datetime", "price_lag_1d"]
            ]
            .rename(columns={"price_lag_1d": "price"})
            .assign(series="Previous day"),
            price_forecast[
                ["datetime", "price_lag_7d"]
            ]
            .rename(columns={"price_lag_7d": "price"})
            .assign(series="Same weekday previous week"),
            price_forecast[
                ["datetime", "price_anchor"]
            ]
            .rename(columns={"price_anchor": "price"})
            .assign(series="Absolute-price anchor"),
            price_forecast[
                ["datetime", "conditional_price_median_eur_mwh"]
            ]
            .rename(
                columns={
                    "conditional_price_median_eur_mwh": "price"
                }
            )
            .assign(series="Historical similar-gap median"),
            price_forecast[
                ["datetime", "price_upper_guardrail_eur_mwh"]
            ]
            .rename(
                columns={
                    "price_upper_guardrail_eur_mwh": "price"
                }
            )
            .assign(series="Dynamic upper guardrail"),
        ],
        ignore_index=True,
    )

    domain = [
        "DA price forecast",
        "Previous day",
        "Same weekday previous week",
        "Absolute-price anchor",
        "Historical similar-gap median",
        "Dynamic upper guardrail",
    ]

    chart = (
        alt.Chart(long)
        .mark_line(point=True, strokeWidth=3)
        .encode(
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
                        "#F97316",
                        "#60A5FA",
                        "#64748B",
                        "#A855F7",
                        "#DC2626",
                    ],
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                    columns=3,
                    labelLimit=320,
                ),
            ),
            strokeDash=alt.StrokeDash(
                "series:N",
                legend=None,
                scale=alt.Scale(
                    domain=domain,
                    range=[
                        [1, 0],
                        [5, 3],
                        [2, 2],
                        [8, 3],
                        [3, 2],
                        [10, 4],
                    ],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip(
                    "price:Q",
                    title="€/MWh",
                    format=",.2f",
                ),
            ],
        )
    )
    return configure_chart(chart, height=340)


def forecast_tb4(prices: pd.Series) -> float:
    clean = (
        pd.to_numeric(prices, errors="coerce")
        .dropna()
        .sort_values()
    )
    if len(clean) < 8:
        return np.nan
    return float(
        clean.tail(4).mean() - clean.head(4).mean()
    )


def price_realization_metrics(
    comparison: pd.DataFrame,
) -> dict:
    """Accuracy metrics for forecast versus realised DA prices."""
    empty_result = {
        "mae": np.nan,
        "rmse": np.nan,
        "mape": np.nan,
        "smape": np.nan,
        "forecast_baseload": np.nan,
        "actual_baseload": np.nan,
        "baseload_error": np.nan,
        "forecast_tb4": np.nan,
        "actual_tb4": np.nan,
        "tb4_error": np.nan,
    }

    if comparison is None or comparison.empty:
        return empty_result

    actual = pd.to_numeric(
        comparison["actual_price_eur_mwh"],
        errors="coerce",
    )
    forecast = pd.to_numeric(
        comparison["forecast_price_eur_mwh"],
        errors="coerce",
    )

    valid = actual.notna() & forecast.notna()
    actual = actual[valid]
    forecast = forecast[valid]

    if actual.empty:
        return empty_result

    error = forecast - actual
    nonzero = actual.abs() > 1e-9

    forecast_tb4_value = forecast_tb4(forecast)
    actual_tb4_value = forecast_tb4(actual)

    return {
        "mae": float(error.abs().mean()),
        "rmse": float(np.sqrt((error ** 2).mean())),
        # MAPE is reported only for hours where the realised absolute price
        # exceeds 5 EUR/MWh; otherwise near-zero prices make it explode.
        "mape": (
            float(
                (
                    error[actual.abs() > 5.0].abs()
                    / actual[actual.abs() > 5.0].abs()
                ).mean()
                * 100
            )
            if (actual.abs() > 5.0).any()
            else np.nan
        ),
        # Symmetric MAPE remains bounded between 0% and 200%.
        "smape": float(
            (
                2.0 * error.abs()
                / (
                    actual.abs()
                    + forecast.abs()
                ).replace(0.0, np.nan)
            ).mean()
            * 100
        ),
        "forecast_baseload": float(forecast.mean()),
        "actual_baseload": float(actual.mean()),
        "baseload_error": float(
            forecast.mean() - actual.mean()
        ),
        "forecast_tb4": forecast_tb4_value,
        "actual_tb4": actual_tb4_value,
        "tb4_error": (
            float(
                forecast_tb4_value
                - actual_tb4_value
            )
            if pd.notna(forecast_tb4_value)
            and pd.notna(actual_tb4_value)
            else np.nan
        ),
    }


def build_forecast_vs_real_price_chart(
    comparison: pd.DataFrame,
):
    if comparison is None or comparison.empty:
        return None

    long = pd.concat(
        [
            comparison[
                ["datetime", "forecast_price_eur_mwh"]
            ]
            .rename(
                columns={
                    "forecast_price_eur_mwh": "price"
                }
            )
            .assign(series="Forecast price"),
            comparison[
                ["datetime", "actual_price_eur_mwh"]
            ]
            .rename(
                columns={
                    "actual_price_eur_mwh": "price"
                }
            )
            .assign(series="Real DA price"),
        ],
        ignore_index=True,
    )

    lines = (
        alt.Chart(long)
        .mark_line(
            point=True,
            strokeWidth=3.2,
        )
        .encode(
            x=alt.X(
                "datetime:T",
                title=None,
                axis=alt.Axis(
                    format="%H:%M",
                    labelAngle=0,
                ),
            ),
            y=alt.Y(
                "price:Q",
                title="DA price (€/MWh)",
                scale=alt.Scale(zero=True),
            ),
            color=alt.Color(
                "series:N",
                title="Price series",
                scale=alt.Scale(
                    domain=[
                        "Forecast price",
                        "Real DA price",
                    ],
                    range=[
                        "#111827",
                        "#16A34A",
                    ],
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                ),
            ),
            strokeDash=alt.StrokeDash(
                "series:N",
                legend=None,
                scale=alt.Scale(
                    domain=[
                        "Forecast price",
                        "Real DA price",
                    ],
                    range=[
                        [6, 3],
                        [1, 0],
                    ],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip(
                    "series:N",
                    title="Series",
                ),
                alt.Tooltip(
                    "price:Q",
                    title="€/MWh",
                    format=",.2f",
                ),
            ],
        )
    )

    error_area = (
        alt.Chart(comparison)
        .mark_area(
            opacity=0.10,
            color="#64748B",
        )
        .encode(
            x=alt.X("datetime:T"),
            y=alt.Y(
                "forecast_price_eur_mwh:Q",
                title="DA price (€/MWh)",
            ),
            y2="actual_price_eur_mwh:Q",
        )
    )

    return configure_chart(
        alt.layer(error_area, lines),
        height=340,
    )


# =========================================================
# APP
# =========================================================
st.title("Demand, PBF thermal gap and DA price — forecast test")

st.caption(
    "Daily peninsular electricity demand from REData, historical temperature "
    "from Open-Meteo ERA5-Land and hourly programmed PBF generation from ESIOS."
)

token = require_esios_token()

archive_safe_end = date.today() - timedelta(days=5)
default_end = archive_safe_end
# Default view: the latest seven complete days available in the archive.
default_start = default_end - timedelta(days=6)

controls_1, controls_2, controls_3 = st.columns([1, 1, 1.2])

with controls_1:
    start_day = st.date_input(
        "Start date",
        value=default_start,
        max_value=archive_safe_end,
        key="test_demand_temp_start",
    )

with controls_2:
    end_day = st.date_input(
        "End date",
        value=default_end,
        max_value=archive_safe_end,
        key="test_demand_temp_end",
    )

with controls_3:
    temperature_mode = st.selectbox(
        "Temperature series",
        ["Spain weighted proxy", "Madrid"],
        index=0,
        key="test_temperature_mode",
    )

if start_day > end_day:
    st.error("The start date must be earlier than or equal to the end date.")
    st.stop()

range_days = (end_day - start_day).days + 1

if range_days > MAX_RANGE_DAYS:
    st.error(
        f"Select no more than {MAX_RANGE_DAYS} days so the ESIOS test remains fast."
    )
    st.stop()

with st.spinner("Loading demand, temperature and PBF data..."):
    demand_daily = load_daily_peninsular_demand(
        start_day=start_day,
        end_day=end_day,
    )

    temperature_daily = load_spain_daily_temperature(
        start_day=start_day,
        end_day=end_day,
        mode=temperature_mode,
    )

    pbf_hourly = load_pbf_hourly_mix(
        start_day=start_day,
        end_day=end_day,
        _token=token,
    )


# =========================================================
# DEMAND + TEMPERATURE
# =========================================================
section_header("Daily demand and temperature")

combined = demand_daily.merge(
    temperature_daily,
    on="date",
    how="inner",
).sort_values("date")

if combined.empty:
    st.warning(
        "No common daily demand and temperature observations were returned "
        "for the selected period."
    )
else:
    temperature_label = (
        "Spain weighted temperature proxy"
        if temperature_mode == "Spain weighted proxy"
        else "Madrid daily mean temperature"
    )

    series_options = [
        "Demand daily",
        "Demand rolling average",
        "Temperature daily",
        "Temperature rolling average",
    ]

    series_col, rolling_col = st.columns([2.4, 1])
    with series_col:
        selected_chart_series = st.multiselect(
            "Variables shown in chart",
            options=series_options,
            default=series_options,
            key="demand_temperature_chart_series",
        )

    rolling_is_selected = any(
        "rolling" in series.lower()
        for series in selected_chart_series
    )

    with rolling_col:
        rolling_days = st.number_input(
            "Rolling window (days)",
            min_value=2,
            max_value=30,
            value=7,
            step=1,
            disabled=not rolling_is_selected,
            key="demand_temperature_rolling_days",
        )

    metric_1, metric_2, metric_3, metric_4 = st.columns(4)

    combined["avg_demand_gw"] = combined["demand_gwh"] / 24.0

    metric_1.metric(
        "Average daily demand",
        f"{combined['avg_demand_gw'].mean():,.1f} GW",
    )
    metric_2.metric(
        "Peak average daily demand",
        f"{combined['avg_demand_gw'].max():,.1f} GW",
    )
    metric_3.metric(
        "Average temperature",
        f"{combined['temperature_c'].mean():,.1f} °C",
    )
    metric_4.metric(
        "Temperature range",
        (
            f"{combined['temperature_c'].min():,.1f}"
            f"–{combined['temperature_c'].max():,.1f} °C"
        ),
    )

    demand_temperature_chart = build_demand_temperature_chart(
        combined,
        temperature_label=temperature_label,
        selected_series=selected_chart_series,
        rolling_days=int(rolling_days),
    )

    if demand_temperature_chart is None:
        st.info("Select at least one variable to display in the chart.")
    else:
        st.altair_chart(
            demand_temperature_chart,
            use_container_width=True,
        )

    st.caption(
        "Choose independently whether to show the daily demand, rolling demand, "
        "daily temperature and rolling temperature. Dashed lines are daily observations; "
        "solid lines are rolling averages. Demand is displayed as average daily GW. "
        "The Spain series is a population-weighted proxy, not an official AEMET national index."
    )



# =========================================================
# NEXT-DAY DEMAND FORECAST
# =========================================================
section_header("Step 1 — Day-ahead peninsular demand forecast")

st.caption(
    "Select tomorrow for a live forecast or any historical date from 2024 "
    "for an operational backtest. Historical runs use the weather forecast "
    "issued 24 hours before the selected day; demand is cut off at D-2, "
    "PBF and prices at D-1, so realised target-day information is not used "
    "until the Step 4 comparison."
)

fc1, fc2, fc3, fc4 = st.columns([1.0, 1.0, 1.1, 1.35])
with fc1:
    forecast_target_day = st.date_input(
        "Forecast target day",
        value=date.today() + timedelta(days=1),
        min_value=date(2024, 2, 1),
        max_value=date.today() + timedelta(days=1),
        help=(
            "Choose a historical date to recreate the forecast as if the "
            "model were running on D-1, or choose tomorrow for the live run."
        ),
        key="forecast_target_day",
    )
with fc2:
    forecast_lookback = st.select_slider(
        "Training history",
        options=[365, 450, 550, 650, 730],
        value=550,
        format_func=lambda value: f"{value} days",
        key="forecast_training_days",
    )
with fc3:
    forecast_trend_alpha = st.slider(
        "Recent weekly-trend weight",
        min_value=0.0,
        max_value=1.5,
        value=1.0,
        step=0.1,
        help=(
            "Adjusts the D-7 demand curve using D-2 versus D-9 "
            "and D-3 versus D-10."
        ),
        key="forecast_weekly_trend_alpha",
    )
with fc4:
    downstream_demand_source = st.selectbox(
        "Demand used for thermal gap",
        [
            "Model forecast — green",
            "Previous week + recent trend — orange",
        ],
        index=0,
        key="forecast_downstream_demand_source",
    )

forecast_generation_technologies = st.multiselect(
    "PBF generation included in thermal gap",
    options=list(FORECAST_NON_THERMAL_INDICATORS.keys()),
    default=FORECAST_NON_THERMAL_DEFAULT,
    help=(
        "Thermal gap = PBF-calibrated demand minus forecast NET PBF generation. "
        "Run-of-river corresponds to Hydro non-UGH."
    ),
    key="forecast_generation_technologies",
)

forecast_as_of_day = forecast_target_day - timedelta(days=1)
forecast_is_historical = forecast_target_day <= date.today()
weather_input_label = (
    "Archived D-1 forecast (_previous_day1)"
    if forecast_is_historical
    else "Live Open-Meteo Best Match"
)

st.markdown(
    f"""
    <div style="
        background:#F8FAFC;
        border:1px solid #CBD5E1;
        border-radius:10px;
        padding:10px 13px;
        margin:6px 0 12px 0;
        color:#334155;
        font-size:0.90rem;
    ">
      <b>Forecast simulation date:</b> {forecast_as_of_day:%d/%m/%Y}
      &nbsp;·&nbsp;
      <b>Delivery date:</b> {forecast_target_day:%d/%m/%Y}
      &nbsp;·&nbsp;
      <b>Weather input:</b> {weather_input_label}
    </div>
    """,
    unsafe_allow_html=True,
)

if st.button(
    "Generate demand, thermal gap and DA prices",
    type="primary",
    use_container_width=True,
):
    try:
        if not forecast_generation_technologies:
            raise ValueError(
                "Select at least one PBF generation technology."
            )

        with st.spinner(
            "Step 1/3 — training tomorrow's demand forecast..."
        ):
            demand_result = generate_day_ahead_forecast(
                forecast_target_day,
                int(forecast_lookback),
                temperature_mode,
                float(forecast_trend_alpha),
            )

        forecast_for_market = demand_result["forecast"].copy()

        if downstream_demand_source.startswith("Model"):
            forecast_for_market["selected_demand_mw"] = (
                forecast_for_market["forecast_mw"]
            )
            demand_source_label = "Model forecast — green"
        else:
            forecast_for_market["selected_demand_mw"] = (
                forecast_for_market[
                    "trend_adjusted_lag_7d"
                ]
            )
            demand_source_label = (
                "Previous week + recent trend — orange"
            )

        with st.spinner(
            "Step 2/3 — forecasting PBF generation and thermal gap..."
        ):
            thermal_result = generate_thermal_gap_forecast(
                forecast_target_day,
                int(forecast_lookback),
                forecast_for_market[
                    ["datetime", "selected_demand_mw"]
                ],
                list(forecast_generation_technologies),
                token,
            )

        with st.spinner(
            "Step 3/3 — forecasting tomorrow's DA spot-price curve..."
        ):
            price_result = generate_price_forecast(
                forecast_target_day,
                thermal_result["historical"],
                thermal_result["forecast"],
                token,
            )

        # Loaded only after the forecast has been produced to prevent leakage.
        realised_prices = load_esios_price_history(
            forecast_target_day,
            forecast_target_day,
            token,
        )

        st.session_state["day_ahead_result_v11"] = {
            **demand_result,
            "forecast": forecast_for_market,
            "demand_source_label": demand_source_label,
            "thermal_result": thermal_result,
            "price_result": price_result,
            "realised_prices": realised_prices,
            "target_day": forecast_target_day,
            "as_of_day": forecast_as_of_day,
            "weather_input_label": weather_input_label,
            "generation_technologies": list(
                forecast_generation_technologies
            ),
        }

    except Exception as exc:
        st.error(f"Day-ahead forecast failed: {exc}")

forecast_result = st.session_state.get("day_ahead_result_v11")
if forecast_result:
    forecast_df = forecast_result["forecast"]
    peak = forecast_df.loc[forecast_df["forecast_mw"].idxmax()]
    minimum = forecast_df.loc[forecast_df["forecast_mw"].idxmin()]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Forecast average", f"{forecast_df['forecast_mw'].mean():,.0f} MW")
    m2.metric("Forecast peak", f"{peak['forecast_mw']:,.0f} MW", delta=f"{int(peak['hour']):02d}:00", delta_color="off")
    m3.metric("Forecast minimum", f"{minimum['forecast_mw']:,.0f} MW", delta=f"{int(minimum['hour']):02d}:00", delta_color="off")
    m4.metric("Forecast temperature", f"{forecast_df['temperature_c'].mean():,.1f} °C")

    st.markdown(
        """
        <div style="
            display:flex;
            flex-wrap:wrap;
            gap:18px;
            align-items:center;
            margin:4px 0 8px 0;
            color:#334155;
            font-size:0.88rem;
            font-weight:650;
        ">
          <span><span style="display:inline-block;width:30px;border-top:3px solid #22C55E;margin-right:7px;vertical-align:middle;"></span>Model forecast</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dashed #F97316;margin-right:7px;vertical-align:middle;"></span>Previous week + recent trend</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dashed #64748B;margin-right:7px;vertical-align:middle;"></span>Same weekday previous week</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dotted #93C5FD;margin-right:7px;vertical-align:middle;"></span>D-2 actual reference</span>
          <span><span style="display:inline-block;width:30px;height:12px;background:rgba(16,185,129,0.18);border:1px solid rgba(16,185,129,0.35);margin-right:7px;vertical-align:middle;"></span>P10–P90 uncertainty band</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.altair_chart(build_day_ahead_chart(forecast_df), use_container_width=True)

    average_trend = forecast_df["recent_weekly_trend_mw"].mean()
    evening_mask = forecast_df["hour"].between(18, 22)
    evening_trend = (
        forecast_df.loc[evening_mask, "recent_weekly_trend_mw"].mean()
        if evening_mask.any()
        else np.nan
    )
    st.caption(
        "Recent weekly trend applied to the orange reference: "
        f"{average_trend:+,.0f} MW on average across the day"
        + (
            f" and {evening_trend:+,.0f} MW during 18:00–22:00."
            if pd.notna(evening_trend)
            else "."
        )
        + f" Trend weight α = {forecast_result['trend_alpha']:.1f}."
    )

    thermal_result = forecast_result.get("thermal_result")
    price_result = forecast_result.get("price_result")

    # =====================================================
    # STEP 2 — PBF generation and forecast thermal gap
    # =====================================================
    if thermal_result is not None:
        section_header(
            "Step 2 — Forecast PBF generation and thermal gap"
        )

        thermal_forecast = thermal_result["forecast"]
        generation_long = thermal_result["generation_long"]

        st.caption(
            "The black line is the selected demand curve: "
            f"{forecast_result.get('demand_source_label', 'Model forecast')}. "
            "The stacked area is the hourly PBF forecast for wind, solar, "
            "run-of-river, nuclear and other selected technologies."
        )

        st.altair_chart(
            build_generation_forecast_chart(
                generation_long,
                thermal_forecast,
            ),
            use_container_width=True,
        )

        thermal_gap = thermal_forecast[
            "thermal_gap_forecast_mwh"
        ]

        tg1, tg2, tg3, tg4, tg5 = st.columns(5)
        tg1.metric(
            "Average PBF demand forecast",
            f"{thermal_forecast['pbf_demand_forecast_mwh'].mean():,.0f} MW",
        )
        tg2.metric(
            "Average NET PBF generation",
            f"{thermal_forecast['non_thermal_forecast_mwh'].mean():,.0f} MW",
        )
        tg3.metric(
            "Average thermal gap",
            f"{thermal_gap.mean():,.0f} MW",
        )
        tg4.metric(
            "Peak thermal gap",
            f"{thermal_gap.max():,.0f} MW",
        )
        tg5.metric(
            "Negative-gap hours",
            f"{int((thermal_gap <= 0).sum())} h",
        )

        st.altair_chart(
            build_thermal_gap_forecast_chart(
                thermal_forecast
            ),
            use_container_width=True,
        )

        d1_calibration = thermal_result.get("d1_calibration", pd.DataFrame())
        if d1_calibration is not None and not d1_calibration.empty:
            solar_hours = d1_calibration[
                d1_calibration["hour"].between(10, 18)
            ]
            d1_min_gap = pd.to_numeric(
                solar_hours["actual_thermal_gap_mwh"],
                errors="coerce",
            ).min()
            d1_min_price = pd.to_numeric(
                solar_hours.get(
                    "price_eur_mwh",
                    pd.Series(dtype=float),
                ),
                errors="coerce",
            ).min()

            st.info(
                "D-1 calibration check: during solar hours the supplied "
                f"net-PBF thermal gap remained positive, with a minimum of "
                f"{d1_min_gap:,.0f} MW"
                + (
                    f", while the minimum DA price was "
                    f"{d1_min_price:,.2f} €/MWh."
                    if pd.notna(d1_min_price)
                    else "."
                )
                + " This profile is now used as the latest calibration anchor."
            )

        st.caption(
            "Forecast thermal gap = PBF-calibrated demand forecast − forecast "
            "NET PBF generation. Gross PBF is reduced by bilateral programmes. "
            "The orange dashed line is the actual D-1 thermal-gap profile used "
            "for calibration; historical observations remain hidden and are "
            "used only for training."
        )

        with st.expander(
            "PBF generation forecast quality"
        ):
            st.dataframe(
                thermal_result["generation_stats"],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown(
                """
                - Every technology is forecast in **net PBF terms**: gross PBF minus bilateral PBF.
                - **Solar:** forecast radiation, cloud cover and D-1 / recent net-PBF lags.
                - **Wind:** forecast wind at 100 m and D-1 / recent net-PBF lags.
                - **Run-of-river:** Hydro non-UGH, precipitation and recent net programme.
                - **Hydro UGH, nuclear and other renewables:** D-1 net programme and calendar effects.
                - The supplied 16-Jul profile is embedded as a fallback calibration for the 17-Jul forecast.
                """
            )

    # =====================================================
    # STEP 3 — forecast DA spot price
    # =====================================================
    if price_result is not None:
        section_header(
            "Step 3 — Forecast day-ahead spot prices"
        )

        price_forecast = price_result["forecast"]
        forecast_prices = price_forecast[
            "forecast_price_eur_mwh"
        ]

        p1, p2, p3, p4, p5, p6 = st.columns(6)
        p1.metric(
            "Forecast baseload",
            f"{forecast_prices.mean():,.2f} €/MWh",
        )
        p2.metric(
            "Forecast minimum",
            f"{forecast_prices.min():,.2f} €/MWh",
        )
        p3.metric(
            "Forecast maximum",
            f"{forecast_prices.max():,.2f} €/MWh",
        )
        negative_gap_hours = int(
            price_forecast[
                "negative_thermal_gap_flag"
            ].sum()
        )
        negative_gap_average_price = (
            price_forecast.loc[
                price_forecast[
                    "negative_thermal_gap_flag"
                ],
                "forecast_price_eur_mwh",
            ].mean()
        )
        p4.metric(
            "Negative-gap price",
            (
                f"{negative_gap_average_price:,.2f} €/MWh"
                if negative_gap_hours > 0
                else "—"
            ),
            delta=(
                f"{negative_gap_hours} h"
                if negative_gap_hours > 0
                else None
            ),
            delta_color="off",
        )
        p5.metric(
            "Forecast TB4",
            f"{forecast_tb4(forecast_prices):,.2f} €/MWh",
        )
        p6.metric(
            "Guardrail-limited hours",
            f"{int(price_forecast['guardrail_applied'].sum())} h",
        )

        gas_price_d1 = price_result.get(
            "gas_price_d1_eur_mwh",
            np.nan,
        )
        gas_price_7d = price_result.get(
            "gas_price_7d_avg_eur_mwh",
            np.nan,
        )
        gas_change_d1 = price_result.get(
            "gas_price_change_d1_eur_mwh",
            np.nan,
        )

        gas1, gas2, gas3 = st.columns(3)
        gas1.metric(
            "MIBGAS GDAES D-1",
            (
                f"{gas_price_d1:,.2f} €/MWh"
                if pd.notna(gas_price_d1)
                else "Unavailable"
            ),
            delta=(
                f"Gas delivery {price_result.get('gas_delivery_day_used'):%d/%m/%Y}"
                if pd.notna(gas_price_d1)
                else None
            ),
            delta_color="off",
        )
        gas2.metric(
            "MIBGAS previous 7D average",
            (
                f"{gas_price_7d:,.2f} €/MWh"
                if pd.notna(gas_price_7d)
                else "Unavailable"
            ),
        )
        gas3.metric(
            "MIBGAS change vs D-2",
            (
                f"{gas_change_d1:+,.2f} €/MWh"
                if pd.notna(gas_change_d1)
                else "Unavailable"
            ),
        )

        st.altair_chart(
            build_price_forecast_chart(
                price_forecast
            ),
            use_container_width=True,
        )

        st.caption(
            f"Price model: {price_result['model_name']}. "
            f"Backtest MAPE: {price_result['model_stats']['mape']:,.2f}% "
            f"versus {price_result['anchor_stats']['mape']:,.2f}% for the "
            "unadjusted price anchor. Absolute values are anchored in the "
            "previous day, the same weekday one week earlier and the recent "
            "same-hour profile. A negative thermal gap now imposes a "
            "continuous near-zero price cap: approximately 8 €/MWh at a "
            "just-negative gap, falling to 1 €/MWh at −4 GW or below. "
            "Positive model residuals are also shrunk—especially "
            "during evening and night hours—and cannot exceed all recent "
            "references by a material amount unless the forecast thermal gap "
            "is clearly higher than D-1 and D-7. The red dashed line shows the "
            "dynamic upper guardrail. The model also uses the MIBGAS "
            "GDAES_D+1 Reference Price for gas delivery D-1, its recent "
            "changes and a gas × thermal-gap interaction. "
            f"Gas source: {price_result.get('gas_source', 'Unavailable')}."
        )

        st.caption(
            price_result.get(
                "gas_data_message",
                "MIBGAS source status unavailable.",
            )
        )

        # =================================================
        # STEP 4 — compare against realised prices
        # =================================================
        realised_prices = forecast_result.get(
            "realised_prices",
            pd.DataFrame(),
        )

        if (
            realised_prices is not None
            and not realised_prices.empty
        ):
            comparison = price_forecast[
                [
                    "datetime",
                    "forecast_price_eur_mwh",
                ]
            ].merge(
                realised_prices[
                    ["datetime", "price_eur_mwh"]
                ].rename(
                    columns={
                        "price_eur_mwh": (
                            "actual_price_eur_mwh"
                        )
                    }
                ),
                on="datetime",
                how="inner",
            )

            if not comparison.empty:
                section_header(
                    "Step 4 — Forecast versus realised DA prices"
                )

                real_metrics = price_realization_metrics(
                    comparison
                )

                r1, r2, r3, r4, r5, r6 = st.columns(6)
                r1.metric(
                    "Real baseload",
                    f"{real_metrics['actual_baseload']:,.2f} €/MWh",
                )
                r2.metric(
                    "Baseload error",
                    f"{real_metrics['baseload_error']:+,.2f} €/MWh",
                )
                r3.metric(
                    "Hourly MAE",
                    f"{real_metrics['mae']:,.2f} €/MWh",
                )
                r4.metric(
                    "Hourly RMSE",
                    f"{real_metrics['rmse']:,.2f} €/MWh",
                )
                r5.metric(
                    "Hourly sMAPE",
                    f"{real_metrics['smape']:,.2f}%",
                    help=(
                        "Symmetric MAPE is bounded and remains meaningful "
                        "when realised prices are close to zero."
                    ),
                )
                r6.metric(
                    "TB4 error",
                    f"{real_metrics['tb4_error']:+,.2f} €/MWh",
                )

                realised_chart = (
                    build_forecast_vs_real_price_chart(
                        comparison
                    )
                )
                if realised_chart is not None:
                    st.altair_chart(
                        realised_chart,
                        use_container_width=True,
                    )

                st.caption(
                    "The forecast was generated using only information "
                    f"available by {forecast_result.get('as_of_day'):%d/%m/%Y}. "
                    "Realised target-day prices are loaded afterwards and are "
                    "used exclusively for this comparison."
                )

                with st.expander(
                    "Hourly forecast error"
                ):
                    comparison["error_eur_mwh"] = (
                        comparison[
                            "forecast_price_eur_mwh"
                        ]
                        - comparison[
                            "actual_price_eur_mwh"
                        ]
                    )
                    comparison["absolute_error_eur_mwh"] = (
                        comparison[
                            "error_eur_mwh"
                        ].abs()
                    )
                    st.dataframe(
                        comparison,
                        use_container_width=True,
                        hide_index=True,
                    )
        else:
            section_header(
                "Step 4 — Forecast versus realised DA prices"
            )
            st.info(
                "Real DA prices are not available yet for the selected "
                "delivery date. This comparison will appear once ESIOS "
                "publishes them and the forecast is rerun."
            )

        requested_price_output_columns = [
            "datetime",
            "forecast_price_eur_mwh",
            "price_anchor",
            "price_lag_1d",
            "price_lag_7d",
            "mibgas_d1_eur_mwh",
            "mibgas_d2_eur_mwh",
            "mibgas_d7_eur_mwh",
            "mibgas_7d_avg_eur_mwh",
            "mibgas_change_d1_eur_mwh",
            "mibgas_change_vs_7d_eur_mwh",
            "mibgas_data_available",
            "mibgas_x_positive_gap",
            "negative_thermal_gap_flag",
            "conditional_price_median_eur_mwh",
            "conditional_price_p25_eur_mwh",
            "conditional_price_p75_eur_mwh",
            "probability_price_le_zero_pct",
            "probability_price_below_5_pct",
            "low_gap_blend_weight",
            "similar_gap_observations",
            "hourly_bias_correction_eur_mwh",
            "model_residual_before_guardrail_eur_mwh",
            "residual_shrink_factor",
            "gap_shock_vs_recent_mw",
            "gap_uplift_allowance_eur_mwh",
            "gas_uplift_allowance_eur_mwh",
            "recent_reference_upper_eur_mwh",
            "price_upper_guardrail_eur_mwh",
            "guardrail_reduction_eur_mwh",
            "guardrail_applied",
            "negative_gap_near_zero_cap_eur_mwh",
            "negative_gap_price_before_calibration_eur_mwh",
            "negative_gap_price_compression_eur_mwh",
            "negative_gap_near_zero_applied",
        ]

        available_price_output_columns = [
            column
            for column in requested_price_output_columns
            if column in price_forecast.columns
        ]

        complete_output = thermal_forecast.merge(
            price_forecast[
                available_price_output_columns
            ],
            on="datetime",
            how="left",
        )

        generation_export = (
            generation_long.pivot_table(
                index="datetime",
                columns="technology",
                values="forecast_mwh",
                aggfunc="sum",
            )
            .reset_index()
        )
        generation_export.columns.name = None
        complete_output = complete_output.merge(
            generation_export,
            on="datetime",
            how="left",
        )

        with st.expander(
            "Complete hourly demand → PBF → thermal gap → price output"
        ):
            st.dataframe(
                complete_output,
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Download complete market forecast CSV",
                data=complete_output.to_csv(
                    index=False
                ).encode("utf-8"),
                file_name=(
                    f"demand_pbf_thermal_gap_price_"
                    f"{forecast_target_day}.csv"
                ),
                mime="text/csv",
                use_container_width=True,
            )

    model_stats = forecast_result["model_stats"]
    baseline_stats = forecast_result["baseline_stats"]
    trend_baseline_stats = forecast_result["trend_baseline_stats"]

    q1, q2, q3, q4, q5 = st.columns(5)
    q1.metric(
        "Backtest model MAPE",
        f"{model_stats['mape']:,.2f}%",
    )
    q2.metric(
        "Trend-adjusted D-7 MAPE",
        f"{trend_baseline_stats['mape']:,.2f}%",
    )
    q3.metric(
        "Plain D-7 MAPE",
        f"{baseline_stats['mape']:,.2f}%",
    )
    q4.metric(
        "Backtest model MAE",
        f"{model_stats['mae']:,.0f} MW",
    )
    q5.metric(
        "Training observations",
        f"{forecast_result['training_rows']:,}",
    )

    st.markdown(
        f"""
        <div style="
            background:#F8FAFC;
            border:1px solid #E2E8F0;
            border-radius:10px;
            padding:10px 13px;
            margin:8px 0 12px 0;
            color:#475569;
            font-size:0.88rem;
            line-height:1.45;
        ">
          <b>MAPE</b> (Mean Absolute Percentage Error): average hourly absolute error as a percentage of real demand.
          A model MAPE of <b>{model_stats['mape']:,.2f}%</b> means that the hourly forecast differs from realised
          demand by approximately that percentage on average. <b>Plain D-7 MAPE</b> copies the curve from
          the same weekday one week earlier. <b>Trend-adjusted D-7 MAPE</b> first shifts that D-7 curve by the
          most recent same-weekday change observed between D-2 and D-9, blended with D-3 versus D-10.<br>
          <b>MAE</b> (Mean Absolute Error): average hourly absolute deviation in MW.
          A MAE of <b>{model_stats['mae']:,.0f} MW</b> means that the forecast was, on average,
          that many MW above or below realised demand. <b>Lower values are better</b>.
        </div>
        """,
        unsafe_allow_html=True,
    )

    model_vs_plain = baseline_stats["mape"] - model_stats["mape"]
    trend_vs_plain = baseline_stats["mape"] - trend_baseline_stats["mape"]

    message_parts = []
    if pd.notna(model_vs_plain):
        message_parts.append(
            f"Model vs plain D-7: {model_vs_plain:+,.2f} pp"
        )
    if pd.notna(trend_vs_plain):
        message_parts.append(
            f"Trend-adjusted D-7 vs plain D-7: {trend_vs_plain:+,.2f} pp"
        )

    if message_parts:
        if pd.notna(trend_vs_plain) and trend_vs_plain > 0:
            st.success(
                " | ".join(message_parts)
                + ". Positive values indicate a MAPE improvement."
            )
        else:
            st.info(
                " | ".join(message_parts)
                + ". Positive values indicate a MAPE improvement."
            )

    with st.expander("Backtest and methodology"):
        st.altair_chart(build_backtest_chart(forecast_result["backtest"]), use_container_width=True)
        st.markdown(
            """
            **Inputs used by the prototype**

            - Hour, weekday/weekend and annual seasonality.
            - National holidays and the adjacent days.
            - Forecast temperature, heating degrees and cooling degrees.
            - Demand for the same hour on D-2, D-3, D-7, D-9, D-10, D-14, D-21 and D-28.
            - Explicit recent weekly changes: D-2 minus D-9 and D-3 minus D-10.
            - Trend-adjusted D-7 curve:

              `D-7 adjusted = D-7 + alpha × [0.65 × (D-2 − D-9) + 0.35 × (D-3 − D-10)]`

            - Average same-hour demand over the previous four weeks.

            The validation is chronological over the latest 42 historical days.
            Historical realised weather is used in training, whereas the target day
            uses forecast weather. Therefore, the backtest is slightly optimistic
            because it does not include the full weather-forecast error.
            """
        )
        st.caption(
            f"Model: {forecast_result['model_name']}. Demand history used: "
            f"{forecast_result['history_start']} to {forecast_result['history_end']}."
        )

    export = forecast_df.rename(columns={
        "datetime": "Datetime",
        "forecast_mw": "Forecast demand MW",
        "p10_mw": "Forecast P10 MW",
        "p90_mw": "Forecast P90 MW",
        "temperature_c": "Forecast temperature °C",
        "lag_7d": "Same weekday previous week MW",
        "lag_2d": "D-2 actual MW",
        "lag_9d": "D-9 actual MW",
        "weekly_change_d2_vs_d9": "Weekly change D-2 minus D-9 MW",
        "weekly_change_d3_vs_d10": "Weekly change D-3 minus D-10 MW",
        "recent_weekly_trend_mw": "Blended recent weekly trend MW",
        "trend_adjusted_lag_7d": "Previous week plus recent trend MW",
    })
    st.download_button(
        "Download forecast CSV",
        data=export.to_csv(index=False).encode("utf-8"),
        file_name=f"peninsular_demand_forecast_{forecast_df['date'].iloc[0]}.csv",
        mime="text/csv",
    )
else:
    st.info(
        "Select a delivery date and press the button. Historical dates are "
        "recreated as an operational D-1 backtest; tomorrow uses the live "
        "weather forecast. When realised DA prices exist, Step 4 compares "
        "them directly against the forecast."
    )


# =========================================================
# PBF HOURLY MIX
# =========================================================
section_header("PBF hourly programmed-generation mix")

summary = pd.DataFrame()

if pbf_hourly.empty:
    st.warning(
        "ESIOS did not return hourly PBF technology data for the selected "
        "period. Check the token and selected dates."
    )
else:
    pbf_area_chart = build_pbf_hourly_area_chart(pbf_hourly)
    st.altair_chart(
        pbf_area_chart,
        use_container_width=True,
    )

    hours_with_data = max(
        int(pbf_hourly["datetime"].nunique()),
        1,
    )

    summary = (
        pbf_hourly.groupby(
            "technology",
            as_index=False,
        )["energy_mwh"]
        .sum()
        .rename(columns={"energy_mwh": "period_total_mwh"})
    )
    summary["period_total_gwh"] = (
        summary["period_total_mwh"] / 1_000.0
    )
    summary["average_programmed_gw"] = (
        summary["period_total_mwh"]
        / hours_with_data
        / 1_000.0
    )

    total_period_gwh = summary["period_total_gwh"].sum()
    total_average_gw = (
        summary["average_programmed_gw"].sum()
    )
    summary["share_pct"] = (
        summary["period_total_gwh"] / total_period_gwh
        if total_period_gwh != 0
        else pd.NA
    )

    summary = summary.sort_values(
        "average_programmed_gw",
        ascending=False,
    ).reset_index(drop=True)

    pbf_m1, pbf_m2, pbf_m3 = st.columns(3)

    pbf_m1.metric(
        "PBF generation in period",
        f"{total_period_gwh:,.1f} GWh",
    )
    pbf_m2.metric(
        "Average PBF programmed power",
        f"{total_average_gw:,.1f} GW",
    )
    pbf_m3.metric(
        "Hourly timestamps with PBF data",
        f"{hours_with_data:,}",
    )

    st.subheader(
        "Average hourly PBF composition during selected period"
    )

    average_chart = build_pbf_average_chart(summary)
    st.altair_chart(
        average_chart,
        use_container_width=True,
    )

    table = summary[
        [
            "technology",
            "average_programmed_gw",
            "period_total_gwh",
            "share_pct",
        ]
    ].rename(
        columns={
            "technology": "Technology",
            "average_programmed_gw": "Average programmed GW",
            "period_total_gwh": "Period total GWh",
            "share_pct": "PBF energy share",
        }
    )

    st.dataframe(
        table.style.format(
            {
                "Average programmed GW": "{:,.2f}",
                "Period total GWh": "{:,.1f}",
                "PBF energy share": "{:.1%}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "The stacked PBF chart is now hourly. Each ESIOS observation is "
        "requested with time_trunc=hour and time_agg=sum; hourly MWh are "
        "shown as average GW for that hour. The summary divides each "
        "technology's total MWh by the number of hourly timestamps. Imports, "
        "exports, pumping consumption and demand-side programme components "
        "remain excluded."
    )


# =========================================================
# RAW DATA DOWNLOAD
# =========================================================
with st.expander("Show / download test data"):
    tab_1, tab_2, tab_3 = st.tabs(
        [
            "Demand and temperature",
            "PBF hourly mix",
            "PBF average",
        ]
    )

    with tab_1:
        st.dataframe(
            combined,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download demand + temperature CSV",
            data=combined.to_csv(index=False).encode("utf-8"),
            file_name=(
                f"demand_temperature_{start_day}_{end_day}.csv"
            ),
            mime="text/csv",
        )

    with tab_2:
        pbf_export = pbf_hourly.copy()
        if not pbf_export.empty:
            pbf_export["average_power_gw"] = (
                pbf_export["energy_mwh"] / 1_000.0
            )

        st.dataframe(
            pbf_export,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download hourly PBF CSV",
            data=pbf_export.to_csv(index=False).encode("utf-8"),
            file_name=f"pbf_hourly_{start_day}_{end_day}.csv",
            mime="text/csv",
        )

    with tab_3:
        st.dataframe(
            summary,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download average PBF CSV",
            data=summary.to_csv(index=False).encode("utf-8"),
            file_name=f"pbf_average_{start_day}_{end_day}.csv",
            mime="text/csv",
        )
