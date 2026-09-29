import os
import re
import sys
import traceback
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
REDATA_REALTIME_DEMAND_URL = (
    "https://apidatos.ree.es/es/datos/demanda/demanda-tiempo-real"
)

# Official System Operator D+1 demand forecast. Indicator 1775 is the daily
# D+1 forecast snapshot intended for the day-ahead horizon. The public REData
# real-time forecast is used as a no-token fallback when available.
REE_D1_DEMAND_FORECAST_INDICATOR_ID = 1775
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

# Total programmed PBF demand: daily-market acquisitions plus physical
# bilateral acquisitions.
PBF_DEMAND_INDICATOR_ID = 10141

# Total physical bilateral sales in the PBF. At system level these sales have
# an equivalent acquisition side, so they are deducted from total PBF demand
# to estimate demand exposed to the day-ahead auction.
PBF_TOTAL_BILATERAL_SALES_INDICATOR_ID = 10235

DA_GAP_PERIMETER = (
    "DA residual demand minus non-bilateral low-marginal-cost PBF generation"
)

# No fixed 8.7/9 GW self-consumption plant is added to, or deducted from, the
# thermal gap. Installed power is not hourly generation. Any future explicit
# self-consumption adjustment must be an hourly production estimate and must
# be applied only to a demand curve whose published perimeter includes it.
EXPLICIT_SELF_CONSUMPTION_ADJUSTMENT_ENABLED = False

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

# Physical bilateral generation is deducted from total PBF generation by
# technology. The resulting series is the generation volume still exposed to
# the day-ahead market. The matching bilateral demand is deducted separately
# through indicator 10235.
FORECAST_BILATERAL_INDICATORS = {
    "Hydro UGH": [421],
    "Wind": [432, 433],
    "Solar PV": [],
    "Solar thermal": [],
    "Run-of-river": [422],
    "Nuclear": [424],
    "Other renewables": [10234],
}

# Exogenous / must-run generation deducted when calculating the structural
# thermal gap. Hydro UGH is deliberately excluded because it is dispatchable
# and reacts to prices and to the gap itself.
STRUCTURAL_THERMAL_GAP_TECHS = [
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Run-of-river",
    "Nuclear",
    "Other renewables",
]

FORECAST_NON_THERMAL_DEFAULT = STRUCTURAL_THERMAL_GAP_TECHS.copy()

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

# The forecast remains model-driven but is anchored in the latest complete
# non-bilateral PBF profile. Stable technologies receive a stronger anchor.
D1_DEMAND_BLEND_WEIGHT = 0.25
D1_GENERATION_BLEND_WEIGHT = 0.20

GENERATION_D1_BLEND_WEIGHTS = {
    "Nuclear": 0.80,
    "Other renewables": 0.65,
    "Run-of-river": 0.55,
    "Hydro UGH": 0.45,
    "Wind": 0.35,
    "Solar PV": 0.30,
    "Solar thermal": 0.35,
}


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
# V17: data integrity, fixed-run weather and model utilities
# =========================================================
FORECAST_ENGINE_VERSION = "v17_2_cow_fix"
OPEN_METEO_SINGLE_RUNS_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
FIXED_WEATHER_MODEL = "ecmwf_ifs"
PRICE_RECENCY_HALF_LIFE_DAYS = 90.0
PRICE_MODEL_ITERATIONS = 240
PRICE_CALIBRATION_DAYS = 28
PRICE_VALIDATION_DAYS = 28


def _v17_failure_details(exc: Exception, stage: str) -> dict:
    """Return diagnostics without dumping local variables or API credentials."""
    stack = "".join(traceback.TracebackException.from_exception(
        exc, capture_locals=False,
    ).format())
    message = str(exc)
    for name in ("ESIOS_TOKEN", "ESIOS_API_TOKEN"):
        secret = os.getenv(name, "").strip()
        if secret:
            stack = stack.replace(secret, "[REDACTED]")
            message = message.replace(secret, "[REDACTED]")
    return {
        "engine": FORECAST_ENGINE_VERSION,
        "stage": stage,
        "exception": type(exc).__name__,
        "message": message,
        "python": sys.version.split()[0],
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "streamlit": getattr(st, "__version__", "unknown"),
        "traceback": stack,
    }


def _v17_numeric(frame: pd.DataFrame, column: str, default=np.nan) -> pd.Series:
    value = frame[column] if column in frame else pd.Series(default, index=frame.index)
    return pd.to_numeric(value, errors="coerce").replace([np.inf, -np.inf], np.nan)


def _v17_day_grid(target_day: date) -> pd.DataFrame:
    """This hourly prototype deliberately refuses ambiguous DST delivery days."""
    start = pd.Timestamp(target_day, tz="Europe/Madrid")
    end = pd.Timestamp(target_day + timedelta(days=1), tz="Europe/Madrid")
    grid = pd.date_range(start, end, freq="h", inclusive="left")
    if len(grid) != 24:
        raise ValueError(
            "This 24-hour prototype cannot safely process a 23/25-hour DST day. "
            "Use a UTC-indexed 15-minute model for that delivery date."
        )
    return pd.DataFrame({"datetime": grid.tz_localize(None)})


def _v17_sample_weights(frame: pd.DataFrame, half_life: float = 90.0) -> np.ndarray:
    timestamps = pd.to_datetime(frame["datetime"])
    age = (timestamps.max() - timestamps).dt.total_seconds().to_numpy() / 86400.0
    return np.maximum(np.exp2(-age / half_life), 0.015)


def _v17_model(loss: str = "absolute_error", iterations: int = 240):
    return HistGradientBoostingRegressor(
        loss=loss, learning_rate=0.055, max_iter=iterations,
        max_leaf_nodes=25, min_samples_leaf=24,
        l2_regularization=6.0, early_stopping=False, random_state=42,
    )


def _v17_fit(model, x, y, weights=None):
    # Keep cloud CPU usage bounded; no random hold-out early stopping.
    from threadpoolctl import threadpool_limits
    with threadpool_limits(limits=2):
        if weights is None:
            model.fit(x, y)
        else:
            model.fit(x, y, sample_weight=weights)
    return model


def _v17_predict(model, x):
    from threadpoolctl import threadpool_limits
    with threadpool_limits(limits=2):
        return model.predict(x)


def _v17_weighted_quantile(values, quantiles, weights):
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    valid = np.isfinite(v) & np.isfinite(w) & (w > 0)
    if not valid.any():
        return np.full(len(np.atleast_1d(quantiles)), np.nan)
    v, w = v[valid], w[valid]
    order = np.argsort(v)
    v, w = v[order], w[order]
    cdf = (np.cumsum(w) - 0.5 * w) / w.sum()
    return np.interp(quantiles, cdf, v, left=v[0], right=v[-1])


@st.cache_data(show_spinner=False, ttl=3600)
def _v17_fixed_weather_payload(target_day: date, points_tuple: tuple, variables: tuple):
    """00 UTC D-1 run, not the latest run and not a rolling 24-hour lead series.

    Model initialisation is not publication time. The selected run normally has
    ample time to publish before a D-1 11:00 Madrid decision point, but historical
    actual publication timestamps still require an external vintage archive.
    No fallback to later/live weather is allowed if this run is missing.
    """
    _v17_day_grid(target_day)
    run_day = target_day - timedelta(days=1)
    params = {
        "latitude": ",".join(str(point[0]) for point in points_tuple),
        "longitude": ",".join(str(point[1]) for point in points_tuple),
        "hourly": ",".join(variables),
        "models": FIXED_WEATHER_MODEL,
        "run": f"{run_day.isoformat()}T00:00",
        "timezone": "Europe/Madrid",
        "forecast_days": 3,
    }
    errors = []
    for attempt in range(2):
        try:
            response = requests.get(OPEN_METEO_SINGLE_RUNS_URL, params=params, timeout=(15, 120))
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("error"):
                raise ValueError(str(payload.get("reason", "Weather API error")))
            return payload
        except (requests.RequestException, ValueError) as exc:
            errors.append(str(exc)[:200])
            if attempt == 0:
                sleep(1.0)
    raise ValueError(
        f"Fixed {FIXED_WEATHER_MODEL} run {run_day} 00 UTC unavailable. "
        "No later-run weather has been substituted. " + " | ".join(errors)
    )


def _v17_impute(train: pd.DataFrame, target: pd.DataFrame, features: list[str]):
    """Fit imputation on training only. Preserve missingness indicators upstream."""
    x = train.reindex(columns=features).apply(pd.to_numeric, errors="coerce")
    z = target.reindex(columns=features).apply(pd.to_numeric, errors="coerce")
    x = x.replace([np.inf, -np.inf], np.nan)
    z = z.replace([np.inf, -np.inf], np.nan)
    observed_columns = x.columns[x.notna().any(axis=0)]
    medians = pd.Series(0.0, index=x.columns)
    medians.loc[observed_columns] = x[observed_columns].median()
    return x.fillna(medians), z.fillna(medians)


def _v17_generation_features(frame: pd.DataFrame) -> list[str]:
    regional = [c for c in frame.columns if c.startswith("wx_")]
    extra = [
        "radiation_spatial_std", "radiation_spatial_max", "wind_spatial_std",
        "wind_cube_mean", "radiation_lag_1d", "wind_lag_1d",
        "gen_lag_1d_available", "weather_adjusted_d1_mwh",
    ]
    return list(dict.fromkeys(GENERATION_FEATURES + regional + extra))


def _v17_generation_anchor_features(frame: pd.DataFrame, technology: str,
                                    weather_lookup: dict, wind_lookup: dict) -> pd.DataFrame:
    out = frame.copy()
    out["gen_lag_1d_available"] = _v17_numeric(out, "gen_lag_1d").notna().astype(int)
    out["radiation_lag_1d"] = [
        weather_lookup.get((d - timedelta(days=1), int(h)), np.nan)
        for d, h in zip(out["date"], out["hour"])
    ]
    out["wind_lag_1d"] = [
        wind_lookup.get((d - timedelta(days=1), int(h)), np.nan)
        for d, h in zip(out["date"], out["hour"])
    ]
    if "run_d1_radiation" in out:
        out["radiation_lag_1d"] = _v17_numeric(out, "run_d1_radiation").combine_first(out["radiation_lag_1d"])
    if "run_d1_wind" in out:
        out["wind_lag_1d"] = _v17_numeric(out, "run_d1_wind").combine_first(out["wind_lag_1d"])
    anchor = _v17_numeric(out, "gen_lag_1d")
    if technology == "Solar PV":
        current = _v17_numeric(out, "shortwave_radiation")
        old = _v17_numeric(out, "radiation_lag_1d")
        ratio = (current / old.where(old > 30)).clip(0.0, 3.0)
        adjusted = anchor * ratio
        adjusted = adjusted.where(current > 2, 0.0)
    elif technology == "Wind":
        current = _v17_numeric(out, "wind_speed_100m")
        old = _v17_numeric(out, "wind_lag_1d")
        # Proxy only; spatial wind variables let the learned model improve it.
        ratio = ((current / old.where(old > 3)) ** 2).clip(0.0, 3.0)
        adjusted = anchor * ratio
    else:
        adjusted = anchor
    out["weather_adjusted_d1_mwh"] = adjusted
    return out


def _v17_generation_candidates(technology, frame, prediction):
    model = np.maximum(np.asarray(prediction, dtype=float), 0.0)
    raw_anchor = _v17_numeric(frame, "gen_lag_1d")
    if technology in {"Solar PV", "Wind"}:
        # Do not copy yesterday's solar/wind shape when today's weather changed.
        anchor = _v17_numeric(frame, "weather_adjusted_d1_mwh")
    else:
        anchor = raw_anchor
    # This array is changed below. With pandas Copy-on-Write, to_numpy()
    # can return a read-only view even after fillna(). Own a writable copy;
    # do not disable Copy-on-Write or modify shared pandas memory.
    anchor = anchor.fillna(pd.Series(model, index=frame.index)).to_numpy(
        dtype=float, copy=True,
    )
    if technology == "Solar PV":
        dark = _v17_numeric(frame, "shortwave_radiation").fillna(np.inf).to_numpy() <= 2.0
        model[dark] = 0.0
        anchor[dark] = 0.0
    return model, np.maximum(anchor, 0.0)


def _v17_choose_generation_weight(technology, truth, model, anchor):
    default = float(GENERATION_D1_BLEND_WEIGHTS.get(technology, 0.2))
    if len(truth) < 24 * 5:
        return default
    weights = [0.0, 0.15, 0.30, 0.50, 0.75, 1.0]
    y = np.asarray(truth, float)
    errors = [np.mean(np.abs(y - ((1 - w) * model + w * anchor))) for w in weights]
    best = int(np.argmin(errors))
    return float(weights[best])


def _v17_bridge_pbf_demand(target_day: date, physical_forecast: pd.DataFrame,
                          pbf_history: pd.DataFrame, physical_history: pd.DataFrame):
    """Learn the additive PBF-minus-physical perimeter difference through D-2.

    This is a statistical perimeter bridge, not an explicit forecast of exports,
    pumping or all market acquisition components. No component is added twice.
    """
    target = physical_forecast.copy()
    target["hour"] = pd.to_datetime(target["datetime"]).dt.hour
    cutoff = target_day - timedelta(days=2)
    hist = pbf_history.merge(physical_history, on="datetime", how="inner")
    hist["date"] = pd.to_datetime(hist["datetime"]).dt.date
    hist = hist[(hist["date"] <= cutoff) &
                (hist["date"] >= target_day - timedelta(days=120))].copy()
    hist["hour"] = pd.to_datetime(hist["datetime"]).dt.hour
    hist["dow"] = pd.to_datetime(hist["datetime"]).dt.dayofweek
    hist["offset"] = _v17_numeric(hist, "pbf_demand_mwh") - _v17_numeric(hist, "demand_mw")
    hist = hist.dropna(subset=["offset"])
    if hist["date"].nunique() < 14:
        raise ValueError("Fewer than 14 paired days to calibrate the PBF demand perimeter; not mixing physical demand with yesterday's PBF.")
    weekday = pd.Timestamp(target_day).dayofweek
    output, counts = [], []
    for hour in target["hour"]:
        candidates = hist[hist["hour"] == int(hour)].copy()
        same_dow = candidates[candidates["dow"] == weekday]
        if len(same_dow) >= 5:
            candidates = same_dow
        else:
            same_type = candidates[(candidates["dow"] >= 5) == (weekday >= 5)]
            if len(same_type) >= 5:
                candidates = same_type
        if candidates.empty:
            raise ValueError(f"No valid PBF perimeter observations at hour {hour}.")
        age = candidates["date"].map(lambda d: (target_day - d).days).to_numpy()
        w = np.exp2(-age / 30.0)
        output.append(float(_v17_weighted_quantile(candidates["offset"], [0.5], w)[0]))
        counts.append(len(candidates))
    target["pbf_perimeter_offset_mwh"] = output
    target["pbf_perimeter_observations"] = counts
    target["pbf_demand_forecast_mwh"] = (
        target["selected_demand_mw"] + target["pbf_perimeter_offset_mwh"]
    ).clip(lower=0.0)
    target["pbf_perimeter_method"] = "Physical forecast + recent weekday/hour PBF offset (through D-2)"
    return target

PRICE_COMPONENTS_V17 = [
    "pbf_demand_mwh", "bilateral_sales_mwh", "da_demand_mwh", "non_thermal_mwh",
    "solar_pv_mwh", "wind_mwh", "nuclear_mwh", "solar_thermal_mwh",
    "run_of_river_mwh", "other_renewables_mwh",
]
PRICE_EXTRA_FEATURES_V17 = [
    "negative_gap_mwh", "gap_signed_log", "gap_daily_mean", "gap_daily_min",
    "gap_daily_max", "gap_midday_mean", "gap_evening_mean", "gap_solar_vs_evening",
    "gap_ramp_forward", "gap_ramp_backward", "gap_daily_rank", "solar_share",
    "wind_share", "bilateral_share", "net_supply_share", "price_previous_day_mean",
    "price_previous_day_solar_mean", "price_previous_day_evening_mean",
    "price_anchor_available", "is_solar_block",
] + PRICE_COMPONENTS_V17 + [f"{c}_available" for c in PRICE_COMPONENTS_V17]


def _v17_price_feature_columns(frame):
    base = PRICE_FEATURES + PRICE_EXTRA_FEATURES_V17
    # V18 fundamentals only when they carry information in this frame.
    v18 = [c for c in V18_FEATURES if c in frame and pd.to_numeric(frame[c], errors="coerce").notna().any()]
    return list(dict.fromkeys(base + v18))


def _v17_fundamental_columns(frame):
    # Deliberately exclude all lagged prices and the price anchor. This candidate
    # can respond to a regime change instead of copying yesterday's price shape.
    all_features = _v17_price_feature_columns(frame)
    return [c for c in all_features if not (c.startswith("price_") or c == "same_hour_price_4w")]


def _v17_price_objective(frame, truth, prediction):
    y, p = np.asarray(truth, float), np.asarray(prediction, float)
    hourly_mae = float(np.mean(np.abs(p - y)))
    solar = frame["hour"].between(10, 17).to_numpy()
    solar_mae = float(np.mean(np.abs(p[solar] - y[solar]))) if solar.any() else hourly_mae
    temp = pd.DataFrame({"date": frame["date"].to_numpy(), "y": y, "p": p})
    daily = temp.groupby("date")[["y", "p"]].mean()
    baseload = float(np.mean(np.abs(daily["p"] - daily["y"])))
    spread_errors = []
    for _, day in temp.groupby("date"):
        if len(day) >= 23:
            spread_errors.append(abs(forecast_tb4(day["p"]) - forecast_tb4(day["y"])))
    tb4 = float(np.mean(spread_errors)) if spread_errors else 0.0
    return hourly_mae + 0.25 * solar_mae + 0.15 * baseload + 0.15 * tb4


def _v17_select_price_weights(frame, predictions: pd.DataFrame):
    """Sparse convex selection using only earlier labels; vectorised for cloud CPU."""
    count = len(predictions.columns)
    candidates = list(np.eye(count))
    for i in range(count):
        for j in range(i + 1, count):
            for w in (0.25, 0.5, 0.75):
                c = np.zeros(count)
                c[i], c[j] = w, 1 - w
                candidates.append(c)
    candidate_weights = np.asarray(candidates)
    all_predictions = predictions.to_numpy(float) @ candidate_weights.T
    y = frame["price_eur_mwh"].to_numpy(float)
    error = np.abs(all_predictions - y[:, None])
    solar = frame["hour"].between(10, 17).to_numpy()
    score = error.mean(axis=0)
    score += .25 * (error[solar].mean(axis=0) if solar.any() else error.mean(axis=0))
    base_errors, spread_errors = [], []
    dates = frame["date"].to_numpy()
    for day in pd.unique(dates):
        select = dates == day
        daily_prediction, daily_y = all_predictions[select], y[select]
        base_errors.append(np.abs(daily_prediction.mean(axis=0) - daily_y.mean()))
        if len(daily_y) >= 23:
            sorted_p = np.sort(daily_prediction, axis=0)
            sorted_y = np.sort(daily_y)
            spreads = sorted_p[-4:].mean(axis=0) - sorted_p[:4].mean(axis=0)
            real_spread = sorted_y[-4:].mean() - sorted_y[:4].mean()
            spread_errors.append(np.abs(spreads - real_spread))
    if base_errors:
        score += .15 * np.mean(base_errors, axis=0)
    if spread_errors:
        score += .15 * np.mean(spread_errors, axis=0)
    return candidate_weights[int(np.argmin(score))]


def _v17_candidate_price_forecasts(train, target):
    candidates = pd.DataFrame(index=target.index)
    empirical = empirical_similar_gap_price_reference(train, target, max_candidates=40)
    candidates["Similar-gap"] = empirical["conditional_price_median_eur_mwh"].to_numpy()
    candidates["Anchor"] = target["price_anchor"].to_numpy()
    if SKLEARN_AVAILABLE and len(train) >= 24 * 60:
        features = _v17_price_feature_columns(train)
        x, z = _v17_impute(train, target, features)
        w = _v17_sample_weights(train, PRICE_RECENCY_HALF_LIFE_DAYS)
        direct = _v17_fit(_v17_model(iterations=PRICE_MODEL_ITERATIONS), x, train["price_eur_mwh"], w)
        candidates["Direct-price ML"] = _v17_predict(direct, z)
        residual = _v17_fit(_v17_model(iterations=PRICE_MODEL_ITERATIONS), x,
                           train["price_eur_mwh"] - train["price_anchor"], w)
        candidates["Residual ML"] = target["price_anchor"].to_numpy() + _v17_predict(residual, z)
        features = _v17_fundamental_columns(train)
        x, z = _v17_impute(train, target, features)
        fundamental = _v17_fit(_v17_model(iterations=PRICE_MODEL_ITERATIONS), x, train["price_eur_mwh"], w)
        candidates["Fundamentals ML"] = _v17_predict(fundamental, z)
        # V18: same fundamentals on an asinh-scaled target. Squared loss in asinh
        # space tracks the level of normal hours without being dragged by spikes
        # or by the zero/negative solar block.
        forward, inverse = _v18_asinh_scale(train["price_eur_mwh"].to_numpy(float))
        features = _v17_price_feature_columns(train)
        x, z = _v17_impute(train, target, features)
        asinh_model = _v17_fit(_v17_model(loss="squared_error", iterations=PRICE_MODEL_ITERATIONS),
                               x, forward(train["price_eur_mwh"]), w)
        candidates["Asinh ML"] = inverse(_v17_predict(asinh_model, z))
    if len(train) >= 24 * V18_LEAR_MIN_DAYS:
        lear_features = [c for c in _v17_price_feature_columns(train)
                         if c not in {"hour", "hour_sin", "hour_cos", "is_solar_block"}]
        try:
            candidates["LEAR"] = _v18_lear_forecast(train, target, lear_features)
        except Exception:
            candidates["LEAR"] = candidates["Anchor"].to_numpy()
    if not np.isfinite(candidates.to_numpy()).all():
        raise ValueError("Non-finite candidate prices. Check input/anchor completeness.")
    return candidates, empirical


def build_post_clearing_gap_diagnostic(target_day: date, technologies: list[str],
                                      forecast_gap: pd.DataFrame, price_forecast: pd.DataFrame,
                                      token: str) -> pd.DataFrame:
    """Ex-post diagnosis ONLY. Never called by a forecasting/training function.

    'Observed' below means the published target-day PBF programme, not physical
    metered generation. These outcomes must not be used to improve that day's
    claimed ex-ante forecast.
    """
    observed_gen = load_forecast_pbf_history(target_day, target_day, tuple(technologies), token)
    observed_demand = load_pbf_demand_history(target_day, target_day, token)
    observed_bilateral = load_total_bilateral_sales_history(target_day, target_day, token)
    if observed_gen.empty or observed_demand.empty or observed_bilateral.empty:
        raise ValueError("Target-day PBF components have not all been published.")
    actual = build_previous_day_calibration(target_day + timedelta(days=1), technologies,
                                           observed_gen, observed_demand, observed_bilateral)
    if actual.empty:
        raise ValueError("No complete target-day PBF balance is available.")
    mapping = {
        "pbf_demand_mwh": "observed_pbf_demand_mwh", "bilateral_sales_mwh": "observed_bilateral_mwh",
        "non_thermal_net_mwh": "observed_non_thermal_mwh", "actual_thermal_gap_mwh": "observed_pbf_gap_mwh",
    }
    actual_keep = actual[["datetime", *mapping, *[t for t in technologies if t in actual]]].copy()
    actual_keep = actual_keep.rename(columns={**mapping, **{t: f"observed_{t}" for t in technologies}})
    out = forecast_gap.merge(actual_keep, on="datetime", how="inner")
    out["demand_contribution_to_gap_error_mw"] = out["pbf_demand_forecast_mwh"] - out["observed_pbf_demand_mwh"]
    out["bilateral_contribution_to_gap_error_mw"] = -(out["bilateral_sales_forecast_mwh"] - out["observed_bilateral_mwh"])
    out["generation_contribution_to_gap_error_mw"] = -(out["non_thermal_forecast_mwh"] - out["observed_non_thermal_mwh"])
    out["gap_error_mw"] = out["thermal_gap_forecast_mwh"] - out["observed_pbf_gap_mwh"]
    reconstructed = (out["demand_contribution_to_gap_error_mw"] + out["bilateral_contribution_to_gap_error_mw"]
                     + out["generation_contribution_to_gap_error_mw"])
    out["gap_reconciliation_error_mw"] = out["gap_error_mw"] - reconstructed
    for tech in technologies:
        if tech in out and f"observed_{tech}" in out:
            out[f"forecast_minus_observed_{tech}_mw"] = out[tech] - out[f"observed_{tech}"]
    price_columns = [c for c in ["datetime", "forecast_price_eur_mwh", "raw_model_price_eur_mwh", "fundamental_price_eur_mwh"] if c in price_forecast]
    out = out.merge(price_forecast[price_columns], on="datetime", how="left")
    actual_prices = load_esios_price_history(target_day, target_day, token)
    if not actual_prices.empty:
        out = out.merge(actual_prices.rename(columns={"price_eur_mwh": "cleared_price_eur_mwh"}), on="datetime", how="left")
        out["price_error_eur_mwh"] = out["forecast_price_eur_mwh"] - out["cleared_price_eur_mwh"]
    first = ["datetime", "thermal_gap_forecast_mwh", "observed_pbf_gap_mwh", "gap_error_mw",
             "demand_contribution_to_gap_error_mw", "bilateral_contribution_to_gap_error_mw",
             "generation_contribution_to_gap_error_mw", "forecast_price_eur_mwh", "cleared_price_eur_mwh", "price_error_eur_mwh"]
    columns = [c for c in first if c in out] + [c for c in out if c not in first]
    return out[columns].sort_values("datetime").reset_index(drop=True)


# =========================================================
# V18: ex-ante fundamentals, carbon, LEAR and bias correction
# =========================================================
# Why: the V17 price model explains price with a thermal gap built from PBF
# programmes. PBF solar/wind are *post-auction* and are curtailed when the
# price is <= 0, so in history the gap partly depends on the price itself, and
# at forecast time the gap comes from a different (forecast) source. V18 adds
# REE's sealed D+1 forecasts, which are ex-ante in history AND at forecast time
# (published ~10:50 D-1, never revised), so train and target see the same thing.
FORECAST_ENGINE_VERSION = "v18_1_daytype_anchor_bias"

# ESIOS "Previsión diaria D+1" series (sealed D-1, no post-publication revision).
# 1775 demand is already used above. Check 1777/1779 once in ESIOS: if either
# returns empty the model keeps running with an availability flag = 0.
REE_D1_WIND_FORECAST_INDICATOR_ID = 1777
REE_D1_PV_FORECAST_INDICATOR_ID = 1779

# EUA settlement prices from a local file you maintain (ICE/EEX daily settle),
# placed in the same data/ folder as the MIBGAS files. Columns: date, price.
EUA_LOCAL_FILE_PATTERNS = ["EUA_*.csv", "EUA_*.xlsx"]
CCGT_EFFICIENCY = 0.55          # MWh_e / MWh_th
CCGT_EMISSION_FACTOR = 0.37     # tCO2 / MWh_e

V18_BIAS_WINDOW_DAYS = 21   # per day type: ~15 workdays or ~6 off-days
V18_BIAS_MIN_DAYS = 3
V18_LEAR_MIN_DAYS = 90

V18_FEATURES = [
    "ree_d1_demand_mw", "ree_d1_wind_mw", "ree_d1_pv_mw", "ree_d1_residual_mw",
    "ree_d1_residual_daily_mean", "ree_d1_residual_daily_min",
    "ree_d1_residual_daily_max", "ree_d1_residual_rank",
    "ree_d1_residual_change_d1", "ree_d1_renewable_share", "ree_d1_available",
    "eua_d1_eur_t", "eua_available", "ccgt_srmc_eur_mwh",
    "ccgt_srmc_x_residual", "price_lag_same_type", "price_same_type_solar_mean",
]


def _v18_is_offday(day: date, holidays_set: set) -> bool:
    return day.weekday() >= 5 or day in holidays_set


def _v18_same_type_day(day: date, holidays_set: set):
    """Most recent earlier day of the same type, looking back at most 7 days.

    Working day -> last working day (Monday -> Friday).
    Saturday -> previous Saturday. Sunday/holiday -> last Sunday or holiday.
    """
    if day.weekday() == 5 and day not in holidays_set:
        return day - timedelta(days=7)
    offday = _v18_is_offday(day, holidays_set)
    for lag in range(1, 8):
        candidate = day - timedelta(days=lag)
        if offday:
            if candidate.weekday() == 6 or candidate in holidays_set:
                return candidate
        elif not _v18_is_offday(candidate, holidays_set):
            return candidate
    return None


@st.cache_data(show_spinner=False, ttl=3600)
def _v18_fetch_esios_hourly_average(indicator_id: int, start_day: date,
                                    end_day: date, _token: str) -> pd.DataFrame:
    """Hourly average MW of one ESIOS indicator, chunked like the PBF loader."""
    frames, chunk_start = [], start_day
    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=13))
        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(chunk_end + timedelta(days=1), tz="Europe/Madrid")
        params = {
            "start_date": start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time_trunc": "hour",
            "time_agg": "average",
        }
        for attempt in range(3):
            try:
                response = requests.get(f"{ESIOS_API_BASE}/{indicator_id}",
                                        headers=esios_headers(_token),
                                        params=params, timeout=(15, 120))
                response.raise_for_status()
                parsed = parse_esios_values(response.json())
                if not parsed.empty:
                    lower, upper = start_local.tz_localize(None), end_local.tz_localize(None)
                    frames.append(parsed[(parsed["datetime"] >= lower) & (parsed["datetime"] < upper)])
                break
            except requests.RequestException:
                sleep(1.5 * (attempt + 1))
        chunk_start = chunk_end + timedelta(days=1)
    if not frames:
        return pd.DataFrame(columns=["datetime", "value"])
    out = pd.concat(frames, ignore_index=True)
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return (out.dropna(subset=["datetime", "value"])
            .groupby("datetime", as_index=False)["value"].mean()
            .sort_values("datetime").reset_index(drop=True))


def load_ree_d1_fundamentals(start_day: date, end_day: date, token: str) -> pd.DataFrame:
    """REE sealed D+1 demand, wind and PV forecasts, hourly MW (ex-ante)."""
    series = {
        "ree_d1_demand_mw": REE_D1_DEMAND_FORECAST_INDICATOR_ID,
        "ree_d1_wind_mw": REE_D1_WIND_FORECAST_INDICATOR_ID,
        "ree_d1_pv_mw": REE_D1_PV_FORECAST_INDICATOR_ID,
    }
    grid = pd.DataFrame({"datetime": pd.date_range(pd.Timestamp(start_day),
                                                   pd.Timestamp(end_day + timedelta(days=1)),
                                                   freq="h", inclusive="left")})
    for column, indicator in series.items():
        try:
            data = _v18_fetch_esios_hourly_average(indicator, start_day, end_day, token)
        except Exception:
            data = pd.DataFrame(columns=["datetime", "value"])
        grid = grid.merge(data.rename(columns={"value": column}), on="datetime", how="left")
    return grid


def _v18_find_eua_file() -> Path | None:
    for directory in _mibgas_data_directories():
        for pattern in EUA_LOCAL_FILE_PATTERNS:
            matches = sorted(directory.glob(pattern)) if directory.exists() else []
            if matches:
                return matches[-1]
    return None


@st.cache_data(show_spinner=False, ttl=3600)
def load_eua_daily_lookup() -> tuple[dict, str]:
    """{settlement date: EUA EUR/t}. Missing file = empty lookup, never an error."""
    path = _v18_find_eua_file()
    if path is None:
        return {}, "EUA file not found (data/EUA_*.csv or .xlsx) - carbon features off"
    try:
        raw = pd.read_excel(path) if path.suffix.lower() == ".xlsx" else pd.read_csv(path)
        columns = {c: str(c).strip().lower() for c in raw.columns}
        raw = raw.rename(columns=columns)
        date_col = next(c for c in raw.columns if "date" in c or "fecha" in c)
        price_col = next(c for c in raw.columns if c != date_col and
                         pd.to_numeric(raw[c], errors="coerce").notna().mean() > 0.8)
        frame = pd.DataFrame({
            "date": pd.to_datetime(raw[date_col], errors="coerce", dayfirst=True).dt.date,
            "price": pd.to_numeric(raw[price_col], errors="coerce"),
        }).dropna()
        return dict(zip(frame["date"], frame["price"])), f"EUA from {path.name}"
    except Exception as exc:
        return {}, f"EUA file unreadable ({type(exc).__name__}) - carbon features off"


def _v18_last_settle(lookup: dict, delivery_day: date, max_back: int = 5) -> float:
    """Latest settle strictly before delivery day D (known before the D-1 auction)."""
    for lag in range(1, max_back + 1):
        value = lookup.get(delivery_day - timedelta(days=lag))
        if value is not None and np.isfinite(value):
            return float(value)
    return np.nan


def _v18_add_fundamental_features(frame: pd.DataFrame, eua_lookup: dict,
                                  ree_daily_mean: dict | None = None) -> pd.DataFrame:
    """Derived REE D+1 and carbon features. Expects REE columns already merged."""
    out = frame.copy()
    for column in ["ree_d1_demand_mw", "ree_d1_wind_mw", "ree_d1_pv_mw"]:
        out[column] = _v17_numeric(out, column)
    out["ree_d1_residual_mw"] = (out["ree_d1_demand_mw"] - out["ree_d1_wind_mw"].fillna(0)
                                 - out["ree_d1_pv_mw"].fillna(0))
    out.loc[out["ree_d1_demand_mw"].isna(), "ree_d1_residual_mw"] = np.nan
    out["ree_d1_available"] = out["ree_d1_residual_mw"].notna().astype(int)
    grouped = out.groupby("date")["ree_d1_residual_mw"]
    out["ree_d1_residual_daily_mean"] = grouped.transform("mean")
    out["ree_d1_residual_daily_min"] = grouped.transform("min")
    out["ree_d1_residual_daily_max"] = grouped.transform("max")
    out["ree_d1_residual_rank"] = grouped.rank(pct=True)
    # Previous-day residual comes from the full REE series, so the one-day
    # target frame gets the same value history would have had.
    daily_mean = ree_daily_mean if ree_daily_mean is not None else (
        out.groupby("date")["ree_d1_residual_mw"].mean().to_dict())
    out["ree_d1_residual_change_d1"] = out["ree_d1_residual_daily_mean"] - out["date"].map(
        lambda d: daily_mean.get(d - timedelta(days=1), np.nan))
    demand = out["ree_d1_demand_mw"].where(out["ree_d1_demand_mw"] > 0)
    out["ree_d1_renewable_share"] = (out["ree_d1_wind_mw"] + out["ree_d1_pv_mw"]) / demand
    out["eua_d1_eur_t"] = out["date"].map(lambda d: _v18_last_settle(eua_lookup, d))
    out["eua_available"] = out["eua_d1_eur_t"].notna().astype(int)
    gas = _v17_numeric(out, "mibgas_d1_eur_mwh").where(_v17_numeric(out, "mibgas_data_available") > 0)
    out["ccgt_srmc_eur_mwh"] = gas / CCGT_EFFICIENCY + CCGT_EMISSION_FACTOR * out["eua_d1_eur_t"].fillna(0)
    out["ccgt_srmc_x_residual"] = out["ccgt_srmc_eur_mwh"] * out["ree_d1_residual_mw"].clip(lower=0) / 10_000.0
    return out


def _v18_asinh_scale(y: np.ndarray):
    """epftoolbox-style variance stabilising transform for spiky/negative prices."""
    median = float(np.nanmedian(y))
    mad = float(np.nanmedian(np.abs(y - median))) * 1.4826
    mad = mad if mad > 1e-6 else 1.0
    forward = lambda v: np.arcsinh((np.asarray(v, float) - median) / mad)
    inverse = lambda z: np.sinh(np.asarray(z, float)) * mad + median
    return forward, inverse


def _v18_lear_forecast(train: pd.DataFrame, target: pd.DataFrame, features: list[str]) -> np.ndarray:
    """LEAR: one LASSO per delivery hour on asinh prices (Lago et al. benchmark)."""
    from sklearn.linear_model import LassoLarsIC
    from sklearn.preprocessing import StandardScaler
    x_all, z_all = _v17_impute(train, target, features)
    forward, inverse = _v18_asinh_scale(train["price_eur_mwh"].to_numpy(float))
    prediction = np.full(len(target), np.nan)
    for hour in range(24):
        tr = (train["hour"] == hour).to_numpy()
        te = (target["hour"] == hour).to_numpy()
        if not te.any():
            continue
        if tr.sum() < 30:
            prediction[te] = np.nanmedian(train.loc[tr, "price_eur_mwh"]) if tr.any() else np.nan
            continue
        scaler = StandardScaler().fit(x_all[tr])
        model = LassoLarsIC(criterion="aic", max_iter=500)
        model.fit(np.nan_to_num(scaler.transform(x_all[tr])), forward(train.loc[tr, "price_eur_mwh"]))
        prediction[te] = inverse(model.predict(np.nan_to_num(scaler.transform(z_all[te]))))
    return prediction


def _v18_day_types(frame: pd.DataFrame) -> np.ndarray:
    """'offday' for weekends and national holidays, else 'workday'."""
    weekend = _v17_numeric(frame, "is_weekend", 0).fillna(0).to_numpy() > 0
    holiday = _v17_numeric(frame, "is_holiday", 0).fillna(0).to_numpy() > 0
    return np.where(weekend | holiday, "offday", "workday")


def _v18_hourly_bias(validation: pd.DataFrame, prediction: np.ndarray, before_day: date,
                     day_type: str, window_days: int = V18_BIAS_WINDOW_DAYS) -> pd.Series:
    """Median signed error by hour over earlier days of the SAME day type.

    Mixing types hid the workday solar-hour bias (about -35 EUR/MWh in Sept
    2026) behind near-zero weekend errors.
    """
    frame = pd.DataFrame({"date": validation["date"].to_numpy(), "hour": validation["hour"].to_numpy(),
                          "type": _v18_day_types(validation),
                          "error": validation["price_eur_mwh"].to_numpy(float) - np.asarray(prediction, float)})
    frame = frame[(frame["date"] < before_day) & (frame["type"] == day_type)
                  & (frame["date"] >= before_day - timedelta(days=window_days))]
    if frame["date"].nunique() < V18_BIAS_MIN_DAYS:
        return pd.Series(0.0, index=range(24))
    return frame.groupby("hour")["error"].median().reindex(range(24)).fillna(0.0)


def _v18_bias_corrected(validation: pd.DataFrame, prediction: np.ndarray, shrink: float) -> np.ndarray:
    """Prequential correction: each day only sees earlier days' errors."""
    base = np.asarray(prediction, float)
    corrected = base.copy()
    types = _v18_day_types(validation)
    for day in sorted(validation["date"].unique()):
        mask = (validation["date"] == day).to_numpy()
        bias = _v18_hourly_bias(validation, base, day, types[mask][0])
        corrected[mask] = base[mask] + shrink * validation.loc[mask, "hour"].map(bias).to_numpy()
    return corrected


def _v18_choose_bias_shrink(validation: pd.DataFrame, prediction: np.ndarray) -> float:
    """Pick the shrink (0 = off) that minimised prequential MAE on validation."""
    y = validation["price_eur_mwh"].to_numpy(float)
    best_shrink, best_mae = 0.0, float(np.mean(np.abs(y - np.asarray(prediction, float))))
    for shrink in (0.25, 0.5, 0.75, 1.0):
        mae = float(np.mean(np.abs(y - _v18_bias_corrected(validation, prediction, shrink))))
        if mae < best_mae - 0.05:
            best_shrink, best_mae = shrink, mae
    return best_shrink


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
# DAILY DEMAND - REData
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
# DAILY TEMPERATURE - OPEN-METEO
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


@st.cache_data(show_spinner=False, ttl=900)
def load_ree_official_demand_forecast(
    target_day: date,
    _token: str,
) -> dict:
    """
    Load the official REE demand forecast for the selected delivery day.

    Priority:
    1. ESIOS indicator 1775: daily D+1 demand forecast.
    2. Public REData ``demanda-tiempo-real`` forecast series.

    The function never stops the app: an unavailable REE curve is returned as
    an empty dataframe and the proprietary model can still be executed.
    """
    expected = pd.DataFrame(
        {
            "datetime": pd.date_range(
                start=pd.Timestamp(target_day),
                periods=24,
                freq="h",
            )
        }
    )

    errors = []

    # -----------------------------------------------------
    # Source 1: ESIOS daily D+1 demand forecast, indicator 1775
    # -----------------------------------------------------
    try:
        start_local = pd.Timestamp(target_day, tz="Europe/Madrid")
        end_local = pd.Timestamp(
            target_day + timedelta(days=1),
            tz="Europe/Madrid",
        )
        response = requests.get(
            (
                f"{ESIOS_API_BASE}/"
                f"{REE_D1_DEMAND_FORECAST_INDICATOR_ID}"
            ),
            headers=esios_headers(_token),
            params={
                "start_date": start_local.tz_convert("UTC").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "end_date": end_local.tz_convert("UTC").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "time_trunc": "hour",
                "time_agg": "average",
            },
            timeout=90,
        )
        response.raise_for_status()
        parsed = parse_esios_values(response.json())

        if not parsed.empty:
            official = (
                parsed.groupby("datetime", as_index=False)["value"]
                .mean()
                .rename(columns={"value": "ree_forecast_mw"})
            )
            official = expected.merge(
                official,
                on="datetime",
                how="left",
            )
            official["ree_forecast_mw"] = pd.to_numeric(
                official["ree_forecast_mw"],
                errors="coerce",
            ).interpolate(limit_direction="both")

            if official["ree_forecast_mw"].notna().sum() >= 23:
                return {
                    "forecast": official,
                    "source": "ESIOS indicator 1775 - REE D+1 forecast",
                    "available": True,
                    "error": "",
                }
    except Exception as exc:
        errors.append(f"ESIOS 1775: {type(exc).__name__}: {exc}")

    # -----------------------------------------------------
    # Source 2: public REData real-time demand widget
    # -----------------------------------------------------
    try:
        response = requests.get(
            REDATA_REALTIME_DEMAND_URL,
            params={
                "start_date": f"{target_day.isoformat()}T00:00",
                "end_date": f"{target_day.isoformat()}T23:59",
                "time_trunc": "hour",
                "geo_trunc": "electric_system",
                "geo_limit": "peninsular",
                "geo_ids": "8741",
            },
            timeout=90,
        )
        response.raise_for_status()
        raw = parse_redata_hourly(response.json())

        if not raw.empty:
            normalised_title = (
                raw["title"]
                .astype(str)
                .str.lower()
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
            )
            forecast_rows = raw[
                normalised_title.str.contains(
                    "demanda prevista|forecast demand|forecast",
                    regex=True,
                    na=False,
                )
            ].copy()

            if not forecast_rows.empty:
                official = (
                    forecast_rows.groupby("datetime", as_index=False)["value"]
                    .mean()
                    .rename(columns={"value": "ree_forecast_mw"})
                )
                official["datetime"] = pd.to_datetime(
                    official["datetime"],
                    errors="coerce",
                ).dt.floor("h")
                official = expected.merge(
                    official,
                    on="datetime",
                    how="left",
                )
                official["ree_forecast_mw"] = pd.to_numeric(
                    official["ree_forecast_mw"],
                    errors="coerce",
                ).interpolate(limit_direction="both")

                if official["ree_forecast_mw"].notna().sum() >= 23:
                    return {
                        "forecast": official,
                        "source": (
                            "REData demanda-tiempo-real - "
                            "official REE forecast"
                        ),
                        "available": True,
                        "error": "",
                    }
    except Exception as exc:
        errors.append(f"REData: {type(exc).__name__}: {exc}")

    return {
        "forecast": expected.assign(ree_forecast_mw=np.nan),
        "source": "REE forecast unavailable",
        "available": False,
        "error": " | ".join(errors),
    }


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
def load_hourly_temperature_forecast(target_day: date, mode: str) -> pd.DataFrame:
    points = _temperature_points(mode)
    payload = _v17_fixed_weather_payload(
        target_day, tuple((p["latitude"], p["longitude"]) for p in points),
        ("temperature_2m",),
    )
    out = _weighted_hourly_temperature(payload, points)
    out = _v17_day_grid(target_day).merge(out, on="datetime", how="left")
    if out["temperature_c"].notna().sum() != 24:
        raise ValueError("Fixed-run temperature forecast has missing target hours.")
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
    actual, predicted = np.asarray(actual, float), np.asarray(predicted, float)
    valid = np.isfinite(actual) & np.isfinite(predicted)
    y, p = actual[valid], predicted[valid]
    if not len(y):
        return {"mae": np.nan, "rmse": np.nan, "mape": np.nan}
    error = p - y
    nonzero = np.abs(y) > 1e-6
    return {
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(np.sqrt(np.mean(error ** 2))),
        "mape": float(np.mean(np.abs(error[nonzero] / y[nonzero])) * 100) if nonzero.any() else np.nan,
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
    weather = load_hourly_temperature_history(history_start, target_day - timedelta(days=6), temperature_mode)
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
            max_leaf_nodes=31, min_samples_leaf=30, l2_regularization=8, early_stopping=False, random_state=42,
        )
        _v17_fit(validation_model, train[FORECAST_FEATURES], train["demand_mw"])
        validation_prediction = _v17_predict(validation_model, validation[FORECAST_FEATURES])

        final_model = HistGradientBoostingRegressor(
            loss="absolute_error", learning_rate=0.055, max_iter=300,
            max_leaf_nodes=31, min_samples_leaf=30, l2_regularization=8, early_stopping=False, random_state=42,
        )
        _v17_fit(final_model, model_data[FORECAST_FEATURES], model_data["demand_mw"])
        target_prediction = _v17_predict(final_model, target_data[FORECAST_FEATURES])
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
    band = alt.Chart(forecast).mark_area(
        opacity=0.16,
        color=CORP_GREEN,
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

    series_specs = [
        (
            "forecast_mw",
            "Model forecast",
            "#0F766E",
            [1, 0],
        ),
        (
            "ree_forecast_mw",
            "REE official D+1 forecast",
            "#22C55E",
            [4, 2],
        ),
        (
            "trend_adjusted_lag_7d",
            "Previous week + recent trend",
            "#F97316",
            [8, 3],
        ),
        (
            "lag_7d",
            "Same weekday previous week",
            "#64748B",
            [5, 3],
        ),
        (
            "lag_2d",
            "D-2 actual reference",
            "#93C5FD",
            [2, 2],
        ),
    ]

    frames = []
    domain = []
    colour_range = []
    dash_range = []

    for column, label, colour, dash in series_specs:
        if column not in forecast.columns:
            continue

        frame = forecast[["datetime", column]].copy()
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        )
        frame = frame.dropna(subset=[column])
        if frame.empty:
            continue

        frames.append(
            frame.rename(columns={column: "value"}).assign(
                series=label
            )
        )
        domain.append(label)
        colour_range.append(colour)
        dash_range.append(dash)

    if not frames:
        return configure_chart(band, height=430)

    lines = pd.concat(frames, ignore_index=True)

    chart = alt.Chart(lines).mark_line(strokeWidth=3).encode(
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
            title="Forecast series",
            scale=alt.Scale(
                domain=domain,
                range=colour_range,
            ),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                columns=3,
                labelLimit=380,
                titleLimit=240,
                symbolLimit=380,
            ),
        ),
        strokeDash=alt.StrokeDash(
            "series:N",
            legend=None,
            scale=alt.Scale(
                domain=domain,
                range=dash_range,
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
                "value:Q",
                title="Demand",
                format=",.0f",
            ),
        ],
    )
    return configure_chart(
        alt.layer(band, chart),
        height=430,
    )


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
# PBF DAILY MIX - ESIOS
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
                    chunk_lower = start_local.tz_localize(None)
                    chunk_upper = end_local.tz_localize(None)
                    parsed = parsed[(parsed["datetime"] >= chunk_lower) & (parsed["datetime"] < chunk_upper)]
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
# STEP 2 - FORECAST PBF GENERATION AND THERMAL GAP
# =========================================================
def _weighted_generation_weather(payload, variable_suffix: str = "") -> pd.DataFrame:
    """Preserve point-level weather and aggregate diagnostics for both train/target.

    Open-Meteo shortwave radiation is a preceding-hour mean. Shift it to the
    starting timestamp of the corresponding market energy interval.
    """
    if isinstance(payload, dict):
        payload = [payload]
    frames, regional = [], []
    for idx, item in enumerate(payload):
        if idx >= len(GENERATION_WEATHER_POINTS):
            break
        hourly = item.get("hourly", {}) or {}
        times = hourly.get("time", []) or []
        if not times:
            continue
        frame = pd.DataFrame({"datetime": pd.to_datetime(times, errors="coerce")})
        for variable in GENERATION_WEATHER_VARIABLES:
            values = hourly.get(f"{variable}{variable_suffix}", []) or []
            frame[variable] = pd.to_numeric(pd.Series(values), errors="coerce") if len(values) == len(times) else np.nan
        frame = frame.sort_values("datetime").reset_index(drop=True)
        frame["shortwave_radiation"] = frame["shortwave_radiation"].shift(-1)
        point = GENERATION_WEATHER_POINTS[idx]
        frame["weight"] = float(point["weight"])
        frame["point"] = idx
        frame["wind_cube"] = frame["wind_speed_100m"].clip(lower=0) ** 3
        frames.append(frame)
        reg = frame[["datetime", "shortwave_radiation", "cloud_cover", "wind_speed_100m"]].copy()
        reg = reg.rename(columns={
            "shortwave_radiation": f"wx_p{idx:02d}_radiation",
            "cloud_cover": f"wx_p{idx:02d}_cloud",
            "wind_speed_100m": f"wx_p{idx:02d}_wind",
        })
        regional.append(reg)
    if not frames:
        return pd.DataFrame(columns=["datetime"] + GENERATION_WEATHER_VARIABLES)
    long = pd.concat(frames, ignore_index=True)
    result = pd.DataFrame({"datetime": sorted(long["datetime"].dropna().unique())})
    for variable in GENERATION_WEATHER_VARIABLES + ["wind_cube"]:
        valid = long.dropna(subset=[variable]).copy()
        valid["weighted"] = valid[variable] * valid["weight"]
        agg = valid.groupby("datetime").agg(total=("weighted", "sum"), weight=("weight", "sum"))
        column = "wind_cube_mean" if variable == "wind_cube" else variable
        agg[column] = agg["total"] / agg["weight"]
        result = result.merge(agg[[column]], on="datetime", how="left")
    spread = long.groupby("datetime").agg(
        radiation_spatial_std=("shortwave_radiation", "std"),
        radiation_spatial_max=("shortwave_radiation", "max"),
        wind_spatial_std=("wind_speed_100m", "std"),
    )
    result = result.merge(spread, on="datetime", how="left")
    for reg in regional:
        reg = reg.groupby("datetime", as_index=False).mean(numeric_only=True)
        result = result.merge(reg, on="datetime", how="left")
    return result.sort_values("datetime").reset_index(drop=True)


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
def load_generation_weather_forecast(target_day: date) -> pd.DataFrame:
    payload = _v17_fixed_weather_payload(
        target_day,
        tuple((p["latitude"], p["longitude"]) for p in GENERATION_WEATHER_POINTS),
        tuple(GENERATION_WEATHER_VARIABLES),
    )
    weather = _weighted_generation_weather(payload)
    out = _v17_day_grid(target_day).merge(weather, on="datetime", how="left")
    prior = weather[pd.to_datetime(weather["datetime"]).dt.date == target_day - timedelta(days=1)].copy()
    prior["hour"] = pd.to_datetime(prior["datetime"]).dt.hour
    out["run_d1_radiation"] = out["datetime"].dt.hour.map(prior.groupby("hour")["shortwave_radiation"].mean())
    out["run_d1_wind"] = out["datetime"].dt.hour.map(prior.groupby("hour")["wind_speed_100m"].mean())
    for column in GENERATION_WEATHER_VARIABLES:
        if column not in out or out[column].notna().sum() != 24:
            raise ValueError(f"Fixed-run {column} forecast has missing target hours; no silent climatology substitution.")
    return out


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


def _generation_lags(frame: pd.DataFrame, lookup: dict) -> pd.DataFrame:
    out = frame.copy()
    # Critical fix: D-1 was requested by the model/anchor but never generated.
    for lag in [1, 2, 3, 7, 9, 10, 14, 21, 28]:
        out[f"gen_lag_{lag}d"] = [
            lookup.get((d - timedelta(days=lag), int(h)), np.nan)
            for d, h in zip(out["date"], out["hour"])
        ]
    out["gen_change_d2_d9"] = out["gen_lag_2d"] - out["gen_lag_9d"]
    out["gen_change_d3_d10"] = out["gen_lag_3d"] - out["gen_lag_10d"]
    out["gen_adjusted_d7"] = out["gen_lag_7d"] + 0.65 * out["gen_change_d2_d9"] + 0.35 * out["gen_change_d3_d10"]
    out["gen_same_hour_4w"] = out[["gen_lag_7d", "gen_lag_14d", "gen_lag_21d", "gen_lag_28d"]].mean(axis=1)
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
    Load the non-bilateral PBF programme by technology:

        generation exposed to DA = total PBF - bilateral PBF

    This is the low-marginal-cost generation volume that remains exposed to
    the day-ahead auction. Consistency requires deducting total bilateral sales
    from PBF demand as well; that adjustment is made separately below.
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
            if bilateral_ids:
                raise ValueError(f"Missing configured bilateral series for {technology}.")
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


@st.cache_data(show_spinner=False, ttl=3600)
def load_total_bilateral_sales_history(
    start_day: date,
    end_day: date,
    _token: str,
) -> pd.DataFrame:
    """
    Load the total PBF physical bilateral sales programme.

    At system level, physical bilateral sales have a matching acquisition side.
    This series is therefore used to remove bilateral demand from total PBF
    demand before estimating the volume exposed to the day-ahead auction.
    """
    bilateral = fetch_one_pbf_indicator_hourly(
        indicator_id=PBF_TOTAL_BILATERAL_SALES_INDICATOR_ID,
        start_day=start_day,
        end_day=end_day,
        token=_token,
    )
    if bilateral.empty:
        return pd.DataFrame(
            columns=["datetime", "bilateral_sales_mwh"]
        )

    out = bilateral.rename(
        columns={"energy_mwh": "bilateral_sales_mwh"}
    )[["datetime", "bilateral_sales_mwh"]].copy()

    out["datetime"] = pd.to_datetime(
        out["datetime"],
        errors="coerce",
    ).dt.floor("h")
    out["bilateral_sales_mwh"] = (
        pd.to_numeric(
            out["bilateral_sales_mwh"],
            errors="coerce",
        )
        .clip(lower=0.0)
    )

    return (
        out.dropna(subset=["datetime", "bilateral_sales_mwh"])
        .sort_values("datetime")
        .drop_duplicates("datetime", keep="last")
        .reset_index(drop=True)
    )


def forecast_total_bilateral_sales(
    target_day: date,
    bilateral_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    Forecast D+1 bilateral sales without using target-day information.

    Bilateral nominations are typically persistent by weekday and hour. The
    forecast combines D-1 with the same weekday in the previous four weeks.
    Missing lags are handled through a recent same-weekday/hour median.
    """
    expected = pd.DataFrame(
        {
            "datetime": pd.date_range(
                start=pd.Timestamp(target_day),
                periods=24,
                freq="h",
            )
        }
    )
    expected["date"] = expected["datetime"].dt.date
    expected["hour"] = expected["datetime"].dt.hour

    history = bilateral_history.copy()
    history["datetime"] = pd.to_datetime(
        history["datetime"],
        errors="coerce",
    )
    history["date"] = history["datetime"].dt.date
    history["hour"] = history["datetime"].dt.hour
    history["dow"] = history["datetime"].dt.dayofweek
    history["bilateral_sales_mwh"] = pd.to_numeric(
        history["bilateral_sales_mwh"],
        errors="coerce",
    )
    history = history.dropna(
        subset=["date", "hour", "bilateral_sales_mwh"]
    )

    lookup = {
        (row.date, int(row.hour)): float(row.bilateral_sales_mwh)
        for row in history.itertuples(index=False)
    }

    for lag in [1, 7, 14, 21, 28]:
        expected[f"bilateral_lag_{lag}d"] = [
            lookup.get(
                (delivery_date - timedelta(days=lag), int(hour)),
                np.nan,
            )
            for delivery_date, hour in zip(
                expected["date"],
                expected["hour"],
            )
        ]

    target_dow = pd.Timestamp(target_day).dayofweek
    recent_start = target_day - timedelta(days=70)
    recent = history[
        (history["date"] >= recent_start)
        & (history["date"] < target_day)
        & (history["dow"] == target_dow)
    ].copy()

    same_weekday_median = (
        recent.groupby("hour")["bilateral_sales_mwh"].median()
        if not recent.empty
        else pd.Series(dtype=float)
    )
    same_hour_median = (
        history[
            (history["date"] >= recent_start)
            & (history["date"] < target_day)
        ]
        .groupby("hour")["bilateral_sales_mwh"]
        .median()
    )

    expected["bilateral_same_weekday_median_mwh"] = (
        expected["hour"].map(same_weekday_median)
    )
    expected["bilateral_same_hour_median_mwh"] = (
        expected["hour"].map(same_hour_median)
    )

    weighted_columns = {
        "bilateral_lag_1d": 0.15,
        "bilateral_lag_7d": 0.40,
        "bilateral_lag_14d": 0.20,
        "bilateral_lag_21d": 0.10,
        "bilateral_lag_28d": 0.05,
        "bilateral_same_weekday_median_mwh": 0.10,
    }

    numerator = pd.Series(0.0, index=expected.index)
    denominator = pd.Series(0.0, index=expected.index)

    for column, weight in weighted_columns.items():
        values = pd.to_numeric(expected[column], errors="coerce")
        available = values.notna()
        numerator = numerator + values.fillna(0.0) * weight
        denominator = denominator + available.astype(float) * weight

    expected["bilateral_sales_forecast_mwh"] = (
        numerator
        / denominator.replace(0.0, np.nan)
    )
    expected["bilateral_sales_forecast_mwh"] = (
        expected["bilateral_sales_forecast_mwh"]
        .fillna(expected["bilateral_same_hour_median_mwh"])
        .fillna(0.0)
        .clip(lower=0.0)
    )

    return expected.sort_values("datetime").reset_index(drop=True)


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

    structural_columns = [
        tech
        for tech in STRUCTURAL_THERMAL_GAP_TECHS
        if tech in out.columns
    ]
    out["non_thermal_net_mwh"] = out[
        structural_columns
    ].sum(axis=1)

    # Recalculate the D-1 reference on the corrected structural perimeter.
    out["actual_thermal_gap_mwh"] = (
        out["pbf_demand_mwh"]
        - out["non_thermal_net_mwh"]
    )
    out["hydro_ugh_net_mwh"] = pd.to_numeric(
        out.get("Hydro UGH", 0.0),
        errors="coerce",
    ).fillna(0.0)

    return out.sort_values("datetime").reset_index(drop=True)


def build_previous_day_calibration(
    target_day: date,
    technologies: list[str],
    pbf_history: pd.DataFrame,
    pbf_demand_history: pd.DataFrame,
    bilateral_sales_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a D-1 reference using one consistent day-ahead market perimeter:

        DA residual demand
        = total PBF demand - total bilateral sales

        non-bilateral low-MC generation
        = total PBF generation - bilateral generation by technology

        DA structural thermal gap
        = DA residual demand - non-bilateral low-MC generation
    """
    calibration_day = target_day - timedelta(days=1)

    generation = pbf_history[
        pbf_history["datetime"].dt.date == calibration_day
    ].copy()
    demand = pbf_demand_history[
        pbf_demand_history["datetime"].dt.date == calibration_day
    ].copy()
    bilateral = bilateral_sales_history[
        bilateral_sales_history["datetime"].dt.date == calibration_day
    ].copy()

    if generation.empty or demand.empty or bilateral.empty:
        # The legacy embedded profile has no total bilateral-demand series and
        # is therefore not comparable with the corrected DA perimeter.
        return pd.DataFrame()

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

    out = (
        demand.merge(
            bilateral[["datetime", "bilateral_sales_mwh"]],
            on="datetime",
            how="inner",
        )
        .merge(
            generation_wide,
            on="datetime",
            how="inner",
        )
    )
    out["hour"] = out["datetime"].dt.hour

    structural_available = [
        tech
        for tech in technologies
        if tech != "Hydro UGH"
        and tech in out.columns
    ]
    out["non_thermal_net_mwh"] = out[
        structural_available
    ].sum(axis=1)

    out["hydro_ugh_net_mwh"] = (
        pd.to_numeric(
            out["Hydro UGH"],
            errors="coerce",
        ).fillna(0.0)
        if "Hydro UGH" in out.columns
        else 0.0
    )

    out["da_demand_mwh"] = (
        pd.to_numeric(out["pbf_demand_mwh"], errors="coerce")
        - pd.to_numeric(
            out["bilateral_sales_mwh"],
            errors="coerce",
        )
    ).clip(lower=0.0)

    out["actual_thermal_gap_mwh"] = (
        out["da_demand_mwh"]
        - out["non_thermal_net_mwh"]
    )
    out["calibration_source"] = (
        "ESIOS D-1 DA residual demand / non-bilateral PBF"
    )

    return out.sort_values("datetime").reset_index(drop=True)






def forecast_pbf_technology(technology: str, history: pd.DataFrame,
                            weather_history: pd.DataFrame, target_weather: pd.DataFrame):
    technology_history = history.loc[history["technology"] == technology, ["datetime", "energy_mwh"]].copy()
    technology_history = technology_history.sort_values("datetime")
    target_date = pd.to_datetime(target_weather["datetime"]).dt.date.min()
    # Defence in depth: even a caller supplying target/future observations is filtered.
    technology_history = technology_history[pd.to_datetime(technology_history["datetime"]).dt.date < target_date]
    history_weather = weather_history[pd.to_datetime(weather_history["datetime"]).dt.date < target_date].copy()
    lookup = _hourly_lookup(technology_history, "energy_mwh")
    radiation_lookup = _hourly_lookup(history_weather, "shortwave_radiation")
    wind_lookup = _hourly_lookup(history_weather, "wind_speed_100m")
    training = technology_history.merge(history_weather, on="datetime", how="inner")
    training = _generation_lags(_generation_weather_features(_market_calendar(training)), lookup)
    target = _generation_lags(_generation_weather_features(_market_calendar(target_weather)), lookup)
    training = _v17_generation_anchor_features(training, technology, radiation_lookup, wind_lookup)
    target = _v17_generation_anchor_features(target, technology, radiation_lookup, wind_lookup)
    training = training.dropna(subset=["energy_mwh", "datetime"]).sort_values("datetime").reset_index(drop=True)
    target = target.sort_values("datetime").reset_index(drop=True)
    if len(target) != 24:
        raise ValueError(f"Expected 24 complete target hours for {technology}.")
    # Raw D-1 remains untouched; imputation is only used inside model matrices.
    if target["gen_lag_1d"].notna().sum() == 0 and len(training) < 24 * 30:
        raise ValueError(f"No usable history or D-1 programme for {technology}.")
    features = _v17_generation_features(training)
    stats = {"mae": np.nan, "mape": np.nan}
    weight = float(GENERATION_D1_BLEND_WEIGHTS.get(technology, 0.20))
    model_name = "Available-lag fallback"
    if SKLEARN_AVAILABLE and len(training) >= 24 * 120:
        dates = sorted(training["date"].unique())
        cal_start, val_start = dates[-28], dates[-14]
        train = training[training["date"] < cal_start]
        calibration = training[(training["date"] >= cal_start) & (training["date"] < val_start)]
        validation = training[training["date"] >= val_start]
        x, z = _v17_impute(train, pd.concat([calibration, validation]), features)
        model = _v17_fit(_v17_model(iterations=220), x, train["energy_mwh"], _v17_sample_weights(train))
        preds = _v17_predict(model, z)
        cm, ca = _v17_generation_candidates(technology, calibration, preds[:len(calibration)])
        weight = _v17_choose_generation_weight(technology, calibration["energy_mwh"], cm, ca)
        vm, va = _v17_generation_candidates(technology, validation, preds[len(calibration):])
        stats = forecast_metrics(validation["energy_mwh"], (1 - weight) * vm + weight * va)
        # The weight was chosen before this 14-day validation, not on its outcomes.
        x, z = _v17_impute(training, target, features)
        final_model = _v17_fit(_v17_model(iterations=240), x, training["energy_mwh"], _v17_sample_weights(training))
        prediction = _v17_predict(final_model, z)
        model_name = "Spatial-weather boosting + calibrated D-1 anchor"
    else:
        reference = target[["gen_lag_1d", "gen_lag_2d", "gen_lag_7d", "gen_same_hour_4w"]].bfill(axis=1).iloc[:, 0]
        hourly = training.groupby("hour")["energy_mwh"].median()
        prediction = reference.fillna(target["hour"].map(hourly)).to_numpy(float)
        if not np.isfinite(prediction).all():
            raise ValueError(f"Unfilled generation forecast for {technology}; refusing fabricated zeros.")
    model_values, anchor = _v17_generation_candidates(technology, target, prediction)
    final = (1 - weight) * model_values + weight * anchor
    # A historical output quantile is not an installed-capacity limit. No cap to
    # 90-day output is imposed; alert on extrapolation rather than deleting it.
    output = target[["datetime", "date", "hour", "shortwave_radiation", "wind_speed_100m",
                     "gen_lag_1d", "gen_lag_2d", "gen_lag_7d", "gen_adjusted_d7",
                     "gen_lag_1d_available", "weather_adjusted_d1_mwh"]].copy()
    output["technology"] = technology
    output["model_net_forecast_mwh"] = model_values
    output["anchor_used_mwh"] = anchor
    output["anchor_weight"] = weight
    output["forecast_mwh"] = np.maximum(final, 0.0)
    output["generation_above_historical_max"] = final > float(training["energy_mwh"].max()) if len(training) else False
    return output, {
        "Technology": technology, "PBF perimeter": "Non-bilateral PBF generation",
        "Model": model_name, "D-1 blend weight": weight,
        "D-1 available hours": int(target["gen_lag_1d_available"].sum()),
        "D-1 average (MW)": float(target["gen_lag_1d"].mean()),
        "Forecast average (MW)": float(output["forecast_mwh"].mean()),
        "Validation MAE (MW)": stats["mae"], "Validation MAPE (%)": stats["mape"],
        "Validation scope": "14-day holdout; historical weather, not full operational weather uncertainty",
    }


def generate_thermal_gap_forecast(target_day: date, lookback_days: int,
                                  selected_demand: pd.DataFrame, technologies: list[str], token: str) -> dict:
    _v17_day_grid(target_day)
    history_start = target_day - timedelta(days=int(lookback_days) + 2)
    end = target_day - timedelta(days=1)
    pbf = load_forecast_pbf_history(history_start, end, tuple(technologies), token)
    demand_hist = load_pbf_demand_history(history_start, end, token)
    bilateral_hist = load_total_bilateral_sales_history(history_start, end, token)
    weather = load_generation_weather_history(history_start, target_day - timedelta(days=6))
    target_weather = load_generation_weather_forecast(target_day)
    for name, data in [("PBF", pbf), ("PBF demand", demand_hist), ("bilaterals", bilateral_hist), ("weather", weather)]:
        if data is None or data.empty:
            raise ValueError(f"Missing {name} history.")
    pbf = pbf[pd.to_datetime(pbf["datetime"]).dt.date < target_day].copy()
    demand_hist = demand_hist[pd.to_datetime(demand_hist["datetime"]).dt.date < target_day].copy()
    bilateral_hist = bilateral_hist[pd.to_datetime(bilateral_hist["datetime"]).dt.date < target_day].copy()
    calibration = build_previous_day_calibration(target_day, technologies, pbf, demand_hist, bilateral_hist)
    bilateral_forecast = forecast_total_bilateral_sales(target_day, bilateral_hist)
    frames, rows = [], []
    for technology in technologies:
        if pbf[pbf["technology"] == technology].empty:
            raise ValueError(f"Missing {technology} history. It cannot be silently omitted from the gap.")
        forecast, stats = forecast_pbf_technology(technology, pbf, weather, target_weather)
        frames.append(forecast)
        rows.append(stats)
    generation = pd.concat(frames, ignore_index=True)
    wide = generation.pivot(index="datetime", columns="technology", values="forecast_mwh").reset_index()
    wide.columns.name = None
    structural = [t for t in technologies if t != "Hydro UGH"]
    wide["non_thermal_forecast_mwh"] = wide[structural].sum(axis=1, min_count=len(structural))
    wide["hydro_ugh_forecast_mwh"] = _v17_numeric(wide, "Hydro UGH", 0.0)
    physical_history = load_hourly_peninsular_demand(target_day - timedelta(days=120), target_day - timedelta(days=2))
    bridge = _v17_bridge_pbf_demand(target_day, selected_demand, demand_hist, physical_history)
    forecast = wide.merge(bridge, on="datetime", how="inner")
    forecast = forecast.merge(bilateral_forecast[["datetime", "bilateral_sales_forecast_mwh", "bilateral_lag_1d", "bilateral_lag_7d"]], on="datetime", how="left")
    forecast["date"] = pd.to_datetime(forecast["datetime"]).dt.date
    forecast["hour"] = pd.to_datetime(forecast["datetime"]).dt.hour
    if forecast[["non_thermal_forecast_mwh", "bilateral_sales_forecast_mwh", "pbf_demand_forecast_mwh"]].isna().any().any():
        raise ValueError("Incomplete target gap components; not substituting missing values with zeros.")
    if (forecast["bilateral_sales_forecast_mwh"] > forecast["pbf_demand_forecast_mwh"]).any():
        raise ValueError("Forecast bilaterals exceed total PBF demand. Review source units/perimeters; no artificial 95% cap applied.")
    forecast["da_demand_forecast_mwh"] = forecast["pbf_demand_forecast_mwh"] - forecast["bilateral_sales_forecast_mwh"]
    forecast["thermal_gap_forecast_mwh"] = forecast["da_demand_forecast_mwh"] - forecast["non_thermal_forecast_mwh"]
    forecast["residual_gap_after_hydro_ugh_mwh"] = forecast["thermal_gap_forecast_mwh"] - forecast["hydro_ugh_forecast_mwh"]
    forecast["da_demand_share_of_total_pct"] = 100 * forecast["da_demand_forecast_mwh"] / forecast["pbf_demand_forecast_mwh"].replace(0, np.nan)
    forecast["explicit_self_consumption_adjustment_mwh"] = 0.0
    ref_columns = {
        "pbf_demand_mwh": "d1_total_pbf_demand_mwh", "bilateral_sales_mwh": "d1_bilateral_sales_mwh",
        "da_demand_mwh": "d1_da_demand_mwh", "actual_thermal_gap_mwh": "d1_actual_thermal_gap_mwh",
        "non_thermal_net_mwh": "d1_non_thermal_net_mwh",
    }
    if not calibration.empty:
        reference = calibration[["hour", *ref_columns, "calibration_source"]].drop_duplicates("hour").rename(columns=ref_columns)
        forecast = forecast.merge(reference, on="hour", how="left")
    else:
        for column in ref_columns.values():
            forecast[column] = np.nan
        forecast["calibration_source"] = "D-1 reference unavailable"
    forecast["d1_pbf_demand_mwh"] = forecast["d1_total_pbf_demand_mwh"]
    hist_wide = pbf.pivot_table(index="datetime", columns="technology", values="energy_mwh", aggfunc="sum").reindex(columns=technologies)
    hist_wide["non_thermal_mwh"] = hist_wide[structural].sum(axis=1, min_count=len(structural))
    hist_wide["hydro_ugh_mwh"] = _v17_numeric(hist_wide, "Hydro UGH", 0.0)
    historical = demand_hist.merge(bilateral_hist, on="datetime", how="inner").merge(hist_wide.reset_index(), on="datetime", how="inner")
    historical["da_demand_mwh"] = historical["pbf_demand_mwh"] - historical["bilateral_sales_mwh"]
    historical["thermal_gap_mwh"] = historical["da_demand_mwh"] - historical["non_thermal_mwh"]
    historical = historical.dropna(subset=["thermal_gap_mwh"])
    component_map = {"Solar PV": "solar_pv_mwh", "Wind": "wind_mwh", "Nuclear": "nuclear_mwh",
                     "Solar thermal": "solar_thermal_mwh", "Run-of-river": "run_of_river_mwh", "Other renewables": "other_renewables_mwh"}
    for tech, name in component_map.items():
        if tech in structural:
            historical[name] = historical[tech]
            forecast[name] = forecast[tech]
    # Canonical same-perimeter component fields consumed by the price engine.
    for source, name in {"pbf_demand_forecast_mwh": "pbf_demand_mwh", "bilateral_sales_forecast_mwh": "bilateral_sales_mwh",
                         "da_demand_forecast_mwh": "da_demand_mwh", "non_thermal_forecast_mwh": "non_thermal_mwh"}.items():
        forecast[name] = forecast[source]
    forecast["weather_run_utc"] = f"{end}T00:00Z"
    forecast["engine_version"] = FORECAST_ENGINE_VERSION
    return {"forecast": forecast.sort_values("datetime").reset_index(drop=True),
            "generation_long": generation.sort_values(["datetime", "technology"]),
            "generation_stats": pd.DataFrame(rows), "historical": historical.sort_values("datetime").reset_index(drop=True),
            "d1_calibration": calibration, "bilateral_sales_forecast": bilateral_forecast,
            "bilateral_sales_history": bilateral_hist, "gap_perimeter": DA_GAP_PERIMETER}


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
                title="Demand and non-bilateral PBF generation (MW)",
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

    # Physical / total PBF demand. This is the line users normally recognise as
    # system demand and avoids presenting the DA residual as if it were total
    # Spanish demand.
    total_demand_line = (
        alt.Chart(forecast)
        .mark_line(
            color="#111827",
            strokeWidth=3.2,
        )
        .encode(
            x="datetime:T",
            y=alt.Y(
                "pbf_demand_forecast_mwh:Q",
                title="Demand and non-bilateral PBF generation (MW)",
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip(
                    "pbf_demand_forecast_mwh:Q",
                    title="Total PBF demand",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "bilateral_sales_forecast_mwh:Q",
                    title="Bilateral programme",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "da_demand_forecast_mwh:Q",
                    title="DA residual demand",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "non_thermal_forecast_mwh:Q",
                    title="Non-bilateral low-MC PBF",
                    format=",.0f",
                ),
            ],
        )
    )

    # Demand exposed to the DA auction after deducting the bilateral programme.
    da_residual_line = (
        alt.Chart(forecast)
        .mark_line(
            color="#0F766E",
            strokeWidth=2.7,
            strokeDash=[8, 5],
        )
        .encode(
            x="datetime:T",
            y="da_demand_forecast_mwh:Q",
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip(
                    "da_demand_forecast_mwh:Q",
                    title="DA residual demand",
                    format=",.0f",
                ),
            ],
        )
    )

    return configure_chart(
        alt.layer(
            area,
            total_demand_line,
            da_residual_line,
        ).resolve_scale(y="shared"),
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
                    "pbf_demand_forecast_mwh:Q",
                    title="Total PBF demand",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "bilateral_sales_forecast_mwh:Q",
                    title="Bilateral demand",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "da_demand_forecast_mwh:Q",
                    title="DA residual demand",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "non_thermal_forecast_mwh:Q",
                    title="Non-bilateral low-MC PBF",
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
                    title="D-1 actual DA residual gap",
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
# MIBGAS GDAES D+1 - DAILY GAS INPUT
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
    def parse(value):
        if pd.isna(value):
            return np.nan
        text = str(value).strip().replace("€", "").replace("\xa0", "").replace(" ", "")
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            text = text.replace(",", ".")
        return pd.to_numeric(text, errors="coerce")
    return series.map(parse)


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
# STEP 3 - FORECAST DA SPOT PRICE
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
                    chunk_lower = start_local.tz_localize(None)
                    chunk_upper = end_local.tz_localize(None)
                    parsed = parsed[(parsed["datetime"] >= chunk_lower) & (parsed["datetime"] < chunk_upper)]
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


def _price_features(frame: pd.DataFrame, price_lookup: dict, gap_lookup: dict,
                    gas_price_lookup: dict) -> pd.DataFrame:
    out = _market_calendar(frame).sort_values("datetime").reset_index(drop=True)
    for lag in [1, 2, 7, 14, 21, 28]:
        out[f"price_lag_{lag}d"] = [price_lookup.get((d - timedelta(days=lag), int(h)), np.nan)
                                       for d, h in zip(out["date"], out["hour"])]
    for lag in [1, 2, 7]:
        out[f"gap_lag_{lag}d"] = [gap_lookup.get((d - timedelta(days=lag), int(h)), np.nan)
                                     for d, h in zip(out["date"], out["hour"])]
    out["price_lag_1d"] = out["price_lag_1d"].fillna(out["price_lag_2d"])
    out["gap_lag_1d"] = out["gap_lag_1d"].fillna(out["gap_lag_2d"])
    weekly = out[["price_lag_7d", "price_lag_14d", "price_lag_21d", "price_lag_28d"]]
    available_week = weekly.notna().any(axis=1)
    out["same_hour_price_4w"] = np.nan
    out.loc[available_week, "same_hour_price_4w"] = weekly.loc[available_week].median(axis=1)
    # V18.1: the short-lag term of the anchor uses the most recent day of the
    # SAME day type. A Monday used to lean 35% on Sunday, whose solar valley is
    # structurally deeper; it now uses Friday (or the last working day).
    years = sorted({d.year for d in out["date"]} | {(d - timedelta(days=8)).year for d in out["date"]})
    holidays_set = national_holidays(years)
    out["price_lag_same_type"] = [
        price_lookup.get((_v18_same_type_day(d, holidays_set), int(h)), np.nan)
        if _v18_same_type_day(d, holidays_set) is not None else np.nan
        for d, h in zip(out["date"], out["hour"])
    ]
    out["price_lag_same_type"] = out["price_lag_same_type"].fillna(out["price_lag_1d"])
    same_type_solar = {}
    for day in out["date"].unique():
        ref = _v18_same_type_day(day, holidays_set)
        vals = np.asarray([price_lookup.get((ref, h), np.nan) for h in range(10, 18)], float) if ref else np.array([np.nan])
        same_type_solar[day] = float(np.nanmean(vals)) if np.isfinite(vals).any() else np.nan
    out["price_same_type_solar_mean"] = out["date"].map(same_type_solar)
    anchor_values = out[["price_lag_7d", "price_lag_same_type", "same_hour_price_4w"]]
    weights = np.array([0.45, 0.35, 0.20])
    denominator = anchor_values.notna().to_numpy() @ weights
    out["price_anchor"] = (anchor_values.fillna(0).to_numpy() @ weights) / np.where(denominator > 0, denominator, np.nan)
    out["price_anchor_available"] = (denominator > 0).astype(int)
    gap = _v17_numeric(out, "thermal_gap_mwh")
    out["positive_gap"] = gap.clip(lower=0)
    out["negative_gap_mwh"] = gap.clip(upper=0)
    out["gap_signed_log"] = np.sign(gap) * np.log1p(gap.abs() / 1000.0)
    out["gap_sq_scaled"] = (out["positive_gap"] / 10000.0) ** 2
    out["gap_change_d1"] = gap - out["gap_lag_1d"]
    out["gap_change_d7"] = gap - out["gap_lag_7d"]
    # Whole-day *forecast* geometry is available at the target decision point.
    # During module validation these are observed historical PBF gaps, so that
    # validation is explicitly conditional, not a full operational backtest.
    for stat in ["mean", "min", "max"]:
        out[f"gap_daily_{stat}"] = out.groupby("date")["thermal_gap_mwh"].transform(stat)
    solar = out["hour"].between(10, 17)
    evening = out["hour"].between(18, 22)
    out["is_solar_block"] = solar.astype(int)
    out["gap_midday_mean"] = out["date"].map(out[solar].groupby("date")["thermal_gap_mwh"].mean())
    out["gap_evening_mean"] = out["date"].map(out[evening].groupby("date")["thermal_gap_mwh"].mean())
    out["gap_solar_vs_evening"] = out["gap_evening_mean"] - out["gap_midday_mean"]
    out["gap_ramp_forward"] = out.groupby("date")["thermal_gap_mwh"].shift(-1) - gap
    out["gap_ramp_backward"] = gap - out.groupby("date")["thermal_gap_mwh"].shift(1)
    out["gap_daily_rank"] = out.groupby("date")["thermal_gap_mwh"].rank(pct=True)
    for c in PRICE_COMPONENTS_V17:
        out[c] = _v17_numeric(out, c)
        out[f"{c}_available"] = out[c].notna().astype(int)
    base = out["pbf_demand_mwh"].where(out["pbf_demand_mwh"] > 0)
    out["solar_share"] = out["solar_pv_mwh"] / base
    out["wind_share"] = out["wind_mwh"] / base
    out["bilateral_share"] = out["bilateral_sales_mwh"] / base
    out["net_supply_share"] = out["non_thermal_mwh"] / base
    for name, hours in [("mean", range(24)), ("solar_mean", range(10, 18)), ("evening_mean", range(18, 23))]:
        lookup = {}
        for day in out["date"].unique():
            vals = [price_lookup.get((day - timedelta(days=1), int(h)), np.nan) for h in hours]
            valid = np.asarray(vals, float)
            lookup[day] = float(np.nanmean(valid)) if np.isfinite(valid).any() else np.nan
        out[f"price_previous_day_{name}"] = out["date"].map(lookup)
    return add_mibgas_features(out, gas_price_lookup)


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



def empirical_similar_gap_price_reference(historical_data: pd.DataFrame, target_data: pd.DataFrame,
                                         max_candidates: int = 40) -> pd.DataFrame:
    history = historical_data.dropna(subset=["thermal_gap_mwh", "price_eur_mwh", "datetime"]).copy()
    if history.empty:
        raise ValueError("No complete comparable gap/price observations.")
    cols = ["thermal_gap_mwh", "gap_daily_mean", "gap_midday_mean", "gap_solar_vs_evening",
            "pbf_demand_mwh", "solar_pv_mwh", "wind_mwh", "nuclear_mwh",
            "mibgas_d1_eur_mwh", "hour_sin", "hour_cos", "doy_sin", "doy_cos", "is_weekend"]
    cols = [c for c in cols if c in history and c in target_data and history[c].notna().any()]
    matrix = history[cols].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    scale = np.nanquantile(matrix, .75, axis=0) - np.nanquantile(matrix, .25, axis=0)
    minimum_scale = np.array([1500.0 if c.endswith("mwh") or c.startswith("gap_") else (8.0 if c == "mibgas_d1_eur_mwh" else .5) for c in cols])
    scale = np.maximum(np.nan_to_num(scale, nan=1.0), minimum_scale)
    importance = np.array([3.0 if c == "thermal_gap_mwh" else (0.5 if c.startswith("doy_") else 1.0) for c in cols])
    prices = history["price_eur_mwh"].to_numpy(float)
    dates = pd.to_datetime(history["datetime"]).dt.normalize().to_numpy(dtype="datetime64[D]")
    rows = []
    for _, target in target_data.iterrows():
        day = np.datetime64(pd.Timestamp(target["datetime"]).date(), "D")
        age = (day - dates).astype(int)
        valid_history = (age >= 1) & (age <= 550)
        x = pd.to_numeric(target[cols], errors="coerce").to_numpy(float)
        known = np.isfinite(matrix) & np.isfinite(x)
        component = np.where(known, ((matrix - x) / scale) ** 2, 0.0)
        normalizer = (known * importance).sum(axis=1)
        distance = (component * importance).sum(axis=1) / np.maximum(normalizer, 1e-9)
        # Missing gas is not a real zero-priced gas observation.
        if "mibgas_data_available" in history and float(target.get("mibgas_data_available", 0)) > 0:
            distance += (history["mibgas_data_available"].to_numpy() == 0) * .5
        distance += age.clip(min=0) / 365.0 * .12
        distance[~valid_history | (normalizer == 0)] = np.inf
        ids = np.flatnonzero(np.isfinite(distance))
        if not len(ids):
            raise ValueError("No strictly earlier analogue observations.")
        k = min(int(max_candidates), len(ids))
        ids = ids[np.argpartition(distance[ids], k - 1)[:k]]
        d = np.sqrt(distance[ids])
        w = np.exp(-2.5 * (d - d.min())) * np.exp2(-age[ids] / 120.0)
        q = _v17_weighted_quantile(prices[ids], [.10, .25, .5, .75, .90], w)
        rows.append({
            "conditional_price_p10_eur_mwh": q[0], "conditional_price_p25_eur_mwh": q[1],
            "conditional_price_median_eur_mwh": q[2], "conditional_price_p75_eur_mwh": q[3],
            "conditional_price_p90_eur_mwh": q[4],
            "probability_price_le_zero_pct": float(np.average(prices[ids] <= 0, weights=w) * 100),
            "probability_price_below_5_pct": float(np.average(prices[ids] <= 5, weights=w) * 100),
            "similar_gap_observations": k,
            "similar_gap_effective_observations": float(w.sum() ** 2 / np.sum(w ** 2)),
            "closest_analogue_distance": float(d.min()),
        })
    return pd.DataFrame(rows, index=target_data.index)












def generate_price_forecast(target_day: date, historical_gap: pd.DataFrame,
                            forecast_gap: pd.DataFrame, token: str) -> dict:
    _v17_day_grid(target_day)
    historical = historical_gap.copy()
    historical["datetime"] = pd.to_datetime(historical["datetime"])
    historical = historical[historical["datetime"].dt.date < target_day].copy()
    prices = load_esios_price_history(historical["datetime"].min().date(), target_day - timedelta(days=1), token)
    prices = prices[pd.to_datetime(prices["datetime"]).dt.date < target_day].copy()
    if prices.empty:
        raise ValueError("No strictly earlier spot-price history.")
    gas_actuals, gas_message = load_mibgas_gdaes_actuals()
    gas_lookup, gas_source = build_mibgas_daily_lookup(gas_actuals,
        historical["datetime"].min().date() - timedelta(days=35), target_day - timedelta(days=1))
    price_lookup = _hourly_lookup(prices, "price_eur_mwh")
    gap_lookup = _hourly_lookup(historical, "thermal_gap_mwh")
    # V18: ex-ante REE D+1 fundamentals for history and target, plus carbon.
    ree_d1 = load_ree_d1_fundamentals(historical["datetime"].min().date(), target_day, token)
    eua_lookup, eua_message = load_eua_daily_lookup()
    ree_residual = (ree_d1["ree_d1_demand_mw"] - ree_d1["ree_d1_wind_mw"].fillna(0)
                    - ree_d1["ree_d1_pv_mw"].fillna(0)).where(ree_d1["ree_d1_demand_mw"].notna())
    ree_daily_mean = ree_residual.groupby(ree_d1["datetime"].dt.date).mean().dropna().to_dict()
    historical = historical.drop(columns=[c for c in ree_d1 if c != "datetime" and c in historical]).merge(
        ree_d1, on="datetime", how="left")
    training = _price_features(historical.merge(prices, on="datetime", how="inner"), price_lookup, gap_lookup, gas_lookup)
    training = _v18_add_fundamental_features(training, eua_lookup, ree_daily_mean)
    target_frame = forecast_gap.copy()
    target_frame["thermal_gap_mwh"] = target_frame["thermal_gap_forecast_mwh"]
    target_frame["datetime"] = pd.to_datetime(target_frame["datetime"])
    target_frame = target_frame.drop(columns=[c for c in ree_d1 if c != "datetime" and c in target_frame]).merge(
        ree_d1, on="datetime", how="left")
    target = _price_features(target_frame, price_lookup, gap_lookup, gas_lookup)
    target = _v18_add_fundamental_features(target, eua_lookup, ree_daily_mean)
    training = training.dropna(subset=["price_eur_mwh", "price_anchor", "thermal_gap_mwh"]).reset_index(drop=True)
    target = target.reset_index(drop=True)
    if len(training) < 24 * 120:
        raise ValueError("Fewer than 120 complete days for the conditional price model.")
    if len(target) != 24 or target[["price_anchor", "thermal_gap_mwh"]].isna().any().any():
        raise ValueError("Incomplete target price anchors or gaps.")
    dates = sorted(training["date"].unique())
    cal_start = dates[-(PRICE_CALIBRATION_DAYS + PRICE_VALIDATION_DAYS)]
    val_start = dates[-PRICE_VALIDATION_DAYS]
    before_cal = training[training["date"] < cal_start].reset_index(drop=True)
    calibration = training[(training["date"] >= cal_start) & (training["date"] < val_start)].reset_index(drop=True)
    validation = training[training["date"] >= val_start].reset_index(drop=True)
    calibration_candidates, _ = _v17_candidate_price_forecasts(before_cal, calibration)
    before_val = training[training["date"] < val_start].reset_index(drop=True)
    validation_candidates, _ = _v17_candidate_price_forecasts(before_val, validation)
    candidate_names = calibration_candidates.columns.tolist()
    validation_candidates = validation_candidates[candidate_names]
    pool_frame, pool_candidates = calibration.copy(), calibration_candidates.copy()
    validation_prediction = pd.Series(index=validation.index, dtype=float)
    # Prequential meta-selection: each validation day's weights see only earlier
    # dates. The base estimators are frozen at the start of the validation block.
    for day in sorted(validation["date"].unique()):
        mask = validation["date"] == day
        cutoff = day - timedelta(days=PRICE_CALIBRATION_DAYS)
        use = (pool_frame["date"] < day) & (pool_frame["date"] >= cutoff)
        weights = _v17_select_price_weights(pool_frame.loc[use].reset_index(drop=True), pool_candidates.loc[use].reset_index(drop=True))
        validation_prediction.loc[mask] = validation_candidates.loc[mask].to_numpy() @ weights
        pool_frame = pd.concat([pool_frame, validation.loc[mask]], ignore_index=True)
        pool_candidates = pd.concat([pool_candidates, validation_candidates.loc[mask]], ignore_index=True)
    use = pool_frame["date"] >= target_day - timedelta(days=PRICE_CALIBRATION_DAYS + 2)
    if use.sum() < 24 * 7:
        use = pd.Series(True, index=pool_frame.index)
    weights = _v17_select_price_weights(pool_frame.loc[use].reset_index(drop=True), pool_candidates.loc[use].reset_index(drop=True))
    target_candidates, empirical = _v17_candidate_price_forecasts(training, target)
    target_candidates = target_candidates[candidate_names]
    final = target_candidates.to_numpy() @ weights
    # V18: prequential hourly bias correction. The shrink is chosen on the
    # validation block (0 = off if it does not reduce MAE), then applied with
    # the median hourly error of earlier days of the same day type (workday or
    # weekend/holiday) within the last V18_BIAS_WINDOW_DAYS before D.
    raw_validation = validation_prediction.to_numpy(float)
    bias_shrink = _v18_choose_bias_shrink(validation, raw_validation)
    if bias_shrink > 0:
        validation_prediction = pd.Series(_v18_bias_corrected(validation, raw_validation, bias_shrink),
                                          index=validation.index)
        target_type = _v18_day_types(target)[0]
        target_bias = _v18_hourly_bias(validation, raw_validation, target_day, target_type)
        final = final + bias_shrink * target["hour"].map(target_bias).to_numpy()
    if not np.isfinite(final).all():
        raise ValueError("Price model produced non-finite outputs.")
    output_columns = ["datetime", "date", "hour", "thermal_gap_mwh", "price_anchor", "price_lag_1d", "price_lag_7d", "same_hour_price_4w",
                      "mibgas_d1_eur_mwh", "mibgas_d2_eur_mwh", "mibgas_d7_eur_mwh", "mibgas_7d_avg_eur_mwh",
                      "mibgas_change_d1_eur_mwh", "mibgas_change_vs_7d_eur_mwh", "mibgas_data_available", "mibgas_x_positive_gap"]
    output = target[output_columns].copy()
    output["forecast_price_eur_mwh"] = final
    output["raw_model_price_eur_mwh"] = target_candidates.get("Direct-price ML", target_candidates["Similar-gap"]).to_numpy()
    output["fundamental_price_eur_mwh"] = target_candidates.get("Fundamentals ML", target_candidates["Similar-gap"]).to_numpy()
    output["residual_model_price_eur_mwh"] = target_candidates.get("Residual ML", target_candidates["Anchor"]).to_numpy()
    output["negative_thermal_gap_flag"] = output["thermal_gap_mwh"] < 0
    output["guardrail_applied"] = False  # retained for old exporters, no price guardrails used
    output["price_upper_guardrail_eur_mwh"] = np.nan
    output["engine_version"] = FORECAST_ENGINE_VERSION
    for c in empirical:
        output[c] = empirical[c].to_numpy()
    errors = validation["price_eur_mwh"].to_numpy() - validation_prediction.to_numpy()
    residuals = pd.DataFrame({"hour": validation["hour"], "error": errors})
    hourly = residuals.groupby("hour")["error"].quantile([.10, .90]).unstack()
    output["conditional_error_p10_eur_mwh"] = output["hour"].map(hourly[.10]).fillna(np.quantile(errors, .10))
    output["conditional_error_p90_eur_mwh"] = output["hour"].map(hourly[.90]).fillna(np.quantile(errors, .90))
    output["forecast_p10_eur_mwh"] = final + output["conditional_error_p10_eur_mwh"]
    output["forecast_p90_eur_mwh"] = final + output["conditional_error_p90_eur_mwh"]
    # Enclose the point forecast. These are indicative conditional residual bands,
    # not calibrated probabilities for the full demand/weather forecasting chain.
    output["forecast_p10_eur_mwh"] = np.minimum(output["forecast_p10_eur_mwh"], final)
    output["forecast_p90_eur_mwh"] = np.maximum(output["forecast_p90_eur_mwh"], final)
    score_rows = []
    for name, weight in zip(candidate_names, weights):
        score_rows.append({"Candidate": name, "Final weight (%)": 100 * float(weight),
            "Conditional validation MAE (EUR/MWh)": float(np.mean(np.abs(validation_candidates[name].to_numpy() - validation["price_eur_mwh"].to_numpy()))),
            "Target baseload (EUR/MWh)": float(target_candidates[name].mean()),
            "Target TB4 (EUR/MWh)": forecast_tb4(target_candidates[name])})
    vbacktest = validation[["datetime", "date", "hour", "price_eur_mwh"]].copy()
    vbacktest["model_forecast_price_eur_mwh"] = validation_prediction.to_numpy()
    gas_available = int(target["mibgas_data_available"].max()) > 0
    return {
        "forecast": output, "model_stats": forecast_metrics(validation["price_eur_mwh"], validation_prediction),
        "anchor_stats": forecast_metrics(validation["price_eur_mwh"], validation["price_anchor"]),
        "model_name": "V17 chronological sparse ensemble: direct, fundamental, residual and gap analogues",
        "training_rows": len(training), "candidate_scores": pd.DataFrame(score_rows),
        "conditional_validation": vbacktest,
        "validation_scope": "Conditional on historical observed PBF components; Step 6 is the operational chain backtest. Source publication vintages are not fully audited.",
        "gas_data_message": gas_message, "gas_delivery_day_used": target_day - timedelta(days=1),
        "gas_price_d1_eur_mwh": float(target["mibgas_d1_eur_mwh"].iloc[0]) if gas_available else np.nan,
        "gas_price_7d_avg_eur_mwh": float(target["mibgas_7d_avg_eur_mwh"].iloc[0]) if gas_available else np.nan,
        "gas_price_change_d1_eur_mwh": float(target["mibgas_change_d1_eur_mwh"].iloc[0]) if gas_available else np.nan,
        "gas_source": gas_source.get(target_day - timedelta(days=1), "Unavailable"),
        "bias_correction_shrink": bias_shrink,
        "ree_d1_available": bool(target["ree_d1_available"].min() > 0),
        "eua_message": eua_message,
    }


def build_price_forecast_chart(price_forecast: pd.DataFrame):
    specs = {"forecast_price_eur_mwh": "Final forecast", "fundamental_price_eur_mwh": "Fundamentals model",
             "conditional_price_median_eur_mwh": "Similar-gap median", "price_anchor": "Price anchor",
             "price_lag_1d": "Previous day", "price_lag_7d": "Same weekday previous week"}
    frames = [price_forecast[["datetime", c]].rename(columns={c: "price"}).assign(series=label)
              for c, label in specs.items() if c in price_forecast]
    plot = pd.concat(frames, ignore_index=True)
    lines = alt.Chart(plot).mark_line(point=True, strokeWidth=2.7).encode(
        x=alt.X("datetime:T", title=None, axis=alt.Axis(format="%H:%M", labelAngle=0)),
        y=alt.Y("price:Q", title="Price (EUR/MWh)", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title="Price series", scale=alt.Scale(
            domain=list(specs.values()), range=["#111827", "#0F766E", "#A855F7", "#64748B", "#F97316", "#60A5FA"])),
        strokeDash=alt.StrokeDash("series:N", legend=None),
        tooltip=[alt.Tooltip("datetime:T", format="%d-%m-%Y %H:%M"), "series:N", alt.Tooltip("price:Q", format=",.2f")],
    )
    band = alt.Chart(price_forecast).mark_area(opacity=.12, color=CORP_GREEN).encode(
        x="datetime:T", y="forecast_p10_eur_mwh:Q", y2="forecast_p90_eur_mwh:Q")
    return configure_chart(alt.layer(band, lines), height=350)


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
# STEP 5 - BESS VALUE OF THE PRICE FORECAST
# =========================================================
BESS_POWER_MW = 1.0
BESS_ENERGY_MWH = 4.0
BESS_RTE = 0.85
BESS_CHARGE_EFFICIENCY = float(np.sqrt(BESS_RTE))
BESS_DISCHARGE_EFFICIENCY = float(np.sqrt(BESS_RTE))
BESS_CHARGE_HOURS = 4
BESS_DISCHARGE_HOURS = 4
BESS_ANNUALIZATION_DAYS = 365.0


def optimize_chronological_tb4_schedule(
    price_frame: pd.DataFrame,
    price_column: str,
    rte: float = BESS_RTE,
    charge_hours: int = BESS_CHARGE_HOURS,
    discharge_hours: int = BESS_DISCHARGE_HOURS,
) -> dict | None:
    """
    Select one complete chronological 4-hour BESS cycle.

    Assumptions:
    - Battery starts empty.
    - Charge at 1 MW during exactly four hourly periods.
    - Every charge hour occurs before every discharge hour.
    - Round-trip efficiency is split symmetrically:
          eta_charge = eta_discharge = sqrt(RTE).
    - To store 1 MWh, grid purchase is 1 / eta_charge MWh.
    - To discharge 1 MWh from storage, grid sale is eta_discharge MWh.
    - No degradation, fees or variable O&M.
    """
    required = {"datetime", price_column}
    if (
        price_frame is None
        or price_frame.empty
        or not required.issubset(price_frame.columns)
    ):
        return None

    clean = price_frame[
        ["datetime", price_column]
    ].copy()
    clean["datetime"] = pd.to_datetime(
        clean["datetime"],
        errors="coerce",
    )
    clean[price_column] = pd.to_numeric(
        clean[price_column],
        errors="coerce",
    )
    clean = (
        clean.dropna(subset=["datetime", price_column])
        .sort_values("datetime")
        .drop_duplicates("datetime", keep="last")
        .reset_index(drop=True)
    )

    if len(clean) < charge_hours + discharge_hours:
        return None

    best = None

    # split_position is the last position where charging may occur.
    for split_position in range(
        charge_hours - 1,
        len(clean) - discharge_hours,
    ):
        charge_candidates = clean.iloc[
            : split_position + 1
        ]
        discharge_candidates = clean.iloc[
            split_position + 1 :
        ]

        if (
            len(charge_candidates) < charge_hours
            or len(discharge_candidates) < discharge_hours
        ):
            continue

        charge = (
            charge_candidates.nsmallest(
                charge_hours,
                price_column,
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )
        discharge = (
            discharge_candidates.nlargest(
                discharge_hours,
                price_column,
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        average_charge_price = float(
            charge[price_column].mean()
        )
        average_discharge_price = float(
            discharge[price_column].mean()
        )
        raw_tb4 = (
            average_discharge_price
            - average_charge_price
        )
        charge_efficiency = float(np.sqrt(rte))
        discharge_efficiency = float(np.sqrt(rte))

        rte_adjusted_spread = (
            discharge_efficiency
            * average_discharge_price
            - average_charge_price
            / charge_efficiency
        )

        charge_cost_eur = float(
            BESS_POWER_MW
            / charge_efficiency
            * charge[price_column].sum()
        )
        discharge_revenue_eur = float(
            BESS_POWER_MW
            * discharge_efficiency
            * discharge[price_column].sum()
        )
        daily_revenue_eur_mw = (
            discharge_revenue_eur
            - charge_cost_eur
        )

        candidate = {
            "charge_hours": charge["datetime"].tolist(),
            "discharge_hours": discharge["datetime"].tolist(),
            "charge_prices": charge[
                price_column
            ].tolist(),
            "discharge_prices": discharge[
                price_column
            ].tolist(),
            "average_charge_price_eur_mwh": (
                average_charge_price
            ),
            "average_discharge_price_eur_mwh": (
                average_discharge_price
            ),
            "chronological_tb4_eur_mwh": raw_tb4,
            "rte_adjusted_spread_eur_mwh": (
                rte_adjusted_spread
            ),
            "charge_cost_eur_mw_day": charge_cost_eur,
            "discharge_revenue_eur_mw_day": (
                discharge_revenue_eur
            ),
            "daily_revenue_eur_mw": (
                daily_revenue_eur_mw
            ),
            "annualized_revenue_eur_mw_yr": (
                daily_revenue_eur_mw
                * BESS_ANNUALIZATION_DAYS
            ),
            "rte": float(rte),
            "charge_efficiency": charge_efficiency,
            "discharge_efficiency": discharge_efficiency,
            "battery_side_charge_power_mw": BESS_POWER_MW,
            "battery_side_discharge_power_mw": BESS_POWER_MW,
            "grid_charge_power_mw": (
                BESS_POWER_MW / charge_efficiency
            ),
            "grid_discharge_power_mw": (
                BESS_POWER_MW * discharge_efficiency
            ),
        }

        if (
            best is None
            or candidate["daily_revenue_eur_mw"]
            > best["daily_revenue_eur_mw"]
        ):
            best = candidate

    return best


def settle_bess_schedule_on_actual_prices(
    schedule: dict | None,
    actual_price_frame: pd.DataFrame,
    actual_price_column: str = "actual_price_eur_mwh",
    rte: float = BESS_RTE,
) -> dict | None:
    """
    Settle a previously selected schedule against realised OMIE prices.

    This is used for the forecast strategy: the hours are selected using the
    forecast curve, but all purchases and sales are valued at actual prices.
    """
    if schedule is None:
        return None

    required = {"datetime", actual_price_column}
    if (
        actual_price_frame is None
        or actual_price_frame.empty
        or not required.issubset(actual_price_frame.columns)
    ):
        return None

    actual = actual_price_frame[
        ["datetime", actual_price_column]
    ].copy()
    actual["datetime"] = pd.to_datetime(
        actual["datetime"],
        errors="coerce",
    )
    actual[actual_price_column] = pd.to_numeric(
        actual[actual_price_column],
        errors="coerce",
    )
    actual = (
        actual.dropna(
            subset=["datetime", actual_price_column]
        )
        .drop_duplicates("datetime", keep="last")
        .set_index("datetime")
    )

    charge_hours = [
        pd.Timestamp(value)
        for value in schedule["charge_hours"]
    ]
    discharge_hours = [
        pd.Timestamp(value)
        for value in schedule["discharge_hours"]
    ]

    if not all(
        timestamp in actual.index
        for timestamp in charge_hours + discharge_hours
    ):
        return None

    charge_prices = actual.loc[
        charge_hours,
        actual_price_column,
    ].astype(float)
    discharge_prices = actual.loc[
        discharge_hours,
        actual_price_column,
    ].astype(float)

    average_charge_price = float(
        charge_prices.mean()
    )
    average_discharge_price = float(
        discharge_prices.mean()
    )

    charge_efficiency = float(np.sqrt(rte))
    discharge_efficiency = float(np.sqrt(rte))

    charge_cost = float(
        BESS_POWER_MW
        / charge_efficiency
        * charge_prices.sum()
    )
    discharge_revenue = float(
        BESS_POWER_MW
        * discharge_efficiency
        * discharge_prices.sum()
    )
    daily_revenue = discharge_revenue - charge_cost

    settled = {
        **schedule,
        "actual_charge_prices": charge_prices.tolist(),
        "actual_discharge_prices": (
            discharge_prices.tolist()
        ),
        "actual_average_charge_price_eur_mwh": (
            average_charge_price
        ),
        "actual_average_discharge_price_eur_mwh": (
            average_discharge_price
        ),
        "actual_chronological_tb4_eur_mwh": (
            average_discharge_price
            - average_charge_price
        ),
        "actual_rte_adjusted_spread_eur_mwh": (
            discharge_efficiency
            * average_discharge_price
            - average_charge_price
            / charge_efficiency
        ),
        "actual_charge_efficiency": charge_efficiency,
        "actual_discharge_efficiency": discharge_efficiency,
        "actual_charge_cost_eur_mw_day": charge_cost,
        "actual_discharge_revenue_eur_mw_day": (
            discharge_revenue
        ),
        "actual_daily_revenue_eur_mw": daily_revenue,
        "actual_annualized_revenue_eur_mw_yr": (
            daily_revenue
            * BESS_ANNUALIZATION_DAYS
        ),
    }
    return settled



BESS_PRICE_CURVE_BENCHMARKS = {
    "Final forecast - black": "forecast_price_eur_mwh",
    "Previous day": "price_lag_1d",
    "Previous week": "price_lag_7d",
    "Price anchor": "price_anchor",
    "Similar-gap median": "conditional_price_median_eur_mwh",
}

BESS_PRICE_CURVE_SHORT_LABELS = {
    "Final forecast - black": "Final forecast",
    "Previous day": "D-1",
    "Previous week": "D-7",
    "Price anchor": "Price anchor",
    "Similar-gap median": "Similar-gap",
}

BESS_PRICE_CURVE_SLUGS = {
    "Final forecast - black": "final_forecast",
    "Previous day": "previous_day",
    "Previous week": "previous_week",
    "Price anchor": "price_anchor",
    "Similar-gap median": "similar_gap",
}


def evaluate_bess_price_curve_benchmarks(
    price_forecast: pd.DataFrame,
    actual_price_frame: pd.DataFrame,
    perfect_schedule: dict | None,
    actual_price_column: str = "actual_price_eur_mwh",
    rte: float = BESS_RTE,
) -> pd.DataFrame:
    """
    Select a chronological 4h/4h BESS schedule independently with each
    available price curve and settle every selected schedule against the same
    realised OMIE prices.

    This isolates the economic usefulness of the shape/ranking of each curve.
    """
    if (
        price_forecast is None
        or price_forecast.empty
        or actual_price_frame is None
        or actual_price_frame.empty
        or perfect_schedule is None
    ):
        return pd.DataFrame()

    actual = actual_price_frame[
        ["datetime", actual_price_column]
    ].copy()
    actual["datetime"] = pd.to_datetime(
        actual["datetime"],
        errors="coerce",
    )
    actual[actual_price_column] = pd.to_numeric(
        actual[actual_price_column],
        errors="coerce",
    )
    actual = actual.dropna(
        subset=["datetime", actual_price_column]
    )

    perfect_revenue = float(
        perfect_schedule["daily_revenue_eur_mw"]
    )
    perfect_charge = set(
        pd.Timestamp(value)
        for value in perfect_schedule["charge_hours"]
    )
    perfect_discharge = set(
        pd.Timestamp(value)
        for value in perfect_schedule["discharge_hours"]
    )

    rows = []

    for strategy_name, price_column in (
        BESS_PRICE_CURVE_BENCHMARKS.items()
    ):
        if price_column not in price_forecast.columns:
            continue

        curve = price_forecast[
            ["datetime", price_column]
        ].copy()
        curve["datetime"] = pd.to_datetime(
            curve["datetime"],
            errors="coerce",
        )
        curve[price_column] = pd.to_numeric(
            curve[price_column],
            errors="coerce",
        )

        curve = curve.merge(
            actual,
            on="datetime",
            how="inner",
        ).dropna(
            subset=[
                "datetime",
                price_column,
                actual_price_column,
            ]
        )

        if len(curve) < 23:
            continue

        schedule = optimize_chronological_tb4_schedule(
            curve,
            price_column=price_column,
            rte=rte,
        )
        settlement = settle_bess_schedule_on_actual_prices(
            schedule,
            curve,
            actual_price_column=actual_price_column,
            rte=rte,
        )

        if schedule is None or settlement is None:
            continue

        realised_revenue = float(
            settlement[
                "actual_daily_revenue_eur_mw"
            ]
        )
        expected_revenue = float(
            schedule["daily_revenue_eur_mw"]
        )
        capture_pct = (
            100.0
            * realised_revenue
            / perfect_revenue
            if abs(perfect_revenue) > 1e-9
            else np.nan
        )

        charge_hours = set(
            pd.Timestamp(value)
            for value in schedule["charge_hours"]
        )
        discharge_hours = set(
            pd.Timestamp(value)
            for value in schedule["discharge_hours"]
        )

        rows.append(
            {
                "Strategy": strategy_name,
                "Short label": (
                    BESS_PRICE_CURVE_SHORT_LABELS[
                        strategy_name
                    ]
                ),
                "Slug": (
                    BESS_PRICE_CURVE_SLUGS[
                        strategy_name
                    ]
                ),
                "Price column": price_column,
                "Revenue capture (%)": capture_pct,
                "Realised revenue (€/MW-day)": (
                    realised_revenue
                ),
                "Expected revenue (€/MW-day)": (
                    expected_revenue
                ),
                "Annualized realised revenue (€/MW/yr)": (
                    realised_revenue
                    * BESS_ANNUALIZATION_DAYS
                ),
                "Opportunity loss (€/MW-day)": (
                    perfect_revenue
                    - realised_revenue
                ),
                "Charge overlap": len(
                    perfect_charge.intersection(
                        charge_hours
                    )
                ),
                "Discharge overlap": len(
                    perfect_discharge.intersection(
                        discharge_hours
                    )
                ),
                "Charge hours": _hours_as_text(
                    schedule["charge_hours"]
                ),
                "Discharge hours": _hours_as_text(
                    schedule["discharge_hours"]
                ),
                "Forecast chronological TB4 (€/MWh)": (
                    schedule[
                        "chronological_tb4_eur_mwh"
                    ]
                ),
                "Realised RTE-adjusted spread (€/MWh)": (
                    settlement[
                        "actual_rte_adjusted_spread_eur_mwh"
                    ]
                ),
            }
        )

    if not rows:
        return pd.DataFrame()

    output = pd.DataFrame(rows)
    final_capture = output.loc[
        output["Slug"] == "final_forecast",
        "Revenue capture (%)",
    ]
    final_capture_value = (
        float(final_capture.iloc[0])
        if not final_capture.empty
        else np.nan
    )
    output["Difference vs final (pp)"] = (
        output["Revenue capture (%)"]
        - final_capture_value
    )
    output["Rank"] = (
        output["Revenue capture (%)"]
        .rank(
            method="min",
            ascending=False,
        )
        .astype("Int64")
    )

    return output.sort_values(
        ["Rank", "Strategy"]
    ).reset_index(drop=True)


def build_price_curve_capture_chart(
    benchmark: pd.DataFrame,
):
    if benchmark is None or benchmark.empty:
        return None

    plot = benchmark.copy()

    chart = (
        alt.Chart(plot)
        .mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5,
        )
        .encode(
            x=alt.X(
                "Short label:N",
                title=None,
                sort=[
                    "Final forecast",
                    "D-1",
                    "D-7",
                    "Price anchor",
                    "Similar-gap",
                ],
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y(
                "Revenue capture (%):Q",
                title="Revenue captured vs real perfect foresight (%)",
            ),
            color=alt.Color(
                "Short label:N",
                legend=None,
                scale=alt.Scale(
                    domain=[
                        "Final forecast",
                        "D-1",
                        "D-7",
                        "Price anchor",
                        "Similar-gap",
                    ],
                    range=[
                        "#111827",
                        "#F97316",
                        "#60A5FA",
                        "#64748B",
                        "#A855F7",
                    ],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "Strategy:N",
                    title="Price curve",
                ),
                alt.Tooltip(
                    "Revenue capture (%):Q",
                    title="Capture",
                    format=",.1f",
                ),
                alt.Tooltip(
                    "Realised revenue (€/MW-day):Q",
                    title="Realised €/MW-day",
                    format=",.2f",
                ),
                alt.Tooltip(
                    "Difference vs final (pp):Q",
                    title="Difference vs final",
                    format="+.1f",
                ),
                alt.Tooltip(
                    "Charge hours:N",
                    title="Charge",
                ),
                alt.Tooltip(
                    "Discharge hours:N",
                    title="Discharge",
                ),
            ],
        )
    )

    labels = (
        alt.Chart(plot)
        .mark_text(
            dy=-10,
            fontWeight="bold",
        )
        .encode(
            x=alt.X(
                "Short label:N",
                sort=[
                    "Final forecast",
                    "D-1",
                    "D-7",
                    "Price anchor",
                    "Similar-gap",
                ],
            ),
            y=alt.Y("Revenue capture (%):Q"),
            text=alt.Text(
                "Revenue capture (%):Q",
                format=",.1f",
            ),
        )
    )

    return configure_chart(
        alt.layer(chart, labels),
        height=310,
    )


def ytd_price_curve_capture_summary(
    successful: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate realised BESS revenue by price curve across walk-forward dates.
    """
    if successful is None or successful.empty:
        return pd.DataFrame()

    perfect_column = (
        "perfect_foresight_daily_revenue_eur_mw"
    )
    if perfect_column not in successful.columns:
        return pd.DataFrame()

    perfect_total = pd.to_numeric(
        successful[perfect_column],
        errors="coerce",
    ).sum()
    tested_days = len(successful)

    rows = []
    for strategy_name, slug in BESS_PRICE_CURVE_SLUGS.items():
        revenue_column = (
            f"{slug}_realized_daily_revenue_eur_mw"
        )
        if revenue_column not in successful.columns:
            continue

        revenue_total = pd.to_numeric(
            successful[revenue_column],
            errors="coerce",
        ).sum()
        rows.append(
            {
                "Strategy": strategy_name,
                "Short label": (
                    BESS_PRICE_CURVE_SHORT_LABELS[
                        strategy_name
                    ]
                ),
                "YTD realised revenue (€/MW)": (
                    revenue_total
                ),
                "Revenue capture (%)": (
                    100.0
                    * revenue_total
                    / perfect_total
                    if abs(perfect_total) > 1e-9
                    else np.nan
                ),
                "Annualized revenue (€/MW/yr)": (
                    revenue_total
                    / tested_days
                    * BESS_ANNUALIZATION_DAYS
                    if tested_days > 0
                    else np.nan
                ),
                "YTD opportunity loss (€/MW)": (
                    perfect_total - revenue_total
                ),
            }
        )

    return pd.DataFrame(rows).sort_values(
        "Revenue capture (%)",
        ascending=False,
    ).reset_index(drop=True)

def build_bess_dispatch_comparison_chart(
    comparison: pd.DataFrame,
    perfect_schedule: dict,
    forecast_settlement: dict,
):
    """
    Plot forecast and realised prices with the two selected dispatch schedules.
    All dispatch markers are placed on the realised price curve.
    """
    if comparison is None or comparison.empty:
        return None

    price_long = pd.concat(
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
            .assign(series="Real OMIE price"),
        ],
        ignore_index=True,
    )

    lines = (
        alt.Chart(price_long)
        .mark_line(
            point=True,
            strokeWidth=2.7,
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
                scale=alt.Scale(zero=False),
            ),
            color=alt.Color(
                "series:N",
                title="Price series",
                scale=alt.Scale(
                    domain=[
                        "Forecast price",
                        "Real OMIE price",
                    ],
                    range=[
                        "#111827",
                        "#16A34A",
                    ],
                ),
            ),
            strokeDash=alt.StrokeDash(
                "series:N",
                legend=None,
                scale=alt.Scale(
                    domain=[
                        "Forecast price",
                        "Real OMIE price",
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
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip(
                    "price:Q",
                    title="€/MWh",
                    format=",.2f",
                ),
            ],
        )
    )

    actual_lookup = (
        comparison[
            ["datetime", "actual_price_eur_mwh"]
        ]
        .drop_duplicates("datetime")
        .set_index("datetime")[
            "actual_price_eur_mwh"
        ]
    )

    marker_rows = []

    schedule_specs = [
        (
            "Real-price optimum",
            perfect_schedule,
            "#2563EB",
        ),
        (
            "Forecast-selected",
            forecast_settlement,
            "#F97316",
        ),
    ]

    for strategy, schedule, _ in schedule_specs:
        if schedule is None:
            continue

        for action, datetimes in [
            ("Charge", schedule["charge_hours"]),
            ("Discharge", schedule["discharge_hours"]),
        ]:
            for timestamp in datetimes:
                timestamp = pd.Timestamp(timestamp)
                if timestamp not in actual_lookup.index:
                    continue
                marker_rows.append(
                    {
                        "datetime": timestamp,
                        "actual_price_eur_mwh": float(
                            actual_lookup.loc[timestamp]
                        ),
                        "strategy_action": (
                            f"{strategy} - {action}"
                        ),
                    }
                )

    marker_frame = pd.DataFrame(marker_rows)

    if marker_frame.empty:
        return configure_chart(lines, height=380)

    marker_domain = [
        "Real-price optimum - Charge",
        "Real-price optimum - Discharge",
        "Forecast-selected - Charge",
        "Forecast-selected - Discharge",
    ]

    markers = (
        alt.Chart(marker_frame)
        .mark_point(
            filled=True,
            size=155,
            stroke="#FFFFFF",
            strokeWidth=1.3,
        )
        .encode(
            x=alt.X("datetime:T"),
            y=alt.Y(
                "actual_price_eur_mwh:Q",
                title="DA price (€/MWh)",
            ),
            color=alt.Color(
                "strategy_action:N",
                title="BESS dispatch",
                scale=alt.Scale(
                    domain=marker_domain,
                    range=[
                        "#2563EB",
                        "#2563EB",
                        "#F97316",
                        "#F97316",
                    ],
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                    columns=2,
                    labelLimit=320,
                ),
            ),
            shape=alt.Shape(
                "strategy_action:N",
                title="BESS dispatch",
                scale=alt.Scale(
                    domain=marker_domain,
                    range=[
                        "triangle-down",
                        "triangle-up",
                        "diamond",
                        "square",
                    ],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip(
                    "datetime:T",
                    title="Hour",
                    format="%d-%m-%Y %H:%M",
                ),
                alt.Tooltip(
                    "strategy_action:N",
                    title="Dispatch",
                ),
                alt.Tooltip(
                    "actual_price_eur_mwh:Q",
                    title="Real OMIE price",
                    format=",.2f",
                ),
            ],
        )
    )

    return configure_chart(
        alt.layer(lines, markers),
        height=390,
    )


def build_bess_annualized_revenue_chart(
    perfect_revenue: float,
    forecast_strategy_revenue: float,
):
    chart_data = pd.DataFrame(
        {
            "Strategy": [
                "Real-price perfect foresight",
                "Forecast-selected hours",
            ],
            "Annualized revenue": [
                perfect_revenue,
                forecast_strategy_revenue,
            ],
        }
    )

    chart = (
        alt.Chart(chart_data)
        .mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5,
        )
        .encode(
            x=alt.X(
                "Strategy:N",
                title=None,
                sort=[
                    "Real-price perfect foresight",
                    "Forecast-selected hours",
                ],
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y(
                "Annualized revenue:Q",
                title="Annualized BESS revenue (€/MW/yr)",
            ),
            color=alt.Color(
                "Strategy:N",
                legend=None,
                scale=alt.Scale(
                    domain=[
                        "Real-price perfect foresight",
                        "Forecast-selected hours",
                    ],
                    range=[
                        "#2563EB",
                        "#F97316",
                    ],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "Strategy:N",
                    title="Strategy",
                ),
                alt.Tooltip(
                    "Annualized revenue:Q",
                    title="€/MW/yr",
                    format=",.0f",
                ),
            ],
        )
    )

    labels = (
        alt.Chart(chart_data)
        .mark_text(
            dy=-10,
            fontWeight="bold",
        )
        .encode(
            x=alt.X(
                "Strategy:N",
                sort=[
                    "Real-price perfect foresight",
                    "Forecast-selected hours",
                ],
            ),
            y=alt.Y("Annualized revenue:Q"),
            text=alt.Text(
                "Annualized revenue:Q",
                format=",.0f",
            ),
        )
    )

    return configure_chart(
        alt.layer(chart, labels),
        height=320,
    )


def build_bess_schedule_table(
    comparison: pd.DataFrame,
    perfect_schedule: dict,
    forecast_schedule: dict,
) -> pd.DataFrame:
    actual_lookup = (
        comparison[
            ["datetime", "actual_price_eur_mwh"]
        ]
        .drop_duplicates("datetime")
        .set_index("datetime")[
            "actual_price_eur_mwh"
        ]
    )
    forecast_lookup = (
        comparison[
            ["datetime", "forecast_price_eur_mwh"]
        ]
        .drop_duplicates("datetime")
        .set_index("datetime")[
            "forecast_price_eur_mwh"
        ]
    )

    rows = []

    for strategy, schedule in [
        ("Real-price perfect foresight", perfect_schedule),
        ("Forecast-selected hours", forecast_schedule),
    ]:
        if schedule is None:
            continue

        for action, datetimes in [
            ("Charge", schedule["charge_hours"]),
            ("Discharge", schedule["discharge_hours"]),
        ]:
            for timestamp in datetimes:
                timestamp = pd.Timestamp(timestamp)
                rows.append(
                    {
                        "Strategy": strategy,
                        "Action": action,
                        "Hour": timestamp,
                        "Forecast price (€/MWh)": (
                            float(forecast_lookup.loc[timestamp])
                            if timestamp in forecast_lookup.index
                            else np.nan
                        ),
                        "Real OMIE price (€/MWh)": (
                            float(actual_lookup.loc[timestamp])
                            if timestamp in actual_lookup.index
                            else np.nan
                        ),
                        "Grid power (MW)": (
                            -BESS_POWER_MW
                            / BESS_CHARGE_EFFICIENCY
                            if action == "Charge"
                            else BESS_POWER_MW
                            * BESS_DISCHARGE_EFFICIENCY
                        ),
                    }
                )

    return pd.DataFrame(rows).sort_values(
        ["Strategy", "Hour"]
    ).reset_index(drop=True)


# =========================================================
# YTD DAILY WALK-FORWARD BACKTEST
# =========================================================
YTD_BACKTEST_STATE_KEY = "ytd_walk_forward_results_v17_2"
YTD_BACKTEST_CHECKPOINT_VERSION = "v17_2_cow_fix"


def _safe_ratio(
    numerator: float,
    denominator: float,
) -> float:
    if (
        pd.isna(numerator)
        or pd.isna(denominator)
        or abs(float(denominator)) <= 1e-9
    ):
        return np.nan
    return float(numerator) / float(denominator)


def _hours_as_text(values) -> str:
    if values is None:
        return ""
    return ", ".join(
        pd.Timestamp(value).strftime("%H:%M")
        for value in values
    )


@st.cache_data(show_spinner=False, ttl=86400)
def run_one_walk_forward_backtest_day(
    target_day: date,
    lookback_days: int,
    temperature_mode: str,
    trend_alpha: float,
    demand_source: str,
    technologies_tuple: tuple[str, ...],
    _token: str,
    engine_version: str = FORECAST_ENGINE_VERSION,
) -> dict:
    """
    Recreate one complete operational D-1 forecast and settle it against the
    realised target-day OMIE prices.

    Information cut-offs are inherited from the single-day engine:
    - Demand history: through D-2.
    - Target weather: archived forecast issued at D-1.
    - PBF, bilateral programmes, spot prices and gas inputs: through D-1.
    - Realised target-day prices are loaded only after the forecast is built.
    """
    forecast_as_of_day = target_day - timedelta(days=1)

    demand_result = generate_day_ahead_forecast(
        target_day=target_day,
        lookback_days=int(lookback_days),
        temperature_mode=temperature_mode,
        trend_alpha=float(trend_alpha),
    )

    demand_forecast = demand_result["forecast"].copy()

    if demand_source.startswith("Model"):
        demand_forecast["selected_demand_mw"] = (
            demand_forecast["forecast_mw"]
        )
        demand_source_label = "Model forecast"
    else:
        demand_forecast["selected_demand_mw"] = (
            demand_forecast["trend_adjusted_lag_7d"]
        )
        demand_source_label = "Previous week + recent trend"

    thermal_result = generate_thermal_gap_forecast(
        target_day=target_day,
        lookback_days=int(lookback_days),
        selected_demand=demand_forecast[
            ["datetime", "selected_demand_mw"]
        ],
        technologies=list(technologies_tuple),
        token=_token,
    )

    price_result = generate_price_forecast(
        target_day=target_day,
        historical_gap=thermal_result["historical"],
        forecast_gap=thermal_result["forecast"],
        token=_token,
    )

    realised_prices = load_esios_price_history(
        start_day=target_day,
        end_day=target_day,
        _token=_token,
    )

    if realised_prices.empty:
        raise ValueError(
            "Real target-day OMIE prices are not available."
        )

    price_forecast = price_result["forecast"].copy()

    ytd_curve_columns = [
        "datetime",
        *[
            column
            for column in BESS_PRICE_CURVE_BENCHMARKS.values()
            if column in price_forecast.columns
        ],
    ]

    comparison = price_forecast[
        ytd_curve_columns
    ].merge(
        realised_prices[
            ["datetime", "price_eur_mwh"]
        ].rename(
            columns={
                "price_eur_mwh": "actual_price_eur_mwh"
            }
        ),
        on="datetime",
        how="inner",
    )

    comparison["forecast_price_eur_mwh"] = pd.to_numeric(
        comparison["forecast_price_eur_mwh"],
        errors="coerce",
    )
    comparison["actual_price_eur_mwh"] = pd.to_numeric(
        comparison["actual_price_eur_mwh"],
        errors="coerce",
    )
    comparison = comparison.dropna(
        subset=[
            "datetime",
            "forecast_price_eur_mwh",
            "actual_price_eur_mwh",
        ]
    ).sort_values("datetime")

    if len(comparison) < 23:
        raise ValueError(
            f"Only {len(comparison)} complete forecast/real price hours."
        )

    price_metrics = price_realization_metrics(comparison)

    perfect_schedule = optimize_chronological_tb4_schedule(
        comparison,
        price_column="actual_price_eur_mwh",
        rte=BESS_RTE,
    )
    forecast_schedule = optimize_chronological_tb4_schedule(
        comparison,
        price_column="forecast_price_eur_mwh",
        rte=BESS_RTE,
    )
    forecast_settlement = settle_bess_schedule_on_actual_prices(
        forecast_schedule,
        comparison,
        actual_price_column="actual_price_eur_mwh",
        rte=BESS_RTE,
    )

    if (
        perfect_schedule is None
        or forecast_schedule is None
        or forecast_settlement is None
    ):
        raise ValueError(
            "The chronological BESS schedules could not be completed."
        )

    ytd_curve_benchmark = (
        evaluate_bess_price_curve_benchmarks(
            price_forecast=price_forecast,
            actual_price_frame=comparison,
            perfect_schedule=perfect_schedule,
            actual_price_column="actual_price_eur_mwh",
            rte=BESS_RTE,
        )
    )

    ytd_curve_values = {}
    if not ytd_curve_benchmark.empty:
        for benchmark_row in (
            ytd_curve_benchmark.itertuples(index=False)
        ):
            slug = getattr(benchmark_row, "Slug")
            # itertuples sanitises names containing punctuation, so use
            # the original dataframe row for the metric fields.
            original_row = ytd_curve_benchmark[
                ytd_curve_benchmark["Slug"] == slug
            ].iloc[0]
            ytd_curve_values[
                f"{slug}_realized_daily_revenue_eur_mw"
            ] = float(
                original_row[
                    "Realised revenue (€/MW-day)"
                ]
            )
            ytd_curve_values[
                f"{slug}_revenue_capture_pct"
            ] = float(
                original_row[
                    "Revenue capture (%)"
                ]
            )

    perfect_daily_revenue = float(
        perfect_schedule["daily_revenue_eur_mw"]
    )
    forecast_realized_daily_revenue = float(
        forecast_settlement[
            "actual_daily_revenue_eur_mw"
        ]
    )
    forecast_expected_daily_revenue = float(
        forecast_schedule["daily_revenue_eur_mw"]
    )

    opportunity_loss = (
        perfect_daily_revenue
        - forecast_realized_daily_revenue
    )
    revenue_capture_pct = (
        100.0
        * _safe_ratio(
            forecast_realized_daily_revenue,
            perfect_daily_revenue,
        )
    )

    perfect_charge_set = set(
        pd.Timestamp(value)
        for value in perfect_schedule["charge_hours"]
    )
    forecast_charge_set = set(
        pd.Timestamp(value)
        for value in forecast_schedule["charge_hours"]
    )
    perfect_discharge_set = set(
        pd.Timestamp(value)
        for value in perfect_schedule["discharge_hours"]
    )
    forecast_discharge_set = set(
        pd.Timestamp(value)
        for value in forecast_schedule["discharge_hours"]
    )

    thermal_forecast = thermal_result["forecast"].copy()
    thermal_gap = pd.to_numeric(
        thermal_forecast["thermal_gap_forecast_mwh"],
        errors="coerce",
    )

    actual_prices = comparison["actual_price_eur_mwh"]
    forecast_prices = comparison["forecast_price_eur_mwh"]

    return {
        "checkpoint_version": YTD_BACKTEST_CHECKPOINT_VERSION,
        "target_day": target_day.isoformat(),
        "as_of_day": forecast_as_of_day.isoformat(),
        "status": "ok",
        "error": "",
        "demand_source": demand_source_label,
        "temperature_mode": temperature_mode,
        "lookback_days": int(lookback_days),
        "trend_alpha": float(trend_alpha),
        "technologies": " | ".join(technologies_tuple),
        "complete_price_hours": int(len(comparison)),
        "forecast_baseload_eur_mwh": float(
            forecast_prices.mean()
        ),
        "actual_baseload_eur_mwh": float(
            actual_prices.mean()
        ),
        "baseload_error_eur_mwh": float(
            price_metrics["baseload_error"]
        ),
        "price_mae_eur_mwh": float(
            price_metrics["mae"]
        ),
        "price_rmse_eur_mwh": float(
            price_metrics["rmse"]
        ),
        "price_smape_pct": float(
            price_metrics["smape"]
        ),
        "forecast_min_price_eur_mwh": float(
            forecast_prices.min()
        ),
        "actual_min_price_eur_mwh": float(
            actual_prices.min()
        ),
        "forecast_max_price_eur_mwh": float(
            forecast_prices.max()
        ),
        "actual_max_price_eur_mwh": float(
            actual_prices.max()
        ),
        "forecast_zero_or_negative_hours": int(
            (forecast_prices <= 0).sum()
        ),
        "actual_zero_or_negative_hours": int(
            (actual_prices <= 0).sum()
        ),
        "forecast_chronological_tb4_eur_mwh": float(
            forecast_schedule[
                "chronological_tb4_eur_mwh"
            ]
        ),
        "actual_chronological_tb4_eur_mwh": float(
            perfect_schedule[
                "chronological_tb4_eur_mwh"
            ]
        ),
        "tb4_error_eur_mwh": float(
            forecast_schedule[
                "chronological_tb4_eur_mwh"
            ]
            - perfect_schedule[
                "chronological_tb4_eur_mwh"
            ]
        ),
        "forecast_expected_daily_revenue_eur_mw": (
            forecast_expected_daily_revenue
        ),
        "forecast_strategy_realized_daily_revenue_eur_mw": (
            forecast_realized_daily_revenue
        ),
        "perfect_foresight_daily_revenue_eur_mw": (
            perfect_daily_revenue
        ),
        "daily_opportunity_loss_eur_mw": (
            opportunity_loss
        ),
        "revenue_capture_pct": revenue_capture_pct,
        "forecast_strategy_annualized_eur_mw_yr": (
            forecast_realized_daily_revenue
            * BESS_ANNUALIZATION_DAYS
        ),
        "perfect_foresight_annualized_eur_mw_yr": (
            perfect_daily_revenue
            * BESS_ANNUALIZATION_DAYS
        ),
        "annualized_opportunity_loss_eur_mw_yr": (
            opportunity_loss
            * BESS_ANNUALIZATION_DAYS
        ),
        "charge_hour_overlap": int(
            len(
                perfect_charge_set.intersection(
                    forecast_charge_set
                )
            )
        ),
        "discharge_hour_overlap": int(
            len(
                perfect_discharge_set.intersection(
                    forecast_discharge_set
                )
            )
        ),
        "perfect_charge_hours": _hours_as_text(
            perfect_schedule["charge_hours"]
        ),
        "perfect_discharge_hours": _hours_as_text(
            perfect_schedule["discharge_hours"]
        ),
        "forecast_charge_hours": _hours_as_text(
            forecast_schedule["charge_hours"]
        ),
        "forecast_discharge_hours": _hours_as_text(
            forecast_schedule["discharge_hours"]
        ),
        "average_forecast_thermal_gap_mw": float(
            thermal_gap.mean()
        ),
        "minimum_forecast_thermal_gap_mw": float(
            thermal_gap.min()
        ),
        "negative_forecast_thermal_gap_hours": int(
            (thermal_gap < 0).sum()
        ),
        "demand_model_validation_mae_mw": float(
            demand_result["model_stats"]["mae"]
        ),
        "demand_model_validation_mape_pct": float(
            demand_result["model_stats"]["mape"]
        ),
        **ytd_curve_values,
    }


def failed_walk_forward_row(
    target_day: date,
    exc: Exception,
) -> dict:
    return {
        "checkpoint_version": (
            YTD_BACKTEST_CHECKPOINT_VERSION
        ),
        "target_day": target_day.isoformat(),
        "as_of_day": (
            target_day - timedelta(days=1)
        ).isoformat(),
        "status": "error",
        "error": str(exc)[:500],
    }


def normalize_walk_forward_results(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()

    output = frame.copy()

    if "target_day" not in output.columns:
        return pd.DataFrame()

    output["target_day"] = pd.to_datetime(
        output["target_day"],
        errors="coerce",
    ).dt.date
    output = output.dropna(subset=["target_day"])

    if "status" not in output.columns:
        output["status"] = "ok"

    output = (
        output.sort_values("target_day")
        .drop_duplicates(
            subset=["target_day"],
            keep="last",
        )
        .reset_index(drop=True)
    )
    return output


def aggregate_walk_forward_monthly(
    successful: pd.DataFrame,
) -> pd.DataFrame:
    if successful is None or successful.empty:
        return pd.DataFrame()

    monthly = successful.copy()
    monthly["target_day"] = pd.to_datetime(
        monthly["target_day"],
        errors="coerce",
    )
    monthly["month"] = monthly[
        "target_day"
    ].dt.to_period("M").astype(str)

    result = (
        monthly.groupby("month", as_index=False)
        .agg(
            days=("target_day", "count"),
            price_mae_eur_mwh=(
                "price_mae_eur_mwh",
                "mean",
            ),
            price_smape_pct=(
                "price_smape_pct",
                "mean",
            ),
            actual_baseload_eur_mwh=(
                "actual_baseload_eur_mwh",
                "mean",
            ),
            forecast_baseload_eur_mwh=(
                "forecast_baseload_eur_mwh",
                "mean",
            ),
            perfect_revenue_eur_mw=(
                "perfect_foresight_daily_revenue_eur_mw",
                "sum",
            ),
            forecast_strategy_revenue_eur_mw=(
                "forecast_strategy_realized_daily_revenue_eur_mw",
                "sum",
            ),
            opportunity_loss_eur_mw=(
                "daily_opportunity_loss_eur_mw",
                "sum",
            ),
            average_charge_overlap=(
                "charge_hour_overlap",
                "mean",
            ),
            average_discharge_overlap=(
                "discharge_hour_overlap",
                "mean",
            ),
        )
    )

    result["revenue_capture_pct"] = (
        100.0
        * result[
            "forecast_strategy_revenue_eur_mw"
        ]
        / result[
            "perfect_revenue_eur_mw"
        ].replace(0.0, np.nan)
    )
    result[
        "forecast_strategy_annualized_eur_mw_yr"
    ] = (
        result[
            "forecast_strategy_revenue_eur_mw"
        ]
        / result["days"]
        * BESS_ANNUALIZATION_DAYS
    )
    result[
        "perfect_annualized_eur_mw_yr"
    ] = (
        result["perfect_revenue_eur_mw"]
        / result["days"]
        * BESS_ANNUALIZATION_DAYS
    )

    return result


def build_ytd_cumulative_revenue_chart(
    successful: pd.DataFrame,
):
    if successful is None or successful.empty:
        return None

    plot = successful.sort_values(
        "target_day"
    ).copy()
    plot["target_day"] = pd.to_datetime(
        plot["target_day"],
        errors="coerce",
    )
    plot[
        "Forecast-selected hours"
    ] = plot[
        "forecast_strategy_realized_daily_revenue_eur_mw"
    ].cumsum()
    plot[
        "Real-price perfect foresight"
    ] = plot[
        "perfect_foresight_daily_revenue_eur_mw"
    ].cumsum()

    long = plot.melt(
        id_vars=["target_day"],
        value_vars=[
            "Forecast-selected hours",
            "Real-price perfect foresight",
        ],
        var_name="Strategy",
        value_name="Cumulative revenue",
    )

    chart = (
        alt.Chart(long)
        .mark_line(
            strokeWidth=3,
        )
        .encode(
            x=alt.X(
                "target_day:T",
                title=None,
                axis=alt.Axis(format="%d-%b"),
            ),
            y=alt.Y(
                "Cumulative revenue:Q",
                title="Cumulative realised BESS revenue (€/MW)",
            ),
            color=alt.Color(
                "Strategy:N",
                title="Strategy",
                scale=alt.Scale(
                    domain=[
                        "Real-price perfect foresight",
                        "Forecast-selected hours",
                    ],
                    range=[
                        "#2563EB",
                        "#F97316",
                    ],
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "target_day:T",
                    title="Date",
                    format="%d-%m-%Y",
                ),
                alt.Tooltip(
                    "Strategy:N",
                    title="Strategy",
                ),
                alt.Tooltip(
                    "Cumulative revenue:Q",
                    title="Cumulative €/MW",
                    format=",.1f",
                ),
            ],
        )
    )
    return configure_chart(chart, height=350)


def build_ytd_rolling_capture_chart(
    successful: pd.DataFrame,
    rolling_days: int = 30,
):
    if successful is None or successful.empty:
        return None

    plot = successful.sort_values(
        "target_day"
    ).copy()
    plot["target_day"] = pd.to_datetime(
        plot["target_day"],
        errors="coerce",
    )

    perfect_rolling = plot[
        "perfect_foresight_daily_revenue_eur_mw"
    ].rolling(
        rolling_days,
        min_periods=max(5, rolling_days // 4),
    ).sum()

    forecast_rolling = plot[
        "forecast_strategy_realized_daily_revenue_eur_mw"
    ].rolling(
        rolling_days,
        min_periods=max(5, rolling_days // 4),
    ).sum()

    plot["rolling_capture_pct"] = (
        100.0
        * forecast_rolling
        / perfect_rolling.replace(0.0, np.nan)
    )

    chart = (
        alt.Chart(plot)
        .mark_line(
            point=True,
            strokeWidth=2.7,
            color="#0F766E",
        )
        .encode(
            x=alt.X(
                "target_day:T",
                title=None,
                axis=alt.Axis(format="%d-%b"),
            ),
            y=alt.Y(
                "rolling_capture_pct:Q",
                title=(
                    f"{rolling_days}-day rolling "
                    "revenue capture (%)"
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "target_day:T",
                    title="Date",
                    format="%d-%m-%Y",
                ),
                alt.Tooltip(
                    "rolling_capture_pct:Q",
                    title="Revenue capture",
                    format=",.1f",
                ),
            ],
        )
    )

    hundred = (
        alt.Chart(pd.DataFrame({"capture": [100.0]}))
        .mark_rule(
            color="#64748B",
            strokeDash=[5, 3],
        )
        .encode(y="capture:Q")
    )

    return configure_chart(
        alt.layer(hundred, chart),
        height=310,
    )


def build_monthly_bess_revenue_chart(
    monthly: pd.DataFrame,
):
    if monthly is None or monthly.empty:
        return None

    long = monthly.melt(
        id_vars=["month"],
        value_vars=[
            "perfect_annualized_eur_mw_yr",
            "forecast_strategy_annualized_eur_mw_yr",
        ],
        var_name="Strategy",
        value_name="Annualized revenue",
    )
    long["Strategy"] = long["Strategy"].map(
        {
            "perfect_annualized_eur_mw_yr": (
                "Real-price perfect foresight"
            ),
            "forecast_strategy_annualized_eur_mw_yr": (
                "Forecast-selected hours"
            ),
        }
    )

    chart = (
        alt.Chart(long)
        .mark_bar()
        .encode(
            x=alt.X(
                "month:N",
                title=None,
                sort=monthly["month"].tolist(),
            ),
            y=alt.Y(
                "Annualized revenue:Q",
                title="Monthly behaviour annualized (€/MW/yr)",
            ),
            xOffset="Strategy:N",
            color=alt.Color(
                "Strategy:N",
                title="Strategy",
                scale=alt.Scale(
                    domain=[
                        "Real-price perfect foresight",
                        "Forecast-selected hours",
                    ],
                    range=[
                        "#2563EB",
                        "#F97316",
                    ],
                ),
            ),
            tooltip=[
                alt.Tooltip("month:N", title="Month"),
                alt.Tooltip(
                    "Strategy:N",
                    title="Strategy",
                ),
                alt.Tooltip(
                    "Annualized revenue:Q",
                    title="€/MW/yr",
                    format=",.0f",
                ),
            ],
        )
    )

    return configure_chart(chart, height=330)


# =========================================================
# APP
# =========================================================
st.title("Demand, PBF thermal gap and DA price - forecast test")

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
section_header("Step 1 - Day-ahead peninsular demand forecast")
st.caption(f"Forecast engine: {FORECAST_ENGINE_VERSION}")

st.caption(
    "Weather uses a fixed 00 UTC D-1 model run, not the latest live run. "
    "Demand history ends at D-2, PBF and price labels at D-1. Historical "
    "publication/revision timestamps of the market series are not fully audited. "
    "The target-day clearing price is used only for subsequent evaluation."
)

fc1, fc2, fc3, fc4 = st.columns([1.0, 1.0, 1.1, 1.35])
with fc1:
    forecast_target_day = st.date_input(
        "Forecast target day",
        value=date.today() + timedelta(days=1),
        min_value=date(2024, 3, 15),
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
            "Model forecast - dark green",
            "REE official D+1 forecast - bright green",
            "Previous week + recent trend - orange",
        ],
        index=0,
        key="forecast_downstream_demand_source",
    )

forecast_generation_technologies = st.multiselect(
    "PBF generation forecast (Hydro UGH excluded from structural gap)",
    options=list(FORECAST_NON_THERMAL_INDICATORS.keys()),
    default=FORECAST_NON_THERMAL_DEFAULT,
    help=(
        "Structural thermal gap = PBF-calibrated demand minus forecast NET "
        "wind, solar, nuclear, run-of-river and other renewables. Hydro UGH "
        "is flexible and is not deducted from the structural gap. "
        "Run-of-river corresponds to Hydro non-UGH."
    ),
    key="forecast_generation_technologies",
)

forecast_as_of_day = forecast_target_day - timedelta(days=1)
forecast_is_historical = forecast_target_day <= date.today()
weather_input_label = f"Fixed {FIXED_WEATHER_MODEL} run: {forecast_as_of_day} 00 UTC"

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
    forecast_stage = "Input validation"
    try:
        st.session_state.pop("day_ahead_result_v17_2_cow_fix", None)
        if not forecast_generation_technologies:
            raise ValueError(
                "Select at least one PBF generation technology."
            )

        forecast_stage = "Step 1/3: demand forecast"
        with st.spinner(
            "Step 1/3 - training tomorrow's demand forecast..."
        ):
            demand_result = generate_day_ahead_forecast(
                forecast_target_day,
                int(forecast_lookback),
                temperature_mode,
                float(forecast_trend_alpha),
            )

        forecast_for_market = demand_result["forecast"].copy()

        forecast_stage = "Official REE demand reference"
        ree_demand_result = load_ree_official_demand_forecast(
            forecast_target_day,
            token,
        )
        ree_demand_forecast = ree_demand_result["forecast"]
        forecast_for_market = forecast_for_market.merge(
            ree_demand_forecast,
            on="datetime",
            how="left",
        )

        if downstream_demand_source.startswith("Model"):
            forecast_for_market["selected_demand_mw"] = (
                forecast_for_market["forecast_mw"]
            )
            demand_source_label = "Model forecast - dark green"
        elif downstream_demand_source.startswith("REE"):
            if (
                not ree_demand_result["available"]
                or forecast_for_market[
                    "ree_forecast_mw"
                ].notna().sum() < 23
            ):
                raise ValueError(
                    "The official REE D+1 demand forecast is not available "
                    "for the selected delivery day yet. Select the model "
                    "forecast or rerun after REE publishes the D+1 curve."
                )
            forecast_for_market["selected_demand_mw"] = (
                forecast_for_market["ree_forecast_mw"]
            )
            demand_source_label = (
                "REE official D+1 forecast - bright green"
            )
        else:
            forecast_for_market["selected_demand_mw"] = (
                forecast_for_market[
                    "trend_adjusted_lag_7d"
                ]
            )
            demand_source_label = (
                "Previous week + recent trend - orange"
            )

        forecast_stage = "Step 2/3: PBF generation and thermal gap"
        with st.spinner(
            "Step 2/3 - forecasting PBF generation and thermal gap..."
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

        forecast_stage = "Step 3/3: DA price forecast"
        with st.spinner(
            "Step 3/3 - forecasting tomorrow's DA spot-price curve..."
        ):
            price_result = generate_price_forecast(
                forecast_target_day,
                thermal_result["historical"],
                thermal_result["forecast"],
                token,
            )

        # Loaded only after the forecast has been produced to prevent leakage.
        forecast_stage = "Post-forecast realised-price comparison"
        realised_prices = load_esios_price_history(
            forecast_target_day,
            forecast_target_day,
            token,
        )

        st.session_state["day_ahead_result_v17_2_cow_fix"] = {
            **demand_result,
            "forecast": forecast_for_market,
            "demand_source_label": demand_source_label,
            "thermal_result": thermal_result,
            "price_result": price_result,
            "realised_prices": realised_prices,
            "target_day": forecast_target_day,
            "as_of_day": forecast_as_of_day,
            "weather_input_label": weather_input_label,
            "ree_demand_source": ree_demand_result["source"],
            "ree_demand_available": ree_demand_result["available"],
            "ree_demand_error": ree_demand_result["error"],
            "generation_technologies": list(
                forecast_generation_technologies
            ),
        }

    except Exception as exc:
        details = _v17_failure_details(exc, forecast_stage)
        st.error(
            f"Day-ahead forecast failed during {forecast_stage}: "
            f"{details['exception']}: {details['message']}"
        )
        with st.expander("Technical error details", expanded=True):
            st.write({k: v for k, v in details.items() if k != "traceback"})
            st.code(details["traceback"], language="text")

forecast_result = st.session_state.get("day_ahead_result_v17_2_cow_fix")
if forecast_result:
    if forecast_result.get("target_day") != forecast_target_day:
        st.info(f"Showing the last successful forecast for {forecast_result.get('target_day')}. Press Generate to run the selected date.")
    forecast_df = forecast_result["forecast"]
    peak = forecast_df.loc[forecast_df["forecast_mw"].idxmax()]
    minimum = forecast_df.loc[forecast_df["forecast_mw"].idxmin()]

    ree_available = (
        "ree_forecast_mw" in forecast_df.columns
        and forecast_df["ree_forecast_mw"].notna().sum() >= 23
    )

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric(
        "Model average",
        f"{forecast_df['forecast_mw'].mean():,.0f} MW",
    )
    m2.metric(
        "Model peak",
        f"{peak['forecast_mw']:,.0f} MW",
        delta=f"{int(peak['hour']):02d}:00",
        delta_color="off",
    )
    m3.metric(
        "Model minimum",
        f"{minimum['forecast_mw']:,.0f} MW",
        delta=f"{int(minimum['hour']):02d}:00",
        delta_color="off",
    )
    m4.metric(
        "REE official average",
        (
            f"{forecast_df['ree_forecast_mw'].mean():,.0f} MW"
            if ree_available
            else "Unavailable"
        ),
        delta=(
            f"{forecast_df['ree_forecast_mw'].mean() - forecast_df['forecast_mw'].mean():+,.0f} MW vs model"
            if ree_available
            else None
        ),
        delta_color="off",
    )
    m5.metric(
        "Forecast temperature",
        f"{forecast_df['temperature_c'].mean():,.1f} °C",
    )

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
          <span><span style="display:inline-block;width:30px;border-top:3px solid #0F766E;margin-right:7px;vertical-align:middle;"></span>Model forecast</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dashed #22C55E;margin-right:7px;vertical-align:middle;"></span>REE official D+1 forecast</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dashed #F97316;margin-right:7px;vertical-align:middle;"></span>Previous week + recent trend</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dashed #64748B;margin-right:7px;vertical-align:middle;"></span>Same weekday previous week</span>
          <span><span style="display:inline-block;width:30px;border-top:3px dotted #93C5FD;margin-right:7px;vertical-align:middle;"></span>D-2 actual reference</span>
          <span><span style="display:inline-block;width:30px;height:12px;background:rgba(16,185,129,0.18);border:1px solid rgba(16,185,129,0.35);margin-right:7px;vertical-align:middle;"></span>P10–P90 uncertainty band</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.altair_chart(
        build_day_ahead_chart(forecast_df),
        use_container_width=True,
    )

    if forecast_result.get("ree_demand_available", False):
        model_vs_ree_mae = (
            forecast_df["forecast_mw"]
            - forecast_df["ree_forecast_mw"]
        ).abs().mean()
        st.caption(
            "Official REE demand source: "
            f"{forecast_result.get('ree_demand_source')}. "
            f"Mean absolute difference between our model and REE: "
            f"{model_vs_ree_mae:,.0f} MW."
        )
    else:
        st.info(
            "The official REE D+1 curve is not available for this run. "
            "The proprietary demand forecast remains available."
        )

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
    # STEP 2 - PBF generation and forecast thermal gap
    # =====================================================
    if thermal_result is not None:
        section_header(
            "Step 2 - Forecast PBF generation and thermal gap"
        )

        thermal_forecast = thermal_result["forecast"]
        generation_long = thermal_result["generation_long"]

        st.caption(
            "The solid black line is total forecast PBF demand. The dashed "
            "green line is demand exposed to the day-ahead auction after "
            "deducting the bilateral programme. The stacked area is "
            "non-bilateral low-marginal-cost PBF generation. The thermal gap "
            "and price model use the dashed green DA-residual demand, not the "
            "solid total-demand line. No fixed 8.7/9 GW self-consumption "
            "plant is added to or deducted from this gap; the explicit "
            "self-consumption adjustment is 0 MW. PBF demand is the physical "
            "forecast plus a weekday/hour historical perimeter offset, not a "
            "blend with yesterday's PBF level. Base physical-demand source: "
            f"{forecast_result.get('demand_source_label', 'Model forecast')}."
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

        tg1, tg2, tg3, tg4, tg5, tg6, tg7, tg8 = st.columns(8)
        tg1.metric(
            "Average total PBF demand",
            f"{thermal_forecast['pbf_demand_forecast_mwh'].mean():,.0f} MW",
        )
        tg2.metric(
            "Average bilateral demand",
            f"{thermal_forecast['bilateral_sales_forecast_mwh'].mean():,.0f} MW",
        )
        tg3.metric(
            "Average DA residual demand",
            f"{thermal_forecast['da_demand_forecast_mwh'].mean():,.0f} MW",
        )
        tg4.metric(
            "Average non-bilateral low-MC PBF",
            f"{thermal_forecast['non_thermal_forecast_mwh'].mean():,.0f} MW",
        )
        tg5.metric(
            "Average DA thermal gap",
            f"{thermal_gap.mean():,.0f} MW",
        )
        tg6.metric(
            "Peak DA thermal gap",
            f"{thermal_gap.max():,.0f} MW",
        )
        tg7.metric(
            "DA demand share",
            (
                f"{thermal_forecast['da_demand_share_of_total_pct'].mean():,.1f}%"
                if "da_demand_share_of_total_pct"
                in thermal_forecast.columns
                else "-"
            ),
            help=(
                "Share of total PBF demand remaining after deducting the "
                "bilateral programme. This explains why DA residual demand "
                "can be substantially below physical system demand."
            ),
        )
        tg8.metric(
            "Hydro UGH forecast",
            (
                f"{thermal_forecast['hydro_ugh_forecast_mwh'].mean():,.0f} MW"
                if "hydro_ugh_forecast_mwh"
                in thermal_forecast.columns
                else "-"
            ),
            help=(
                "Non-bilateral Hydro UGH is shown separately because it is "
                "dispatchable and is not deducted from the structural gap."
            ),
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
                f"DA-residual thermal gap had a minimum of "
                f"{d1_min_gap:,.0f} MW"
                + (
                    f", while the minimum DA price was "
                    f"{d1_min_price:,.2f} €/MWh."
                    if pd.notna(d1_min_price)
                    else "."
                )
                + " This is a diagnostic reference, not a constraint on tomorrow's prices."
            )

        st.caption(
            "DA structural thermal gap = (total PBF demand − total bilateral "
            "sales) − non-bilateral wind − solar − nuclear − run-of-river − "
            "other renewables. Hydro UGH is dispatchable and is shown "
            "separately. The orange dashed line is the D-1 reference calculated "
            "with exactly the same DA-market perimeter."
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
                - **DA residual demand** is total programmed PBF demand minus total bilateral PBF sales (indicator 10235).
                - Each technology is forecast as **non-bilateral PBF generation**: total PBF minus its bilateral programme.
                - The price-relevant gap is **DA residual demand minus non-bilateral low-MC generation**.
                - **Run-of-river** corresponds to Hydro non-UGH and is treated as non-dispatchable.
                - **Hydro UGH is forecast separately but is not deducted**, because its dispatch responds to price and scarcity.
                - The former embedded 16-Jul profile is not used as a fallback because it does not contain total bilateral demand and would mix perimeters.
                """
            )

    # =====================================================
    # STEP 3 - forecast DA spot price
    # =====================================================
    if price_result is not None:
        section_header(
            "Step 3 - Forecast day-ahead spot prices"
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
                else "-"
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
            "Forecast hours <= 5 EUR/MWh",
            f"{int((price_forecast['forecast_price_eur_mwh'] <= 5.0).sum())} h",
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
            f"Conditional validation MAE: {price_result['model_stats']['mae']:,.2f} EUR/MWh "
            f"versus {price_result['anchor_stats']['mae']:,.2f} for the anchor. "
            "Candidate weights are selected using earlier, out-of-sample predictions. "
            "No seasonal shape, zero-price floor or negative-gap cap is imposed. "
            "The shaded band is indicative conditional residual P10-P90, not a "
            "fully calibrated interval including input-forecast uncertainty. "
            f"{price_result.get('validation_scope', '')}"
        )
        with st.expander("V17 candidate weights and conditional validation"):
            st.dataframe(price_result.get("candidate_scores", pd.DataFrame()), use_container_width=True, hide_index=True)
            validation_export = price_result.get("conditional_validation", pd.DataFrame())
            st.download_button("Download conditional price validation CSV", validation_export.to_csv(index=False).encode("utf-8"),
                               file_name="v17_conditional_price_validation.csv", mime="text/csv")


        st.caption(
            price_result.get(
                "gas_data_message",
                "MIBGAS source status unavailable.",
            )
        )

        # =================================================
        # STEP 4 - compare against realised prices
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
                    "Step 4 - Forecast versus realised DA prices"
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
                    f"Forecast reference date: {forecast_result.get('as_of_day'):%d/%m/%Y}. "
                    "Market training labels exclude the delivery date; weather uses the fixed 00 UTC D-1 run. "
                    "Source publication vintages are not fully audited. Target-day clearing prices are "
                    "loaded separately for evaluation, even when the market has already cleared."
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


                # =============================================
                # STEP 5 - BESS revenue value of the forecast
                # =============================================
                section_header(
                    "Step 5 - BESS revenue captured by the price forecast"
                )

                actual_perfect_schedule = (
                    optimize_chronological_tb4_schedule(
                        comparison,
                        price_column=(
                            "actual_price_eur_mwh"
                        ),
                        rte=BESS_RTE,
                    )
                )
                forecast_optimal_schedule = (
                    optimize_chronological_tb4_schedule(
                        comparison,
                        price_column=(
                            "forecast_price_eur_mwh"
                        ),
                        rte=BESS_RTE,
                    )
                )
                forecast_schedule_settlement = (
                    settle_bess_schedule_on_actual_prices(
                        forecast_optimal_schedule,
                        comparison,
                        actual_price_column=(
                            "actual_price_eur_mwh"
                        ),
                        rte=BESS_RTE,
                    )
                )

                if (
                    actual_perfect_schedule is not None
                    and forecast_schedule_settlement
                    is not None
                ):
                    perfect_daily_revenue = float(
                        actual_perfect_schedule[
                            "daily_revenue_eur_mw"
                        ]
                    )
                    forecast_realized_daily_revenue = (
                        float(
                            forecast_schedule_settlement[
                                "actual_daily_revenue_eur_mw"
                            ]
                        )
                    )

                    perfect_annualized = float(
                        actual_perfect_schedule[
                            "annualized_revenue_eur_mw_yr"
                        ]
                    )
                    forecast_realized_annualized = (
                        float(
                            forecast_schedule_settlement[
                                "actual_annualized_revenue_eur_mw_yr"
                            ]
                        )
                    )

                    opportunity_loss = (
                        perfect_daily_revenue
                        - forecast_realized_daily_revenue
                    )
                    annualized_opportunity_loss = (
                        perfect_annualized
                        - forecast_realized_annualized
                    )

                    revenue_capture = (
                        forecast_realized_daily_revenue
                        / perfect_daily_revenue
                        * 100.0
                        if abs(perfect_daily_revenue)
                        > 1e-9
                        else np.nan
                    )

                    charge_overlap = len(
                        set(
                            actual_perfect_schedule[
                                "charge_hours"
                            ]
                        ).intersection(
                            set(
                                forecast_optimal_schedule[
                                    "charge_hours"
                                ]
                            )
                        )
                    )
                    discharge_overlap = len(
                        set(
                            actual_perfect_schedule[
                                "discharge_hours"
                            ]
                        ).intersection(
                            set(
                                forecast_optimal_schedule[
                                    "discharge_hours"
                                ]
                            )
                        )
                    )

                    b1, b2, b3, b4 = st.columns(4)
                    b1.metric(
                        "Real chronological TB4",
                        (
                            f"{actual_perfect_schedule['chronological_tb4_eur_mwh']:,.2f} "
                            "€/MWh"
                        ),
                        help=(
                            "Four cheapest realised hours followed "
                            "chronologically by four realised discharge hours."
                        ),
                    )
                    b2.metric(
                        "Perfect-foresight revenue",
                        (
                            f"{perfect_daily_revenue:,.2f} "
                            "€/MW-day"
                        ),
                        delta=(
                            f"{perfect_annualized:,.0f} "
                            "€/MW/yr"
                        ),
                        delta_color="off",
                    )
                    b3.metric(
                        "Forecast-strategy revenue",
                        (
                            f"{forecast_realized_daily_revenue:,.2f} "
                            "€/MW-day"
                        ),
                        delta=(
                            f"{forecast_realized_annualized:,.0f} "
                            "€/MW/yr"
                        ),
                        delta_color="off",
                    )
                    b4.metric(
                        "Revenue capture",
                        (
                            f"{revenue_capture:,.1f}%"
                            if pd.notna(revenue_capture)
                            else "-"
                        ),
                        delta=(
                            f"{-opportunity_loss:,.2f} "
                            "€/MW-day vs optimum"
                        ),
                        delta_color=(
                            "normal"
                            if opportunity_loss <= 0
                            else "inverse"
                        ),
                    )

                    curve_benchmark = (
                        evaluate_bess_price_curve_benchmarks(
                            price_forecast=price_forecast,
                            actual_price_frame=comparison,
                            perfect_schedule=(
                                actual_perfect_schedule
                            ),
                            actual_price_column=(
                                "actual_price_eur_mwh"
                            ),
                            rte=BESS_RTE,
                        )
                    )

                    if not curve_benchmark.empty:
                        st.markdown(
                            "#### Revenue capture by price curve"
                        )
                        st.caption(
                            "Each curve independently selects its four "
                            "charge hours and four later discharge hours. "
                            "Every schedule is then settled against the same "
                            "real OMIE prices. The hourly TB4 benchmark and chronological "
                            "BESS schedule are different metrics."
                        )

                        benchmark_display_order = [
                            "final_forecast",
                            "previous_day",
                            "previous_week",
                            "price_anchor",
                            "similar_gap",
                        ]
                        benchmark_by_slug = (
                            curve_benchmark.set_index("Slug")
                        )

                        benchmark_columns = st.columns(
                            len(benchmark_display_order)
                        )
                        final_capture_for_delta = (
                            float(
                                benchmark_by_slug.loc[
                                    "final_forecast",
                                    "Revenue capture (%)",
                                ]
                            )
                            if "final_forecast"
                            in benchmark_by_slug.index
                            else np.nan
                        )

                        for metric_column, slug in zip(
                            benchmark_columns,
                            benchmark_display_order,
                        ):
                            if slug not in benchmark_by_slug.index:
                                continue

                            row = benchmark_by_slug.loc[slug]
                            capture_value = float(
                                row["Revenue capture (%)"]
                            )
                            realised_value = float(
                                row[
                                    "Realised revenue (€/MW-day)"
                                ]
                            )

                            if slug == "final_forecast":
                                delta_text = (
                                    f"{realised_value:,.2f} "
                                    "€/MW-day realised"
                                )
                                delta_color = "off"
                            else:
                                difference_pp = (
                                    capture_value
                                    - final_capture_for_delta
                                )
                                delta_text = (
                                    f"{difference_pp:+,.1f} pp "
                                    "vs final"
                                )
                                delta_color = (
                                    "normal"
                                    if difference_pp >= 0
                                    else "inverse"
                                )

                            metric_column.metric(
                                label=str(
                                    row["Short label"]
                                ),
                                value=(
                                    f"{capture_value:,.1f}%"
                                ),
                                delta=delta_text,
                                delta_color=delta_color,
                                help=(
                                    f"Realised revenue: "
                                    f"{realised_value:,.2f} €/MW-day. "
                                    f"Charge: {row['Charge hours']}. "
                                    f"Discharge: "
                                    f"{row['Discharge hours']}."
                                ),
                            )

                        best_curve = curve_benchmark.iloc[0]
                        final_row = curve_benchmark[
                            curve_benchmark["Slug"]
                            == "final_forecast"
                        ]

                        if (
                            not final_row.empty
                            and best_curve["Slug"]
                            != "final_forecast"
                        ):
                            st.info(
                                f"Best curve for this day: "
                                f"**{best_curve['Strategy']}**, capturing "
                                f"{best_curve['Revenue capture (%)']:,.1f}% "
                                f"of perfect-foresight revenue, "
                                f"{best_curve['Difference vs final (pp)']:+,.1f} "
                                "percentage points versus the final black "
                                "forecast. This is a useful signal for testing "
                                "blended price curves, but the weighting should "
                                "be decided from the multi-day walk-forward "
                                "results rather than from one day."
                            )

                        curve_capture_chart = (
                            build_price_curve_capture_chart(
                                curve_benchmark
                            )
                        )
                        if curve_capture_chart is not None:
                            st.altair_chart(
                                curve_capture_chart,
                                use_container_width=True,
                            )

                        with st.expander(
                            "Detailed BESS capture by price curve"
                        ):
                            st.dataframe(
                                curve_benchmark.style.format(
                                    {
                                        "Revenue capture (%)": (
                                            "{:,.1f}%"
                                        ),
                                        "Realised revenue (€/MW-day)": (
                                            "{:,.2f}"
                                        ),
                                        "Expected revenue (€/MW-day)": (
                                            "{:,.2f}"
                                        ),
                                        "Annualized realised revenue (€/MW/yr)": (
                                            "{:,.0f}"
                                        ),
                                        "Opportunity loss (€/MW-day)": (
                                            "{:,.2f}"
                                        ),
                                        "Difference vs final (pp)": (
                                            "{:+,.1f}"
                                        ),
                                        "Forecast chronological TB4 (€/MWh)": (
                                            "{:,.2f}"
                                        ),
                                        "Realised RTE-adjusted spread (€/MWh)": (
                                            "{:,.2f}"
                                        ),
                                    }
                                ),
                                use_container_width=True,
                                hide_index=True,
                            )

                    b5, b6, b7, b8 = st.columns(4)
                    b5.metric(
                        "Forecast expected revenue",
                        (
                            f"{forecast_optimal_schedule['daily_revenue_eur_mw']:,.2f} "
                            "€/MW-day"
                        ),
                        help=(
                            "Revenue expected from the forecast curve "
                            "before actual OMIE settlement."
                        ),
                    )
                    b6.metric(
                        "Realized symmetric-efficiency spread",
                        (
                            f"{forecast_schedule_settlement['actual_rte_adjusted_spread_eur_mwh']:,.2f} "
                            "€/MWh"
                        ),
                    )
                    b7.metric(
                        "Selected-hour overlap",
                        (
                            f"{charge_overlap}/4 charge · "
                            f"{discharge_overlap}/4 discharge"
                        ),
                    )
                    b8.metric(
                        "Annualized opportunity loss",
                        (
                            f"{annualized_opportunity_loss:,.0f} "
                            "€/MW/yr"
                        ),
                    )

                    st.altair_chart(
                        build_bess_annualized_revenue_chart(
                            perfect_annualized,
                            forecast_realized_annualized,
                        ),
                        use_container_width=True,
                    )

                    dispatch_chart = (
                        build_bess_dispatch_comparison_chart(
                            comparison,
                            actual_perfect_schedule,
                            forecast_schedule_settlement,
                        )
                    )
                    if dispatch_chart is not None:
                        st.altair_chart(
                            dispatch_chart,
                            use_container_width=True,
                        )

                    st.caption(
                        "Both strategies use the same 1 MW / 4 MWh "
                        "battery, start the day empty and complete one "
                        "full chronological cycle. The 85% round-trip "
                        "efficiency is split symmetrically, so both one-way "
                        "efficiencies equal sqrt(0.85) = 92.20%. For each "
                        "1 MWh stored, the grid purchase is 1/sqrt(0.85) "
                        "MWh; for each 1 MWh discharged from the battery, "
                        "sqrt(0.85) MWh is sold to the grid. Daily revenue "
                        "is therefore calculated as sale price × sqrt(0.85) "
                        "minus purchase price / sqrt(0.85), and is annualized by "
                        "multiplying by 365. The forecast strategy "
                        "selects its hours using forecast prices but "
                        "is settled entirely against realised OMIE "
                        "prices."
                    )

                    schedule_table = build_bess_schedule_table(
                        comparison,
                        actual_perfect_schedule,
                        forecast_optimal_schedule,
                    )

                    with st.expander(
                        "Selected BESS charge and discharge hours"
                    ):
                        st.dataframe(
                            schedule_table.style.format(
                                {
                                    "Forecast price (€/MWh)": (
                                        "{:,.2f}"
                                    ),
                                    "Real OMIE price (€/MWh)": (
                                        "{:,.2f}"
                                    ),
                                    "Grid power (MW)": "{:,.2f}",
                                }
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )

                        bess_summary_export = pd.DataFrame(
                            [
                                {
                                    "Strategy": (
                                        "Real-price perfect foresight"
                                    ),
                                    "Chronological TB4 (€/MWh)": (
                                        actual_perfect_schedule[
                                            "chronological_tb4_eur_mwh"
                                        ]
                                    ),
                                    "RTE-adjusted spread (€/MWh)": (
                                        actual_perfect_schedule[
                                            "rte_adjusted_spread_eur_mwh"
                                        ]
                                    ),
                                    "Daily revenue (€/MW-day)": (
                                        perfect_daily_revenue
                                    ),
                                    "Annualized revenue (€/MW/yr)": (
                                        perfect_annualized
                                    ),
                                },
                                {
                                    "Strategy": (
                                        "Forecast-selected, settled real"
                                    ),
                                    "Chronological TB4 (€/MWh)": (
                                        forecast_schedule_settlement[
                                            "actual_chronological_tb4_eur_mwh"
                                        ]
                                    ),
                                    "RTE-adjusted spread (€/MWh)": (
                                        forecast_schedule_settlement[
                                            "actual_rte_adjusted_spread_eur_mwh"
                                        ]
                                    ),
                                    "Daily revenue (€/MW-day)": (
                                        forecast_realized_daily_revenue
                                    ),
                                    "Annualized revenue (€/MW/yr)": (
                                        forecast_realized_annualized
                                    ),
                                },
                            ]
                        )

                        st.download_button(
                            "Download BESS forecast-value backtest CSV",
                            data=(
                                bess_summary_export.to_csv(
                                    index=False
                                ).encode("utf-8")
                            ),
                            file_name=(
                                "bess_forecast_value_"
                                f"{forecast_result.get('target_day')}.csv"
                            ),
                            mime="text/csv",
                            use_container_width=True,
                        )
                else:
                    st.warning(
                        "The BESS forecast-value comparison could "
                        "not be calculated because fewer than eight "
                        "complete hourly forecast/real price points "
                        "were available."
                    )
        else:
            section_header(
                "Step 4 - Forecast versus realised DA prices"
            )
            st.info(
                "Real DA prices are not available yet for the selected "
                "delivery date. This comparison will appear once ESIOS "
                "publishes them and the forecast is rerun."
            )

        requested_price_output_columns = ["datetime"] + [
            c for c in price_forecast.columns
            if c not in {"datetime", "date", "hour", "thermal_gap_mwh", "engine_version"}
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
        missing_generation_columns = [c for c in generation_export if c == "datetime" or c not in complete_output]
        complete_output = complete_output.merge(generation_export[missing_generation_columns], on="datetime", how="left")

        with st.expander("V17 post-clearing gap diagnosis (not an ex-ante input)"):
            st.caption(
                "Compares the forecast with the published target-day PBF, not metered physical generation. "
                "Positive contributions mean the component made the forecast gap too high. "
                "These target-day outcomes are never fed back into the forecast."
            )
            diagnostic_key = f"v17_gap_diagnostic_{forecast_result.get('target_day')}_{'|'.join(forecast_result.get('generation_technologies', []))}"
            if st.button("Load post-clearing PBF diagnosis", key="v17_load_gap_diagnosis"):
                try:
                    with st.spinner("Loading published target-day PBF for ex-post diagnosis only..."):
                        diagnostic = build_post_clearing_gap_diagnostic(
                            forecast_result.get("target_day"), forecast_result.get("generation_technologies", []),
                            thermal_forecast, price_forecast, token,
                        )
                        st.session_state[diagnostic_key] = diagnostic
                except Exception as exc:
                    st.warning(f"Post-clearing diagnosis unavailable: {exc}")
            diagnostic = st.session_state.get(diagnostic_key)
            if diagnostic is not None:
                st.dataframe(diagnostic, use_container_width=True, hide_index=True)
                st.download_button("Download ex-post gap diagnosis CSV", diagnostic.to_csv(index=False).encode("utf-8"),
                                   file_name=f"ex_post_gap_diagnostic_{forecast_result.get('target_day')}.csv", mime="text/csv")

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
# YTD DAILY WALK-FORWARD BACKTEST UI
# =========================================================
section_header(
    "Step 6 - 2026 YTD daily walk-forward forecast backtest"
)

st.caption(
    "This section reruns the complete demand → net-PBF generation → "
    "structural thermal-gap → DA-price → BESS workflow independently for "
    "every selected delivery day. Each day uses only information available "
    "at D-1/D-2, selects the BESS hours from the forecast and settles those "
    "hours against realised OMIE prices."
)

yt1, yt2, yt3, yt4 = st.columns(
    [1.0, 1.0, 1.15, 1.25]
)

with yt1:
    ytd_start_day = st.date_input(
        "Walk-forward start",
        value=date(2026, 1, 1),
        min_value=date(2024, 3, 15),
        max_value=max(
            date.today() - timedelta(days=1),
            date(2024, 3, 15),
        ),
        key="ytd_walk_forward_start",
    )

with yt2:
    ytd_end_day = st.date_input(
        "Walk-forward end",
        value=max(
            date(2026, 1, 1),
            date.today() - timedelta(days=1),
        ),
        min_value=ytd_start_day,
        max_value=max(
            date.today() - timedelta(days=1),
            ytd_start_day,
        ),
        key="ytd_walk_forward_end",
    )

with yt3:
    ytd_batch_size_label = st.selectbox(
        "Days processed per click",
        [
            "3 days",
            "7 days",
            "14 days",
            "31 days",
            "All pending days",
        ],
        index=1,
        help=(
            "The exact daily model is computationally heavy. "
            "Run it in batches and download a checkpoint after each batch."
        ),
        key="ytd_walk_forward_batch_size",
    )

with yt4:
    ytd_retry_failed = st.checkbox(
        "Retry failed dates",
        value=True,
        help=(
            "Retries dates that previously failed because of an API or "
            "temporary data-availability issue."
        ),
        key="ytd_retry_failed",
    )

yc1, yc2, yc3, yc4 = st.columns(
    [1.0, 1.0, 1.1, 1.35]
)

with yc1:
    ytd_lookback_days = st.select_slider(
        "YTD training history",
        options=[365, 450, 550, 650, 730],
        value=550,
        format_func=lambda value: f"{value} days",
        key="ytd_walk_forward_lookback",
    )

with yc2:
    ytd_trend_alpha = st.slider(
        "YTD weekly-trend α",
        min_value=0.0,
        max_value=1.5,
        value=1.0,
        step=0.1,
        key="ytd_walk_forward_trend_alpha",
    )

with yc3:
    ytd_demand_source = st.selectbox(
        "YTD demand curve",
        [
            "Model forecast - green",
            "Previous week + recent trend - orange",
        ],
        index=0,
        key="ytd_walk_forward_demand_source",
    )

with yc4:
    ytd_temperature_mode = st.selectbox(
        "YTD temperature proxy",
        [
            "Spain weighted proxy",
            "Madrid",
        ],
        index=0,
        key="ytd_walk_forward_temperature_mode",
    )

ytd_technologies = st.multiselect(
    "YTD structural-gap technologies",
    options=list(
        FORECAST_NON_THERMAL_INDICATORS.keys()
    ),
    default=FORECAST_NON_THERMAL_DEFAULT,
    help=(
        "Hydro UGH may be forecast as a diagnostic but remains excluded "
        "from the structural thermal gap."
    ),
    key="ytd_walk_forward_technologies",
)

checkpoint_upload = st.file_uploader(
    "Optional: upload a previous YTD checkpoint CSV",
    type=["csv"],
    key="ytd_checkpoint_upload",
)

load_checkpoint_col, reset_checkpoint_col = st.columns(2)

with load_checkpoint_col:
    load_checkpoint = st.button(
        "Load uploaded checkpoint",
        use_container_width=True,
        disabled=checkpoint_upload is None,
        key="load_ytd_checkpoint",
    )

with reset_checkpoint_col:
    reset_checkpoint = st.button(
        "Reset YTD backtest",
        use_container_width=True,
        key="reset_ytd_checkpoint",
    )

if reset_checkpoint:
    st.session_state[
        YTD_BACKTEST_STATE_KEY
    ] = pd.DataFrame()
    st.success("YTD backtest results were reset.")

if load_checkpoint and checkpoint_upload is not None:
    try:
        uploaded_checkpoint = pd.read_csv(
            checkpoint_upload
        )
        uploaded_checkpoint = (
            normalize_walk_forward_results(
                uploaded_checkpoint
            )
        )
        if "checkpoint_version" not in uploaded_checkpoint or not uploaded_checkpoint["checkpoint_version"].eq(YTD_BACKTEST_CHECKPOINT_VERSION).all():
            raise ValueError("This checkpoint uses another engine version. Do not combine V16 and V17 results.")
        st.session_state[
            YTD_BACKTEST_STATE_KEY
        ] = uploaded_checkpoint
        st.success(
            f"Loaded {len(uploaded_checkpoint):,} checkpoint rows."
        )
    except Exception as exc:
        st.error(f"Could not load checkpoint: {exc}")

existing_ytd_results = normalize_walk_forward_results(
    st.session_state.get(
        YTD_BACKTEST_STATE_KEY,
        pd.DataFrame(),
    )
)

all_target_days = [
    timestamp.date()
    for timestamp in pd.date_range(
        start=ytd_start_day,
        end=ytd_end_day,
        freq="D",
    )
]

processed_status = {}
if not existing_ytd_results.empty:
    processed_status = {
        row.target_day: row.status
        for row in existing_ytd_results[
            ["target_day", "status"]
        ].itertuples(index=False)
    }

pending_target_days = []
for target_day_value in all_target_days:
    status = processed_status.get(target_day_value)

    if status is None:
        pending_target_days.append(target_day_value)
    elif status == "error" and ytd_retry_failed:
        pending_target_days.append(target_day_value)

batch_size_map = {
    "3 days": 3,
    "7 days": 7,
    "14 days": 14,
    "31 days": 31,
    "All pending days": len(pending_target_days),
}
selected_batch_size = batch_size_map[
    ytd_batch_size_label
]

status_1, status_2, status_3, status_4 = st.columns(4)
status_1.metric(
    "Dates in selected range",
    f"{len(all_target_days):,}",
)
status_2.metric(
    "Completed dates",
    f"{sum(status == 'ok' for status in processed_status.values()):,}",
)
status_3.metric(
    "Failed dates",
    f"{sum(status == 'error' for status in processed_status.values()):,}",
)
status_4.metric(
    "Pending / retry dates",
    f"{len(pending_target_days):,}",
)

st.warning(
    "An exact January-to-date run retrains the demand, generation and price "
    "models for every delivery day and makes many API calls. A full YTD run "
    "can take a long time on Streamlit Cloud. The checkpoint lets you process "
    "it safely in several batches without losing completed dates."
)

run_ytd_batch = st.button(
    "Run next YTD walk-forward batch",
    type="primary",
    use_container_width=True,
    disabled=(
        not pending_target_days
        or not ytd_technologies
    ),
    key="run_ytd_walk_forward_batch",
)

if run_ytd_batch:
    try:
        ytd_token = require_esios_token()
    except Exception as exc:
        st.error(str(exc))
        ytd_token = None

    if ytd_token is not None:
        dates_to_run = pending_target_days[
            :selected_batch_size
        ]

        progress = st.progress(0.0)
        progress_text = st.empty()

        result_rows = (
            existing_ytd_results.to_dict(
                orient="records"
            )
            if not existing_ytd_results.empty
            else []
        )

        for index_value, target_day_value in enumerate(
            dates_to_run,
            start=1,
        ):
            progress_text.write(
                "Running delivery date "
                f"**{target_day_value:%d/%m/%Y}** "
                f"({index_value}/{len(dates_to_run)})"
            )

            try:
                result_row = (
                    run_one_walk_forward_backtest_day(
                        target_day=target_day_value,
                        lookback_days=int(
                            ytd_lookback_days
                        ),
                        temperature_mode=(
                            ytd_temperature_mode
                        ),
                        trend_alpha=float(
                            ytd_trend_alpha
                        ),
                        demand_source=(
                            ytd_demand_source
                        ),
                        technologies_tuple=tuple(
                            ytd_technologies
                        ),
                        _token=ytd_token,
                    )
                )
            except Exception as exc:
                result_row = failed_walk_forward_row(
                    target_day_value,
                    exc,
                )

            result_rows = [
                row
                for row in result_rows
                if str(row.get("target_day"))
                != target_day_value.isoformat()
                and row.get("target_day")
                != target_day_value
            ]
            result_rows.append(result_row)

            interim = normalize_walk_forward_results(
                pd.DataFrame(result_rows)
            )
            st.session_state[
                YTD_BACKTEST_STATE_KEY
            ] = interim

            progress.progress(
                index_value / len(dates_to_run)
            )

        progress_text.empty()
        progress.empty()

        completed_batch = normalize_walk_forward_results(
            st.session_state[
                YTD_BACKTEST_STATE_KEY
            ]
        )
        batch_successes = completed_batch[
            completed_batch["target_day"].isin(
                dates_to_run
            )
            & (completed_batch["status"] == "ok")
        ]
        batch_failures = completed_batch[
            completed_batch["target_day"].isin(
                dates_to_run
            )
            & (completed_batch["status"] == "error")
        ]

        st.success(
            f"Batch finished: {len(batch_successes)} successful "
            f"and {len(batch_failures)} failed dates."
        )

ytd_results = normalize_walk_forward_results(
    st.session_state.get(
        YTD_BACKTEST_STATE_KEY,
        existing_ytd_results,
    )
)

if not ytd_results.empty:
    st.download_button(
        "Download / save YTD checkpoint CSV",
        data=ytd_results.to_csv(
            index=False
        ).encode("utf-8"),
        file_name=(
            f"ytd_walk_forward_checkpoint_"
            f"{ytd_start_day}_{ytd_end_day}.csv"
        ),
        mime="text/csv",
        use_container_width=True,
        key="download_ytd_checkpoint",
    )

    ytd_range_results = ytd_results[
        ytd_results["target_day"].isin(
            all_target_days
        )
    ].copy()

    successful_ytd = ytd_range_results[
        ytd_range_results["status"] == "ok"
    ].copy()
    failed_ytd = ytd_range_results[
        ytd_range_results["status"] == "error"
    ].copy()

    if not successful_ytd.empty:
        numeric_columns = [
            column
            for column in successful_ytd.columns
            if column
            not in {
                "checkpoint_version",
                "target_day",
                "as_of_day",
                "status",
                "error",
                "demand_source",
                "temperature_mode",
                "technologies",
                "perfect_charge_hours",
                "perfect_discharge_hours",
                "forecast_charge_hours",
                "forecast_discharge_hours",
            }
        ]
        for column in numeric_columns:
            successful_ytd[column] = pd.to_numeric(
                successful_ytd[column],
                errors="coerce",
            )

        tested_days = len(successful_ytd)

        perfect_total = successful_ytd[
            "perfect_foresight_daily_revenue_eur_mw"
        ].sum()
        forecast_total = successful_ytd[
            "forecast_strategy_realized_daily_revenue_eur_mw"
        ].sum()
        total_opportunity_loss = (
            perfect_total - forecast_total
        )
        aggregate_capture = (
            100.0
            * _safe_ratio(
                forecast_total,
                perfect_total,
            )
        )

        perfect_annualized = (
            perfect_total
            / tested_days
            * BESS_ANNUALIZATION_DAYS
        )
        forecast_annualized = (
            forecast_total
            / tested_days
            * BESS_ANNUALIZATION_DAYS
        )

        section_header(
            "YTD walk-forward results"
        )

        yr1, yr2, yr3, yr4, yr5, yr6 = st.columns(6)
        yr1.metric(
            "Successful days",
            f"{tested_days:,}",
        )
        yr2.metric(
            "Forecast strategy YTD",
            f"{forecast_total:,.0f} €/MW",
        )
        yr3.metric(
            "Perfect foresight YTD",
            f"{perfect_total:,.0f} €/MW",
        )
        yr4.metric(
            "YTD revenue capture",
            (
                f"{aggregate_capture:,.1f}%"
                if pd.notna(aggregate_capture)
                else "-"
            ),
        )
        yr5.metric(
            "Cumulative opportunity loss",
            f"{total_opportunity_loss:,.0f} €/MW",
        )
        yr6.metric(
            "Average price MAE",
            (
                f"{successful_ytd['price_mae_eur_mwh'].mean():,.2f} "
                "€/MWh"
            ),
        )

        ya1, ya2, ya3, ya4 = st.columns(4)
        ya1.metric(
            "Forecast strategy annualized",
            f"{forecast_annualized:,.0f} €/MW/yr",
        )
        ya2.metric(
            "Perfect foresight annualized",
            f"{perfect_annualized:,.0f} €/MW/yr",
        )
        ya3.metric(
            "Average hourly sMAPE",
            (
                f"{successful_ytd['price_smape_pct'].mean():,.1f}%"
            ),
        )
        ya4.metric(
            "Average selected-hour overlap",
            (
                f"{successful_ytd['charge_hour_overlap'].mean():,.2f}/4 "
                "charge · "
                f"{successful_ytd['discharge_hour_overlap'].mean():,.2f}/4 "
                "discharge"
            ),
        )

        ytd_curve_summary = (
            ytd_price_curve_capture_summary(
                successful_ytd
            )
        )

        if not ytd_curve_summary.empty:
            st.subheader(
                "YTD BESS revenue capture by price curve"
            )
            st.caption(
                "This is the relevant comparison for deciding whether the "
                "final black forecast should be blended with D-1, D-7, the "
                "price anchor or the similar-gap curve. It aggregates realised "
                "BESS revenue across all successfully backtested days."
            )

            ytd_curve_chart = (
                build_price_curve_capture_chart(
                    ytd_curve_summary.rename(
                        columns={
                            "YTD realised revenue (€/MW)": (
                                "Realised revenue (€/MW-day)"
                            )
                        }
                    )
                )
            )
            if ytd_curve_chart is not None:
                st.altair_chart(
                    ytd_curve_chart,
                    use_container_width=True,
                )

            st.dataframe(
                ytd_curve_summary.style.format(
                    {
                        "YTD realised revenue (€/MW)": (
                            "{:,.0f}"
                        ),
                        "Revenue capture (%)": (
                            "{:,.1f}%"
                        ),
                        "Annualized revenue (€/MW/yr)": (
                            "{:,.0f}"
                        ),
                        "YTD opportunity loss (€/MW)": (
                            "{:,.0f}"
                        ),
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

        cumulative_chart = (
            build_ytd_cumulative_revenue_chart(
                successful_ytd
            )
        )
        if cumulative_chart is not None:
            st.altair_chart(
                cumulative_chart,
                use_container_width=True,
            )

        rolling_chart = build_ytd_rolling_capture_chart(
            successful_ytd,
            rolling_days=30,
        )
        if rolling_chart is not None:
            st.altair_chart(
                rolling_chart,
                use_container_width=True,
            )

        monthly_ytd = aggregate_walk_forward_monthly(
            successful_ytd
        )
        monthly_chart = build_monthly_bess_revenue_chart(
            monthly_ytd
        )
        if monthly_chart is not None:
            st.altair_chart(
                monthly_chart,
                use_container_width=True,
            )

        st.subheader("Monthly walk-forward summary")
        st.dataframe(
            monthly_ytd.style.format(
                {
                    "price_mae_eur_mwh": "{:,.2f}",
                    "price_smape_pct": "{:,.1f}%",
                    "actual_baseload_eur_mwh": "{:,.2f}",
                    "forecast_baseload_eur_mwh": "{:,.2f}",
                    "perfect_revenue_eur_mw": "{:,.0f}",
                    "forecast_strategy_revenue_eur_mw": "{:,.0f}",
                    "opportunity_loss_eur_mw": "{:,.0f}",
                    "revenue_capture_pct": "{:,.1f}%",
                    "forecast_strategy_annualized_eur_mw_yr": "{:,.0f}",
                    "perfect_annualized_eur_mw_yr": "{:,.0f}",
                    "average_charge_overlap": "{:,.2f}",
                    "average_discharge_overlap": "{:,.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

        with st.expander(
            "Daily YTD walk-forward results"
        ):
            st.dataframe(
                successful_ytd.sort_values(
                    "target_day"
                ),
                use_container_width=True,
                hide_index=True,
            )

    if not failed_ytd.empty:
        with st.expander(
            f"Failed dates ({len(failed_ytd)})"
        ):
            st.dataframe(
                failed_ytd[
                    [
                        "target_day",
                        "as_of_day",
                        "error",
                    ]
                ].sort_values("target_day"),
                use_container_width=True,
                hide_index=True,
            )
else:
    st.info(
        "No YTD dates have been processed yet. Start with a small batch "
        "to validate the APIs, then continue until the selected range is "
        "complete."
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
