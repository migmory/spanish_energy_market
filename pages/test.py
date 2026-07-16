import os
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

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
DEMAND_BLUE = "#1D4ED8"
TEMPERATURE_ORANGE = "#EA580C"

MAX_RANGE_DAYS = 124
FORECAST_VALIDATION_DAYS = 42


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


def _weighted_hourly_temperature(payload, points: list[dict]) -> pd.DataFrame:
    if isinstance(payload, dict):
        payload = [payload]
    frames = []
    for idx, item in enumerate(payload):
        if idx >= len(points):
            continue
        hourly = item.get("hourly", {}) or {}
        times = hourly.get("time", []) or []
        values = hourly.get("temperature_2m", []) or []
        if not times or not values:
            continue
        frame = pd.DataFrame({
            "datetime": pd.to_datetime(times, errors="coerce"),
            "temperature_c": pd.to_numeric(pd.Series(values), errors="coerce"),
        })
        frame["weight"] = float(points[idx]["weight"])
        frames.append(frame.dropna(subset=["datetime", "temperature_c"]))

    if not frames:
        return pd.DataFrame(columns=["datetime", "temperature_c"])

    long = pd.concat(frames, ignore_index=True)
    long["weighted"] = long["temperature_c"] * long["weight"]
    out = long.groupby("datetime", as_index=False).agg(weighted=("weighted", "sum"), weight=("weight", "sum"))
    out["temperature_c"] = out["weighted"] / out["weight"]
    return out[["datetime", "temperature_c"]].sort_values("datetime")


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
    params = {
        "latitude": ",".join(str(p["latitude"]) for p in points),
        "longitude": ",".join(str(p["longitude"]) for p in points),
        "hourly": "temperature_2m",
        "timezone": "Europe/Madrid",
        "forecast_days": min(max((target_day - date.today()).days + 1, 1), 16),
    }
    response = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=120)
    response.raise_for_status()
    out = _weighted_hourly_temperature(response.json(), points)
    return out[out["datetime"].dt.date == target_day].reset_index(drop=True)


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
    for lag in [2, 3, 7, 9, 10, 14, 21, 28]:
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
# APP
# =========================================================
st.title("Demand, temperature and PBF — test")

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
section_header("Day-ahead peninsular demand forecast")

st.caption(
    "Prototype of a next-day hourly demand curve using calendar effects, "
    "forecast temperature, national holidays and historical demand lags. "
    "Demand is cut off at target D-2 so incomplete D-1 demand is never used."
)

fc1, fc2, fc3, fc4 = st.columns([1.0, 1.0, 1.1, 1.25])
with fc1:
    forecast_target_day = st.date_input(
        "Forecast target day",
        value=date.today() + timedelta(days=1),
        min_value=date.today(),
        max_value=date.today() + timedelta(days=15),
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
            "Adjusts the D-7 curve using the recent change between "
            "D-2 and D-9, blended with D-3 versus D-10. "
            "0 ignores the adjustment; 1 applies it fully."
        ),
        key="forecast_weekly_trend_alpha",
    )
with fc4:
    st.markdown(
        "<div style='padding-top:1.8rem;color:#475569;font-size:0.88rem;'>"
        "<b>Publication:</b> target D-1.<br>"
        "<b>Latest complete demand:</b> target D-2.<br>"
        "<b>Trend pair:</b> D-2 vs D-9."
        "</div>",
        unsafe_allow_html=True,
    )

if st.button("Generate next-day demand curve", type="primary", use_container_width=True):
    try:
        with st.spinner("Downloading hourly history and training the forecast model..."):
            st.session_state["day_ahead_result"] = generate_day_ahead_forecast(
                forecast_target_day,
                int(forecast_lookback),
                temperature_mode,
                float(forecast_trend_alpha),
            )
    except Exception as exc:
        st.error(f"Demand forecast failed: {exc}")

forecast_result = st.session_state.get("day_ahead_result")
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
    st.info("Press the button to generate the day-ahead curve. The first run is slower because the hourly history is downloaded and cached.")


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
