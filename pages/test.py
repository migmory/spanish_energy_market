import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
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

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
DEMAND_BLUE = "#1D4ED8"
TEMPERATURE_ORANGE = "#EA580C"

MAX_RANGE_DAYS = 124


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
        .dt.normalize()
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


def fetch_one_pbf_indicator_daily(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
) -> pd.DataFrame:
    frames = []
    chunk_start = start_day

    # Monthly chunks keep ESIOS requests small and stable.
    while chunk_start <= end_day:
        chunk_end = min(
            end_day,
            chunk_start + timedelta(days=30),
        )

        start_local = pd.Timestamp(
            chunk_start,
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
            "time_trunc": "day",
            "time_agg": "sum",
        }

        last_error = None

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

                last_error = None
                break

            except requests.RequestException as exc:
                last_error = exc
                sleep(1.5 * (attempt + 1))

        if last_error is not None:
            # Return the chunks that did work instead of breaking the whole page.
            pass

        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["date", "energy_mwh"])

    out = pd.concat(frames, ignore_index=True)

    return (
        out.groupby("datetime", as_index=False)["value"]
        .sum()
        .rename(
            columns={
                "datetime": "date",
                "value": "energy_mwh",
            }
        )
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


def load_one_pbf_technology(
    technology: str,
    indicator_ids: list[int],
    start_day: date,
    end_day: date,
    token: str,
) -> pd.DataFrame:
    frames = []

    for indicator_id in indicator_ids:
        indicator_df = fetch_one_pbf_indicator_daily(
            indicator_id=indicator_id,
            start_day=start_day,
            end_day=end_day,
            token=token,
        )
        if not indicator_df.empty:
            frames.append(indicator_df)

    if not frames:
        return pd.DataFrame(
            columns=["date", "technology", "energy_mwh"]
        )

    combined = pd.concat(frames, ignore_index=True)

    combined = (
        combined.groupby("date", as_index=False)["energy_mwh"]
        .sum()
    )
    combined["technology"] = technology

    return combined[["date", "technology", "energy_mwh"]]


@st.cache_data(show_spinner=False, ttl=3600)
def load_pbf_daily_mix(
    start_day: date,
    end_day: date,
    _token: str,
) -> pd.DataFrame:
    results = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(
                load_one_pbf_technology,
                technology,
                indicator_ids,
                start_day,
                end_day,
                _token,
            ): technology
            for technology, indicator_ids in PBF_TECH_INDICATORS.items()
        }

        for future in as_completed(futures):
            technology = futures[future]

            try:
                frame = future.result()
            except Exception:
                frame = pd.DataFrame(
                    columns=["date", "technology", "energy_mwh"]
                )

            if not frame.empty:
                results.append(frame)

    if not results:
        return pd.DataFrame(
            columns=["date", "technology", "energy_mwh"]
        )

    out = pd.concat(results, ignore_index=True)
    out["energy_mwh"] = pd.to_numeric(
        out["energy_mwh"],
        errors="coerce",
    )
    out = out.dropna(subset=["date", "technology", "energy_mwh"])

    return (
        out.groupby(
            ["date", "technology"],
            as_index=False,
        )["energy_mwh"]
        .sum()
        .sort_values(["date", "technology"])
        .reset_index(drop=True)
    )


# =========================================================
# CHARTS
# =========================================================
def build_demand_temperature_chart(
    combined: pd.DataFrame,
    temperature_label: str,
):
    if combined.empty:
        return None

    plot = combined.copy().sort_values("date")
    plot["avg_demand_gw"] = plot["demand_gwh"] / 24.0
    plot["avg_demand_7d_gw"] = (
        plot["avg_demand_gw"]
        .rolling(7, min_periods=1)
        .mean()
    )
    plot["temperature_7d_c"] = (
        plot["temperature_c"]
        .rolling(7, min_periods=1)
        .mean()
    )

    demand_long = pd.concat(
        [
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["avg_demand_gw"],
                    "series": "Demand daily",
                }
            ),
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["avg_demand_7d_gw"],
                    "series": "Demand 7d avg",
                }
            ),
        ],
        ignore_index=True,
    )

    temperature_long = pd.concat(
        [
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["temperature_c"],
                    "series": "Temperature daily",
                }
            ),
            pd.DataFrame(
                {
                    "date": plot["date"],
                    "value": plot["temperature_7d_c"],
                    "series": "Temperature 7d avg",
                }
            ),
        ],
        ignore_index=True,
    )

    color_scale = alt.Scale(
        domain=[
            "Demand daily",
            "Demand 7d avg",
            "Temperature daily",
            "Temperature 7d avg",
        ],
        range=[
            "#A5B4FC",
            DEMAND_BLUE,
            "#FDBA74",
            TEMPERATURE_ORANGE,
        ],
    )

    dash_scale = alt.Scale(
        domain=[
            "Demand daily",
            "Demand 7d avg",
            "Temperature daily",
            "Temperature 7d avg",
        ],
        range=[[4, 2], [1, 0], [4, 2], [1, 0]],
    )

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
                sort=[
                    "Demand daily",
                    "Demand 7d avg",
                    "Temperature daily",
                    "Temperature 7d avg",
                ],
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

    temperature_chart = (
        alt.Chart(temperature_long)
        .mark_line(strokeWidth=2.6)
        .encode(
            x=alt.X("date:T", title=None),
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
                sort=[
                    "Demand daily",
                    "Demand 7d avg",
                    "Temperature daily",
                    "Temperature 7d avg",
                ],
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

    chart = alt.layer(demand_chart, temperature_chart).resolve_scale(y="independent")
    return configure_chart(chart, height=420)


def build_pbf_daily_area_chart(pbf_daily: pd.DataFrame):
    if pbf_daily.empty:
        return None

    plot = pbf_daily.copy()
    plot["energy_gwh"] = plot["energy_mwh"] / 1_000.0

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
                "date:T",
                title=None,
                axis=alt.Axis(format="%d-%b", labelAngle=0),
            ),
            y=alt.Y(
                "sum(energy_gwh):Q",
                title="Daily PBF programmed generation (GWh)",
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
            ),
            order=alt.Order(
                "technology:N",
                sort="ascending",
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip(
                    "sum(energy_gwh):Q",
                    title="PBF energy (GWh)",
                    format=",.2f",
                ),
            ],
        )
    )

    return configure_chart(chart, height=390)


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
                "average_daily_gwh:Q",
                title="Average programmed energy (GWh/day)",
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
                    "average_daily_gwh:Q",
                    title="Average GWh/day",
                    format=",.2f",
                ),
                alt.Tooltip(
                    "period_total_gwh:Q",
                    title="Period total (GWh)",
                    format=",.1f",
                ),
                alt.Tooltip(
                    "share_pct:Q",
                    title="Share",
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
            x=alt.X("average_daily_gwh:Q"),
            text=alt.Text(
                "average_daily_gwh:Q",
                format=",.1f",
            ),
        )
    )

    chart = alt.layer(bars, labels)

    return configure_chart(chart, height=max(340, 31 * len(summary)))


# =========================================================
# APP
# =========================================================
st.title("Demand, temperature and PBF — test")

st.caption(
    "Daily peninsular electricity demand from REData, historical temperature "
    "from Open-Meteo ERA5-Land and programmed PBF generation from ESIOS."
)

token = require_esios_token()

archive_safe_end = date.today() - timedelta(days=5)
default_end = archive_safe_end
default_start = default_end - timedelta(days=61)

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

    pbf_daily = load_pbf_daily_mix(
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

    metric_1, metric_2, metric_3, metric_4 = st.columns(4)

    combined['avg_demand_gw'] = combined['demand_gwh'] / 24.0

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
    )
    st.altair_chart(
        demand_temperature_chart,
        use_container_width=True,
    )

    st.caption(
        "Dashed lines show daily observations and solid lines show the 7-day moving "
        "average. Demand is displayed as average daily GW. The Spain series is a "
        "population-weighted proxy based on representative peninsular cities, not an "
        "official AEMET national index."
    )


# =========================================================
# PBF DAILY MIX
# =========================================================
section_header("PBF daily programmed-generation mix")

if pbf_daily.empty:
    st.warning(
        "ESIOS did not return PBF technology data for the selected period. "
        "Check the token and the selected dates."
    )
else:
    pbf_area_chart = build_pbf_daily_area_chart(pbf_daily)
    st.altair_chart(
        pbf_area_chart,
        use_container_width=True,
    )

    days_with_data = max(
        int(pbf_daily["date"].nunique()),
        1,
    )

    summary = (
        pbf_daily.groupby("technology", as_index=False)["energy_mwh"]
        .sum()
        .rename(columns={"energy_mwh": "period_total_mwh"})
    )

    summary["period_total_gwh"] = (
        summary["period_total_mwh"] / 1_000.0
    )
    summary["average_daily_gwh"] = (
        summary["period_total_gwh"] / days_with_data
    )

    total_period_gwh = summary["period_total_gwh"].sum()
    summary["share_pct"] = (
        summary["period_total_gwh"] / total_period_gwh
        if total_period_gwh != 0
        else pd.NA
    )

    summary = summary.sort_values(
        "average_daily_gwh",
        ascending=False,
    ).reset_index(drop=True)

    pbf_m1, pbf_m2, pbf_m3 = st.columns(3)

    pbf_m1.metric(
        "PBF generation in period",
        f"{total_period_gwh:,.1f} GWh",
    )
    pbf_m2.metric(
        "Average PBF generation",
        f"{total_period_gwh / days_with_data:,.1f} GWh/day",
    )
    pbf_m3.metric(
        "Days with PBF data",
        f"{days_with_data:,}",
    )

    st.subheader("Average PBF composition during selected period")

    average_chart = build_pbf_average_chart(summary)
    st.altair_chart(
        average_chart,
        use_container_width=True,
    )

    table = summary[
        [
            "technology",
            "average_daily_gwh",
            "period_total_gwh",
            "share_pct",
        ]
    ].rename(
        columns={
            "technology": "Technology",
            "average_daily_gwh": "Average GWh/day",
            "period_total_gwh": "Period total GWh",
            "share_pct": "PBF share",
        }
    )

    st.dataframe(
        table.style.format(
            {
                "Average GWh/day": "{:,.2f}",
                "Period total GWh": "{:,.1f}",
                "PBF share": "{:.1%}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "PBF averages are calculated as total programmed energy for each "
        "technology divided by the number of dates returned by ESIOS. "
        "Imports, exports, pumping consumption and demand-side programme "
        "components are intentionally excluded from this generation mix."
    )


# =========================================================
# RAW DATA DOWNLOAD
# =========================================================
with st.expander("Show / download test data"):
    tab_1, tab_2, tab_3 = st.tabs(
        [
            "Demand and temperature",
            "PBF daily mix",
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
        pbf_export = pbf_daily.copy()
        if not pbf_export.empty:
            pbf_export["energy_gwh"] = (
                pbf_export["energy_mwh"] / 1_000.0
            )

        st.dataframe(
            pbf_export,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download daily PBF CSV",
            data=pbf_export.to_csv(index=False).encode("utf-8"),
            file_name=f"pbf_daily_{start_day}_{end_day}.csv",
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
