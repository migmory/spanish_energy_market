import os
import re
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# ENV / CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Day Ahead", layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-size: 101% !important;
    }

    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 101% !important;
    }

    h1 {
        font-size: 2.0rem !important;
    }

    h2, h3 {
        font-size: 1.35rem !important;
    }

    div[data-testid="stMetricValue"] {
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Day Ahead - Spain Spot Prices")

DATA_DIR = BASE_DIR / "data"
LIVE_START_DATE = date(2026, 1, 1)
DEFAULT_START_DATE = date(2021, 1, 1)
MADRID_TZ = ZoneInfo("Europe/Madrid")

HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_SOLAR_FILE = DATA_DIR / "p48solar_since21.csv"
HIST_WORKBOOK_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_MIX_FILE = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"
HIST_INSTALLED_CAP_FILE = DATA_DIR / "installed_capacity_monthly.xlsx"

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542

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

LOCAL_MIX_TECH_MAP = {
    "Hidráulica": "Hydro",
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
}

RENEWABLE_TECHS = {
    "Hydro",
    "Hydro UGH",
    "Hydro non-UGH",
    "Pumped hydro",
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Other renewables",
    "Biomass",
    "Biogas",
}

TECH_COLOR_SCALE = alt.Scale(
    domain=[
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
        "Coal",
        "Fuel + Gas",
        "Steam turbine",
        "Other non-renewables",
    ],
    range=[
        "#9CA3AF",
        "#60A5FA",
        "#C084FC",
        "#FACC15",
        "#FCA5A5",
        "#2563EB",
        "#F97316",
        "#16A34A",
        "#22C55E",
        "#14B8A6",
        "#374151",
        "#6B7280",
        "#4B5563",
        "#7C2D12",
    ],
)

TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
GREY_SHADE = "#F3F4F6"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
BLUE_PRICE = "#1D4ED8"
GREEN_RENEWABLES = "#059669"

# =========================================================
# DISPLAY HELPERS
# =========================================================
def section_header(title: str):
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
            color: white;
            padding: 12px 18px;
            border-radius: 12px;
            font-weight: 800;
            font-size: 1.25rem;
            margin-top: 14px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def subtle_subsection(title: str):
    st.markdown(
        f"""
        <div style="
            margin-top: 14px;
            margin-bottom: 8px;
            padding: 8px 0 4px 0;
            color: #1F2937;
            font-size: 1.05rem;
            font-weight: 700;
            border-bottom: 1px solid #E5E7EB;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def styled_df(df: pd.DataFrame, pct_cols: list[str] | None = None):
    pct_cols = pct_cols or []
    numeric_cols = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols
    ]

    fmt = {c: "{:,.2f}" for c in numeric_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})

    styles = [
        {
            "selector": "th",
            "props": [
                ("background-color", "#4B5563"),
                ("color", "white"),
                ("font-weight", "bold"),
                ("font-size", TABLE_HEADER_FONT_PCT),
                ("text-align", "center"),
                ("padding", "10px 8px"),
            ],
        },
        {
            "selector": "td",
            "props": [
                ("font-size", TABLE_BODY_FONT_PCT),
                ("padding", "6px 8px"),
            ],
        },
    ]
    return df.style.format(fmt).set_table_styles(styles)


def apply_common_chart_style(chart, height: int = 360):
    chart_dict = chart.to_dict()
    if "vconcat" in chart_dict or "hconcat" in chart_dict or "concat" in chart_dict:
        styled = chart
    else:
        styled = chart.properties(height=height)

    return (
        styled
        .configure_view(stroke="#E5E7EB", fill="white")
        .configure_axis(
            grid=True,
            gridColor="#E5E7EB",
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor="#111827",
            titleColor="#111827",
            labelFontSize=12,
            titleFontSize=14,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=12,
            titleFontSize=13,
            symbolStrokeWidth=3,
        )
    )


def format_metric(value, suffix="", decimals=2):
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}{suffix}"


# =========================================================
# TIME / TOKEN
# =========================================================
def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def allow_next_day_refresh() -> bool:
    return now_madrid().time() >= time(15, 0)


def max_refresh_day() -> date:
    return date.today() + timedelta(days=1) if allow_next_day_refresh() else date.today()


def require_esios_token() -> str:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    if not token:
        raise ValueError(f"No token found in {ENV_PATH}")
    return token


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


# =========================================================
# GENERIC PARSERS
# =========================================================
def parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT

    # Historical file has two different date encodings:
    # 2021 -> dd/mm/YYYY
    # 2022+ -> weird REE export like 2022006001T00:00:00+02:00 (= 2022-06-01)
    # Pattern = YYYY + "00" + M_or_MM + DDD, where day is zero-padded to 3 digits.
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
        ts = pd.to_datetime(s, utc=True, errors="raise")
        return ts.tz_convert("Europe/Madrid").tz_localize(None)
    except Exception:
        pass

    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise")
    except Exception:
        return pd.NaT


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
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


def infer_interval_hours(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    diffs = df.sort_values("datetime")["datetime"].diff().dt.total_seconds().div(3600).dropna()
    if diffs.empty:
        interval = 1.0
    else:
        interval = 0.25 if diffs.median() <= 0.30 else 1.0
    return pd.Series(interval, index=df.index)


def to_hourly_mean(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name])
    out = df.copy()
    out["datetime_hour"] = out["datetime"].dt.floor("h")
    out = (
        out.groupby("datetime_hour", as_index=False)
        .agg(value=(df.columns[1] if df.columns[1] != "datetime" else "value", "mean"))
        .rename(columns={"datetime_hour": "datetime", "value": value_col_name})
    )
    return out.sort_values("datetime").reset_index(drop=True)


def to_energy_intervals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh"])
    out = df.copy()
    if "mw" not in out.columns:
        out = out.rename(columns={"value": "mw"})
    out["mw"] = pd.to_numeric(out["mw"], errors="coerce")
    out["interval_h"] = infer_interval_hours(out)
    out["energy_mwh"] = out["mw"] * out["interval_h"]
    return out.dropna(subset=["datetime", "mw", "energy_mwh"]).copy()


def to_programmed_generation_energy(df: pd.DataFrame) -> pd.DataFrame:
    '''
    For 2026 live generation-mix indicators we keep the ESIOS values as energy per period.
    This avoids undercounting the post-2025 quarter-hour series by a factor of ~4 versus REE monthly totals.
    '''
    if df.empty:
        return pd.DataFrame(columns=["datetime", "energy_mwh"])
    out = df.copy()
    if "value" in out.columns and "energy_mwh" not in out.columns:
        out["energy_mwh"] = pd.to_numeric(out["value"], errors="coerce")
    elif "mw" in out.columns and "energy_mwh" not in out.columns:
        out["energy_mwh"] = pd.to_numeric(out["mw"], errors="coerce")
    out = out.dropna(subset=["datetime", "energy_mwh"]).copy()
    return out[["datetime", "energy_mwh"]]


# =========================================================
# HISTORICAL FILE LOADERS
# =========================================================
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
    df = df.dropna(subset=["datetime", "price"])
    df = df[df["datetime"].dt.year <= 2025].copy()
    return df[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_historical_solar() -> pd.DataFrame:
    # Prefer the full workbook export because the standalone CSV may be only a short slice.
    if HIST_WORKBOOK_FILE.exists():
        try:
            df = pd.read_excel(HIST_WORKBOOK_FILE, sheet_name="solar_hourly_best")
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
            if "solar_source" not in df.columns:
                df["solar_source"] = "Historical workbook"
            df = df.dropna(subset=["datetime", "solar_best_mw"])
            df = df[df["datetime"].dt.year <= 2025].copy()
            return df[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)
        except Exception:
            pass

    if not HIST_SOLAR_FILE.exists():
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

    df = pd.read_csv(HIST_SOLAR_FILE)
    if "solar_best_mw" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "solar_best_mw"})
    if "solar_source" not in df.columns:
        df["solar_source"] = "Historical file"
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
    df = df.dropna(subset=["datetime", "solar_best_mw"])
    df = df[df["datetime"].dt.year <= 2025].copy()
    return df[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_historical_generation_mix_daily() -> pd.DataFrame:
    if not HIST_MIX_FILE.exists():
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])

    raw = pd.read_excel(HIST_MIX_FILE, sheet_name="data", header=None)
    dates = [parse_mixed_date(v) for v in raw.iloc[4, 1:].tolist()]
    tech_rows = raw.iloc[5:19, :].copy()

    records = []
    for _, row in tech_rows.iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        for col_idx, dt in enumerate(dates, start=1):
            if pd.isna(dt):
                continue
            val = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.isna(val):
                continue
            records.append(
                {
                    "datetime": pd.Timestamp(dt).normalize(),
                    "technology": tech,
                    "energy_mwh": float(val) * 1000.0,
                    "data_source": "Historical file",
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        return out
    out = out[out["datetime"].dt.year <= 2025].copy()
    return out.sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_installed_capacity_monthly() -> pd.DataFrame:
    if not HIST_INSTALLED_CAP_FILE.exists():
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])

    raw = pd.read_excel(HIST_INSTALLED_CAP_FILE, sheet_name="data", header=None)
    dates = [parse_mixed_date(v) for v in raw.iloc[4, 1:].tolist()]
    tech_rows = raw.iloc[5:19, :].copy()

    records = []
    for _, row in tech_rows.iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        for col_idx, dt in enumerate(dates, start=1):
            if pd.isna(dt):
                continue
            val = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.isna(val):
                continue
            records.append(
                {
                    "datetime": pd.Timestamp(dt).normalize(),
                    "technology": tech,
                    "capacity_mw": float(val),
                }
            )
    out = pd.DataFrame(records)
    if out.empty:
        return out
    return out.sort_values(["datetime", "technology"]).reset_index(drop=True)


# =========================================================
# LIVE 2026 EXTRACTION
# =========================================================
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
    time_trunc: str | None = None,
) -> pd.DataFrame:
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    start_local = pd.Timestamp(start_day, tz="Europe/Madrid")
    end_local = pd.Timestamp(end_day + timedelta(days=1), tz="Europe/Madrid")
    start_utc = start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    if time_trunc is None:
        time_trunc = "quarter_hour" if start_day >= date(2025, 10, 1) else "hour"

    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    resp = requests.get(
        url,
        headers=build_headers(token),
        params={
            "start_date": start_utc,
            "end_date": end_utc,
            "time_trunc": time_trunc,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return parse_esios_indicator(resp.json(), source_name=f"esios_{indicator_id}")

def build_best_solar_hourly(solar_p48_hourly: pd.DataFrame, solar_forecast_hourly: pd.DataFrame) -> pd.DataFrame:
    if solar_p48_hourly.empty and solar_forecast_hourly.empty:
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])
    if solar_p48_hourly.empty:
        out = solar_forecast_hourly.rename(columns={"solar_forecast_mw": "solar_best_mw"}).copy()
        out["solar_source"] = "Forecast"
        return out[["datetime", "solar_best_mw", "solar_source"]]
    if solar_forecast_hourly.empty:
        out = solar_p48_hourly.rename(columns={"solar_p48_mw": "solar_best_mw"}).copy()
        out["solar_source"] = "P48"
        return out[["datetime", "solar_best_mw", "solar_source"]]

    merged = solar_p48_hourly.merge(solar_forecast_hourly, on="datetime", how="outer")
    merged["solar_best_mw"] = merged["solar_p48_mw"].combine_first(merged["solar_forecast_mw"])
    merged["solar_source"] = merged["solar_p48_mw"].apply(lambda x: "P48" if pd.notna(x) else None)
    merged.loc[merged["solar_source"].isna() & merged["solar_forecast_mw"].notna(), "solar_source"] = "Forecast"
    return merged[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_prices(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token)
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])
    out = raw[["datetime", "value"]].rename(columns={"value": "price"})
    out["datetime"] = out["datetime"].dt.floor("h")
    return out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_solar(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    p48 = fetch_esios_range(SOLAR_P48_INDICATOR_ID, start_day, end_day, token)
    fc = fetch_esios_range(SOLAR_FORECAST_INDICATOR_ID, start_day, end_day, token)

    if not p48.empty:
        p48 = p48[["datetime", "value"]].rename(columns={"value": "solar_p48_mw"})
        p48["datetime"] = p48["datetime"].dt.floor("h")
        p48 = p48.groupby("datetime", as_index=False)["solar_p48_mw"].mean()
    else:
        p48 = pd.DataFrame(columns=["datetime", "solar_p48_mw"])

    if not fc.empty:
        fc = fc[["datetime", "value"]].rename(columns={"value": "solar_forecast_mw"})
        fc["datetime"] = fc["datetime"].dt.floor("h")
        fc = fc.groupby("datetime", as_index=False)["solar_forecast_mw"].mean()
    else:
        fc = pd.DataFrame(columns=["datetime", "solar_forecast_mw"])

    return build_best_solar_hourly(p48, fc)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_mix_daily(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    frames = []
    for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
        official = pd.DataFrame(columns=["datetime", "value"])
        forecast = pd.DataFrame(columns=["datetime", "value"])

        if official_id is not None:
            try:
                official = fetch_esios_range(official_id, start_day, end_day, token, time_trunc="day")[["datetime", "value"]].copy()
            except Exception:
                official = fetch_esios_range(official_id, start_day, end_day, token)[["datetime", "value"]].copy()
            official["data_source"] = "Official"

        forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)
        if forecast_id is not None:
            try:
                forecast = fetch_esios_range(forecast_id, start_day, end_day, token, time_trunc="day")[["datetime", "value"]].copy()
            except Exception:
                forecast = fetch_esios_range(forecast_id, start_day, end_day, token)[["datetime", "value"]].copy()
            forecast["data_source"] = "Forecast"

        if not official.empty:
            official["datetime"] = pd.to_datetime(official["datetime"]).dt.normalize()
        if not forecast.empty:
            forecast["datetime"] = pd.to_datetime(forecast["datetime"]).dt.normalize()

        off_e = to_programmed_generation_energy(official) if not official.empty else pd.DataFrame()
        fc_e = to_programmed_generation_energy(forecast) if not forecast.empty else pd.DataFrame()

        if off_e.empty and fc_e.empty:
            continue

        if off_e.empty:
            best = fc_e.copy()
            best["data_source"] = "Forecast"
        elif fc_e.empty:
            best = off_e.copy()
            best["data_source"] = "Official"
        else:
            a = off_e[["datetime", "energy_mwh"]].rename(columns={"energy_mwh": "off_energy"})
            b = fc_e[["datetime", "energy_mwh"]].rename(columns={"energy_mwh": "fc_energy"})
            best = a.merge(b, on="datetime", how="outer")
            best["energy_mwh"] = best["off_energy"].combine_first(best["fc_energy"])
            best["data_source"] = best["off_energy"].apply(lambda x: "Official" if pd.notna(x) else "Forecast")
            best = best[["datetime", "energy_mwh", "data_source"]]

        best = best.groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
        best["technology"] = tech_name
        frames.append(best)

    if not frames:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])

    out = pd.concat(frames, ignore_index=True)

    hydro = (
        out[out["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])]
        .groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
    )
    hydro["technology"] = "Hydro"

    keep = out[~out["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
    out = pd.concat([keep, hydro], ignore_index=True)
    out = out.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum()
    return out.sort_values(["datetime", "technology", "data_source"]).reset_index(drop=True)

def combine_hist_and_live(hist_df: pd.DataFrame, live_df: pd.DataFrame, subset_cols: list[str]) -> pd.DataFrame:
    combined = pd.concat([hist_df, live_df], ignore_index=True)
    if combined.empty:
        return combined
    return combined.sort_values(subset_cols).drop_duplicates(subset=subset_cols, keep="last").reset_index(drop=True)


# =========================================================
# ANALYTICS
# =========================================================
def compute_period_metrics(price_df: pd.DataFrame, solar_df: pd.DataFrame, start_d: date, end_d: date) -> dict:
    period_price = price_df[(price_df["datetime"].dt.date >= start_d) & (price_df["datetime"].dt.date <= end_d)].copy()
    period_solar = solar_df[(solar_df["datetime"].dt.date >= start_d) & (solar_df["datetime"].dt.date <= end_d)].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None
    merged = period_price.merge(period_solar[["datetime", "solar_best_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()

    captured_uncurtailed = None
    captured_curtailed = None
    if not merged.empty and merged["solar_best_mw"].sum() != 0:
        captured_uncurtailed = (merged["price"] * merged["solar_best_mw"]).sum() / merged["solar_best_mw"].sum()
        curtailed = merged[merged["price"] > 0].copy()
        if not curtailed.empty and curtailed["solar_best_mw"].sum() != 0:
            captured_curtailed = (curtailed["price"] * curtailed["solar_best_mw"]).sum() / curtailed["solar_best_mw"].sum()

    return {
        "avg_price": avg_price,
        "captured_uncurtailed": captured_uncurtailed,
        "captured_curtailed": captured_curtailed,
        "capture_pct_uncurtailed": (captured_uncurtailed / avg_price) if (captured_uncurtailed is not None and avg_price not in [None, 0]) else None,
        "capture_pct_curtailed": (captured_curtailed / avg_price) if (captured_curtailed is not None and avg_price not in [None, 0]) else None,
    }


def build_monthly_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    monthly_avg = (
        price_hourly.assign(month=price_hourly["datetime"].dt.to_period("M").dt.to_timestamp())
        .groupby("month", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_monthly_price"})
    )

    merged = price_hourly.merge(solar_hourly[["datetime", "solar_best_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()
    if merged.empty:
        monthly_avg["captured_solar_price_uncurtailed"] = pd.NA
        monthly_avg["captured_solar_price_curtailed"] = pd.NA
        monthly_avg["capture_pct_uncurtailed"] = pd.NA
        monthly_avg["capture_pct_curtailed"] = pd.NA
        return monthly_avg

    merged["month"] = merged["datetime"].dt.to_period("M").dt.to_timestamp()
    merged["weighted_price"] = merged["price"] * merged["solar_best_mw"]
    all_months = merged.groupby("month", as_index=False).agg(weighted_price_sum=("weighted_price", "sum"), solar_sum=("solar_best_mw", "sum"))
    all_months["captured_solar_price_uncurtailed"] = all_months["weighted_price_sum"] / all_months["solar_sum"]

    curtailed = merged[merged["price"] > 0].copy()
    if curtailed.empty:
        curtailed_months = pd.DataFrame(columns=["month", "captured_solar_price_curtailed"])
    else:
        curtailed["weighted_price"] = curtailed["price"] * curtailed["solar_best_mw"]
        curtailed_months = curtailed.groupby("month", as_index=False).agg(weighted_price_sum=("weighted_price", "sum"), solar_sum=("solar_best_mw", "sum"))
        curtailed_months["captured_solar_price_curtailed"] = curtailed_months["weighted_price_sum"] / curtailed_months["solar_sum"]
        curtailed_months = curtailed_months[["month", "captured_solar_price_curtailed"]]

    monthly_combo = monthly_avg.merge(all_months[["month", "captured_solar_price_uncurtailed"]], on="month", how="left").merge(curtailed_months, on="month", how="left")
    monthly_combo["capture_pct_uncurtailed"] = monthly_combo["captured_solar_price_uncurtailed"] / monthly_combo["avg_monthly_price"]
    monthly_combo["capture_pct_curtailed"] = monthly_combo["captured_solar_price_curtailed"] / monthly_combo["avg_monthly_price"]
    return monthly_combo


def build_negative_price_curves(price_hourly: pd.DataFrame, mode: str) -> pd.DataFrame:
    df = price_hourly.copy()
    if df.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "cum_count"])

    df["flag"] = (df["price"] < 0).astype(int) if mode == "Only negative prices" else (df["price"] <= 0).astype(int)
    df["year"] = df["datetime"].dt.year
    df["month_num"] = df["datetime"].dt.month
    df["month_name"] = df["datetime"].dt.strftime("%b")

    monthly = df.groupby(["year", "month_num", "month_name"], as_index=False)["flag"].sum().rename(columns={"flag": "count"})
    rows = []
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    for y in sorted(monthly["year"].unique().tolist()):
        temp = monthly[monthly["year"] == y].set_index("month_num")
        cum = 0
        max_month = int(temp.index.max()) if len(temp.index) else 0
        for m in range(1, max_month + 1):
            if m in temp.index:
                cum += float(temp.loc[m, "count"])
            rows.append({"year": str(y), "month_num": m, "month_name": month_names[m - 1], "cum_count": cum})
    return pd.DataFrame(rows)


def build_energy_mix_period(mix_df: pd.DataFrame, granularity: str, year_sel: int | None = None, day_range: tuple[date, date] | None = None):
    if mix_df.empty:
        return pd.DataFrame()

    tmp = mix_df.copy()
    if granularity == "Annual":
        tmp["period_label"] = tmp["datetime"].dt.year.astype(str)
        tmp["sort_key"] = tmp["datetime"].dt.year
    elif granularity == "Monthly":
        if year_sel is not None:
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
        tmp["period_label"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
    else:
        if day_range is not None:
            d0, d1 = day_range
            tmp = tmp[(tmp["datetime"].dt.date >= d0) & (tmp["datetime"].dt.date <= d1)].copy()
        tmp["period_label"] = tmp["datetime"].dt.strftime("%a %d-%b")
        tmp["sort_key"] = tmp["datetime"].dt.normalize()

    grouped = tmp.groupby(["period_label", "sort_key", "technology"], as_index=False)["energy_mwh"].sum()

    totals = grouped.groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "total_generation_mwh"})
    renew = grouped[grouped["technology"].isin(RENEWABLE_TECHS)].groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "renewable_generation_mwh"})
    summary = totals.merge(renew, on=["period_label", "sort_key"], how="left")
    summary["renewable_generation_mwh"] = summary["renewable_generation_mwh"].fillna(0.0)
    summary["renewable_share_pct"] = summary["renewable_generation_mwh"] / summary["total_generation_mwh"]

    return grouped.merge(summary, on=["period_label", "sort_key"], how="left")


def build_energy_mix_period_chart(mix_period: pd.DataFrame):
    if mix_period.empty:
        return None

    plot = mix_period.copy()
    plot["energy_gwh"] = plot["energy_mwh"] / 1000.0
    summary = plot[["period_label", "sort_key", "total_generation_mwh", "renewable_generation_mwh", "renewable_share_pct"]].drop_duplicates().sort_values("sort_key")
    order_list = summary["period_label"].tolist()

    bars = alt.Chart(plot).mark_bar().encode(
        x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y("energy_gwh:Q", title="Generation (GWh)"),
        color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("energy_gwh:Q", title="Generation (GWh)", format=",.2f"),
        ],
    )

    line = alt.Chart(summary).mark_line(point=True, color=GREEN_RENEWABLES, strokeWidth=3).encode(
        x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y("renewable_share_pct:Q", title="% Renewables", axis=alt.Axis(format=".0%")),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("renewable_generation_mwh:Q", title="Renewables (MWh)", format=",.0f"),
            alt.Tooltip("total_generation_mwh:Q", title="Total generation (MWh)", format=",.0f"),
            alt.Tooltip("renewable_share_pct:Q", title="% Renewables", format=".1%"),
        ],
    )

    chart = alt.layer(bars, line).resolve_scale(y="independent").properties(height=430)
    return apply_common_chart_style(chart, height=430)


def build_monthly_renewables_table(mix_df: pd.DataFrame) -> pd.DataFrame:
    if mix_df.empty:
        return pd.DataFrame(columns=["Month", "Renewables (MWh)", "Total generation (MWh)", "% Renewables"])
    tmp = mix_df.copy()
    tmp["month"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
    total = tmp.groupby("month", as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "Total generation (MWh)"})
    renew = tmp[tmp["technology"].isin(RENEWABLE_TECHS)].groupby("month", as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "Renewables (MWh)"})
    out = total.merge(renew, on="month", how="left")
    out["Renewables (MWh)"] = out["Renewables (MWh)"].fillna(0.0)
    out["% Renewables"] = out["Renewables (MWh)"] / out["Total generation (MWh)"]
    out["Month"] = out["month"].dt.strftime("%b - %Y")
    return out[["Month", "Renewables (MWh)", "Total generation (MWh)", "% Renewables"]]


def build_selected_day_chart(day_price: pd.DataFrame, day_solar: pd.DataFrame, metrics: dict):
    if day_price.empty:
        return None

    base = alt.Chart(day_price).encode(x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0)))
    price_line = base.mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
        y=alt.Y("price:Q", title="Price €/MWh"),
        tooltip=[alt.Tooltip("datetime:T", title="Time"), alt.Tooltip("price:Q", title="Price", format=".2f")],
    )

    layers = [price_line]
    rules = []
    if metrics.get("captured_curtailed") is not None:
        rules.append({"series": "Curtailed captured", "value": metrics["captured_curtailed"], "color": YELLOW_DARK, "dash": [6, 4]})
    if metrics.get("captured_uncurtailed") is not None:
        rules.append({"series": "Uncurtailed captured", "value": metrics["captured_uncurtailed"], "color": YELLOW_LIGHT, "dash": [2, 2]})
    if rules:
        rule_df = pd.DataFrame(rules)
        layers.append(
            alt.Chart(rule_df).mark_rule(strokeWidth=2).encode(
                y="value:Q",
                color=alt.Color("series:N", legend=None, scale=alt.Scale(domain=rule_df["series"].tolist(), range=rule_df["color"].tolist())),
                strokeDash=alt.StrokeDash("series:N", legend=None, scale=alt.Scale(domain=rule_df["series"].tolist(), range=rule_df["dash"].tolist())),
            )
        )
    left_chart = alt.layer(*layers)

    if not day_solar.empty:
        solar_chart = alt.Chart(day_solar).mark_area(opacity=0.35, color=YELLOW_LIGHT).encode(
            x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0)),
            y=alt.Y("solar_best_mw:Q", title="Solar MW"),
            tooltip=[alt.Tooltip("datetime:T", title="Time"), alt.Tooltip("solar_best_mw:Q", title="Solar", format=",.2f")],
        )
        chart = alt.layer(left_chart, solar_chart).resolve_scale(y="independent").properties(height=360)
    else:
        chart = left_chart.properties(height=360)
    return apply_common_chart_style(chart, height=360)


def build_monthly_shading_df(monthly_combo: pd.DataFrame) -> pd.DataFrame:
    years = sorted(monthly_combo["month"].dt.year.unique().tolist()) if not monthly_combo.empty else []
    if not years:
        return pd.DataFrame(columns=["x_start", "x_end", "year", "shade"])

    rows = []
    for i, year in enumerate(years):
        rows.append(
            {
                "x_start": pd.Timestamp(year, 1, 1),
                "x_end": pd.Timestamp(year + 1, 1, 1),
                "year": str(year),
                "shade": 1 if i % 2 == 0 else 0,
            }
        )
    return pd.DataFrame(rows)


def build_monthly_main_chart(monthly_combo: pd.DataFrame):
    if monthly_combo.empty:
        return None

    plot_df = monthly_combo.copy().rename(
        columns={
            "avg_monthly_price": "Average spot price",
            "captured_solar_price_curtailed": "Solar captured (curtailed)",
            "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)",
        }
    )

    long_df = plot_df.melt(
        id_vars=["month"],
        value_vars=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])

    long_df["year"] = long_df["month"].dt.year.astype(str)
    long_df["year_mid"] = pd.to_datetime(long_df["month"].dt.year.astype(str) + "-07-01")

    shading = build_monthly_shading_df(monthly_combo)

    color_scale = alt.Scale(
        domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        range=[BLUE_PRICE, YELLOW_DARK, YELLOW_LIGHT],
    )
    dash_scale = alt.Scale(
        domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        range=[[1, 0], [6, 4], [2, 2]],
    )

    base = alt.Chart(long_df).encode(
        x=alt.X(
            "month:T",
            axis=alt.Axis(
                title=None,
                format="%b",
                labelAngle=0,
                labelPadding=8,
                labelFlush=False,
                ticks=False,
                domain=False,
                grid=False,
                labelOverlap="greedy",
            ),
        )
    )

    layers = []
    layers.append(
        base.mark_line(point=True, strokeWidth=3).encode(
            y=alt.Y("value:Q", title="€/MWh"),
            color=alt.Color("series:N", title=None, scale=color_scale),
            strokeDash=alt.StrokeDash("series:N", title=None, scale=dash_scale),
            tooltip=[
                alt.Tooltip("month:T", title="Month"),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("value:Q", title="€/MWh", format=",.2f"),
            ],
        )
    )

    main = alt.layer(*layers).properties(height=330)

    year_df = long_df[["year", "year_mid"]].drop_duplicates().sort_values("year_mid").reset_index(drop=True)

    year_layers = []
    if not shading.empty:
        year_layers.append(
            alt.Chart(shading[shading["shade"] == 1]).mark_rect(color=GREY_SHADE, opacity=0.9).encode(
                x="x_start:T",
                x2="x_end:T",
            )
        )

        year_layers.append(
            alt.Chart(shading.iloc[1:]).mark_rule(color="#D1D5DB", strokeWidth=1.2).encode(
                x="x_start:T"
            )
        )

    year_layers.append(
        alt.Chart(year_df).mark_text(fontWeight="bold", dy=0, fontSize=13, color="#111827").encode(
            x=alt.X("year_mid:T", axis=alt.Axis(title=None, labels=False, ticks=False, domain=False, grid=False)),
            text="year:N",
        )
    )

    year_band = alt.layer(*year_layers).properties(height=36)

    chart = alt.vconcat(main, year_band, spacing=1).resolve_scale(x="shared")
    return apply_common_chart_style(chart, height=330)

def build_negative_price_chart(negative_df: pd.DataFrame, mode: str):
    if negative_df.empty:
        return None
    years = sorted(negative_df["year"].unique().tolist())
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626", "#0EA5E9"]
    chart = alt.Chart(negative_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("month_num:O", sort=list(range(1, 13)), axis=alt.Axis(title=None, labelAngle=0, labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]")),
        y=alt.Y("cum_count:Q", title=("Cumulative # hours" if mode == "Zero and negative prices" else "Cumulative # negative hours")),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=colors[:len(years)])),
        detail="year:N",
        tooltip=[alt.Tooltip("year:N", title="Year"), alt.Tooltip("month_name:N", title="Month"), alt.Tooltip("cum_count:Q", title="Cumulative count", format=",.0f")],
    ).properties(height=330)
    return apply_common_chart_style(chart, height=330)


def build_installed_capacity_chart(cap_df: pd.DataFrame, selected_techs: list[str]):
    if cap_df.empty or not selected_techs:
        return None
    plot = cap_df[cap_df["technology"].isin(selected_techs)].copy()
    if plot.empty:
        return None
    plot["month_label"] = plot["datetime"].dt.strftime("%b-%y")
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%b-%y", labelAngle=0)),
        y=alt.Y("capacity_mw:Q", title="Installed capacity (MW)"),
        color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
        tooltip=[alt.Tooltip("datetime:T", title="Month"), alt.Tooltip("technology:N", title="Technology"), alt.Tooltip("capacity_mw:Q", title="MW", format=",.2f")],
    ).properties(height=360)
    return apply_common_chart_style(chart, height=360)


def build_price_workbook(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, monthly_combo: pd.DataFrame, negative_price_df: pd.DataFrame, mix_monthly_table: pd.DataFrame, installed_capacity: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_hourly")
        solar_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="solar_hourly")
        monthly_combo.sort_values("month").to_excel(writer, index=False, sheet_name="monthly_capture")
        negative_price_df.to_excel(writer, index=False, sheet_name="negative_prices")
        mix_monthly_table.to_excel(writer, index=False, sheet_name="monthly_renewables")
        installed_capacity.sort_values(["datetime", "technology"]).to_excel(writer, index=False, sheet_name="installed_capacity")
    output.seek(0)
    return output.getvalue()


# =========================================================
# MAIN
# =========================================================
try:
    token = require_esios_token()

    st.caption(
        f"Madrid time now: {now_madrid().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Historic source: /data until 2025 | "
        f"Live extraction: ESIOS from 2026"
    )

    start_day = st.date_input(
        "Analysis start date",
        value=DEFAULT_START_DATE,
        min_value=DEFAULT_START_DATE,
        max_value=max_refresh_day(),
    )

    live_start = max(LIVE_START_DATE, start_day)
    live_end = max_refresh_day()

    with st.spinner("Loading historical files + live 2026 data..."):
        hist_prices = load_historical_prices()
        hist_solar = load_historical_solar()
        hist_mix = load_historical_generation_mix_daily()
        installed_capacity = load_installed_capacity_monthly()

        live_prices = load_live_2026_prices(token, live_start, live_end)
        live_solar = load_live_2026_solar(token, live_start, live_end)
        live_mix = load_live_2026_mix_daily(token, live_start, live_end)

    price_hourly = combine_hist_and_live(hist_prices, live_prices, ["datetime"])
    solar_hourly = combine_hist_and_live(hist_solar, live_solar, ["datetime"])
    mix_daily = combine_hist_and_live(hist_mix, live_mix, ["datetime", "technology", "data_source"])

    price_hourly = price_hourly[price_hourly["datetime"].dt.date >= start_day].copy()
    solar_hourly = solar_hourly[solar_hourly["datetime"].dt.date >= start_day].copy()
    mix_daily = mix_daily[mix_daily["datetime"].dt.date >= start_day].copy()

    if price_hourly.empty:
        st.error("No price data available.")
        st.stop()

    # Monthly spot / captured price
    monthly_combo = build_monthly_capture_table(price_hourly, solar_hourly)
    section_header("Monthly spot and solar captured price - Spain")
    monthly_chart = build_monthly_main_chart(monthly_combo)
    if monthly_chart is not None:
        st.altair_chart(monthly_chart, use_container_width=True)

    monthly_table = monthly_combo.copy()
    if not monthly_table.empty:
        monthly_table["Month"] = monthly_table["month"].dt.strftime("%b - %Y")
        monthly_table = monthly_table.rename(columns={
            "avg_monthly_price": "Average spot price",
            "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)",
            "captured_solar_price_curtailed": "Solar captured (curtailed)",
            "capture_pct_uncurtailed": "Capture rate (uncurtailed)",
            "capture_pct_curtailed": "Capture rate (curtailed)",
        })
        st.dataframe(
            styled_df(
                monthly_table[["Month", "Average spot price", "Solar captured (uncurtailed)", "Solar captured (curtailed)", "Capture rate (uncurtailed)", "Capture rate (curtailed)"]],
                pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"],
            ),
            use_container_width=True,
        )

    # Selected day
    section_header("Selected day: price vs solar")
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()
    selected_day = st.date_input("Select day", value=max_date, min_value=min_date, max_value=max_date)

    day_price = price_hourly[price_hourly["datetime"].dt.date == selected_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()
    day_metrics = compute_period_metrics(price_hourly, solar_hourly, selected_day, selected_day)
    selected_day_chart = build_selected_day_chart(day_price, day_solar, day_metrics)
    if selected_day_chart is not None:
        st.altair_chart(selected_day_chart, use_container_width=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Average spot price", format_metric(day_metrics.get("avg_price"), " €/MWh"))
    c2.metric("Captured solar (uncurtailed)", format_metric(day_metrics.get("captured_uncurtailed"), " €/MWh"))
    c3.metric("Captured solar (curtailed)", format_metric(day_metrics.get("captured_curtailed"), " €/MWh"))

    # Negative prices
    section_header("Negative prices")
    neg_mode = st.radio("Series to display", ["Zero and negative prices", "Only negative prices"], horizontal=True)
    negative_price_df = build_negative_price_curves(price_hourly, neg_mode)
    neg_chart = build_negative_price_chart(negative_price_df, neg_mode)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)
    subtle_subsection("Negative prices data")
    st.dataframe(styled_df(negative_price_df), use_container_width=True)

    # Energy mix
    section_header("Energy mix")
    if mix_daily.empty:
        st.info("No energy mix data available.")
    else:
        granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Daily"], index=1)
        available_years = sorted(price_hourly["datetime"].dt.year.unique().tolist())
        year_sel = available_years[-1]
        day_range = None
        if granularity == "Monthly":
            year_sel = st.selectbox("Year", available_years, index=len(available_years) - 1)
        elif granularity == "Daily":
            daily_min = mix_daily["datetime"].dt.date.min()
            daily_max = mix_daily["datetime"].dt.date.max()
            d1, d2 = st.columns(2)
            with d1:
                daily_start = st.date_input("Daily range start", value=max(daily_min, daily_max - timedelta(days=14)), min_value=daily_min, max_value=daily_max, key="mix_daily_start")
            with d2:
                daily_end = st.date_input("Daily range end", value=daily_max, min_value=daily_min, max_value=daily_max, key="mix_daily_end")
            day_range = (daily_start, daily_end)

        mix_period = build_energy_mix_period(mix_daily, granularity, year_sel=year_sel, day_range=day_range)
        if granularity == "Monthly" and mix_period.empty:
            st.info(f"No energy mix historical file is available for {year_sel}. In /data the uploaded mix file only contains 2021, and live extraction starts in 2026.")
        mix_chart = build_energy_mix_period_chart(mix_period)
        if mix_chart is not None:
            st.altair_chart(mix_chart, use_container_width=True)

        subtle_subsection("Monthly renewables summary")
        monthly_renewables_table = build_monthly_renewables_table(mix_daily)
        if not monthly_renewables_table.empty:
            sel_year = st.selectbox("Year for monthly renewables table", sorted(pd.to_datetime(monthly_renewables_table["Month"], format="%b - %Y").dt.year.unique().tolist()), index=len(sorted(pd.to_datetime(monthly_renewables_table["Month"], format="%b - %Y").dt.year.unique().tolist())) - 1)
            mt = monthly_renewables_table.copy()
            mt["_year"] = pd.to_datetime(mt["Month"], format="%b - %Y").dt.year
            mt = mt[mt["_year"] == sel_year].drop(columns=["_year"])
            st.dataframe(styled_df(mt, pct_cols=["% Renewables"]), use_container_width=True)
        else:
            st.info("No monthly renewables table available.")

    # Installed capacity
    section_header("Installed capacity")
    if installed_capacity.empty:
        st.info("No installed capacity file found in /data.")
    else:
        cap_years = sorted(installed_capacity["datetime"].dt.year.unique().tolist())
        cap_year = st.selectbox("Installed capacity year", cap_years, index=len(cap_years) - 1)
        cap_df_year = installed_capacity[installed_capacity["datetime"].dt.year == cap_year].copy()
        default_techs = [t for t in ["Solar PV", "Wind", "Hydro", "CCGT", "Nuclear"] if t in cap_df_year["technology"].unique()]
        selected_techs = st.multiselect("Technologies", sorted(cap_df_year["technology"].unique().tolist()), default=default_techs or sorted(cap_df_year["technology"].unique().tolist())[:5])
        cap_chart = build_installed_capacity_chart(cap_df_year, selected_techs)
        if cap_chart is not None:
            st.altair_chart(cap_chart, use_container_width=True)

        cap_summary = cap_df_year.groupby("datetime", as_index=False)["capacity_mw"].sum().rename(columns={"capacity_mw": "Total installed capacity (MW)"})
        cap_renew = cap_df_year[cap_df_year["technology"].isin(RENEWABLE_TECHS)].groupby("datetime", as_index=False)["capacity_mw"].sum().rename(columns={"capacity_mw": "Renewable capacity (MW)"})
        cap_table = cap_summary.merge(cap_renew, on="datetime", how="left")
        cap_table["Renewable capacity (MW)"] = cap_table["Renewable capacity (MW)"].fillna(0.0)
        cap_table["% Renewable capacity"] = cap_table["Renewable capacity (MW)"] / cap_table["Total installed capacity (MW)"]
        cap_table["Month"] = cap_table["datetime"].dt.strftime("%b - %Y")
        subtle_subsection("Installed capacity monthly summary")
        st.dataframe(styled_df(cap_table[["Month", "Total installed capacity (MW)", "Renewable capacity (MW)", "% Renewable capacity"]], pct_cols=["% Renewable capacity"]), use_container_width=True)

    # Download workbook
    section_header("Extraction workbook")
    workbook_bytes = build_price_workbook(
        price_hourly=price_hourly,
        solar_hourly=solar_hourly,
        monthly_combo=monthly_combo,
        negative_price_df=negative_price_df,
        mix_monthly_table=build_monthly_renewables_table(mix_daily),
        installed_capacity=installed_capacity,
    )
    st.download_button(
        label="Download Excel workbook",
        data=workbook_bytes,
        file_name="day_ahead_hybrid_extraction.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    subtle_subsection("Hourly prices")
    st.dataframe(styled_df(price_hourly.head(500)), use_container_width=True)
    subtle_subsection("Hourly solar")
    st.dataframe(styled_df(solar_hourly.head(500)), use_container_width=True)

except Exception as e:
    st.error(f"Error: {e}")
