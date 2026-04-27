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
DEMAND_INDICATOR_ID = 1293

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
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {"geo_trunc": "electric_system", "geo_limit": "peninsular", "geo_ids": "8741"}

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


def normalize_ree_energy_to_mwh(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    max_abs = vals.abs().max(skipna=True) if not vals.empty else None
    if pd.notna(max_abs) and max_abs < 10000:
        return vals * 1000.0
    return vals


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


def _postprocess_ree_mix_df(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    if freq == "month":
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    else:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.normalize()
    df["technology"] = df["title"].map(lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip()))
    df["energy_mwh"] = normalize_ree_energy_to_mwh(df["value"])
    df["data_source"] = "REE API"
    df = df.dropna(subset=["datetime", "technology", "energy_mwh"]).copy()
    df = df.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum()
    hydro = df[df["technology"].isin(["Hydro", "Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
    hydro["technology"] = "Hydro"
    non_hydro = df[~df["technology"].isin(["Hydro", "Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
    df = pd.concat([non_hydro, hydro], ignore_index=True)
    return df.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


def load_live_2026_mix_daily_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    try:
        payload = fetch_ree_widget("generacion", "estructura-generacion", start_day, end_day, time_trunc="day")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    return _postprocess_ree_mix_df(df, freq="day")


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_mix_monthly_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    try:
        payload = fetch_ree_widget("generacion", "estructura-generacion", start_day, end_day, time_trunc="month")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    return _postprocess_ree_mix_df(df, freq="month")


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_demand_monthly_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "demand_mwh"])
    try:
        payload = fetch_ree_widget("demanda", "ire-general", start_day, end_day, time_trunc="month")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return pd.DataFrame(columns=["datetime", "demand_mwh"])
    if df.empty:
        return pd.DataFrame(columns=["datetime", "demand_mwh"])
    df["title_norm"] = df["title"].astype(str).str.strip().str.lower()
    preferred = df[df["title_norm"].str.contains("real", na=False)].copy()
    if preferred.empty:
        preferred = df.copy()
    preferred["datetime"] = pd.to_datetime(preferred["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    preferred["demand_mwh"] = normalize_ree_energy_to_mwh(preferred["value"])
    preferred = preferred.dropna(subset=["datetime", "demand_mwh"]).copy()
    return preferred.groupby("datetime", as_index=False)["demand_mwh"].sum().sort_values("datetime").reset_index(drop=True)


def load_live_2026_installed_capacity_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])
    try:
        payload = fetch_ree_widget("generacion", "potencia-instalada", start_day, end_day, time_trunc="month")
        df = parse_ree_included_series(payload, value_field="value")
    except Exception:
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])
    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    df["technology"] = df["title"].map(lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip()))
    df["capacity_mw"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "technology", "capacity_mw"]).copy()
    return df[["datetime", "technology", "capacity_mw"]].groupby(["datetime", "technology"], as_index=False)["capacity_mw"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


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

    # Row 4 contains daily timestamps from 2022-01-01 to 2025-12-31 in ISO format.
    # We still run parse_mixed_date as a fallback because some REE exports use mixed encodings.
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

    # Technology rows. Exclude "Generación total" because totals are computed later.
    tech_rows = raw.iloc[5:18, :].copy()

    records = []
    for _, row in tech_rows.iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)

        values = pd.to_numeric(row.iloc[1:], errors="coerce")
        for dt, val in zip(dates, values):
            if pd.isna(dt) or pd.isna(val):
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
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])

    out = out[out["datetime"].dt.year.between(2022, 2025)].copy()

    # Align historical hydro format with the chart format used elsewhere.
    hydro = (
        out[out["technology"] == "Hydro"]
        .groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
    )
    hydro["technology"] = "Hydro"

    non_hydro = out[out["technology"] != "Hydro"].copy()
    out = pd.concat([non_hydro, hydro], ignore_index=True)
    out = (
        out.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"]
        .sum()
        .sort_values(["datetime", "technology", "data_source"])
        .reset_index(drop=True)
    )
    return out



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
def load_live_2026_demand(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw = fetch_esios_range(DEMAND_INDICATOR_ID, start_day, end_day, token, time_trunc="hour")
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"])
    out = raw[["datetime", "value"]].rename(columns={"value": "demand_mw"})
    out["datetime"] = out["datetime"].dt.floor("h")
    out = out.groupby("datetime", as_index=False)["demand_mw"].mean().sort_values("datetime")
    out["energy_mwh"] = out["demand_mw"]
    return out


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
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])

    frames = []

    for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
        official_df = pd.DataFrame(columns=["datetime", "value"])
        forecast_df = pd.DataFrame(columns=["datetime", "value"])

        if official_id is not None:
            try:
                official_df = fetch_esios_range(official_id, start_day, end_day, token)
            except Exception:
                official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

        forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)
        if forecast_id is not None:
            try:
                forecast_df = fetch_esios_range(forecast_id, start_day, end_day, token)
            except Exception:
                forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

        # Keep 2026 exactly on the ESIOS live path: these mix indicators are handled as energy per bucket.
        official_energy = to_programmed_generation_energy(official_df) if not official_df.empty else pd.DataFrame(columns=["datetime", "energy_mwh"])
        forecast_energy = to_programmed_generation_energy(forecast_df) if not forecast_df.empty else pd.DataFrame(columns=["datetime", "energy_mwh"])

        if official_energy.empty and forecast_energy.empty:
            continue

        if official_energy.empty:
            best = forecast_energy.copy()
            best["data_source"] = "Forecast"
        elif forecast_energy.empty:
            best = official_energy.copy()
            best["data_source"] = "Official"
        else:
            off = official_energy.rename(columns={"energy_mwh": "energy_mwh_official"})
            fc = forecast_energy.rename(columns={"energy_mwh": "energy_mwh_forecast"})
            best = off.merge(fc, on="datetime", how="outer")
            best["energy_mwh"] = best["energy_mwh_official"].combine_first(best["energy_mwh_forecast"])
            best["data_source"] = best["energy_mwh_official"].apply(lambda x: "Official" if pd.notna(x) else None)
            best.loc[best["data_source"].isna() & best["energy_mwh_forecast"].notna(), "data_source"] = "Forecast"
            best = best[["datetime", "energy_mwh", "data_source"]]

        hourly = best.copy()
        hourly["datetime"] = pd.to_datetime(hourly["datetime"], errors="coerce").dt.floor("h")
        hourly = hourly.groupby(["datetime", "data_source"], as_index=False)["energy_mwh"].sum()
        hourly["technology"] = tech_name

        daily = hourly.copy()
        daily["datetime"] = daily["datetime"].dt.normalize()
        daily = daily.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum()
        frames.append(daily)

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
    out = (
        out.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"]
        .sum()
        .sort_values(["datetime", "technology", "data_source"])
        .reset_index(drop=True)
    )
    return out


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


def build_hourly_profile_table(price_hourly: pd.DataFrame, start_sel: date, end_sel: date) -> pd.DataFrame:
    range_df = price_hourly[
        (price_hourly["datetime"].dt.date >= start_sel) &
        (price_hourly["datetime"].dt.date <= end_sel)
    ].copy()
    if range_df.empty:
        return pd.DataFrame(columns=["hour", "Average price (€/MWh)"])

    range_df["hour"] = range_df["datetime"].dt.hour
    hourly_profile = (
        range_df.groupby("hour", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "Average price (€/MWh)"})
        .sort_values("hour")
    )
    hourly_profile["hour_label"] = hourly_profile["hour"].map(lambda x: f"{int(x):02d}:00")
    return hourly_profile


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


def build_demand_period(demand_hourly: pd.DataFrame, granularity: str, year_sel: int | None = None, day_range: tuple[date, date] | None = None) -> pd.DataFrame:
    if demand_hourly.empty:
        return pd.DataFrame(columns=["period_label", "sort_key", "demand_mwh"])

    tmp = demand_hourly.copy()
    if "energy_mwh" not in tmp.columns and "demand_mw" in tmp.columns:
        tmp["energy_mwh"] = tmp["demand_mw"]

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

    return tmp.groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})

def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.DataFrame | None = None):
    if mix_period.empty:
        return None

    plot = mix_period.copy()
    plot["energy_gwh"] = plot["energy_mwh"] / 1000.0

    summary = (
        plot.groupby(["period_label", "sort_key"], as_index=False)
        .agg(total_generation_mwh=("energy_mwh", "sum"))
        .sort_values("sort_key")
    )
    renew = (
        plot[plot["technology"].isin(RENEWABLE_TECHS)]
        .groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"]
        .sum()
        .rename(columns={"energy_mwh": "renewable_generation_mwh"})
    )
    summary = summary.merge(renew, on=["period_label", "sort_key"], how="left")
    summary["renewable_generation_mwh"] = summary["renewable_generation_mwh"].fillna(0.0)
    summary["renewable_share_pct"] = summary["renewable_generation_mwh"] / summary["total_generation_mwh"]
    summary["generation_gwh"] = summary["total_generation_mwh"] / 1000.0

    if demand_period is not None and not demand_period.empty:
        demand_tmp = demand_period.copy()
        demand_tmp["demand_gwh"] = demand_tmp["demand_mwh"] / 1000.0
        summary = summary.merge(
            demand_tmp[["period_label", "sort_key", "demand_gwh"]],
            on=["period_label", "sort_key"],
            how="left",
        )
    else:
        summary["demand_gwh"] = pd.NA

    order_list = summary["period_label"].tolist()
    max_left = float(
        pd.concat(
            [
                summary["generation_gwh"],
                summary["demand_gwh"].dropna() if "demand_gwh" in summary.columns else pd.Series(dtype=float),
            ],
            ignore_index=True,
        ).max()
    ) if not summary.empty else 0.0
    max_left = max(max_left, 1.0) * 1.10

    left_scale = alt.Scale(domain=[0, max_left])

    bars = alt.Chart(plot).mark_bar().encode(
        x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y(
            "energy_gwh:Q",
            title=None,
            axis=alt.Axis(title="Generation and demand (GWh)", orient="left"),
            scale=left_scale,
        ),
        color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("energy_gwh:Q", title="Generation (GWh)", format=",.2f"),
        ],
    )

    left_layers = [bars]
    if summary["demand_gwh"].notna().any():
        demand_line = (
            alt.Chart(summary.dropna(subset=["demand_gwh"]))
            .mark_line(point=True, color="#111827", strokeWidth=2.8)
            .encode(
                x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0)),
                y=alt.Y("demand_gwh:Q", title=None, axis=None, scale=left_scale),
                tooltip=[
                    alt.Tooltip("period_label:N", title="Period"),
                    alt.Tooltip("demand_gwh:Q", title="Demand (GWh)", format=",.2f"),
                ],
            )
        )
        left_layers.append(demand_line)

    left_chart = alt.layer(*left_layers)

    re_line = alt.Chart(summary).mark_line(point=True, color=GREEN_RENEWABLES, strokeWidth=3).encode(
        x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y(
            "renewable_share_pct:Q",
            title="% RE",
            axis=alt.Axis(format=".0%", values=[0, 0.2, 0.4, 0.6, 0.8, 1.0], orient="right"),
            scale=alt.Scale(domain=[0, 1]),
        ),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("renewable_generation_mwh:Q", title="Renewables (MWh)", format=",.0f"),
            alt.Tooltip("total_generation_mwh:Q", title="Total generation (MWh)", format=",.0f"),
            alt.Tooltip("renewable_share_pct:Q", title="% RE", format=".1%"),
        ],
    )

    chart = alt.layer(left_chart, re_line).resolve_scale(y="independent").properties(height=430)
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
        return pd.DataFrame(columns=["x_start", "x_end", "year", "shade_flag"])

    rows = []
    for i, year in enumerate(years):
        rows.append(
            {
                "x_start": pd.Timestamp(year, 1, 1),
                "x_end": pd.Timestamp(year + 1, 1, 1),
                "year": str(year),
                "shade_flag": 1 if i % 2 == 0 else 0,
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
            alt.Chart(shading[shading.get("shade_flag", pd.Series(dtype=int)) == 1]).mark_rect(color=GREY_SHADE, opacity=0.9).encode(
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


def build_zero_negative_hour_table(price_hourly: pd.DataFrame, year_sel: int) -> pd.DataFrame:
    months = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    cols = [f"H{h:02d}" for h in range(24)]
    if price_hourly.empty:
        return pd.DataFrame(columns=["Month"] + cols)

    tmp = price_hourly[price_hourly["datetime"].dt.year == year_sel].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Month"] + cols)

    tmp["month_num"] = tmp["datetime"].dt.month
    tmp["hour"] = tmp["datetime"].dt.hour
    tmp["flag"] = (tmp["price"] <= 0).astype(float)

    grouped = tmp.groupby(["month_num", "hour"], as_index=False)["flag"].mean()
    pivot = grouped.pivot(index="month_num", columns="hour", values="flag").reindex(index=range(1, 13), columns=range(24)).fillna(0.0)
    pivot = pivot * 100.0
    pivot.insert(0, "Month", months)
    pivot.columns = ["Month"] + cols
    return pivot.reset_index(drop=True)


def build_zero_negative_heatmap(price_hourly: pd.DataFrame, year_sel: int):
    table = build_zero_negative_hour_table(price_hourly, year_sel)
    if table.empty:
        return None

    plot = table.melt(id_vars="Month", var_name="Hour", value_name="pct_hours")
    month_order = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]

    available_months = set(
        price_hourly[price_hourly["datetime"].dt.year == year_sel]["datetime"].dt.strftime("%b").unique().tolist()
    )
    plot["is_future"] = ~plot["Month"].isin(available_months)
    plot["pct_label"] = plot.apply(lambda r: "" if r["is_future"] else f'{r["pct_hours"]:.0f}%', axis=1)

    base = alt.Chart(plot)

    rect_past = base.transform_filter("!datum.is_future").mark_rect().encode(
        x=alt.X("Hour:N", title="Hour of day"),
        y=alt.Y("Month:N", title="Month", sort=month_order),
        color=alt.Color("pct_hours:Q", title="% hours", scale=alt.Scale(scheme="teals")),
        tooltip=[
            alt.Tooltip("Month:N", title="Month"),
            alt.Tooltip("Hour:N", title="Hour"),
            alt.Tooltip("pct_hours:Q", title="% of hours", format=",.1f"),
        ],
    )

    rect_future = base.transform_filter("datum.is_future").mark_rect(
        fill="white",
        stroke="#D1D5DB",
        strokeWidth=0.6
    ).encode(
        x=alt.X("Hour:N", title="Hour of day"),
        y=alt.Y("Month:N", title="Month", sort=month_order),
        tooltip=[
            alt.Tooltip("Month:N", title="Month"),
            alt.Tooltip("Hour:N", title="Hour"),
        ],
    )

    text_past = base.transform_filter("!datum.is_future").mark_text(fontSize=9).encode(
        x="Hour:N",
        y=alt.Y("Month:N", sort=month_order),
        text="pct_label:N",
        color=alt.condition("datum.pct_hours >= 50", alt.value("white"), alt.value("#111827")),
    )

    chart = alt.layer(rect_future, rect_past, text_past).properties(
        height=420,
        title=f"Annual Zero/Negative Prices Frequency ({year_sel})"
    )
    return apply_common_chart_style(chart, height=420)


def build_economic_curtailment_monthly(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, years: list[int] | None = None) -> pd.DataFrame:
    cols = ["year", "month_num", "month_name", "affected_production_mwh", "total_production_mwh", "pct_curtailment"]
    if price_hourly.empty or solar_hourly.empty:
        return pd.DataFrame(columns=cols)

    merged = price_hourly[["datetime", "price"]].merge(
        solar_hourly[["datetime", "solar_best_mw"]], on="datetime", how="inner"
    )
    merged["solar_best_mw"] = pd.to_numeric(merged["solar_best_mw"], errors="coerce").fillna(0.0)
    merged = merged[merged["solar_best_mw"] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=cols)

    merged["year"] = merged["datetime"].dt.year
    if years:
        merged = merged[merged["year"].isin(years)].copy()
    if merged.empty:
        return pd.DataFrame(columns=cols)

    merged["month_num"] = merged["datetime"].dt.month
    merged["month_name"] = merged["datetime"].dt.strftime("%b")
    total = merged.groupby(["year", "month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "total_production_mwh"})
    affected = merged[merged["price"] <= 0].groupby(["year", "month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "affected_production_mwh"})
    out = total.merge(affected, on=["year", "month_num", "month_name"], how="left")
    out["affected_production_mwh"] = out["affected_production_mwh"].fillna(0.0)
    out["pct_curtailment"] = out["affected_production_mwh"] / out["total_production_mwh"].where(out["total_production_mwh"] != 0)
    out["pct_curtailment"] = out["pct_curtailment"].fillna(0.0)
    return out[cols].sort_values(["year", "month_num"]).reset_index(drop=True)


def build_economic_curtailment_chart(curt_df: pd.DataFrame):
    if curt_df.empty:
        return None
    years = sorted(curt_df["year"].unique().tolist())
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626", "#0EA5E9"]
    plot = curt_df.copy()
    plot["month_label"] = plot["month_name"] + " - " + plot["year"].astype(str)
    chart = alt.Chart(plot).mark_bar().encode(
        x=alt.X(
            "month_label:N",
            title=None,
            sort=plot.sort_values(["year", "month_num"])["month_label"].tolist(),
            axis=alt.Axis(labelAngle=0),
        ),
        y=alt.Y("pct_curtailment:Q", title="Economic curtailment", axis=alt.Axis(format=".0%")),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=colors[:len(years)])),
        tooltip=[
            alt.Tooltip("year:N", title="Year"),
            alt.Tooltip("month_name:N", title="Month"),
            alt.Tooltip("affected_production_mwh:Q", title="Affected P48 (MWh)", format=",.0f"),
            alt.Tooltip("total_production_mwh:Q", title="Total P48 (MWh)", format=",.0f"),
            alt.Tooltip("pct_curtailment:Q", title="Economic curtailment", format=".1%"),
        ],
    ).properties(height=330)
    return apply_common_chart_style(chart, height=330)


def build_installed_capacity_chart(cap_df: pd.DataFrame, selected_techs: list[str]):
    if cap_df.empty or not selected_techs:
        return None
    plot = cap_df[cap_df["technology"].isin(selected_techs)].copy()
    if plot.empty:
        return None

    plot["datetime"] = pd.to_datetime(plot["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    plot = (
        plot.groupby(["datetime", "technology"], as_index=False)["capacity_mw"]
        .sum()
        .sort_values(["datetime", "technology"])
    )

    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%b-%y", labelAngle=0, tickCount="month")),
        y=alt.Y("capacity_mw:Q", title="Installed capacity (MW)"),
        color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
        tooltip=[
            alt.Tooltip("datetime:T", title="Month"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("capacity_mw:Q", title="MW", format=",.2f"),
        ],
    ).properties(height=360)
    return apply_common_chart_style(chart, height=360)

def build_price_workbook(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, monthly_combo: pd.DataFrame, negative_price_df: pd.DataFrame, mix_monthly_table: pd.DataFrame, installed_capacity: pd.DataFrame, curtailment_table: pd.DataFrame | None = None, zero_negative_hour_table: pd.DataFrame | None = None, demand_hourly: pd.DataFrame | None = None) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_hourly")
        solar_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="solar_hourly")
        monthly_combo.sort_values("month").to_excel(writer, index=False, sheet_name="monthly_capture")
        negative_price_df.to_excel(writer, index=False, sheet_name="negative_prices")
        mix_monthly_table.to_excel(writer, index=False, sheet_name="monthly_renewables")
        installed_capacity.sort_values(["datetime", "technology"]).to_excel(writer, index=False, sheet_name="installed_capacity")
        if curtailment_table is not None and not curtailment_table.empty:
            curtailment_table.to_excel(writer, index=False, sheet_name="economic_curtailment")
        if zero_negative_hour_table is not None and not zero_negative_hour_table.empty:
            zero_negative_hour_table.to_excel(writer, index=False, sheet_name="zero_neg_12x24")
        if demand_hourly is not None and not demand_hourly.empty:
            demand_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="demand_hourly")
    output.seek(0)
    return output.getvalue()


# =========================================================
# MAIN
# =========================================================
demand_hourly = pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"])
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
        hist_installed_capacity = load_installed_capacity_monthly()

        live_prices = load_live_2026_prices(token, live_start, live_end)
        live_solar = load_live_2026_solar(token, live_start, live_end)
        live_demand = load_live_2026_demand(token, live_start, live_end)
        live_mix = load_live_2026_mix_daily(token, live_start, live_end)
        live_installed_capacity = load_live_2026_installed_capacity_from_ree(live_start, live_end)

    price_hourly = combine_hist_and_live(hist_prices, live_prices, ["datetime"])
    solar_hourly = combine_hist_and_live(hist_solar, live_solar, ["datetime"])
    mix_daily = combine_hist_and_live(hist_mix, live_mix, ["datetime", "technology", "data_source"])
    installed_capacity = combine_hist_and_live(hist_installed_capacity, live_installed_capacity, ["datetime", "technology"])
    demand_hourly = live_demand.copy() if live_demand is not None else pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"])
    if demand_hourly.empty:
        demand_hourly = pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"])
    elif "energy_mwh" not in demand_hourly.columns and "demand_mw" in demand_hourly.columns:
        demand_hourly["energy_mwh"] = demand_hourly["demand_mw"]

    price_hourly = price_hourly[price_hourly["datetime"].dt.date >= start_day].copy()
    solar_hourly = solar_hourly[solar_hourly["datetime"].dt.date >= start_day].copy()
    mix_daily = mix_daily[mix_daily["datetime"].dt.date >= start_day].copy()
    if not demand_hourly.empty:
        demand_hourly = demand_hourly[demand_hourly["datetime"].dt.date >= start_day].copy()

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
    section_header("Average prices / solar / captured for selected range")
    c1, c2 = st.columns(2)
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()
    with c1:
        range_start = st.date_input(
            "Average range start",
            value=max(min_date, date(max_date.year, 1, 1)),
            min_value=min_date,
            max_value=max_date,
            key="avg_range_start",
        )
    with c2:
        range_end = st.date_input(
            "Average range end",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="avg_range_end",
        )

    if range_start > range_end:
        st.warning("Average range start cannot be later than average range end.")
    else:
        avg_metrics = compute_period_metrics(price_hourly, solar_hourly, range_start, range_end)
        m1, m2, m3 = st.columns(3)
        m1.metric("Average spot price", format_metric(avg_metrics.get("avg_price"), " €/MWh"))
        m2.metric("Captured solar (uncurtailed)", format_metric(avg_metrics.get("captured_uncurtailed"), " €/MWh"))
        m3.metric("Captured solar (curtailed)", format_metric(avg_metrics.get("captured_curtailed"), " €/MWh"))

        avg_table = pd.DataFrame([
            {
                "Range start": pd.Timestamp(range_start),
                "Range end": pd.Timestamp(range_end),
                "Average spot price": avg_metrics.get("avg_price"),
                "Captured solar (uncurtailed)": avg_metrics.get("captured_uncurtailed"),
                "Captured solar (curtailed)": avg_metrics.get("captured_curtailed"),
                "Capture rate (uncurtailed)": avg_metrics.get("capture_pct_uncurtailed"),
                "Capture rate (curtailed)": avg_metrics.get("capture_pct_curtailed"),
            }
        ])
        st.dataframe(
            styled_df(avg_table, pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"]),
            use_container_width=True,
        )

        hourly_profile = build_hourly_profile_table(price_hourly, range_start, range_end)
        if not hourly_profile.empty:
            profile_chart = alt.Chart(hourly_profile).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
                x=alt.X("hour_label:N", sort=hourly_profile["hour_label"].tolist(), axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("Average price (€/MWh):Q", title="Average price (€/MWh)"),
                tooltip=[
                    alt.Tooltip("hour_label:N", title="Hour"),
                    alt.Tooltip("Average price (€/MWh):Q", title="Average price", format=",.2f"),
                ],
            )
            st.altair_chart(apply_common_chart_style(profile_chart.properties(height=320), height=320), use_container_width=True)
            st.dataframe(styled_df(hourly_profile[["hour", "Average price (€/MWh)"]]), use_container_width=True)

    section_header("Negative prices")
    neg_mode = st.radio("Series to display", ["Zero and negative prices", "Only negative prices"], horizontal=True)
    negative_price_df = build_negative_price_curves(price_hourly, neg_mode)
    neg_chart = build_negative_price_chart(negative_price_df, neg_mode)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)
    subtle_subsection("Negative prices data")
    st.dataframe(styled_df(negative_price_df), use_container_width=True)

    section_header("Economic curtailment and zero / negative price occurrence")
    available_price_years = sorted(price_hourly["datetime"].dt.year.unique().tolist()) if not price_hourly.empty else []
    selected_curtailment_years = st.multiselect(
        "Years for economic curtailment",
        available_price_years,
        default=available_price_years[-3:] if len(available_price_years) >= 3 else available_price_years,
        key="selected_curtailment_years",
    )
    curt_df = build_economic_curtailment_monthly(price_hourly, solar_hourly, selected_curtailment_years)
    curt_chart = build_economic_curtailment_chart(curt_df)
    if curt_chart is not None:
        st.altair_chart(curt_chart, use_container_width=True)
        st.caption("Monthly economic curtailment = % of monthly P48 production generated during zero or negative price hours.")
    if not curt_df.empty:
        curt_table = curt_df.copy()
        curt_table["Month"] = curt_table["month_name"] + " - " + curt_table["year"].astype(str)
        curt_table = curt_table[["Month", "affected_production_mwh", "total_production_mwh", "pct_curtailment"]].rename(columns={
            "affected_production_mwh": "Affected P48 (MWh)",
            "total_production_mwh": "Total P48 (MWh)",
            "pct_curtailment": "Economic curtailment",
        })
        subtle_subsection("Economic curtailment table")
        st.dataframe(styled_df(curt_table, pct_cols=["Economic curtailment"]), use_container_width=True)

    heatmap_year = st.selectbox(
        "Year for annual zero / negative price frequency",
        available_price_years,
        index=len(available_price_years) - 1 if available_price_years else 0,
        key="heatmap_year_select",
    ) if available_price_years else None
    heat_table = pd.DataFrame()
    if heatmap_year is not None:
        heat_table = build_zero_negative_hour_table(price_hourly, heatmap_year)
        subtle_subsection("Annual Zero/Negative Prices Frequency")
        heat_chart = build_zero_negative_heatmap(price_hourly, heatmap_year)
        if heat_chart is not None:
            st.altair_chart(heat_chart, use_container_width=True)

    # Energy mix
    section_header("Energy mix")
    if mix_daily.empty:
        st.info("No energy mix data available.")
    else:
        granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Daily"], index=1)
        available_years = sorted(mix_daily["datetime"].dt.year.unique().tolist())
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

        mix_source_df = mix_daily
        if granularity == "Monthly" and year_sel >= 2026:
            monthly_live = load_live_2026_mix_monthly_from_ree(date(year_sel, 1, 1), max_refresh_day())
            if not monthly_live.empty:
                mix_source_df = pd.concat([
                    mix_daily[mix_daily["datetime"].dt.year < 2026],
                    monthly_live,
                ], ignore_index=True)
        mix_period = build_energy_mix_period(mix_source_df, granularity, year_sel=year_sel, day_range=day_range)
        demand_period = build_demand_period(
            demand_hourly if isinstance(demand_hourly, pd.DataFrame) else pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"]),
            granularity,
            year_sel=year_sel,
            day_range=day_range,
        )
        if granularity == "Monthly" and mix_period.empty:
            st.info(f"No energy mix data available for {year_sel}. The historical mix file covers 2022-2025 and live extraction starts in 2026.")
        mix_chart = build_energy_mix_period_chart(mix_period, demand_period)
        if mix_chart is not None:
            st.altair_chart(mix_chart, use_container_width=True)
            st.caption("Línea verde = % RE (eje derecho 0%-100%). Línea negra = demanda total del periodo (eje izquierdo).")

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
        default_years = cap_years[-3:] if len(cap_years) >= 3 else cap_years
        selected_cap_years = st.multiselect("Installed capacity years", cap_years, default=default_years)
        cap_df_year = installed_capacity[installed_capacity["datetime"].dt.year.isin(selected_cap_years)].copy()
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

    # Raw 12x24 table download (kept at the end)
    if not heat_table.empty:
        subtle_subsection("Download raw 12x24 zero / negative price table")
        with st.expander("Show raw 12x24 table", expanded=False):
            st.dataframe(heat_table, use_container_width=True)
        csv_zero_neg = heat_table.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download 12x24 zero / negative price table (CSV)",
            data=csv_zero_neg,
            file_name=f"zero_negative_price_frequency_{heatmap_year}.csv",
            mime="text/csv",
            key="download_zero_negative_table_csv",
        )

    # Download workbook
    section_header("Extraction workbook")
    workbook_bytes = build_price_workbook(
        price_hourly=price_hourly,
        solar_hourly=solar_hourly,
        monthly_combo=monthly_combo,
        negative_price_df=negative_price_df,
        mix_monthly_table=build_monthly_renewables_table(mix_daily),
        installed_capacity=installed_capacity,
        curtailment_table=build_economic_curtailment_monthly(price_hourly, solar_hourly, sorted(price_hourly["datetime"].dt.year.unique().tolist())),
        zero_negative_hour_table=build_zero_negative_hour_table(price_hourly, int(price_hourly["datetime"].dt.year.max())) if not price_hourly.empty else pd.DataFrame(),
        demand_hourly=demand_hourly,
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
