import os
import re
from datetime import date, datetime, time, timedelta
from time import sleep
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt

# Altair default limit is 5,000 rows.
# The OMIE full-year hourly heatmap needs 8,760/8,784 rows.
try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
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


CAPACITY_COLOR_DOMAIN = [
    "Solar PV",
    "Wind",
    "Hydro",
    "CCGT",
    "Nuclear",
    "Solar thermal",
    "Pumped hydro",
    "CHP",
    "Biomass",
    "Biogas",
    "Other renewables",
    "Coal",
    "Fuel + Gas",
    "Steam turbine",
    "Other non-renewables",
]

CAPACITY_COLOR_RANGE = [
    "#FACC15",  # Solar PV - yellow
    "#2563EB",  # Wind - blue
    "#38BDF8",  # Hydro - light blue
    "#9CA3AF",  # CCGT - grey
    "#C084FC",
    "#FCA5A5",
    "#0284C7",
    "#F97316",
    "#16A34A",
    "#22C55E",
    "#14B8A6",
    "#374151",
    "#6B7280",
    "#4B5563",
    "#7C2D12",
]

CAPACITY_COLOR_SCALE = alt.Scale(domain=CAPACITY_COLOR_DOMAIN, range=CAPACITY_COLOR_RANGE)

TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
GREY_SHADE = "#F3F4F6"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
BLUE_PRICE = "#1D4ED8"
GREEN_RENEWABLES = "#059669"
PRICE_LOW_GREEN_DARK = "#006400"
PRICE_LOW_GREEN = "#16A34A"
PRICE_MID_YELLOW = "#FDE047"
PRICE_MID_ORANGE = "#F97316"
PRICE_HIGH_RED = "#DC2626"
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {"geo_trunc": "electric_system", "geo_limit": "peninsular", "geo_ids": "8741"}

EEX_MARKET_DATA_HUB_URL = "https://www.eex.com/en/market-data/market-data-hub"
EEX_FORWARD_LOCAL_CSV = DATA_DIR / "eex_forward_market.csv"
EEX_FORWARD_LOCAL_XLSX = DATA_DIR / "eex_forward_market.xlsx"

# AEMET OpenData. Get a free API key from AEMET OpenData and place it in .env as AEMET_API_KEY=...
AEMET_API_BASE = "https://opendata.aemet.es/opendata/api"
AEMET_CACHE_FILE = DATA_DIR / "aemet_daily_cache.csv"

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

    # In the REE monthly demand widget, values are reported in GWh for the monthly view.
    # Convert them explicitly to MWh so the chart, which later divides by 1000 to display GWh,
    # lands in the correct 20k-24k GWh range.
    preferred["demand_mwh"] = pd.to_numeric(preferred["value"], errors="coerce") * 1000.0

    preferred = preferred.dropna(subset=["datetime", "demand_mwh"]).copy()
    return preferred.groupby("datetime", as_index=False)["demand_mwh"].sum().sort_values("datetime").reset_index(drop=True)


def load_live_2026_installed_capacity_from_ree(start_day: date, end_day: date) -> pd.DataFrame:
    """Load live 2026 installed capacity from REE monthly API.

    Installed capacity is a stock and REE sometimes publishes the current year
    with only the latest completed/available month. This function therefore tries
    several month-end windows and keeps whatever 2026 months are available, so
    the annual additions chart can show 2026 even when the year/month is not
    complete yet.
    """
    cols = ["datetime", "technology", "capacity_mw"]
    start_day = max(start_day, LIVE_START_DATE)
    if start_day > end_day:
        return pd.DataFrame(columns=cols)

    def _month_end(d: date) -> date:
        first_next = date(d.year + (1 if d.month == 12 else 0), 1 if d.month == 12 else d.month + 1, 1)
        return first_next - timedelta(days=1)

    candidate_windows: list[tuple[date, date]] = []
    # Main request: from 1-Jan-2026 to the selected/live end day.
    candidate_windows.append((start_day, end_day))
    # If the current month is not yet published, try through the previous month-end.
    first_this_month = date(end_day.year, end_day.month, 1)
    prev_month_end = first_this_month - timedelta(days=1)
    if prev_month_end >= start_day:
        candidate_windows.append((start_day, prev_month_end))
    # Robust fallback: query each 2026 month separately, including the partial current month.
    m = date(start_day.year, start_day.month, 1)
    while m <= end_day:
        me = min(_month_end(m), end_day)
        if me >= start_day:
            candidate_windows.append((max(m, start_day), me))
        if m.month == 12:
            m = date(m.year + 1, 1, 1)
        else:
            m = date(m.year, m.month + 1, 1)

    frames = []
    for s_day, e_day in candidate_windows:
        try:
            payload = fetch_ree_widget("generacion", "potencia-instalada", s_day, e_day, time_trunc="month")
            df = parse_ree_included_series(payload, value_field="value")
        except Exception:
            df = pd.DataFrame(columns=["datetime", "title", "value"])
        if df.empty:
            continue
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        df["technology"] = df["title"].map(lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip()))
        df["capacity_mw"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["datetime", "technology", "capacity_mw"]).copy()
        if not df.empty:
            frames.append(df[cols])

    if not frames:
        return pd.DataFrame(columns=cols)

    out = pd.concat(frames, ignore_index=True)
    out = (
        out.groupby(["datetime", "technology"], as_index=False)["capacity_mw"]
        .sum()
        .sort_values(["datetime", "technology"])
        .drop_duplicates(subset=["datetime", "technology"], keep="last")
        .reset_index(drop=True)
    )
    return out


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
    """
    Fetch ESIOS data in smaller chunks.

    The ESIOS endpoint often times out when asking for several months of
    quarter-hourly data in a single request. Chunking + retries avoids the
    Streamlit app crashing with Read timed out errors. If a chunk still fails,
    it is skipped and the rest of the app continues with historical data.
    """
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if time_trunc is None:
        time_trunc = "quarter_hour" if start_day >= date(2025, 10, 1) else "hour"

    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    frames = []

    chunk_start = start_day
    chunk_days = 14 if time_trunc == "quarter_hour" else 31

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
            # Do not crash the whole dashboard if ESIOS is slow/down for one chunk.
            # Returning partial data is better than blocking the historical analysis.
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

    # The live demand series can arrive one order of magnitude above the expected
    # Spanish hourly load level in this extraction path. Keep the hourly-series logic
    # but normalize the scale when the median is clearly out of range.
    median_mw = out["demand_mw"].median() if not out.empty else None
    if median_mw is not None and pd.notna(median_mw) and median_mw > 100000:
        out["demand_mw"] = out["demand_mw"] / 10.0

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


def normalize_installed_capacity_df(cap_df: pd.DataFrame) -> pd.DataFrame:
    """Make installed-capacity data robust before any .dt calls.

    The historical Excel loader and the live REE API path can occasionally return
    empty/object-typed datetime columns. Streamlit then raises:
    "Can only use .dt accessor with datetimelike values".
    This function standardises the dataframe to monthly naive timestamps and
    numeric MW values, and always returns the expected columns.
    """
    expected_cols = ["datetime", "technology", "capacity_mw"]
    if cap_df is None or cap_df.empty:
        return pd.DataFrame(columns=expected_cols)

    out = cap_df.copy()
    for col in expected_cols:
        if col not in out.columns:
            out[col] = pd.NA

    # Use UTC parsing to handle possible tz-aware strings from REE and plain
    # Excel timestamps in one path, then convert back to Madrid/no timezone.
    dt = pd.to_datetime(out["datetime"], errors="coerce", utc=True)
    out["datetime"] = (
        dt.dt.tz_convert("Europe/Madrid")
        .dt.tz_localize(None)
        .dt.to_period("M")
        .dt.to_timestamp()
    )
    out["technology"] = out["technology"].astype(str).str.strip()
    out["capacity_mw"] = pd.to_numeric(out["capacity_mw"], errors="coerce")

    out = out.dropna(subset=["datetime", "technology", "capacity_mw"]).copy()
    if out.empty:
        return pd.DataFrame(columns=expected_cols)

    return (
        out[expected_cols]
        .groupby(["datetime", "technology"], as_index=False)["capacity_mw"]
        .sum()
        .sort_values(["datetime", "technology"])
        .reset_index(drop=True)
    )


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


def build_capture_price_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, aggregation: str = "Monthly") -> pd.DataFrame:
    """
    Build spot and solar captured prices at the selected aggregation level.

    Spot price is a simple average of hourly prices.
    Solar captured price is a generation-weighted average:
        sum(price * solar_generation) / sum(solar_generation)

    For the curtailed captured price, hours with price <= 0 are excluded.
    """
    if aggregation not in {"Daily", "Weekly", "Monthly", "Annual"}:
        aggregation = "Monthly"

    def add_period(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if aggregation == "Daily":
            out["period"] = out["datetime"].dt.normalize()
        elif aggregation == "Weekly":
            # Monday-start week, so each point represents the week starting on that Monday.
            out["period"] = out["datetime"].dt.to_period("W-SUN").dt.start_time
        elif aggregation == "Monthly":
            out["period"] = out["datetime"].dt.to_period("M").dt.to_timestamp()
        else:
            out["period"] = out["datetime"].dt.to_period("Y").dt.to_timestamp()
        return out

    cols = [
        "period",
        "avg_spot_price",
        "captured_solar_price_uncurtailed",
        "captured_solar_price_curtailed",
        "capture_pct_uncurtailed",
        "capture_pct_curtailed",
    ]
    if price_hourly.empty:
        return pd.DataFrame(columns=cols)

    price_period = add_period(price_hourly)
    avg_price = (
        price_period.groupby("period", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_spot_price"})
    )

    if solar_hourly.empty:
        avg_price["captured_solar_price_uncurtailed"] = pd.NA
        avg_price["captured_solar_price_curtailed"] = pd.NA
        avg_price["capture_pct_uncurtailed"] = pd.NA
        avg_price["capture_pct_curtailed"] = pd.NA
        return avg_price.sort_values("period").reset_index(drop=True)

    merged = price_hourly.merge(solar_hourly[["datetime", "solar_best_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()
    if merged.empty:
        avg_price["captured_solar_price_uncurtailed"] = pd.NA
        avg_price["captured_solar_price_curtailed"] = pd.NA
        avg_price["capture_pct_uncurtailed"] = pd.NA
        avg_price["capture_pct_curtailed"] = pd.NA
        return avg_price.sort_values("period").reset_index(drop=True)

    merged = add_period(merged)
    merged["weighted_price"] = merged["price"] * merged["solar_best_mw"]

    uncurtailed = (
        merged.groupby("period", as_index=False)
        .agg(weighted_price_sum=("weighted_price", "sum"), solar_sum=("solar_best_mw", "sum"))
    )
    uncurtailed["captured_solar_price_uncurtailed"] = uncurtailed["weighted_price_sum"] / uncurtailed["solar_sum"]
    uncurtailed = uncurtailed[["period", "captured_solar_price_uncurtailed"]]

    curtailed = merged[merged["price"] > 0].copy()
    if curtailed.empty:
        curtailed_periods = pd.DataFrame(columns=["period", "captured_solar_price_curtailed"])
    else:
        curtailed_periods = (
            curtailed.groupby("period", as_index=False)
            .agg(weighted_price_sum=("weighted_price", "sum"), solar_sum=("solar_best_mw", "sum"))
        )
        curtailed_periods["captured_solar_price_curtailed"] = curtailed_periods["weighted_price_sum"] / curtailed_periods["solar_sum"]
        curtailed_periods = curtailed_periods[["period", "captured_solar_price_curtailed"]]

    out = avg_price.merge(uncurtailed, on="period", how="left").merge(curtailed_periods, on="period", how="left")
    out["capture_pct_uncurtailed"] = out["captured_solar_price_uncurtailed"] / out["avg_spot_price"]
    out["capture_pct_curtailed"] = out["captured_solar_price_curtailed"] / out["avg_spot_price"]
    return out.sort_values("period").reset_index(drop=True)


def build_monthly_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    """Backwards-compatible monthly wrapper."""
    out = build_capture_price_table(price_hourly, solar_hourly, "Monthly").rename(
        columns={"period": "month", "avg_spot_price": "avg_monthly_price"}
    )
    return out

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


# =========================================================
# WEEKLY LOAD EVOLUTION / AEMET WEATHER ANOMALIES
# =========================================================
def normalize_ree_demand_to_mwh(series: pd.Series) -> pd.Series:
    """REE demand widgets can come in GWh in some truncations; normalize to MWh."""
    vals = pd.to_numeric(series, errors="coerce")
    max_abs = vals.abs().max(skipna=True) if not vals.empty else None
    # Spanish daily demand is normally hundreds of GWh. If values are < 5,000,
    # treat them as GWh and convert to MWh.
    if pd.notna(max_abs) and max_abs < 5000:
        return vals * 1000.0
    return vals


@st.cache_data(show_spinner=False, ttl=86400)
def load_ree_demand_daily_history(start_day: date, end_day: date) -> pd.DataFrame:
    """Load daily peninsular demand from REE and return MWh per day.

    This is used for weekly load evolution. It is independent from ESIOS and
    therefore more robust for historical demand comparisons.
    """
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "demand_mwh", "data_source"])

    frames = []
    y0, y1 = start_day.year, end_day.year
    for year in range(y0, y1 + 1):
        s_day = max(start_day, date(year, 1, 1))
        e_day = min(end_day, date(year, 12, 31))
        try:
            payload = fetch_ree_widget("demanda", "ire-general", s_day, e_day, time_trunc="day")
            df = parse_ree_included_series(payload, value_field="value")
        except Exception:
            df = pd.DataFrame(columns=["datetime", "title", "value"])

        if df.empty:
            continue

        df["title_norm"] = df["title"].astype(str).str.strip().str.lower()
        preferred = df[df["title_norm"].str.contains("real", na=False)].copy()
        if preferred.empty:
            preferred = df.copy()

        preferred["datetime"] = pd.to_datetime(preferred["datetime"], errors="coerce").dt.normalize()
        preferred["demand_mwh"] = normalize_ree_demand_to_mwh(preferred["value"])
        preferred["data_source"] = "REE API"
        preferred = preferred.dropna(subset=["datetime", "demand_mwh"]).copy()
        if not preferred.empty:
            frames.append(preferred[["datetime", "demand_mwh", "data_source"]])

    if not frames:
        return pd.DataFrame(columns=["datetime", "demand_mwh", "data_source"])

    out = pd.concat(frames, ignore_index=True)
    return (
        out.groupby(["datetime", "data_source"], as_index=False)["demand_mwh"]
        .sum()
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def build_weekly_load_evolution(demand_daily: pd.DataFrame) -> pd.DataFrame:
    cols = ["year", "week", "week_start", "weekly_load_gwh", "cum_load_gwh"]
    if demand_daily.empty:
        return pd.DataFrame(columns=cols)
    tmp = demand_daily.copy()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp["demand_mwh"] = pd.to_numeric(tmp["demand_mwh"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime", "demand_mwh"])
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    iso = tmp["datetime"].dt.isocalendar()
    tmp["year"] = iso["year"].astype(int)
    tmp["week"] = iso["week"].astype(int)
    tmp["week_start"] = tmp["datetime"].dt.to_period("W-SUN").dt.start_time
    weekly = (
        tmp.groupby(["year", "week", "week_start"], as_index=False)["demand_mwh"]
        .sum()
        .sort_values(["year", "week"])
    )
    weekly["weekly_load_gwh"] = weekly["demand_mwh"] / 1000.0
    weekly["cum_load_gwh"] = weekly.groupby("year")["weekly_load_gwh"].cumsum()
    return weekly[cols]


def build_weekly_load_chart(weekly_load: pd.DataFrame, selected_years: list[int], cumulative: bool = False):
    if weekly_load.empty or not selected_years:
        return None
    plot = weekly_load[weekly_load["year"].isin(selected_years)].copy()
    if plot.empty:
        return None
    y_col = "cum_load_gwh" if cumulative else "weekly_load_gwh"
    y_title = "Cumulative load (GWh)" if cumulative else "Weekly load (GWh)"
    title = "Cumulative load evolution by ISO week" if cumulative else "Weekly load evolution by ISO week"
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626", "#0EA5E9"]
    years = sorted(plot["year"].unique().tolist())
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("week:O", sort=list(range(1, 54)), axis=alt.Axis(title="ISO week", labelAngle=0)),
        y=alt.Y(f"{y_col}:Q", title=y_title),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=[str(y) for y in years], range=colors[:len(years)])),
        detail="year:N",
        tooltip=[
            alt.Tooltip("year:N", title="Year"),
            alt.Tooltip("week:O", title="ISO week"),
            alt.Tooltip("week_start:T", title="Week start", format="%Y-%m-%d"),
            alt.Tooltip(f"{y_col}:Q", title=y_title, format=",.1f"),
        ],
    ).properties(height=360, title=title)
    return apply_common_chart_style(chart, height=360)


def parse_spanish_decimal(value):
    if pd.isna(value):
        return pd.NA
    s = str(value).strip().replace(",", ".")
    if not s or s.lower() in {"nan", "none", "ip"}:
        return pd.NA
    try:
        return float(s)
    except Exception:
        return pd.NA


def get_aemet_token() -> str | None:
    token = (os.getenv("AEMET_API_KEY") or os.getenv("AEMET_TOKEN") or "").strip()
    return token or None


def _aemet_indirect_get(endpoint: str, token: str) -> list[dict]:
    """AEMET OpenData returns a first JSON with a 'datos' URL; fetch that URL."""
    url = f"{AEMET_API_BASE}/{endpoint.lstrip('/')}"
    first = requests.get(url, params={"api_key": token}, timeout=(15, 60))
    first.raise_for_status()
    meta = first.json()
    datos_url = meta.get("datos")
    if not datos_url:
        return []
    second = requests.get(datos_url, timeout=(15, 120))
    second.raise_for_status()
    payload = second.json()
    return payload if isinstance(payload, list) else []


def _read_aemet_cache() -> pd.DataFrame:
    cols = ["fecha", "indicativo", "provincia", "nombre", "tmed", "hrMedia"]
    if not AEMET_CACHE_FILE.exists():
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(AEMET_CACHE_FILE, dtype={"indicativo": str})
    except Exception:
        return pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    return df[cols].dropna(subset=["fecha"]).copy()


def _write_aemet_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return
    out = df.copy()
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.drop_duplicates(subset=["fecha", "indicativo"], keep="last")
    AEMET_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(AEMET_CACHE_FILE, index=False)


@st.cache_data(show_spinner=False, ttl=86400)
def load_aemet_daily_all_stations(start_day: date, end_day: date, token: str) -> pd.DataFrame:
    """Fetch AEMET daily climatological data for all stations, with a CSV cache.

    AEMET daily all-station requests are limited in practice, so this function
    chunks the requested range in 10-day blocks and caches responses in /data.
    """
    cols = ["fecha", "indicativo", "provincia", "nombre", "tmed", "hrMedia"]
    if start_day > end_day:
        return pd.DataFrame(columns=cols)

    cache = _read_aemet_cache()
    available_dates = set(cache["fecha"].tolist()) if not cache.empty else set()
    needed_dates = pd.date_range(start_day, end_day, freq="D").date.tolist()
    missing_dates = [d for d in needed_dates if d not in available_dates]

    frames = [cache] if not cache.empty else []
    if missing_dates:
        chunk_start = min(missing_dates)
        while chunk_start <= end_day:
            # Skip forward until the next missing day.
            if chunk_start not in missing_dates:
                chunk_start += timedelta(days=1)
                continue
            chunk_end = min(chunk_start + timedelta(days=9), end_day)
            endpoint = (
                "valores/climatologicos/diarios/datos/"
                f"fechaini/{chunk_start.isoformat()}T00:00:00UTC/"
                f"fechafin/{chunk_end.isoformat()}T23:59:59UTC/"
                "todasestaciones"
            )
            try:
                rows = _aemet_indirect_get(endpoint, token)
                if rows:
                    raw = pd.DataFrame(rows)
                    df = pd.DataFrame()
                    df["fecha"] = pd.to_datetime(raw.get("fecha"), errors="coerce").dt.date
                    df["indicativo"] = raw.get("indicativo", pd.Series(dtype=str)).astype(str)
                    df["provincia"] = raw.get("provincia", pd.Series(dtype=str)).astype(str)
                    df["nombre"] = raw.get("nombre", pd.Series(dtype=str)).astype(str)
                    df["tmed"] = raw.get("tmed", pd.Series(dtype=object)).map(parse_spanish_decimal)
                    # AEMET field is usually hrMedia. Some old records may not include it.
                    if "hrMedia" in raw.columns:
                        df["hrMedia"] = raw["hrMedia"].map(parse_spanish_decimal)
                    elif "hrmedia" in raw.columns:
                        df["hrMedia"] = raw["hrmedia"].map(parse_spanish_decimal)
                    else:
                        df["hrMedia"] = pd.NA
                    df = df.dropna(subset=["fecha", "indicativo"]).copy()
                    frames.append(df[cols])
            except Exception:
                pass
            sleep(0.35)
            chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=cols)

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["fecha", "indicativo"], keep="last")
    # Keep only requested range for return, but write full cache.
    _write_aemet_cache(out[cols])
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce")
    return out[(out["fecha"].dt.date >= start_day) & (out["fecha"].dt.date <= end_day)][cols].reset_index(drop=True)


def build_aemet_weekly_anomalies(current_df: pd.DataFrame, baseline_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "week_start", "tmed_actual", "tmed_normal", "temperature_anomaly_c",
        "humidity_actual", "humidity_normal", "humidity_anomaly_pp", "stations_count"
    ]
    if current_df.empty or baseline_df.empty:
        return pd.DataFrame(columns=cols)

    cur = current_df.copy()
    base = baseline_df.copy()
    for df in [cur, base]:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["tmed"] = pd.to_numeric(df["tmed"], errors="coerce")
        df["hrMedia"] = pd.to_numeric(df["hrMedia"], errors="coerce")
        df["doy"] = df["fecha"].dt.dayofyear

    # Spain-wide simple station average. For precision, this can later be replaced
    # with area-weighted station interpolation, but this is robust for dashboard monitoring.
    cur_daily = cur.groupby("fecha", as_index=False).agg(
        tmed_actual=("tmed", "mean"),
        humidity_actual=("hrMedia", "mean"),
        stations_count=("indicativo", "nunique"),
    )
    cur_daily["doy"] = cur_daily["fecha"].dt.dayofyear

    base_daily = base.groupby(["fecha", "doy"], as_index=False).agg(
        tmed=("tmed", "mean"),
        hrMedia=("hrMedia", "mean"),
    )
    normals = base_daily.groupby("doy", as_index=False).agg(
        tmed_normal=("tmed", "mean"),
        humidity_normal=("hrMedia", "mean"),
    )

    merged = cur_daily.merge(normals, on="doy", how="left")
    merged["temperature_anomaly_c"] = merged["tmed_actual"] - merged["tmed_normal"]
    merged["humidity_anomaly_pp"] = merged["humidity_actual"] - merged["humidity_normal"]
    merged["week_start"] = merged["fecha"].dt.to_period("W-SUN").dt.start_time

    weekly = merged.groupby("week_start", as_index=False).agg(
        tmed_actual=("tmed_actual", "mean"),
        tmed_normal=("tmed_normal", "mean"),
        temperature_anomaly_c=("temperature_anomaly_c", "mean"),
        humidity_actual=("humidity_actual", "mean"),
        humidity_normal=("humidity_normal", "mean"),
        humidity_anomaly_pp=("humidity_anomaly_pp", "mean"),
        stations_count=("stations_count", "mean"),
    )
    return weekly[cols].sort_values("week_start").reset_index(drop=True)


def build_aemet_anomaly_charts(anom_df: pd.DataFrame):
    if anom_df.empty:
        return None
    temp = alt.Chart(anom_df).mark_line(point=True, strokeWidth=3, color="#DC2626").encode(
        x=alt.X("week_start:T", title=None, axis=alt.Axis(format="%d-%b", labelAngle=0)),
        y=alt.Y("temperature_anomaly_c:Q", title="Temp anomaly (°C)"),
        tooltip=[
            alt.Tooltip("week_start:T", title="Week", format="%Y-%m-%d"),
            alt.Tooltip("temperature_anomaly_c:Q", title="Temperature anomaly °C", format=",.2f"),
            alt.Tooltip("tmed_actual:Q", title="Actual tmed °C", format=",.2f"),
            alt.Tooltip("tmed_normal:Q", title="Baseline tmed °C", format=",.2f"),
            alt.Tooltip("stations_count:Q", title="Stations", format=",.0f"),
        ],
    ).properties(height=250, title="AEMET weekly temperature anomaly vs baseline")

    hum = alt.Chart(anom_df.dropna(subset=["humidity_anomaly_pp"])).mark_line(point=True, strokeWidth=3, color=CORP_GREEN).encode(
        x=alt.X("week_start:T", title=None, axis=alt.Axis(format="%d-%b", labelAngle=0)),
        y=alt.Y("humidity_anomaly_pp:Q", title="Humidity anomaly (pp)"),
        tooltip=[
            alt.Tooltip("week_start:T", title="Week", format="%Y-%m-%d"),
            alt.Tooltip("humidity_anomaly_pp:Q", title="Humidity anomaly pp", format=",.2f"),
            alt.Tooltip("humidity_actual:Q", title="Actual humidity %", format=",.2f"),
            alt.Tooltip("humidity_normal:Q", title="Baseline humidity %", format=",.2f"),
            alt.Tooltip("stations_count:Q", title="Stations", format=",.0f"),
        ],
    ).properties(height=250, title="AEMET weekly relative humidity anomaly vs baseline")

    zero_temp = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="#6B7280", strokeDash=[4, 4]).encode(y="y:Q")
    temp = alt.layer(temp, zero_temp).resolve_scale(y="shared")
    hum = alt.layer(hum, zero_temp).resolve_scale(y="shared")
    return apply_common_chart_style(alt.vconcat(temp, hum, spacing=18), height=250)


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


def build_period_shading_df(capture_combo: pd.DataFrame) -> pd.DataFrame:
    if capture_combo.empty or "period" not in capture_combo.columns:
        return pd.DataFrame(columns=["x_start", "x_end", "year", "shade_flag"])

    years = sorted(capture_combo["period"].dt.year.unique().tolist())
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


def build_capture_price_chart(capture_combo: pd.DataFrame, aggregation: str = "Monthly"):
    if capture_combo.empty:
        return None

    plot_df = capture_combo.copy().rename(
        columns={
            "avg_spot_price": "Average spot price",
            "captured_solar_price_curtailed": "Solar captured (curtailed)",
            "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)",
        }
    )

    long_df = plot_df.melt(
        id_vars=["period"],
        value_vars=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])

    if long_df.empty:
        return None

    long_df["year"] = long_df["period"].dt.year.astype(str)
    long_df["year_mid"] = pd.to_datetime(long_df["period"].dt.year.astype(str) + "-07-01")

    shading = build_period_shading_df(capture_combo)

    color_scale = alt.Scale(
        domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        range=[BLUE_PRICE, YELLOW_DARK, YELLOW_LIGHT],
    )
    dash_scale = alt.Scale(
        domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        range=[[1, 0], [6, 4], [2, 2]],
    )

    if aggregation == "Annual":
        # Use a nominal x-axis for yearly data. If we keep period:T, Vega-Lite auto-generates
        # intermediate monthly ticks, which makes the same year appear many times.
        annual_order = sorted(long_df["period"].dt.year.astype(str).unique().tolist())
        long_df["period_label"] = long_df["period"].dt.year.astype(str)

        base = alt.Chart(long_df).encode(
            x=alt.X(
                "period_label:N",
                sort=annual_order,
                axis=alt.Axis(
                    title=None,
                    labelAngle=0,
                    labelPadding=8,
                    ticks=False,
                    domain=False,
                    grid=False,
                ),
            )
        )

        main = base.mark_line(point=True, strokeWidth=3).encode(
            y=alt.Y("value:Q", title="€/MWh"),
            color=alt.Color("series:N", title=None, scale=color_scale),
            strokeDash=alt.StrokeDash("series:N", title=None, scale=dash_scale),
            tooltip=[
                alt.Tooltip("period_label:N", title="Year"),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("value:Q", title="€/MWh", format=",.2f"),
            ],
        ).properties(height=330)

        return apply_common_chart_style(main, height=330)

    x_format = {
        "Daily": "%d-%b",
        "Weekly": "%d-%b",
        "Monthly": "%b",
    }.get(aggregation, "%b")

    base = alt.Chart(long_df).encode(
        x=alt.X(
            "period:T",
            axis=alt.Axis(
                title=None,
                format=x_format,
                labelAngle=0 if aggregation == "Monthly" else -35,
                labelPadding=8,
                labelFlush=False,
                ticks=False,
                domain=False,
                grid=False,
                labelOverlap="greedy",
            ),
        )
    )

    main = base.mark_line(point=True, strokeWidth=3).encode(
        y=alt.Y("value:Q", title="€/MWh"),
        color=alt.Color("series:N", title=None, scale=color_scale),
        strokeDash=alt.StrokeDash("series:N", title=None, scale=dash_scale),
        tooltip=[
            alt.Tooltip("period:T", title="Period"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="€/MWh", format=",.2f"),
        ],
    ).properties(height=330)

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


def build_monthly_shading_df(monthly_combo: pd.DataFrame) -> pd.DataFrame:
    tmp = monthly_combo.rename(columns={"month": "period"}) if "month" in monthly_combo.columns else monthly_combo.copy()
    return build_period_shading_df(tmp)


def build_monthly_main_chart(monthly_combo: pd.DataFrame):
    tmp = monthly_combo.rename(
        columns={"month": "period", "avg_monthly_price": "avg_spot_price"}
    ) if "month" in monthly_combo.columns else monthly_combo.copy()
    return build_capture_price_chart(tmp, "Monthly")


def build_hourly_price_heatmap_table(price_hourly: pd.DataFrame, year_sel: int) -> pd.DataFrame:
    """Return a proper 24 x 365/366 OMIE hourly price grid for a selected year.

    Important: this function keeps one column per calendar day and one row per hour.
    The chart should use day_of_year as an ORDINAL x-axis, not a temporal x-axis,
    otherwise Vega/Altair can render huge overlapping rectangles that look like
    horizontal bands by hour rather than a real daily heatmap.
    """
    cols = [
        "datetime",
        "date",
        "date_label",
        "day_of_year",
        "month_num",
        "month_name",
        "hour",
        "price",
        "is_missing",
    ]
    if price_hourly.empty:
        return pd.DataFrame(columns=cols)

    tmp = price_hourly.copy()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp["price"] = pd.to_numeric(tmp["price"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime", "price"])
    tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    tmp["date"] = tmp["datetime"].dt.normalize()
    tmp["hour"] = tmp["datetime"].dt.hour

    # Full calendar grid: 365/366 days x 24 hours.
    full_days = pd.date_range(date(year_sel, 1, 1), date(year_sel, 12, 31), freq="D")
    full_grid = pd.MultiIndex.from_product(
        [full_days, range(24)],
        names=["date", "hour"],
    ).to_frame(index=False)
    full_grid["date"] = pd.to_datetime(full_grid["date"])
    full_grid["datetime"] = full_grid["date"] + pd.to_timedelta(full_grid["hour"], unit="h")
    full_grid["date_label"] = full_grid["date"].dt.strftime("%Y-%m-%d")
    full_grid["day_of_year"] = full_grid["date"].dt.dayofyear.astype(int)
    full_grid["month_num"] = full_grid["date"].dt.month.astype(int)
    full_grid["month_name"] = full_grid["date"].dt.strftime("%b")

    # If there are duplicated hourly observations, average them before merging.
    hourly = (
        tmp.groupby(["date", "hour"], as_index=False)["price"]
        .mean()
        .sort_values(["date", "hour"])
    )

    out = full_grid.merge(hourly, on=["date", "hour"], how="left")
    out["is_missing"] = out["price"].isna()
    return out[cols]


def _month_start_day_numbers(year_sel: int) -> list[int]:
    """Day-of-year positions used as month tick marks on the heatmap x-axis."""
    return [pd.Timestamp(year_sel, m, 1).dayofyear for m in range(1, 13)]


def _month_label_expr(year_sel: int) -> str:
    """Vega label expression that prints Jan/Feb/... at month-start day numbers."""
    month_starts = _month_start_day_numbers(year_sel)
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    parts = [f"datum.value == {d} ? '{name}'" for d, name in zip(month_starts, month_names)]
    return " : ".join(parts) + " : ''"


def build_hourly_price_heatmap(price_hourly: pd.DataFrame, year_sel: int):
    """Altair heatmap: x = day of year, y = hour, color = spot price.

    Color convention requested:
      * strong green = very low prices
      * yellow/orange = medium prices
      * strong red = very high prices

    The x-axis is ordinal day_of_year. This avoids the misleading rendering that
    happens when mark_rect is plotted against a temporal axis without x2: cells
    overlap and the chart can look like smooth horizontal bands.
    """
    plot = build_hourly_price_heatmap_table(price_hourly, year_sel)
    if plot.empty:
        return None

    valid_prices = plot["price"].dropna()
    if valid_prices.empty:
        return None

    # Use a robust scale: include real negatives if present, but avoid one outlier
    # dominating the whole palette. Keep 0 as the low-price reference.
    p_low = float(min(valid_prices.min(), valid_prices.quantile(0.01), 0.0))
    p_high = float(max(valid_prices.quantile(0.99), 120.0))
    if p_high <= p_low:
        p_high = p_low + 1.0

    p_mid_1 = p_low + (p_high - p_low) * 0.30
    p_mid_2 = p_low + (p_high - p_low) * 0.55
    p_mid_3 = p_low + (p_high - p_low) * 0.78

    month_starts = _month_start_day_numbers(year_sel)
    label_expr = _month_label_expr(year_sel)

    x_enc = alt.X(
        "day_of_year:O",
        title="Month",
        sort=list(range(1, int(plot["day_of_year"].max()) + 1)),
        axis=alt.Axis(
            values=month_starts,
            labelExpr=label_expr,
            labelAngle=0,
            grid=False,
            ticks=True,
            domain=True,
        ),
    )
    y_enc = alt.Y(
        "hour:O",
        title="Time [hour]",
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
                range=[PRICE_LOW_GREEN_DARK, PRICE_LOW_GREEN, PRICE_MID_YELLOW, PRICE_MID_ORANGE, PRICE_HIGH_RED],
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
        height=455,
        title=f"OMIE hourly price heatmap | {year_sel} | 24 x 365",
    )
    return apply_common_chart_style(chart, height=455)

def build_negative_price_chart(negative_df: pd.DataFrame, mode: str):
    if negative_df.empty:
        return None

    is_negative_only = mode == "Only negative prices"
    y_title = "Cumulative negative hours" if is_negative_only else "Cumulative zero / negative hours"
    chart_title = "Cumulative negative price hours" if is_negative_only else "Cumulative zero and negative price hours"
    tooltip_title = "Cumulative negative hours" if is_negative_only else "Cumulative zero / negative hours"

    years = sorted(negative_df["year"].unique().tolist())
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626", "#0EA5E9"]
    chart = alt.Chart(negative_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("month_num:O", sort=list(range(1, 13)), axis=alt.Axis(title=None, labelAngle=0, labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]")),
        y=alt.Y("cum_count:Q", title=y_title),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=colors[:len(years)])),
        detail="year:N",
        tooltip=[
            alt.Tooltip("year:N", title="Year"),
            alt.Tooltip("month_name:N", title="Month"),
            alt.Tooltip("cum_count:Q", title=tooltip_title, format=",.0f"),
        ],
    ).properties(height=330, title=chart_title)
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



# =========================================================
# EEX FORWARD MARKET HELPERS
# =========================================================
def _clean_col_name(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")


def _first_existing_col(columns: list[str], candidates: list[str]) -> str | None:
    col_set = set(columns)
    for c in candidates:
        if c in col_set:
            return c
    return None


def _parse_contract_sort_value(value):
    """Best-effort sort key for EEX delivery products: months, quarters and years."""
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT

    # Direct datetime parsing first: 2027-01-01, Jan-27, etc.
    direct = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(direct):
        return pd.Timestamp(direct).normalize()

    s_up = s.upper().replace("_", " ").replace("-", " ").replace("/", " ")

    # Quarter formats: Q1 2027, 2027 Q1, CAL 2027 Q1.
    q_match = re.search(r"Q([1-4]).*?(20\d{2})", s_up) or re.search(r"(20\d{2}).*?Q([1-4])", s_up)
    if q_match:
        g1, g2 = q_match.groups()
        if g1.startswith("20"):
            year = int(g1)
            quarter = int(g2)
        else:
            quarter = int(g1)
            year = int(g2)
        month = (quarter - 1) * 3 + 1
        return pd.Timestamp(year, month, 1)

    # Year / calendar formats: Cal-27, Calendar 2027, Year 2027, 2027 Baseload.
    y_match = re.search(r"(20\d{2})", s_up)
    if y_match and re.search(r"CAL|CALENDAR|YEAR|YR|BASE|PEAK|POWER", s_up):
        return pd.Timestamp(int(y_match.group(1)), 1, 1)

    return pd.NaT


def normalize_eex_forward_market_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a CSV/XLSX export from EEX Market Data Hub / DataSource.

    The EEX export column names can vary by product/view. This function maps common
    column names to a compact forward-curve schema used by the dashboard.
    Required output: contract + price. Optional: as_of_date, product, market_area, load_type, currency.
    """
    cols_out = [
        "as_of_date",
        "product",
        "market_area",
        "load_type",
        "contract",
        "contract_sort",
        "price",
        "currency",
        "source",
    ]
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=cols_out)

    df = raw_df.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    columns = df.columns.tolist()

    date_col = _first_existing_col(columns, [
        "as_of_date", "trading_day", "trading_date", "trade_date", "business_date",
        "date", "settlement_date", "data_date", "timestamp",
    ])
    product_col = _first_existing_col(columns, [
        "product", "product_name", "instrument", "instrument_name", "contract_name",
        "name", "eex_product", "commodity",
    ])
    market_col = _first_existing_col(columns, [
        "market_area", "market", "area", "country", "zone", "delivery_area", "hub",
    ])
    load_col = _first_existing_col(columns, [
        "load_type", "load", "profile", "base_peak", "baseload_peakload", "contract_type",
    ])
    contract_col = _first_existing_col(columns, [
        "contract", "delivery_period", "maturity", "maturity_date", "delivery", "delivery_start",
        "period", "expiry", "expiration", "contract_month", "contract_year",
    ])
    price_col = _first_existing_col(columns, [
        "settlement_price", "settlement", "settle", "settle_price", "final_settlement_price",
        "last_price", "last", "price", "close", "closing_price", "px_last",
    ])
    currency_col = _first_existing_col(columns, ["currency", "ccy"])

    if contract_col is None or price_col is None:
        return pd.DataFrame(columns=cols_out)

    out = pd.DataFrame()
    out["as_of_date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT
    out["product"] = df[product_col].astype(str).str.strip() if product_col else "EEX forward"
    out["market_area"] = df[market_col].astype(str).str.strip() if market_col else ""
    out["load_type"] = df[load_col].astype(str).str.strip() if load_col else ""
    out["contract"] = df[contract_col].astype(str).str.strip()
    out["price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["currency"] = df[currency_col].astype(str).str.strip() if currency_col else "EUR/MWh"
    out["source"] = "EEX upload/local"

    # Infer load type when it is embedded in product/contract text.
    combined_txt = (out["product"].fillna("") + " " + out["contract"].fillna("")).str.lower()
    out.loc[out["load_type"].isin(["", "nan", "None"]), "load_type"] = ""
    out.loc[out["load_type"].eq("") & combined_txt.str.contains("base|baseload", na=False), "load_type"] = "Baseload"
    out.loc[out["load_type"].eq("") & combined_txt.str.contains("peak|peakload", na=False), "load_type"] = "Peakload"
    out.loc[out["load_type"].eq(""), "load_type"] = "All"

    out["contract_sort"] = out["contract"].map(_parse_contract_sort_value)
    fallback_sort = pd.to_datetime(out["contract"], errors="coerce", dayfirst=True)
    out["contract_sort"] = out["contract_sort"].combine_first(fallback_sort)
    out = out.dropna(subset=["contract", "price"]).copy()
    out = out.sort_values(["product", "market_area", "load_type", "contract_sort", "contract"]).reset_index(drop=True)
    return out[cols_out]


def load_eex_forward_market_file(uploaded_file=None) -> pd.DataFrame:
    """Load an EEX forward curve either from an uploaded file or from /data."""
    try:
        if uploaded_file is not None:
            name = uploaded_file.name.lower()
            if name.endswith((".xlsx", ".xls")):
                raw = pd.read_excel(uploaded_file)
            else:
                raw = pd.read_csv(uploaded_file)
            return normalize_eex_forward_market_df(raw)

        if EEX_FORWARD_LOCAL_XLSX.exists():
            return normalize_eex_forward_market_df(pd.read_excel(EEX_FORWARD_LOCAL_XLSX))
        if EEX_FORWARD_LOCAL_CSV.exists():
            return normalize_eex_forward_market_df(pd.read_csv(EEX_FORWARD_LOCAL_CSV))
    except Exception:
        return pd.DataFrame(columns=["as_of_date", "product", "market_area", "load_type", "contract", "contract_sort", "price", "currency", "source"])

    return pd.DataFrame(columns=["as_of_date", "product", "market_area", "load_type", "contract", "contract_sort", "price", "currency", "source"])


def build_forward_market_chart(forward_df: pd.DataFrame):
    if forward_df.empty:
        return None
    plot = forward_df.copy()
    plot["series"] = plot[["market_area", "load_type"]].fillna("").agg(" | ".join, axis=1).str.strip(" |")
    plot.loc[plot["series"].eq(""), "series"] = plot["product"]
    plot["contract_axis"] = plot["contract"]
    order = plot.sort_values(["contract_sort", "contract"])["contract_axis"].drop_duplicates().tolist()

    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("contract_axis:N", sort=order, axis=alt.Axis(title="Delivery contract", labelAngle=-35)),
        y=alt.Y("price:Q", title="Forward price / settlement (EUR/MWh)"),
        color=alt.Color("series:N", title="Market / load"),
        tooltip=[
            alt.Tooltip("as_of_date:T", title="As of", format="%Y-%m-%d"),
            alt.Tooltip("product:N", title="Product"),
            alt.Tooltip("market_area:N", title="Market area"),
            alt.Tooltip("load_type:N", title="Load type"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("price:Q", title="Price", format=",.2f"),
            alt.Tooltip("currency:N", title="Currency"),
        ],
    ).properties(height=360, title="EEX forward curve")
    return apply_common_chart_style(chart, height=360)


def capacity_period_start(dt: pd.Series, granularity: str) -> pd.Series:
    """Return period start timestamps for installed-capacity aggregation."""
    dt = pd.to_datetime(dt, errors="coerce")
    if granularity == "Annual":
        return dt.dt.to_period("Y").dt.to_timestamp()
    if granularity == "Quarterly":
        return dt.dt.to_period("Q").dt.to_timestamp()
    return dt.dt.to_period("M").dt.to_timestamp()


def capacity_period_label(dt: pd.Series, granularity: str) -> pd.Series:
    """Readable labels for installed-capacity periods."""
    dt = pd.to_datetime(dt, errors="coerce")
    if granularity == "Annual":
        return dt.dt.strftime("%Y")
    if granularity == "Quarterly":
        q = dt.dt.quarter.astype(str)
        y = dt.dt.year.astype(str)
        return "Q" + q + "-" + y
    return dt.dt.strftime("%b-%y")


def build_installed_capacity_period_df(
    cap_df: pd.DataFrame,
    selected_techs: list[str],
    granularity: str = "Monthly",
) -> pd.DataFrame:
    """Aggregate installed capacity to the selected granularity.

    For Monthly/Quarterly/Annual, capacity is a stock variable, so we keep the
    last available value in the period for each technology rather than summing
    all observations inside the period.
    """
    cols = ["period", "technology", "capacity_mw"]
    if cap_df.empty or not selected_techs:
        return pd.DataFrame(columns=cols)

    plot = cap_df[cap_df["technology"].isin(selected_techs)].copy()
    if plot.empty:
        return pd.DataFrame(columns=cols)

    plot["datetime"] = pd.to_datetime(plot["datetime"], errors="coerce")
    plot["capacity_mw"] = pd.to_numeric(plot["capacity_mw"], errors="coerce")
    plot = plot.dropna(subset=["datetime", "technology", "capacity_mw"]).copy()
    if plot.empty:
        return pd.DataFrame(columns=cols)

    plot["period"] = capacity_period_start(plot["datetime"], granularity)
    plot = plot.sort_values(["technology", "period", "datetime"])
    out = (
        plot.groupby(["period", "technology"], as_index=False)
        .tail(1)[["period", "technology", "capacity_mw"]]
        .sort_values(["period", "technology"])
        .reset_index(drop=True)
    )
    return out


def build_installed_capacity_additions_df(
    cap_df: pd.DataFrame,
    selected_techs: list[str],
    granularity: str = "Monthly",
) -> pd.DataFrame:
    """Return cumulative MW additions versus the first selected period.

    For each technology, the baseline is the first value within the selected
    date range after applying the selected granularity.
    """
    cols = ["period", "technology", "capacity_mw", "baseline_capacity_mw", "addition_mw"]
    plot = build_installed_capacity_period_df(cap_df, selected_techs, granularity)
    if plot.empty:
        return pd.DataFrame(columns=cols)

    baselines = (
        plot.sort_values(["technology", "period"])
        .groupby("technology", as_index=False)
        .first()[["technology", "capacity_mw"]]
        .rename(columns={"capacity_mw": "baseline_capacity_mw"})
    )
    plot = plot.merge(baselines, on="technology", how="left")
    plot["addition_mw"] = plot["capacity_mw"] - plot["baseline_capacity_mw"]
    return plot[cols].sort_values(["period", "technology"]).reset_index(drop=True)



def build_installed_capacity_waterfall_df(
    cap_df: pd.DataFrame,
    selected_tech: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Annual bottom-up / waterfall for one technology.

    Additions mode is intentionally annual-only and single-technology:
    - first bar = initial installed-capacity base;
    - following bars = annual MW additions, floating from the previous total;
    - total line shows the resulting cumulative installed capacity.
    """
    cols = [
        "period", "technology", "capacity_mw", "delta_mw", "y_start", "y_end",
        "bar_low", "bar_high", "component", "delta_label", "total_label", "period_label",
    ]
    if cap_df.empty or not selected_tech:
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=["period", "period_label", "capacity_mw"])

    annual = build_installed_capacity_period_df(cap_df, [selected_tech], "Annual")
    if annual.empty:
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=["period", "period_label", "capacity_mw"])

    annual = annual.sort_values("period").reset_index(drop=True)
    annual["prev_capacity_mw"] = annual["capacity_mw"].shift(1).fillna(0.0)
    annual["delta_mw"] = annual["capacity_mw"] - annual["prev_capacity_mw"]
    annual.loc[annual.index[0], "delta_mw"] = annual.loc[annual.index[0], "capacity_mw"]
    annual["y_start"] = annual["prev_capacity_mw"]
    annual.loc[annual.index[0], "y_start"] = 0.0
    annual["y_end"] = annual["capacity_mw"]
    annual["bar_low"] = annual[["y_start", "y_end"]].min(axis=1)
    annual["bar_high"] = annual[["y_start", "y_end"]].max(axis=1)
    annual["component"] = "Initial base"
    annual.loc[annual.index[1:], "component"] = annual.loc[annual.index[1:], "delta_mw"].apply(
        lambda x: "Addition" if x >= 0 else "Reduction"
    )
    annual["period_label"] = capacity_period_label(annual["period"], "Annual")
    annual["technology"] = selected_tech

    def fmt_delta(x: float, is_first: bool) -> str:
        if is_first:
            return f"Base {x:,.0f} MW"
        sign = "+" if x >= 0 else ""
        return f"{sign}{x:,.0f} MW"

    annual["delta_label"] = [fmt_delta(v, i == 0) for i, v in enumerate(annual["delta_mw"].tolist())]
    annual["total_label"] = annual["capacity_mw"].map(lambda x: f"{x:,.0f} MW")
    totals = annual[["period", "period_label", "capacity_mw"]].copy()
    return annual[cols], totals


def build_installed_capacity_chart(
    cap_df: pd.DataFrame,
    selected_techs: list[str],
    view_mode: str = "Additions from initial base",
    granularity: str = "Monthly",
):
    """Build installed-capacity chart.

    - Additions from initial base: annual-only, single-technology bottom-up / waterfall.
    - Total installed evolution: monthly or annual capacity curves.
    """
    if view_mode == "Additions from initial base":
        selected_tech = selected_techs[0] if selected_techs else None
        plot, totals = build_installed_capacity_waterfall_df(cap_df, selected_tech)
        if plot.empty or totals.empty:
            return None

        order = totals.sort_values("period")["period_label"].tolist()
        y_max = max(float(plot["bar_high"].max()) * 1.15, 1.0)

        # In additions / waterfall mode, the first bar is not a generic component:
        # it is the initial base for the selected technology. Therefore it should
        # inherit that technology's colour, e.g. Solar PV = yellow, Wind = blue,
        # Hydro = light blue, CCGT = grey. Additions and reductions keep green/red.
        capacity_colour_map = dict(zip(CAPACITY_COLOR_DOMAIN, CAPACITY_COLOR_RANGE))
        initial_base_colour = capacity_colour_map.get(selected_tech, "#9CA3AF")
        plot = plot.copy()
        plot["component_for_colour"] = plot["component"].replace(
            {"Initial base": f"Initial base ({selected_tech})"}
        )

        bars = alt.Chart(plot).mark_bar(size=46).encode(
            x=alt.X("period_label:N", sort=order, axis=alt.Axis(title=None, labelAngle=0, labelPadding=8)),
            y=alt.Y("bar_low:Q", title="Installed capacity bridge (MW)", scale=alt.Scale(domain=[0, y_max])),
            y2="bar_high:Q",
            color=alt.Color(
                "component_for_colour:N",
                title="Component",
                scale=alt.Scale(
                    domain=[f"Initial base ({selected_tech})", "Addition", "Reduction"],
                    range=[initial_base_colour, CORP_GREEN, PRICE_HIGH_RED],
                ),
            ),
            tooltip=[
                alt.Tooltip("period_label:N", title="Year"),
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip("component:N", title="Component"),
                alt.Tooltip("delta_mw:Q", title="Annual addition MW", format=",.0f"),
                alt.Tooltip("capacity_mw:Q", title="Installed capacity MW", format=",.0f"),
            ],
        )

        # Dashed line joining the resulting capacity after each step.
        line = alt.Chart(totals).mark_line(point=True, strokeDash=[6, 4], strokeWidth=2, color="#111827").encode(
            x=alt.X("period_label:N", sort=order),
            y=alt.Y("capacity_mw:Q"),
            tooltip=[
                alt.Tooltip("period_label:N", title="Year"),
                alt.Tooltip("capacity_mw:Q", title="Installed capacity MW", format=",.0f"),
            ],
        )

        delta_labels = alt.Chart(plot).mark_text(
            align="center", baseline="middle", fontSize=12, fontWeight="bold", color="#111827"
        ).encode(
            x=alt.X("period_label:N", sort=order),
            y=alt.Y("bar_high:Q"),
            text="delta_label:N",
        )

        total_labels = alt.Chart(totals).mark_text(
            align="center", baseline="bottom", dy=-18, fontSize=12, fontWeight="bold", color="#111827"
        ).encode(
            x=alt.X("period_label:N", sort=order),
            y=alt.Y("capacity_mw:Q"),
            text=alt.Text("capacity_mw:Q", format=",.0f"),
        )

        chart_title = f"{selected_tech} installed capacity additions: initial base + annual additions"
        chart = alt.layer(bars, line, delta_labels, total_labels).properties(height=430, title=chart_title)
        return apply_common_chart_style(chart, height=430)

    # Total installed evolution: only Monthly or Annual, shown as curves by technology.
    # Do not include a total-total line here; each selected technology is shown separately.
    if granularity not in {"Monthly", "Annual"}:
        granularity = "Monthly"
    plot = build_installed_capacity_period_df(cap_df, selected_techs, granularity)
    if plot.empty:
        return None

    plot = plot.copy()
    plot["period_label"] = capacity_period_label(plot["period"], granularity)
    tech_order = [t for t in selected_techs if t in plot["technology"].unique().tolist()]

    x_axis = alt.Axis(title=None, labelAngle=-35 if granularity == "Monthly" else 0, labelPadding=8)
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=2.8).encode(
        x=alt.X("period:T", axis=x_axis),
        y=alt.Y("capacity_mw:Q", title="Installed capacity (MW)", scale=alt.Scale(zero=False)),
        color=alt.Color("technology:N", title="Technology", scale=CAPACITY_COLOR_SCALE, sort=tech_order),
        tooltip=[
            alt.Tooltip("period:T", title="Period", format="%Y-%m-%d"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("capacity_mw:Q", title="Installed capacity MW", format=",.0f"),
        ],
    ).properties(height=410, title=f"Installed capacity evolution by technology ({granularity.lower()})")
    return apply_common_chart_style(chart, height=410)

def build_price_workbook(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, monthly_combo: pd.DataFrame, negative_price_df: pd.DataFrame, mix_monthly_table: pd.DataFrame, installed_capacity: pd.DataFrame, curtailment_table: pd.DataFrame | None = None, zero_negative_hour_table: pd.DataFrame | None = None, demand_hourly: pd.DataFrame | None = None, weekly_load_df: pd.DataFrame | None = None, aemet_anomalies_df: pd.DataFrame | None = None) -> bytes:
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
        if weekly_load_df is not None and not weekly_load_df.empty:
            weekly_load_df.sort_values(["year", "week"]).to_excel(writer, index=False, sheet_name="weekly_load")
        if aemet_anomalies_df is not None and not aemet_anomalies_df.empty:
            aemet_anomalies_df.sort_values("week_start").to_excel(writer, index=False, sheet_name="aemet_anomalies")
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
        live_demand = pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"])
        live_mix = load_live_2026_mix_daily(token, live_start, live_end)
        live_installed_capacity = load_live_2026_installed_capacity_from_ree(live_start, live_end)

    price_hourly = combine_hist_and_live(hist_prices, live_prices, ["datetime"])
    solar_hourly = combine_hist_and_live(hist_solar, live_solar, ["datetime"])
    mix_daily = combine_hist_and_live(hist_mix, live_mix, ["datetime", "technology", "data_source"])
    installed_capacity = normalize_installed_capacity_df(
        combine_hist_and_live(hist_installed_capacity, live_installed_capacity, ["datetime", "technology"])
    )
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

    # Spot / captured price by selected aggregation
    section_header("Spot and solar captured price - Spain")
    capture_aggregation = st.radio(
        "Aggregation",
        options=["Daily", "Weekly", "Monthly", "Annual"],
        index=2,
        horizontal=True,
        key="capture_price_aggregation",
    )

    capture_combo = build_capture_price_table(price_hourly, solar_hourly, capture_aggregation)
    capture_chart = build_capture_price_chart(capture_combo, capture_aggregation)
    if capture_chart is not None:
        st.altair_chart(capture_chart, use_container_width=True)

    capture_table = capture_combo.copy()
    if not capture_table.empty:
        if capture_aggregation == "Daily":
            capture_table["Period"] = capture_table["period"].dt.strftime("%d-%b-%Y")
        elif capture_aggregation == "Weekly":
            capture_table["Period"] = capture_table["period"].dt.strftime("Week from %d-%b-%Y")
        elif capture_aggregation == "Monthly":
            capture_table["Period"] = capture_table["period"].dt.strftime("%b - %Y")
        else:
            capture_table["Period"] = capture_table["period"].dt.strftime("%Y")

        capture_table = capture_table.rename(columns={
            "avg_spot_price": "Average spot price",
            "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)",
            "captured_solar_price_curtailed": "Solar captured (curtailed)",
            "capture_pct_uncurtailed": "Capture rate (uncurtailed)",
            "capture_pct_curtailed": "Capture rate (curtailed)",
        })
        st.dataframe(
            styled_df(
                capture_table[["Period", "Average spot price", "Solar captured (uncurtailed)", "Solar captured (curtailed)", "Capture rate (uncurtailed)", "Capture rate (curtailed)"]],
                pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"],
            ),
            use_container_width=True,
        )

    # Keep the monthly table available for the Excel export, regardless of the chart aggregation selected above.
    monthly_combo = build_monthly_capture_table(price_hourly, solar_hourly)



    # Forward market moved to pages/2_Forward_Market.py.

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

    section_header("OMIE hourly price heatmap (24x365)")
    available_price_years = sorted(price_hourly["datetime"].dt.year.unique().tolist()) if not price_hourly.empty else []
    price_heatmap_year = st.selectbox(
        "Year for OMIE hourly price heatmap",
        available_price_years,
        index=len(available_price_years) - 1 if available_price_years else 0,
        key="price_heatmap_year_select",
    ) if available_price_years else None
    if price_heatmap_year is not None:
        price_heatmap = build_hourly_price_heatmap(price_hourly, int(price_heatmap_year))
        if price_heatmap is not None:
            st.altair_chart(price_heatmap, use_container_width=True)
            st.caption("Color scale: strong green = very low spot price; yellow/orange = medium price; strong red = very high spot price. Future or missing hours are shown in light grey.")

    section_header("Negative prices")
    neg_mode = st.radio(
        "Negative price metric",
        ["Only negative prices", "Zero and negative prices"],
        index=0,
        horizontal=True,
        help="Choose whether the cumulative curve counts only hours with price < 0, or both zero-price and negative-price hours (price <= 0).",
        key="negative_price_metric_mode",
    )
    negative_price_df = build_negative_price_curves(price_hourly, neg_mode)
    neg_chart = build_negative_price_chart(negative_price_df, neg_mode)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)
        if neg_mode == "Only negative prices":
            st.caption("This chart shows the cumulative number of hours with price below zero by month.")
        else:
            st.caption("This chart shows the cumulative number of hours with price equal to or below zero by month.")
    subtle_subsection("Cumulative negative / zero-price hours data")
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


    # Weekly load evolution section removed because REE daily load pull is currently unreliable.

    # Weather anomalies intentionally disabled for now.
    # AEMET API integration can be re-enabled once AEMET_API_KEY is available.
    aemet_anomalies_df = pd.DataFrame()

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
        if granularity == "Monthly" and year_sel >= 2026:
            demand_live_monthly = load_live_2026_demand_monthly_from_ree(date(year_sel, 1, 1), max_refresh_day())
            if not demand_live_monthly.empty:
                demand_period = demand_live_monthly.copy()
                demand_period["period_label"] = demand_period["datetime"].dt.to_period("M").dt.strftime("%b - %Y")
                demand_period["sort_key"] = demand_period["datetime"].dt.to_period("M").dt.to_timestamp()
                demand_period = demand_period[["period_label", "sort_key", "demand_mwh"]].copy()
            else:
                demand_period = build_demand_period(
                    demand_hourly if isinstance(demand_hourly, pd.DataFrame) else pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"]),
                    granularity,
                    year_sel=year_sel,
                    day_range=day_range,
                )
        else:
            demand_period = build_demand_period(
                demand_hourly if isinstance(demand_hourly, pd.DataFrame) else pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh"]),
                granularity,
                year_sel=year_sel,
                day_range=day_range,
            )
        if granularity == "Monthly" and mix_period.empty:
            st.info(f"No energy mix data available for {year_sel}. The historical mix file covers 2022-2025 and live extraction starts in 2026.")
        mix_chart = build_energy_mix_period_chart(mix_period, None)
        if mix_chart is not None:
            st.altair_chart(mix_chart, use_container_width=True)
            st.caption("Línea verde = % RE (eje derecho 0%-100%).")

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
    installed_capacity = normalize_installed_capacity_df(installed_capacity)
    if installed_capacity.empty:
        st.info("No installed capacity file found in /data.")
    else:
        cap_years = sorted(installed_capacity["datetime"].dt.year.unique().tolist())
        default_years = cap_years[-5:] if len(cap_years) >= 5 else cap_years
        # Make sure the current/live year is selected by default whenever it exists.
        current_year = max_refresh_day().year
        if current_year in cap_years and current_year not in default_years:
            default_years = sorted(default_years + [current_year])
        selected_cap_years = st.multiselect("Installed capacity years", cap_years, default=default_years)
        cap_df_year = installed_capacity[installed_capacity["datetime"].dt.year.isin(selected_cap_years)].copy()
        if current_year >= 2026 and 2026 not in cap_years:
            st.warning(
                "No 2026 installed-capacity data was returned by REE for this run. "
                "The additions bridge can show a partial 2026 year as soon as REE's monthly installed-capacity endpoint returns any 2026 month."
            )

        cap_view_mode = st.radio(
            "Installed capacity view",
            ["Additions from initial base", "Total installed evolution"],
            index=0,
            horizontal=True,
            help="Additions is annual-only and single-technology. Total installed evolution can be monthly or annual.",
        )

        available_cap_techs = sorted(cap_df_year["technology"].dropna().unique().tolist())
        if not available_cap_techs:
            st.info("No installed capacity data available for the selected years.")
        elif cap_view_mode == "Additions from initial base":
            default_add_tech = "Solar PV" if "Solar PV" in available_cap_techs else available_cap_techs[0]
            selected_add_tech = st.selectbox(
                "Technology for annual additions",
                available_cap_techs,
                index=available_cap_techs.index(default_add_tech),
                help="Additions mode is restricted to one technology so the bottom-up bridge is easy to read.",
            )
            cap_granularity = "Annual"
            st.caption("Additions mode uses annual granularity only: first bar = initial base; following bars = annual MW additions on top of the previous total. The latest year is shown even if it is partial, using the latest available monthly capacity value in that year.")
            cap_chart = build_installed_capacity_chart(cap_df_year, [selected_add_tech], cap_view_mode, cap_granularity)
            if cap_chart is not None:
                st.altair_chart(cap_chart, use_container_width=True)

            add_plot, _ = build_installed_capacity_waterfall_df(cap_df_year, selected_add_tech)
            if not add_plot.empty:
                cap_table = add_plot[["period", "technology", "capacity_mw", "delta_mw"]].copy()
                cap_table["Period"] = capacity_period_label(cap_table["period"], "Annual")
                cap_table = cap_table.rename(
                    columns={
                        "technology": "Technology",
                        "capacity_mw": "Installed capacity (MW)",
                        "delta_mw": "Annual addition (MW)",
                    }
                )
                cap_table["Cumulative additions from initial base (MW)"] = (
                    cap_table["Installed capacity (MW)"] - cap_table["Installed capacity (MW)"].iloc[0]
                )
                subtle_subsection("Annual capacity additions bridge")
                st.dataframe(
                    styled_df(
                        cap_table[[
                            "Period",
                            "Technology",
                            "Annual addition (MW)",
                            "Cumulative additions from initial base (MW)",
                            "Installed capacity (MW)",
                        ]]
                    ),
                    use_container_width=True,
                )
            else:
                st.info("No installed capacity data available for the selected technology / years.")

        else:
            default_techs = [t for t in ["Solar PV", "Wind", "Hydro", "CCGT", "Nuclear"] if t in available_cap_techs]
            selected_techs = st.multiselect(
                "Technologies",
                available_cap_techs,
                default=default_techs or available_cap_techs[:5],
                help="The chart shows each selected technology separately; no total-total line is included.",
            )
            cap_granularity = st.selectbox(
                "Installed capacity granularity",
                ["Monthly", "Annual"],
                index=0,
                help="Capacity is a stock variable: annual view keeps the last available capacity value in each year.",
            )
            cap_chart = build_installed_capacity_chart(cap_df_year, selected_techs, cap_view_mode, cap_granularity)
            if cap_chart is not None:
                st.altair_chart(cap_chart, use_container_width=True)

            cap_period = build_installed_capacity_period_df(cap_df_year, selected_techs, cap_granularity)
            if not cap_period.empty:
                cap_table = cap_period.copy().sort_values(["period", "technology"]).reset_index(drop=True)
                cap_table["Period"] = capacity_period_label(cap_table["period"], cap_granularity)
                cap_table = cap_table.rename(
                    columns={
                        "technology": "Technology",
                        "capacity_mw": "Installed capacity (MW)",
                    }
                )

                subtle_subsection(f"Installed capacity {cap_granularity.lower()} evolution by technology")
                st.dataframe(
                    styled_df(
                        cap_table[["Period", "Technology", "Installed capacity (MW)"]]
                    ),
                    use_container_width=True,
                )
            else:
                st.info("No installed capacity data available for the selected years / technologies.")

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
        weekly_load_df=weekly_load_df if "weekly_load_df" in locals() else pd.DataFrame(),
        aemet_anomalies_df=aemet_anomalies_df if "aemet_anomalies_df" in locals() else pd.DataFrame(),
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
