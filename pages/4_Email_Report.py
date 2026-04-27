from __future__ import annotations

import os
import base64
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import matplotlib.pyplot as plt
import pandas as pd
import pulp
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

HISTORICAL_DIR = BASE_DIR / "historical_data"
HISTORICAL_DIR.mkdir(exist_ok=True)
DATA_DIR = BASE_DIR / "data"

PRICE_RAW_CSV_PATH = HISTORICAL_DIR / "day_ahead_spain_spot_600_raw.csv"
SOLAR_P48_RAW_CSV_PATH = HISTORICAL_DIR / "solar_p48_spain_84_raw.csv"
SOLAR_FORECAST_RAW_CSV_PATH = HISTORICAL_DIR / "solar_forecast_spain_542_raw.csv"
DEMAND_RAW_CSV_PATH = HISTORICAL_DIR / "demand_p48_total_10027_raw.csv"
REE_MIX_DAILY_CSV_PATH = HISTORICAL_DIR / "ree_generation_structure_daily_peninsular.csv"

HIST_PRICES_XLSX_PATH = DATA_DIR / "hourly_avg_price_since2021.xlsx"
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

report_mode = st.selectbox("Report mode", ["1-day report", "Monthly comparison"], index=0)


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
    if SOLAR_P48_RAW_CSV_PATH.exists():
        return load_raw_history(SOLAR_P48_RAW_CSV_PATH, "esios_84")

    if not HIST_SOLAR_CSV_PATH.exists():
        return pd.DataFrame()

    df = pd.read_csv(HIST_SOLAR_CSV_PATH)
    if df.empty:
        return pd.DataFrame()

    if "solar_best_mw" in df.columns and "value" not in df.columns:
        df = df.rename(columns={"solar_best_mw": "value"})
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "historical_solar_csv"
    df["geo_name"] = None
    df["geo_id"] = None
    return df

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
def load_ree_mix_daily() -> pd.DataFrame:
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
    Devuelve valor diario de arbitrage por MWh nominal de batería.
    Está alineado con la lógica de la pestaña BESS standalone:
    - generacion = 0
    - consumo = 0
    - omie_compra = omie_venta
    - spread = beneficio neto / capacidad nominal
    """
    if hourly_df.empty or "Price (€/MWh)" not in hourly_df.columns:
        return None

    prices = pd.to_numeric(hourly_df["Price (€/MWh)"], errors="coerce").tolist()
    prices = [p for p in prices if pd.notna(p)]
    n = len(prices)
    if n == 0:
        return None

    omie_sell = prices
    omie_buy = prices
    gen = [0.0] * n
    load = [0.0] * n

    max_power = capacity_mwh * c_rate
    max_grid_flow = max([0.0] + gen + load) + max_power
    max_grid_flow = max(max_grid_flow, 1.0)

    model = pulp.LpProblem("email_report_bess_tb", pulp.LpMaximize)

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

        model += g_to_grid[t] + batt_for_sell[t] <= max_power
        model += g_to_grid[t] + batt_for_sell[t] <= max_grid_flow * is_export[t]
        model += grid_purchase[t] + grid_charge[t] <= max_grid_flow * (1 - is_export[t])

        model += g_to_grid[t] + g_to_batt[t] + g_to_self[t] == gen[t]
        model += load[t] - g_to_self[t] == batt_for_load[t] + grid_purchase[t]

        model += soc[t + 1] == (
            soc[t]
            + eta_ch * (g_to_batt[t] + grid_charge[t])
            - (1 / max(eta_dis, 1e-9)) * (batt_for_load[t] + batt_for_sell[t])
        )
        model += soc[t] <= capacity_mwh

    model += soc[n] <= capacity_mwh
    model += pulp.lpSum(g_to_batt[t] + grid_charge[t] for t in range(n)) <= capacity_mwh / max(eta_ch, 1e-9)
    model += pulp.lpSum(batt_for_load[t] + batt_for_sell[t] for t in range(n)) <= pulp.lpSum(
        g_to_batt[t] + grid_charge[t] for t in range(n)
    )

    # Igual que la pestaña BESS: beneficio neto
    model += pulp.lpSum(
        g_to_grid[t] * omie_sell[t]
        + batt_for_sell[t] * omie_sell[t]
        - grid_purchase[t] * omie_buy[t]
        - grid_charge[t] * omie_buy[t]
        for t in range(n)
    )

    solver = pulp.PULP_CBC_CMD(msg=False)
    model.solve(solver)

    if pulp.LpStatus[model.status] != "Optimal":
        return None

    revenue = sum(
        ((pulp.value(g_to_grid[t]) or 0.0) * omie_sell[t])
        + ((pulp.value(batt_for_sell[t]) or 0.0) * omie_sell[t])
        - ((pulp.value(grid_purchase[t]) or 0.0) * omie_buy[t])
        - ((pulp.value(grid_charge[t]) or 0.0) * omie_buy[t])
        for t in range(n)
    )

    if capacity_mwh <= 0:
        return None

    return revenue / capacity_mwh


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
default_report_day = min(tomorrow_allowed, max(latest_available_day, today_madrid()))

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

report_day = st.date_input(
    "Report day",
    value=default_report_day,
    min_value=price_hourly["datetime"].dt.date.min(),
    max_value=tomorrow_allowed,
)

default_subject = f"Day Ahead report - {report_day.strftime('%d-%b-%Y')}"
subject = st.text_input("Subject", value=default_subject)

intro_text = st.text_area(
    "Intro text",
    value=(
        f"Hi all,\n\n"
        f"Please find below the day-ahead update for {report_day.strftime('%d-%b-%Y')}.\n"
    ),
    height=120,
)

available_months = sorted(price_hourly["datetime"].dt.to_period("M").astype(str).unique().tolist()) if not price_hourly.empty else []
primary_month = None
compare_month = None
if report_mode == "Monthly comparison" and available_months:
    mc1, mc2 = st.columns(2)
    default_primary = "2026-04" if "2026-04" in available_months else available_months[-1]
    default_compare = "2025-04" if "2025-04" in available_months else (available_months[-2] if len(available_months) > 1 else available_months[0])
    with mc1:
        primary_month = st.selectbox("Primary month", available_months, index=available_months.index(default_primary), key="primary_month_select")
    with mc2:
        compare_month = st.selectbox("Comparison month", available_months, index=available_months.index(default_compare), key="compare_month_select")

if report_mode == "1-day report":
    if report_day > latest_available_day:
        live_price_day = fetch_live_hourly_prices_for_day(report_day, token)
        if not live_price_day.empty:
            cached = price_hourly[price_hourly["datetime"].dt.date != report_day].copy()
            price_hourly = pd.concat([cached, live_price_day], ignore_index=True).sort_values("datetime").reset_index(drop=True)

    solar_profile_day = build_solar_profile_for_report_day(
        solar_p48_hourly=solar_p48_hourly,
        solar_forecast_hourly=solar_forecast_hourly,
        report_day=report_day,
        token=token,
    )

    hourly_df, capture_price = build_daily_dataset(price_hourly, solar_profile_day, report_day)

    historical_best_solar = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
    metrics_df = make_metrics_df(
        price_hourly=price_hourly,
        historical_best_solar=historical_best_solar,
        solar_profile_day=solar_profile_day,
        report_day=report_day,
    )

    day_mix_hourly = build_all_mix_hourly_for_day(
        report_day=report_day,
        ree_daily_df=ree_daily_raw,
        solar_profile_day=solar_profile_day,
        token=token,
    )

    demand_hourly_day = build_hourly_demand_for_day(demand_raw, report_day)

    if hourly_df.empty:
        st.warning("No hourly price data available for the selected report day.")
        st.stop()

    preview_table = hourly_df[["Hour", "Price (€/MWh)", "Solar (MW)", "Solar source"]].copy()
    overlay_chart = build_overlay_chart(hourly_df)
    overlay_chart_b64 = line_area_png_base64(hourly_df) or chart_to_base64_png(overlay_chart)

    mix_preview = build_mix_matrix_table(day_mix_hourly, demand_hourly_day)
    mix_with_demand_chart = build_mix_with_demand_chart(day_mix_hourly, demand_hourly_day)
    mix_with_demand_b64 = mix_with_demand_png_base64(day_mix_hourly, demand_hourly_day) or chart_to_base64_png(mix_with_demand_chart)
else:
    historical_best_solar = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
    if primary_month is None or compare_month is None:
        st.warning("Select both primary and comparison months.")
        st.stop()
    monthly_comp_df = build_monthly_comparison_table(price_hourly, historical_best_solar, pd.Timestamp(primary_month), pd.Timestamp(compare_month))
    monthly_profile_chart = build_monthly_avg_profile_chart(price_hourly, pd.Timestamp(primary_month), pd.Timestamp(compare_month))
    monthly_mix_chart, monthly_mix_table = build_monthly_mix_compare_chart(ree_daily_raw, pd.Timestamp(primary_month), pd.Timestamp(compare_month))
    overlay_chart_b64 = chart_to_base64_png(monthly_profile_chart)
    mix_with_demand_b64 = chart_to_base64_png(monthly_mix_chart)

if report_mode == "1-day report":
    renewable_pct, negative_hours = compute_energy_mix_kpis(day_mix_hourly, hourly_df)
    tb_df = make_tb_spreads_table(hourly_df)
else:
    renewable_pct, negative_hours = None, None
    tb_df = pd.DataFrame()

if report_mode == "1-day report":
    st.subheader("Preview chart")
    if overlay_chart is not None:
        st.altair_chart(overlay_chart, use_container_width=True)

    st.subheader("Preview metrics")
    st.dataframe(metrics_df, use_container_width=True)

    st.subheader("Preview TB spreads")
    st.dataframe(tb_df, use_container_width=True)

    st.subheader("Preview hourly table")
    st.dataframe(preview_table, use_container_width=True)

    st.subheader("Preview hourly energy mix + demand")
    if not mix_preview.empty:
        st.dataframe(mix_preview, use_container_width=True)
    else:
        st.info("No hourly energy mix / demand available for selected day.")

    st.subheader("Preview hourly energy mix + demand chart")
    if mix_with_demand_chart is not None:
        st.altair_chart(mix_with_demand_chart, use_container_width=True)
    else:
        st.info("No hourly energy mix / demand chart available.")

    st.caption(
        f"Debug | mix rows: {len(day_mix_hourly)} | "
        f"mix techs: {', '.join(sorted(day_mix_hourly['technology'].unique().tolist())) if not day_mix_hourly.empty else 'none'} | "
        f"demand rows: {len(demand_hourly_day)} | "
        f"solar source(s): {', '.join(sorted(hourly_df['Solar source'].dropna().unique().tolist()))}"
    )
else:
    st.subheader("Monthly comparison metrics")
    st.dataframe(monthly_comp_df, use_container_width=True)
    st.subheader("Average hourly price profile by month")
    if monthly_profile_chart is not None:
        st.altair_chart(monthly_profile_chart, use_container_width=True)
    st.subheader("Monthly energy mix comparison")
    if monthly_mix_chart is not None:
        st.altair_chart(monthly_mix_chart, use_container_width=True)
    if not monthly_mix_table.empty:
        st.dataframe(monthly_mix_table, use_container_width=True)

if report_mode == "1-day report":
    capture_text = f"{capture_price:.2f} €/MWh" if capture_price is not None else "n/a"
    day_sources = ", ".join(sorted(hourly_df["Solar source"].dropna().unique().tolist()))
    renewable_pct, negative_hours = compute_energy_mix_kpis(day_mix_hourly, hourly_df)
    renewable_text = f"{renewable_pct:.1%}" if renewable_pct is not None else "n/a"
    tb_df = make_tb_spreads_table(hourly_df)
    tb_html = df_to_html_table(tb_df, small=True)
    metrics_html = df_to_html_table(metrics_df, pct_cols=["Solar capture rate uncurtailed (%)", "Solar capture rate curtailed (%)"])
    hourly_html = df_to_html_table(preview_table)
    mix_hourly_html = df_to_html_table(mix_preview) if not mix_preview.empty else "<p>No hourly energy mix / demand available.</p>"
    kpi_cards_html = make_kpi_cards_html(
        [
            ("Captured solar price", capture_text),
            ("Renewables share", renewable_text),
            ("Zero / negative price hours", str(negative_hours)),
            ("Solar source used", day_sources if day_sources else "n/a"),
        ]
    )
    overlay_chart_html = ""
    if overlay_chart_b64:
        overlay_chart_html = f"""
        <h3>Hourly price and solar chart</h3>
        <img src="data:image/png;base64,{overlay_chart_b64}" alt="Hourly chart" style="max-width:100%; height:auto; border:1px solid #ddd;" />
        <br><br>
        """
    mix_with_demand_chart_html = ""
    if mix_with_demand_b64:
        mix_with_demand_chart_html = f"""
        <h3>Hourly energy mix and demand chart</h3>
        <img src="data:image/png;base64,{mix_with_demand_b64}" alt="Hourly energy mix and demand chart" style="max-width:100%; height:auto; border:1px solid #ddd;" />
        <br><br>
        """
    tomorrow = today_madrid() + timedelta(days=1)
    mix_source_note = (
        "Tomorrow: solar uses next-day forecast when available; otherwise it uses the previous day's reliable profile shifted to tomorrow. Demand and technology-level mix also use previous-day hourly shapes when next-day hourly data is not reliably available, while REE daily totals are used to preserve daily technology totals."
        if report_day == tomorrow
        else "Selected day: solar uses the best available hourly series for the selected day. Technology-level mix uses ESIOS hourly shapes when available and REE daily totals with hourly shaping as fallback."
    )
    email_html = f"""
    <html><body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
      <p>{intro_text.replace(chr(10), '<br>')}</p>
      <p><strong>Selected day:</strong> {report_day.strftime('%d-%b-%Y')}<br><strong>Energy mix source rule:</strong> {mix_source_note}</p>
      {kpi_cards_html}<br><br>
      {overlay_chart_html}
      <h3>Summary metrics</h3>{metrics_html}<br>
      <h3>BESS daily spreads</h3>{tb_html}<br>
      <h3>Hourly price / solar table</h3>{hourly_html}<br>
      {mix_with_demand_chart_html}
      <h3>Hourly energy mix and demand table</h3>{mix_hourly_html}<br>
      <p>Best regards,</p>
    </body></html>
    """
else:
    monthly_metrics_html = df_to_html_table(monthly_comp_df, pct_cols=["Solar capture rate (%)"])
    monthly_mix_html = df_to_html_table(monthly_mix_table) if not monthly_mix_table.empty else "<p>No monthly mix available.</p>"
    overlay_chart_html = ""
    if overlay_chart_b64:
        overlay_chart_html = f"""
        <h3>Average hourly price profile</h3>
        <img src="data:image/png;base64,{overlay_chart_b64}" alt="Monthly average profile" style="max-width:100%; height:auto; border:1px solid #ddd;" />
        <br><br>
        """
    mix_chart_html = ""
    if mix_with_demand_b64:
        mix_chart_html = f"""
        <h3>Monthly energy mix comparison</h3>
        <img src="data:image/png;base64,{mix_with_demand_b64}" alt="Monthly mix comparison" style="max-width:100%; height:auto; border:1px solid #ddd;" />
        <br><br>
        """
    kpi_cards_html = make_kpi_cards_html([
        ("Primary month", pd.Timestamp(primary_month).strftime('%b-%Y')),
        ("Comparison month", pd.Timestamp(compare_month).strftime('%b-%Y')),
    ])
    email_html = f"""
    <html><body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
      <p>{intro_text.replace(chr(10), '<br>')}</p>
      {kpi_cards_html}<br><br>
      <h3>Monthly comparison metrics</h3>{monthly_metrics_html}<br>
      {overlay_chart_html}
      {mix_chart_html}
      <h3>Monthly energy mix table</h3>{monthly_mix_html}<br>
      <p>Best regards,</p>
    </body></html>
    """

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
    "TB spreads now use a standalone battery arbitrage LP aligned with the BESS tab logic and are reported as daily arbitrage value per 1 MWh nominal battery. "
    "Hourly mix tries to use ESIOS hourly shapes first; for tomorrow, if no reliable hourly source exists, it shifts yesterday's shape."
)
