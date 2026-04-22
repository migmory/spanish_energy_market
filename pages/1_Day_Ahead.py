
import os
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
    html, body, [class*="css"] { font-size: 101% !important; }
    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 101% !important;
    }
    h1 { font-size: 2.0rem !important; }
    h2, h3 { font-size: 1.35rem !important; }
    div[data-testid="stMetricValue"] { font-weight: 700; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Day Ahead - Spain Spot Prices")

DATA_BASE_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "historical_data"
DATA_BASE_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

PRICE_BOOK_PATH = DATA_BASE_DIR / "hourly_avg_price_since2021.xlsx"
MIX_DAILY_PATH = DATA_BASE_DIR / "generation_mix_daily_2021_2025.xlsx"
INSTALLED_PATH = DATA_BASE_DIR / "Potencia instalada_01-01-2024_31-12-2025.xlsx"

if not INSTALLED_PATH.exists():
    alt_inst = DATA_BASE_DIR / "installed_capacity_monthly.xlsx"
    if alt_inst.exists():
        INSTALLED_PATH = alt_inst

PRICE_RAW_2026_CSV_PATH = CACHE_DIR / "_price_raw_2026.csv"
SOLAR_P48_RAW_2026_CSV_PATH = CACHE_DIR / "_solar_p48_raw_2026.csv"
SOLAR_FORECAST_RAW_2026_CSV_PATH = CACHE_DIR / "_solar_forecast_raw_2026.csv"
DEMAND_RAW_2026_CSV_PATH = CACHE_DIR / "_demand_raw_2026.csv"
MIX_DAILY_2026_CSV_PATH = CACHE_DIR / "_mix_daily_2026.csv"

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027
PENINSULAR_GEO_ID = 8741

REFRESH_DAYS_PRICES = 7
MADRID_TZ = ZoneInfo("Europe/Madrid")

TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
GREY_SHADE = "#F3F4F6"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
BLUE_PRICE = "#1D4ED8"
TEAL_ACCENT = "#0F766E"

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
    "Coal": "#6B7280",
    "Fuel + Gas": "#A16207",
    "Renewable waste": "#34D399",
    "Non-renewable waste": "#64748B",
    "Steam turbine": "#FF2D2D",
}
RENEWABLE_TECHS = {
    "Hydro", "Wind", "Solar PV", "Solar thermal", "Biomass", "Biogas",
    "Other renewables", "Renewable waste"
}


# =========================================================
# DISPLAY HELPERS
# =========================================================


def force_naive_datetime(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=True)
    try:
        return s.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    except Exception:
        try:
            return s.dt.tz_localize(None)
        except Exception:
            return pd.to_datetime(series, errors="coerce")

def ensure_datetime_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = force_naive_datetime(out[col])
    return out

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
    styled = chart if ("vconcat" in chart_dict or "hconcat" in chart_dict or "concat" in chart_dict) else chart.properties(height=height)
    return (
        styled
        .configure_view(stroke="#E5E7EB", fill="white")
        .configure_axis(
            grid=True, gridColor="#E5E7EB", domainColor="#CBD5E1", tickColor="#CBD5E1",
            labelColor="#111827", titleColor="#111827", labelFontSize=12, titleFontSize=14,
        )
        .configure_legend(orient="top", direction="horizontal", labelFontSize=12, titleFontSize=13)
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


def resolve_time_trunc(day: date) -> str:
    return "hour" if day < date(2025, 10, 1) else "quarter_hour"


# =========================================================
# GENERIC HELPERS
# =========================================================
def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def tech_map(x: str) -> str:
    x = str(x).strip()
    mapping = {
        "Hidráulica": "Hydro",
        "Hidraulica": "Hydro",
        "Nuclear": "Nuclear",
        "Carbón": "Coal",
        "Carbon": "Coal",
        "Fuel + Gas": "Fuel + Gas",
        "Ciclo combinado": "CCGT",
        "Eólica": "Wind",
        "Eolica": "Wind",
        "Solar fotovoltaica": "Solar PV",
        "Solar térmica": "Solar thermal",
        "Solar termica": "Solar thermal",
        "Cogeneración": "CHP",
        "Cogeneracion": "CHP",
        "Residuos no renovables": "Non-renewable waste",
        "Residuos renovables": "Renewable waste",
        "Biomasa": "Biomass",
        "Biogás": "Biogas",
        "Biogas": "Biogas",
        "Turbina de vapor": "Steam turbine",
        "Otras renovables": "Other renewables",
    }
    return mapping.get(x, x)


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    raise ValueError("No datetime column found")


def upsert_raw_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return existing_df.copy()
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .reset_index(drop=True)
    )
    return combined


def load_raw_history(csv_path: Path, source_name: str) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True).dt.tz_convert("Europe/Madrid").dt.tz_localize(None) if pd.to_datetime(df["datetime"], errors="coerce", utc=True).notna().any() else pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "source" not in df.columns:
        df["source"] = source_name
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None
    df = df.dropna(subset=["datetime", "value"]).copy()
    return df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")


def save_raw_history(df: pd.DataFrame, csv_path: Path) -> None:
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    df.to_csv(csv_path, index=False)


def clear_file(csv_path: Path) -> None:
    if csv_path.exists():
        csv_path.unlink()


# =========================================================
# FETCH / PARSE ONLINE 2026
# =========================================================
def fetch_esios_day(indicator_id: int, day: date, token: str) -> dict:
    start_local = pd.Timestamp(day, tz="Europe/Madrid")
    end_local = start_local + pd.Timedelta(days=1)
    params = {
        "start_date": start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date": end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "time_trunc": resolve_time_trunc(day),
    }
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    resp = requests.get(url, headers=build_headers(token), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_esios_indicator(raw_json: dict, source_name: str, filter_date: date | None = None) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df = pd.DataFrame(values)
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    # Keep the same broad logic as the working file
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

    if df["datetime"].duplicated().any():
        dup_mask = df["datetime"].duplicated(keep="first")
        df.loc[dup_mask, "datetime"] = df.loc[dup_mask, "datetime"] + pd.Timedelta(minutes=1)

    df["source"] = source_name
    return df[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")


def build_or_refresh_raw_2026(indicator_id: int, source_name: str, csv_path: Path, token: str, days_back: int = 7) -> pd.DataFrame:
    hist = load_raw_history(csv_path, source_name)
    end_day = max_refresh_day()

    if hist.empty:
        start_day = date(2026, 1, 1)
        updated = hist.copy()
        for day in daterange(start_day, end_day):
            try:
                raw = fetch_esios_day(indicator_id, day, token)
                daily = parse_esios_indicator(raw, source_name=source_name, filter_date=day)
                if not daily.empty:
                    updated = upsert_raw_data(updated, daily)
            except Exception:
                pass
        save_raw_history(updated, csv_path)
        return updated

    start_day = date.today() - timedelta(days=days_back)
    updated = hist.copy()
    for day in daterange(start_day, end_day):
        try:
            raw = fetch_esios_day(indicator_id, day, token)
            daily = parse_esios_indicator(raw, source_name=source_name, filter_date=day)
            if not daily.empty:
                updated = upsert_raw_data(updated, daily)
        except Exception:
            pass
    save_raw_history(updated, csv_path)
    return updated


# =========================================================
# BASE DATA FROM /data
# =========================================================
def load_price_book_base():
    xls = pd.ExcelFile(PRICE_BOOK_PATH)
    price_raw = pd.read_excel(xls, sheet_name="prices_raw_qh")
    price_raw["datetime"] = force_naive_datetime(price_raw["datetime"])
    price_raw["value"] = pd.to_numeric(price_raw["value"], errors="coerce")
    price_raw = price_raw.dropna(subset=["datetime", "value"])
    if "source" not in price_raw.columns:
        price_raw["source"] = "esios_600"
    if "geo_name" not in price_raw.columns:
        price_raw["geo_name"] = None
    if "geo_id" not in price_raw.columns:
        price_raw["geo_id"] = None

    solar_best = pd.read_excel(xls, sheet_name="solar_hourly_best")
    solar_best["datetime"] = force_naive_datetime(solar_best["datetime"])
    solar_best["solar_best_mw"] = pd.to_numeric(solar_best["solar_best_mw"], errors="coerce")
    solar_best = solar_best.dropna(subset=["datetime", "solar_best_mw"])
    if "solar_source" not in solar_best.columns:
        solar_best["solar_source"] = "P48"

    demand = pd.read_excel(xls, sheet_name="demand_hourly")
    demand["datetime"] = force_naive_datetime(demand["datetime"])
    demand["demand_mw"] = pd.to_numeric(demand["demand_mw"], errors="coerce")
    if "energy_mwh" in demand.columns:
        demand["energy_mwh"] = pd.to_numeric(demand["energy_mwh"], errors="coerce")
    else:
        demand["energy_mwh"] = demand["demand_mw"]
    demand = demand.dropna(subset=["datetime", "demand_mw"])

    return price_raw, solar_best, demand


def load_mix_daily_base_from_excel() -> pd.DataFrame:
    raw = pd.read_excel(MIX_DAILY_PATH, sheet_name="data", header=None)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    long = body.melt(id_vars=["technology"], var_name="sort_key", value_name="energy_gwh")
    long["sort_key"] = force_naive_datetime(long["sort_key"])
    long["energy_gwh"] = pd.to_numeric(long["energy_gwh"], errors="coerce")
    long = long.dropna(subset=["sort_key", "energy_gwh"]).copy()
    mask_total = long["technology"].str.contains("total", case=False, na=False) | long["technology"].str.contains("generación total", case=False, na=False)
    long = long[~mask_total].copy()
    long["technology"] = long["technology"].map(tech_map)
    long["period_label"] = long["sort_key"].dt.strftime("%a %d-%b")
    long["energy_gwh"] = long["energy_gwh"].astype(float)
    return long[["sort_key", "period_label", "technology", "energy_gwh"]].sort_values(["sort_key", "technology"]).reset_index(drop=True)


def parse_installed_month_label(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    for fmt in ("%m/%Y", "%m/%y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce", dayfirst=True)


def load_installed_capacity_monthly_long() -> pd.DataFrame:
    raw = pd.read_excel(INSTALLED_PATH, sheet_name="data", header=None)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    long = body.melt(id_vars=["technology"], var_name="sort_key", value_name="mw")
    long["sort_key"] = long["sort_key"].apply(parse_installed_month_label)
    long["mw"] = pd.to_numeric(long["mw"], errors="coerce")
    long = long.dropna(subset=["sort_key", "mw"]).copy()
    mask_total = long["technology"].str.contains("total", case=False, na=False) | long["technology"].str.contains("generación total", case=False, na=False)
    long = long[~mask_total].copy()
    long["Technology"] = long["technology"].map(tech_map)
    long["Period"] = long["sort_key"].dt.strftime("%b - %Y")
    long["Installed GW"] = long["mw"] / 1000.0
    return long[["Period", "Technology", "Installed GW", "sort_key"]].sort_values(["sort_key", "Technology"]).reset_index(drop=True)


# =========================================================
# MIX DAILY 2026 FROM REE
# =========================================================
def normalize_remote_datetime(series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=True)
    return s.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)


def refresh_mix_daily_2026(token: str | None = None) -> pd.DataFrame:
    # token not used but kept for symmetry/future
    cache = pd.read_csv(MIX_DAILY_2026_CSV_PATH) if MIX_DAILY_2026_CSV_PATH.exists() else pd.DataFrame()
    if not cache.empty:
        cache["sort_key"] = force_naive_datetime(cache["sort_key"])
        cache["energy_gwh"] = pd.to_numeric(cache["energy_gwh"], errors="coerce")
        cache = cache.dropna(subset=["sort_key", "technology", "energy_gwh"])

    start_day = date(2026, 1, 1)
    if not cache.empty:
        max_cached = cache["sort_key"].max()
        if pd.notna(max_cached):
            start_day = max_cached.date() + timedelta(days=1)

    end_day = max_refresh_day()
    rows = []
    if start_day <= end_day:
        url = (
            "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
            f"?start_date={start_day:%Y-%m-%d}T00:00&end_date={end_day:%Y-%m-%d}T23:59"
            f"&time_trunc=day&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}"
        )
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("included", []):
                attrs = item.get("attributes", {})
                tech = tech_map(attrs.get("title"))
                if "total" in str(tech).lower():
                    continue
                for v in attrs.get("values", []):
                    dt = normalize_remote_datetime(pd.Series([v.get("datetime")])).iloc[0]
                    val = pd.to_numeric(v.get("value"), errors="coerce")
                    if pd.isna(dt) or pd.isna(val):
                        continue
                    rows.append({
                        "sort_key": pd.Timestamp(dt).normalize(),
                        "period_label": pd.Timestamp(dt).normalize().strftime("%a %d-%b"),
                        "technology": tech,
                        "energy_gwh": float(val),
                    })
        except Exception:
            pass

    if rows:
        new = pd.DataFrame(rows)
        cache = pd.concat([cache, new], ignore_index=True)
        cache["sort_key"] = force_naive_datetime(cache["sort_key"])
        cache["energy_gwh"] = pd.to_numeric(cache["energy_gwh"], errors="coerce")
        cache = cache.dropna(subset=["sort_key", "technology", "energy_gwh"]).sort_values(["sort_key", "technology"]).drop_duplicates(["sort_key", "technology"], keep="last")
        cache.to_csv(MIX_DAILY_2026_CSV_PATH, index=False)

    if cache.empty:
        return pd.DataFrame(columns=["sort_key", "period_label", "technology", "energy_gwh"])
    cache["period_label"] = cache["sort_key"].dt.strftime("%a %d-%b")
    return cache[["sort_key", "period_label", "technology", "energy_gwh"]].sort_values(["sort_key", "technology"]).reset_index(drop=True)


# =========================================================
# TRANSFORM PRICE/SOLAR/DEMAND
# =========================================================
def infer_interval_hours(df: pd.DataFrame) -> pd.Series:
    if df.empty or "datetime" not in df.columns:
        return pd.Series(dtype=float)
    out = df.sort_values("datetime").copy()
    diffs = out["datetime"].diff().dt.total_seconds().div(3600)
    if diffs.dropna().empty:
        interval = 1.0
    else:
        median_diff = diffs.dropna().median()
        interval = 0.25 if median_diff <= 0.30 else 1.0
    return pd.Series(interval, index=df.index)


def to_hourly_mean(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])
    out = df.copy()
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
    merged["geo_name"] = merged.get("geo_name_p48", pd.Series([None] * len(merged))).combine_first(merged.get("geo_name_fc", pd.Series([None] * len(merged))))
    merged["geo_id"] = merged.get("geo_id_p48", pd.Series([None] * len(merged))).combine_first(merged.get("geo_id_fc", pd.Series([None] * len(merged))))

    out = merged[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]].copy()
    return out.sort_values("datetime").reset_index(drop=True)


# =========================================================
# METRICS / CHARTS
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
        return monthly_avg.assign(
            captured_solar_price_uncurtailed=pd.NA,
            captured_solar_price_curtailed=pd.NA,
            capture_pct_uncurtailed=pd.NA,
            capture_pct_curtailed=pd.NA,
        )
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
    years = sorted(monthly["year"].unique().tolist())
    rows = []
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    for y in years:
        temp = monthly[monthly["year"] == y].set_index("month_num")
        cum = 0
        max_month_for_year = int(temp.index.max()) if len(temp.index) else 0
        for m in range(1, max_month_for_year + 1):
            if m in temp.index:
                cum += float(temp.loc[m, "count"])
            rows.append({"year": str(y), "month_num": m, "month_name": month_names[m - 1], "cum_count": cum})
    return pd.DataFrame(rows)


def build_monthly_shading_df(monthly_combo: pd.DataFrame) -> pd.DataFrame:
    years = sorted(monthly_combo["month"].dt.year.unique().tolist()) if not monthly_combo.empty else []
    if len(years) < 2:
        return pd.DataFrame(columns=["x_start", "x_end", "year"])
    max_year = max(years)
    shade_years = list(range(max_year - 1, min(years) - 1, -2))
    return pd.DataFrame({"x_start": [pd.Timestamp(y, 1, 1) for y in shade_years], "x_end": [pd.Timestamp(y + 1, 1, 1) for y in shade_years], "year": [str(y) for y in shade_years]})


def build_monthly_main_chart(monthly_combo: pd.DataFrame):
    if monthly_combo.empty:
        return None
    plot_df = monthly_combo.copy().rename(columns={"avg_monthly_price": "Average spot price", "captured_solar_price_curtailed": "Solar captured (curtailed)", "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)"})
    long_df = plot_df.melt(id_vars=["month"], value_vars=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"], var_name="series", value_name="value").dropna(subset=["value"])
    long_df["year"] = long_df["month"].dt.year.astype(str)
    long_df["year_mid"] = pd.to_datetime(long_df["month"].dt.year.astype(str) + "-07-01")
    shading = build_monthly_shading_df(monthly_combo)
    color_scale = alt.Scale(domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"], range=[BLUE_PRICE, YELLOW_DARK, YELLOW_LIGHT])
    dash_scale = alt.Scale(domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"], range=[[1, 0], [6, 4], [2, 2]])
    base = alt.Chart(long_df).encode(x=alt.X("month:T", axis=alt.Axis(title=None, format="%b", labelAngle=0, labelPadding=8, ticks=False, domain=False, grid=False)))
    layers = []
    if not shading.empty:
        layers.append(alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(x="x_start:T", x2="x_end:T"))
    layers.append(base.mark_line(point=True, strokeWidth=3).encode(y=alt.Y("value:Q", title="€/MWh"), color=alt.Color("series:N", title=None, scale=color_scale), strokeDash=alt.StrokeDash("series:N", title=None, scale=dash_scale), tooltip=[alt.Tooltip("month:T", title="Month"), alt.Tooltip("series:N", title="Series"), alt.Tooltip("value:Q", title="€/MWh", format=",.2f")]))
    main = alt.layer(*layers).properties(height=330)
    year_df = long_df[["year", "year_mid"]].drop_duplicates().sort_values("year_mid")
    year_layers = []
    if not shading.empty:
        year_layers.append(alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(x="x_start:T", x2="x_end:T"))
    year_layers.append(alt.Chart(year_df).mark_text(fontWeight="bold", fontSize=13, color="#111827").encode(x=alt.X("year_mid:T", axis=alt.Axis(title=None, labels=False, ticks=False, domain=False, grid=False)), text="year:N"))
    year_band = alt.layer(*year_layers).properties(height=24)
    return apply_common_chart_style(alt.vconcat(main, year_band, spacing=2).resolve_scale(x="shared"), height=330)


def build_selected_day_chart(day_price: pd.DataFrame, day_solar: pd.DataFrame, metrics: dict):
    day_price = ensure_datetime_col(day_price, "datetime")
    day_solar = ensure_datetime_col(day_solar, "datetime")
    if day_price.empty:
        return None
    price_base = alt.Chart(day_price).encode(x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8)))
    price_line = price_base.mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(y=alt.Y("price:Q", title="Price €/MWh"), tooltip=[alt.Tooltip("datetime:T", title="Time"), alt.Tooltip("price:Q", title="Price", format=".2f")])
    left_layers = [price_line]
    rule_rows = []
    if metrics.get("captured_curtailed") is not None:
        rule_rows.append({"series": "Curtailed captured", "value": metrics["captured_curtailed"], "color": YELLOW_DARK, "dash": [6, 4]})
    if metrics.get("captured_uncurtailed") is not None:
        rule_rows.append({"series": "Uncurtailed captured", "value": metrics["captured_uncurtailed"], "color": YELLOW_LIGHT, "dash": [2, 2]})
    if rule_rows:
        rules = pd.DataFrame(rule_rows)
        left_layers.append(alt.Chart(rules).mark_rule(strokeWidth=2).encode(y=alt.Y("value:Q"), color=alt.Color("series:N", title=None, legend=alt.Legend(orient="top", direction="horizontal"), scale=alt.Scale(domain=rules["series"].tolist(), range=rules["color"].tolist())), strokeDash=alt.StrokeDash("series:N", legend=None, scale=alt.Scale(domain=rules["series"].tolist(), range=rules["dash"].tolist()))))
    left_chart = alt.layer(*left_layers)
    if not day_solar.empty:
        solar_chart = alt.Chart(day_solar).mark_area(opacity=0.35, color=YELLOW_LIGHT).encode(x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8)), y=alt.Y("solar_best_mw:Q", title="Solar MW", axis=alt.Axis(titlePadding=14, labelPadding=6)))
        overlay = alt.layer(left_chart, solar_chart).resolve_scale(y="independent").properties(height=360)
    else:
        overlay = left_chart.properties(height=360)
    return apply_common_chart_style(overlay, height=360)


def build_negative_price_chart(negative_df: pd.DataFrame):
    if negative_df.empty:
        return None
    years = sorted(negative_df["year"].unique().tolist())
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626"]
    chart = alt.Chart(negative_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("month_num:O", sort=list(range(1, 13)), axis=alt.Axis(title=None, labelAngle=0, labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]")),
        y=alt.Y("cum_count:Q", title="Cumulative count"),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=colors[:len(years)])),
        detail="year:N",
    )
    return apply_common_chart_style(chart.properties(height=330), height=330)


# =========================================================
# ENERGY MIX & RE / INSTALLED
# =========================================================
def build_energy_mix_period_from_daily_combined(mix_daily_all: pd.DataFrame, demand_energy: pd.DataFrame, granularity: str, year_sel=None, month_sel=None, day_range=None):
    mix_daily_all = ensure_datetime_col(mix_daily_all, "sort_key")
    demand_energy = ensure_datetime_col(demand_energy, "datetime") if not demand_energy.empty else demand_energy
    mix = mix_daily_all.copy()
    demand = demand_energy.copy()
    demand["datetime"] = force_naive_datetime(demand["datetime"])
    demand["sort_key"] = demand["datetime"].dt.normalize()
    demand["energy_gwh"] = pd.to_numeric(demand["energy_mwh"], errors="coerce") / 1000.0

    if granularity == "Annual":
        mix["Period"] = mix["sort_key"].dt.year.astype(str)
        mix["period_order"] = mix["sort_key"].dt.year
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_gwh"].sum()

        demand["Period"] = demand["sort_key"].dt.year.astype(str)
        demand["period_order"] = demand["sort_key"].dt.year
        demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_gwh"].sum().rename(columns={"energy_gwh": "demand_gwh"})

    elif granularity == "Monthly":
        mix = mix[mix["sort_key"].dt.year == year_sel].copy()
        mix["Period"] = mix["sort_key"].dt.strftime("%b - %Y")
        mix["period_order"] = mix["sort_key"].dt.to_period("M").dt.to_timestamp()
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_gwh"].sum()

        demand = demand[demand["sort_key"].dt.year == year_sel].copy()
        demand["Period"] = demand["sort_key"].dt.strftime("%b - %Y")
        demand["period_order"] = demand["sort_key"].dt.to_period("M").dt.to_timestamp()
        demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_gwh"].sum().rename(columns={"energy_gwh": "demand_gwh"})

    elif granularity == "Weekly":
        month_ts = pd.Timestamp(month_sel)
        mix = mix[mix["sort_key"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
        mix["Period"] = "W" + mix["sort_key"].dt.isocalendar().week.astype(str)
        mix["period_order"] = mix["sort_key"].dt.to_period("W-MON").apply(lambda p: p.start_time)
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_gwh"].sum()

        demand = demand[demand["sort_key"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
        demand["Period"] = "W" + demand["sort_key"].dt.isocalendar().week.astype(str)
        demand["period_order"] = demand["sort_key"].dt.to_period("W-MON").apply(lambda p: p.start_time)
        demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_gwh"].sum().rename(columns={"energy_gwh": "demand_gwh"})

    else:
        d0, d1 = day_range
        mix = mix[(mix["sort_key"].dt.date >= d0) & (mix["sort_key"].dt.date <= d1)].copy()
        mix["Period"] = mix["sort_key"].dt.strftime("%a %d-%b")
        mix["period_order"] = mix["sort_key"]
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_gwh"].sum()

        full_days = pd.date_range(d0, d1, freq="D")
        techs = sorted(mixp["technology"].dropna().astype(str).unique().tolist())
        if techs:
            scaffold = pd.MultiIndex.from_product([full_days, techs], names=["period_order", "technology"]).to_frame(index=False)
            scaffold["Period"] = pd.to_datetime(scaffold["period_order"]).dt.strftime("%a %d-%b")
            mixp = scaffold.merge(mixp, on=["Period", "period_order", "technology"], how="left")
            mixp["energy_gwh"] = pd.to_numeric(mixp["energy_gwh"], errors="coerce").fillna(0.0)

        demand = demand[(demand["sort_key"].dt.date >= d0) & (demand["sort_key"].dt.date <= d1)].copy()
        demand["Period"] = demand["sort_key"].dt.strftime("%a %d-%b")
        demand["period_order"] = demand["sort_key"]
        demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_gwh"].sum().rename(columns={"energy_gwh": "demand_gwh"})
        full_d = pd.DataFrame({"period_order": full_days})
        full_d["Period"] = full_d["period_order"].dt.strftime("%a %d-%b")
        demandp = full_d.merge(demandp, on=["Period", "period_order"], how="left")
        demandp["demand_gwh"] = pd.to_numeric(demandp["demand_gwh"], errors="coerce").fillna(0.0)

    return mixp, demandp


def build_re_share_table_from_period(mix_period: pd.DataFrame) -> pd.DataFrame:
    if mix_period.empty:
        return pd.DataFrame(columns=["Period", "period_order", "Renewable generation (GWh)", "Total generation (GWh)", "% RE"])
    tmp = mix_period.copy()
    tmp["is_renewable"] = tmp["technology"].isin(RENEWABLE_TECHS)
    total = tmp.groupby(["Period", "period_order"], as_index=False)["energy_gwh"].sum().rename(columns={"energy_gwh": "Total generation (GWh)"})
    ren = tmp[tmp["is_renewable"]].groupby(["Period", "period_order"], as_index=False)["energy_gwh"].sum().rename(columns={"energy_gwh": "Renewable generation (GWh)"})
    out = total.merge(ren, on=["Period", "period_order"], how="left")
    out["Renewable generation (GWh)"] = out["Renewable generation (GWh)"].fillna(0.0)
    out["% RE"] = out["Renewable generation (GWh)"] / out["Total generation (GWh)"]
    return out.sort_values("period_order").reset_index(drop=True)


def build_energy_mix_chart_from_period(mix_period: pd.DataFrame, demand_period: pd.DataFrame, re_share: pd.DataFrame):
    if mix_period.empty:
        return None
    order_df = mix_period[["Period", "period_order"]].drop_duplicates().sort_values("period_order")
    order_list = order_df["Period"].tolist()

    bars = alt.Chart(mix_period).mark_bar().encode(
        x=alt.X("Period:N", sort=order_list, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("sum(energy_gwh):Q", title="Generation & demand (GWh)"),
        color=alt.Color("technology:N", title="Technology", scale=alt.Scale(domain=list(TECH_COLORS.keys()), range=list(TECH_COLORS.values()))),
        tooltip=[
            alt.Tooltip("Period:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("sum(energy_gwh):Q", title="Generation (GWh)", format=",.2f"),
        ],
    )

    layers = [bars]
    if not demand_period.empty:
        layers.append(
            alt.Chart(demand_period).mark_line(point=True, color="#111827", strokeWidth=2.5).encode(
                x=alt.X("Period:N", sort=order_list, title=None),
                y=alt.Y("demand_gwh:Q", title="Generation & demand (GWh)"),
                tooltip=[alt.Tooltip("Period:N", title="Period"), alt.Tooltip("demand_gwh:Q", title="Demand (GWh)", format=",.2f")]
            )
        )

    left = alt.layer(*layers)
    if not re_share.empty:
        right = alt.Chart(re_share).mark_line(color=TEAL_ACCENT, point=alt.OverlayMarkDef(shape="diamond", size=65, filled=True)).encode(
            x=alt.X("Period:N", sort=order_list, title=None),
            y=alt.Y("% RE:Q", title="% RE", axis=alt.Axis(format=".0%"), scale=alt.Scale(domain=[0, 1])),
            tooltip=[alt.Tooltip("Period:N", title="Period"), alt.Tooltip("% RE:Q", title="% RE", format=".1%")],
        )
        return apply_common_chart_style(alt.layer(left, right).resolve_scale(y="independent").properties(height=420), height=420)
    return apply_common_chart_style(left.properties(height=420), height=420)


def build_installed_capacity_chart(installed_period: pd.DataFrame):
    if installed_period.empty:
        return None
    order = installed_period[["Period", "sort_key"]].drop_duplicates().sort_values("sort_key")["Period"].tolist()
    chart = alt.Chart(installed_period).mark_bar().encode(
        x=alt.X("Period:N", sort=order, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("Installed GW:Q", title="Installed capacity (GW)"),
        color=alt.Color("Technology:N", title="Technology", scale=alt.Scale(domain=list(TECH_COLORS.keys()), range=list(TECH_COLORS.values()))),
        tooltip=["Period", "Technology", alt.Tooltip("Installed GW:Q", format=",.2f")]
    ).properties(height=360)
    return apply_common_chart_style(chart, height=360)


# =========================================================
# MAIN
# =========================================================
try:
    token = require_esios_token()

    st.caption(
        f"Madrid time now: {now_madrid().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Tomorrow available: {'Yes' if allow_next_day_refresh() else 'No'} | "
        f"Historical base from /data through 2025 | Only 2026 is refreshed online"
    )

    top_left, top_right = st.columns([1.8, 1.2])
    with top_left:
        start_day = st.date_input("Extraction start date", value=date(2021, 1, 1), min_value=date(2021, 1, 1), max_value=max_refresh_day())
    with top_right:
        btn1, btn2 = st.columns(2)
        with btn1:
            st.write("")
            st.write("")
            rebuild_hist = st.button("Rebuild 2026 online cache")
        with btn2:
            st.write("")
            st.write("")
            refresh_energy_mix = st.button("Refresh energy mix")

    if rebuild_hist:
        for p in [PRICE_RAW_2026_CSV_PATH, SOLAR_P48_RAW_2026_CSV_PATH, SOLAR_FORECAST_RAW_2026_CSV_PATH, DEMAND_RAW_2026_CSV_PATH, MIX_DAILY_2026_CSV_PATH]:
            clear_file(p)
        st.success("2026 cache deleted. Reloading...")
        st.rerun()

    # base history from /data
    price_raw_base, solar_best_base, demand_base = load_price_book_base()

    # build raw-like 2026 append for price
    price_2026_raw = build_or_refresh_raw_2026(PRICE_INDICATOR_ID, "esios_600", PRICE_RAW_2026_CSV_PATH, token, REFRESH_DAYS_PRICES)
    price_raw = pd.concat([price_raw_base[["datetime", "value", "source", "geo_name", "geo_id"]], price_2026_raw], ignore_index=True)
    price_raw = price_raw.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last").reset_index(drop=True)
    price_hourly = to_hourly_mean(price_raw, "price")

    # base best solar split into p48/fc proxies
    solar_p48_base = solar_best_base[solar_best_base["solar_source"].astype(str).str.contains("P48", case=False, na=False)].copy()
    solar_p48_base = solar_p48_base.rename(columns={"solar_best_mw": "solar_p48_mw"})
    solar_forecast_base = solar_best_base[solar_best_base["solar_source"].astype(str).str.contains("Forecast", case=False, na=False)].copy()
    solar_forecast_base = solar_forecast_base.rename(columns={"solar_best_mw": "solar_forecast_mw"})

    solar_p48_2026_raw = build_or_refresh_raw_2026(SOLAR_P48_INDICATOR_ID, "esios_84", SOLAR_P48_RAW_2026_CSV_PATH, token, REFRESH_DAYS_PRICES)
    solar_fc_2026_raw = build_or_refresh_raw_2026(SOLAR_FORECAST_INDICATOR_ID, "esios_542", SOLAR_FORECAST_RAW_2026_CSV_PATH, token, REFRESH_DAYS_PRICES)

    solar_p48_hourly = pd.concat([
        solar_p48_base[["datetime", "solar_p48_mw", "source", "geo_name", "geo_id"]],
        to_hourly_mean(solar_p48_2026_raw, "solar_p48_mw")
    ], ignore_index=True).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)

    solar_forecast_hourly = pd.concat([
        solar_forecast_base[["datetime", "solar_forecast_mw", "source", "geo_name", "geo_id"]],
        to_hourly_mean(solar_fc_2026_raw, "solar_forecast_mw")
    ], ignore_index=True).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)

    solar_hourly = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)

    demand_2026_raw = build_or_refresh_raw_2026(DEMAND_INDICATOR_ID, "esios_10027", DEMAND_RAW_2026_CSV_PATH, token, REFRESH_DAYS_PRICES)
    demand_hourly = pd.concat([
        demand_base[["datetime", "demand_mw", "source", "geo_name", "geo_id", "energy_mwh"]],
        to_hourly_mean(demand_2026_raw, "demand_mw").assign(energy_mwh=lambda d: d["demand_mw"])
    ], ignore_index=True).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)

    max_allowed_day = max_refresh_day()
    for df, dtcol in [(price_hourly, "datetime"), (solar_p48_hourly, "datetime"), (solar_forecast_hourly, "datetime"), (solar_hourly, "datetime"), (demand_hourly, "datetime")]:
        df[dtcol] = force_naive_datetime(df[dtcol])
    price_hourly = price_hourly[(price_hourly["datetime"].dt.date >= start_day) & (price_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    solar_p48_hourly = solar_p48_hourly[(solar_p48_hourly["datetime"].dt.date >= start_day) & (solar_p48_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    solar_forecast_hourly = solar_forecast_hourly[(solar_forecast_hourly["datetime"].dt.date >= start_day) & (solar_forecast_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    solar_hourly = solar_hourly[(solar_hourly["datetime"].dt.date >= start_day) & (solar_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    demand_hourly = demand_hourly[(demand_hourly["datetime"].dt.date >= start_day) & (demand_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    demand_energy = demand_hourly.copy()

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
        st.dataframe(styled_df(monthly_table[["Month", "Average spot price", "Solar captured (uncurtailed)", "Solar captured (curtailed)", "Capture rate (uncurtailed)", "Capture rate (curtailed)"]], pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"]), use_container_width=True)

    section_header("Selected day: price vs solar")
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()
    selected_day = st.date_input("Select day", value=max_date, min_value=min_date, max_value=max_date, key="selected_day_overlay")

    day_price = price_hourly[price_hourly["datetime"].dt.date == selected_day].copy()
    today_local = now_madrid().date()
    if selected_day >= today_local:
        day_fc = solar_forecast_hourly[solar_forecast_hourly["datetime"].dt.date == selected_day].copy()
        if not day_fc.empty:
            day_solar = day_fc.rename(columns={"solar_forecast_mw": "solar_best_mw"}).copy()
            day_solar["solar_source"] = "Forecast"
        else:
            day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()
    else:
        day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()

    if not day_solar.empty and "solar_source" in day_solar.columns:
        day_source_text = ", ".join(sorted(day_solar["solar_source"].dropna().astype(str).unique().tolist()))
        st.caption(f"Solar source used for selected day: {day_source_text}")

    day_metrics = compute_period_metrics(price_hourly, day_solar if not day_solar.empty else solar_hourly, selected_day, selected_day)
    selected_day_chart = build_selected_day_chart(day_price, day_solar, day_metrics)
    if selected_day_chart is not None:
        st.altair_chart(selected_day_chart, use_container_width=True)

    d1, d2, d3 = st.columns(3)
    d1.metric("Average spot price", format_metric(day_metrics.get("avg_price"), " €/MWh"))
    d2.metric("Captured solar (uncurtailed)", format_metric(day_metrics.get("captured_uncurtailed"), " €/MWh"))
    d3.metric("Captured solar (curtailed)", format_metric(day_metrics.get("captured_curtailed"), " €/MWh"))

    section_header("Spot / captured metrics")
    month_start = selected_day.replace(day=1)
    ytd_start = selected_day.replace(month=1, day=1)
    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly, month_start, selected_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly, ytd_start, selected_day)
    metric_rows = pd.DataFrame([
        {"Period": "Day", "Average spot price": day_metrics["avg_price"], "Captured solar (uncurtailed)": day_metrics["captured_uncurtailed"], "Captured solar (curtailed)": day_metrics["captured_curtailed"], "Capture rate (uncurtailed)": day_metrics["capture_pct_uncurtailed"], "Capture rate (curtailed)": day_metrics["capture_pct_curtailed"]},
        {"Period": "MTD", "Average spot price": mtd_metrics["avg_price"], "Captured solar (uncurtailed)": mtd_metrics["captured_uncurtailed"], "Captured solar (curtailed)": mtd_metrics["captured_curtailed"], "Capture rate (uncurtailed)": mtd_metrics["capture_pct_uncurtailed"], "Capture rate (curtailed)": mtd_metrics["capture_pct_curtailed"]},
        {"Period": "YTD", "Average spot price": ytd_metrics["avg_price"], "Captured solar (uncurtailed)": ytd_metrics["captured_uncurtailed"], "Captured solar (curtailed)": ytd_metrics["captured_curtailed"], "Capture rate (uncurtailed)": ytd_metrics["capture_pct_uncurtailed"], "Capture rate (curtailed)": ytd_metrics["capture_pct_curtailed"]},
    ])
    st.dataframe(styled_df(metric_rows, pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"]), use_container_width=True)

    section_header("Average 24h hourly profile for selected period")
    c1, c2 = st.columns(2)
    with c1:
        start_sel = st.date_input("Profile start date", value=max(min_date, date(max_date.year, 1, 1)), min_value=min_date, max_value=max_date, key="profile_start")
    with c2:
        end_sel = st.date_input("Profile end date", value=max_date, min_value=min_date, max_value=max_date, key="profile_end")

    if start_sel <= end_sel:
        range_df = price_hourly[(price_hourly["datetime"].dt.date >= start_sel) & (price_hourly["datetime"].dt.date <= end_sel)].copy()
        profile_metrics = compute_period_metrics(price_hourly, solar_hourly, start_sel, end_sel)
        m1, m2, m3 = st.columns(3)
        m1.metric("Average price", format_metric(profile_metrics.get("avg_price"), " €/MWh"))
        m2.metric("Captured solar (uncurtailed)", format_metric(profile_metrics.get("captured_uncurtailed"), " €/MWh"))
        m3.metric("Captured solar (curtailed)", format_metric(profile_metrics.get("captured_curtailed"), " €/MWh"))
        if not range_df.empty:
            range_df["hour"] = range_df["datetime"].dt.hour
            hourly_profile = range_df.groupby("hour", as_index=False)["price"].mean().rename(columns={"price": "Average price (€/MWh)"}).sort_values("hour")
            hourly_profile["hour_label"] = hourly_profile["hour"].map(lambda x: f"{int(x):02d}:00")
            profile_chart = alt.Chart(hourly_profile).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
                x=alt.X("hour_label:N", sort=hourly_profile["hour_label"].tolist(), axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("Average price (€/MWh):Q", title="Average price (€/MWh)"),
                tooltip=[alt.Tooltip("hour_label:N", title="Hour"), alt.Tooltip("Average price (€/MWh):Q", title="Average price", format=",.2f")],
            )
            st.altair_chart(apply_common_chart_style(profile_chart.properties(height=320), height=320), use_container_width=True)
            st.dataframe(styled_df(hourly_profile[["hour", "Average price (€/MWh)"]]), use_container_width=True)
    else:
        st.warning("Start date cannot be later than end date.")

    section_header("Negative prices")
    neg_mode = st.radio("Series to display", ["Zero and negative prices", "Only negative prices"], index=0, horizontal=True)
    negative_price_df = build_negative_price_curves(price_hourly, neg_mode)
    neg_chart = build_negative_price_chart(negative_price_df)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)
    subtle_subsection("Negative prices data")
    st.dataframe(styled_df(negative_price_df), use_container_width=True)

    section_header("Energy mix")
    mix_daily_all = pd.concat([load_mix_daily_base_from_excel(), refresh_mix_daily_2026(token) if refresh_energy_mix or True else pd.DataFrame()], ignore_index=True)
    mix_daily_all["sort_key"] = pd.to_datetime(mix_daily_all["sort_key"], errors="coerce")
    mix_daily_all = mix_daily_all.dropna(subset=["sort_key", "technology", "energy_gwh"]).sort_values(["sort_key", "technology"]).drop_duplicates(["sort_key", "technology"], keep="last").reset_index(drop=True)

    if not mix_daily_all.empty:
        granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Weekly", "Daily"], index=3)
        available_years = sorted(mix_daily_all["sort_key"].dt.year.unique().tolist())
        year_sel = None
        month_sel = None
        day_range = None
        if granularity == "Monthly":
            year_sel = st.selectbox("Year", available_years, index=len(available_years) - 1)
        elif granularity == "Weekly":
            monthly_options = sorted(mix_daily_all["sort_key"].dt.to_period("M").dt.to_timestamp().drop_duplicates().tolist())
            month_sel = st.selectbox("Month", monthly_options, format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"), index=len(monthly_options) - 1)
        elif granularity == "Daily":
            daily_min = mix_daily_all["sort_key"].dt.date.min()
            daily_max = mix_daily_all["sort_key"].dt.date.max()
            cc1, cc2 = st.columns(2)
            with cc1:
                daily_start = st.date_input("Daily range start", value=max(daily_min, daily_max - timedelta(days=14)), min_value=daily_min, max_value=daily_max, key="mix_daily_start")
            with cc2:
                daily_end = st.date_input("Daily range end", value=daily_max, min_value=daily_min, max_value=daily_max, key="mix_daily_end")
            if daily_start > daily_end:
                st.warning("Daily range start cannot be later than daily range end.")
                st.stop()
            day_range = (daily_start, daily_end)
            st.caption(f"Showing daily periods from {daily_start} to {daily_end}")

        mix_period, demand_period = build_energy_mix_period_from_daily_combined(mix_daily_all, demand_energy, granularity=granularity, year_sel=year_sel, month_sel=month_sel, day_range=day_range)
        re_share = build_re_share_table_from_period(mix_period)
        emix_chart = build_energy_mix_chart_from_period(mix_period, demand_period, re_share)
        if emix_chart is not None:
            st.altair_chart(emix_chart, use_container_width=True)

        subtle_subsection(f"Energy mix detail for {selected_day}")
        day_key = pd.Timestamp(selected_day)
        day_mix_table = mix_daily_all[mix_daily_all["sort_key"] == day_key].groupby("technology", as_index=False)["energy_gwh"].sum().rename(columns={"technology": "Technology", "energy_gwh": "Generation (GWh)"}).sort_values("Technology")
        if not day_mix_table.empty:
            st.dataframe(styled_df(day_mix_table), use_container_width=True)
        else:
            st.info("No energy mix detail available for selected day.")

        if not mix_period.empty:
            mix_table = mix_period.rename(columns={"technology": "Technology", "energy_gwh": "Generation (GWh)"}).sort_values(["period_order", "Technology"])
            if not demand_period.empty:
                mix_table = mix_table.merge(demand_period.rename(columns={"demand_gwh": "Demand (GWh)"}), on=["Period", "period_order"], how="left")
            mix_table = mix_table.drop(columns=["period_order"], errors="ignore")
            subtle_subsection("Energy mix table")
            st.dataframe(styled_df(mix_table), use_container_width=True)

            section_header("% RE over total generation")
            if not re_share.empty:
                st.dataframe(styled_df(re_share[["Period", "Renewable generation (GWh)", "Total generation (GWh)", "% RE"]], pct_cols=["% RE"]), use_container_width=True)
    else:
        st.info("No energy mix data available yet.")

    section_header("Installed capacity")
    installed_long = load_installed_capacity_monthly_long()
    if not installed_long.empty:
        i1, i2 = st.columns(2)
        min_month = installed_long["sort_key"].min().date()
        max_month = installed_long["sort_key"].max().date()
        with i1:
            installed_start = st.date_input("Installed capacity period start", value=min_month, min_value=min_month, max_value=max_month, key="installed_start")
        with i2:
            installed_end = st.date_input("Installed capacity period end", value=max_month, min_value=min_month, max_value=max_month, key="installed_end")
        inst_period = installed_long[(installed_long["sort_key"].dt.date >= installed_start) & (installed_long["sort_key"].dt.date <= installed_end)].copy()
        inst_chart = build_installed_capacity_chart(inst_period)
        if inst_chart is not None:
            st.altair_chart(inst_chart, use_container_width=True)
        st.dataframe(styled_df(inst_period.drop(columns=["sort_key"])), use_container_width=True)
    else:
        st.info("No installed capacity data available.")

except Exception as e:
    st.error(f"Error: {e}")
