from __future__ import annotations

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

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Historical base files in repo
PRICE_BASE_CANDIDATES = [
    DATA_DIR / "hourly_avg_price_since2021.xlsx",
    *sorted(DATA_DIR.glob("*price*since2021*.xlsx")),
]
MIX_BASE_CANDIDATES = [
    DATA_DIR / "generation_mix_daily_2021_2025.xlsx",
    *sorted(DATA_DIR.glob("*generation*mix*daily*.xlsx")),
    *sorted(DATA_DIR.glob("Estructura de la generación*.xlsx")),
]
INSTALLED_BASE_CANDIDATES = [
    DATA_DIR / "installed_capacity_monthly.xlsx",
    *sorted(DATA_DIR.glob("*installed*capacity*.xlsx")),
    *sorted(DATA_DIR.glob("Potencia instalada*.xlsx")),
]
P48_BASE_CANDIDATES = [
    DATA_DIR / "p48solar_since21.csv",
    *sorted(DATA_DIR.glob("*p48*solar*.csv")),
]

# 2026 incremental cache files
PRICE_2026_CSV = DATA_DIR / "day_ahead_price_2026_refresh.csv"
SOLAR_P48_2026_CSV = DATA_DIR / "solar_p48_2026_refresh.csv"
SOLAR_FORECAST_2026_CSV = DATA_DIR / "solar_forecast_2026_refresh.csv"
DEMAND_2026_CSV = DATA_DIR / "demand_2026_refresh.csv"
MIX_2026_CSV = DATA_DIR / "generation_mix_daily_2026_refresh.csv"
INSTALLED_2026_CSV = DATA_DIR / "installed_capacity_2026_refresh.csv"

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027

MADRID_TZ = ZoneInfo("Europe/Madrid")
PENINSULAR_GEO_ID = 8741

TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
GREY_SHADE = "#F3F4F6"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
BLUE_PRICE = "#1D4ED8"

TECH_COLOR_SCALE = alt.Scale(
    domain=[
        "CCGT", "Hydro", "Nuclear", "Solar PV", "Solar thermal", "Wind",
        "CHP", "Biomass", "Biogas", "Other renewables", "Coal",
        "Fuel + Gas", "Renewable waste", "Non-renewable waste",
    ],
    range=[
        "#9CA3AF", "#60A5FA", "#C084FC", "#FACC15", "#FCA5A5", "#2563EB",
        "#F97316", "#16A34A", "#22C55E", "#14B8A6", "#6B7280", "#A16207",
        "#34D399", "#64748B",
    ],
)

RENEWABLE_TECHS = {
    "Hydro", "Wind", "Solar PV", "Solar thermal",
    "Biomass", "Biogas", "Other renewables", "Renewable waste",
}


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
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols]
    fmt = {c: "{:,.2f}" for c in numeric_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})

    return (
        df.style.format(fmt).set_table_styles(
            [
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
        )
    )


def apply_common_chart_style(chart, height: int = 360):
    chart_dict = chart.to_dict()
    styled = chart if ("vconcat" in chart_dict or "hconcat" in chart_dict or "concat" in chart_dict) else chart.properties(height=height)
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


def resolve_time_trunc(day: date) -> str:
    return "hour" if day < date(2025, 10, 1) else "quarter_hour"


def normalize_to_madrid_naive(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)



# =========================================================
# FILE HELPERS
# =========================================================
def first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def load_cache_csv(path: Path, schema: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=schema)
    df = pd.read_csv(path)
    return df if not df.empty else pd.DataFrame(columns=schema)


def save_cache_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(exist_ok=True, parents=True)
    df.to_csv(path, index=False)


def daterange(start_date: date, end_date: date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


# =========================================================
# BASE LOADERS FROM data/
# =========================================================
def load_price_base() -> pd.DataFrame:
    path = first_existing(PRICE_BASE_CANDIDATES)
    if path is None:
        return pd.DataFrame(columns=["datetime", "price"])

    df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {c.lower(): c for c in df.columns}

    if "datetime" in col_map:
        dt = pd.to_datetime(df[col_map["datetime"]], errors="coerce")
    elif "date" in col_map and "hour" in col_map:
        dt = pd.to_datetime(df[col_map["date"]], errors="coerce").dt.tz_localize(None) + pd.to_timedelta(
            pd.to_numeric(df[col_map["hour"]], errors="coerce") - 1, unit="h"
        )
    else:
        return pd.DataFrame(columns=["datetime", "price"])

    price_col = None
    for candidate in ["price", "price_eur_mwh", "hourly avg price", "average price", "value"]:
        if candidate in col_map:
            price_col = col_map[candidate]
            break

    if price_col is None:
        numeric_cols = [c for c in df.columns if c != "datetime" and pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            return pd.DataFrame(columns=["datetime", "price"])
        price_col = numeric_cols[-1]

    out = pd.DataFrame({"datetime": dt, "price": pd.to_numeric(df[price_col], errors="coerce")})
    out = out.dropna(subset=["datetime", "price"]).copy()
    out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    return out.reset_index(drop=True)


def load_p48_base() -> pd.DataFrame:
    path = first_existing(P48_BASE_CANDIDATES)
    if path is None:
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {c.lower(): c for c in df.columns}

    dt_col = next((col_map[c] for c in ["datetime", "date"] if c in col_map), None)
    val_col = next((col_map[c] for c in ["solar_best_mw", "solar_p48_mw", "value", "mw", "p48"] if c in col_map), None)
    if dt_col is None or val_col is None:
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

    out = pd.DataFrame({
        "datetime": normalize_to_madrid_naive(df[dt_col]),
        "solar_best_mw": pd.to_numeric(df[val_col], errors="coerce"),
    })
    out = out.dropna(subset=["datetime", "solar_best_mw"]).copy()
    out["solar_source"] = "P48"
    return out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


def load_mix_base_daily() -> pd.DataFrame:
    path = first_existing(MIX_BASE_CANDIDATES)
    if path is None:
        return pd.DataFrame(columns=["date", "technology", "energy_mwh", "renewable"])

    # Standard long format
    try:
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        col_map = {c.lower(): c for c in df.columns}

        date_col = next((col_map[c] for c in ["date", "datetime", "day"] if c in col_map), None)
        tech_col = next((col_map[c] for c in ["technology", "tech", "fuel"] if c in col_map), None)
        val_col = next((col_map[c] for c in ["energy_mwh", "value_mwh", "generation_mwh", "value", "mwh"] if c in col_map), None)
        ren_col = next((col_map[c] for c in ["renewable", "renewable_flag", "re"] if c in col_map), None)

        if date_col and tech_col and val_col:
            out = pd.DataFrame({
                "date": pd.to_datetime(df[date_col], errors="coerce").dt.normalize(),
                "technology": df[tech_col].astype(str).str.strip(),
                "energy_mwh": pd.to_numeric(df[val_col], errors="coerce"),
            })
            if ren_col:
                out["renewable"] = df[ren_col].astype(str).str.strip().str.lower().isin(["true", "1", "yes", "y", "renovable", "renewable"])
            else:
                out["renewable"] = out["technology"].isin(RENEWABLE_TECHS)
            out = out.dropna(subset=["date", "technology", "energy_mwh"]).copy()
            return out.sort_values(["date", "technology"]).reset_index(drop=True)
    except Exception:
        pass

    # REE exported matrix fallback
    raw = pd.read_excel(path, header=None)
    if raw.empty or raw.shape[0] < 6:
        return pd.DataFrame(columns=["date", "technology", "energy_mwh", "renewable"])

    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    first_col = body.columns[0]
    body = body.rename(columns={first_col: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()

    long = body.melt(id_vars=["technology"], var_name="date", value_name="energy_mwh")
    long["date"] = pd.to_datetime(long["date"], errors="coerce").dt.normalize()
    long["energy_mwh"] = pd.to_numeric(long["energy_mwh"], errors="coerce")
    long = long.dropna(subset=["date", "energy_mwh"]).copy()
    long = long[~long["technology"].str.contains("total", case=False, na=False)].copy()

    tech_map = {
        "Hidráulica": "Hydro", "Hidraulica": "Hydro",
        "Nuclear": "Nuclear",
        "Carbón": "Coal", "Carbon": "Coal",
        "Fuel + Gas": "Fuel + Gas",
        "Ciclo combinado": "CCGT",
        "Eólica": "Wind", "Eolica": "Wind",
        "Solar fotovoltaica": "Solar PV",
        "Solar térmica": "Solar thermal", "Solar termica": "Solar thermal",
        "Cogeneración": "CHP", "Cogeneracion": "CHP",
        "Residuos no renovables": "Non-renewable waste",
        "Residuos renovables": "Other renewables",
        "Biomasa": "Biomass",
        "Biogás": "Biogas", "Biogas": "Biogas",
    }
    long["technology"] = long["technology"].map(lambda x: tech_map.get(x, x))
    long["renewable"] = long["technology"].isin(RENEWABLE_TECHS)
    return long.sort_values(["date", "technology"]).reset_index(drop=True)


def load_installed_capacity_monthly_long() -> pd.DataFrame:
    path = first_existing(INSTALLED_BASE_CANDIDATES)
    if path is None:
        return pd.DataFrame(columns=["datetime", "technology", "mw"])

    # Standard long format
    try:
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        col_map = {c.lower(): c for c in df.columns}

        date_col = next((col_map[c] for c in ["date", "datetime", "month"] if c in col_map), None)
        tech_col = next((col_map[c] for c in ["technology", "tech", "fuel"] if c in col_map), None)
        val_col = next((col_map[c] for c in ["mw", "value_mw", "installed_mw", "value"] if c in col_map), None)
        if date_col and tech_col and val_col:
            out = pd.DataFrame({
                "datetime": normalize_to_madrid_naive(df[date_col]),
                "technology": df[tech_col].astype(str).str.strip(),
                "mw": pd.to_numeric(df[val_col], errors="coerce"),
            })
            out = out.dropna(subset=["datetime", "technology", "mw"]).copy()
            out = out[~out["technology"].str.contains("total", case=False, na=False)].copy()
            return out.sort_values(["datetime", "technology"]).reset_index(drop=True)
    except Exception:
        pass

    # Matrix fallback
    raw = pd.read_excel(path, header=None, sheet_name=0)
    if raw.empty or raw.shape[0] < 6:
        return pd.DataFrame(columns=["datetime", "technology", "mw"])

    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    first_col = body.columns[0]
    body = body.rename(columns={first_col: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()

    long = body.melt(id_vars=["technology"], var_name="datetime", value_name="mw")
    long["datetime"] = normalize_to_madrid_naive(long["datetime"])
    long["mw"] = pd.to_numeric(long["mw"], errors="coerce")
    long = long.dropna(subset=["datetime", "mw"]).copy()
    long = long[~long["technology"].str.contains("total", case=False, na=False)].copy()

    tech_map = {
        "Hidráulica": "Hydro", "Hidraulica": "Hydro",
        "Nuclear": "Nuclear",
        "Carbón": "Coal", "Carbon": "Coal",
        "Fuel + Gas": "Fuel + Gas",
        "Ciclo combinado": "CCGT",
        "Eólica": "Wind", "Eolica": "Wind",
        "Solar fotovoltaica": "Solar PV",
        "Solar térmica": "Solar thermal", "Solar termica": "Solar thermal",
        "Cogeneración": "CHP", "Cogeneracion": "CHP",
        "Residuos no renovables": "Non-renewable waste",
        "Residuos renovables": "Other renewables",
        "Biomasa": "Biomass",
        "Biogás": "Biogas", "Biogas": "Biogas",
    }
    long["technology"] = long["technology"].map(lambda x: tech_map.get(x, x))
    return long.sort_values(["datetime", "technology"]).reset_index(drop=True)


# =========================================================
# API HELPERS FOR 2026
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


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    raise ValueError("No datetime column found")


def parse_esios_indicator(raw_json: dict, filter_date: date | None = None) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value"])
    df = pd.DataFrame(values)
    df["datetime"] = parse_datetime_label(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    if filter_date is not None:
        df = df[df["datetime"].dt.date == filter_date].copy()
    return df[["datetime", "value"]].sort_values("datetime").reset_index(drop=True)


def to_hourly_mean(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name])
    out = df.copy()
    out["datetime_hour"] = out["datetime"].dt.floor("h")
    out = out.groupby("datetime_hour", as_index=False)["value"].mean().rename(
        columns={"datetime_hour": "datetime", "value": value_col_name}
    )
    return out.sort_values("datetime").reset_index(drop=True)


def latest_date_for_year(df: pd.DataFrame, dt_col: str, year: int) -> date | None:
    if df.empty or dt_col not in df.columns:
        return None
    tmp = df.copy()
    tmp[dt_col] = pd.to_datetime(tmp[dt_col], errors="coerce")
    tmp = tmp.dropna(subset=[dt_col])
    tmp = tmp[tmp[dt_col].dt.year == year]
    if tmp.empty:
        return None
    return tmp[dt_col].max().date()


def update_hourly_series_2026(base_df: pd.DataFrame, indicator_id: int, cache_path: Path, value_name: str, token: str) -> tuple[pd.DataFrame, int]:
    cache = load_cache_csv(cache_path, ["datetime", value_name])
    if not cache.empty:
        cache["datetime"] = normalize_to_madrid_naive(cache["datetime"])
        cache[value_name] = pd.to_numeric(cache[value_name], errors="coerce")
        cache = cache.dropna(subset=["datetime", value_name])

    existing = pd.concat([base_df, cache], ignore_index=True)
    existing["datetime"] = normalize_to_madrid_naive(existing["datetime"])
    existing = existing.dropna(subset=["datetime"]).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    start = latest_date_for_year(existing, "datetime", 2026)
    start = date(2026, 1, 1) if start is None else start + timedelta(days=1)
    end = max_refresh_day()

    failures = 0
    rows = []
    if start <= end:
        for d in daterange(start, end):
            try:
                raw = fetch_esios_day(indicator_id, d, token)
                day_df = parse_esios_indicator(raw, filter_date=d)
                hourly = to_hourly_mean(day_df, value_name)
                if not hourly.empty:
                    rows.append(hourly)
            except Exception:
                failures += 1

    if rows:
        new_df = pd.concat(rows, ignore_index=True)
        existing = pd.concat([existing, new_df], ignore_index=True).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")

    cache_out = existing[existing["datetime"].dt.year == 2026][["datetime", value_name]].copy()
    save_cache_csv(cache_out, cache_path)
    return existing.reset_index(drop=True), failures


def ree_generation_structure_url(start_d: date, end_d: date) -> str:
    return (
        "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
        f"?start_date={start_d.strftime('%Y-%m-%d')}T00:00"
        f"&end_date={end_d.strftime('%Y-%m-%d')}T23:59"
        f"&time_trunc=day&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}"
    )


def ree_installed_capacity_url(start_d: date, end_d: date) -> str:
    return (
        "https://apidatos.ree.es/es/datos/generacion/potencia-instalada"
        f"?start_date={start_d.strftime('%Y-%m-%d')}T00:00"
        f"&end_date={end_d.strftime('%Y-%m-%d')}T23:59"
        f"&time_trunc=month&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}"
    )


def update_mix_daily_2026(base_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    cache = load_cache_csv(MIX_2026_CSV, ["date", "technology", "energy_mwh", "renewable"])
    if not cache.empty:
        cache["date"] = pd.to_datetime(cache["date"], errors="coerce").dt.normalize()
        cache["energy_mwh"] = pd.to_numeric(cache["energy_mwh"], errors="coerce")
        cache["renewable"] = cache["renewable"].astype(str).str.strip().str.lower().isin(["true", "1", "yes", "y", "renovable", "renewable"])
        cache = cache.dropna(subset=["date", "technology", "energy_mwh"])

    existing = pd.concat([base_df, cache], ignore_index=True).sort_values(["date", "technology"]).drop_duplicates(subset=["date", "technology"], keep="last")
    start = latest_date_for_year(existing.rename(columns={"date": "datetime"}), "datetime", 2026)
    start = date(2026, 1, 1) if start is None else start + timedelta(days=1)
    end = max_refresh_day()

    failures = 0
    new_df = pd.DataFrame(columns=["date", "technology", "energy_mwh", "renewable"])
    if start <= end:
        try:
            resp = requests.get(ree_generation_structure_url(start, end), timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            rows = []
            for item in payload.get("included", []):
                attrs = item.get("attributes", {})
                tech = attrs.get("title")
                renewable = str(attrs.get("type", "")).strip().lower() == "renovable"
                for val in attrs.get("values", []):
                    rows.append({
                        "date": normalize_to_madrid_naive(pd.Series([val.get("datetime")])).iloc[0].normalize(),
                        "technology": tech,
                        "energy_mwh": pd.to_numeric(val.get("value"), errors="coerce"),
                        "renewable": renewable,
                    })
            new_df = pd.DataFrame(rows).dropna(subset=["date", "technology", "energy_mwh"])
        except Exception:
            failures = 1

    if not new_df.empty:
        existing = pd.concat([existing, new_df], ignore_index=True).sort_values(["date", "technology"]).drop_duplicates(subset=["date", "technology"], keep="last")

    cache_out = existing[existing["date"].dt.year == 2026].copy()
    save_cache_csv(cache_out, MIX_2026_CSV)
    return existing.reset_index(drop=True), failures


def update_installed_capacity_2026(base_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    cache = load_cache_csv(INSTALLED_2026_CSV, ["datetime", "technology", "mw"])
    if not cache.empty:
        cache["datetime"] = normalize_to_madrid_naive(cache["datetime"])
        cache["mw"] = pd.to_numeric(cache["mw"], errors="coerce")
        cache = cache.dropna(subset=["datetime", "technology", "mw"])

    existing = pd.concat([base_df, cache], ignore_index=True)
    existing["datetime"] = normalize_to_madrid_naive(existing["datetime"])
    existing = existing.dropna(subset=["datetime"]).sort_values(["datetime", "technology"]).drop_duplicates(subset=["datetime", "technology"], keep="last")
    start = latest_date_for_year(existing, "datetime", 2026)
    start = date(2026, 1, 1) if start is None else start + timedelta(days=1)
    end = max_refresh_day()

    failures = 0
    new_df = pd.DataFrame(columns=["datetime", "technology", "mw"])
    if start <= end:
        try:
            resp = requests.get(ree_installed_capacity_url(start, end), timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            tech_map = {
                "Hidráulica": "Hydro", "Hidraulica": "Hydro",
                "Nuclear": "Nuclear",
                "Carbón": "Coal", "Carbon": "Coal",
                "Fuel + Gas": "Fuel + Gas",
                "Ciclo combinado": "CCGT",
                "Eólica": "Wind", "Eolica": "Wind",
                "Solar fotovoltaica": "Solar PV",
                "Solar térmica": "Solar thermal", "Solar termica": "Solar thermal",
                "Cogeneración": "CHP", "Cogeneracion": "CHP",
                "Residuos no renovables": "Non-renewable waste",
                "Residuos renovables": "Other renewables",
                "Biomasa": "Biomass",
                "Biogás": "Biogas", "Biogas": "Biogas",
            }
            rows = []
            for item in payload.get("included", []):
                attrs = item.get("attributes", {})
                tech = tech_map.get(attrs.get("title"), attrs.get("title"))
                for val in attrs.get("values", []):
                    rows.append({
                        "datetime": normalize_to_madrid_naive(pd.Series([val.get("datetime")])).iloc[0],
                        "technology": tech,
                        "mw": pd.to_numeric(val.get("value"), errors="coerce"),
                    })
            new_df = pd.DataFrame(rows).dropna(subset=["datetime", "technology", "mw"])
            new_df = new_df[~new_df["technology"].str.contains("total", case=False, na=False)].copy()
        except Exception:
            failures = 1

    if not new_df.empty:
        existing = pd.concat([existing, new_df], ignore_index=True).sort_values(["datetime", "technology"]).drop_duplicates(subset=["datetime", "technology"], keep="last")

    cache_out = existing[existing["datetime"].dt.year == 2026].copy()
    save_cache_csv(cache_out, INSTALLED_2026_CSV)
    return existing.reset_index(drop=True), failures


# =========================================================
# DERIVED SERIES
# =========================================================
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

    capture_pct_unc = (captured_uncurtailed / avg_price) if (captured_uncurtailed is not None and avg_price not in [None, 0]) else None
    capture_pct_cur = (captured_curtailed / avg_price) if (captured_curtailed is not None and avg_price not in [None, 0]) else None

    return {
        "avg_price": avg_price,
        "captured_uncurtailed": captured_uncurtailed,
        "captured_curtailed": captured_curtailed,
        "capture_pct_uncurtailed": capture_pct_unc,
        "capture_pct_curtailed": capture_pct_cur,
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

    all_months = merged.groupby("month", as_index=False).agg(
        weighted_price_sum=("weighted_price", "sum"),
        solar_sum=("solar_best_mw", "sum"),
    )
    all_months["captured_solar_price_uncurtailed"] = all_months["weighted_price_sum"] / all_months["solar_sum"]

    curtailed = merged[merged["price"] > 0].copy()
    if curtailed.empty:
        curtailed_months = pd.DataFrame(columns=["month", "captured_solar_price_curtailed"])
    else:
        curtailed["weighted_price"] = curtailed["price"] * curtailed["solar_best_mw"]
        curtailed_months = curtailed.groupby("month", as_index=False).agg(
            weighted_price_sum=("weighted_price", "sum"),
            solar_sum=("solar_best_mw", "sum"),
        )
        curtailed_months["captured_solar_price_curtailed"] = curtailed_months["weighted_price_sum"] / curtailed_months["solar_sum"]
        curtailed_months = curtailed_months[["month", "captured_solar_price_curtailed"]]

    monthly_combo = monthly_avg.merge(
        all_months[["month", "captured_solar_price_uncurtailed"]], on="month", how="left"
    ).merge(
        curtailed_months, on="month", how="left"
    )

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
    years = sorted(monthly["year"].unique().tolist())
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    for y in years:
        temp = monthly[monthly["year"] == y].set_index("month_num")
        cum = 0
        max_month = int(temp.index.max()) if len(temp.index) else 0
        for m in range(1, max_month + 1):
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
    return pd.DataFrame(
        {
            "x_start": [pd.Timestamp(y, 1, 1) for y in shade_years],
            "x_end": [pd.Timestamp(y + 1, 1, 1) for y in shade_years],
            "year": [str(y) for y in shade_years],
        }
    )


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
            axis=alt.Axis(title=None, format="%b", labelAngle=0, labelPadding=8, ticks=False, domain=False, grid=False),
        )
    )

    layers = []
    if not shading.empty:
        layers.append(alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(x="x_start:T", x2="x_end:T"))

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
        year_layers.append(alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(x="x_start:T", x2="x_end:T"))
    year_layers.append(
        alt.Chart(year_df).mark_text(fontWeight="bold", dy=0, fontSize=13, color="#111827").encode(
            x=alt.X("year_mid:T", axis=alt.Axis(title=None, labels=False, ticks=False, domain=False, grid=False)),
            text="year:N",
        )
    )
    year_band = alt.layer(*year_layers).properties(height=24)

    chart = alt.vconcat(main, year_band, spacing=2).resolve_scale(x="shared")
    return apply_common_chart_style(chart, height=330)


def build_selected_day_chart(day_price: pd.DataFrame, day_solar: pd.DataFrame, metrics: dict):
    if day_price.empty:
        return None

    price_base = alt.Chart(day_price).encode(
        x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8))
    )

    price_line = price_base.mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
        y=alt.Y("price:Q", title="Price €/MWh"),
        tooltip=[
            alt.Tooltip("datetime:T", title="Time"),
            alt.Tooltip("price:Q", title="Price", format=".2f"),
        ],
    )

    left_layers = [price_line]
    rules = []
    if metrics.get("captured_curtailed") is not None:
        rules.append({"series": "Curtailed captured", "value": metrics["captured_curtailed"], "color": YELLOW_DARK, "dash": [6, 4]})
    if metrics.get("captured_uncurtailed") is not None:
        rules.append({"series": "Uncurtailed captured", "value": metrics["captured_uncurtailed"], "color": YELLOW_LIGHT, "dash": [2, 2]})
    if rules:
        rules_df = pd.DataFrame(rules)
        left_layers.append(
            alt.Chart(rules_df).mark_rule(strokeWidth=2).encode(
                y=alt.Y("value:Q"),
                color=alt.Color("series:N", title=None, scale=alt.Scale(domain=rules_df["series"].tolist(), range=rules_df["color"].tolist())),
                strokeDash=alt.StrokeDash("series:N", legend=None, scale=alt.Scale(domain=rules_df["series"].tolist(), range=rules_df["dash"].tolist())),
            )
        )

    left_chart = alt.layer(*left_layers)
    if not day_solar.empty:
        solar_chart = alt.Chart(day_solar).mark_area(opacity=0.35, color=YELLOW_LIGHT).encode(
            x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8)),
            y=alt.Y("solar_best_mw:Q", title="Solar MW", axis=alt.Axis(titlePadding=14, labelPadding=6)),
            tooltip=[
                alt.Tooltip("datetime:T", title="Time"),
                alt.Tooltip("solar_best_mw:Q", title="Solar", format=",.2f"),
                alt.Tooltip("solar_source:N", title="Solar source"),
            ],
        )
        overlay = alt.layer(left_chart, solar_chart).resolve_scale(y="independent").properties(height=360)
    else:
        overlay = left_chart.properties(height=360)

    return apply_common_chart_style(overlay, height=360)


def build_negative_price_chart(negative_df: pd.DataFrame, mode: str):
    if negative_df.empty:
        return None
    years = sorted(negative_df["year"].unique().tolist())
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626"]
    color_map = colors[:len(years)]

    chart = alt.Chart(negative_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X(
            "month_num:O",
            sort=list(range(1, 13)),
            axis=alt.Axis(
                title=None,
                labelAngle=0,
                labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]",
            ),
        ),
        y=alt.Y("cum_count:Q", title=("Cumulative # hours" if mode == "Zero and negative prices" else "Cumulative # negative hours")),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=color_map)),
        detail="year:N",
        tooltip=[
            alt.Tooltip("year:N", title="Year"),
            alt.Tooltip("month_name:N", title="Month"),
            alt.Tooltip("cum_count:Q", title="Cumulative count", format=",.0f"),
        ],
    )
    return apply_common_chart_style(chart.properties(height=330), height=330)


def build_energy_mix_period(
    mix_daily: pd.DataFrame,
    demand_hourly: pd.DataFrame,
    granularity: str,
    year_sel: int | None = None,
    month_sel: pd.Timestamp | None = None,
    day_range: tuple[date, date] | None = None,
):
    mix = mix_daily.copy()
    demand = demand_hourly.copy()

    if granularity == "Annual":
        mix["Period"] = mix["date"].dt.year.astype(str)
        mix["sort_key"] = mix["date"].dt.year
        demand["Period"] = demand["datetime"].dt.year.astype(str)
        demand["sort_key"] = demand["datetime"].dt.year

    elif granularity == "Monthly":
        mix = mix[mix["date"].dt.year == year_sel].copy()
        demand = demand[demand["datetime"].dt.year == year_sel].copy()
        mix["Period"] = mix["date"].dt.strftime("%b - %Y")
        mix["sort_key"] = mix["date"].dt.to_period("M").dt.to_timestamp()
        demand["Period"] = demand["datetime"].dt.strftime("%b - %Y")
        demand["sort_key"] = demand["datetime"].dt.to_period("M").dt.to_timestamp()

    elif granularity == "Weekly":
        month_ts = pd.Timestamp(month_sel)
        mix = mix[mix["date"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
        demand = demand[demand["datetime"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
        mix["Period"] = "W" + mix["date"].dt.isocalendar().week.astype(str)
        mix["sort_key"] = mix["date"].dt.to_period("W-MON").apply(lambda p: p.start_time)
        demand["Period"] = "W" + demand["datetime"].dt.isocalendar().week.astype(str)
        demand["sort_key"] = demand["datetime"].dt.to_period("W-MON").apply(lambda p: p.start_time)

    else:
        d0, d1 = day_range
        mix = mix[(mix["date"].dt.date >= d0) & (mix["date"].dt.date <= d1)].copy()
        demand = demand[(demand["datetime"].dt.date >= d0) & (demand["datetime"].dt.date <= d1)].copy()
        mix["Period"] = mix["date"].dt.strftime("%a %d-%b")
        mix["sort_key"] = mix["date"].dt.normalize()
        demand["Period"] = demand["datetime"].dt.strftime("%a %d-%b")
        demand["sort_key"] = demand["datetime"].dt.normalize()

    mix_period = mix.groupby(["Period", "sort_key", "technology"], as_index=False)["energy_mwh"].sum()

    # Merge hydro families
    hydro = (
        mix_period[mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro", "Hydro"])]
        .groupby(["Period", "sort_key"], as_index=False)["energy_mwh"].sum()
    )
    if not hydro.empty:
        hydro["technology"] = "Hydro"
        keep = mix_period[~mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
        mix_period = pd.concat([keep, hydro], ignore_index=True).groupby(["Period", "sort_key", "technology"], as_index=False)["energy_mwh"].sum()

    demand["energy_mwh"] = demand["demand_mw"]
    demand_period = demand.groupby(["Period", "sort_key"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})

    return mix_period, demand_period


def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.DataFrame):
    if mix_period.empty:
        return None

    mix_plot = mix_period.copy()
    mix_plot["energy_gwh"] = mix_plot["energy_mwh"] / 1000.0
    order_list = mix_plot[["Period", "sort_key"]].drop_duplicates().sort_values("sort_key")["Period"].tolist()

    layers = [
        alt.Chart(mix_plot).mark_bar().encode(
            x=alt.X("Period:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
            y=alt.Y("energy_gwh:Q", title="Generation & demand (GWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
            color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE, legend=alt.Legend(labelFontSize=14, titleFontSize=16)),
            tooltip=[
                alt.Tooltip("Period:N", title="Period"),
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip("energy_gwh:Q", title="Generation (GWh)", format=",.2f"),
            ],
        )
    ]

    if not demand_period.empty:
        demand_plot = demand_period.copy()
        demand_plot["demand_gwh"] = demand_plot["demand_mwh"] / 1000.0
        layers.append(
            alt.Chart(demand_plot).mark_line(point=True, color="#111827", strokeWidth=2.5).encode(
                x=alt.X("Period:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
                y=alt.Y("demand_gwh:Q", title="Generation & demand (GWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
                tooltip=[
                    alt.Tooltip("Period:N", title="Period"),
                    alt.Tooltip("demand_gwh:Q", title="Demand (GWh)", format=",.2f"),
                ],
            )
        )

    chart = alt.layer(*layers).properties(height=420)
    return apply_common_chart_style(chart, height=420)


def build_day_energy_mix_table(mix_daily: pd.DataFrame, selected_day: date) -> pd.DataFrame:
    tmp = mix_daily[mix_daily["date"].dt.date == selected_day].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Technology", "Generation (MWh)"])
    out = tmp.groupby("technology", as_index=False)["energy_mwh"].sum().rename(
        columns={"technology": "Technology", "energy_mwh": "Generation (MWh)"}
    )
    return out.sort_values("Technology").reset_index(drop=True)


def build_renewable_share_period(mix_period: pd.DataFrame) -> pd.DataFrame:
    if mix_period.empty:
        return pd.DataFrame(columns=["Period", "renewable_pct", "Renewable generation (MWh)", "Total generation (MWh)"])
    tmp = mix_period.copy()
    tmp["is_renewable"] = tmp["technology"].isin(RENEWABLE_TECHS)
    grouped = tmp.groupby(["Period", "sort_key"], as_index=False).agg(
        total_mwh=("energy_mwh", "sum"),
        renewable_mwh=("energy_mwh", lambda s: s[tmp.loc[s.index, "is_renewable"]].sum()),
    )
    grouped["renewable_pct"] = grouped["renewable_mwh"] / grouped["total_mwh"]
    grouped = grouped.rename(
        columns={
            "total_mwh": "Total generation (MWh)",
            "renewable_mwh": "Renewable generation (MWh)",
        }
    )
    return grouped.sort_values("sort_key").reset_index(drop=True)


def build_renewable_share_chart(re_df: pd.DataFrame):
    if re_df.empty:
        return None
    chart = alt.Chart(re_df).mark_line(point=True, strokeWidth=3, color=CORP_GREEN_DARK).encode(
        x=alt.X("Period:N", sort=re_df["Period"].tolist(), axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
        y=alt.Y("renewable_pct:Q", title="% RE over total generation", axis=alt.Axis(format=".0%", labelFontSize=14, titleFontSize=16)),
        tooltip=[
            alt.Tooltip("Period:N", title="Period"),
            alt.Tooltip("renewable_pct:Q", title="% RE", format=".1%"),
            alt.Tooltip("Renewable generation (MWh):Q", title="Renewable gen", format=",.0f"),
            alt.Tooltip("Total generation (MWh):Q", title="Total gen", format=",.0f"),
        ],
    ).properties(height=320)
    return apply_common_chart_style(chart, height=320)


def build_installed_capacity_period(installed_long: pd.DataFrame, granularity: str, year_sel=None, month_sel=None, day_range=None):
    if installed_long.empty:
        return pd.DataFrame(columns=["Period", "Technology", "Installed GW", "sort_key"])
    tmp = installed_long.copy()

    if granularity == "Annual":
        tmp["Period"] = tmp["datetime"].dt.year.astype(str)
        tmp["sort_key"] = tmp["datetime"].dt.year
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)
    elif granularity == "Monthly":
        tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
        tmp["Period"] = tmp["datetime"].dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)
    else:
        if month_sel is not None:
            month_ts = pd.Timestamp(month_sel)
            tmp = tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
        elif day_range is not None:
            _, d1 = day_range
            tmp = tmp[tmp["datetime"].dt.date <= d1].copy()
        tmp["Period"] = tmp["datetime"].dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)

    tmp["Installed GW"] = tmp["mw"] / 1000.0
    tmp = tmp.rename(columns={"technology": "Technology"})
    return tmp[["Period", "Technology", "Installed GW", "sort_key"]].sort_values(["sort_key", "Technology"]).reset_index(drop=True)


def build_installed_capacity_chart(df: pd.DataFrame):
    if df.empty:
        return None
    order_list = df[["Period", "sort_key"]].drop_duplicates().sort_values("sort_key")["Period"].tolist()
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("Period:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
        y=alt.Y("Installed GW:Q", title="Installed capacity (GW)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
        color=alt.Color("Technology:N", title="Technology", scale=TECH_COLOR_SCALE, legend=alt.Legend(labelFontSize=14, titleFontSize=16)),
        tooltip=[
            alt.Tooltip("Period:N", title="Period"),
            alt.Tooltip("Technology:N", title="Technology"),
            alt.Tooltip("Installed GW:Q", title="Installed GW", format=",.2f"),
        ],
    ).properties(height=380)
    return apply_common_chart_style(chart, height=380)


def build_price_workbook(
    price_hourly: pd.DataFrame,
    solar_hourly: pd.DataFrame,
    demand_hourly: pd.DataFrame,
    monthly_combo: pd.DataFrame,
    negative_price_df: pd.DataFrame,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_hourly_avg")
        solar_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="solar_hourly_best")
        demand_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="demand_hourly")
        monthly_combo.sort_values("month").to_excel(writer, index=False, sheet_name="monthly_capture")
        negative_price_df.to_excel(writer, index=False, sheet_name="negative_prices")
    output.seek(0)
    return output.getvalue()


def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFrame, re_share_df: pd.DataFrame, installed_period: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        wrote = False
        if not mix_period.empty:
            mix_period.to_excel(writer, index=False, sheet_name="mix_period")
            wrote = True
        if not demand_period.empty:
            demand_period.to_excel(writer, index=False, sheet_name="demand_period")
            wrote = True
        if not re_share_df.empty:
            re_share_df.to_excel(writer, index=False, sheet_name="renewable_share")
            wrote = True
        if not installed_period.empty:
            installed_period.to_excel(writer, index=False, sheet_name="installed_capacity")
            wrote = True
        if not wrote:
            pd.DataFrame({"info": ["No energy mix data available"]}).to_excel(writer, index=False, sheet_name="info")
    output.seek(0)
    return output.getvalue()


# =========================================================
# MAIN
# =========================================================
try:
    token = require_esios_token()

    st.caption(
        f"Madrid time now: {now_madrid().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Tomorrow available: {'Yes' if allow_next_day_refresh() else 'No'} | "
        f"Base history from /data through 2025 | "
        f"Only 2026 is refreshed online"
    )

    # Top controls
    top_left, top_right = st.columns([1.8, 1.2])
    with top_left:
        start_day = st.date_input(
            "Extraction start date",
            value=date(2021, 1, 1),
            min_value=date(2020, 1, 1),
            max_value=max_refresh_day(),
        )
    with top_right:
        btn1, btn2 = st.columns(2)
        with btn1:
            st.write("")
            st.write("")
            rebuild_2026 = st.button("Rebuild 2026 online cache")
        with btn2:
            st.write("")
            st.write("")
            refresh_energy_mix = st.button("Refresh energy mix / capacity")

    if rebuild_2026:
        for p in [PRICE_2026_CSV, SOLAR_P48_2026_CSV, SOLAR_FORECAST_2026_CSV, DEMAND_2026_CSV, MIX_2026_CSV, INSTALLED_2026_CSV]:
            if p.exists():
                p.unlink()
        st.success("2026 cache deleted. Reloading...")
        st.rerun()

    # Load historical base
    price_base = load_price_base()
    p48_base = load_p48_base()
    mix_base = load_mix_base_daily()
    installed_base = load_installed_capacity_monthly_long()

    # Refresh only 2026 online
    with st.spinner("Refreshing 2026 prices..."):
        price_hourly, price_failures = update_hourly_series_2026(price_base, PRICE_INDICATOR_ID, PRICE_2026_CSV, "price", token)

    with st.spinner("Refreshing 2026 solar P48..."):
        solar_p48_hourly, solar_p48_failures = update_hourly_series_2026(
            p48_base.rename(columns={"solar_best_mw": "solar_p48_mw"})[["datetime", "solar_p48_mw"]] if not p48_base.empty else pd.DataFrame(columns=["datetime", "solar_p48_mw"]),
            SOLAR_P48_INDICATOR_ID,
            SOLAR_P48_2026_CSV,
            "solar_p48_mw",
            token,
        )

    with st.spinner("Refreshing 2026 solar forecast..."):
        solar_forecast_hourly, solar_fc_failures = update_hourly_series_2026(
            pd.DataFrame(columns=["datetime", "solar_forecast_mw"]),
            SOLAR_FORECAST_INDICATOR_ID,
            SOLAR_FORECAST_2026_CSV,
            "solar_forecast_mw",
            token,
        )

    with st.spinner("Refreshing 2026 demand..."):
        demand_hourly, demand_failures = update_hourly_series_2026(
            pd.DataFrame(columns=["datetime", "demand_mw"]),
            DEMAND_INDICATOR_ID,
            DEMAND_2026_CSV,
            "demand_mw",
            token,
        )

    if refresh_energy_mix or True:
        with st.spinner("Refreshing 2026 energy mix..."):
            mix_daily, mix_failures = update_mix_daily_2026(mix_base)
        with st.spinner("Refreshing 2026 installed capacity..."):
            installed_long, installed_failures = update_installed_capacity_2026(installed_base)
    else:
        mix_daily, mix_failures = mix_base, 0
        installed_long, installed_failures = installed_base, 0

    # Apply start date filter
    max_allowed_day = max_refresh_day()
    price_hourly = price_hourly[(price_hourly["datetime"].dt.date >= start_day) & (price_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    solar_p48_hourly = solar_p48_hourly[(solar_p48_hourly["datetime"].dt.date >= start_day) & (solar_p48_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    solar_forecast_hourly = solar_forecast_hourly[(solar_forecast_hourly["datetime"].dt.date >= start_day) & (solar_forecast_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    demand_hourly = demand_hourly[(demand_hourly["datetime"].dt.date >= start_day) & (demand_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    mix_daily = mix_daily[(mix_daily["date"].dt.date >= start_day) & (mix_daily["date"].dt.date <= max_allowed_day)].copy()

    if price_hourly.empty:
        st.error("No price data available.")
        st.stop()

    solar_hourly = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
    monthly_combo = build_monthly_capture_table(price_hourly, solar_hourly)

    # =====================================================
    # Monthly spot & solar captured
    # =====================================================
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
                monthly_table[[
                    "Month", "Average spot price", "Solar captured (uncurtailed)",
                    "Solar captured (curtailed)", "Capture rate (uncurtailed)", "Capture rate (curtailed)",
                ]],
                pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"],
            ),
            use_container_width=True,
        )

    # =====================================================
    # Selected day
    # =====================================================
    section_header("Selected day: price vs solar")
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()

    selected_day = st.date_input(
        "Select day",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        key="selected_day_overlay",
    )

    day_price = price_hourly[price_hourly["datetime"].dt.date == selected_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()

    if not day_solar.empty:
        st.caption(f"Solar source used for selected day: {', '.join(sorted(day_solar['solar_source'].dropna().unique().tolist()))}")

    day_metrics = compute_period_metrics(price_hourly, solar_hourly, selected_day, selected_day)
    selected_day_chart = build_selected_day_chart(day_price, day_solar, day_metrics)
    if selected_day_chart is not None:
        st.altair_chart(selected_day_chart, use_container_width=True)

    d1, d2, d3 = st.columns(3)
    d1.metric("Average spot price", format_metric(day_metrics.get("avg_price"), " €/MWh"))
    d2.metric("Captured solar (uncurtailed)", format_metric(day_metrics.get("captured_uncurtailed"), " €/MWh"))
    d3.metric("Captured solar (curtailed)", format_metric(day_metrics.get("captured_curtailed"), " €/MWh"))

    # =====================================================
    # Spot / captured metrics
    # =====================================================
    section_header("Spot / captured metrics")
    month_start = selected_day.replace(day=1)
    ytd_start = selected_day.replace(month=1, day=1)

    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly, month_start, selected_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly, ytd_start, selected_day)

    metric_rows = pd.DataFrame([
        {
            "Period": "Day",
            "Average spot price": day_metrics["avg_price"],
            "Captured solar (uncurtailed)": day_metrics["captured_uncurtailed"],
            "Captured solar (curtailed)": day_metrics["captured_curtailed"],
            "Capture rate (uncurtailed)": day_metrics["capture_pct_uncurtailed"],
            "Capture rate (curtailed)": day_metrics["capture_pct_curtailed"],
        },
        {
            "Period": "MTD",
            "Average spot price": mtd_metrics["avg_price"],
            "Captured solar (uncurtailed)": mtd_metrics["captured_uncurtailed"],
            "Captured solar (curtailed)": mtd_metrics["captured_curtailed"],
            "Capture rate (uncurtailed)": mtd_metrics["capture_pct_uncurtailed"],
            "Capture rate (curtailed)": mtd_metrics["capture_pct_curtailed"],
        },
        {
            "Period": "YTD",
            "Average spot price": ytd_metrics["avg_price"],
            "Captured solar (uncurtailed)": ytd_metrics["captured_uncurtailed"],
            "Captured solar (curtailed)": ytd_metrics["captured_curtailed"],
            "Capture rate (uncurtailed)": ytd_metrics["capture_pct_uncurtailed"],
            "Capture rate (curtailed)": ytd_metrics["capture_pct_curtailed"],
        },
    ])
    st.dataframe(
        styled_df(metric_rows, pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"]),
        use_container_width=True,
    )

    # =====================================================
    # Average 24h profile
    # =====================================================
    section_header("Average 24h hourly profile for selected period")
    c1, c2 = st.columns(2)
    with c1:
        start_sel = st.date_input(
            "Profile start date",
            value=max(min_date, date(max_date.year, 1, 1)),
            min_value=min_date,
            max_value=max_date,
            key="profile_start",
        )
    with c2:
        end_sel = st.date_input(
            "Profile end date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="profile_end",
        )

    if start_sel > end_sel:
        st.warning("Start date cannot be later than end date.")
    else:
        range_df = price_hourly[(price_hourly["datetime"].dt.date >= start_sel) & (price_hourly["datetime"].dt.date <= end_sel)].copy()
        profile_metrics = compute_period_metrics(price_hourly, solar_hourly, start_sel, end_sel)

        m1, m2, m3 = st.columns(3)
        m1.metric("Average price", format_metric(profile_metrics.get("avg_price"), " €/MWh"))
        m2.metric("Captured solar (uncurtailed)", format_metric(profile_metrics.get("captured_uncurtailed"), " €/MWh"))
        m3.metric("Captured solar (curtailed)", format_metric(profile_metrics.get("captured_curtailed"), " €/MWh"))

        if range_df.empty:
            st.info("No data in the selected range.")
        else:
            range_df["hour"] = range_df["datetime"].dt.hour
            hourly_profile = (
                range_df.groupby("hour", as_index=False)["price"]
                .mean()
                .rename(columns={"price": "Average price (€/MWh)"})
                .sort_values("hour")
            )
            hourly_profile["hour_label"] = hourly_profile["hour"].map(lambda x: f"{int(x):02d}:00")
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

    # =====================================================
    # Negative prices
    # =====================================================
    section_header("Negative prices")
    neg_mode = st.radio("Series to display", ["Zero and negative prices", "Only negative prices"], index=0, horizontal=True)
    negative_price_df = build_negative_price_curves(price_hourly, neg_mode)
    neg_chart = build_negative_price_chart(negative_price_df, neg_mode)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)

    subtle_subsection("Negative prices data")
    st.dataframe(styled_df(negative_price_df), use_container_width=True)

    # =====================================================
    # Energy mix
    # =====================================================
    section_header("Energy mix")

    granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Weekly", "Daily"], index=3)
    available_years = sorted(mix_daily["date"].dt.year.unique().tolist()) if not mix_daily.empty else []
    year_sel = None
    month_sel = None
    day_range = None

    if granularity == "Monthly":
        year_sel = st.selectbox("Year", available_years, index=len(available_years) - 1 if available_years else 0)
    elif granularity == "Weekly":
        monthly_options = sorted(mix_daily["date"].dt.to_period("M").dt.to_timestamp().drop_duplicates().tolist()) if not mix_daily.empty else []
        month_sel = st.selectbox("Month", monthly_options, format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"), index=len(monthly_options) - 1 if monthly_options else 0)
    elif granularity == "Daily":
        daily_min = mix_daily["date"].dt.date.min() if not mix_daily.empty else min_date
        daily_max = mix_daily["date"].dt.date.max() if not mix_daily.empty else max_date
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

    mix_period, demand_period = build_energy_mix_period(
        mix_daily=mix_daily,
        demand_hourly=demand_hourly,
        granularity=granularity,
        year_sel=year_sel,
        month_sel=month_sel,
        day_range=day_range,
    )

    mix_chart = build_energy_mix_period_chart(mix_period, demand_period)
    if mix_chart is not None:
        st.altair_chart(mix_chart, use_container_width=True)

    subtle_subsection(f"Energy mix detail for {selected_day}")
    day_mix_table = build_day_energy_mix_table(mix_daily, selected_day)
    if not day_mix_table.empty:
        st.dataframe(styled_df(day_mix_table), use_container_width=True)
    else:
        st.info("No daily energy mix data for the selected day.")

    # =====================================================
    # % RE
    # =====================================================
    section_header("% RE over total generation")
    re_share_df = build_renewable_share_period(mix_period)
    re_chart = build_renewable_share_chart(re_share_df)
    if re_chart is not None:
        st.altair_chart(re_chart, use_container_width=True)

    if not re_share_df.empty:
        show_re = re_share_df[["Period", "Renewable generation (MWh)", "Total generation (MWh)", "renewable_pct"]].rename(
            columns={"renewable_pct": "% RE"}
        )
        st.dataframe(styled_df(show_re, pct_cols=["% RE"]), use_container_width=True)

    # =====================================================
    # Installed capacity
    # =====================================================
    section_header("Installed capacity")
    installed_period = build_installed_capacity_period(
        installed_long=installed_long,
        granularity=granularity,
        year_sel=year_sel,
        month_sel=month_sel,
        day_range=day_range,
    )
    inst_chart = build_installed_capacity_chart(installed_period)
    if inst_chart is not None:
        st.altair_chart(inst_chart, use_container_width=True)
        st.dataframe(styled_df(installed_period[["Period", "Technology", "Installed GW"]]), use_container_width=True)
    else:
        st.info("No installed capacity data available.")

    # =====================================================
    # Downloads
    # =====================================================
    section_header("Downloads")
    price_xlsx = build_price_workbook(price_hourly, solar_hourly, demand_hourly, monthly_combo, negative_price_df)
    mix_xlsx = build_energy_mix_workbook(mix_period, demand_period, re_share_df, installed_period)

    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button(
            "Download Day Ahead workbook",
            data=price_xlsx,
            file_name="day_ahead_outputs.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with dl2:
        st.download_button(
            "Download Energy Mix workbook",
            data=mix_xlsx,
            file_name="day_ahead_energy_mix_outputs.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # =====================================================
    # Refresh status
    # =====================================================
    section_header("Refresh status")
    s1, s2, s3 = st.columns(3)
    s1.metric("Price / solar / demand 2026", f"P:{price_failures} | P48:{solar_p48_failures} | FC:{solar_fc_failures} | D:{demand_failures}")
    s2.metric("Energy mix 2026", f"Failures: {mix_failures}")
    s3.metric("Installed capacity 2026", f"Failures: {installed_failures}")

except Exception as e:
    st.error(f"Unexpected error: {e}")
