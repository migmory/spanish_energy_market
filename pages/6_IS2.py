# is2_scada_revenues_corporate.py
# IS2 dashboard pulling generation DIRECTLY FROM SCADA, not from uploaded files.
#
# What this does:
# - Reads IS2 generation from a SCADA API configured in .env / Streamlit secrets.
# - Converts SCADA timestamps to Europe/Madrid.
# - Aggregates 15-min generation by site.
# - Calculates revenues using the same Day Ahead workbook used by the Day Ahead tab:
#       data/hourly_avg_price_since2021.xlsx
#       sheet: prices_hourly_avg
#       columns: datetime, price
# - Corporate layout for revenues.
#
# Run:
#   streamlit run is2_scada_revenues_corporate.py
#
# Required config in .env or Streamlit secrets:
#
#   SCADA_BASE_URL=https://your-scada-api-url
#   SCADA_TOKEN=your_token
#
# Optional config:
#   SCADA_ENDPOINT=/api/timeseries
#   SCADA_AUTH_HEADER=Authorization
#   SCADA_AUTH_PREFIX=Bearer
#   SCADA_SITE_FIELD=site
#   SCADA_DATETIME_FIELD=datetime
#   SCADA_VALUE_FIELD=value
#   SCADA_UNIT=kWh per 15-min
#
# Site/tag mapping option A, JSON:
#   SCADA_TAGS_JSON={"Carmona Central 36":"tag_1","Carmona Central 36.1":"tag_2"}
#
# Site/tag mapping option B, CSV file:
#   data/is2_scada_tags.csv with columns: site, tag
#
# Expected SCADA response supported shapes:
#   1) list of rows:
#      [{"datetime":"2026-05-01T00:00:00Z","site":"Carmona","value":0}, ...]
#   2) dict with "data", "values", "results", or "items" containing rows
#   3) nested values per tag/site where rows still contain timestamp + value
#
# You may need to adjust build_scada_params() if your SCADA endpoint uses different
# parameter names. The dashboard exposes the active params in Diagnostics.

from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Iterable, Optional, Any
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# =========================================================
# Config
# =========================================================
MADRID_TZ = "Europe/Madrid"
UTC_TZ = "UTC"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREY = "#4B5563"
CORP_BORDER = "#E5E7EB"
CORP_BLUE = "#1D4ED8"
CORP_ORANGE = "#D97706"
CORP_RED = "#DC2626"

BASE_DIR = Path(__file__).resolve().parents[1] if len(Path(__file__).resolve().parents) > 1 else Path.cwd()
ENV_PATH = BASE_DIR / ".env"
if load_dotenv is not None:
    load_dotenv(dotenv_path=ENV_PATH, override=True)

DATA_DIR = BASE_DIR / "data"
DAY_AHEAD_PRICE_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
SCADA_TAGS_FILE = DATA_DIR / "is2_scada_tags.csv"


# =========================================================
# Page setup
# =========================================================
st.set_page_config(
    page_title="IS2 | SCADA revenues",
    page_icon="⚡",
    layout="wide",
)

st.markdown(
    f"""
    <style>
        .main .block-container {{
            padding-top: 1.25rem;
            padding-bottom: 2rem;
            max-width: 1540px;
        }}

        h1 {{
            font-size: 2.05rem !important;
            font-weight: 850 !important;
            color: #111827 !important;
            letter-spacing: -0.02em;
        }}

        .corp-header {{
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 58%, #C7F0DD 100%);
            color: white;
            padding: 16px 22px;
            border-radius: 16px;
            font-weight: 850;
            font-size: 1.25rem;
            margin: 18px 0 14px 0;
            box-shadow: 0 3px 12px rgba(15,118,110,0.16);
        }}

        .pill {{
            display:inline-block;
            border-radius:999px;
            padding:6px 11px;
            font-size:0.85rem;
            font-weight:700;
            margin:4px 6px 12px 0;
        }}

        .pill-blue {{
            background:#EEF2FF;
            border:1px solid #C7D2FE;
            color:#3730A3;
        }}

        .pill-green {{
            background:#ECFDF5;
            border:1px solid #BBF7D0;
            color:#065F46;
        }}

        div[data-testid="metric-container"] {{
            background: white;
            border: 1px solid {CORP_BORDER};
            padding: 16px 18px;
            border-radius: 16px;
            box-shadow: 0 1px 3px rgba(15,23,42,0.05);
        }}

        div[data-testid="metric-container"] label {{
            color: #6B7280 !important;
            font-weight: 650 !important;
        }}

        div[data-testid="metric-container"] [data-testid="stMetricValue"] {{
            color: #111827 !important;
            font-weight: 850 !important;
        }}

        .ok-box {{
            background:#ECFDF5;
            color:#065F46;
            border:1px solid #BBF7D0;
            border-radius:14px;
            padding:13px 15px;
            margin:8px 0 16px 0;
        }}

        .warning-box {{
            background:#FFF8E6;
            color:#7A5200;
            border:1px solid #FFE1A6;
            border-radius:14px;
            padding:13px 15px;
            margin:8px 0 16px 0;
        }}

        .danger-box {{
            background:#FEF2F2;
            color:#991B1B;
            border:1px solid #FECACA;
            border-radius:14px;
            padding:13px 15px;
            margin:8px 0 16px 0;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# UI helpers
# =========================================================
def section_header(title: str) -> None:
    st.markdown(f'<div class="corp-header">{title}</div>', unsafe_allow_html=True)


def box(kind: str, text: str) -> None:
    cls = {"ok": "ok-box", "warning": "warning-box", "danger": "danger-box"}.get(kind, "warning-box")
    st.markdown(f'<div class="{cls}">{text}</div>', unsafe_allow_html=True)


def pill(text: str, kind: str = "blue") -> None:
    cls = "pill-green" if kind == "green" else "pill-blue"
    st.markdown(f'<span class="pill {cls}">{text}</span>', unsafe_allow_html=True)


def fmt_eur(x: float) -> str:
    return "—" if pd.isna(x) else f"€{x:,.0f}"


def fmt_mwh(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} MWh"


def fmt_price(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} €/MWh"


def norm_col(c: str) -> str:
    return (
        str(c).strip().lower()
        .replace(" ", "_").replace("-", "_").replace("/", "_")
        .replace(".", "_").replace("€", "eur")
    )


def find_col(cols: Iterable[str], candidates: list[str]) -> Optional[str]:
    mapping = {norm_col(c): c for c in cols}
    for cand in candidates:
        if norm_col(cand) in mapping:
            return mapping[norm_col(cand)]
    for c in cols:
        nc = norm_col(c)
        if any(norm_col(cand) in nc for cand in candidates):
            return c
    return None


def clean_numeric(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    txt = s.astype(str).str.strip()
    euro_mask = txt.str.contains(",", regex=False) & txt.str.contains(".", regex=False)
    txt = txt.where(~euro_mask, txt.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    txt = txt.where(euro_mask, txt.str.replace(",", ".", regex=False))
    return pd.to_numeric(txt.replace({"nan": np.nan, "None": np.nan, "": np.nan}), errors="coerce")


def chart_layout(title: str, subtitle: str = "", height: int = 470) -> dict:
    title_text = f"<b>{title}</b><br><sup>{subtitle}</sup>" if subtitle else f"<b>{title}</b>"
    return dict(
        title=dict(text=title_text, x=0.01, xanchor="left"),
        height=height,
        margin=dict(l=58, r=58, t=78, b=52),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.85)",
        ),
        font=dict(size=12, color="#111827"),
    )


def style_table(df: pd.DataFrame):
    return (
        df.style
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", CORP_GREY),
                        ("color", "white"),
                        ("font-weight", "750"),
                        ("font-size", "13px"),
                        ("text-align", "center"),
                        ("padding", "9px 8px"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("font-size", "12px"),
                        ("padding", "7px 8px"),
                    ],
                },
            ]
        )
    )


# =========================================================
# Config readers
# =========================================================
def secret_or_env(key: str, default: str = "") -> str:
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return str(os.getenv(key, default) or default)


def load_scada_tags() -> pd.DataFrame:
    raw_json = secret_or_env("SCADA_TAGS_JSON", "").strip()
    if raw_json:
        try:
            obj = json.loads(raw_json)
            if isinstance(obj, dict):
                return pd.DataFrame([{"site": k, "tag": v} for k, v in obj.items()])
            if isinstance(obj, list):
                return pd.DataFrame(obj)
        except Exception as exc:
            box("warning", f"Could not parse SCADA_TAGS_JSON: {exc}")

    if SCADA_TAGS_FILE.exists():
        try:
            return pd.read_csv(SCADA_TAGS_FILE)
        except Exception as exc:
            box("warning", f"Could not parse {SCADA_TAGS_FILE}: {exc}")

    return pd.DataFrame(columns=["site", "tag"])


# =========================================================
# Time conversion and generation
# =========================================================
def to_madrid_datetime(series: pd.Series, source_tz: str) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    if getattr(dt.dt, "tz", None) is not None:
        return dt.dt.tz_convert(MADRID_TZ)
    if source_tz == "UTC":
        return dt.dt.tz_localize(UTC_TZ, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(MADRID_TZ)
    return dt.dt.tz_localize(source_tz, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(MADRID_TZ)


def normalize_generation(values: pd.Series, unit: str) -> pd.Series:
    v = clean_numeric(values)
    if unit == "kWh per 15-min":
        return v / 1000.0
    if unit == "MWh per 15-min":
        return v
    if unit == "kW average":
        return v * 0.25 / 1000.0
    if unit == "MW average":
        return v * 0.25
    if unit == "kW average in 15-min":
        return v * 0.25 / 1000.0
    if unit == "MW average in 15-min":
        return v * 0.25
    return v


def prepare_generation(
    raw: pd.DataFrame,
    dt_col: str,
    site_col: str,
    value_col: str,
    source_tz: str,
    unit: str,
    zero_night: bool,
    night_start_hour: int,
    night_end_hour: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame()
    df["datetime_madrid"] = to_madrid_datetime(raw[dt_col], source_tz).dt.floor("15min")
    df["site"] = raw[site_col].astype(str).str.strip()
    df["generation_mwh_15min_raw"] = normalize_generation(raw[value_col], unit)

    df = df.dropna(subset=["datetime_madrid", "site", "generation_mwh_15min_raw"])
    df = (
        df.groupby(["site", "datetime_madrid"], as_index=False)["generation_mwh_15min_raw"]
        .sum()
        .sort_values(["site", "datetime_madrid"])
    )

    hour_decimal = df["datetime_madrid"].dt.hour + df["datetime_madrid"].dt.minute / 60.0
    is_night = (hour_decimal >= night_start_hour) | (hour_decimal < night_end_hour)

    df["is_night_window"] = is_night
    df["night_generation_mwh_raw"] = np.where(is_night, df["generation_mwh_15min_raw"], 0.0)
    df["generation_mwh_15min"] = np.where(is_night, 0.0, df["generation_mwh_15min_raw"]) if zero_night else df["generation_mwh_15min_raw"]
    df["generation_kwh_15min"] = df["generation_mwh_15min"] * 1000.0
    df["hour_madrid"] = df["datetime_madrid"].dt.floor("h")
    df["month"] = df["datetime_madrid"].dt.to_period("M").astype(str)

    diag = (
        df.groupby("site", as_index=False)
        .agg(
            rows=("generation_mwh_15min", "size"),
            start=("datetime_madrid", "min"),
            end=("datetime_madrid", "max"),
            raw_generation_mwh=("generation_mwh_15min_raw", "sum"),
            clean_generation_mwh=("generation_mwh_15min", "sum"),
            raw_night_generation_mwh=("night_generation_mwh_raw", "sum"),
        )
    )
    diag["raw_night_generation_pct"] = np.where(
        diag["raw_generation_mwh"] > 0,
        diag["raw_night_generation_mwh"] / diag["raw_generation_mwh"] * 100,
        0.0,
    )
    return df, diag


# =========================================================
# SCADA fetch
# =========================================================
def flatten_scada_payload(payload: Any) -> list[dict]:
    """
    Tries to normalize common SCADA JSON shapes into a list of dict rows.
    """
    if payload is None:
        return []

    if isinstance(payload, list):
        rows: list[dict] = []
        for item in payload:
            if isinstance(item, dict):
                rows.append(item)
            elif isinstance(item, list):
                rows.extend(flatten_scada_payload(item))
        return rows

    if isinstance(payload, dict):
        for key in ["data", "values", "results", "items", "records", "measurements"]:
            if key in payload:
                return flatten_scada_payload(payload[key])

        # Shape like {"tag1": [{"datetime":..., "value":...}], "tag2": [...]}
        rows = []
        for key, value in payload.items():
            if isinstance(value, list):
                nested = flatten_scada_payload(value)
                for row in nested:
                    if "tag" not in row:
                        row["tag"] = key
                    rows.append(row)
        if rows:
            return rows

        return [payload]

    return []


def build_scada_params(
    tag: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    interval: str,
) -> dict:
    """
    Adjust this if your SCADA API uses different parameter names.
    Current generic params cover many REST time-series APIs.
    """
    return {
        "tag": tag,
        "asset": tag,
        "id": tag,
        "start": start_dt.tz_convert("UTC").isoformat(),
        "end": end_dt.tz_convert("UTC").isoformat(),
        "from": start_dt.tz_convert("UTC").isoformat(),
        "to": end_dt.tz_convert("UTC").isoformat(),
        "interval": interval,
        "resolution": interval,
    }


@st.cache_data(show_spinner=False, ttl=900)
def fetch_scada_tag(
    base_url: str,
    endpoint: str,
    token: str,
    auth_header: str,
    auth_prefix: str,
    tag: str,
    site: str,
    start_iso_madrid: str,
    end_iso_madrid: str,
    interval: str,
    request_mode: str,
) -> pd.DataFrame:
    start_dt = pd.Timestamp(start_iso_madrid)
    end_dt = pd.Timestamp(end_iso_madrid)

    if start_dt.tzinfo is None:
        start_dt = start_dt.tz_localize(MADRID_TZ)
    if end_dt.tzinfo is None:
        end_dt = end_dt.tz_localize(MADRID_TZ)

    url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
    headers = {"Accept": "application/json"}

    if token:
        if auth_prefix:
            headers[auth_header] = f"{auth_prefix} {token}"
        else:
            headers[auth_header] = token

    params = build_scada_params(tag, start_dt, end_dt, interval)

    if request_mode == "POST":
        resp = requests.post(url, headers=headers, json=params, timeout=(15, 90))
    else:
        resp = requests.get(url, headers=headers, params=params, timeout=(15, 90))

    resp.raise_for_status()
    payload = resp.json()
    rows = flatten_scada_payload(payload)

    if not rows:
        return pd.DataFrame(columns=["site", "tag", "datetime", "value"])

    df = pd.DataFrame(rows)
    df["site"] = site
    df["tag"] = tag
    return df


def fetch_scada_all(
    tags_df: pd.DataFrame,
    start_day: date,
    end_day: date,
    interval: str,
    base_url: str,
    endpoint: str,
    token: str,
    auth_header: str,
    auth_prefix: str,
    request_mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_dt = pd.Timestamp(start_day, tz=MADRID_TZ)
    end_dt = pd.Timestamp(end_day + timedelta(days=1), tz=MADRID_TZ)

    frames = []
    logs = []

    for _, row in tags_df.iterrows():
        site = str(row["site"])
        tag = str(row["tag"])
        try:
            temp = fetch_scada_tag(
                base_url=base_url,
                endpoint=endpoint,
                token=token,
                auth_header=auth_header,
                auth_prefix=auth_prefix,
                tag=tag,
                site=site,
                start_iso_madrid=start_dt.isoformat(),
                end_iso_madrid=end_dt.isoformat(),
                interval=interval,
                request_mode=request_mode,
            )
            frames.append(temp)
            logs.append({"site": site, "tag": tag, "status": "ok", "rows": len(temp), "error": ""})
        except Exception as exc:
            logs.append({"site": site, "tag": tag, "status": "error", "rows": 0, "error": str(exc)})

    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return raw, pd.DataFrame(logs)


# =========================================================
# Day Ahead prices
# =========================================================
@st.cache_data(show_spinner=False)
def load_day_ahead_prices() -> pd.DataFrame:
    if not DAY_AHEAD_PRICE_FILE.exists():
        return pd.DataFrame(columns=["hour_madrid", "price_eur_mwh", "price_source"])

    try:
        df = pd.read_excel(DAY_AHEAD_PRICE_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(DAY_AHEAD_PRICE_FILE, sheet_name=0)

    dt_col = find_col(df.columns, ["datetime", "date", "timestamp", "hour"])
    price_col = find_col(df.columns, ["price", "precio", "value", "eur_mwh", "price_eur_mwh"])

    if dt_col is None or price_col is None:
        raise ValueError(f"Could not detect datetime/price columns. Available: {list(df.columns)}")

    dt = pd.to_datetime(df[dt_col], errors="coerce")
    out = pd.DataFrame()
    if getattr(dt.dt, "tz", None) is not None:
        out["hour_madrid"] = dt.dt.tz_convert(MADRID_TZ).dt.floor("h")
    else:
        out["hour_madrid"] = dt.dt.tz_localize(MADRID_TZ, nonexistent="shift_forward", ambiguous="NaT").dt.floor("h")

    out["price_eur_mwh"] = clean_numeric(df[price_col])
    out = (
        out.dropna(subset=["hour_madrid", "price_eur_mwh"])
        .sort_values("hour_madrid")
        .drop_duplicates("hour_madrid", keep="last")
    )
    out["price_source"] = f"Day Ahead workbook — {DAY_AHEAD_PRICE_FILE.name}"
    return out[["hour_madrid", "price_eur_mwh", "price_source"]]


def price_sanity(price_df: pd.DataFrame) -> tuple[str, str]:
    if price_df.empty:
        return "danger", "No hourly Day Ahead prices found for the selected SCADA period."

    p = price_df["price_eur_mwh"].dropna()
    avg, med, pmin, pmax = p.mean(), p.median(), p.min(), p.max()

    if avg > 200 or med > 180 or pmax > 700 or pmin < -300:
        return (
            "warning",
            f"<b>Price sanity warning:</b> baseload {avg:.1f} €/MWh, median {med:.1f}, "
            f"min {pmin:.1f}, max {pmax:.1f}. Check the Day Ahead workbook values for the selected period.",
        )

    return (
        "ok",
        f"<b>Price sanity check passed:</b> baseload {avg:.1f} €/MWh, median {med:.1f}, "
        f"min {pmin:.1f}, max {pmax:.1f}.",
    )


# =========================================================
# Revenues
# =========================================================
def calculate_revenues(gen: pd.DataFrame, price: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    merged = gen.merge(
        price[["hour_madrid", "price_eur_mwh", "price_source"]],
        on="hour_madrid",
        how="left",
    )
    merged["is_priced"] = merged["price_eur_mwh"].notna()
    merged["revenue_eur"] = merged["generation_mwh_15min"] * merged["price_eur_mwh"]

    monthly = (
        merged.groupby(["site", "month"], as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_intervals=("is_priced", "sum"),
            total_intervals=("is_priced", "size"),
        )
    )
    monthly["missing_price_intervals"] = monthly["total_intervals"] - monthly["priced_intervals"]
    monthly["captured_price_eur_mwh"] = np.where(
        monthly["generation_mwh"] > 0,
        monthly["revenue_eur"] / monthly["generation_mwh"],
        np.nan,
    )

    price_month = price.copy()
    price_month["month"] = price_month["hour_madrid"].dt.to_period("M").astype(str)
    baseload_month = (
        price_month.groupby("month", as_index=False)["price_eur_mwh"]
        .mean()
        .rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    )
    monthly = monthly.merge(baseload_month, on="month", how="left")
    monthly["capture_factor_pct"] = monthly["captured_price_eur_mwh"] / monthly["baseload_price_eur_mwh"] * 100

    merged["year"] = merged["datetime_madrid"].dt.year
    annual = (
        merged.groupby(["site", "year"], as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_intervals=("is_priced", "sum"),
            total_intervals=("is_priced", "size"),
        )
    )
    annual["missing_price_intervals"] = annual["total_intervals"] - annual["priced_intervals"]
    annual["captured_price_eur_mwh"] = np.where(
        annual["generation_mwh"] > 0,
        annual["revenue_eur"] / annual["generation_mwh"],
        np.nan,
    )

    price_year = price.copy()
    price_year["year"] = price_year["hour_madrid"].dt.year
    baseload_year = (
        price_year.groupby("year", as_index=False)["price_eur_mwh"]
        .mean()
        .rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    )
    annual = annual.merge(baseload_year, on="year", how="left")
    annual["capture_factor_pct"] = annual["captured_price_eur_mwh"] / annual["baseload_price_eur_mwh"] * 100

    portfolio = (
        merged.groupby("month", as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_intervals=("is_priced", "sum"),
            total_intervals=("is_priced", "size"),
        )
    )
    portfolio["missing_price_intervals"] = portfolio["total_intervals"] - portfolio["priced_intervals"]
    portfolio["captured_price_eur_mwh"] = np.where(
        portfolio["generation_mwh"] > 0,
        portfolio["revenue_eur"] / portfolio["generation_mwh"],
        np.nan,
    )
    portfolio = portfolio.merge(baseload_month, on="month", how="left")
    portfolio["capture_factor_pct"] = portfolio["captured_price_eur_mwh"] / portfolio["baseload_price_eur_mwh"] * 100

    return merged, monthly, annual, portfolio


def display_revenue_table(df: pd.DataFrame) -> None:
    out = df.copy()
    rename = {
        "site": "Site",
        "month": "Month",
        "year": "Year",
        "generation_mwh": "Generation (MWh)",
        "revenue_eur": "Revenue (€)",
        "captured_price_eur_mwh": "Captured price (€/MWh)",
        "baseload_price_eur_mwh": "Baseload price (€/MWh)",
        "capture_factor_pct": "Capture factor (%)",
        "priced_intervals": "Priced intervals",
        "total_intervals": "Total intervals",
        "missing_price_intervals": "Missing price intervals",
    }
    out = out.rename(columns=rename)
    ordered = [v for v in rename.values() if v in out.columns]
    out = out[ordered]

    fmt = {
        "Generation (MWh)": "{:,.2f}",
        "Revenue (€)": "€{:,.0f}",
        "Captured price (€/MWh)": "{:,.2f}",
        "Baseload price (€/MWh)": "{:,.2f}",
        "Capture factor (%)": "{:,.1f}%",
        "Priced intervals": "{:,.0f}",
        "Total intervals": "{:,.0f}",
        "Missing price intervals": "{:,.0f}",
    }

    st.dataframe(
        style_table(out).format({k: v for k, v in fmt.items() if k in out.columns}),
        use_container_width=True,
        hide_index=True,
    )


# =========================================================
# Sidebar
# =========================================================
st.sidebar.title("IS2 SCADA controls")

default_start = date.today().replace(day=1)
default_end = date.today()

start_day = st.sidebar.date_input("Start date", value=default_start)
end_day = st.sidebar.date_input("End date", value=default_end)

st.sidebar.markdown("---")
st.sidebar.subheader("SCADA connection")

base_url = st.sidebar.text_input("SCADA base URL", value=secret_or_env("SCADA_BASE_URL", ""))
endpoint = st.sidebar.text_input("SCADA endpoint", value=secret_or_env("SCADA_ENDPOINT", "/api/timeseries"))
token = st.sidebar.text_input("SCADA token", value=secret_or_env("SCADA_TOKEN", ""), type="password")
auth_header = st.sidebar.text_input("Auth header", value=secret_or_env("SCADA_AUTH_HEADER", "Authorization"))
auth_prefix = st.sidebar.text_input("Auth prefix", value=secret_or_env("SCADA_AUTH_PREFIX", "Bearer"))
request_mode = st.sidebar.selectbox("Request method", ["GET", "POST"], index=0)
scada_interval = st.sidebar.selectbox("SCADA interval", ["15min", "quarter_hour", "PT15M", "hour"], index=0)

st.sidebar.markdown("---")
st.sidebar.subheader("SCADA fields")
dt_field_default = secret_or_env("SCADA_DATETIME_FIELD", "datetime")
site_field_default = secret_or_env("SCADA_SITE_FIELD", "site")
value_field_default = secret_or_env("SCADA_VALUE_FIELD", "value")
scada_source_tz = st.sidebar.selectbox("SCADA timestamp timezone", ["UTC", "Europe/Madrid"], index=0)
scada_unit = st.sidebar.selectbox(
    "SCADA generation unit",
    ["kWh per 15-min", "MWh per 15-min", "kW average", "MW average"],
    index=["kWh per 15-min", "MWh per 15-min", "kW average", "MW average"].index(secret_or_env("SCADA_UNIT", "kWh per 15-min"))
    if secret_or_env("SCADA_UNIT", "kWh per 15-min") in ["kWh per 15-min", "MWh per 15-min", "kW average", "MW average"]
    else 0,
)

st.sidebar.markdown("---")
st.sidebar.subheader("Solar cleaning")
zero_night = st.sidebar.checkbox("Set impossible night generation to zero", value=True)
c1, c2 = st.sidebar.columns(2)
night_start = c1.number_input("Night starts", min_value=18, max_value=24, value=22, step=1)
night_end = c2.number_input("Night ends", min_value=0, max_value=9, value=5, step=1)

show_diagnostics = st.sidebar.checkbox("Show diagnostics", value=True)


# =========================================================
# Main
# =========================================================
st.title("IS2 SCADA generation & day-ahead revenues")
st.caption("Generation is pulled directly from SCADA. Revenues use the same hourly Day Ahead price workbook as the Day Ahead tab.")

tags_df = load_scada_tags()

if tags_df.empty or "site" not in tags_df.columns or "tag" not in tags_df.columns:
    box(
        "danger",
        "No SCADA site/tag mapping found. Add either <code>SCADA_TAGS_JSON</code> in .env/secrets "
        "or create <code>data/is2_scada_tags.csv</code> with columns <code>site, tag</code>.",
    )
    st.stop()

with st.expander("SCADA site/tag mapping", expanded=False):
    st.dataframe(tags_df, use_container_width=True, hide_index=True)

if not base_url:
    box("danger", "Missing SCADA_BASE_URL. Add it in the sidebar, .env, or Streamlit secrets.")
    st.stop()

if pd.Timestamp(start_day) > pd.Timestamp(end_day):
    box("danger", "Start date must be before end date.")
    st.stop()

fetch_button = st.button("Fetch SCADA data", type="primary", use_container_width=False)

if not fetch_button:
    box("ok", "Ready to fetch SCADA data. Use the sidebar to select the period, then click <b>Fetch SCADA data</b>.")
    st.stop()

with st.spinner("Fetching IS2 generation from SCADA..."):
    raw_scada, scada_log = fetch_scada_all(
        tags_df=tags_df,
        start_day=start_day,
        end_day=end_day,
        interval=scada_interval,
        base_url=base_url,
        endpoint=endpoint,
        token=token,
        auth_header=auth_header,
        auth_prefix=auth_prefix,
        request_mode=request_mode,
    )

if raw_scada.empty:
    box("danger", "SCADA returned no data for the selected period/tags. Check endpoint, token, date range and tag mapping.")
    if show_diagnostics:
        st.dataframe(scada_log, use_container_width=True, hide_index=True)
    st.stop()

# Detect fields from SCADA response
dt_col = dt_field_default if dt_field_default in raw_scada.columns else find_col(raw_scada.columns, ["datetime", "timestamp", "date", "time", "ts"])
site_col = site_field_default if site_field_default in raw_scada.columns else find_col(raw_scada.columns, ["site", "asset", "plant", "park", "name"])
value_col = value_field_default if value_field_default in raw_scada.columns else find_col(raw_scada.columns, ["value", "generation", "energy", "kwh", "mwh", "mw", "reading"])

if dt_col is None or site_col is None or value_col is None:
    box(
        "danger",
        f"Could not detect SCADA datetime/site/value fields. Columns received: {list(raw_scada.columns)}. "
        "Set SCADA_DATETIME_FIELD, SCADA_SITE_FIELD and SCADA_VALUE_FIELD in .env/secrets.",
    )
    if show_diagnostics:
        st.dataframe(raw_scada.head(100), use_container_width=True)
        st.dataframe(scada_log, use_container_width=True)
    st.stop()

gen_df, gen_diag = prepare_generation(
    raw_scada,
    dt_col=dt_col,
    site_col=site_col,
    value_col=value_col,
    source_tz=scada_source_tz,
    unit=scada_unit,
    zero_night=zero_night,
    night_start_hour=int(night_start),
    night_end_hour=int(night_end),
)

# Load Day Ahead prices and restrict to SCADA period
try:
    price_df = load_day_ahead_prices()
except Exception as exc:
    price_df = pd.DataFrame(columns=["hour_madrid", "price_eur_mwh", "price_source"])
    box("danger", f"Could not load Day Ahead price workbook: {exc}")

if not price_df.empty:
    price_df = price_df[
        (price_df["hour_madrid"] >= gen_df["hour_madrid"].min()) &
        (price_df["hour_madrid"] <= gen_df["hour_madrid"].max())
    ].copy()

section_header("SCADA generation profile")

pill(f"SCADA rows: {len(raw_scada):,}", "green")
pill(f"Parsed field mapping: {dt_col} / {site_col} / {value_col}", "blue")
pill(f"Price file: {DAY_AHEAD_PRICE_FILE}", "blue")

total_gen = gen_df["generation_mwh_15min"].sum()
raw_total = gen_diag["raw_generation_mwh"].sum()
raw_night = gen_diag["raw_night_generation_mwh"].sum()
raw_night_pct = raw_night / raw_total * 100 if raw_total > 0 else 0.0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Clean SCADA generation", fmt_mwh(total_gen))
m2.metric("Raw night generation", fmt_mwh(raw_night), f"{raw_night_pct:.2f}% raw")
m3.metric("Sites", f"{gen_df['site'].nunique():,.0f}")
m4.metric("15-min intervals", f"{len(gen_df):,.0f}")

if raw_night > max(0.005 * raw_total, 0.01):
    box(
        "warning",
        f"<b>Solar sanity warning:</b> raw SCADA data contains {raw_night:,.2f} MWh in the configured night window "
        f"({raw_night_pct:.2f}% of raw generation). Cleaning applied: "
        f"<b>{'night values set to zero' if zero_night else 'night values kept'}</b>.",
    )
else:
    box("ok", "<b>Solar sanity check passed:</b> SCADA night generation is negligible.")

site_options = sorted(gen_df["site"].unique())
selected_sites = st.multiselect(
    "Sites to display",
    site_options,
    default=site_options[: min(6, len(site_options))],
)

plot_gen = gen_df[gen_df["site"].isin(selected_sites)].copy()

fig = go.Figure()
for site in selected_sites:
    sub = plot_gen[plot_gen["site"] == site]
    fig.add_trace(
        go.Scatter(
            x=sub["datetime_madrid"],
            y=sub["generation_kwh_15min"],
            mode="lines",
            name=site,
            line=dict(width=1.8),
            hovertemplate="%{x|%d-%b %H:%M}<br>%{y:,.1f} kWh/15-min<extra>" + site + "</extra>",
        )
    )

fig.update_layout(
    **chart_layout(
        "15-min SCADA generation profile",
        "No upload file; values fetched from SCADA and converted to Europe/Madrid",
        520,
    )
)
fig.update_xaxes(title="Madrid date and hour", showgrid=False)
fig.update_yaxes(title="Generation (kWh / 15-min)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8")
st.plotly_chart(fig, use_container_width=True)


section_header("Day-ahead revenues — SCADA operational parks")

if price_df.empty:
    box(
        "danger",
        "No Day Ahead hourly prices found for the SCADA period. "
        "Check that <code>data/hourly_avg_price_since2021.xlsx</code> contains this date range.",
    )
    st.stop()

status, msg = price_sanity(price_df)
box(status, msg)

revenues_df, monthly, annual, portfolio_month = calculate_revenues(gen_df, price_df)

priced_ratio = revenues_df["is_priced"].mean() * 100 if len(revenues_df) else 0.0
total_revenue = revenues_df["revenue_eur"].sum(skipna=True)
captured_price = total_revenue / total_gen if total_gen > 0 else np.nan
baseload_price = price_df["price_eur_mwh"].mean()
capture_factor = captured_price / baseload_price * 100 if baseload_price and not pd.isna(captured_price) else np.nan

r1, r2, r3, r4 = st.columns(4)
r1.metric("Portfolio revenue", fmt_eur(total_revenue))
r2.metric("Captured price", fmt_price(captured_price))
r3.metric("Baseload price", fmt_price(baseload_price))
r4.metric("Capture factor", f"{capture_factor:.1f}%" if not pd.isna(capture_factor) else "—", f"{priced_ratio:.1f}% priced intervals")

hourly_gen = (
    revenues_df.groupby("hour_madrid", as_index=False)
    .agg(
        generation_mwh=("generation_mwh_15min", "sum"),
        revenue_eur=("revenue_eur", "sum"),
    )
)
hourly = hourly_gen.merge(price_df[["hour_madrid", "price_eur_mwh"]], on="hour_madrid", how="left")

fig_rev = go.Figure()
fig_rev.add_trace(
    go.Bar(
        x=hourly["hour_madrid"],
        y=hourly["generation_mwh"],
        name="SCADA generation",
        yaxis="y",
        marker=dict(color=CORP_GREEN),
        opacity=0.82,
        hovertemplate="%{x|%d-%b %H:%M}<br>Generation: %{y:,.2f} MWh<extra></extra>",
    )
)
fig_rev.add_trace(
    go.Scatter(
        x=hourly["hour_madrid"],
        y=hourly["price_eur_mwh"],
        name="Day-ahead price",
        yaxis="y2",
        mode="lines",
        line=dict(color=CORP_BLUE, width=2.6),
        hovertemplate="%{x|%d-%b %H:%M}<br>Price: %{y:,.2f} €/MWh<extra></extra>",
    )
)
fig_rev.update_layout(
    **chart_layout(
        "Hourly SCADA generation and day-ahead price",
        "Generation aggregated from 15-min SCADA intervals; price joined by Madrid hour",
        470,
    )
)
fig_rev.update_layout(
    yaxis=dict(
        title="SCADA generation (MWh)",
        gridcolor="#E5E7EB",
        zeroline=True,
        zerolinecolor="#94A3B8",
    ),
    yaxis2=dict(
        title="Price (€/MWh)",
        overlaying="y",
        side="right",
        showgrid=False,
    ),
    bargap=0.04,
)
fig_rev.update_xaxes(title="Madrid date and hour", showgrid=False)
st.plotly_chart(fig_rev, use_container_width=True)

st.markdown("#### Portfolio monthly summary")
display_revenue_table(portfolio_month.sort_values("month"))

tab_month, tab_annual, tab_detail = st.tabs(["Monthly by site", "Annual by site", "15-min revenue detail"])

with tab_month:
    display_revenue_table(monthly.sort_values(["site", "month"]))

with tab_annual:
    display_revenue_table(annual.sort_values(["site", "year"]))

with tab_detail:
    detail = revenues_df[
        [
            "datetime_madrid",
            "site",
            "generation_mwh_15min",
            "hour_madrid",
            "price_eur_mwh",
            "revenue_eur",
            "is_priced",
        ]
    ].rename(
        columns={
            "datetime_madrid": "15-min timestamp",
            "site": "Site",
            "generation_mwh_15min": "Generation (MWh)",
            "hour_madrid": "Price hour",
            "price_eur_mwh": "Price (€/MWh)",
            "revenue_eur": "Revenue (€)",
            "is_priced": "Priced",
        }
    )
    st.dataframe(
        style_table(detail.head(800)).format(
            {
                "Generation (MWh)": "{:,.4f}",
                "Price (€/MWh)": "{:,.2f}",
                "Revenue (€)": "€{:,.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Showing first 800 rows. Download the complete detail below.")

if show_diagnostics:
    section_header("Diagnostics")

    with st.expander("SCADA request log", expanded=False):
        st.dataframe(scada_log, use_container_width=True, hide_index=True)

    with st.expander("SCADA raw sample", expanded=False):
        st.dataframe(raw_scada.head(100), use_container_width=True, hide_index=True)

    with st.expander("Generation diagnostics", expanded=False):
        st.dataframe(
            style_table(gen_diag).format(
                {
                    "raw_generation_mwh": "{:,.3f}",
                    "clean_generation_mwh": "{:,.3f}",
                    "raw_night_generation_mwh": "{:,.3f}",
                    "raw_night_generation_pct": "{:,.2f}%",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("Price diagnostics", expanded=False):
        pdiag = price_df["price_eur_mwh"].describe().to_frame("price_eur_mwh").T
        st.dataframe(style_table(pdiag).format("{:,.2f}"), use_container_width=True)
        st.dataframe(price_df.head(80), use_container_width=True, hide_index=True)

section_header("Downloads")
d1, d2, d3 = st.columns(3)
d1.download_button(
    "Cleaned SCADA generation",
    data=gen_df.to_csv(index=False).encode("utf-8"),
    file_name="is2_scada_generation_15min_cleaned.csv",
    mime="text/csv",
)
d2.download_button(
    "Revenue detail",
    data=revenues_df.to_csv(index=False).encode("utf-8"),
    file_name="is2_scada_revenue_detail_15min.csv",
    mime="text/csv",
)
d3.download_button(
    "Monthly revenue metrics",
    data=monthly.to_csv(index=False).encode("utf-8"),
    file_name="is2_scada_monthly_revenue_metrics.csv",
    mime="text/csv",
)
