from __future__ import annotations

import calendar
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Iterable

import altair as alt
import numpy as np
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

# =========================================================
# PAGE / THEME
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
FORWARD_CURVES_DIRS = [
    BASE_DIR / "forward_curves",
    DATA_DIR / "forward_curves",
    Path(__file__).resolve().parent / "forward_curves",
]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Monthly Market Report", layout="wide")

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREEN_LIGHT = "#D1FAE5"
BLUE = "#1D4ED8"
BLUE_DARK = "#1E3A8A"
YELLOW = "#FBBF24"
YELLOW_DARK = "#D97706"
ORANGE = "#EA580C"
RED = "#DC2626"
PURPLE = "#7C3AED"
GREY = "#6B7280"
GRID = "#E5E7EB"
TEXT = "#111827"
WHITE = "#FFFFFF"

AURORA_COLOR = ORANGE
BARINGA_COLOR = BLUE_DARK

st.markdown(
    f"""
    <style>
    html, body, [class*="css"] {{
        font-size: 100% !important;
    }}
    .stApp {{
        background: #FFFFFF;
    }}
    h1 {{
        font-size: 2.2rem !important;
        letter-spacing: -0.02em;
    }}
    h2 {{
        font-size: 1.4rem !important;
    }}
    h3 {{
        font-size: 1.15rem !important;
    }}
    .report-hero {{
        background: linear-gradient(115deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #B7EAD6 100%);
        border-radius: 18px;
        padding: 22px 24px;
        color: white;
        margin-bottom: 16px;
        box-shadow: 0 8px 24px rgba(15, 118, 110, 0.12);
    }}
    .report-hero-title {{
        font-size: 2rem;
        font-weight: 850;
        line-height: 1.1;
        margin-bottom: 6px;
    }}
    .report-hero-subtitle {{
        font-size: 1rem;
        opacity: 0.95;
    }}
    .section-banner {{
        background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 58%, #C7F0DD 100%);
        color: white;
        padding: 12px 16px;
        border-radius: 12px;
        font-weight: 800;
        font-size: 1.15rem;
        margin-top: 20px;
        margin-bottom: 12px;
        box-shadow: 0 2px 8px rgba(15,118,110,0.14);
    }}
    .mini-banner {{
        color: {TEXT};
        border-left: 5px solid {CORP_GREEN};
        padding: 7px 12px;
        font-weight: 800;
        font-size: 1.02rem;
        margin-top: 14px;
        margin-bottom: 8px;
        background: #F8FFFC;
        border-radius: 8px;
    }}
    .card-note {{
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 12px 14px;
        color: #334155;
        margin-bottom: 10px;
    }}
    .pill {{
        display: inline-block;
        background: #ECFDF5;
        border: 1px solid #A7F3D0;
        color: {CORP_GREEN_DARK};
        border-radius: 999px;
        padding: 4px 10px;
        font-size: 0.88rem;
        font-weight: 750;
        margin-right: 6px;
    }}
    .metric-caption {{
        color: #64748B;
        font-size: 0.82rem;
        font-weight: 600;
    }}
    div[data-testid="stMetricValue"] {{
        font-weight: 850;
        letter-spacing: -0.02em;
    }}
    div[data-testid="stMetricLabel"] {{
        font-weight: 720;
    }}
    .metric-footnote {{
        color: #64748B;
        font-size: 0.78rem;
        line-height: 1.35;
        margin-top: -0.35rem;
        margin-bottom: 0.55rem;
    }}
    .comparison-note {{
        color: #475569;
        font-size: 0.82rem;
        margin-top: 0.15rem;
        margin-bottom: 0.35rem;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

def section(title: str, icon: str = "") -> None:
    st.markdown(f'<div class="section-banner">{icon} {title}</div>', unsafe_allow_html=True)

def subsection(title: str) -> None:
    st.markdown(f'<div class="mini-banner">{title}</div>', unsafe_allow_html=True)

def card_note(text: str) -> None:
    st.markdown(f'<div class="card-note">{text}</div>', unsafe_allow_html=True)

def pills(items: list[str]) -> None:
    html = "".join(f'<span class="pill">{item}</span>' for item in items)
    st.markdown(html, unsafe_allow_html=True)

def apply_chart_style(chart, height: int = 340):
    return (
        chart.properties(height=height)
        .configure_view(stroke=GRID, fill=WHITE)
        .configure_axis(
            grid=True,
            gridColor=GRID,
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor=TEXT,
            titleColor=TEXT,
            labelFontSize=11,
            titleFontSize=13,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=11,
            titleFontSize=12,
            symbolStrokeWidth=3,
        )
        .configure_title(fontSize=14, anchor="start", color=TEXT)
    )

def fmt_eur(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{decimals}f} €/MWh"

def fmt_pct(value: float | int | None, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.{decimals}%}"

def fmt_mw_revenue(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.0f} €/MW".replace(",", ".")

def month_label(ts: pd.Timestamp, mtd: bool = False) -> str:
    label = ts.strftime("%b %Y")
    return f"{label} (MTD)" if mtd else label

def month_start(d: date | pd.Timestamp) -> pd.Timestamp:
    t = pd.Timestamp(d)
    return pd.Timestamp(t.year, t.month, 1)

def month_end(ts: pd.Timestamp, current_day: date | None = None) -> pd.Timestamp:
    if current_day and ts.year == current_day.year and ts.month == current_day.month:
        return pd.Timestamp(current_day)
    last = calendar.monthrange(ts.year, ts.month)[1]
    return pd.Timestamp(ts.year, ts.month, last)

def previous_month(ts: pd.Timestamp) -> pd.Timestamp:
    return (ts - pd.offsets.MonthBegin(1)).normalize().replace(day=1)

def yoy_month(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts.year - 1, ts.month, 1)

def comparable_ytd_end(end_ts: pd.Timestamp, target_year: int) -> pd.Timestamp:
    """Return the same month/day cut-off in another year, with leap-day safety."""
    try:
        return pd.Timestamp(target_year, end_ts.month, end_ts.day)
    except ValueError:
        return pd.Timestamp(target_year, end_ts.month, 1) + pd.offsets.MonthEnd(0)

def values_equal_month(series: pd.Series, ts: pd.Timestamp) -> pd.Series:
    return (series.dt.year == ts.year) & (series.dt.month == ts.month)

# =========================================================
# FILES / CONFIG
# =========================================================
HIST_WORKBOOK_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_SOLAR_FILE = DATA_DIR / "p48solar_since21.csv"
EEX_FORWARD_LOCAL_CSV = DATA_DIR / "eex_forward_market.csv"
EEX_FORWARD_LOCAL_XLSX = DATA_DIR / "eex_forward_market.xlsx"

BESS_MONTHLY_CANDIDATES = [
    DATA_DIR / "bess_monthly_metrics.xlsx",
    DATA_DIR / "bess_monthly_metrics.csv",
    DATA_DIR / "bess_report_monthly.xlsx",
    DATA_DIR / "bess_report_monthly.csv",
]
HYBRID_MONTHLY_CANDIDATES = [
    DATA_DIR / "hybrid_captured_price_monthly.xlsx",
    DATA_DIR / "hybrid_captured_price_monthly.csv",
    DATA_DIR / "pv_bess_hybrid_monthly.xlsx",
    DATA_DIR / "pv_bess_hybrid_monthly.csv",
]

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
LIVE_START_DATE = date(2026, 1, 1)

# =========================================================
# HISTORICAL + LIVE DATA
# =========================================================
def build_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }

def _parse_datetime_esios(df: pd.DataFrame) -> pd.Series:
    for col in ["datetime", "datetime_utc", "datetime_local", "date"]:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", utc=True)
            if dt.notna().any():
                return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    return pd.to_datetime(pd.Series([pd.NaT] * len(df)))

def parse_esios_indicator(raw_json: dict, source_name: str) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    df = pd.DataFrame(values)
    if "geo_id" not in df.columns:
        df["geo_id"] = pd.NA
    if "geo_name" not in df.columns:
        df["geo_name"] = pd.NA
    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    df["datetime"] = _parse_datetime_esios(df)
    df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    df["source"] = source_name
    return df[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(indicator_id: int, start_day: date, end_day: date, token: str) -> pd.DataFrame:
    if not token or start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    frames: list[pd.DataFrame] = []
    chunk_start = start_day
    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=13))
        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(chunk_end + timedelta(days=1), tz="Europe/Madrid")
        params = {
            "start_date": start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time_trunc": "quarter_hour" if chunk_start >= date(2025, 10, 1) else "hour",
        }
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=build_headers(token), params=params, timeout=(15, 120))
                resp.raise_for_status()
                parsed = parse_esios_indicator(resp.json(), f"esios_{indicator_id}")
                if not parsed.empty:
                    frames.append(parsed)
                break
            except requests.exceptions.RequestException:
                sleep(1.2 * (attempt + 1))
        chunk_start = chunk_end + timedelta(days=1)
    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )

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
    df = df.dropna(subset=["datetime", "price"]).copy()
    return df[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_historical_solar() -> pd.DataFrame:
    if HIST_WORKBOOK_FILE.exists():
        try:
            df = pd.read_excel(HIST_WORKBOOK_FILE, sheet_name="solar_hourly_best")
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
            df = df.dropna(subset=["datetime", "solar_best_mw"]).copy()
            return df[["datetime", "solar_best_mw"]].sort_values("datetime").reset_index(drop=True)
        except Exception:
            pass
    if not HIST_SOLAR_FILE.exists():
        return pd.DataFrame(columns=["datetime", "solar_best_mw"])
    df = pd.read_csv(HIST_SOLAR_FILE)
    if "solar_best_mw" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "solar_best_mw"})
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
    df = df.dropna(subset=["datetime", "solar_best_mw"]).copy()
    return df[["datetime", "solar_best_mw"]].sort_values("datetime").reset_index(drop=True)

def build_best_solar_hourly(p48: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    p48 = p48.copy()
    fc = forecast.copy()
    if p48.empty and fc.empty:
        return pd.DataFrame(columns=["datetime", "solar_best_mw"])
    if p48.empty:
        out = fc.rename(columns={"solar_forecast_mw": "solar_best_mw"})[["datetime", "solar_best_mw"]]
        return out.sort_values("datetime").reset_index(drop=True)
    if fc.empty:
        out = p48.rename(columns={"solar_p48_mw": "solar_best_mw"})[["datetime", "solar_best_mw"]]
        return out.sort_values("datetime").reset_index(drop=True)
    out = p48.merge(fc, on="datetime", how="outer").sort_values("datetime")
    out["solar_best_mw"] = out["solar_p48_mw"].combine_first(out["solar_forecast_mw"])
    return out[["datetime", "solar_best_mw"]].dropna().sort_values("datetime").reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def load_live_prices(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token)
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])
    out = raw[["datetime", "value"]].rename(columns={"value": "price"})
    out["datetime"] = out["datetime"].dt.floor("h")
    return out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")

@st.cache_data(show_spinner=False, ttl=3600)
def load_live_solar(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw_p48 = fetch_esios_range(SOLAR_P48_INDICATOR_ID, start_day, end_day, token)
    raw_fc = fetch_esios_range(SOLAR_FORECAST_INDICATOR_ID, start_day, end_day, token)
    if not raw_p48.empty:
        p48 = raw_p48[["datetime", "value"]].rename(columns={"value": "solar_p48_mw"})
        p48["datetime"] = p48["datetime"].dt.floor("h")
        p48 = p48.groupby("datetime", as_index=False)["solar_p48_mw"].mean()
    else:
        p48 = pd.DataFrame(columns=["datetime", "solar_p48_mw"])
    if not raw_fc.empty:
        fc = raw_fc[["datetime", "value"]].rename(columns={"value": "solar_forecast_mw"})
        fc["datetime"] = fc["datetime"].dt.floor("h")
        fc = fc.groupby("datetime", as_index=False)["solar_forecast_mw"].mean()
    else:
        fc = pd.DataFrame(columns=["datetime", "solar_forecast_mw"])
    return build_best_solar_hourly(p48, fc)

def combine_hist_live(hist: pd.DataFrame, live: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    out = pd.concat([hist, live], ignore_index=True)
    if out.empty:
        return out
    return out.sort_values(subset).drop_duplicates(subset=subset, keep="last").reset_index(drop=True)

# =========================================================
# FORWARD HOURLY SCENARIOS (AURORA / BARINGA)
# =========================================================
def _norm_col(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")

def _scenario_model_from_name(path: Path) -> str | None:
    name = path.name.lower()
    if "aurora" in name:
        return "Aurora"
    if "baringa" in name or "bringa" in name:
        return "Baringa"
    return None

def _forward_datetime_from_sheet(df: pd.DataFrame) -> pd.Series:
    cols = {_norm_col(c): c for c in df.columns}
    for key in ["datetime", "date_time", "timestamp", "cet", "cest", "local_datetime", "period_start"]:
        if key in cols:
            dt = pd.to_datetime(df[cols[key]], errors="coerce", dayfirst=True)
            if dt.notna().any():
                return dt
    day_col = next((cols[k] for k in ["day", "date", "fecha", "dia"] if k in cols), None)
    hour_col = next((cols[k] for k in ["hour", "period", "hora", "he"] if k in cols), None)
    if day_col is not None and hour_col is not None:
        day = pd.to_datetime(df[day_col], errors="coerce", dayfirst=True).dt.normalize()
        hour = pd.to_numeric(df[hour_col], errors="coerce")
        valid = hour.dropna()
        if not valid.empty and valid.min() >= 1 and valid.max() <= 24:
            hour = hour - 1
        return day + pd.to_timedelta(hour, unit="h")
    return pd.Series(pd.NaT, index=df.index)

def _forward_price_col(df: pd.DataFrame) -> str | None:
    cols = {_norm_col(c): c for c in df.columns}
    for key in ["omie_venta", "price", "spot_price", "market_price", "value", "eur_mwh"]:
        if key in cols:
            return cols[key]
    return None

def _normalise_forward_candidate(raw: pd.DataFrame, model: str, path: Path, sheet: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["datetime", "price", "model", "source_file", "source_sheet"])
    dt = _forward_datetime_from_sheet(raw)
    price_col = _forward_price_col(raw)
    if price_col is None:
        return pd.DataFrame(columns=["datetime", "price", "model", "source_file", "source_sheet"])
    out = pd.DataFrame({
        "datetime": pd.to_datetime(dt, errors="coerce"),
        "price": pd.to_numeric(raw[price_col], errors="coerce"),
        "model": model,
        "source_file": path.name,
        "source_sheet": sheet,
    }).dropna(subset=["datetime", "price"])
    return out.sort_values("datetime").reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_forward_hourly_scenarios() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    seen: set[Path] = set()
    for folder in FORWARD_CURVES_DIRS:
        if not folder.exists():
            continue
        for pattern in ("*.xlsx", "*.xls", "*.csv"):
            for path in folder.glob(pattern):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                model = _scenario_model_from_name(path)
                if model is None:
                    continue
                candidates: list[pd.DataFrame] = []
                try:
                    if path.suffix.lower() == ".csv":
                        candidates.append(_normalise_forward_candidate(pd.read_csv(path), model, path, "CSV"))
                    else:
                        xls = pd.ExcelFile(path)
                        for sheet in xls.sheet_names:
                            try:
                                candidates.append(_normalise_forward_candidate(pd.read_excel(path, sheet_name=sheet), model, path, sheet))
                            except Exception:
                                continue
                except Exception:
                    continue
                candidates = [c for c in candidates if not c.empty]
                if candidates:
                    frames.append(max(candidates, key=len))
    if not frames:
        return pd.DataFrame(columns=["datetime", "price", "model", "source_file", "source_sheet"])
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(subset=["datetime", "model"], keep="last").sort_values(["model", "datetime"]).reset_index(drop=True)

# =========================================================
# CAPTURE / CURTAILMENT / HEATMAPS
# =========================================================
def period_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> dict[str, float | None]:
    mask_p = (price_hourly["datetime"] >= start_ts) & (price_hourly["datetime"] < end_ts + pd.Timedelta(days=1))
    p = price_hourly.loc[mask_p, ["datetime", "price"]].copy()
    if p.empty:
        return {"avg_price": None, "captured_uncurtailed": None, "captured_curtailed": None, "capture_rate_uncurtailed": None, "capture_rate_curtailed": None}
    avg_price = float(p["price"].mean())
    mask_s = (solar_hourly["datetime"] >= start_ts) & (solar_hourly["datetime"] < end_ts + pd.Timedelta(days=1))
    s = solar_hourly.loc[mask_s, ["datetime", "solar_best_mw"]].copy()
    merged = p.merge(s, on="datetime", how="inner")
    merged = merged[pd.to_numeric(merged["solar_best_mw"], errors="coerce").fillna(0) > 0].copy()
    if merged.empty:
        return {"avg_price": avg_price, "captured_uncurtailed": None, "captured_curtailed": None, "capture_rate_uncurtailed": None, "capture_rate_curtailed": None}
    merged["weighted"] = merged["price"] * merged["solar_best_mw"]
    unc = merged["weighted"].sum() / merged["solar_best_mw"].sum()
    curtailed = merged[merged["price"] > 0].copy()
    cur = curtailed["weighted"].sum() / curtailed["solar_best_mw"].sum() if not curtailed.empty and curtailed["solar_best_mw"].sum() else np.nan
    return {
        "avg_price": float(avg_price),
        "captured_uncurtailed": float(unc),
        "captured_curtailed": None if pd.isna(cur) else float(cur),
        "capture_rate_uncurtailed": None if avg_price == 0 else float(unc / avg_price),
        "capture_rate_curtailed": None if avg_price == 0 or pd.isna(cur) else float(cur / avg_price),
    }

def monthly_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame(columns=["period", "avg_spot_price", "captured_solar_price_uncurtailed", "captured_solar_price_curtailed", "capture_pct_uncurtailed", "capture_pct_curtailed"])
    months = sorted(price_hourly["datetime"].dt.to_period("M").dropna().unique())
    rows = []
    for month in months:
        start = month.to_timestamp()
        end = month_end(start)
        m = period_metrics(price_hourly, solar_hourly, start, end)
        rows.append({
            "period": start,
            "avg_spot_price": m["avg_price"],
            "captured_solar_price_uncurtailed": m["captured_uncurtailed"],
            "captured_solar_price_curtailed": m["captured_curtailed"],
            "capture_pct_uncurtailed": m["capture_rate_uncurtailed"],
            "capture_pct_curtailed": m["capture_rate_curtailed"],
        })
    return pd.DataFrame(rows).sort_values("period").reset_index(drop=True)

def annual_capture_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp | None = None) -> dict[str, float | None]:
    start = pd.Timestamp(year, 1, 1)
    end = end_ts if end_ts is not None else pd.Timestamp(year, 12, 31)
    return period_metrics(price_hourly, solar_hourly, start, end)

def build_report_capture_table(
    monthly: pd.DataFrame,
    price_hourly: pd.DataFrame,
    solar_hourly: pd.DataFrame,
    forward_scenarios: pd.DataFrame,
    report_month: pd.Timestamp,
    report_end: pd.Timestamp,
) -> pd.DataFrame:
    month_names = [calendar.month_abbr[m] for m in range(1, 13)]
    rows = []
    monthly = monthly.copy()
    monthly["year"] = monthly["period"].dt.year
    monthly["month_num"] = monthly["period"].dt.month
    for m in range(1, 13):
        row = {"Month": month_names[m - 1]}
        for year in [2025, 2026]:
            hit = monthly[(monthly["year"] == year) & (monthly["month_num"] == m)]
            prefix = str(year)
            if not hit.empty:
                r = hit.iloc[-1]
                row[f"{prefix} Baseload"] = r["avg_spot_price"]
                row[f"{prefix} Solar unc."] = r["captured_solar_price_uncurtailed"]
                row[f"{prefix} Rate unc."] = r["capture_pct_uncurtailed"]
                row[f"{prefix} Solar curt."] = r["captured_solar_price_curtailed"]
                row[f"{prefix} Rate curt."] = r["capture_pct_curtailed"]
            else:
                for col in ["Baseload", "Solar unc.", "Rate unc.", "Solar curt.", "Rate curt."]:
                    row[f"{prefix} {col}"] = np.nan
        rows.append(row)

    # Annual / YTD actuals
    yr25 = annual_capture_metrics(price_hourly, solar_hourly, 2025)
    ytd26_end = report_end if report_month.year == 2026 else pd.Timestamp(2026, 12, 31)
    ytd26 = annual_capture_metrics(price_hourly, solar_hourly, 2026, end_ts=ytd26_end)
    final = {"Month": "YR / YTD"}
    for prefix, metrics in [("2025", yr25), ("2026", ytd26)]:
        final[f"{prefix} Baseload"] = metrics["avg_price"]
        final[f"{prefix} Solar unc."] = metrics["captured_uncurtailed"]
        final[f"{prefix} Rate unc."] = metrics["capture_rate_uncurtailed"]
        final[f"{prefix} Solar curt."] = metrics["captured_curtailed"]
        final[f"{prefix} Rate curt."] = metrics["capture_rate_curtailed"]
    rows.append(final)

    # 2026 YTD forecast rows for direct Aurora / Baringa comparison
    if report_month.year == 2026 and forward_scenarios is not None and not forward_scenarios.empty:
        for model in ["Aurora", "Baringa"]:
            f = forward_scenarios[
                (forward_scenarios["model"] == model)
                & (forward_scenarios["datetime"].dt.year == 2026)
                & (forward_scenarios["datetime"] <= ytd26_end + pd.Timedelta(days=1))
            ][["datetime", "price"]].copy()
            row = {"Month": f"YTD {model}"}
            for col in ["Baseload", "Solar unc.", "Rate unc.", "Solar curt.", "Rate curt."]:
                row[f"2025 {col}"] = np.nan
            metrics = annual_capture_metrics(f, solar_hourly, 2026, end_ts=ytd26_end) if not f.empty else {
                "avg_price": None,
                "captured_uncurtailed": None,
                "capture_rate_uncurtailed": None,
                "captured_curtailed": None,
                "capture_rate_curtailed": None,
            }
            row["2026 Baseload"] = metrics["avg_price"]
            row["2026 Solar unc."] = metrics["captured_uncurtailed"]
            row["2026 Rate unc."] = metrics["capture_rate_uncurtailed"]
            row["2026 Solar curt."] = metrics["captured_curtailed"]
            row["2026 Rate curt."] = metrics["capture_rate_curtailed"]
            rows.append(row)
    return pd.DataFrame(rows)

def format_capture_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    fmt = {}
    for col in df.columns:
        if "Rate" in col:
            fmt[col] = "{:.1%}"
        elif col != "Month":
            fmt[col] = "{:,.1f}"

    def _style_col(col: pd.Series) -> list[str]:
        name = str(col.name)
        if name.startswith("2025 "):
            base = "background-color: #F8FAFC;"
            if "Baseload" in name:
                base = "background-color: #DBEAFE; font-weight: 750;"
            return [base] * len(col)
        if name.startswith("2026 ") and "Baseload" in name:
            return ["background-color: #EFF6FF; font-weight: 750;"] * len(col)
        return [""] * len(col)

    def _style_rows(row: pd.Series) -> list[str]:
        label = str(row.get("Month", ""))
        if label == "YR / YTD":
            return ["background-color: #ECFDF5; font-weight: 850; border-top: 2px solid #10B981;"] * len(row)
        if label.startswith("YTD Aurora"):
            return ["background-color: #FFF7ED; font-weight: 750;"] * len(row)
        if label.startswith("YTD Baringa"):
            return ["background-color: #EFF6FF; font-weight: 750;"] * len(row)
        return [""] * len(row)

    return (
        df.style.format(fmt, na_rep="—")
        .apply(_style_col, axis=0)
        .apply(_style_rows, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "5px 7px")]},
        ])
    )

def ytd_price_heatmap(price_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp):
    p = price_hourly[(price_hourly["datetime"].dt.year == year) & (price_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    if p.empty:
        return None
    p["month"] = p["datetime"].dt.strftime("%b")
    p["month_num"] = p["datetime"].dt.month
    p["hour"] = p["datetime"].dt.hour
    grouped = p.groupby(["month_num", "month", "hour"], as_index=False)["price"].mean()
    month_order = [calendar.month_abbr[m] for m in range(1, 13)]
    grouped["label"] = grouped["price"].map(lambda x: f"{x:.0f}" if pd.notna(x) else "")
    chart = alt.layer(
        alt.Chart(grouped).mark_rect().encode(
            x=alt.X("hour:O", title="Hour", sort=list(range(24))),
            y=alt.Y("month:N", title="Month", sort=month_order),
            color=alt.Color("price:Q", title="Avg spot €/MWh", scale=alt.Scale(scheme="redyellowgreen", reverse=True)),
            tooltip=[
                alt.Tooltip("month:N", title="Month"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("price:Q", title="Average spot price", format=",.2f"),
            ],
        ),
        alt.Chart(grouped).mark_text(fontSize=8).encode(
            x=alt.X("hour:O", sort=list(range(24))),
            y=alt.Y("month:N", sort=month_order),
            text="label:N",
            color=alt.condition("datum.price >= 120", alt.value("white"), alt.value(TEXT)),
        ),
    ).properties(title=f"YTD average hourly spot heatmap | {year}")
    return apply_chart_style(chart, height=420)

def negative_frequency_heatmap(price_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp):
    p = price_hourly[(price_hourly["datetime"].dt.year == year) & (price_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    if p.empty:
        return None
    p["month"] = p["datetime"].dt.strftime("%b")
    p["month_num"] = p["datetime"].dt.month
    p["hour"] = p["datetime"].dt.hour
    p["flag"] = (p["price"] <= 0).astype(float)
    grouped = p.groupby(["month_num", "month", "hour"], as_index=False)["flag"].mean()
    grouped["pct"] = grouped["flag"] * 100.0
    grouped["label"] = grouped["pct"].map(lambda x: f"{x:.0f}%" if x > 0 else "")
    month_order = [calendar.month_abbr[m] for m in range(1, 13)]
    chart = alt.layer(
        alt.Chart(grouped).mark_rect().encode(
            x=alt.X("hour:O", title="Hour", sort=list(range(24))),
            y=alt.Y("month:N", title="Month", sort=month_order),
            color=alt.Color("pct:Q", title="% hours", scale=alt.Scale(scheme="yelloworangered")),
            tooltip=[
                alt.Tooltip("month:N", title="Month"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("pct:Q", title="Zero/negative frequency", format=",.1f"),
            ],
        ),
        alt.Chart(grouped).mark_text(fontSize=9).encode(
            x=alt.X("hour:O", sort=list(range(24))),
            y=alt.Y("month:N", sort=month_order),
            text="label:N",
            color=alt.condition("datum.pct >= 50", alt.value("white"), alt.value(TEXT)),
        ),
    ).properties(title=f"YTD zero / negative price frequency | {year}")
    return apply_chart_style(chart, height=420)

def ytd_hourly_overlay(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, forward_scenarios: pd.DataFrame, year: int, end_ts: pd.Timestamp):
    p = price_hourly[(price_hourly["datetime"].dt.year == year) & (price_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    s = solar_hourly[(solar_hourly["datetime"].dt.year == year) & (solar_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    if p.empty:
        return None
    p["hour"] = p["datetime"].dt.hour
    p_avg = p.groupby("hour", as_index=False)["price"].mean()
    p_avg["series"] = f"Spot {year} YTD"
    p_avg["value"] = p_avg["price"]
    price_frames = [p_avg[["hour", "series", "value"]]]
    if year == 2026 and not forward_scenarios.empty:
        fs = forward_scenarios[(forward_scenarios["datetime"].dt.year == 2026) & (forward_scenarios["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
        for model in ["Aurora", "Baringa"]:
            m = fs[fs["model"] == model].copy()
            if m.empty:
                continue
            m["hour"] = m["datetime"].dt.hour
            avg = m.groupby("hour", as_index=False)["price"].mean()
            avg["series"] = model
            avg["value"] = avg["price"]
            price_frames.append(avg[["hour", "series", "value"]])
    price_plot = pd.concat(price_frames, ignore_index=True)
    if not s.empty:
        s["hour"] = s["datetime"].dt.hour
        solar_plot = s.groupby("hour", as_index=False)["solar_best_mw"].mean()
    else:
        solar_plot = pd.DataFrame(columns=["hour", "solar_best_mw"])
    price_color_scale = alt.Scale(
        domain=[f"Spot {year} YTD", "Aurora", "Baringa"],
        range=[BLUE, AURORA_COLOR, BARINGA_COLOR],
    )
    dash = alt.Scale(
        domain=[f"Spot {year} YTD", "Aurora", "Baringa"],
        range=[[1, 0], [7, 3], [7, 3]],
    )
    layers = []
    if not solar_plot.empty:
        layers.append(
            alt.Chart(solar_plot).mark_area(opacity=0.33, color=YELLOW).encode(
                x=alt.X("hour:O", title="Hour", sort=list(range(24))),
                y=alt.Y("solar_best_mw:Q", title="Solar generation (MW)"),
                tooltip=[
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("solar_best_mw:Q", title="Avg. solar generation", format=",.0f"),
                ],
            )
        )
    layers.append(
        alt.Chart(price_plot).mark_line(point=alt.OverlayMarkDef(filled=True, size=60), strokeWidth=3).encode(
            x=alt.X("hour:O", title="Hour", sort=list(range(24))),
            y=alt.Y("value:Q", title="Average price (€/MWh)"),
            color=alt.Color("series:N", title="Price series", scale=price_color_scale),
            strokeDash=alt.StrokeDash("series:N", title="Price series", scale=dash),
            detail="series:N",
            tooltip=[
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("value:Q", title="Average price", format=",.2f"),
            ],
        )
    )
    chart = alt.layer(*layers).resolve_scale(y="independent").properties(title=f"YTD 24h profile | {year}")
    return apply_chart_style(chart, height=360)

def monthly_economic_curtailment(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, year: int, end_ts: pd.Timestamp) -> pd.DataFrame:
    p = price_hourly[(price_hourly["datetime"].dt.year == year) & (price_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    s = solar_hourly[(solar_hourly["datetime"].dt.year == year) & (solar_hourly["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
    if p.empty or s.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "affected_mwh", "total_mwh", "pct_curtailment"])
    merged = p.merge(s, on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "affected_mwh", "total_mwh", "pct_curtailment"])
    merged["month_num"] = merged["datetime"].dt.month
    merged["month_name"] = merged["datetime"].dt.strftime("%b")
    total = merged.groupby(["month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "total_mwh"})
    affected = merged[merged["price"] <= 0].groupby(["month_num", "month_name"], as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "affected_mwh"})
    out = total.merge(affected, on=["month_num", "month_name"], how="left")
    out["affected_mwh"] = out["affected_mwh"].fillna(0.0)
    out["pct_curtailment"] = out["affected_mwh"] / out["total_mwh"].where(out["total_mwh"] != 0)
    out["year"] = year
    return out[["year", "month_num", "month_name", "affected_mwh", "total_mwh", "pct_curtailment"]]

def monthly_economic_curtailment_forward(forward_scenarios: pd.DataFrame, solar_hourly: pd.DataFrame, end_ts: pd.Timestamp) -> pd.DataFrame:
    if forward_scenarios.empty:
        return pd.DataFrame(columns=["model", "month_num", "month_name", "pct_curtailment"])
    frames = []
    for model in ["Aurora", "Baringa"]:
        f = forward_scenarios[(forward_scenarios["model"] == model) & (forward_scenarios["datetime"].dt.year == 2026) & (forward_scenarios["datetime"] <= end_ts + pd.Timedelta(days=1))].copy()
        if f.empty:
            continue
        tmp = monthly_economic_curtailment(f[["datetime", "price"]], solar_hourly, 2026, end_ts)
        if tmp.empty:
            continue
        tmp["model"] = model
        frames.append(tmp[["model", "month_num", "month_name", "pct_curtailment"]])
    if not frames:
        return pd.DataFrame(columns=["model", "month_num", "month_name", "pct_curtailment"])
    return pd.concat(frames, ignore_index=True)

def monthly_curtailment_chart(actual: pd.DataFrame, forward: pd.DataFrame, year: int):
    if actual.empty and forward.empty:
        return None
    month_order = [calendar.month_abbr[m] for m in range(1, 13)]
    layers = []
    if not actual.empty:
        layers.append(
            alt.Chart(actual).mark_bar(color=YELLOW_DARK, opacity=0.82).encode(
                x=alt.X("month_name:N", title=None, sort=month_order),
                y=alt.Y("pct_curtailment:Q", title="Economic curtailment", axis=alt.Axis(format=".0%")),
                tooltip=[
                    alt.Tooltip("month_name:N", title="Month"),
                    alt.Tooltip("pct_curtailment:Q", title="Actual", format=".1%"),
                ],
            )
        )
    if not forward.empty and year == 2026:
        layers.append(
            alt.Chart(forward).mark_line(point=alt.OverlayMarkDef(filled=True, size=65), strokeWidth=3.2).encode(
                x=alt.X("month_name:N", title=None, sort=month_order),
                y=alt.Y("pct_curtailment:Q", title="Economic curtailment", axis=alt.Axis(format=".0%")),
                color=alt.Color("model:N", title="2026 forecast", scale=alt.Scale(domain=["Aurora", "Baringa"], range=[AURORA_COLOR, BARINGA_COLOR])),
                strokeDash=alt.StrokeDash("model:N", title="2026 forecast", scale=alt.Scale(domain=["Aurora", "Baringa"], range=[[7, 3], [7, 3]])),
                detail="model:N",
                tooltip=[
                    alt.Tooltip("model:N", title="Model"),
                    alt.Tooltip("month_name:N", title="Month"),
                    alt.Tooltip("pct_curtailment:Q", title="Economic curtailment", format=".1%"),
                ],
            )
        )
    chart = alt.layer(*layers).resolve_scale(color="independent", strokeDash="independent").properties(title=f"Monthly economic curtailment | {year}")
    return apply_chart_style(chart, height=330)

# =========================================================
# EEX / FORWARD MARKET CURVES
# =========================================================
def _clean_col_name(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")

def _first_existing(columns: list[str], candidates: list[str]) -> str | None:
    s = set(columns)
    return next((c for c in candidates if c in s), None)

def _contract_sort(value) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    direct = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(direct):
        return pd.Timestamp(direct).normalize()
    up = s.upper().replace("_", " ").replace("-", " ").replace("/", " ")
    q = re.search(r"Q([1-4]).*?(20\d{2})", up) or re.search(r"(20\d{2}).*?Q([1-4])", up)
    if q:
        a, b = q.groups()
        if a.startswith("20"):
            year, quarter = int(a), int(b)
        else:
            quarter, year = int(a), int(b)
        return pd.Timestamp(year, (quarter - 1) * 3 + 1, 1)
    y = re.search(r"(20\d{2})", up)
    if y:
        return pd.Timestamp(int(y.group(1)), 1, 1)
    return pd.NaT

def normalize_forward_market_df(raw: pd.DataFrame) -> pd.DataFrame:
    cols_out = ["as_of_date", "product", "market_area", "load_type", "contract", "contract_sort", "price", "currency", "source", "curve_family"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols_out)
    df = raw.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    columns = df.columns.tolist()
    date_col = _first_existing(columns, ["as_of_date", "trading_day", "trading_date", "trade_date", "business_date", "date", "settlement_date", "data_date", "timestamp"])
    product_col = _first_existing(columns, ["product", "product_name", "instrument", "instrument_name", "contract_name", "name", "commodity"])
    market_col = _first_existing(columns, ["market_area", "market", "area", "country", "zone", "delivery_area", "hub"])
    load_col = _first_existing(columns, ["load_type", "load", "profile", "base_peak", "baseload_peakload", "contract_type"])
    contract_col = _first_existing(columns, ["contract", "delivery_period", "maturity", "maturity_date", "delivery", "delivery_start", "period", "expiry", "expiration", "contract_month", "contract_year"])
    price_col = _first_existing(columns, ["settlement_price", "settlement", "settle", "settle_price", "final_settlement_price", "last_price", "last", "price", "close", "closing_price", "px_last"])
    currency_col = _first_existing(columns, ["currency", "ccy"])
    if contract_col is None or price_col is None:
        return pd.DataFrame(columns=cols_out)
    out = pd.DataFrame()
    out["as_of_date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT
    out["product"] = df[product_col].astype(str).str.strip() if product_col else "Forward"
    out["market_area"] = df[market_col].astype(str).str.strip() if market_col else ""
    out["load_type"] = df[load_col].astype(str).str.strip() if load_col else ""
    out["contract"] = df[contract_col].astype(str).str.strip()
    out["price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["currency"] = df[currency_col].astype(str).str.strip() if currency_col else "EUR/MWh"
    out["source"] = "EEX local"
    combo = (out["product"].fillna("") + " " + out["load_type"].fillna("") + " " + out["contract"].fillna("")).str.lower()
    out["curve_family"] = np.where(combo.str.contains("solar|pv", na=False), "Solar", "Baseload")
    out.loc[out["load_type"].eq("") & combo.str.contains("base|baseload", na=False), "load_type"] = "Baseload"
    out["contract_sort"] = out["contract"].map(_contract_sort)
    out = out.dropna(subset=["price", "contract_sort"]).copy()
    return out[cols_out].sort_values(["as_of_date", "curve_family", "contract_sort"]).reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_forward_market_history() -> pd.DataFrame:
    if EEX_FORWARD_LOCAL_XLSX.exists():
        try:
            return normalize_forward_market_df(pd.read_excel(EEX_FORWARD_LOCAL_XLSX))
        except Exception:
            pass
    if EEX_FORWARD_LOCAL_CSV.exists():
        try:
            return normalize_forward_market_df(pd.read_csv(EEX_FORWARD_LOCAL_CSV))
        except Exception:
            pass
    return pd.DataFrame(columns=["as_of_date", "product", "market_area", "load_type", "contract", "contract_sort", "price", "currency", "source", "curve_family"])

def classify_contracts(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty:
        return pd.DataFrame(columns=["curve_family", "period", "contract", "price"])
    rows = []
    for family, fam in latest.groupby("curve_family"):
        fam = fam.dropna(subset=["contract_sort"]).copy()
        fam["contract_text"] = fam["contract"].astype(str).str.upper()
        quarters = fam[fam["contract_text"].str.contains(r"\bQ[1-4]\b", regex=True)].sort_values("contract_sort")
        years = fam[~fam["contract_text"].str.contains(r"\bQ[1-4]\b", regex=True)].sort_values("contract_sort")
        for idx, (_, row) in enumerate(quarters.head(2).iterrows(), start=1):
            rows.append({"curve_family": family, "period": f"Q+{idx}", "contract": row["contract"], "price": row["price"], "contract_sort": row["contract_sort"]})
        for idx, (_, row) in enumerate(years.head(2).iterrows(), start=1):
            rows.append({"curve_family": family, "period": f"Y+{idx}", "contract": row["contract"], "price": row["price"], "contract_sort": row["contract_sort"]})
    return pd.DataFrame(rows)

def forward_snapshot_and_monthly_change(history: pd.DataFrame) -> pd.DataFrame:
    cols = ["curve_family", "period", "contract", "latest_price", "m_minus_1_price", "monthly_change_pct", "as_of_date"]
    if history.empty or history["as_of_date"].dropna().empty:
        return pd.DataFrame(columns=cols)
    latest_date = history["as_of_date"].dropna().max()
    latest = history[history["as_of_date"] == latest_date].copy()
    latest_classified = classify_contracts(latest)
    month_ago_date = latest_date - pd.DateOffset(months=1)
    previous_candidates = history[history["as_of_date"] <= month_ago_date].copy()
    if previous_candidates.empty:
        previous_date = history["as_of_date"].dropna().min()
    else:
        previous_date = previous_candidates["as_of_date"].max()
    previous = classify_contracts(history[history["as_of_date"] == previous_date].copy())
    out = latest_classified.merge(
        previous[["curve_family", "period", "price"]].rename(columns={"price": "m_minus_1_price"}),
        on=["curve_family", "period"],
        how="left",
    )
    out = out.rename(columns={"price": "latest_price"})
    out["monthly_change_pct"] = (out["latest_price"] / out["m_minus_1_price"] - 1.0)
    out["as_of_date"] = latest_date
    return out[cols].sort_values(["period", "curve_family"]).reset_index(drop=True)

def forward_snapshot_chart(snapshot: pd.DataFrame):
    if snapshot.empty:
        return None
    period_order = ["Q+1", "Q+2", "Y+1", "Y+2"]
    chart = alt.Chart(snapshot).mark_bar().encode(
        x=alt.X("period:N", title=None, sort=period_order),
        xOffset=alt.XOffset("curve_family:N"),
        y=alt.Y("latest_price:Q", title="Latest quote (€/MWh)"),
        color=alt.Color("curve_family:N", title="Curve", scale=alt.Scale(domain=["Baseload", "Solar"], range=[BLUE, YELLOW_DARK])),
        tooltip=[
            alt.Tooltip("curve_family:N", title="Curve"),
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("latest_price:Q", title="Latest quote", format=",.2f"),
            alt.Tooltip("monthly_change_pct:Q", title="M-1 change", format=".1%"),
        ],
    ).properties(title="Latest forward snapshot | Baseload vs Solar")
    return apply_chart_style(chart, height=330)

# =========================================================
# BESS SECTION
# =========================================================
def top_bottom_spread_daily(price_hourly: pd.DataFrame, duration_h: int) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame(columns=["date", "spread"])
    p = price_hourly.copy()
    p["date"] = p["datetime"].dt.normalize()
    rows = []
    for d, g in p.groupby("date"):
        prices = pd.to_numeric(g["price"], errors="coerce").dropna().sort_values()
        if len(prices) < duration_h * 2:
            continue
        bottom = prices.head(duration_h).mean()
        top = prices.tail(duration_h).mean()
        rows.append({"date": d, "spread": float(top - bottom)})
    return pd.DataFrame(rows)

def top_bottom_summary(price_hourly: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> dict[str, float | None]:
    p = price_hourly[(price_hourly["datetime"] >= start_ts) & (price_hourly["datetime"] < end_ts + pd.Timedelta(days=1))].copy()
    out = {}
    for h in [1, 2, 4]:
        daily = top_bottom_spread_daily(p, h)
        out[f"TB{h}"] = None if daily.empty else float(daily["spread"].mean())
    return out

def bess_monthly_proxy(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    cols = ["period", "tb1", "tb2", "tb4", "revenue_wo_demand_eur_mw", "revenue_w_demand_1c_eur_mw", "revenue_w_demand_1_4c_eur_mw"]
    if price_hourly.empty:
        return pd.DataFrame(columns=cols)
    p = price_hourly.copy()
    p["period"] = p["datetime"].dt.to_period("M").dt.to_timestamp()
    solar = solar_hourly.copy()
    rows = []
    rte = 0.85
    for period, g in p.groupby("period"):
        s = solar[(solar["datetime"].dt.to_period("M").dt.to_timestamp() == period)].copy()
        tb = top_bottom_summary(g, period, month_end(period))
        g["date"] = g["datetime"].dt.normalize()
        day_revenues_grid = []
        day_revenues_solar = []
        for d, day in g.groupby("date"):
            day_prices = day[["datetime", "price"]].dropna().sort_values("price")
            if len(day_prices) < 8:
                continue
            charge_grid = day_prices.head(4)["price"].sum() / rte
            discharge = day_prices.tail(4)["price"].sum()
            grid_rev = float(discharge - charge_grid)
            day_revenues_grid.append(grid_rev)
            solar_day = s[s["datetime"].dt.normalize() == d].merge(day[["datetime", "price"]], on="datetime", how="inner")
            solar_day = solar_day[solar_day["solar_best_mw"] > 0].sort_values("price")
            if len(solar_day) >= 4:
                charge_pv = solar_day.head(4)["price"].sum() / rte
                solar_rev = float(discharge - charge_pv)
                day_revenues_solar.append(solar_rev)
        days = max(len(day_revenues_grid), 1)
        grid_1c = float(np.nansum(day_revenues_grid))
        solar_wo = float(np.nansum(day_revenues_solar))
        rows.append({
            "period": period,
            "tb1": tb["TB1"],
            "tb2": tb["TB2"],
            "tb4": tb["TB4"],
            "revenue_wo_demand_eur_mw": solar_wo,
            "revenue_w_demand_1c_eur_mw": grid_1c,
            "revenue_w_demand_1_4c_eur_mw": grid_1c * 1.4,
        })
    return pd.DataFrame(rows, columns=cols).sort_values("period").reset_index(drop=True)

def _load_optional_table(paths: list[Path]) -> pd.DataFrame:
    for path in paths:
        if not path.exists():
            continue
        try:
            raw = pd.read_excel(path) if path.suffix.lower() in {".xlsx", ".xls"} else pd.read_csv(path)
            if not raw.empty:
                return raw
        except Exception:
            continue
    return pd.DataFrame()

def normalize_bess_table(raw: pd.DataFrame) -> pd.DataFrame:
    proxy_cols = ["period", "tb1", "tb2", "tb4", "revenue_wo_demand_eur_mw", "revenue_w_demand_1c_eur_mw", "revenue_w_demand_1_4c_eur_mw"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=proxy_cols)
    df = raw.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    period_col = _first_existing(df.columns.tolist(), ["period", "month", "date", "datetime"])
    if period_col is None:
        return pd.DataFrame(columns=proxy_cols)
    out = pd.DataFrame()
    out["period"] = pd.to_datetime(df[period_col], errors="coerce").dt.to_period("M").dt.to_timestamp()
    mapping = {
        "tb1": ["tb1", "top_bottom_1h", "spread_1h"],
        "tb2": ["tb2", "top_bottom_2h", "spread_2h"],
        "tb4": ["tb4", "top_bottom_4h", "spread_4h"],
        "revenue_wo_demand_eur_mw": ["revenue_wo_demand_eur_mw", "wo_demand_eur_mw", "revenue_without_demand"],
        "revenue_w_demand_1c_eur_mw": ["revenue_w_demand_1c_eur_mw", "w_demand_1c_eur_mw", "revenue_with_demand_1c"],
        "revenue_w_demand_1_4c_eur_mw": ["revenue_w_demand_1_4c_eur_mw", "w_demand_1_4c_eur_mw", "revenue_with_demand_1_4c"],
    }
    for target, candidates in mapping.items():
        col = _first_existing(df.columns.tolist(), candidates)
        out[target] = pd.to_numeric(df[col], errors="coerce") if col else np.nan
    return out.dropna(subset=["period"]).sort_values("period").reset_index(drop=True)

def load_or_build_bess_monthly(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    raw = _load_optional_table(BESS_MONTHLY_CANDIDATES)
    normalized = normalize_bess_table(raw)
    if not normalized.empty:
        return normalized, "file"
    return bess_monthly_proxy(price_hourly, solar_hourly), "proxy"

def bess_summary_table(bess: pd.DataFrame, report_month: pd.Timestamp, report_end: pd.Timestamp) -> pd.DataFrame:
    cols = ["Metric", "Selected month", "YTD", "Annualized", "Previous year"]
    if bess.empty:
        return pd.DataFrame(columns=cols)
    b = bess.copy()
    b["year"] = b["period"].dt.year
    b["month"] = b["period"].dt.month
    selected = b[(b["year"] == report_month.year) & (b["month"] == report_month.month)]
    ytd = b[(b["year"] == report_month.year) & (b["period"] <= report_month)]
    prev = b[b["year"] == report_month.year - 1]
    metrics = [
        ("TB4", "tb4", "eur"),
        ("TB2", "tb2", "eur"),
        ("TB1", "tb1", "eur"),
        ("Revenue w/o demand", "revenue_wo_demand_eur_mw", "rev"),
        ("Revenue w. demand 1.0c", "revenue_w_demand_1c_eur_mw", "rev"),
        ("Revenue w. demand 1.4c", "revenue_w_demand_1_4c_eur_mw", "rev"),
    ]
    days_elapsed = max((report_end - pd.Timestamp(report_month.year, 1, 1)).days + 1, 1)
    rows = []
    for label, col, kind in metrics:
        sel_val = selected[col].mean() if not selected.empty else np.nan
        ytd_val = ytd[col].sum() if kind == "rev" else ytd[col].mean()
        annualized = ytd_val * 365 / days_elapsed if kind == "rev" and pd.notna(ytd_val) else ytd_val
        prev_val = prev[col].sum() if kind == "rev" else prev[col].mean()
        rows.append({
            "Metric": label,
            "Selected month": sel_val,
            "YTD": ytd_val,
            "Annualized": annualized,
            "Previous year": prev_val,
        })
    return pd.DataFrame(rows, columns=cols)

def format_bess_summary(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def _fmt(metric: str, value):
        if pd.isna(value):
            return "—"
        if "Revenue" in metric:
            return fmt_mw_revenue(value)
        return fmt_eur(value)

    display = df.copy()
    for col in ["Selected month", "YTD", "Annualized", "Previous year"]:
        display[col] = display.apply(lambda r: _fmt(r["Metric"], r[col]), axis=1)

    def _row_style(row: pd.Series) -> list[str]:
        metric = str(row.get("Metric", ""))
        if metric.startswith("TB"):
            return ["background-color: #EFF6FF; font-weight: 750;"] * len(row)
        if metric.startswith("Revenue"):
            return ["background-color: #ECFDF5; font-weight: 750;"] * len(row)
        return [""] * len(row)

    return (
        display.style
        .apply(_row_style, axis=1)
        .set_properties(**{"text-align": "center"})
        .set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "6px 8px")]},
        ])
    )

def hybrid_proxy_monthly(monthly_capture: pd.DataFrame, bess: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    cols = ["period", "baseload", "hybrid_wo_demand", "hybrid_w_demand"]
    if monthly_capture.empty or bess.empty or solar_hourly.empty:
        return pd.DataFrame(columns=cols)
    solar = solar_hourly.copy()
    solar["period"] = solar["datetime"].dt.to_period("M").dt.to_timestamp()
    solar_sum = solar.groupby("period", as_index=False)["solar_best_mw"].sum().rename(columns={"solar_best_mw": "solar_mwh"})
    out = monthly_capture.merge(bess, on="period", how="left").merge(solar_sum, on="period", how="left")
    out["baseload"] = out["avg_spot_price"]
    # Dashboard hybrid proxy = baseload + non-negative storage uplift per solar MWh.
    # This keeps the proxy visually and economically interpretable as an uplift above baseload.
    uplift_wo = out["revenue_wo_demand_eur_mw"] / out["solar_mwh"].where(out["solar_mwh"] != 0)
    uplift_w = out["revenue_w_demand_1c_eur_mw"] / out["solar_mwh"].where(out["solar_mwh"] != 0)
    out["hybrid_wo_demand"] = out["baseload"] + uplift_wo.clip(lower=0)
    out["hybrid_w_demand"] = out["baseload"] + uplift_w.clip(lower=0)
    return out[cols].dropna(subset=["period"]).sort_values("period").reset_index(drop=True)

def hybrid_chart(hybrid: pd.DataFrame):
    if hybrid.empty:
        return None
    h = hybrid[hybrid["period"] >= pd.Timestamp(2025, 1, 1)].copy()
    if h.empty:
        return None
    long = h.melt(id_vars=["period"], value_vars=["baseload", "hybrid_wo_demand", "hybrid_w_demand"], var_name="series", value_name="value").dropna(subset=["value"])
    names = {
        "baseload": "Monthly baseload",
        "hybrid_wo_demand": "PV + BESS hybrid, w/o demand",
        "hybrid_w_demand": "PV + BESS hybrid, with demand",
    }
    long["series"] = long["series"].map(names)
    chart = alt.Chart(long).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("period:T", title=None, axis=alt.Axis(format="%b-%y", labelAngle=-35)),
        y=alt.Y("value:Q", title="€/MWh"),
        color=alt.Color("series:N", title="Series", scale=alt.Scale(
            domain=list(names.values()),
            range=[BLUE, YELLOW_DARK, CORP_GREEN],
        )),
        strokeDash=alt.StrokeDash("series:N", title="Series", scale=alt.Scale(
            domain=list(names.values()),
            range=[[1, 0], [6, 3], [3, 2]],
        )),
        tooltip=[
            alt.Tooltip("period:T", title="Month", format="%b %Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="Price", format=",.2f"),
        ],
    ).properties(title="Monthly baseload vs PV + BESS captured price proxy")
    return apply_chart_style(chart, height=340)

# =========================================================
# MAIN LOAD
# =========================================================
st.markdown(
    """
    <div class="report-hero">
      <div class="report-hero-title">📊 Monthly Market Report</div>
      <div class="report-hero-subtitle">Corporate dashboard for Day-Ahead, Forward Market and BESS. The current month is always shown as MTD.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

token = os.getenv("ESIOS_API_TOKEN") or os.getenv("ESIOS_TOKEN") or ""
today = date.today()
live_end = today
live_start = LIVE_START_DATE

with st.spinner("Loading market data and refreshing live 2026 figures..."):
    historical_prices = load_historical_prices()
    historical_solar = load_historical_solar()
    live_prices = load_live_prices(token, live_start, live_end) if token and today >= LIVE_START_DATE else pd.DataFrame(columns=["datetime", "price"])
    live_solar = load_live_solar(token, live_start, live_end) if token and today >= LIVE_START_DATE else pd.DataFrame(columns=["datetime", "solar_best_mw"])
    price_hourly = combine_hist_live(historical_prices, live_prices, ["datetime"])
    solar_hourly = combine_hist_live(historical_solar, live_solar, ["datetime"])
    forward_hourly = load_forward_hourly_scenarios()
    forward_history = load_forward_market_history()

if price_hourly.empty:
    st.error("No hourly price data is available. The report cannot be generated.")
    st.stop()

latest_data_ts = price_hourly["datetime"].max()
current_month_start = pd.Timestamp(today.year, today.month, 1)
min_month = max(pd.Timestamp(2025, 1, 1), price_hourly["datetime"].min().to_period("M").to_timestamp())
max_month = current_month_start
month_starts = pd.date_range(min_month, max_month, freq="MS").tolist()
option_labels = [month_label(m, m.year == today.year and m.month == today.month) for m in month_starts]
selected_label = st.selectbox("📅 Month to display", options=option_labels, index=len(option_labels) - 1)
selected_month = month_starts[option_labels.index(selected_label)]
is_current_mtd = selected_month.year == today.year and selected_month.month == today.month
report_end = month_end(selected_month, today if is_current_mtd else None)
report_end = min(report_end, pd.Timestamp(latest_data_ts.date()))
comparison_2025_end = comparable_ytd_end(report_end, 2025)

pills([
    f"Report month: {selected_label}",
    f"Data cut-off: {report_end:%d %b %Y}",
    "Current month = MTD" if is_current_mtd else "Closed month",
])

if is_current_mtd:
    card_note("The selected month is the current month. All same-month values are explicitly MTD; YTD metrics are calculated through the latest available market day.")
if not token and today.year >= 2026:
    st.info("No ESIOS token was found in the environment, so live 2026 refresh may rely only on files already stored in /data.")

monthly_capture = monthly_capture_table(price_hourly, solar_hourly)

# =========================================================
# SECTION 1 — DAY AHEAD
# =========================================================
section("Day-Ahead Market", "⚡")

subsection("Quick read | selected month, previous month and same month last year")
selected_metrics = period_metrics(price_hourly, solar_hourly, selected_month, report_end)
prev_month = previous_month(selected_month)
prev_metrics = period_metrics(price_hourly, solar_hourly, prev_month, month_end(prev_month))
yoy = yoy_month(selected_month)
yoy_metrics = period_metrics(price_hourly, solar_hourly, yoy, month_end(yoy))

c1, c2, c3 = st.columns(3)
with c1:
    st.metric(
        f"Baseload | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(selected_metrics["avg_price"]),
        help="Simple average of hourly day-ahead prices.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month baseload: <b>{fmt_eur(prev_metrics["avg_price"])}</b><br>Same month LY: <b>{fmt_eur(yoy_metrics["avg_price"])}</b></div>',
        unsafe_allow_html=True,
    )
with c2:
    st.metric(
        f"Previous month | {month_label(prev_month)}",
        fmt_eur(prev_metrics["avg_price"]),
    )
    st.markdown(
        f'<div class="metric-footnote">Solar unc.: <b>{fmt_eur(prev_metrics["captured_uncurtailed"])}</b><br>Solar curt.: <b>{fmt_eur(prev_metrics["captured_curtailed"])}</b></div>',
        unsafe_allow_html=True,
    )
with c3:
    st.metric(
        f"Same month LY | {month_label(yoy)}",
        fmt_eur(yoy_metrics["avg_price"]),
    )
    st.markdown(
        f'<div class="metric-footnote">Solar unc.: <b>{fmt_eur(yoy_metrics["captured_uncurtailed"])}</b><br>Solar curt.: <b>{fmt_eur(yoy_metrics["captured_curtailed"])}</b></div>',
        unsafe_allow_html=True,
    )

c4, c5, c6 = st.columns(3)
with c4:
    st.metric(
        f"Solar captured unc. | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(selected_metrics["captured_uncurtailed"]),
        help="Generation-weighted solar captured price including zero/negative hours.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_eur(prev_metrics["captured_uncurtailed"])}</b><br>Same month LY: <b>{fmt_eur(yoy_metrics["captured_uncurtailed"])}</b></div>',
        unsafe_allow_html=True,
    )
with c5:
    st.metric(
        f"Solar captured curt. | {month_label(selected_month, is_current_mtd)}",
        fmt_eur(selected_metrics["captured_curtailed"]),
        help="Generation-weighted solar captured price excluding zero/negative price hours.",
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_eur(prev_metrics["captured_curtailed"])}</b><br>Same month LY: <b>{fmt_eur(yoy_metrics["captured_curtailed"])}</b></div>',
        unsafe_allow_html=True,
    )
with c6:
    st.metric(
        "Solar capture rate | curtailed",
        fmt_pct(selected_metrics["capture_rate_curtailed"]),
    )
    st.markdown(
        f'<div class="metric-footnote">Prev. month: <b>{fmt_pct(prev_metrics["capture_rate_curtailed"])}</b><br>Same month LY: <b>{fmt_pct(yoy_metrics["capture_rate_curtailed"])}</b></div>',
        unsafe_allow_html=True,
    )

subsection("Monthly baseload vs Solar PV capture table | 2025 history and 2026 YTD")
capture_report = build_report_capture_table(monthly_capture, price_hourly, solar_hourly, forward_hourly, selected_month, report_end)
st.dataframe(format_capture_table(capture_report), use_container_width=True, height=520)

subsection("YTD spot market heatmap | current year vs 2025")
spot_heatmap = ytd_price_heatmap(price_hourly, selected_month.year, report_end)
if spot_heatmap is not None:
    st.altair_chart(spot_heatmap, use_container_width=True)
if selected_month.year == 2026:
    spot_heatmap_2025 = ytd_price_heatmap(price_hourly, 2025, comparison_2025_end)
    if spot_heatmap_2025 is not None:
        st.markdown('<div class="comparison-note">2025 comparable YTD cut-off</div>', unsafe_allow_html=True)
        st.altair_chart(spot_heatmap_2025, use_container_width=True)

subsection("YTD zero / negative price frequency heatmap | current year vs 2025")
neg_heatmap = negative_frequency_heatmap(price_hourly, selected_month.year, report_end)
if neg_heatmap is not None:
    st.altair_chart(neg_heatmap, use_container_width=True)
if selected_month.year == 2026:
    neg_heatmap_2025 = negative_frequency_heatmap(price_hourly, 2025, comparison_2025_end)
    if neg_heatmap_2025 is not None:
        st.markdown('<div class="comparison-note">2025 comparable YTD cut-off</div>', unsafe_allow_html=True)
        st.altair_chart(neg_heatmap_2025, use_container_width=True)

subsection("YTD 24h average market profile vs solar generation")
hourly_overlay = ytd_hourly_overlay(price_hourly, solar_hourly, forward_hourly, selected_month.year, report_end)
if hourly_overlay is not None:
    st.altair_chart(hourly_overlay, use_container_width=True)

subsection("Monthly economic curtailment | actual vs Aurora / Baringa")
actual_curt = monthly_economic_curtailment(price_hourly, solar_hourly, selected_month.year, report_end)
forward_curt = monthly_economic_curtailment_forward(forward_hourly, solar_hourly, report_end) if selected_month.year == 2026 else pd.DataFrame()
curt_chart = monthly_curtailment_chart(actual_curt, forward_curt, selected_month.year)
if curt_chart is not None:
    st.altair_chart(curt_chart, use_container_width=True)
if selected_month.year == 2026:
    actual_curt_2025 = monthly_economic_curtailment(price_hourly, solar_hourly, 2025, comparison_2025_end)
    curt_chart_2025 = monthly_curtailment_chart(actual_curt_2025, pd.DataFrame(), 2025)
    if curt_chart_2025 is not None:
        st.markdown('<div class="comparison-note">2025 comparable YTD actual economic curtailment</div>', unsafe_allow_html=True)
        st.altair_chart(curt_chart_2025, use_container_width=True)

# =========================================================
# SECTION 2 — FORWARD MARKET
# =========================================================
section("Forward Market", "📈")

forward_snapshot = forward_snapshot_and_monthly_change(forward_history)
if forward_snapshot.empty:
    st.info("No normalized EEX/OMIP forward-history file was found in /data. This snapshot only activates when eex_forward_market.xlsx or eex_forward_market.csv is present and normalizable; Aurora/Baringa scenario curves, when available, are used in the forecast comparisons above but do not populate this market snapshot.")
else:
    as_of = pd.to_datetime(forward_snapshot["as_of_date"].iloc[0]).date()
    pills([f"Latest forward quote date: {as_of:%d %b %Y}", "Q+1 / Q+2", "Y+1 / Y+2", "Baseload + Solar"])
    forward_chart = forward_snapshot_chart(forward_snapshot)
    if forward_chart is not None:
        st.altair_chart(forward_chart, use_container_width=True)
    forward_display = forward_snapshot.copy()
    forward_display["M-1 change"] = forward_display["monthly_change_pct"]
    forward_display = forward_display[["curve_family", "period", "contract", "latest_price", "m_minus_1_price", "M-1 change"]].rename(columns={
        "curve_family": "Curve",
        "period": "Bucket",
        "contract": "Contract",
        "latest_price": "Latest quote",
        "m_minus_1_price": "Quote one month ago",
    })
    st.dataframe(
        forward_display.style.format({
            "Latest quote": "{:,.2f}",
            "Quote one month ago": "{:,.2f}",
            "M-1 change": "{:.1%}",
        }, na_rep="—").set_table_styles([
            {"selector": "th", "props": [("background-color", "#475569"), ("color", "white"), ("font-weight", "bold"), ("text-align", "center")]},
        ]),
        use_container_width=True,
    )

# =========================================================
# SECTION 3 — BESS
# =========================================================
section("BESS", "🔋")

bess_monthly, bess_source = load_or_build_bess_monthly(price_hourly, solar_hourly)
if bess_source == "proxy":
    card_note("BESS values are generated with a dashboard proxy when no dedicated BESS monthly input file is found. The proxy uses hourly spot prices, Top/Bottom spreads and a simplified 4h arbitrage logic with RTE = 85%.")
else:
    card_note("BESS values are being read from the dedicated monthly BESS file in /data.")

subsection("Top-Bottom spreads and BESS revenue overview")
bess_summary = bess_summary_table(bess_monthly, selected_month, report_end)
if bess_summary.empty:
    st.info("No BESS metrics are available.")
else:
    st.dataframe(format_bess_summary(bess_summary), use_container_width=True)

subsection("PV + BESS hybrid captured price vs monthly baseload")
hybrid = hybrid_proxy_monthly(monthly_capture, bess_monthly, solar_hourly)
hybrid_plot = hybrid_chart(hybrid)
if hybrid_plot is not None:
    st.altair_chart(hybrid_plot, use_container_width=True)
    if bess_source == "proxy":
        st.caption("Hybrid captured prices are shown as a dashboard proxy: monthly baseload plus a non-negative BESS revenue uplift divided by monthly solar MWh, so the hybrid series stays above baseload by construction.")
else:
    st.info("No hybrid captured-price series could be generated. Add a hybrid monthly file in /data or ensure the solar and BESS inputs are populated.")

st.caption("Monthly Market Report | Corporate dashboard based on the app's hourly market data, forward curve files and BESS monthly inputs/proxies.")
