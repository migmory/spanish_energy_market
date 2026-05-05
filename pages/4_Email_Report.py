from __future__ import annotations

import base64
import os
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

DATA_DIR = BASE_DIR / "data"
HISTORICAL_DIR = BASE_DIR / "historical_data"
OUTPUT_DIR = BASE_DIR / "reports"
OUTPUT_DIR.mkdir(exist_ok=True)

MADRID_TZ = ZoneInfo("Europe/Madrid")

HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_SOLAR_FILE = DATA_DIR / "p48solar_since21.csv"
HIST_WORKBOOK_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
HIST_MIX_FILE = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"
HIST_INSTALLED_CAP_FILE = DATA_DIR / "installed_capacity_monthly.xlsx"
DEMAND_RAW_FILE = HISTORICAL_DIR / "demand_p48_total_10027_raw.csv"

LOGO_CANDIDATES = [
    DATA_DIR / "nexwell-power-.jpg",
    DATA_DIR / "nexwell-power.jpg",
    BASE_DIR / "Data" / "nexwell-power-.jpg",
    BASE_DIR / "Data" / "nexwell-power.jpg",
    Path("/mnt/data/nexwell-power-.jpg"),
]

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027
LIVE_START_DATE = date(2026, 1, 1)

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREY = "#4B5563"
LIGHT_GREY = "#F3F4F6"

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
    "Hydro", "Pumped hydro", "Wind", "Solar PV", "Solar thermal",
    "Other renewables", "Biomass", "Biogas"
}

TECH_ORDER = [
    "CCGT", "Hydro", "Pumped hydro", "Nuclear", "Solar PV", "Solar thermal",
    "Wind", "CHP", "Biomass", "Biogas", "Other renewables", "Coal",
    "Fuel + Gas", "Steam turbine", "Other non-renewables"
]

# =========================================================
# STREAMLIT PAGE
# =========================================================
st.set_page_config(page_title="Monthly YTD Email Report", layout="wide")
st.markdown(
    """
    <style>
    h1 {font-size: 2.0rem !important;}
    h2, h3 {font-size: 1.25rem !important;}
    .metric-card {border:1px solid #e5e7eb; border-radius:14px; padding:14px; background:#f8fafc;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Nexwell Power - Monthly YTD Email Report")

if "email_admin_password" in st.secrets:
    pwd = st.text_input("Password", type="password")
    if pwd != st.secrets["email_admin_password"]:
        st.stop()

# =========================================================
# BASIC HELPERS
# =========================================================
def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def parse_emails(raw: str) -> list[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def fmt_num(x, decimals=2, suffix="") -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.{decimals}f}{suffix}"


def fmt_pct(x, decimals=1) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:.{decimals}%}"


def pct_change(current, previous):
    if previous is None or pd.isna(previous) or previous == 0 or pd.isna(current):
        return pd.NA
    return current / previous - 1


def value_change(current, previous):
    if pd.isna(current) or pd.isna(previous):
        return pd.NA
    return current - previous


def find_logo_path() -> Path | None:
    for p in LOGO_CANDIDATES:
        if p.exists():
            return p
    return None


def image_to_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("utf-8")


def df_to_html_table(df: pd.DataFrame, pct_cols: list[str] | None = None) -> str:
    pct_cols = pct_cols or []
    out = df.copy()
    for c in out.columns:
        if c in pct_cols:
            out[c] = out[c].map(lambda v: "" if pd.isna(v) else f"{v:.1%}")
        elif pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].map(lambda v: "" if pd.isna(v) else f"{v:,.2f}")
    css = """
    <style>
    table.email-table{border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:12px;}
    table.email-table th{background:#4B5563;color:white;border:1px solid #d1d5db;padding:7px;text-align:center;}
    table.email-table td{border:1px solid #e5e7eb;padding:7px;text-align:center;}
    </style>
    """
    return css + out.to_html(index=False, classes="email-table", border=0, escape=False)


def metric_cards_html(items: list[tuple[str, str, str | None]]) -> str:
    cards = []
    for title, value, delta in items:
        delta_html = f'<div style="font-size:12px;color:#475569;margin-top:4px;">{delta}</div>' if delta else ""
        cards.append(f"""
        <div style="flex:1 1 170px;min-width:170px;border:1px solid #e5e7eb;border-radius:12px;padding:12px;background:#f8fafc;">
          <div style="font-size:12px;color:#475569;margin-bottom:5px;">{title}</div>
          <div style="font-size:19px;font-weight:700;color:#111827;">{value}</div>
          {delta_html}
        </div>
        """)
    return '<div style="display:flex;gap:10px;flex-wrap:wrap;">' + ''.join(cards) + '</div>'


def fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

# =========================================================
# LOADERS
# =========================================================
def parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT
    try:
        ts = pd.to_datetime(s, utc=True, errors="raise")
        return ts.tz_convert("Europe/Madrid").tz_localize(None)
    except Exception:
        pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise")
    except Exception:
        return pd.NaT


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
    return df.dropna(subset=["datetime", "price"])[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_historical_solar() -> pd.DataFrame:
    if HIST_WORKBOOK_FILE.exists():
        try:
            df = pd.read_excel(HIST_WORKBOOK_FILE, sheet_name="solar_hourly_best")
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
            if "solar_source" not in df.columns:
                df["solar_source"] = "Historical workbook"
            return df.dropna(subset=["datetime", "solar_best_mw"])[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)
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
    return df.dropna(subset=["datetime", "solar_best_mw"])[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_generation_mix_daily() -> pd.DataFrame:
    if not HIST_MIX_FILE.exists():
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    raw = pd.read_excel(HIST_MIX_FILE, sheet_name="data", header=None)
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
    records = []
    for _, row in raw.iloc[5:19, :].iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        if tech.lower().startswith("generación total") or tech.lower().startswith("generacion total"):
            continue
        vals = pd.to_numeric(row.iloc[1:], errors="coerce")
        for dt, val in zip(dates, vals):
            if pd.notna(dt) and pd.notna(val):
                records.append({"datetime": pd.Timestamp(dt).normalize(), "technology": tech, "energy_mwh": float(val) * 1000.0, "data_source": "Historical file"})
    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=["datetime", "technology", "energy_mwh", "data_source"])
    return out.groupby(["datetime", "technology", "data_source"], as_index=False)["energy_mwh"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_installed_capacity_monthly() -> pd.DataFrame:
    if not HIST_INSTALLED_CAP_FILE.exists():
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])
    raw = pd.read_excel(HIST_INSTALLED_CAP_FILE, sheet_name="data", header=None)
    dates = [parse_mixed_date(v) for v in raw.iloc[4, 1:].tolist()]
    records = []
    for _, row in raw.iloc[5:19, :].iterrows():
        tech_raw = str(row.iloc[0]).strip()
        tech = LOCAL_MIX_TECH_MAP.get(tech_raw, tech_raw)
        for col_idx, dt in enumerate(dates, start=1):
            if pd.isna(dt):
                continue
            val = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.notna(val):
                records.append({"datetime": pd.Timestamp(dt).to_period("M").to_timestamp(), "technology": tech, "capacity_mw": float(val)})
    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])
    return out.groupby(["datetime", "technology"], as_index=False)["capacity_mw"].sum().sort_values(["datetime", "technology"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_demand_raw() -> pd.DataFrame:
    if not DEMAND_RAW_FILE.exists():
        return pd.DataFrame(columns=["datetime", "demand_mwh"])
    df = pd.read_csv(DEMAND_RAW_FILE)
    if "datetime" not in df.columns:
        return pd.DataFrame(columns=["datetime", "demand_mwh"])
    value_col = "value" if "value" in df.columns else ("demand_mw" if "demand_mw" in df.columns else None)
    if value_col is None:
        return pd.DataFrame(columns=["datetime", "demand_mwh"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["datetime", value_col]).sort_values("datetime")
    diffs = df["datetime"].diff().dt.total_seconds().div(3600).dropna()
    interval_h = 0.25 if not diffs.empty and diffs.median() <= 0.30 else 1.0
    df["demand_mwh"] = df[value_col] * interval_h
    return df.groupby(df["datetime"].dt.floor("h"), as_index=False)["demand_mwh"].sum().rename(columns={"datetime": "datetime"})

# =========================================================
# OPTIONAL LIVE API FOR CURRENT YEAR
# =========================================================
def require_esios_token() -> str | None:
    return (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip() or None


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


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
        geo = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo == "españa").any():
            df = df[geo == "españa"].copy()
        elif (geo == "espana").any():
            df = df[geo == "espana"].copy()
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df["datetime"] = dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = source_name
    return df.dropna(subset=["datetime", "value"])[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(indicator_id: int, start_day: date, end_day: date, token: str) -> pd.DataFrame:
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value"])
    frames = []
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
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
        try:
            resp = requests.get(url, headers=build_headers(token), params=params, timeout=(15, 90))
            resp.raise_for_status()
            parsed = parse_esios_indicator(resp.json(), f"esios_{indicator_id}")
            if not parsed.empty:
                frames.append(parsed)
        except Exception:
            pass
        chunk_start = chunk_end + timedelta(days=1)
    if not frames:
        return pd.DataFrame(columns=["datetime", "value"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")


def append_live_price_and_solar(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_day: date, end_day: date, token: str | None):
    if not token or end_day < LIVE_START_DATE:
        return price_hourly, solar_hourly
    live_start = max(start_day, LIVE_START_DATE)
    raw_price = fetch_esios_range(PRICE_INDICATOR_ID, live_start, end_day, token)
    if not raw_price.empty:
        live_price = raw_price.copy()
        live_price["datetime"] = live_price["datetime"].dt.floor("h")
        live_price = live_price.groupby("datetime", as_index=False)["value"].mean().rename(columns={"value": "price"})
        price_hourly = pd.concat([price_hourly[~price_hourly["datetime"].isin(live_price["datetime"])], live_price], ignore_index=True)
    raw_solar = fetch_esios_range(SOLAR_P48_INDICATOR_ID, live_start, end_day, token)
    if not raw_solar.empty:
        live_solar = raw_solar.copy()
        live_solar["datetime"] = live_solar["datetime"].dt.floor("h")
        live_solar = live_solar.groupby("datetime", as_index=False)["value"].mean().rename(columns={"value": "solar_best_mw"})
        live_solar["solar_source"] = "ESIOS P48"
        solar_hourly = pd.concat([solar_hourly[~solar_hourly["datetime"].isin(live_solar["datetime"])], live_solar], ignore_index=True)
    return price_hourly.sort_values("datetime").reset_index(drop=True), solar_hourly.sort_values("datetime").reset_index(drop=True)

# =========================================================
# METRICS
# =========================================================
@dataclass
class PeriodWindow:
    label: str
    year: int
    start: date
    end: date


def period_filter(df: pd.DataFrame, start: date, end: date) -> pd.Series:
    return (df["datetime"].dt.date >= start) & (df["datetime"].dt.date <= end)


def compute_price_solar_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, window: PeriodWindow) -> dict:
    p = price_hourly[period_filter(price_hourly, window.start, window.end)].copy()
    s = solar_hourly[period_filter(solar_hourly, window.start, window.end)].copy()
    merged = p.merge(s[["datetime", "solar_best_mw"]], on="datetime", how="left") if not p.empty else pd.DataFrame()
    if not merged.empty:
        merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
        solar_positive = merged[merged["solar_best_mw"] > 0].copy()
        solar_uncurtailed_mwh = merged["solar_best_mw"].sum()
        uncurtailed_value = (merged["price"] * merged["solar_best_mw"]).sum()
        curtailed_mw = merged["solar_best_mw"].where(merged["price"] > 0, 0.0)
        curtailed_mwh = curtailed_mw.sum()
        curtailed_value = (merged["price"].clip(lower=0) * curtailed_mw).sum()
    else:
        solar_positive = pd.DataFrame()
        solar_uncurtailed_mwh = curtailed_mwh = uncurtailed_value = curtailed_value = 0.0
    avg_price = p["price"].mean() if not p.empty else pd.NA
    captured_unc = uncurtailed_value / solar_uncurtailed_mwh if solar_uncurtailed_mwh > 0 else pd.NA
    captured_cur = curtailed_value / curtailed_mwh if curtailed_mwh > 0 else pd.NA
    return {
        "Period": window.label,
        "Avg spot (€/MWh)": avg_price,
        "Captured solar uncurtailed (€/MWh)": captured_unc,
        "Captured solar curtailed (€/MWh)": captured_cur,
        "Solar generation (GWh)": solar_uncurtailed_mwh / 1000.0,
        "Economic curtailment (GWh)": max(solar_uncurtailed_mwh - curtailed_mwh, 0) / 1000.0,
        "Economic curtailment (%)": (max(solar_uncurtailed_mwh - curtailed_mwh, 0) / solar_uncurtailed_mwh) if solar_uncurtailed_mwh > 0 else pd.NA,
        "Zero / negative hours": int((p["price"] <= 0).sum()) if not p.empty else 0,
        "Zero / negative solar hours": int(((merged["price"] <= 0) & (merged["solar_best_mw"] > 0)).sum()) if not merged.empty else 0,
        "Capture rate uncurtailed (%)": captured_unc / avg_price if pd.notna(avg_price) and avg_price != 0 and pd.notna(captured_unc) else pd.NA,
        "Capture rate curtailed (%)": captured_cur / avg_price if pd.notna(avg_price) and avg_price != 0 and pd.notna(captured_cur) else pd.NA,
        "Solar hours": len(solar_positive),
    }


def build_ytd_metrics_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    cur = compute_price_solar_metrics(price_hourly, solar_hourly, current)
    prev = compute_price_solar_metrics(price_hourly, solar_hourly, previous)
    metrics = [
        ("Avg spot", "Avg spot (€/MWh)", "€/MWh", "diff"),
        ("Captured solar uncurtailed", "Captured solar uncurtailed (€/MWh)", "€/MWh", "diff"),
        ("Captured solar curtailed", "Captured solar curtailed (€/MWh)", "€/MWh", "diff"),
        ("Solar generation", "Solar generation (GWh)", "GWh", "pct"),
        ("Economic curtailment", "Economic curtailment (GWh)", "GWh", "pct"),
        ("Economic curtailment", "Economic curtailment (%)", "%", "diff_pct_pts"),
        ("Zero / negative hours", "Zero / negative hours", "h", "diff"),
        ("Zero / negative solar hours", "Zero / negative solar hours", "h", "diff"),
        ("Capture rate uncurtailed", "Capture rate uncurtailed (%)", "%", "diff_pct_pts"),
        ("Capture rate curtailed", "Capture rate curtailed (%)", "%", "diff_pct_pts"),
    ]
    rows = []
    for label, key, unit, mode in metrics:
        cv, pv = cur.get(key), prev.get(key)
        rows.append({
            "Metric": label,
            f"YTD {current.year}": cv,
            f"YTD {previous.year}": pv,
            "Abs. change": value_change(cv, pv),
            "% change": pct_change(cv, pv) if mode == "pct" else pd.NA,
            "Unit": unit,
        })
    return pd.DataFrame(rows)


def build_monthly_price_solar_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, years: list[int], cutoff_month: int) -> pd.DataFrame:
    rows = []
    for y in years:
        for m in range(1, cutoff_month + 1):
            start = date(y, m, 1)
            end = (pd.Timestamp(start) + pd.offsets.MonthEnd(0)).date()
            metrics = compute_price_solar_metrics(price_hourly, solar_hourly, PeriodWindow(f"{y}-{m:02d}", y, start, end))
            rows.append({
                "Year": y,
                "Month": pd.Timestamp(start).strftime("%b"),
                "Avg spot (€/MWh)": metrics["Avg spot (€/MWh)"],
                "Captured solar curtailed (€/MWh)": metrics["Captured solar curtailed (€/MWh)"],
                "Solar generation (GWh)": metrics["Solar generation (GWh)"],
                "Zero / negative hours": metrics["Zero / negative hours"],
                "Economic curtailment (%)": metrics["Economic curtailment (%)"],
            })
    return pd.DataFrame(rows)


def build_mix_ytd_table(mix_daily: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    if mix_daily.empty:
        return pd.DataFrame()
    rows = []
    for tech in TECH_ORDER:
        cur = mix_daily[(mix_daily["technology"] == tech) & period_filter(mix_daily, current.start, current.end)]["energy_mwh"].sum() / 1000.0
        prev = mix_daily[(mix_daily["technology"] == tech) & period_filter(mix_daily, previous.start, previous.end)]["energy_mwh"].sum() / 1000.0
        if cur == 0 and prev == 0:
            continue
        rows.append({"Technology": tech, f"YTD {current.year} (GWh)": cur, f"YTD {previous.year} (GWh)": prev, "Abs. change (GWh)": cur - prev, "% change": pct_change(cur, prev)})
    out = pd.DataFrame(rows)
    return out.sort_values(f"YTD {current.year} (GWh)", ascending=False).reset_index(drop=True) if not out.empty else out


def build_demand_ytd_table(demand_hourly: pd.DataFrame, current: PeriodWindow, previous: PeriodWindow) -> pd.DataFrame:
    if demand_hourly.empty:
        return pd.DataFrame()
    cur = demand_hourly[period_filter(demand_hourly, current.start, current.end)]["demand_mwh"].sum() / 1000.0
    prev = demand_hourly[period_filter(demand_hourly, previous.start, previous.end)]["demand_mwh"].sum() / 1000.0
    return pd.DataFrame([{"Metric": "Demand", f"YTD {current.year} (GWh)": cur, f"YTD {previous.year} (GWh)": prev, "Abs. change (GWh)": cur - prev, "% change": pct_change(cur, prev)}])


def build_capacity_table(capacity_monthly: pd.DataFrame, current_month: pd.Timestamp, previous_month: pd.Timestamp) -> pd.DataFrame:
    if capacity_monthly.empty:
        return pd.DataFrame()
    cur = capacity_monthly[capacity_monthly["datetime"] <= current_month.to_period("M").to_timestamp()].copy()
    prev = capacity_monthly[capacity_monthly["datetime"] <= previous_month.to_period("M").to_timestamp()].copy()
    if cur.empty or prev.empty:
        return pd.DataFrame()
    cur_latest = cur["datetime"].max()
    prev_latest = prev["datetime"].max()
    rows = []
    for tech in TECH_ORDER:
        cv = cur[(cur["datetime"] == cur_latest) & (cur["technology"] == tech)]["capacity_mw"].sum()
        pv = prev[(prev["datetime"] == prev_latest) & (prev["technology"] == tech)]["capacity_mw"].sum()
        if cv == 0 and pv == 0:
            continue
        rows.append({"Technology": tech, f"Latest {cur_latest.strftime('%b-%Y')} (MW)": cv, f"Latest {prev_latest.strftime('%b-%Y')} (MW)": pv, "Abs. change (MW)": cv - pv, "% change": pct_change(cv, pv)})
    return pd.DataFrame(rows).sort_values(f"Latest {cur_latest.strftime('%b-%Y')} (MW)", ascending=False)

# =========================================================
# CHARTS
# =========================================================
def chart_price_capture(monthly: pd.DataFrame) -> str | None:
    if monthly.empty:
        return None
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for y, grp in monthly.groupby("Year"):
        ax.plot(grp["Month"], grp["Avg spot (€/MWh)"], marker="o", linewidth=2, label=f"Spot {y}")
        ax.plot(grp["Month"], grp["Captured solar curtailed (€/MWh)"], marker="o", linestyle="--", linewidth=2, label=f"Solar capture {y}")
    ax.set_title("Monthly spot price and curtailed solar capture")
    ax.set_ylabel("€/MWh")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)


def chart_zero_negative(monthly: pd.DataFrame) -> str | None:
    if monthly.empty:
        return None
    fig, ax = plt.subplots(figsize=(10, 4.2))
    for y, grp in monthly.groupby("Year"):
        ax.bar([f"{m}\n{y}" for m in grp["Month"]], grp["Zero / negative hours"], label=str(y), alpha=0.85)
    ax.set_title("Zero / negative price hours by month")
    ax.set_ylabel("Hours")
    ax.grid(axis="y", alpha=0.25)
    return fig_to_b64(fig)


def chart_solar_curtailment(monthly: pd.DataFrame) -> str | None:
    if monthly.empty:
        return None
    fig, ax = plt.subplots(figsize=(10, 4.2))
    for y, grp in monthly.groupby("Year"):
        ax.plot(grp["Month"], grp["Economic curtailment (%)"] * 100, marker="o", linewidth=2, label=str(y))
    ax.set_title("Monthly economic curtailment")
    ax.set_ylabel("% of solar hours/volume exposed")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)


def chart_mix(mix_table: pd.DataFrame, current_year: int, previous_year: int) -> str | None:
    if mix_table.empty:
        return None
    cur_col = f"YTD {current_year} (GWh)"
    prev_col = f"YTD {previous_year} (GWh)"
    plot = mix_table.head(10).copy().sort_values(cur_col)
    y = range(len(plot))
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh([i - 0.18 for i in y], plot[prev_col], height=0.35, label=str(previous_year), alpha=0.7)
    ax.barh([i + 0.18 for i in y], plot[cur_col], height=0.35, label=str(current_year), alpha=0.9)
    ax.set_yticks(list(y))
    ax.set_yticklabels(plot["Technology"])
    ax.set_xlabel("GWh")
    ax.set_title("YTD energy mix comparison")
    ax.grid(axis="x", alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    return fig_to_b64(fig)

# =========================================================
# PDF GENERATION
# =========================================================
def reportlab_table_from_df(df: pd.DataFrame, pct_cols: list[str] | None = None, max_rows: int = 30) -> Table:
    pct_cols = pct_cols or []
    tmp = df.head(max_rows).copy()
    for c in tmp.columns:
        if c in pct_cols:
            tmp[c] = tmp[c].map(lambda v: "" if pd.isna(v) else f"{v:.1%}")
        elif pd.api.types.is_numeric_dtype(tmp[c]):
            tmp[c] = tmp[c].map(lambda v: "" if pd.isna(v) else f"{v:,.2f}")
    data = [list(tmp.columns)] + tmp.astype(str).values.tolist()
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(CORP_GREY)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D1D5DB")),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return table


def image_flowable_from_b64(img_b64: str, width_cm: float = 24.0) -> Image:
    raw = base64.b64decode(img_b64)
    bio = BytesIO(raw)
    img = Image(bio)
    img.drawWidth = width_cm * cm
    img.drawHeight = img.imageHeight * img.drawWidth / img.imageWidth
    return img


def build_pdf_report(output_path: Path, logo_path: Path | None, title: str, kpi_df: pd.DataFrame, monthly_table: pd.DataFrame, mix_table: pd.DataFrame, demand_table: pd.DataFrame, capacity_table: pd.DataFrame, charts: dict[str, str | None], current_year: int, previous_year: int) -> bytes:
    doc = SimpleDocTemplate(str(output_path), pagesize=landscape(A4), leftMargin=1.1*cm, rightMargin=1.1*cm, topMargin=1.0*cm, bottomMargin=1.0*cm)
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], fontSize=18, textColor=colors.HexColor(CORP_GREEN_DARK), alignment=TA_CENTER)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=12, textColor=colors.HexColor("#111827"), spaceBefore=8, spaceAfter=6)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=8.5, leading=11)
    story = []
    if logo_path:
        logo = Image(str(logo_path))
        logo.drawWidth = 5.0 * cm
        logo.drawHeight = logo.imageHeight * logo.drawWidth / logo.imageWidth
        story.append(logo)
    story.append(Paragraph(title, h1))
    story.append(Paragraph(f"Monthly Day Ahead report with YTD focus. Comparison: {current_year} YTD versus {previous_year} YTD, same calendar cut-off.", body))
    story.append(Spacer(1, 0.25*cm))
    story.append(Paragraph("Executive YTD KPIs", h2))
    story.append(reportlab_table_from_df(kpi_df, pct_cols=["% change"], max_rows=12))
    for caption, img_b64 in charts.items():
        if img_b64:
            story.append(Spacer(1, 0.2*cm))
            story.append(Paragraph(caption, h2))
            story.append(image_flowable_from_b64(img_b64, width_cm=24.0))
    story.append(PageBreak())
    story.append(Paragraph("Monthly price, solar and curtailment table", h2))
    story.append(reportlab_table_from_df(monthly_table, pct_cols=["Economic curtailment (%)"], max_rows=24))
    story.append(Spacer(1, 0.25*cm))
    story.append(Paragraph("YTD energy mix", h2))
    story.append(reportlab_table_from_df(mix_table, pct_cols=["% change"], max_rows=20) if not mix_table.empty else Paragraph("No energy mix data available.", body))
    story.append(Spacer(1, 0.25*cm))
    story.append(Paragraph("YTD demand", h2))
    story.append(reportlab_table_from_df(demand_table, pct_cols=["% change"], max_rows=5) if not demand_table.empty else Paragraph("No demand data available.", body))
    story.append(Spacer(1, 0.25*cm))
    story.append(Paragraph("Installed capacity", h2))
    story.append(reportlab_table_from_df(capacity_table, pct_cols=["% change"], max_rows=20) if not capacity_table.empty else Paragraph("No installed capacity data available.", body))
    doc.build(story)
    return output_path.read_bytes()

# =========================================================
# EMAIL SENDERS
# =========================================================
def send_via_webhook(to_list: list[str], cc_list: list[str], subject: str, html_body: str, pdf_bytes: bytes, pdf_filename: str) -> tuple[bool, str]:
    url = st.secrets.get("mail_webhook_url", "")
    token = st.secrets.get("mail_webhook_token", "")
    if not url or not token:
        return False, "Webhook secrets not configured."
    payload = {
        "token": token,
        "to": to_list,
        "cc": cc_list,
        "subject": subject,
        "html_body": html_body,
        "attachments": [
            {
                "filename": pdf_filename,
                "content_base64": base64.b64encode(pdf_bytes).decode("utf-8"),
                "mime_type": "application/pdf",
            }
        ],
    }
    resp = requests.post(url, json=payload, timeout=45)
    if 200 <= resp.status_code < 300:
        return True, "Email request sent successfully through webhook."
    return False, f"Webhook returned {resp.status_code}: {resp.text}"


def send_via_smtp(to_list: list[str], cc_list: list[str], subject: str, html_body: str, pdf_bytes: bytes, pdf_filename: str) -> tuple[bool, str]:
    host = st.secrets.get("smtp_host", "")
    user = st.secrets.get("smtp_user", "")
    password = st.secrets.get("smtp_password", "")
    sender = st.secrets.get("smtp_from", user)
    port = int(st.secrets.get("smtp_port", 587))
    if not host or not user or not password or not sender:
        return False, "SMTP secrets not configured."
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.set_content("Please view this email in HTML format. The PDF report is attached.")
    msg.add_alternative(html_body, subtype="html")
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename)
    with smtplib.SMTP(host, port, timeout=45) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)
    return True, "Email sent successfully through SMTP."

# =========================================================
# APP FLOW
# =========================================================
price_hourly = load_historical_prices()
solar_hourly = load_historical_solar()
mix_daily = load_generation_mix_daily()
demand_hourly = load_demand_raw()
capacity_monthly = load_installed_capacity_monthly()

if price_hourly.empty:
    st.error(f"No price data found. Expected file: {HIST_PRICES_FILE}")
    st.stop()

latest_price_day = price_hourly["datetime"].dt.date.max()
default_month = pd.Timestamp(latest_price_day).to_period("M").to_timestamp()

col_a, col_b = st.columns(2)
with col_a:
    to_emails_raw = st.text_area("To", value=st.secrets.get("default_to", ""), height=80, placeholder="name@company.com; another@company.com")
with col_b:
    cc_emails_raw = st.text_area("Cc", value=st.secrets.get("default_cc", ""), height=80)

available_months = sorted(price_hourly["datetime"].dt.to_period("M").astype(str).unique().tolist())
month_str = st.selectbox("Report month", available_months, index=available_months.index(default_month.strftime("%Y-%m")) if default_month.strftime("%Y-%m") in available_months else len(available_months)-1)
report_month = pd.Timestamp(month_str)
report_year = report_month.year
previous_year = report_year - 1
month_end = (report_month + pd.offsets.MonthEnd(0)).date()
latest_in_month = price_hourly[price_hourly["datetime"].dt.to_period("M") == report_month.to_period("M")]["datetime"].dt.date.max()
cutoff_day = min(month_end, latest_in_month) if pd.notna(latest_in_month) else month_end

same_prev_day = min(cutoff_day.day, (pd.Timestamp(date(previous_year, report_month.month, 1)) + pd.offsets.MonthEnd(0)).day)
current_window = PeriodWindow(f"YTD {report_year}", report_year, date(report_year, 1, 1), cutoff_day)
previous_window = PeriodWindow(f"YTD {previous_year}", previous_year, date(previous_year, 1, 1), date(previous_year, report_month.month, same_prev_day))

# Optionally append live values when the selected YTD includes 2026/current live dates.
token = require_esios_token()
price_hourly, solar_hourly = append_live_price_and_solar(price_hourly, solar_hourly, current_window.start, current_window.end, token)

subject = st.text_input("Subject", value=f"Nexwell Power - Monthly Day Ahead YTD report - {report_month.strftime('%b-%Y')}")
intro_text = st.text_area(
    "Intro text",
    value=(
        "Hi all,\n\n"
        f"Please find below the monthly Day Ahead report for {report_month.strftime('%B %Y')}, "
        f"focused on YTD performance versus {previous_year}. The full PDF report is attached.\n"
    ),
    height=110,
)

kpi_df = build_ytd_metrics_table(price_hourly, solar_hourly, current_window, previous_window)
monthly_table = build_monthly_price_solar_table(price_hourly, solar_hourly, [previous_year, report_year], report_month.month)
mix_table = build_mix_ytd_table(mix_daily, current_window, previous_window)
demand_table = build_demand_ytd_table(demand_hourly, current_window, previous_window)
capacity_table = build_capacity_table(capacity_monthly, report_month, pd.Timestamp(date(previous_year, report_month.month, 1)))

price_capture_b64 = chart_price_capture(monthly_table)
zero_neg_b64 = chart_zero_negative(monthly_table)
curtailment_b64 = chart_solar_curtailment(monthly_table)
mix_b64 = chart_mix(mix_table, report_year, previous_year)
charts = {
    "Spot and solar captured price": price_capture_b64,
    "Zero / negative price frequency": zero_neg_b64,
    "Economic curtailment": curtailment_b64,
    "Energy mix": mix_b64,
}

logo_path = find_logo_path()
logo_html = ""
if logo_path:
    logo_html = f'<div style="margin-bottom:12px;"><img src="data:image/jpeg;base64,{image_to_b64(logo_path)}" alt="Nexwell Power" style="width:210px;height:auto;"></div>'

kpi_lookup = {row["Metric"]: row for _, row in kpi_df.iterrows()}
cards = []
for metric, unit, decimals in [
    ("Avg spot", "€/MWh", 2),
    ("Captured solar curtailed", "€/MWh", 2),
    ("Solar generation", "GWh", 1),
    ("Zero / negative hours", "h", 0),
    ("Economic curtailment", "%", 1),
]:
    if metric in kpi_lookup:
        row = kpi_lookup[metric]
        cur_val = row.get(f"YTD {report_year}")
        prev_val = row.get(f"YTD {previous_year}")
        if unit == "%":
            value = fmt_pct(cur_val, decimals)
            delta = f"vs {previous_year}: {fmt_pct(prev_val, decimals)}"
        else:
            value = fmt_num(cur_val, decimals, f" {unit}")
            delta = f"vs {previous_year}: {fmt_num(prev_val, decimals, f' {unit}') }"
        cards.append((metric, value, delta))

chart_html = ""
for title, img_b64 in charts.items():
    if img_b64:
        chart_html += f'<h3>{title}</h3><img src="data:image/png;base64,{img_b64}" style="max-width:100%;height:auto;border:1px solid #ddd;"><br><br>'

email_html = f"""
<html><body style="font-family:Arial,sans-serif;font-size:13px;color:#111827;">
{logo_html}
<p>{intro_text.replace(chr(10), '<br>')}</p>
<p><strong>Cut-off:</strong> {current_window.end.strftime('%d-%b-%Y')}<br>
<strong>Comparison cut-off:</strong> {previous_window.end.strftime('%d-%b-%Y')}</p>
{metric_cards_html(cards)}<br>
<h3>Executive YTD KPIs</h3>{df_to_html_table(kpi_df, pct_cols=['% change'])}<br>
{chart_html}
<h3>Monthly Day Ahead sections</h3>
<p>This email keeps the Day Ahead sections with a YTD lens: spot/captured price, solar generation, negative prices, economic curtailment, energy mix, demand and installed capacity.</p>
<h3>Monthly price, solar and curtailment table</h3>{df_to_html_table(monthly_table, pct_cols=['Economic curtailment (%)'])}<br>
<h3>YTD energy mix</h3>{df_to_html_table(mix_table, pct_cols=['% change']) if not mix_table.empty else '<p>No energy mix data available.</p>'}<br>
<h3>YTD demand</h3>{df_to_html_table(demand_table, pct_cols=['% change']) if not demand_table.empty else '<p>No demand data available.</p>'}<br>
<h3>Installed capacity</h3>{df_to_html_table(capacity_table, pct_cols=['% change']) if not capacity_table.empty else '<p>No installed capacity data available.</p>'}<br>
<p>Best regards,</p>
</body></html>
"""

pdf_filename = f"nexwell_power_monthly_ytd_report_{report_month.strftime('%Y_%m')}.pdf"
pdf_path = OUTPUT_DIR / pdf_filename
pdf_bytes = build_pdf_report(pdf_path, logo_path, subject, kpi_df, monthly_table, mix_table, demand_table, capacity_table, charts, report_year, previous_year)

st.subheader("Preview")
st.markdown(email_html, unsafe_allow_html=True)

st.subheader("PDF report")
st.download_button("Download PDF report", data=pdf_bytes, file_name=pdf_filename, mime="application/pdf")
st.download_button("Download email HTML", data=email_html, file_name=f"email_body_{report_month.strftime('%Y_%m')}.html", mime="text/html")

st.subheader("Recipients")
st.dataframe(pd.DataFrame({"To": [", ".join(parse_emails(to_emails_raw))], "Cc": [", ".join(parse_emails(cc_emails_raw))], "Subject": [subject]}), use_container_width=True)

send_method = st.radio("Send method", ["Webhook", "SMTP"], horizontal=True)
if st.button("Send monthly report now"):
    to_list = parse_emails(to_emails_raw)
    cc_list = parse_emails(cc_emails_raw)
    if not to_list:
        st.error("Add at least one recipient in To.")
    else:
        try:
            if send_method == "Webhook":
                ok, msg = send_via_webhook(to_list, cc_list, subject, email_html, pdf_bytes, pdf_filename)
            else:
                ok, msg = send_via_smtp(to_list, cc_list, subject, email_html, pdf_bytes, pdf_filename)
            if ok:
                st.success(msg)
            else:
                st.error(msg)
        except Exception as exc:
            st.error(f"Send failed: {exc}")

st.info("The report uses the same local Day Ahead data files where available, adds optional ESIOS live data for the selected current-year window, embeds the Nexwell Power logo in the email/PDF, and attaches the generated PDF to the outbound email.")
