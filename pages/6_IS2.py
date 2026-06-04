# is2_solarpark_scada_revenues_qh_corporate.py
# IS2 Solarpark / UNITY SCADA revenues dashboard
#
# Fixes included:
# 1) SCADA generation conversion:
#    Solarpark returns 10-min average power values in kW.
#    We split each 10-min power interval into two 5-min energy blocks:
#       kWh_5min = kW * 5/60
#    Then we sum every three 5-min blocks into 15-min/QH generation.
#    This avoids the artificial saw-tooth profile created by directly resampling 10-min data into 15-min buckets.
#
# 2) Price matching:
#    - Uses the Day Ahead workbook used by the Day Ahead tab:
#          data/hourly_avg_price_since2021.xlsx
#          sheet prices_hourly_avg
#          columns datetime, price
#    - Uses local Madrid naive timestamps internally to avoid DST ambiguity errors.
#    - Supports mixed hourly and quarter-hourly prices:
#          * If prices are hourly, expands each hour into 4 QH buckets.
#          * If prices are already QH, keeps them at QH.
#          * From Oct-2025 onwards, QH prices are expected and used directly if present.
#
# 3) Revenues:
#       revenue_eur = generation_mwh_15min * qh_or_expanded_price_eur_mwh
#
# Required .env / Streamlit secret:
#       SOLARPARK_COOKIE='IFMSCK=...'
#
# Run:
#       streamlit run is2_solarpark_scada_revenues_qh_corporate.py

from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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
BASE = "https://portal.solarpark-online.com"
MADRID_TZ = ZoneInfo("Europe/Madrid")
UTC_TZ = ZoneInfo("UTC")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
ENV_PATH = BASE_DIR / ".env"

if load_dotenv is not None:
    load_dotenv(dotenv_path=ENV_PATH, override=True)
    load_dotenv(override=True)

DAY_AHEAD_PRICE_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"

POWER_SOURCE_IDS = {
    "Carmona Central 36": "d19246ea-065e-11f0-980e-42010afa015a",
    "Carmona Central 36.1": "cc211458-0892-11f0-9eeb-42010afa015a",
    "Palma del Condado Solar 555": "7113ab0e-2726-11f0-b9a2-42010afa015a",
    "Guarroman Solar 81": "e1e421b8-1382-11f0-85ad-42010afa015a",
}
ID_TO_SITE = {v: k for k, v in POWER_SOURCE_IDS.items()}

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_BLUE = "#1D4ED8"
CORP_GREY = "#4B5563"
CORP_BORDER = "#E5E7EB"
CORP_RED = "#DC2626"


# =========================================================
# Page setup
# =========================================================
st.set_page_config(page_title="IS2 SCADA Revenues", page_icon="⚡", layout="wide")

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

        .pill {{
            display:inline-block;
            border-radius:999px;
            padding:6px 11px;
            font-size:0.85rem;
            font-weight:700;
            margin:4px 6px 12px 0;
            background:#EEF2FF;
            border:1px solid #C7D2FE;
            color:#3730A3;
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


def pill(text: str) -> None:
    st.markdown(f'<span class="pill">{text}</span>', unsafe_allow_html=True)


def fmt_eur(x: float) -> str:
    return "—" if pd.isna(x) else f"€{x:,.0f}"


def fmt_mwh(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} MWh"


def fmt_price(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} €/MWh"


def chart_layout(title: str, subtitle: str = "", height: int = 470) -> dict:
    title_text = f"<b>{title}</b><br><sup>{subtitle}</sup>" if subtitle else f"<b>{title}</b>"
    return dict(
        title=dict(text=title_text, x=0.01, xanchor="left"),
        height=height,
        margin=dict(l=60, r=62, t=78, b=52),
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


def norm_col(c: str) -> str:
    return (
        str(c).strip().lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(".", "_")
        .replace("€", "eur")
    )


def find_col(cols, candidates) -> str | None:
    norm = {norm_col(c): c for c in cols}
    for cand in candidates:
        key = norm_col(cand)
        if key in norm:
            return norm[key]
    for c in cols:
        lc = norm_col(c)
        if any(norm_col(cand) in lc for cand in candidates):
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


# =========================================================
# Solarpark API
# =========================================================
def get_cookie() -> str:
    cookie = ""
    try:
        cookie = str(st.secrets.get("SOLARPARK_COOKIE", "") or "")
    except Exception:
        cookie = ""

    if not cookie:
        cookie = os.getenv("SOLARPARK_COOKIE", "")

    cookie = str(cookie).strip().strip('"').strip("'")
    if not cookie:
        box(
            "danger",
            "Missing <code>SOLARPARK_COOKIE</code>. Put it in local <code>.env</code> or Streamlit Secrets. "
            "Example: <code>SOLARPARK_COOKIE='IFMSCK=...'</code>",
        )
        st.stop()
    return cookie


def madrid_day_to_utc_str(d: date, end_of_day: bool = False) -> str:
    dt_local = datetime.combine(d, time(0, 0), tzinfo=MADRID_TZ)
    if end_of_day:
        dt_local = dt_local + timedelta(days=1)
    return dt_local.astimezone(UTC_TZ).strftime("%Y%m%dT%H%M%SZ")


@st.cache_data(show_spinner=True, ttl=900)
def fetch_power_values(start_utc: str, end_utc: str, cookie: str, source_ids: list[str]) -> dict | list:
    url = f"{BASE}/ifms/sources/values/v2"
    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "millis": "false",
        "lang": "en",
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Origin": BASE,
        "Referer": f"{BASE}/",
        "User-Agent": "Mozilla/5.0",
    }

    r = requests.post(url, params=params, json=source_ids, headers=headers, timeout=120)
    if not r.ok:
        raise RuntimeError(
            f"Solarpark request failed: HTTP {r.status_code}. URL={r.url}. Body preview={r.text[:500]}"
        )
    return r.json()


def events_payload_to_power_df(payload: dict | list) -> pd.DataFrame:
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        events = payload[0].get("events", [])
    elif isinstance(payload, dict):
        events = payload.get("events", [])
    else:
        events = []

    if not events:
        return pd.DataFrame(columns=["datetime_utc", "datetime_madrid", "site", "source_id", "power_kw"])

    df = pd.DataFrame(events, columns=["datetime_utc", "value", "source_id"])
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], format="%Y%m%dT%H%M%SZ", utc=True, errors="coerce")
    df["datetime_madrid"] = df["datetime_utc"].dt.tz_convert("Europe/Madrid")
    df["power_kw"] = pd.to_numeric(df["value"], errors="coerce")
    df["site"] = df["source_id"].map(ID_TO_SITE)

    df = df.dropna(subset=["datetime_utc", "datetime_madrid", "site", "power_kw"]).copy()
    df["power_kw"] = df["power_kw"].clip(lower=0)
    return df[["datetime_utc", "datetime_madrid", "site", "source_id", "power_kw"]].sort_values(["site", "datetime_madrid"])


def power_10min_to_generation_15min_via_5min(power_df: pd.DataFrame, timestamp_position: str) -> pd.DataFrame:
    """
    Convert 10-min average power to 15-min generation by splitting every 10-min
    interval into two 5-min energy packets, then summing three packets per QH.

    If timestamp_position == "interval end", the event timestamp is shifted back
    10 minutes before creating the two 5-min packets.
    """
    if power_df.empty:
        return pd.DataFrame(columns=["site", "datetime_madrid", "generation_kwh_15min"])

    tmp = power_df.copy()

    if timestamp_position == "interval end":
        tmp["interval_start"] = tmp["datetime_madrid"] - pd.Timedelta(minutes=10)
    else:
        tmp["interval_start"] = tmp["datetime_madrid"]

    # Split each 10-min kW point into two 5-min energy packets.
    first = tmp[["site", "source_id", "interval_start", "power_kw"]].copy()
    first["datetime_madrid"] = first["interval_start"]
    second = tmp[["site", "source_id", "interval_start", "power_kw"]].copy()
    second["datetime_madrid"] = second["interval_start"] + pd.Timedelta(minutes=5)

    five = pd.concat([first, second], ignore_index=True)
    five["generation_kwh_5min"] = five["power_kw"] * (5.0 / 60.0)

    # Important: sum every three 5-min energy packets into QH.
    gen15 = (
        five.set_index("datetime_madrid")
            .groupby("site")["generation_kwh_5min"]
            .resample("15min", label="left", closed="left")
            .sum()
            .reset_index()
            .rename(columns={"generation_kwh_5min": "generation_kwh_15min"})
    )

    gen15["generation_kwh_15min"] = gen15["generation_kwh_15min"].clip(lower=0)
    gen15 = gen15.sort_values(["site", "datetime_madrid"]).reset_index(drop=True)
    return gen15


def clean_night_generation(gen15: pd.DataFrame, night_start: int, night_end: int, apply_clean: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    if gen15.empty:
        return gen15, pd.DataFrame()

    out = gen15.copy()
    hour_decimal = out["datetime_madrid"].dt.hour + out["datetime_madrid"].dt.minute / 60.0
    is_night = (hour_decimal >= night_start) | (hour_decimal < night_end)
    out["generation_kwh_15min_raw"] = out["generation_kwh_15min"]
    out["night_generation_kwh_raw"] = np.where(is_night, out["generation_kwh_15min_raw"], 0.0)

    if apply_clean:
        out["generation_kwh_15min"] = np.where(is_night, 0.0, out["generation_kwh_15min_raw"])

    out["generation_mwh_15min"] = out["generation_kwh_15min"] / 1000.0
    out["datetime_madrid_naive"] = out["datetime_madrid"].dt.tz_localize(None)
    out["qh_madrid"] = out["datetime_madrid_naive"].dt.floor("15min")
    out["hour_madrid"] = out["datetime_madrid_naive"].dt.floor("h")
    out["month"] = out["datetime_madrid_naive"].dt.to_period("M").astype(str)

    diag = (
        out.groupby("site", as_index=False)
        .agg(
            rows=("generation_kwh_15min", "size"),
            start=("datetime_madrid_naive", "min"),
            end=("datetime_madrid_naive", "max"),
            raw_generation_mwh=("generation_kwh_15min_raw", lambda x: x.sum() / 1000.0),
            clean_generation_mwh=("generation_kwh_15min", lambda x: x.sum() / 1000.0),
            raw_night_generation_mwh=("night_generation_kwh_raw", lambda x: x.sum() / 1000.0),
        )
    )
    diag["raw_night_generation_pct"] = np.where(
        diag["raw_generation_mwh"] > 0,
        diag["raw_night_generation_mwh"] / diag["raw_generation_mwh"] * 100,
        0.0,
    )
    return out, diag


def make_24h_average_profile(gen15: pd.DataFrame) -> pd.DataFrame:
    if gen15.empty:
        return pd.DataFrame(columns=["site", "time_of_day", "hour_decimal", "avg_generation_kwh_15min", "avg_power_kw"])
    tmp = gen15.copy()
    tmp["time_of_day"] = tmp["datetime_madrid_naive"].dt.strftime("%H:%M")
    tmp["hour_decimal"] = tmp["datetime_madrid_naive"].dt.hour + tmp["datetime_madrid_naive"].dt.minute / 60.0
    profile = (
        tmp.groupby(["site", "time_of_day", "hour_decimal"], as_index=False)
            .agg(
                avg_generation_kwh_15min=("generation_kwh_15min", "mean"),
                obs=("generation_kwh_15min", "count"),
            )
            .sort_values(["site", "hour_decimal"])
    )
    profile["avg_power_kw"] = profile["avg_generation_kwh_15min"] / 0.25
    return profile


# =========================================================
# Day Ahead prices: mixed hourly/QH without DST ambiguity
# =========================================================
def find_day_ahead_workbook() -> Path | None:
    candidates = [
        DAY_AHEAD_PRICE_FILE,
        Path.cwd() / "data" / "hourly_avg_price_since2021.xlsx",
        Path.cwd().parent / "data" / "hourly_avg_price_since2021.xlsx",
        Path(__file__).resolve().parent / "data" / "hourly_avg_price_since2021.xlsx",
        Path(__file__).resolve().parent.parent / "data" / "hourly_avg_price_since2021.xlsx",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


@st.cache_data(show_spinner=False)
def load_day_ahead_prices_qh(path_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load Day Ahead prices and produce QH price buckets.

    Internal timestamp is Madrid local naive to avoid DST ambiguous-localization errors
    such as "2021-10-31 02:00:00 is ambiguous".
    """
    path = Path(path_str)
    try:
        raw = pd.read_excel(path, sheet_name="prices_hourly_avg")
    except Exception:
        raw = pd.read_excel(path, sheet_name=0)

    dt_col = find_col(raw.columns, ["datetime", "date", "timestamp", "hour", "datetime_madrid", "fecha_hora"])
    price_col = find_col(raw.columns, ["price", "precio", "value", "eur_mwh", "price_eur_mwh", "€/mwh"])

    if dt_col is None or price_col is None:
        raise ValueError(f"Could not detect datetime/price columns in {path.name}. Columns: {list(raw.columns)}")

    prices = pd.DataFrame()
    prices["datetime_madrid"] = pd.to_datetime(raw[dt_col], errors="coerce")
    prices["price_eur_mwh"] = clean_numeric(raw[price_col])
    prices = prices.dropna(subset=["datetime_madrid", "price_eur_mwh"]).copy()

    # Remove timezone if present after converting to Madrid; otherwise keep local-naive as is.
    if getattr(prices["datetime_madrid"].dt, "tz", None) is not None:
        prices["datetime_madrid"] = prices["datetime_madrid"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    prices["datetime_madrid"] = prices["datetime_madrid"].dt.floor("15min")
    prices = (
        prices.sort_values("datetime_madrid")
              .drop_duplicates("datetime_madrid", keep="last")
              .reset_index(drop=True)
    )

    # Build QH series:
    # - Any row with minute 15/30/45 is already QH.
    # - Hourly rows at minute 00 are expanded to 00/15/30/45 only when those QH rows
    #   are not already present for that hour.
    qh_rows = []
    existing = set(prices["datetime_madrid"])

    for _, row in prices.iterrows():
        ts = row["datetime_madrid"]
        price = row["price_eur_mwh"]

        if ts.minute in (15, 30, 45):
            qh_rows.append({"qh_madrid": ts, "price_eur_mwh": price, "price_granularity": "QH"})
            continue

        # minute 00: if the workbook has QH rows for this hour, keep only the 00 row as QH.
        qh_candidates = [ts + pd.Timedelta(minutes=m) for m in (15, 30, 45)]
        has_qh_inside_hour = any(q in existing for q in qh_candidates)

        if has_qh_inside_hour:
            qh_rows.append({"qh_madrid": ts, "price_eur_mwh": price, "price_granularity": "QH"})
        else:
            for m in (0, 15, 30, 45):
                qh_rows.append({
                    "qh_madrid": ts + pd.Timedelta(minutes=m),
                    "price_eur_mwh": price,
                    "price_granularity": "Hourly expanded to QH",
                })

    qh = (
        pd.DataFrame(qh_rows)
        .dropna(subset=["qh_madrid", "price_eur_mwh"])
        .sort_values("qh_madrid")
        .drop_duplicates("qh_madrid", keep="last")
        .reset_index(drop=True)
    )
    qh["hour_madrid"] = qh["qh_madrid"].dt.floor("h")
    qh["month"] = qh["qh_madrid"].dt.to_period("M").astype(str)
    qh["price_source"] = f"Day Ahead workbook — {path.name}"

    return qh[["qh_madrid", "hour_madrid", "price_eur_mwh", "price_granularity", "price_source", "month"]], prices


def price_sanity(price_qh: pd.DataFrame) -> tuple[str, str]:
    if price_qh.empty:
        return "danger", "No Day Ahead prices found for the selected period."

    p = price_qh["price_eur_mwh"].dropna()
    avg, med, pmin, pmax = p.mean(), p.median(), p.min(), p.max()

    qh_share = (price_qh["price_granularity"].eq("QH").mean() * 100) if "price_granularity" in price_qh.columns else np.nan

    if avg > 200 or med > 180 or pmax > 700 or pmin < -300:
        return (
            "warning",
            f"<b>Price sanity warning:</b> selected-period baseload {avg:.1f} €/MWh, median {med:.1f}, "
            f"min {pmin:.1f}, max {pmax:.1f}. QH-native share: {qh_share:.1f}%. "
            f"Check the Day Ahead workbook values for this period.",
        )

    return (
        "ok",
        f"<b>Price sanity check passed:</b> selected-period baseload {avg:.1f} €/MWh, median {med:.1f}, "
        f"min {pmin:.1f}, max {pmax:.1f}. QH-native share: {qh_share:.1f}%.",
    )


# =========================================================
# Revenues
# =========================================================
def calculate_revenues_qh(gen15: pd.DataFrame, price_qh: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    joined = gen15.merge(
        price_qh[["qh_madrid", "price_eur_mwh", "price_granularity", "price_source"]],
        on="qh_madrid",
        how="left",
    )
    joined["is_priced"] = joined["price_eur_mwh"].notna()
    joined["revenue_eur"] = joined["generation_mwh_15min"] * joined["price_eur_mwh"]
    joined["year"] = joined["qh_madrid"].dt.year

    monthly = (
        joined.groupby(["site", "month"], as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_qh=("is_priced", "sum"),
            total_qh=("is_priced", "size"),
        )
    )
    monthly["missing_price_qh"] = monthly["total_qh"] - monthly["priced_qh"]
    monthly["captured_price_eur_mwh"] = np.where(
        monthly["generation_mwh"] > 0,
        monthly["revenue_eur"] / monthly["generation_mwh"],
        np.nan,
    )

    baseload_month = (
        price_qh.groupby("month", as_index=False)["price_eur_mwh"]
        .mean()
        .rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    )
    monthly = monthly.merge(baseload_month, on="month", how="left")
    monthly["capture_factor_pct"] = monthly["captured_price_eur_mwh"] / monthly["baseload_price_eur_mwh"] * 100

    annual = (
        joined.groupby(["site", "year"], as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_qh=("is_priced", "sum"),
            total_qh=("is_priced", "size"),
        )
    )
    annual["missing_price_qh"] = annual["total_qh"] - annual["priced_qh"]
    annual["captured_price_eur_mwh"] = np.where(
        annual["generation_mwh"] > 0,
        annual["revenue_eur"] / annual["generation_mwh"],
        np.nan,
    )

    price_year = price_qh.copy()
    price_year["year"] = price_year["qh_madrid"].dt.year
    baseload_year = (
        price_year.groupby("year", as_index=False)["price_eur_mwh"]
        .mean()
        .rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    )
    annual = annual.merge(baseload_year, on="year", how="left")
    annual["capture_factor_pct"] = annual["captured_price_eur_mwh"] / annual["baseload_price_eur_mwh"] * 100

    portfolio = (
        joined.groupby("month", as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_qh=("is_priced", "sum"),
            total_qh=("is_priced", "size"),
        )
    )
    portfolio["missing_price_qh"] = portfolio["total_qh"] - portfolio["priced_qh"]
    portfolio["captured_price_eur_mwh"] = np.where(
        portfolio["generation_mwh"] > 0,
        portfolio["revenue_eur"] / portfolio["generation_mwh"],
        np.nan,
    )
    portfolio = portfolio.merge(baseload_month, on="month", how="left")
    portfolio["capture_factor_pct"] = portfolio["captured_price_eur_mwh"] / portfolio["baseload_price_eur_mwh"] * 100

    return joined, monthly, annual, portfolio


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
        "priced_qh": "Priced QH",
        "total_qh": "Total QH",
        "missing_price_qh": "Missing price QH",
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
        "Priced QH": "{:,.0f}",
        "Total QH": "{:,.0f}",
        "Missing price QH": "{:,.0f}",
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

default_start = date(2026, 5, 1)
default_end = date(2026, 5, 31)

start_day = st.sidebar.date_input("Start day", value=default_start)
end_day = st.sidebar.date_input("End day inclusive", value=default_end)

selected_sites = st.sidebar.multiselect(
    "Sites",
    options=list(POWER_SOURCE_IDS.keys()),
    default=list(POWER_SOURCE_IDS.keys()),
)

timestamp_position = st.sidebar.selectbox(
    "SCADA timestamp represents",
    ["interval start", "interval end"],
    index=0,
    help="If the 10-min event timestamp is the end of the interval, select interval end.",
)

st.sidebar.markdown("---")
st.sidebar.subheader("Solar cleaning")
clean_night = st.sidebar.checkbox("Set impossible night generation to zero", value=True)
c1, c2 = st.sidebar.columns(2)
night_start = c1.number_input("Night starts", min_value=18, max_value=24, value=22, step=1)
night_end = c2.number_input("Night ends", min_value=0, max_value=9, value=5, step=1)

show_diagnostics = st.sidebar.checkbox("Show diagnostics", value=True)


# =========================================================
# Main
# =========================================================
st.title("IS2 SCADA generation & day-ahead revenues")
st.caption(
    "SCADA 10-min power is split into 5-min energy packets and aggregated to QH. "
    "Revenues use QH Day Ahead prices when available, otherwise hourly prices are expanded to QH."
)

if end_day < start_day:
    box("danger", "End day must be greater than or equal to start day.")
    st.stop()

if not selected_sites:
    box("warning", "Select at least one site.")
    st.stop()

selected_source_ids = [POWER_SOURCE_IDS[s] for s in selected_sites]
start_utc = madrid_day_to_utc_str(start_day)
end_utc = madrid_day_to_utc_str(end_day, end_of_day=True)

run = st.button("Fetch SCADA and calculate revenues", type="primary", use_container_width=True)

if not run:
    box(
        "ok",
        "Ready. Select dates/sites and click <b>Fetch SCADA and calculate revenues</b>. "
        "No generation upload is required.",
    )
    st.stop()

cookie = get_cookie()

with st.spinner("Fetching Solarpark / UNITY SCADA power values..."):
    try:
        payload = fetch_power_values(start_utc, end_utc, cookie, selected_source_ids)
    except Exception as exc:
        box("danger", f"Could not fetch Solarpark values: {exc}")
        st.stop()

power_df = events_payload_to_power_df(payload)
if power_df.empty:
    box("danger", "No SCADA power events returned for the selected source IDs and period.")
    st.stop()

gen15_raw = power_10min_to_generation_15min_via_5min(power_df, timestamp_position=timestamp_position)
gen15, gen_diag = clean_night_generation(
    gen15_raw,
    night_start=int(night_start),
    night_end=int(night_end),
    apply_clean=clean_night,
)

# Load and filter prices
workbook = find_day_ahead_workbook()
if workbook is None:
    box(
        "danger",
        "Could not find <code>data/hourly_avg_price_since2021.xlsx</code>. "
        "This file is required to use the same price source as the Day Ahead tab.",
    )
    st.stop()

try:
    price_qh_all, price_raw = load_day_ahead_prices_qh(str(workbook))
except Exception as exc:
    box("danger", f"Could not load Day Ahead prices from workbook: {exc}")
    st.stop()

price_qh = price_qh_all[
    (price_qh_all["qh_madrid"] >= gen15["qh_madrid"].min()) &
    (price_qh_all["qh_madrid"] <= gen15["qh_madrid"].max())
].copy()

section_header("SCADA generation profile")

pill(f"UTC request window: {start_utc} → {end_utc}")
pill(f"SCADA conversion: 10-min kW → 2 × 5-min kWh → QH")
pill(f"Day Ahead price file: {workbook.name}")

total_gen = gen15["generation_mwh_15min"].sum()
raw_total = gen_diag["raw_generation_mwh"].sum()
raw_night = gen_diag["raw_night_generation_mwh"].sum()
raw_night_pct = raw_night / raw_total * 100 if raw_total > 0 else 0.0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Clean SCADA generation", fmt_mwh(total_gen))
m2.metric("Raw night generation", fmt_mwh(raw_night), f"{raw_night_pct:.2f}% raw")
m3.metric("Sites", f"{gen15['site'].nunique():,.0f}")
m4.metric("QH intervals", f"{len(gen15):,.0f}")

if raw_night > max(0.005 * raw_total, 0.01):
    box(
        "warning",
        f"<b>Solar sanity warning:</b> raw SCADA data contains {raw_night:,.2f} MWh in the configured night window "
        f"({raw_night_pct:.2f}% of raw generation). Cleaning applied: "
        f"<b>{'night values set to zero' if clean_night else 'night values kept'}</b>.",
    )
else:
    box("ok", "<b>Solar sanity check passed:</b> SCADA night generation is negligible.")

# 15-min generation profile
fig = go.Figure()
for site in selected_sites:
    sub = gen15[gen15["site"] == site]
    fig.add_trace(
        go.Scatter(
            x=sub["datetime_madrid_naive"],
            y=sub["generation_kwh_15min"],
            mode="lines",
            name=site,
            line=dict(width=1.8),
            hovertemplate="%{x|%d-%b %H:%M}<br>%{y:,.1f} kWh/QH<extra>" + site + "</extra>",
        )
    )
fig.update_layout(
    **chart_layout(
        "15-min SCADA generation profile",
        "10-min power split into 5-min energy packets and summed into QH buckets",
        520,
    )
)
fig.update_xaxes(title="Madrid date and QH", showgrid=False)
fig.update_yaxes(title="Generation (kWh / QH)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8")
st.plotly_chart(fig, use_container_width=True)

# Average 24h profile
profile24 = make_24h_average_profile(gen15)
fig_prof = go.Figure()
for site in selected_sites:
    sub = profile24[profile24["site"] == site]
    fig_prof.add_trace(
        go.Scatter(
            x=sub["hour_decimal"],
            y=sub["avg_power_kw"],
            mode="lines",
            name=site,
            line=dict(width=2.2),
            hovertemplate="Hour %{x:.2f}<br>Avg power: %{y:,.0f} kW<extra>" + site + "</extra>",
        )
    )
fig_prof.update_layout(
    **chart_layout(
        "Average 24h generation profile",
        "Equivalent average power across selected days",
        430,
    )
)
fig_prof.update_xaxes(title="Hour of day", range=[0, 24], dtick=2, showgrid=False)
fig_prof.update_yaxes(title="Equivalent average power (kW)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8")
st.plotly_chart(fig_prof, use_container_width=True)


section_header("Day-ahead revenues — operational parks")

if price_qh.empty:
    box(
        "danger",
        "No Day Ahead prices found for the selected generation period. "
        "Check that the workbook contains the selected date range.",
    )
    st.stop()

status, msg = price_sanity(price_qh)
box(status, msg)

revenues_df, monthly, annual, portfolio_month = calculate_revenues_qh(gen15, price_qh)

priced_ratio = revenues_df["is_priced"].mean() * 100 if len(revenues_df) else 0.0
total_revenue = revenues_df["revenue_eur"].sum(skipna=True)
captured_price = total_revenue / total_gen if total_gen > 0 else np.nan
baseload_price = price_qh["price_eur_mwh"].mean()
capture_factor = captured_price / baseload_price * 100 if baseload_price and not pd.isna(captured_price) else np.nan

r1, r2, r3, r4 = st.columns(4)
r1.metric("Portfolio revenue", fmt_eur(total_revenue))
r2.metric("Captured price", fmt_price(captured_price))
r3.metric("Baseload price", fmt_price(baseload_price))
r4.metric("Capture factor", f"{capture_factor:.1f}%" if not pd.isna(capture_factor) else "—", f"{priced_ratio:.1f}% priced QH")

# Revenue overlay at QH
portfolio_qh = (
    revenues_df.groupby("qh_madrid", as_index=False)
    .agg(
        generation_mwh=("generation_mwh_15min", "sum"),
        revenue_eur=("revenue_eur", "sum"),
    )
    .merge(price_qh[["qh_madrid", "price_eur_mwh", "price_granularity"]], on="qh_madrid", how="left")
)

fig_rev = go.Figure()
fig_rev.add_trace(
    go.Bar(
        x=portfolio_qh["qh_madrid"],
        y=portfolio_qh["generation_mwh"],
        name="SCADA generation",
        yaxis="y",
        marker=dict(color=CORP_GREEN),
        opacity=0.82,
        hovertemplate="%{x|%d-%b %H:%M}<br>Generation: %{y:,.3f} MWh/QH<extra></extra>",
    )
)
fig_rev.add_trace(
    go.Scatter(
        x=portfolio_qh["qh_madrid"],
        y=portfolio_qh["price_eur_mwh"],
        name="Day-ahead price",
        yaxis="y2",
        mode="lines",
        line=dict(color=CORP_BLUE, width=2.4),
        hovertemplate="%{x|%d-%b %H:%M}<br>Price: %{y:,.2f} €/MWh<extra></extra>",
    )
)
fig_rev.update_layout(
    **chart_layout(
        "QH SCADA generation and day-ahead price",
        "QH prices used where available; historical hourly prices expanded to QH",
        480,
    )
)
fig_rev.update_layout(
    yaxis=dict(title="SCADA generation (MWh/QH)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8"),
    yaxis2=dict(title="Price (€/MWh)", overlaying="y", side="right", showgrid=False),
    bargap=0.04,
)
fig_rev.update_xaxes(title="Madrid date and QH", showgrid=False)
st.plotly_chart(fig_rev, use_container_width=True)

st.markdown("#### Portfolio monthly summary")
display_revenue_table(portfolio_month.sort_values("month"))

tab_month, tab_annual, tab_detail = st.tabs(["Monthly by site", "Annual by site", "QH revenue detail"])

with tab_month:
    display_revenue_table(monthly.sort_values(["site", "month"]))

with tab_annual:
    display_revenue_table(annual.sort_values(["site", "year"]))

with tab_detail:
    detail = revenues_df[
        [
            "qh_madrid",
            "site",
            "generation_mwh_15min",
            "price_eur_mwh",
            "price_granularity",
            "revenue_eur",
            "is_priced",
        ]
    ].rename(
        columns={
            "qh_madrid": "QH timestamp",
            "site": "Site",
            "generation_mwh_15min": "Generation (MWh/QH)",
            "price_eur_mwh": "Price (€/MWh)",
            "price_granularity": "Price granularity",
            "revenue_eur": "Revenue (€)",
            "is_priced": "Priced",
        }
    )
    st.dataframe(
        style_table(detail.head(1000)).format(
            {
                "Generation (MWh/QH)": "{:,.4f}",
                "Price (€/MWh)": "{:,.2f}",
                "Revenue (€)": "€{:,.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Showing first 1,000 rows. Download complete detail below.")

if show_diagnostics:
    section_header("Diagnostics")

    with st.expander("SCADA raw power sample", expanded=False):
        st.dataframe(power_df.head(100), use_container_width=True, hide_index=True)

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
        pdiag = price_qh["price_eur_mwh"].describe().to_frame("price_eur_mwh").T
        st.dataframe(style_table(pdiag).format("{:,.2f}"), use_container_width=True)
        st.write("Selected-period price granularity:")
        st.dataframe(
            price_qh["price_granularity"].value_counts().rename_axis("granularity").reset_index(name="rows"),
            use_container_width=True,
            hide_index=True,
        )
        st.dataframe(price_qh.head(120), use_container_width=True, hide_index=True)

    with st.expander("Conversion check: 10-min → 5-min → QH", expanded=False):
        check = power_df.copy()
        check["date"] = check["datetime_madrid"].dt.date
        raw_energy = (
            check.assign(generation_kwh_10min=check["power_kw"] * (10.0 / 60.0))
            .groupby(["site", "date"], as_index=False)["generation_kwh_10min"]
            .sum()
        )
        qh_energy = (
            gen15.assign(date=gen15["datetime_madrid_naive"].dt.date)
            .groupby(["site", "date"], as_index=False)["generation_kwh_15min"]
            .sum()
        )
        conv = raw_energy.merge(qh_energy, on=["site", "date"], how="outer")
        conv["diff_kwh"] = conv["generation_kwh_15min"] - conv["generation_kwh_10min"]
        st.dataframe(conv.head(100), use_container_width=True, hide_index=True)

section_header("Downloads")
d1, d2, d3 = st.columns(3)
d1.download_button(
    "Cleaned QH SCADA generation",
    data=gen15.to_csv(index=False).encode("utf-8"),
    file_name="is2_scada_generation_qh_cleaned.csv",
    mime="text/csv",
)
d2.download_button(
    "QH revenue detail",
    data=revenues_df.to_csv(index=False).encode("utf-8"),
    file_name="is2_scada_revenue_detail_qh.csv",
    mime="text/csv",
)
d3.download_button(
    "Monthly revenue metrics",
    data=monthly.to_csv(index=False).encode("utf-8"),
    file_name="is2_scada_monthly_revenue_metrics.csv",
    mime="text/csv",
)
