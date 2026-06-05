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
#       SOLARPARK_COOKIE='apt.uid=...; apt.sid=...; IFMSCK=...'
#       ESIOS_TOKEN='...'
#       SOLARPARK_CURTAILMENT_SVAR_IDS='b574d090-065e-11f0-980e-42010afa015a,b574a430-065e-11f0-980e-42010afa015a'
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
PRICE_INDICATOR_ID = 600

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
def get_secret_or_env(name: str) -> str:
    value = ""
    try:
        value = str(st.secrets.get(name, "") or "")
    except Exception:
        value = ""
    if not value:
        value = os.getenv(name, "")
    return str(value).strip().strip('"').strip("'")


def get_solarpark_cookie() -> str:
    cookie = get_secret_or_env("SOLARPARK_COOKIE")
    if not cookie:
        box(
            "danger",
            "Missing <code>SOLARPARK_COOKIE</code>. Add the full browser Cookie header in local "
            "<code>.env</code> or Streamlit Secrets, for example: "
            "<code>SOLARPARK_COOKIE='apt.uid=...; apt.sid=...; IFMSCK=...'</code>",
        )
        st.stop()
    return cookie


def build_solarpark_headers(cookie: str, json_request: bool = True) -> dict:
    headers = {
        "Accept": "application/json",
        "Cookie": cookie,
        "Origin": BASE,
        "Referer": f"{BASE}/",
        "User-Agent": "Mozilla/5.0",
    }
    if json_request:
        headers["Content-Type"] = "application/json"
    return headers


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
    headers = build_solarpark_headers(cookie, json_request=True)

    r = requests.post(url, params=params, json=source_ids, headers=headers, timeout=120)
    if not r.ok:
        raise RuntimeError(
            f"Solarpark request failed: HTTP {r.status_code}. URL={r.url}. Body preview={(r.text or '')[:500]}"
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
# Live Day Ahead prices from eSIOS, same logic as Day Ahead tab
# =========================================================
def require_esios_token_optional() -> str:
    """
    Same token structure used by the Day Ahead page:
      local .env:
          ESIOS_TOKEN=...
          or ESIOS_API_TOKEN=...
      Streamlit secrets:
          ESIOS_TOKEN = "..."
          or ESIOS_API_TOKEN = "..."
    """
    token = ""
    try:
        token = str(st.secrets.get("ESIOS_TOKEN", "") or st.secrets.get("ESIOS_API_TOKEN", "") or "")
    except Exception:
        token = ""

    if not token:
        token = os.getenv("ESIOS_TOKEN", "") or os.getenv("ESIOS_API_TOKEN", "")

    return str(token).strip().strip('"').strip("'")


def build_esios_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def parse_esios_datetime_to_madrid_naive(df: pd.DataFrame) -> pd.Series:
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    elif "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    else:
        raise ValueError("eSIOS response has no datetime/datetime_utc column")
    return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)


@st.cache_data(show_spinner=False, ttl=900)
def fetch_esios_day_ahead_qh(token: str, start_day_iso: str, end_day_iso: str) -> pd.DataFrame:
    """
    Fetch indicator 600 exactly for the selected generation window.

    For post Oct-2025 periods we request quarter-hourly values, so revenues are
    matched at QH resolution. If eSIOS returns hourly values, the later QH builder
    expands them to QH.
    """
    start_day = pd.to_datetime(start_day_iso).date()
    end_day = pd.to_datetime(end_day_iso).date()

    url = f"https://api.esios.ree.es/indicators/{PRICE_INDICATOR_ID}"
    frames = []
    chunk_start = start_day
    chunk_days = 14

    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=chunk_days - 1))

        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(chunk_end + timedelta(days=1), tz="Europe/Madrid")

        params = {
            "start_date": start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time_trunc": "quarter_hour",
        }

        last_error = None
        for _ in range(3):
            try:
                r = requests.get(
                    url,
                    headers=build_esios_headers(token),
                    params=params,
                    timeout=(15, 90),
                )
                r.raise_for_status()
                values = r.json().get("indicator", {}).get("values", [])
                if values:
                    df = pd.DataFrame(values)

                    # Keep Spain if the API returns several geographies.
                    if "geo_id" in df.columns and (df["geo_id"].astype(str) == "3").any():
                        df = df[df["geo_id"].astype(str) == "3"].copy()
                    elif "geo_name" in df.columns:
                        geo = df["geo_name"].astype(str).str.strip().str.lower()
                        if (geo == "españa").any():
                            df = df[geo == "españa"].copy()
                        elif (geo == "espana").any():
                            df = df[geo == "espana"].copy()

                    if not df.empty:
                        out = pd.DataFrame()
                        out["datetime_madrid"] = parse_esios_datetime_to_madrid_naive(df)
                        out["price_eur_mwh"] = pd.to_numeric(df["value"], errors="coerce")
                        out = out.dropna(subset=["datetime_madrid", "price_eur_mwh"])
                        frames.append(out)
                last_error = None
                break
            except Exception as exc:
                last_error = exc

        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["datetime_madrid", "price_eur_mwh", "price_source"])

    prices = (
        pd.concat(frames, ignore_index=True)
        .sort_values("datetime_madrid")
        .drop_duplicates("datetime_madrid", keep="last")
        .reset_index(drop=True)
    )
    prices["price_source"] = "eSIOS indicator 600 — Spain"
    return prices[["datetime_madrid", "price_eur_mwh", "price_source"]]



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



def build_qh_price_series_from_rows(prices: pd.DataFrame, source_label: str) -> pd.DataFrame:
    """
    Build QH buckets from raw local-naive Madrid rows.
    Hourly rows are expanded into four QH rows when there are no native QH rows
    for that same hour.
    """
    if prices.empty:
        return pd.DataFrame(columns=["qh_madrid", "hour_madrid", "price_eur_mwh", "price_granularity", "price_source", "month"])

    prices = prices.copy()
    prices["datetime_madrid"] = pd.to_datetime(prices["datetime_madrid"], errors="coerce")
    if getattr(prices["datetime_madrid"].dt, "tz", None) is not None:
        prices["datetime_madrid"] = prices["datetime_madrid"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    prices["datetime_madrid"] = prices["datetime_madrid"].dt.floor("15min")
    prices["price_eur_mwh"] = clean_numeric(prices["price_eur_mwh"])
    prices = (
        prices.dropna(subset=["datetime_madrid", "price_eur_mwh"])
              .sort_values("datetime_madrid")
              .drop_duplicates("datetime_madrid", keep="last")
              .reset_index(drop=True)
    )

    existing = set(prices["datetime_madrid"])
    qh_rows = []

    for _, row in prices.iterrows():
        ts = row["datetime_madrid"]
        price = row["price_eur_mwh"]
        src = row.get("price_source", source_label)

        if ts.minute in (15, 30, 45):
            qh_rows.append({"qh_madrid": ts, "price_eur_mwh": price, "price_granularity": "QH", "price_source": src})
            continue

        qh_candidates = [ts + pd.Timedelta(minutes=m) for m in (15, 30, 45)]
        has_native_qh = any(q in existing for q in qh_candidates)

        if has_native_qh:
            qh_rows.append({"qh_madrid": ts, "price_eur_mwh": price, "price_granularity": "QH", "price_source": src})
        else:
            for m in (0, 15, 30, 45):
                qh_rows.append({
                    "qh_madrid": ts + pd.Timedelta(minutes=m),
                    "price_eur_mwh": price,
                    "price_granularity": "Hourly expanded to QH",
                    "price_source": src,
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
    return qh[["qh_madrid", "hour_madrid", "price_eur_mwh", "price_granularity", "price_source", "month"]]


def load_day_ahead_prices_combined_qh(workbook_path: Path | None, start_day: date, end_day: date) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Load prices as the Day Ahead tab does:
      - historical workbook from /data
      - live 2026 from eSIOS indicator 600 using ESIOS_TOKEN / ESIOS_API_TOKEN

    Returns QH prices for the selected period.
    """
    raw_frames = []
    source_notes = []

    if workbook_path is not None and workbook_path.exists():
        try:
            qh_from_workbook, raw_workbook = load_day_ahead_prices_qh(str(workbook_path))
            if not qh_from_workbook.empty:
                raw_for_merge = qh_from_workbook.rename(columns={"qh_madrid": "datetime_madrid"})[
                    ["datetime_madrid", "price_eur_mwh", "price_source"]
                ].copy()
                raw_frames.append(raw_for_merge)
                source_notes.append(f"workbook {workbook_path.name}")
        except Exception as exc:
            source_notes.append(f"workbook failed: {exc}")

    # Live extraction: if selected range touches 2026 or later, fetch live eSIOS.
    # This mirrors the Day Ahead page structure where historical file is used until
    # 2025 and live extraction is used from 2026.
    if end_day >= date(2026, 1, 1):
        token = require_esios_token_optional()
        if token:
            live_start = max(start_day, date(2026, 1, 1))
            live_prices = fetch_esios_day_ahead_qh(token, live_start.isoformat(), end_day.isoformat())
            if not live_prices.empty:
                raw_frames.append(live_prices)
                source_notes.append("live eSIOS indicator 600")
        else:
            source_notes.append("missing ESIOS_TOKEN / ESIOS_API_TOKEN for live 2026 prices")

    if not raw_frames:
        return (
            pd.DataFrame(columns=["qh_madrid", "hour_madrid", "price_eur_mwh", "price_granularity", "price_source", "month"]),
            pd.DataFrame(),
            "; ".join(source_notes),
        )

    raw = (
        pd.concat(raw_frames, ignore_index=True)
        .sort_values("datetime_madrid")
        .drop_duplicates("datetime_madrid", keep="last")  # live eSIOS overrides workbook if overlap
        .reset_index(drop=True)
    )

    qh = build_qh_price_series_from_rows(raw, "combined Day Ahead price source")
    qh = qh[(qh["qh_madrid"].dt.date >= start_day) & (qh["qh_madrid"].dt.date <= end_day)].copy()
    return qh, raw, "; ".join(source_notes)


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
# Solarpark / UNITY event tracking
# =========================================================
def csv_secret_list(name: str, default: str = "") -> list[str]:
    raw = get_secret_or_env(name) or default
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def parse_ifms_utc(value) -> pd.Timestamp:
    if value is None or value == "":
        return pd.NaT
    txt = str(value).strip()
    # IFMS format usually looks like 20260605T080240.000Z or 20250321T145239Z.
    ts = pd.to_datetime(txt, utc=True, errors="coerce")
    if pd.isna(ts) and txt.endswith("Z"):
        ts = pd.to_datetime(txt.replace("Z", "+00:00"), utc=True, errors="coerce")
    return ts


def solarpark_get_json(url: str, cookie: str, timeout: int = 90) -> dict | list:
    headers = build_solarpark_headers(cookie, json_request=False)
    r = requests.get(url, headers=headers, timeout=timeout)
    if not r.ok:
        raise RuntimeError(
            f"Solarpark GET failed: HTTP {r.status_code}. URL={url}. Body preview={(r.text or '')[:500]}"
        )
    return r.json()



@st.cache_data(show_spinner=False, ttl=900)
def fetch_svar_details(svar_id: str, cookie: str) -> dict:
    url = f"{BASE}/ifms/svars/{svar_id}?lang=en"
    payload = solarpark_get_json(url, cookie)
    return payload if isinstance(payload, dict) else {"payload": payload}


def extract_event_rows(payload, svar_id: str, svar_meta: dict | None = None) -> list[dict]:
    rows = []
    meta = svar_meta or {}

    # Endpoint shapes seen in IFMS can be: list[events], dict{"events": [...]},
    # dict{"data": [...]}, dict{"lastEvent": {...}}, or a single event dict.
    if isinstance(payload, list):
        events = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("events"), list):
            events = payload.get("events")
        elif isinstance(payload.get("data"), list):
            events = payload.get("data")
        elif isinstance(payload.get("items"), list):
            events = payload.get("items")
        elif isinstance(payload.get("content"), list):
            events = payload.get("content")
        elif isinstance(payload.get("lastEvent"), dict):
            events = [payload.get("lastEvent")]
        elif any(k in payload for k in ["date", "startDate", "endDate", "apcode", "severity", "duration"]):
            events = [payload]
        else:
            events = []
    else:
        events = []

    for ev in events:
        if not isinstance(ev, dict):
            continue

        start_raw = ev.get("date") or ev.get("startDate") or ev.get("timestamp") or ev.get("lastEvent")
        end_raw = ev.get("endDate") or ev.get("end_date")
        start_ts_utc = parse_ifms_utc(start_raw)
        end_ts_utc = parse_ifms_utc(end_raw)

        if pd.isna(start_ts_utc):
            continue

        duration_seconds = pd.to_numeric(ev.get("duration"), errors="coerce")
        if pd.isna(duration_seconds):
            if not pd.isna(end_ts_utc):
                duration_seconds = max((end_ts_utc - start_ts_utc).total_seconds(), 0)
            else:
                duration_seconds = np.nan

        rows.append(
            {
                "svar_id": svar_id,
                "svar_name": meta.get("name") or meta.get("apcode") or svar_id,
                "svar_apcode": meta.get("apcode"),
                "agent_id": ((meta.get("agent") or {}).get("id") if isinstance(meta.get("agent"), dict) else None),
                "agent_name": ((meta.get("agent") or {}).get("name") if isinstance(meta.get("agent"), dict) else None),
                "event_id": ev.get("id") or ev.get("eventId"),
                "event_name": ev.get("name") or ev.get("type") or ev.get("apcode"),
                "event_apcode": ev.get("apcode"),
                "severity": ev.get("severity"),
                "quality": ev.get("quality"),
                "ack": ev.get("ack"),
                "start_utc": start_ts_utc,
                "end_utc": end_ts_utc,
                "start_madrid": start_ts_utc.tz_convert(MADRID_TZ).tz_localize(None),
                "end_madrid": (end_ts_utc.tz_convert(MADRID_TZ).tz_localize(None) if not pd.isna(end_ts_utc) else pd.NaT),
                "duration_h": (float(duration_seconds) / 3600.0 if not pd.isna(duration_seconds) else np.nan),
                "uri": ev.get("uri"),
            }
        )

    return rows


@st.cache_data(show_spinner=False, ttl=900)
def fetch_svar_events(svar_id: str, cookie: str) -> tuple[pd.DataFrame, dict]:
    meta = fetch_svar_details(svar_id, cookie)
    events_uri = meta.get("eventsURI") if isinstance(meta, dict) else None
    if not events_uri:
        events_uri = f"{BASE}/ifms/svars/{svar_id}/events?lang=en"

    events_uri = str(events_uri).replace("http://", "https://")
    payload = solarpark_get_json(events_uri, cookie)
    rows = extract_event_rows(payload, svar_id=svar_id, svar_meta=meta if isinstance(meta, dict) else {})
    df = pd.DataFrame(rows)

    info = {
        "svar_id": svar_id,
        "svar_name": meta.get("name") if isinstance(meta, dict) else None,
        "svar_apcode": meta.get("apcode") if isinstance(meta, dict) else None,
        "agent_name": ((meta.get("agent") or {}).get("name") if isinstance(meta, dict) and isinstance(meta.get("agent"), dict) else None),
        "events_uri": events_uri,
        "rows": int(len(df)),
    }
    return df, info


def filter_events_by_madrid_range(events_df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    if events_df.empty:
        return events_df
    out = events_df.copy()
    out["start_madrid"] = pd.to_datetime(out["start_madrid"], errors="coerce")
    start_dt = pd.Timestamp(start_day)
    end_dt = pd.Timestamp(end_day) + pd.Timedelta(days=1)
    out = out[(out["start_madrid"] >= start_dt) & (out["start_madrid"] < end_dt)].copy()
    out["month"] = out["start_madrid"].dt.to_period("M").astype(str)
    return out


def build_events_summary(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()
    return (
        events_df.groupby(["agent_name", "svar_name", "event_apcode", "event_name"], dropna=False, as_index=False)
        .agg(
            events=("event_id", "count"),
            total_duration_h=("duration_h", "sum"),
            first_event=("start_madrid", "min"),
            last_event=("start_madrid", "max"),
        )
        .sort_values(["events", "total_duration_h"], ascending=False)
    )


# =========================================================
# Production price-limit reference line
# =========================================================
def production_price_limit_eur_mwh(ts) -> float:
    """
    Reference price threshold below which the site should not produce.

    Rule supplied by user:
      - 13-16 May 2026 inclusive: -1 €/MWh
      - From 25 May 2026 onwards: -1 €/MWh
      - All other periods: 0.75 €/MWh

    This is shown as reference only. It does not filter or remove generation/revenue data.
    """
    d = pd.Timestamp(ts).date()
    if date(2026, 5, 13) <= d <= date(2026, 5, 16):
        return -1.0
    if d >= date(2026, 5, 25):
        return -1.0
    return 0.75


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
st.caption("Production threshold is shown only when breached; curtailment/control events are diagnostics only; generation/revenue calculations still use all SCADA production.")
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

with st.expander("Solarpark connection", expanded=False):
    _cookie_loaded = bool(get_secret_or_env("SOLARPARK_COOKIE"))
    _event_ids_loaded = bool(get_secret_or_env("SOLARPARK_CURTAILMENT_SVAR_IDS"))
    st.write({
        "SOLARPARK_COOKIE_loaded": _cookie_loaded,
        "SOLARPARK_CURTAILMENT_SVAR_IDS_loaded": _event_ids_loaded,
        "auth_mode": "browser Cookie header only",
    })

run = st.button("Fetch SCADA and calculate revenues", type="primary", use_container_width=True)

if not run:
    box(
        "ok",
        "Ready. Select dates/sites and click <b>Fetch SCADA and calculate revenues</b>. "
        "No generation upload is required.",
    )
    st.stop()

solarpark_cookie = get_solarpark_cookie()

with st.spinner("Fetching Solarpark / UNITY SCADA power values..."):
    try:
        payload = fetch_power_values(start_utc, end_utc, solarpark_cookie, selected_source_ids)
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

# Load and filter prices.
# This mirrors the Day Ahead tab: historical workbook + live eSIOS from 2026.
workbook = find_day_ahead_workbook()
price_qh_all, price_raw, price_source_notes = load_day_ahead_prices_combined_qh(workbook, start_day, end_day)

price_qh = price_qh_all[
    (price_qh_all["qh_madrid"] >= gen15["qh_madrid"].min()) &
    (price_qh_all["qh_madrid"] <= gen15["qh_madrid"].max())
].copy()

section_header("SCADA generation profile")

pill(f"UTC request window: {start_utc} → {end_utc}")
pill(f"SCADA conversion: 10-min kW → 2 × 5-min kWh → QH")
pill(f"Price sources: {price_source_notes}")

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
        "For 2026 the app needs <code>ESIOS_TOKEN</code> or <code>ESIOS_API_TOKEN</code> because the workbook is historical. "
        f"Price source notes: {price_source_notes}",
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
portfolio_qh["price_limit_eur_mwh"] = portfolio_qh["qh_madrid"].apply(production_price_limit_eur_mwh)
portfolio_qh["price_below_limit"] = (
    portfolio_qh["price_eur_mwh"].notna()
    & (portfolio_qh["price_eur_mwh"] < portfolio_qh["price_limit_eur_mwh"])
)
# Only draw the reference threshold when the spot price is below it.
portfolio_qh["price_limit_visible_eur_mwh"] = np.where(
    portfolio_qh["price_below_limit"],
    portfolio_qh["price_limit_eur_mwh"],
    np.nan,
)
portfolio_qh["month"] = portfolio_qh["qh_madrid"].dt.to_period("M").astype(str)
portfolio_qh = portfolio_qh.sort_values("qh_madrid").reset_index(drop=True)
portfolio_qh["monthly_cum_generation_mwh"] = portfolio_qh.groupby("month")["generation_mwh"].cumsum()
portfolio_qh["monthly_cum_revenue_eur"] = portfolio_qh.groupby("month")["revenue_eur"].cumsum()

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
fig_rev.add_trace(
    go.Scatter(
        x=portfolio_qh["qh_madrid"],
        y=portfolio_qh["price_limit_visible_eur_mwh"],
        name="Production threshold when breached",
        yaxis="y2",
        mode="lines",
        line=dict(color=CORP_RED, width=1.8, dash="dash"),
        hovertemplate="%{x|%d-%b %H:%M}<br>Breached threshold: %{y:,.2f} €/MWh<extra></extra>",
    )
)
fig_rev.update_layout(
    **chart_layout(
        "QH SCADA generation, day-ahead price and breached production threshold",
        "Production threshold appears only when spot price is below the configured limit; no production data is filtered or removed",
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

breach_qh = int(portfolio_qh["price_below_limit"].sum()) if "price_below_limit" in portfolio_qh.columns else 0
if breach_qh > 0:
    breached_gen_mwh = portfolio_qh.loc[portfolio_qh["price_below_limit"], "generation_mwh"].sum()
    box(
        "warning",
        f"<b>Production threshold breached in {breach_qh:,.0f} QH intervals.</b> "
        f"Spot price was below the configured threshold; production is still included in all calculations "
        f"({breached_gen_mwh:,.2f} MWh in those intervals)."
    )

section_header("Monthly cumulative production and revenue")
st.caption("Cumulative monthly portfolio production and revenue. Values reset at the start of each month.")

fig_cum = go.Figure()
fig_cum.add_trace(
    go.Scatter(
        x=portfolio_qh["qh_madrid"],
        y=portfolio_qh["monthly_cum_generation_mwh"],
        name="Cumulative production",
        yaxis="y",
        mode="lines",
        line=dict(color=CORP_GREEN, width=2.8),
        hovertemplate="%{x|%d-%b %H:%M}<br>Cum. production: %{y:,.2f} MWh<extra></extra>",
    )
)
fig_cum.add_trace(
    go.Scatter(
        x=portfolio_qh["qh_madrid"],
        y=portfolio_qh["monthly_cum_revenue_eur"],
        name="Cumulative revenue",
        yaxis="y2",
        mode="lines",
        line=dict(color=CORP_BLUE, width=2.8),
        hovertemplate="%{x|%d-%b %H:%M}<br>Cum. revenue: €%{y:,.0f}<extra></extra>",
    )
)
fig_cum.update_layout(
    **chart_layout(
        "Monthly cumulative SCADA production and day-ahead revenue",
        "Cumulative values reset at each month boundary",
    ),
    yaxis=dict(title="Cumulative production (MWh)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8"),
    yaxis2=dict(title="Cumulative revenue (€)", overlaying="y", side="right", showgrid=False),
)
fig_cum.update_xaxes(title="Madrid date and QH", showgrid=False)
st.plotly_chart(fig_cum, use_container_width=True)

section_header("Curtailment / control events tracker")
st.caption(
    "Tracks IFMS svar event histories such as PVCurtailmentStatus or ActivePowerControlAlgorithm. "
    "Set SOLARPARK_CURTAILMENT_SVAR_IDS as a comma-separated list of svar IDs. "
    "Events are diagnostics only; generation and revenue calculations are not modified."
)

default_curtailment_svars = "b574d090-065e-11f0-980e-42010afa015a,b574a430-065e-11f0-980e-42010afa015a"
curtailment_svar_ids = csv_secret_list("SOLARPARK_CURTAILMENT_SVAR_IDS", default=default_curtailment_svars)

ev_frames = []
ev_infos = []
with st.spinner("Fetching Solarpark / UNITY curtailment/control events..."):
    for svar_id in curtailment_svar_ids:
        try:
            ev_df, ev_info = fetch_svar_events(svar_id, solarpark_cookie)
            ev_frames.append(ev_df)
            ev_infos.append(ev_info)
        except Exception as exc:
            ev_infos.append({"svar_id": svar_id, "error": str(exc)[:500], "rows": 0})

events_all = pd.concat([f for f in ev_frames if f is not None and not f.empty], ignore_index=True) if ev_frames else pd.DataFrame()
events_sel = filter_events_by_madrid_range(events_all, start_day, end_day) if not events_all.empty else pd.DataFrame()
if not events_sel.empty and "duration_h" in events_sel.columns:
    # Some IFMS status events return a technical duration that is not a true interval duration.
    # Keep the raw duration in the download, but clean clearly unrealistic values for summaries/charts.
    events_sel["duration_h_raw"] = events_sel["duration_h"]
    events_sel["duration_h"] = pd.to_numeric(events_sel["duration_h"], errors="coerce")
    events_sel.loc[events_sel["duration_h"] > 24 * 366, "duration_h"] = np.nan
events_summary = build_events_summary(events_sel)

with st.expander("Event tracker diagnostics", expanded=False):
    st.json(ev_infos)

if events_sel.empty:
    st.info("No curtailment/control events returned for the selected date range, or the configured svar IDs did not return event history.")
else:
    e1, e2, e3 = st.columns(3)
    e1.metric("Events in range", f"{len(events_sel):,.0f}")
    e2.metric("Total event duration", f"{events_sel['duration_h'].sum(skipna=True):,.2f} h")
    e3.metric("Tracked svars", f"{events_sel['svar_id'].nunique():,.0f}")

    if not events_summary.empty:
        st.dataframe(
            style_table(events_summary).format(
                {
                    "events": "{:,.0f}",
                    "total_duration_h": "{:,.2f}",
                    "first_event": lambda x: "—" if pd.isna(x) else pd.Timestamp(x).strftime("%d-%b-%Y %H:%M"),
                    "last_event": lambda x: "—" if pd.isna(x) else pd.Timestamp(x).strftime("%d-%b-%Y %H:%M"),
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    timeline = events_sel.copy()
    timeline["y_label"] = timeline["agent_name"].fillna("Unknown agent") + " | " + timeline["svar_name"].fillna(timeline["svar_id"])
    timeline["event_apcode"] = timeline["event_apcode"].fillna("Unknown")
    timeline["duration_h_for_size"] = np.maximum(pd.to_numeric(timeline["duration_h"], errors="coerce").fillna(0.05), 0.05)

    fig_ev = go.Figure()
    for event_code, grp in timeline.groupby("event_apcode", dropna=False):
        fig_ev.add_trace(
            go.Scatter(
                x=grp["start_madrid"],
                y=grp["y_label"],
                mode="markers",
                name=str(event_code),
                marker=dict(
                    size=np.clip(grp["duration_h_for_size"] * 2.0 + 7.0, 7.0, 26.0),
                    opacity=0.78,
                    line=dict(width=0.5, color="white"),
                ),
                customdata=np.stack(
                    [
                        grp["event_name"].fillna(""),
                        grp["duration_h"].fillna(np.nan),
                        grp["severity"].fillna(""),
                        grp["quality"].fillna(""),
                        grp["ack"].fillna(""),
                        grp["svar_id"].fillna(""),
                    ],
                    axis=-1,
                ),
                hovertemplate=(
                    "%{x|%d-%b-%Y %H:%M}<br>"
                    "Series: %{y}<br>"
                    "Event: %{customdata[0]}<br>"
                    "Duration: %{customdata[1]:,.2f} h<br>"
                    "Severity: %{customdata[2]}<br>"
                    "Quality: %{customdata[3]}<br>"
                    "Ack: %{customdata[4]}<br>"
                    "Svar: %{customdata[5]}<extra></extra>"
                ),
            )
        )

    fig_ev.update_layout(
        **chart_layout("Curtailment / control events timeline", "Madrid time"),
        height=420,
        margin=dict(l=20, r=20, t=55, b=25),
        yaxis_title=None,
        xaxis_title="Madrid time",
    )
    st.plotly_chart(fig_ev, use_container_width=True)

    st.download_button(
        "Download curtailment/control events",
        data=events_sel.to_csv(index=False).encode("utf-8"),
        file_name="is2_curtailment_control_events.csv",
        mime="text/csv",
    )

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
