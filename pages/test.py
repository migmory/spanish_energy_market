from __future__ import annotations

import os
import re
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from time import sleep
from typing import Any
from zoneinfo import ZoneInfo

import altair as alt
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# TEST — PBF - bilaterales: Hueco térmico + precio spot
# =========================================================
#
# Objetivo:
#   Replicar el gráfico "Hueco Térmico y Precio":
#     - Barras naranjas: hueco térmico horario, una barra por hora
#     - Línea negra: precio spot day-ahead horario
#     - Eje Y izquierdo: Hueco Térmico (MWh)
#     - Eje Y derecho: Precio (€/MWh)
#     - Eje X: hora local Madrid
#
# Precio:
#   Usa exactamente la lógica base del Day Ahead:
#     - data/hourly_avg_price_since2021.xlsx, sheet prices_hourly_avg
#     - ESIOS indicator 600 para live/current 2026
#     - timestamps ESIOS: UTC -> Europe/Madrid -> timezone-naive
#
# .env / Streamlit secrets:
#   ESIOS_TOKEN=...
# =========================================================

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="Hueco térmico PBF + REE demand profile", layout="wide")

# Same path convention as the main app pages.
BASE_DIR = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
ENV_PATH = BASE_DIR / ".env"
DATA_DIR = BASE_DIR / "data"
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
LIVE_START_DATE = date(2026, 1, 1)
MADRID_TZ = ZoneInfo("Europe/Madrid")

load_dotenv(dotenv_path=ENV_PATH, override=True)

BASE = "https://api.esios.ree.es"
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

# ---------------------------------------------------------
# IDs
# ---------------------------------------------------------
PRICE_INDICATOR_ID = 600
DEMAND_PBF_ID = 10141

# PBF gross generation.
# IMPORTANT:
# Avoid non-working aggregated IDs such as 10167/10077/10086 where ESIOS returns no rows.
# Use base PBF technology IDs and aggregate manually.
PBF_GROSS_COMPONENTS = {
    "Hydro UGH": 1,
    "Hydro non-UGH": 2,
    "Nuclear": 4,
    "Coal sub-bituminous": 7,
    "Coal anthracite": 8,
    "Combined cycle GT": 9,
    "Fuel": 10,
    "Natural gas": 11,
    "Wind onshore": 12,
    "Wind offshore": 13,
    "Solar PV": 14,
    "Solar thermal": 15,
    "Cogeneration": 17,
    # These may or may not exist depending on ESIOS/range; handled as optional.
    "Other renewables": 10074,
    "Non-renewable waste": 10095,
}

# Bilateral PBF indicators from the info you provided.
PBF_BILATERAL_COMPONENTS = {
    "Hydro UGH": 421,
    "Hydro non-UGH": 422,
    "Nuclear": 424,
    "Coal sub-bituminous": 426,
    "Coal anthracite": 427,
    "Combined cycle GT": 429,
    "Wind onshore": 432,
    "Wind offshore": 433,
    "Other renewables": 10234,
}
PBF_BILATERAL_TOTAL_SALES_ID = 10235

# Aggregations for the thermal gap formula.
NON_THERMAL_TECHS_DEFAULT = [
    "Hydro UGH",
    "Hydro non-UGH",
    "Nuclear",
    "Wind onshore",
    "Wind offshore",
    "Solar PV",
    "Solar thermal",
    "Other renewables",
]

CONVENTIONAL_TECHS_DEFAULT = [
    "Coal sub-bituminous",
    "Coal anthracite",
    "Combined cycle GT",
    "Fuel",
    "Natural gas",
    "Cogeneration",
    "Non-renewable waste",
]


# =========================================================
# Shared helpers copied/adapted from Day Ahead
# =========================================================
def require_esios_token() -> str:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    try:
        token = str(st.secrets.get("ESIOS_TOKEN", "") or st.secrets.get("ESIOS_API_TOKEN", "") or token).strip()
    except Exception:
        pass

    token = token.strip('"').strip("'")
    if not token:
        raise ValueError(f"No token found in {ENV_PATH}. Expected ESIOS_TOKEN.")
    return token


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    """
    Day Ahead convention:
      ESIOS UTC timestamp -> Europe/Madrid -> timezone-naive local timestamp.

    This avoids the browser/Altair applying another timezone conversion.
    """
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

    # Spain / national scope, like Day Ahead.
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


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
    time_trunc: str = "hour",
    time_agg: str | None = None,
) -> pd.DataFrame:
    """
    Day Ahead style chunked fetch using Madrid local date boundaries converted to UTC.
    """
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    url = f"{BASE}/indicators/{indicator_id}"
    frames = []
    chunk_start = start_day
    chunk_days = 31

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
                        **({"time_agg": time_agg} if time_agg else {}),
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
            # Warning only for core data; optional indicators are handled higher up.
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


@st.cache_data(show_spinner=False)
def load_historical_prices() -> pd.DataFrame:
    """
    Exactly the same source as Day Ahead for historical spot prices.
    """
    if not HIST_PRICES_FILE.exists():
        return pd.DataFrame(columns=["datetime", "price"])

    try:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name=0)
        if "price" not in df.columns and "value" in df.columns:
            df = df.rename(columns={"value": "price"})

    if "datetime" not in df.columns or "price" not in df.columns:
        return pd.DataFrame(columns=["datetime", "price"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["datetime", "price"]).copy()
    df["datetime"] = df["datetime"].dt.floor("h")

    df["price_source"] = f"Day Ahead workbook: {HIST_PRICES_FILE.name}"
    return df[["datetime", "price", "price_source"]].sort_values("datetime").reset_index(drop=True)


def maybe_fix_suspicious_price_scale(prices: pd.Series) -> tuple[pd.Series, str]:
    """
    ESIOS indicator 600 should be €/MWh. In some extraction paths it can arrive 10x higher
    than the Day Ahead page profile for the same day. This guard only applies when the
    whole live series is clearly implausible for the selected range.
    """
    clean = pd.to_numeric(prices, errors="coerce")
    if clean.dropna().empty:
        return clean, "empty"

    median_price = clean.median()
    mean_price = clean.mean()
    max_price = clean.max()

    # A March Spanish DA profile with median > 250 €/MWh and max > 400 €/MWh is almost certainly
    # a scale issue for this workflow, not a normal price profile. Divide by 10.
    if median_price > 250 or (mean_price > 180 and max_price > 350):
        return clean / 10.0, "divided_by_10"

    return clean, "unchanged"


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_2026_prices(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token, time_trunc="hour")
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])

    out = raw[["datetime", "value"]].rename(columns={"value": "price"}).copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["datetime", "price"])

    # Price is €/MWh. Average duplicates; never sum prices.
    out = out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")
    out["price"], scale_status = maybe_fix_suspicious_price_scale(out["price"])
    out["price_source"] = f"ESIOS indicator 600 ({scale_status})"
    return out


def load_prices_like_day_ahead(
    token: str,
    start_day: date,
    end_day: date,
    mode: str,
    auto_fix_scale: bool,
) -> pd.DataFrame:
    """
    Load hourly spot prices with Madrid-local naive timestamps.

    The Day Ahead page builds price_hourly from:
      1) historical workbook
      2) ESIOS indicator 600 for 2026 live data

    This test allows choosing priority because the live ESIOS series can occasionally
    arrive at an unexpected scale in this isolated test page.
    """
    hist = load_historical_prices()
    live_start = max(start_day, LIVE_START_DATE)
    live = pd.DataFrame(columns=["datetime", "price", "price_source"])

    if live_start <= end_day and mode != "Day Ahead workbook only":
        live = load_live_2026_prices(token, live_start, end_day)
        if not auto_fix_scale and not live.empty:
            # Re-fetch without applying the scale guard is not needed here; the guard is conservative.
            # This branch only keeps the UI option explicit.
            pass

    if mode == "Day Ahead workbook only":
        combined = hist.copy()
    elif mode == "ESIOS only":
        combined = live.copy()
    elif mode == "ESIOS first, fill gaps with Day Ahead workbook":
        # Keep ESIOS where overlapping.
        combined = pd.concat([hist, live], ignore_index=True)
        combined["_priority"] = combined["price_source"].astype(str).str.contains("ESIOS", case=False, na=False).astype(int)
        combined = combined.sort_values(["datetime", "_priority"]).drop_duplicates("datetime", keep="last")
        combined = combined.drop(columns=["_priority"])
    else:
        # Default: keep workbook where overlapping, use ESIOS only for missing hours.
        combined = pd.concat([live, hist], ignore_index=True)
        combined["_priority"] = combined["price_source"].astype(str).str.contains("Day Ahead workbook", case=False, na=False).astype(int)
        combined = combined.sort_values(["datetime", "_priority"]).drop_duplicates("datetime", keep="last")
        combined = combined.drop(columns=["_priority"])

    if combined.empty:
        return pd.DataFrame(columns=["datetime", "price", "price_source"])

    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce").dt.floor("h")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    combined = combined.dropna(subset=["datetime", "price"])

    mask = (combined["datetime"].dt.date >= start_day) & (combined["datetime"].dt.date <= end_day)
    return combined.loc[mask, ["datetime", "price", "price_source"]].sort_values("datetime").reset_index(drop=True)


# =========================================================
# PBF fetch/calculation
# =========================================================
def fetch_named_indicators(
    indicators: dict[str, int],
    start_day: date,
    end_day: date,
    token: str,
    *,
    warn_missing: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    frames = []
    missing = []

    progress = st.progress(0, text="Fetching ESIOS PBF indicators...")
    items = list(indicators.items())

    for i, (name, indicator_id) in enumerate(items, start=1):
        try:
            raw = fetch_esios_range(indicator_id, start_day, end_day, token, time_trunc="hour", time_agg="sum")
            if raw.empty:
                missing.append(f"{name} ({indicator_id})")
            else:
                temp = raw[["datetime", "value"]].copy()
                temp["datetime"] = pd.to_datetime(temp["datetime"], errors="coerce").dt.floor("h")
                temp["value"] = pd.to_numeric(temp["value"], errors="coerce")
                temp = temp.dropna(subset=["datetime", "value"])

                # Generation/demand are hourly energy/program values. Sum duplicates.
                temp = temp.groupby("datetime", as_index=False)["value"].sum()
                temp["series"] = name
                temp["indicator_id"] = indicator_id
                frames.append(temp)
        except Exception as exc:
            missing.append(f"{name} ({indicator_id}) error: {exc}")

        progress.progress(i / len(items), text=f"Fetched {i}/{len(items)} PBF indicators")

    progress.empty()

    if warn_missing and missing:
        with st.expander("Missing indicators returned by ESIOS", expanded=False):
            st.write(missing)

    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "series", "indicator_id"]), missing

    return pd.concat(frames, ignore_index=True), missing


def build_wide(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    wide = (
        raw.pivot_table(index="datetime", columns="series", values="value", aggfunc="sum")
        .reset_index()
        .sort_values("datetime")
    )
    wide.columns.name = None
    return wide


def apply_bilateral_netting(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    net PBF tech = gross PBF tech - bilateral PBF tech

    If a bilateral indicator returns no data, it is treated as zero.
    """
    out = wide.copy()
    diag = []

    for tech, gross_id in PBF_GROSS_COMPONENTS.items():
        if tech not in out.columns:
            out[tech] = 0.0

        bilat_col_name = f"{tech} bilateral PBF"
        net_col_name = f"{tech} net PBF"

        bilat_indicator_name = f"Programa bilateral PBF {tech}"
        # Actual column names are nicer Spanish labels below; get them from mapping reverse.
        matching_bilat_cols = []
        for bilat_name, bilat_id in BILATERAL_FETCH_NAMES.items():
            if BILATERAL_TO_GROSS_TECH.get(bilat_name) == tech:
                matching_bilat_cols.append(bilat_name)

        for col in matching_bilat_cols:
            if col not in out.columns:
                out[col] = 0.0

        out[bilat_col_name] = out[matching_bilat_cols].sum(axis=1) if matching_bilat_cols else 0.0
        out[net_col_name] = out[tech] - out[bilat_col_name]

        # For the "PBF - bilaterals" view we do not want tiny negative values due to mismatched revisions.
        # Keep the main thermal gap itself unclipped later.
        out[net_col_name] = out[net_col_name].clip(lower=0)

        gross_sum = out[tech].sum()
        bilat_sum = out[bilat_col_name].sum()
        diag.append(
            {
                "technology": tech,
                "gross_id": gross_id,
                "gross_mwh": gross_sum,
                "bilateral_mwh": bilat_sum,
                "net_mwh": out[net_col_name].sum(),
                "bilateral_share_pct": (bilat_sum / gross_sum * 100) if gross_sum else pd.NA,
                "bilateral_columns": ", ".join(matching_bilat_cols) if matching_bilat_cols else "None / assumed 0",
            }
        )

    return out, pd.DataFrame(diag)


def calculate_thermal_gap(
    wide: pd.DataFrame,
    non_thermal_techs: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    netted, diag = apply_bilateral_netting(wide)
    out = netted.copy()

    if "Total scheduled demand PBF" not in out.columns:
        out["Total scheduled demand PBF"] = 0.0

    net_cols = []
    for tech in non_thermal_techs:
        col = f"{tech} net PBF"
        if col not in out.columns:
            out[col] = 0.0
        net_cols.append(col)

    out["non_thermal_net_pbf_mwh"] = out[net_cols].sum(axis=1)
    out["raw_thermal_gap_mwh"] = out["Total scheduled demand PBF"] - out["non_thermal_net_pbf_mwh"]

    out["date_madrid"] = out["datetime"].dt.date
    out["hour_madrid"] = out["datetime"].dt.hour
    return out, diag


def calculate_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["month"] = tmp["datetime"].dt.strftime("%Y-%m")

    rows = []
    for month, g in tmp.groupby("month"):
        avg_price = g["price"].mean() if "price" in g.columns else pd.NA
        gap_sum = g["raw_thermal_gap_mwh"].sum()
        weighted_price = (
            (g["price"] * g["raw_thermal_gap_mwh"]).sum() / gap_sum
            if "price" in g.columns and gap_sum != 0
            else pd.NA
        )

        rows.append(
            {
                "month": month,
                "avg_spot_price_eur_mwh": avg_price,
                "avg_thermal_gap_mwh": g["raw_thermal_gap_mwh"].mean(),
                "max_thermal_gap_mwh": g["raw_thermal_gap_mwh"].max(),
                "min_thermal_gap_mwh": g["raw_thermal_gap_mwh"].min(),
                "thermal_gap_mwh_sum": gap_sum,
                "price_weighted_by_gap_eur_mwh": weighted_price,
                "demand_pbf_mwh": g["Total scheduled demand PBF"].sum(),
                "non_thermal_net_pbf_mwh": g["non_thermal_net_pbf_mwh"].sum(),
                "missing_price_hours": int(g["price"].isna().sum()) if "price" in g.columns else len(g),
            }
        )

    return pd.DataFrame(rows)


# Nice bilingual names for bilaterals. These columns are fetched and mapped back to gross techs.
BILATERAL_FETCH_NAMES = {
    "Programa bilateral PBF Hidráulica UGH": 421,
    "Programa bilateral PBF Hidráulica no UGH": 422,
    "Programa bilateral PBF Nuclear": 424,
    "Programa bilateral PBF Hulla sub-bituminosa": 426,
    "Programa bilateral PBF Hulla antracita": 427,
    "Programa bilateral PBF Ciclo combinado": 429,
    "Programa bilateral PBF Eólica terrestre": 432,
    "Programa bilateral PBF Eólica marina": 433,
    "Programa bilateral PBF Otras renovables": 10234,
}
BILATERAL_TO_GROSS_TECH = {
    "Programa bilateral PBF Hidráulica UGH": "Hydro UGH",
    "Programa bilateral PBF Hidráulica no UGH": "Hydro non-UGH",
    "Programa bilateral PBF Nuclear": "Nuclear",
    "Programa bilateral PBF Hulla sub-bituminosa": "Coal sub-bituminous",
    "Programa bilateral PBF Hulla antracita": "Coal anthracite",
    "Programa bilateral PBF Ciclo combinado": "Combined cycle GT",
    "Programa bilateral PBF Eólica terrestre": "Wind onshore",
    "Programa bilateral PBF Eólica marina": "Wind offshore",
    "Programa bilateral PBF Otras renovables": "Other renewables",
}



# =========================================================
# REE public demand profile helpers
# Adapted from the working Embalses + demand test.
# =========================================================
def safe_json_response(resp: requests.Response) -> dict | list | str:
    try:
        return resp.json()
    except Exception:
        return (resp.text or "")[:2000]


def parse_ree_public_included_series(payload: dict, value_field: str = "value") -> pd.DataFrame:
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


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_ree_demand_evolution_public(
    start_day: date,
    end_day: date,
    time_trunc: str = "hour",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": time_trunc,
        **REE_PENINSULAR_PARAMS,
    }
    url = f"{REE_API_BASE}/demanda/evolucion"
    try:
        resp = requests.get(url, params=params, timeout=60)
    except Exception as exc:
        return pd.DataFrame(), {"http": "ERROR", "url": url, "rows": 0, "error": str(exc)[:500]}

    payload = safe_json_response(resp)
    if not resp.ok or not isinstance(payload, dict):
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}

    df = parse_ree_public_included_series(payload, value_field="value")
    if df.empty:
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}

    if df["title"].nunique() > 1:
        demand_like = df[df["title"].astype(str).str.contains("demanda", case=False, na=False)].copy()
        if not demand_like.empty:
            df = demand_like

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()

    if time_trunc == "hour":
        df["hourly_avg_mw"] = df["value"]
        df["hourly_avg_gw"] = df["hourly_avg_mw"] / 1000.0
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df["weekday"] = df["datetime"].dt.day_name()
        df["is_weekend"] = df["datetime"].dt.weekday >= 5
    elif time_trunc == "month":
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["demand_mwh"] = df["value"]
        df["demand_gwh"] = df["demand_mwh"] / 1000.0
        df["avg_demand_gw"] = df["demand_gwh"] / (df["month"].dt.days_in_month * 24)

    info = {
        "http": resp.status_code,
        "url": resp.url,
        "rows": int(len(df)),
        "title_values": ", ".join(sorted(df["title"].dropna().astype(str).unique().tolist())[:5]),
        "payload_preview": None,
    }
    return df.sort_values("datetime").reset_index(drop=True), info


def public_month_bounds(d: date) -> tuple[date, date]:
    start = date(d.year, d.month, 1)
    end = date(d.year, 12, 31) if d.month == 12 else date(d.year, d.month + 1, 1) - timedelta(days=1)
    return start, end


def public_previous_month_bounds(d: date) -> tuple[date, date]:
    first = date(d.year, d.month, 1)
    return public_month_bounds(first - timedelta(days=1))


def build_demand_monthly_summary(hourly: pd.DataFrame) -> dict[str, Any]:
    summary = {
        "demand_gwh": None,
        "avg_demand_gw": None,
        "max_hourly_gw": None,
        "min_hourly_gw": None,
        "peak_hour": None,
        "days": None,
        "load_factor": None,
    }
    if hourly is not None and not hourly.empty:
        summary["days"] = hourly["date"].nunique()
        summary["avg_demand_gw"] = hourly["hourly_avg_gw"].mean()
        summary["demand_gwh"] = hourly["hourly_avg_gw"].sum()
        summary["max_hourly_gw"] = hourly["hourly_avg_gw"].max()
        summary["min_hourly_gw"] = hourly["hourly_avg_gw"].min()
        idx = hourly["hourly_avg_gw"].idxmax()
        summary["peak_hour"] = hourly.loc[idx, "datetime"]
        if summary["max_hourly_gw"] not in [None, 0]:
            summary["load_factor"] = summary["avg_demand_gw"] / summary["max_hourly_gw"]
    return summary


def build_demand_hourly_profile(hourly: pd.DataFrame, label: str) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "avg_gw", "min_gw", "max_gw", "obs", "label"])
    out = hourly.groupby("hour", as_index=False).agg(
        avg_gw=("hourly_avg_gw", "mean"),
        min_gw=("hourly_avg_gw", "min"),
        max_gw=("hourly_avg_gw", "max"),
        obs=("hourly_avg_gw", "count"),
    )
    out["label"] = label
    return out


def build_demand_daily_avg_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "avg_gw", "max_gw", "min_gw"])
    return hourly.groupby("date", as_index=False).agg(
        avg_gw=("hourly_avg_gw", "mean"),
        max_gw=("hourly_avg_gw", "max"),
        min_gw=("hourly_avg_gw", "min"),
    )


def build_demand_weekday_hourly_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "day_type", "avg_gw", "obs"])
    tmp = hourly.copy()
    tmp["day_type"] = tmp["is_weekend"].map({True: "Weekend", False: "Weekday"})
    return tmp.groupby(["day_type", "hour"], as_index=False).agg(
        avg_gw=("hourly_avg_gw", "mean"),
        obs=("hourly_avg_gw", "count"),
    )


def pct_delta(cur: Any, prev: Any) -> float | None:
    if cur is None or prev in [None, 0] or pd.isna(cur) or pd.isna(prev):
        return None
    return float(cur) / float(prev) - 1


def delta_text(value: float | None, suffix: str = "", decimals: int = 1, good_when_up: bool = True) -> str:
    if value is None or pd.isna(value):
        return "→ n/a"
    positive = value >= 0
    arrow = "↑" if positive else "↓"
    return f"{arrow} {value:+,.{decimals}f}{suffix}"


def padded_zero_domain(values: pd.Series, pad: float = 0.08) -> list[float]:
    """Return a y-domain that includes zero and adds light padding."""
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return [-1.0, 1.0]
    vmin = min(float(clean.min()), 0.0)
    vmax = max(float(clean.max()), 0.0)
    if abs(vmax - vmin) < 1e-9:
        span = max(abs(vmax), 1.0)
        return [-span, span]
    span = vmax - vmin
    return [vmin - span * pad, vmax + span * pad]


def align_second_axis_zero_domain(primary_domain: list[float], secondary_values: pd.Series, pad: float = 0.08) -> list[float]:
    """
    Build a secondary-axis domain whose zero is at the same relative height as
    zero in primary_domain. This keeps both Y axes visually zero-aligned.
    """
    clean = pd.to_numeric(secondary_values, errors="coerce").dropna()
    if clean.empty:
        return [-1.0, 1.0]

    left_min, left_max = float(primary_domain[0]), float(primary_domain[1])
    if abs(left_max - left_min) < 1e-9 or not (left_min < 0 < left_max):
        return padded_zero_domain(clean, pad=pad)

    zero_frac = (0.0 - left_min) / (left_max - left_min)
    if not (0.0 < zero_frac < 1.0):
        return padded_zero_domain(clean, pad=pad)

    pmin = min(float(clean.min()), 0.0)
    pmax = max(float(clean.max()), 0.0)
    pspan = max(pmax - pmin, 1.0)
    pmin -= pspan * pad
    pmax += pspan * pad

    required_range = max(
        pmax / (1.0 - zero_frac) if pmax > 0 else 0.0,
        (-pmin) / zero_frac if pmin < 0 else 0.0,
        1.0,
    )
    return [-zero_frac * required_range, (1.0 - zero_frac) * required_range]


# =========================================================
# UI
# =========================================================
st.title("Thermal Gap and Price + REE Demand Profile")
st.caption(
    "PBF minus bilateral schedules. Prices are loaded with the same logic as the Day Ahead page. "
    "Everything is displayed in Madrid local time."
)

with st.expander("Indicator IDs", expanded=False):
    st.markdown("**Core**")
    st.write({"Spot price": PRICE_INDICATOR_ID, "Demand PBF": DEMAND_PBF_ID})
    st.markdown("**Gross PBF components**")
    st.json(PBF_GROSS_COMPONENTS)
    st.markdown("**Bilateral PBF components**")
    st.json(BILATERAL_FETCH_NAMES)

col1, col2 = st.columns(2)
with col1:
    start_day = st.date_input("Start day", value=date(2026, 3, 1))
with col2:
    end_day = st.date_input("End day inclusive", value=date(2026, 3, 13))

non_thermal_techs = st.multiselect(
    "Non-thermal net PBF components deducted from demand",
    options=list(PBF_GROSS_COMPONENTS.keys()),
    default=NON_THERMAL_TECHS_DEFAULT,
)

show_diagnostics = st.checkbox("Show diagnostics and extra tables", value=False)
align_zero_axes = st.checkbox(
    "Align zero on both Y-axes",
    value=True,
    help="Forces the right price axis to place 0 €/MWh at the same vertical height as 0 MWh on the left thermal-gap axis.",
)
x_axis_style = st.selectbox(
    "X-axis style",
    ["Day label + hourly numbers 1-24", "Datetime labels"],
    index=0,
)
price_source_mode = st.selectbox(
    "Spot price source",
    [
        "Day Ahead workbook first, fill gaps with ESIOS",
        "ESIOS first, fill gaps with Day Ahead workbook",
        "Day Ahead workbook only",
        "ESIOS only",
    ],
    index=0,
    help=(
        "Use the same hourly price convention as the Day Ahead page: Madrid-local hourly timestamps. "
        "Workbook first is safer when the ESIOS live indicator returns values with an unexpected scale."
    ),
)
auto_fix_price_scale = st.checkbox(
    "Auto-fix suspicious ESIOS price scale",
    value=True,
    help="If live ESIOS prices look 10x too high versus normal Spanish spot prices, divide that live series by 10 before merging.",
)

if end_day < start_day:
    st.error("End day must be >= start day.")
    st.stop()

run = st.button("Fetch and plot", type="primary", use_container_width=True)

if run:
    try:
        token = require_esios_token()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    indicator_map = {"Total scheduled demand PBF": DEMAND_PBF_ID}
    indicator_map.update(PBF_GROSS_COMPONENTS)
    indicator_map.update(BILATERAL_FETCH_NAMES)
    indicator_map["Programa bilateral PBF Total Ventas"] = PBF_BILATERAL_TOTAL_SALES_ID

    raw, missing = fetch_named_indicators(indicator_map, start_day, end_day, token, warn_missing=show_diagnostics)

    if raw.empty:
        st.warning("No PBF data returned.")
        st.stop()

    wide = build_wide(raw)
    thermal, bilat_diag = calculate_thermal_gap(wide, non_thermal_techs)

    prices = load_prices_like_day_ahead(token, start_day, end_day, price_source_mode, auto_fix_price_scale)

    thermal["datetime"] = pd.to_datetime(thermal["datetime"], errors="coerce").dt.floor("h")
    prices["datetime"] = pd.to_datetime(prices["datetime"], errors="coerce").dt.floor("h")

    df = thermal.merge(prices, on="datetime", how="left").sort_values("datetime")

    monthly = calculate_monthly_stats(df)

    # Always show minimal status so the page does not look like "nothing happens".
    st.info(
        f"Fetched {len(raw):,} raw rows | {len(df):,} hourly chart rows | "
        f"Price rows matched: {int(df['price'].notna().sum()) if 'price' in df.columns else 0:,} | "
        f"Missing price hours: {int(df['price'].isna().sum()) if 'price' in df.columns else len(df):,}"
    )
    if "price" in df.columns and df["price"].notna().any():
        price_sources = sorted(df["price_source"].dropna().astype(str).unique().tolist()) if "price_source" in df.columns else []
        st.caption(
            f"Spot price average for selected range: {df['price'].mean():,.2f} €/MWh. "
            f"Price source(s): {', '.join(price_sources) if price_sources else 'unknown'}."
        )

    # Confirm bilateral netting is happening.
    total_bilat = float(bilat_diag["bilateral_mwh"].sum()) if not bilat_diag.empty and "bilateral_mwh" in bilat_diag.columns else 0.0
    total_gross = float(bilat_diag["gross_mwh"].sum()) if not bilat_diag.empty and "gross_mwh" in bilat_diag.columns else 0.0
    st.caption(
        f"Bilateral schedules deducted: {total_bilat:,.0f} MWh deducted from {total_gross:,.0f} MWh gross PBF "
        f"({(total_bilat / total_gross * 100) if total_gross else 0:,.1f}%)."
    )
    if "raw_thermal_gap_mwh" in df.columns:
        st.caption(
            f"Thermal gap range: {df['raw_thermal_gap_mwh'].min():,.0f} to {df['raw_thermal_gap_mwh'].max():,.0f} MWh/h. "
            f"Price range: {df['price'].min():,.2f} to {df['price'].max():,.2f} €/MWh."
            if "price" in df.columns and df["price"].notna().any()
            else f"Thermal gap range: {df['raw_thermal_gap_mwh'].min():,.0f} to {df['raw_thermal_gap_mwh'].max():,.0f} MWh/h."
        )

    # If all thermal gap values are missing/zero, show the key input table.
    if df.empty or "raw_thermal_gap_mwh" not in df.columns or df["raw_thermal_gap_mwh"].dropna().empty:
        st.error("No thermal gap data available after merge/calculation.")
        st.dataframe(wide.head(50), use_container_width=True, hide_index=True)
        st.stop()

    if df["raw_thermal_gap_mwh"].abs().sum() == 0:
        st.warning("Thermal gap is all zero. Check demand/non-thermal indicator IDs or ESIOS returned empty series.")
        st.dataframe(df.head(50), use_container_width=True, hide_index=True)

    # -----------------------------------------------------
    # Main interactive chart + thermal-gap composition
    # -----------------------------------------------------
    st.subheader("Thermal Gap and Price")
    st.caption(
        "Columns: hourly thermal gap. Line: hourly spot price. "
        "Hover over the columns or the price line to see exact values. "
        "X-axis is Europe/Madrid local time. The top chart only shows thermal gap on the left axis and spot price on the right axis."
    )

    plot_df = df.copy()
    plot_df["datetime"] = pd.to_datetime(plot_df["datetime"], errors="coerce")
    plot_df["raw_thermal_gap_mwh"] = pd.to_numeric(plot_df["raw_thermal_gap_mwh"], errors="coerce")
    plot_df["price"] = pd.to_numeric(plot_df["price"], errors="coerce") if "price" in plot_df.columns else pd.NA
    plot_df = plot_df.dropna(subset=["datetime", "raw_thermal_gap_mwh"]).sort_values("datetime").reset_index(drop=True)

    if plot_df.empty:
        st.error("Chart data is empty after datetime/thermal-gap cleaning.")
        st.dataframe(df.head(100), use_container_width=True, hide_index=True)
        st.stop()

    plot_df["datetime_label"] = plot_df["datetime"].dt.strftime("%Y-%m-%d %H:%M")
    plot_df["hour_1_24"] = plot_df["datetime"].dt.hour + 1
    plot_df["date_label"] = plot_df["datetime"].dt.strftime("%Y-%m-%d")

    thermal_y_domain = padded_zero_domain(plot_df["raw_thermal_gap_mwh"])
    price_y_domain = None
    if "price" in plot_df.columns and plot_df["price"].notna().any():
        if align_zero_axes:
            price_y_domain = align_second_axis_zero_domain(thermal_y_domain, plot_df["price"])
        else:
            price_y_domain = padded_zero_domain(plot_df["price"])

    x_axis = alt.Axis(
        title="Madrid local time",
        format="%H" if x_axis_style == "Day label + hourly numbers 1-24" else "%d-%b %H:%M",
        labelAngle=-90,
    )

    thermal_bars = (
        alt.Chart(plot_df)
        .mark_bar(opacity=0.88, color="#2F73C8")
        .encode(
            x=alt.X("datetime:T", title="Madrid local time", axis=x_axis),
            y=alt.Y(
                "raw_thermal_gap_mwh:Q",
                title="Thermal Gap (MWh)",
                axis=alt.Axis(format="~s", orient="left", titleColor="#2F73C8", labelColor="#2F73C8"),
                scale=alt.Scale(domain=thermal_y_domain),
            ),
            tooltip=[
                alt.Tooltip("datetime_label:N", title="Madrid time"),
                alt.Tooltip("date_label:N", title="Date"),
                alt.Tooltip("hour_1_24:Q", title="Hour", format=".0f"),
                alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap (MWh)", format=",.0f"),
                alt.Tooltip("Total scheduled demand PBF:Q", title="Demand PBF (MWh)", format=",.0f"),
                alt.Tooltip("non_thermal_net_pbf_mwh:Q", title="Non-thermal net PBF (MWh)", format=",.0f"),
            ],
        )
    )

    if "price" in plot_df.columns and plot_df["price"].notna().any():
        price_source_in_tooltip = [alt.Tooltip("price_source:N", title="Price source")] if "price_source" in plot_df.columns else []
        price_line = (
            alt.Chart(plot_df.dropna(subset=["price"]))
            .mark_line(strokeWidth=2.5, color="black")
            .encode(
                x=alt.X("datetime:T", title="Madrid local time", axis=x_axis),
                y=alt.Y(
                    "price:Q",
                    title="Spot price (€/MWh)",
                    axis=alt.Axis(format=".0f", orient="right", titleColor="black", labelColor="black"),
                    scale=alt.Scale(domain=price_y_domain),
                ),
                tooltip=[
                    alt.Tooltip("datetime_label:N", title="Madrid time"),
                    alt.Tooltip("price:Q", title="Spot price (€/MWh)", format=",.2f"),
                    *price_source_in_tooltip,
                    alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap (MWh)", format=",.0f"),
                ],
            )
        )
        price_points = (
            alt.Chart(plot_df.dropna(subset=["price"]))
            .mark_point(size=40, opacity=0.001, filled=True)
            .encode(
                x="datetime:T",
                y=alt.Y("price:Q", axis=None, scale=alt.Scale(domain=price_y_domain)),
                tooltip=[
                    alt.Tooltip("datetime_label:N", title="Madrid time"),
                    alt.Tooltip("price:Q", title="Spot price (€/MWh)", format=",.2f"),
                    alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap (MWh)", format=",.0f"),
                ],
            )
        )
        main_chart = alt.layer(thermal_bars, price_line, price_points).resolve_scale(y="independent")
    else:
        st.warning("No price data matched the hourly thermal-gap data. Showing only thermal-gap bars.")
        main_chart = alt.layer(thermal_bars)

    st.altair_chart(
        main_chart.properties(height=470).interactive(bind_y=False),
        use_container_width=True,
    )

    if align_zero_axes:
        st.caption("Zero alignment enabled: the thermal-gap and spot-price axes are scaled so 0 appears at the same vertical height.")

    st.markdown(
        "- **Left Y-axis**: hourly thermal gap, MWh/h\n"
        "- **Right Y-axis**: hourly spot price, €/MWh\n"
        "- **X-axis**: Madrid local time\n"
        "- **Bilateral schedules**: deducted before calculating the thermal gap"
    )

    # -----------------------------------------------------
    # Thermal-gap composition by technology
    # -----------------------------------------------------
    st.subheader("Thermal Gap composition by technology")
    st.caption(
        "For each hour, the stacked technologies below are distributed so that their total equals the hourly thermal gap shown in the top chart. "
        "This bottom chart does not overlay price, so the technology composition is easier to read. "
        "Technology shares are based on the listed net conventional PBF technologies after bilateral netting."
    )

    composition_cols = []
    for tech in CONVENTIONAL_TECHS_DEFAULT:
        col = f"{tech} net PBF"
        if col in plot_df.columns:
            composition_cols.append((tech, col))

    if not composition_cols:
        st.warning("No conventional net PBF technology columns are available for the composition chart.")
    else:
        comp_base_cols = ["datetime", "datetime_label", "hour_1_24", "raw_thermal_gap_mwh"] + [c for _, c in composition_cols]
        comp_wide = plot_df[comp_base_cols].copy()
        value_cols = [c for _, c in composition_cols]
        comp_wide[value_cols] = comp_wide[value_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        comp_wide["conventional_stack_mwh"] = comp_wide[value_cols].sum(axis=1)
        comp_wide["unallocated_gap_mwh"] = comp_wide["raw_thermal_gap_mwh"] - comp_wide["conventional_stack_mwh"]

        # Reallocate each hour proportionally so the stacked technologies sum exactly to the hourly thermal gap.
        comp_alloc = comp_wide.copy()
        valid_mask = comp_alloc["conventional_stack_mwh"].abs() > 1e-9
        for col in value_cols:
            comp_alloc[col] = 0.0
            comp_alloc.loc[valid_mask, col] = (
                comp_wide.loc[valid_mask, col]
                / comp_wide.loc[valid_mask, "conventional_stack_mwh"]
                * comp_wide.loc[valid_mask, "raw_thermal_gap_mwh"]
            )
        comp_alloc["allocated_total_mwh"] = comp_alloc[value_cols].sum(axis=1)

        tech_rename = {col: tech for tech, col in composition_cols}
        stack_df = comp_alloc.melt(
            id_vars=["datetime", "datetime_label", "hour_1_24", "raw_thermal_gap_mwh", "allocated_total_mwh"],
            value_vars=value_cols,
            var_name="technology_col",
            value_name="mwh",
        )
        stack_df["technology"] = stack_df["technology_col"].map(tech_rename).fillna(stack_df["technology_col"])
        stack_df["mwh"] = pd.to_numeric(stack_df["mwh"], errors="coerce").fillna(0.0)
        stack_df = stack_df[stack_df["mwh"].abs() > 1e-9].copy()

        if stack_df.empty:
            st.warning("All conventional technology contributions are zero for the selected period.")
        else:
            comp_y_domain = padded_zero_domain(
                pd.concat([stack_df["mwh"], comp_alloc["raw_thermal_gap_mwh"], comp_alloc["allocated_total_mwh"]], ignore_index=True)
            )

            stacked_bars = (
                alt.Chart(stack_df)
                .mark_bar(opacity=0.97)
                .encode(
                    x=alt.X("datetime:T", title="Madrid local time", axis=x_axis),
                    y=alt.Y(
                        "mwh:Q",
                        stack="zero",
                        title="Thermal gap distributed by technology (MWh)",
                        axis=alt.Axis(format="~s", orient="left", titleColor="#2F73C8", labelColor="#2F73C8"),
                        scale=alt.Scale(domain=comp_y_domain),
                    ),
                    color=alt.Color(
                        "technology:N",
                        title="Technology",
                        legend=alt.Legend(orient="top", direction="horizontal", labelLimit=260, columns=3),
                    ),
                    tooltip=[
                        alt.Tooltip("datetime_label:N", title="Madrid time"),
                        alt.Tooltip("hour_1_24:Q", title="Hour", format=".0f"),
                        alt.Tooltip("technology:N", title="Technology"),
                        alt.Tooltip("mwh:Q", title="Allocated technology contribution (MWh)", format=",.0f"),
                        alt.Tooltip("allocated_total_mwh:Q", title="Stack total (MWh)", format=",.0f"),
                        alt.Tooltip("raw_thermal_gap_mwh:Q", title="Thermal gap total (MWh)", format=",.0f"),
                    ],
                )
            )

            composition_chart = alt.layer(stacked_bars).properties(height=520).interactive(bind_y=False)
            st.altair_chart(composition_chart, use_container_width=True)

            unallocated_abs = float(comp_wide["unallocated_gap_mwh"].abs().sum())
            thermal_abs = float(comp_wide["raw_thermal_gap_mwh"].abs().sum())
            if unallocated_abs > max(1e-6, 0.01 * thermal_abs):
                st.info(
                    "To make the bottom chart match the top thermal-gap total hour by hour, each hour's listed conventional technologies are proportionally scaled to the hourly thermal gap. "
                    "The original difference between the listed conventional technologies and the thermal-gap formula is still shown in the diagnostics below."
                )

            with st.expander("Thermal-gap composition data", expanded=False):
                st.dataframe(
                    stack_df[["datetime_label", "technology", "mwh", "allocated_total_mwh", "raw_thermal_gap_mwh"]]
                    .rename(columns={
                        "datetime_label": "Madrid time",
                        "mwh": "Allocated technology contribution (MWh)",
                        "allocated_total_mwh": "Stack total (MWh)",
                        "raw_thermal_gap_mwh": "Thermal gap total (MWh)",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

            with st.expander("Thermal-gap reconciliation diagnostics", expanded=False):
                st.dataframe(
                    comp_wide[["datetime_label", "raw_thermal_gap_mwh", "conventional_stack_mwh", "unallocated_gap_mwh"]].rename(columns={
                        "datetime_label": "Madrid time",
                        "raw_thermal_gap_mwh": "Thermal gap total (MWh)",
                        "conventional_stack_mwh": "Original listed conventional techs (MWh)",
                        "unallocated_gap_mwh": "Original difference vs thermal gap (MWh)",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )


    # -----------------------------------------------------
    # REE demand average 24h profile — from the working demand test
    # -----------------------------------------------------
    st.subheader("REE Península demand — monthly average hourly shape")
    st.caption(
        "Public REE demanda/evolucion hourly pull. Values are converted to Europe/Madrid local time and averaged by hour of day."
    )

    selected_month_start, selected_month_natural_end = public_month_bounds(start_day)
    selected_month_end = min(end_day, selected_month_natural_end)
    if selected_month_end < selected_month_start:
        selected_month_end = selected_month_natural_end

    prev_month_start, prev_month_end = public_previous_month_bounds(selected_month_start)

    with st.spinner("Pulling REE demanda/evolucion hourly demand data..."):
        selected_hourly, sel_info = fetch_ree_demand_evolution_public(
            selected_month_start,
            selected_month_end,
            time_trunc="hour",
        )
        prev_hourly, prev_info = fetch_ree_demand_evolution_public(
            prev_month_start,
            prev_month_end,
            time_trunc="hour",
        )

    if selected_hourly.empty:
        st.warning("No hourly demand rows returned from REE demanda/evolucion for the selected month.")
        with st.expander("REE demand diagnostics", expanded=False):
            st.json({"selected": sel_info, "previous": prev_info})
    else:
        sel_label = (
            f"{selected_month_start:%b-%Y}"
            if selected_month_end == selected_month_natural_end
            else f"{selected_month_start:%b-%Y} MTD to {selected_month_end:%d-%b}"
        )
        prev_label = f"{prev_month_start:%b-%Y}"

        sel_summary = build_demand_monthly_summary(selected_hourly)
        prev_summary = build_demand_monthly_summary(prev_hourly) if not prev_hourly.empty else {}

        demand_delta = pct_delta(sel_summary.get("demand_gwh"), prev_summary.get("demand_gwh"))
        avg_delta = pct_delta(sel_summary.get("avg_demand_gw"), prev_summary.get("avg_demand_gw"))
        peak_delta = pct_delta(sel_summary.get("max_hourly_gw"), prev_summary.get("max_hourly_gw"))
        lf_delta = pct_delta(sel_summary.get("load_factor"), prev_summary.get("load_factor"))

        d1, d2, d3, d4, d5 = st.columns(5)
        with d1:
            st.metric(f"Demand total | {sel_label}", f"{sel_summary.get('demand_gwh'):,.1f} GWh" if sel_summary.get("demand_gwh") is not None else "—")
            st.caption(delta_text(None if demand_delta is None else demand_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False))
        with d2:
            st.metric("Average demand", f"{sel_summary.get('avg_demand_gw'):,.2f} GW" if sel_summary.get("avg_demand_gw") is not None else "—")
            st.caption(delta_text(None if avg_delta is None else avg_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False))
        with d3:
            st.metric("Max hourly demand", f"{sel_summary.get('max_hourly_gw'):,.2f} GW" if sel_summary.get("max_hourly_gw") is not None else "—")
            st.caption(delta_text(None if peak_delta is None else peak_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False))
        with d4:
            lf = sel_summary.get("load_factor")
            st.metric("Load factor", f"{lf * 100:,.1f}%" if lf is not None else "—")
            st.caption(delta_text(None if lf_delta is None else lf_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=True))
        with d5:
            st.metric("Days included", f"{sel_summary.get('days'):,.0f} d" if sel_summary.get("days") is not None else "—")
            if sel_summary.get("peak_hour") is not None:
                st.caption(f"Peak hour: {pd.Timestamp(sel_summary['peak_hour']):%d-%b %H:%M}")

        profiles = [build_demand_hourly_profile(selected_hourly, sel_label)]
        if not prev_hourly.empty:
            profiles.append(build_demand_hourly_profile(prev_hourly, prev_label))

        profile_df = pd.concat([p for p in profiles if p is not None and not p.empty], ignore_index=True)
        if not profile_df.empty:
            profile_chart = alt.Chart(profile_df).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("hour:O", title="Hour of day", sort=list(range(24))),
                y=alt.Y("avg_gw:Q", title="Average hourly demand (GW)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "label:N",
                    title="Month",
                    legend=alt.Legend(orient="top", direction="horizontal", labelLimit=360, titleLimit=360),
                ),
                strokeDash=alt.StrokeDash("label:N", legend=None),
                tooltip=[
                    alt.Tooltip("label:N", title="Month"),
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f"),
                    alt.Tooltip("min_gw:Q", title="Min GW", format=".2f"),
                    alt.Tooltip("max_gw:Q", title="Max GW", format=".2f"),
                    alt.Tooltip("obs:Q", title="Obs", format=",d"),
                ],
            ).properties(height=380)
            st.altair_chart(profile_chart, use_container_width=True)

        wd = build_demand_weekday_hourly_profile(selected_hourly)
        if not wd.empty:
            wd_chart = alt.Chart(wd).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("hour:O", title="Hour of day", sort=list(range(24))),
                y=alt.Y("avg_gw:Q", title="Average demand (GW)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "day_type:N",
                    title="Day type",
                    legend=alt.Legend(orient="top", direction="horizontal"),
                ),
                tooltip=[
                    alt.Tooltip("day_type:N", title="Day type"),
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f"),
                    alt.Tooltip("obs:Q", title="Obs", format=",d"),
                ],
            ).properties(height=330)
            st.altair_chart(wd_chart, use_container_width=True)

        daily = build_demand_daily_avg_profile(selected_hourly)
        if not daily.empty:
            daily_chart = alt.Chart(daily).mark_bar().encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("avg_gw:Q", title="Daily average demand (GW)", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"),
                    alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f"),
                    alt.Tooltip("max_gw:Q", title="Max GW", format=".2f"),
                    alt.Tooltip("min_gw:Q", title="Min GW", format=".2f"),
                ],
            ).properties(height=300)
            st.altair_chart(daily_chart, use_container_width=True)

        with st.expander("REE demand diagnostics", expanded=False):
            st.json({"selected": sel_info, "previous": prev_info})
            st.dataframe(selected_hourly.head(200), use_container_width=True, hide_index=True)


    if show_diagnostics:
        st.subheader("Diagnostics")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("First hour", str(df["datetime"].min()))
        with c2:
            st.metric("Last hour", str(df["datetime"].max()))
        with c3:
            st.metric("Missing price hours", int(df["price"].isna().sum()))
        with c4:
            st.metric("Missing optional indicators", len(missing))

        st.markdown("**Monthly stats**")
        st.dataframe(monthly, use_container_width=True, hide_index=True)

        with st.expander("Price diagnostics", expanded=False):
            if df["price"].notna().any():
                st.write({
                    "min": float(df["price"].min()),
                    "avg": float(df["price"].mean()),
                    "max": float(df["price"].max()),
                })
                st.dataframe(
                    df[["datetime", "price"]].dropna().sort_values("price", ascending=False).head(25),
                    use_container_width=True,
                    hide_index=True,
                )

        with st.expander("Bilateral netting diagnostics", expanded=False):
            st.dataframe(bilat_diag, use_container_width=True, hide_index=True)

        with st.expander("Missing indicators", expanded=False):
            st.write(missing)

        with st.expander("Chart data", expanded=False):
            cols = [
                "datetime",
                "raw_thermal_gap_mwh",
                "price",
                "price_source",
                "Total scheduled demand PBF",
                "non_thermal_net_pbf_mwh",
            ]
            cols = [c for c in cols if c in df.columns]
            st.dataframe(df[cols], use_container_width=True, hide_index=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "Download chart data CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name=f"hueco_termico_precio_pbf_net_bilat_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "Download bilateral diagnostics CSV",
                bilat_diag.to_csv(index=False).encode("utf-8"),
                file_name=f"bilateral_diagnostics_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c3:
            st.download_button(
                "Download raw indicators CSV",
                raw.to_csv(index=False).encode("utf-8"),
                file_name=f"raw_pbf_bilaterals_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
