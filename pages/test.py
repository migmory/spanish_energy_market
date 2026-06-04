from __future__ import annotations

import os
import re
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from time import sleep
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

st.set_page_config(page_title="Hueco térmico PBF - bilaterales", layout="wide")

# Same path convention as the main app pages.
BASE_DIR = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
ENV_PATH = BASE_DIR / ".env"
DATA_DIR = BASE_DIR / "data"
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
LIVE_START_DATE = date(2026, 1, 1)
MADRID_TZ = ZoneInfo("Europe/Madrid")

load_dotenv(dotenv_path=ENV_PATH, override=True)

BASE = "https://api.esios.ree.es"

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
# UI
# =========================================================
st.title("Thermal Gap and Price")
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
show_thermal_mix_stack = st.checkbox(
    "Decompose thermal gap bars by conventional technology",
    value=True,
    help=(
        "If enabled, the columns are stacked using available net PBF conventional technologies. "
        "If disabled, the columns show the calculated thermal gap as one orange bar."
    ),
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
    available_thermal_cols = [
        f"{tech} net PBF"
        for tech in [
            "Coal sub-bituminous",
            "Coal anthracite",
            "Combined cycle GT",
            "Fuel",
            "Natural gas",
            "Cogeneration",
            "Non-renewable waste",
        ]
        if f"{tech} net PBF" in df.columns
    ]
    if available_thermal_cols:
        df["available_conventional_net_pbf_mwh"] = df[available_thermal_cols].sum(axis=1)
        st.caption(
            f"Available conventional net PBF average: {df['available_conventional_net_pbf_mwh'].mean():,.0f} MWh/h. "
            f"Calculated thermal gap average: {df['raw_thermal_gap_mwh'].mean():,.0f} MWh/h. "
            "If these two differ, the missing indicators / balancing items explain why the stack may not fully match the calculated gap."
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
    # Main chart like the reference image
    # -----------------------------------------------------
    st.subheader("Thermal Gap and Price")
    st.caption(
        "Columns: hourly thermal mix stack or calculated thermal gap. Line: hourly spot price. "
        "X-axis is Europe/Madrid local time. The chart is rendered with matplotlib to avoid "
        "browser/Altair timezone conversions. Exact matching with the LinkedIn chart depends on using the same "
        "thermal-gap definition and the same underlying PBF/bilateral indicator set."
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

    x = list(range(len(plot_df)))
    labels = plot_df["datetime"].dt.strftime("%Y-%m-%d\\n%H").tolist()

    fig, ax_gap = plt.subplots(figsize=(18, 6.2))

    # Bars:
    #   - default: stacked available net PBF conventional technologies
    #   - fallback: single calculated thermal-gap bar
    conventional_techs = [
        "Coal sub-bituminous",
        "Coal anthracite",
        "Combined cycle GT",
        "Fuel",
        "Natural gas",
        "Cogeneration",
        "Non-renewable waste",
    ]
    conventional_net_cols = {
        tech: f"{tech} net PBF"
        for tech in conventional_techs
        if f"{tech} net PBF" in plot_df.columns
        and pd.to_numeric(plot_df[f"{tech} net PBF"], errors="coerce").fillna(0).abs().sum() > 0
    }

    if show_thermal_mix_stack and conventional_net_cols:
        stack_colors = {
            "Coal sub-bituminous": "#6B7280",
            "Coal anthracite": "#374151",
            "Combined cycle GT": "#F97316",
            "Fuel": "#7C2D12",
            "Natural gas": "#A16207",
            "Cogeneration": "#60A5FA",
            "Non-renewable waste": "#9CA3AF",
        }
        bottom = pd.Series(0.0, index=plot_df.index)
        for tech, col in conventional_net_cols.items():
            values = pd.to_numeric(plot_df[col], errors="coerce").fillna(0.0)
            ax_gap.bar(
                x,
                values,
                bottom=bottom,
                width=0.82,
                color=stack_colors.get(tech, "#F5B041"),
                alpha=0.90,
                label=tech,
                zorder=2,
            )
            bottom = bottom + values

        # Keep the calculated thermal-gap total visible as a thin dashed orange outline/line.
        ax_gap.plot(
            x,
            plot_df["raw_thermal_gap_mwh"],
            color="#B45309",
            linewidth=1.4,
            linestyle="--",
            label="Calculated thermal gap",
            zorder=3,
        )
    else:
        ax_gap.bar(
            x,
            plot_df["raw_thermal_gap_mwh"],
            width=0.82,
            color="#F5B041",
            alpha=0.90,
            label="Thermal Gap",
            zorder=2,
        )

    ax_gap.axhline(0, color="black", linewidth=0.8, alpha=0.65, zorder=4)
    ax_gap.set_ylabel("Thermal Gap (MWh)", fontweight="bold")
    ax_gap.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if abs(v) >= 1000 else f"{v:.0f}"))
    ax_gap.grid(axis="y", alpha=0.25, zorder=1)
    ax_gap.set_xlim(-0.8, len(plot_df) - 0.2)

    ax_price = ax_gap.twinx()
    if "price" in plot_df.columns and plot_df["price"].notna().any():
        price_df = plot_df[plot_df["price"].notna()].copy()
        price_x = price_df.index.tolist()
        ax_price.plot(
            price_x,
            price_df["price"],
            color="black",
            linewidth=2.2,
            label="Price",
            zorder=4,
        )
        ax_price.set_ylabel("Price (€/MWh)", fontweight="bold")
        ax_price.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}"))

        if align_zero_axes:
            left_min, left_max = ax_gap.get_ylim()
            if left_min < 0 < left_max:
                zero_frac = (0 - left_min) / (left_max - left_min)
                p_min = float(price_df["price"].min())
                p_max = float(price_df["price"].max())

                # Add light padding.
                p_min = p_min * 1.10 if p_min < 0 else p_min * 0.90
                p_max = p_max * 1.10 if p_max > 0 else p_max * 0.90

                if 0 < zero_frac < 1:
                    required_range = max(
                        p_max / (1 - zero_frac) if p_max > 0 else 0,
                        (-p_min) / zero_frac if p_min < 0 else 0,
                        1.0,
                    )
                    ax_price.set_ylim(
                        -zero_frac * required_range,
                        (1 - zero_frac) * required_range,
                    )
    else:
        ax_price.set_ylabel("Price (€/MWh)", fontweight="bold")
        st.warning("No price data matched the hourly thermal-gap data. Showing only thermal-gap bars.")

    if x_axis_style == "Day label + hourly numbers 1-24":
        # Match the reference chart style:
        #   bottom row = hour numbers 1-24
        #   second row = date label once per day
        # For long ranges, avoid overloading the axis by showing every 3rd hour.
        hour_step = 1 if len(plot_df) <= 14 * 24 else 3
        tick_positions = list(range(0, len(plot_df), hour_step))
        hour_labels = [
            str(int(plot_df.loc[i, "datetime"].hour) + 1)
            for i in tick_positions
        ]
        ax_gap.set_xticks(tick_positions)
        ax_gap.set_xticklabels(hour_labels, rotation=90, ha="center", fontsize=8)
        ax_gap.set_xlabel("")

        # Draw vertical separators and centered date labels under the hour row.
        day_groups = plot_df.groupby(plot_df["datetime"].dt.date).indices
        for day, idxs in day_groups.items():
            idxs = sorted(list(idxs))
            first_i = idxs[0]
            last_i = idxs[-1]
            center_i = (first_i + last_i) / 2

            ax_gap.axvline(first_i - 0.5, color="#D1D5DB", linewidth=0.8, alpha=0.8, zorder=1)
            ax_gap.text(
                center_i,
                -0.20,
                pd.Timestamp(day).strftime("%Y-%m-%d"),
                ha="center",
                va="top",
                fontsize=8,
                transform=ax_gap.get_xaxis_transform(),
            )

        # last separator
        ax_gap.axvline(len(plot_df) - 0.5, color="#D1D5DB", linewidth=0.8, alpha=0.8, zorder=1)
    else:
        # Fallback: compact datetime labels.
        step = 4 if len(plot_df) <= 120 else max(4, int(len(plot_df) / 32))
        tick_positions = list(range(0, len(plot_df), step))
        if (len(plot_df) - 1) not in tick_positions:
            tick_positions.append(len(plot_df) - 1)
        ax_gap.set_xticks(tick_positions)
        ax_gap.set_xticklabels([labels[i] for i in tick_positions], rotation=90, ha="center", fontsize=8)

    ax_gap.set_title("Thermal Gap and Price", loc="left", fontweight="bold", fontsize=14, pad=18)

    handles1, labels1 = ax_gap.get_legend_handles_labels()
    handles2, labels2 = ax_price.get_legend_handles_labels()
    fig.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc="lower center",
        ncol=2,
        frameon=False,
        bbox_to_anchor=(0.5, -0.02),
    )

    fig.tight_layout(rect=[0, 0.12, 1, 1])
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)

    st.markdown(
        "- **Left Y-axis**: hourly thermal mix / calculated thermal gap, MWh/h\n"
        "- **Right Y-axis**: hourly spot price, €/MWh. If zero alignment is enabled, the visible right-axis range may be wider than the actual price range.\n"
        "- **X-axis**: Madrid local time, with day labels and hour numbers 1-24\n"
        "- **Bilateral schedules**: yes, they are deducted before calculating the thermal gap"
    )

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
                "available_conventional_net_pbf_mwh",
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
