from __future__ import annotations

import os
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# TEST — PBF minus bilateral PBF, thermal gap bars + spot price line
# =========================================================
#
# Main chart requested:
#   - Orange columns: hourly thermal gap, one bar per hour, MWh/h, LEFT axis
#   - Black line: hourly day-ahead spot price, €/MWh, RIGHT axis
#   - X axis: Madrid local date and hour
#
# Price logic follows the Day Ahead page style:
#   - Historical prices: data/hourly_avg_price_since2021.xlsx, sheet prices_hourly_avg
#   - Live/current 2026 prices: ESIOS indicator 600
#   - ESIOS timestamps are converted UTC -> Europe/Madrid -> timezone-naive labels
#     before merging and plotting, same convention as the Day Ahead page.
#
# Local .env:
#   ESIOS_TOKEN=your_token
#
# Streamlit Cloud Secrets:
#   ESIOS_TOKEN = "your_token"
# =========================================================

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="PBF net bilateral thermal gap + spot", layout="wide")

# App-like paths, compatible with /pages execution.
BASE_DIR = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
ENV_PATH = BASE_DIR / ".env"
DATA_DIR = BASE_DIR / "data"
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"

load_dotenv(dotenv_path=ENV_PATH, override=True)

BASE = "https://api.esios.ree.es"
MADRID_TZ = ZoneInfo("Europe/Madrid")
LIVE_START_DATE = date(2026, 1, 1)

# ---------------------------------------------------------
# ESIOS IDs
# ---------------------------------------------------------
PRICE_INDICATOR_ID = 600
DEMAND_PBF_ID = 10141

# Gross PBF scheduled generation by technology.
PBF_GROSS_COMPONENTS = {
    "Nuclear": 4,
    "Hydro UGH + non UGH": 10064,
    "Wind": 10073,
    "Solar PV": 14,
    "Solar thermal": 15,
    "Other renewables": 10074,
    "Coal": 10167,
    "Fuel-Gas": 10077,
    "Combined cycle GT": 9,
    "Natural gas": 11,
    "Cogeneration": 10086,
    "Non-renewable waste": 10095,
}

# Programa bilateral PBF by technology.
PBF_BILATERAL_COMPONENTS = {
    "Hydro UGH + non UGH": {
        "Programa bilateral PBF Hidráulica UGH": 421,
        "Programa bilateral PBF Hidráulica no UGH": 422,
    },
    "Nuclear": {
        "Programa bilateral PBF Nuclear": 424,
    },
    "Coal": {
        "Programa bilateral PBF Hulla sub-bituminosa": 426,
        "Programa bilateral PBF Hulla antracita": 427,
    },
    "Combined cycle GT": {
        "Programa bilateral PBF Ciclo combinado": 429,
    },
    "Wind": {
        "Programa bilateral PBF Eólica terrestre": 432,
        "Programa bilateral PBF Eólica marina": 433,
    },
    "Other renewables": {
        "Programa bilateral PBF Otras renovables": 10234,
    },
}

PBF_BILATERAL_TOTAL_SALES_ID = 10235

DEFAULT_NON_THERMAL = [
    "Nuclear",
    "Hydro UGH + non UGH",
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Other renewables",
]

DEFAULT_CONVENTIONAL_STACK = [
    "Coal",
    "Fuel-Gas",
    "Combined cycle GT",
    "Natural gas",
    "Cogeneration",
    "Non-renewable waste",
]


# =========================================================
# Day Ahead compatible ESIOS helpers
# =========================================================
def require_esios_token() -> str:
    token = ""

    try:
        token = str(st.secrets.get("ESIOS_TOKEN", "") or st.secrets.get("ESIOS_API_TOKEN", "") or "")
    except Exception:
        token = ""

    if not token:
        token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()

    token = token.strip().strip('"').strip("'")

    if not token:
        raise ValueError(f"No ESIOS token found. Expected ESIOS_TOKEN in {ENV_PATH} or Streamlit Secrets.")

    return token


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    """
    Same convention as Day Ahead:
      ESIOS UTC timestamp -> Europe/Madrid -> timezone-naive local label.
    This prevents Altair/browser timezone conversions and makes the x-axis match
    local Spanish spot-price hours.
    """
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    raise ValueError("No datetime column found in ESIOS response")


def parse_esios_indicator(raw_json: dict, source_name: str) -> pd.DataFrame:
    """
    Day Ahead compatible parser:
      - keep Spain / geo_id=3 when available
      - convert timestamp to Madrid local timezone-naive
      - return datetime, value
    """
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
) -> pd.DataFrame:
    """
    Fetch ESIOS indicator in chunks, using Madrid local boundaries converted to UTC.
    This mirrors the robust chunked approach from the Day Ahead page.
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
            st.warning(f"Skipped {indicator_id} chunk {chunk_start} → {chunk_end}: {last_error}")

        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    out = pd.concat(frames, ignore_index=True)

    return (
        out.drop_duplicates(subset=["datetime", "geo_id", "source"], keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False)
def load_historical_prices() -> pd.DataFrame:
    """
    Same historical workbook source as Day Ahead:
      data/hourly_avg_price_since2021.xlsx, sheet prices_hourly_avg
    Returns Madrid local timezone-naive datetime labels.
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

    # Keep as timezone-naive Madrid labels, matching Day Ahead.
    df["datetime"] = df["datetime"].dt.floor("h")

    return df[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_prices(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    """
    Same live price source as Day Ahead:
      ESIOS indicator 600, converted to Madrid local timezone-naive and hourly mean.
    """
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token, time_trunc="hour")

    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])

    out = raw[["datetime", "value"]].rename(columns={"value": "price"}).copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["datetime", "price"])

    return out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")


def load_prices_like_day_ahead(token: str, start_day: date, end_day: date) -> pd.DataFrame:
    """
    Combine the Day Ahead historical price workbook with live ESIOS prices.
    For 2026 ranges, live ESIOS indicator 600 is used.
    """
    hist = load_historical_prices()
    frames = []

    if not hist.empty:
        frames.append(hist)

    live_start = max(start_day, LIVE_START_DATE)
    if live_start <= end_day:
        live = load_live_prices(token, live_start, end_day)
        if not live.empty:
            frames.append(live)

    if not frames:
        return pd.DataFrame(columns=["datetime", "price"])

    combined = pd.concat(frames, ignore_index=True)
    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce").dt.floor("h")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    combined = combined.dropna(subset=["datetime", "price"])

    combined = (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime"], keep="last")
        .reset_index(drop=True)
    )

    mask = (combined["datetime"].dt.date >= start_day) & (combined["datetime"].dt.date <= end_day)
    return combined.loc[mask, ["datetime", "price"]].reset_index(drop=True)


# =========================================================
# PBF + bilateral fetch/calculation
# =========================================================
def fetch_named_indicators(
    indicators: dict[str, int],
    start_day: date,
    end_day: date,
    token: str,
) -> pd.DataFrame:
    frames = []
    progress = st.progress(0, text="Fetching ESIOS indicators...")
    items = list(indicators.items())

    for i, (name, indicator_id) in enumerate(items, start=1):
        try:
            raw = fetch_esios_range(indicator_id, start_day, end_day, token, time_trunc="hour")

            if raw.empty:
                st.warning(f"No data returned for {name} ({indicator_id})")
            else:
                temp = raw[["datetime", "value"]].copy()
                temp["datetime"] = pd.to_datetime(temp["datetime"], errors="coerce").dt.floor("h")
                temp["value"] = pd.to_numeric(temp["value"], errors="coerce")
                temp = temp.dropna(subset=["datetime", "value"])

                # Generation/demand indicators are MWh/h. Sum duplicate rows if any.
                temp = temp.groupby("datetime", as_index=False)["value"].sum()
                temp["series"] = name
                temp["indicator_id"] = indicator_id
                frames.append(temp)

        except Exception as exc:
            st.warning(f"Could not fetch {name} ({indicator_id}): {exc}")

        progress.progress(i / len(items), text=f"Fetched {i}/{len(items)} indicators")

    progress.empty()

    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "series", "indicator_id"])

    return pd.concat(frames, ignore_index=True)


def build_wide(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    return (
        raw.pivot_table(index="datetime", columns="series", values="value", aggfunc="sum")
        .reset_index()
        .sort_values("datetime")
        .rename_axis(None, axis=1)
    )


def apply_bilateral_netting(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    net PBF tech = gross PBF tech - bilateral PBF tech
    """
    out = wide.copy()
    diagnostics = []

    for tech in PBF_GROSS_COMPONENTS:
        gross_col = tech

        if gross_col not in out.columns:
            out[gross_col] = 0.0

        bilat_map = PBF_BILATERAL_COMPONENTS.get(tech, {})
        bilat_cols = list(bilat_map.keys())

        for col in bilat_cols:
            if col not in out.columns:
                out[col] = 0.0

        bilat_col = f"{tech} bilateral PBF"
        net_col = f"{tech} net PBF"

        out[bilat_col] = out[bilat_cols].sum(axis=1) if bilat_cols else 0.0
        out[net_col] = out[gross_col] - out[bilat_col]

        gross_sum = out[gross_col].sum()
        bilat_sum = out[bilat_col].sum()
        net_sum = out[net_col].sum()

        diagnostics.append(
            {
                "technology": tech,
                "gross_mwh": gross_sum,
                "bilateral_mwh": bilat_sum,
                "net_mwh": net_sum,
                "bilateral_share_pct": (bilat_sum / gross_sum * 100) if gross_sum else pd.NA,
                "bilateral_indicators": ", ".join(bilat_cols) if bilat_cols else "No bilateral mapped; assumed 0",
            }
        )

    return out, pd.DataFrame(diagnostics)


def calculate_thermal_gap(
    wide: pd.DataFrame,
    non_thermal_components: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    raw_thermal_gap_mwh = demand PBF - sum(net PBF non-thermal technologies)

    The value is NOT clipped, so negative bars can appear exactly like the target chart.
    """
    netted, diag = apply_bilateral_netting(wide)
    out = netted.copy()

    if "Total scheduled demand PBF" not in out.columns:
        out["Total scheduled demand PBF"] = 0.0

    non_thermal_net_cols = []

    for tech in non_thermal_components:
        col = f"{tech} net PBF"
        if col not in out.columns:
            out[col] = 0.0
        non_thermal_net_cols.append(col)

    out["non_thermal_net_pbf_mwh"] = out[non_thermal_net_cols].sum(axis=1)
    out["raw_thermal_gap_mwh"] = out["Total scheduled demand PBF"] - out["non_thermal_net_pbf_mwh"]

    # Keep a clipped version only for optional stats if needed.
    out["thermal_gap_mwh_clipped"] = out["raw_thermal_gap_mwh"].clip(lower=0)

    out["date_madrid"] = out["datetime"].dt.date
    out["hour_madrid"] = out["datetime"].dt.hour
    out["datetime_label"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M")

    return out, diag


def calculate_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["month"] = tmp["datetime"].dt.strftime("%Y-%m")

    rows = []

    for month, g in tmp.groupby("month"):
        baseload = g["price"].mean() if "price" in g.columns else pd.NA

        if "price" in g.columns and g["raw_thermal_gap_mwh"].sum() != 0:
            price_weighted_by_gap = (
                g["price"] * g["raw_thermal_gap_mwh"]
            ).sum() / g["raw_thermal_gap_mwh"].sum()
        else:
            price_weighted_by_gap = pd.NA

        rows.append(
            {
                "month": month,
                "avg_spot_price_eur_mwh": baseload,
                "avg_raw_thermal_gap_mwh": g["raw_thermal_gap_mwh"].mean(),
                "max_raw_thermal_gap_mwh": g["raw_thermal_gap_mwh"].max(),
                "min_raw_thermal_gap_mwh": g["raw_thermal_gap_mwh"].min(),
                "price_weighted_by_raw_thermal_gap_eur_mwh": price_weighted_by_gap,
                "demand_pbf_mwh": g["Total scheduled demand PBF"].sum() if "Total scheduled demand PBF" in g.columns else pd.NA,
                "non_thermal_net_pbf_mwh": g["non_thermal_net_pbf_mwh"].sum(),
                "raw_thermal_gap_mwh_sum": g["raw_thermal_gap_mwh"].sum(),
                "missing_price_hours": int(g["price"].isna().sum()) if "price" in g.columns else len(g),
            }
        )

    return pd.DataFrame(rows)


# =========================================================
# Streamlit UI
# =========================================================
st.title("Hueco térmico y precio — PBF neto de bilaterales")
st.caption(
    "Barras horarias de hueco térmico y precio spot horario. "
    "Fechas en horario local Madrid, usando la misma lógica de precios que la pestaña Day Ahead."
)

with st.expander("Indicator IDs", expanded=False):
    st.markdown("**Core**")
    st.write({"Price": PRICE_INDICATOR_ID, "Demand PBF": DEMAND_PBF_ID})

    st.markdown("**Gross PBF components**")
    st.json(PBF_GROSS_COMPONENTS)

    st.markdown("**Bilateral PBF components**")
    st.json(PBF_BILATERAL_COMPONENTS)

col1, col2 = st.columns(2)
with col1:
    start_day = st.date_input("Start day", value=date(2026, 3, 1))
with col2:
    end_day = st.date_input("End day inclusive", value=date(2026, 3, 13))

non_thermal = st.multiselect(
    "Non-thermal net PBF components deducted from demand",
    options=list(PBF_GROSS_COMPONENTS.keys()),
    default=DEFAULT_NON_THERMAL,
)

show_extra = st.checkbox("Show diagnostics and extra tables", value=True)

if end_day < start_day:
    st.error("End day must be >= start day.")
    st.stop()

if st.button("Fetch and plot", type="primary", use_container_width=True):
    token = require_esios_token()

    indicators = {
        "Total scheduled demand PBF": DEMAND_PBF_ID,
    }
    indicators.update(PBF_GROSS_COMPONENTS)

    for _, bilat_map in PBF_BILATERAL_COMPONENTS.items():
        indicators.update(bilat_map)

    indicators["Programa bilateral PBF Total Ventas"] = PBF_BILATERAL_TOTAL_SALES_ID

    raw = fetch_named_indicators(indicators, start_day, end_day, token)
    if raw.empty:
        st.warning("No PBF data returned.")
        st.stop()

    wide = build_wide(raw)
    thermal, bilat_diag = calculate_thermal_gap(wide, non_thermal)

    prices = load_prices_like_day_ahead(token, start_day, end_day)
    if prices.empty:
        st.warning("No spot prices returned from Day Ahead price logic.")
        prices = pd.DataFrame(columns=["datetime", "price"])

    # Madrid-local, timezone-naive hourly join.
    thermal["datetime"] = pd.to_datetime(thermal["datetime"], errors="coerce").dt.floor("h")
    prices["datetime"] = pd.to_datetime(prices["datetime"], errors="coerce").dt.floor("h")

    df = thermal.merge(prices, on="datetime", how="left")

    monthly = calculate_monthly_stats(df)

    # -----------------------------------------------------
    # Main chart: exact requested shape
    # -----------------------------------------------------
    st.subheader("Hueco Térmico y Precio")
    st.caption(
        "Columnas naranjas: hueco térmico horario PBF neto de bilaterales. "
        "Línea negra: precio spot horario. Todo en horario local Madrid."
    )

    base_x = alt.X(
        "datetime:T",
        title=None,
        axis=alt.Axis(
            format="%Y-%m-%d %H",
            labelAngle=-90,
            labelOverlap=False,
            tickCount={"interval": "hour", "step": 4},
        ),
    )

    bars = (
        alt.Chart(df)
        .mark_bar(color="#F5B041", opacity=0.90)
        .encode(
            x=base_x,
            y=alt.Y(
                "raw_thermal_gap_mwh:Q",
                title="Hueco Térmico (MWh)",
                axis=alt.Axis(titleColor="black", labelColor="black"),
                scale=alt.Scale(zero=True),
            ),
            tooltip=[
                alt.Tooltip("datetime:T", title="Madrid time", format="%Y-%m-%d %H:%M"),
                alt.Tooltip("raw_thermal_gap_mwh:Q", title="Hueco Térmico MWh", format=",.0f"),
                alt.Tooltip("Total scheduled demand PBF:Q", title="Demanda PBF", format=",.0f"),
                alt.Tooltip("non_thermal_net_pbf_mwh:Q", title="No térmica neta", format=",.0f"),
            ],
        )
    )

    if "price" in df.columns:
        price_line = (
            alt.Chart(df)
            .mark_line(color="black", strokeWidth=2.5)
            .encode(
                x=base_x,
                y=alt.Y(
                    "price:Q",
                    title="Precio (€/MWh)",
                    axis=alt.Axis(
                        orient="right",
                        titleColor="black",
                        labelColor="black",
                    ),
                    scale=alt.Scale(zero=False),
                ),
                tooltip=[
                    alt.Tooltip("datetime:T", title="Madrid time", format="%Y-%m-%d %H:%M"),
                    alt.Tooltip("price:Q", title="Precio €/MWh", format=",.2f"),
                ],
            )
        )
        chart = alt.layer(bars, price_line).resolve_scale(y="independent")
    else:
        chart = bars

    chart = (
        chart.properties(height=520)
        .configure_view(stroke=None)
        .configure_axis(
            grid=True,
            gridColor="#E5E7EB",
            domainColor="#9CA3AF",
            tickColor="#9CA3AF",
            labelFontSize=11,
            titleFontSize=13,
        )
        .configure_legend(orient="bottom")
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown(
        "- **Eje Y izquierdo**: Hueco térmico horario, MWh/h\\n"
        "- **Eje Y derecho**: Precio spot horario, €/MWh\\n"
        "- **Eje X**: hora local Madrid, no UTC"
    )

    if show_extra:
        st.subheader("Monthly stats")
        st.dataframe(monthly, use_container_width=True, hide_index=True)

        st.subheader("Timezone / price diagnostics")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("First plotted hour", str(df["datetime"].min()))
        with c2:
            st.metric("Last plotted hour", str(df["datetime"].max()))
        with c3:
            st.metric("Missing price hours", int(df["price"].isna().sum()) if "price" in df.columns else len(df))

        with st.expander("Price check", expanded=False):
            if "price" in df.columns and df["price"].notna().any():
                st.write(
                    {
                        "price_min": float(df["price"].min()),
                        "price_avg": float(df["price"].mean()),
                        "price_max": float(df["price"].max()),
                    }
                )
                st.dataframe(
                    df[["datetime", "price"]]
                    .dropna()
                    .sort_values("price", ascending=False)
                    .head(25),
                    use_container_width=True,
                    hide_index=True,
                )

        with st.expander("Bilateral netting diagnostics", expanded=False):
            st.dataframe(bilat_diag, use_container_width=True, hide_index=True)

        with st.expander("Hourly data used in chart", expanded=False):
            cols = [
                "datetime",
                "raw_thermal_gap_mwh",
                "price",
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
                file_name=f"pbf_net_bilateral_hueco_precio_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "Download bilateral diagnostics CSV",
                bilat_diag.to_csv(index=False).encode("utf-8"),
                file_name=f"pbf_bilateral_diagnostics_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c3:
            st.download_button(
                "Download raw indicators CSV",
                raw.to_csv(index=False).encode("utf-8"),
                file_name=f"raw_esios_pbf_bilaterals_{start_day}_{end_day}.csv",
                mime="text/csv",
                use_container_width=True,
            )
