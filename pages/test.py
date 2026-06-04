from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# TEST — PBF net of bilateral PBF + thermal gap + spot prices
# =========================================================
# Logic:
#   1) Fetch gross PBF scheduled generation by technology from ESIOS.
#   2) Fetch Programa bilateral PBF by technology from ESIOS.
#   3) Calculate net PBF:
#          PBF_net_tech,h = PBF_gross_tech,h - PBF_bilateral_tech,h
#   4) Calculate thermal gap:
#          thermal_gap_h = Demand_PBF_h - sum(PBF_net_non_thermal_tech,h)
#   5) Overlay:
#          left Y-axis: net conventional generation stack, MWh/h
#          right Y-axis: day-ahead spot price, €/MWh
#
# Local .env:
#   ESIOS_TOKEN=your_token
#
# Streamlit Cloud Secrets:
#   ESIOS_TOKEN = "your_token"
# =========================================================

st.set_page_config(page_title="PBF net bilateral thermal gap", layout="wide")

try:
    load_dotenv(override=True)
except Exception:
    pass

BASE = "https://api.esios.ree.es"
MADRID_TZ = ZoneInfo("Europe/Madrid")

# ---------------------------------------------------------
# ESIOS IDs
# ---------------------------------------------------------
DAY_AHEAD_PRICE_ID = 600
DEMAND_PBF_ID = 10141

# Gross PBF scheduled generation by technology.
# These are the PBF technology indicators used in the previous test page.
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
# Split technologies are aggregated to the same gross technology bucket.
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


# ---------------------------------------------------------
# Auth / time helpers
# ---------------------------------------------------------
def get_esios_token() -> str:
    token = ""

    try:
        token = str(st.secrets.get("ESIOS_TOKEN", "") or "")
    except Exception:
        token = ""

    if not token:
        token = os.getenv("ESIOS_TOKEN", "")

    token = str(token).strip().strip('"').strip("'")

    if not token:
        st.error(
            "Missing ESIOS_TOKEN. Add it to local .env or Streamlit Secrets.\n\n"
            "Local .env example:\n"
            "ESIOS_TOKEN=xxxxxxxx"
        )
        st.stop()

    return token


def esios_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
        "User-Agent": "Mozilla/5.0",
    }


def madrid_date_to_api_range(start_day: date, end_day: date) -> tuple[str, str]:
    """
    Use Madrid local boundaries converted to UTC.
    End is exclusive: next local midnight after end_day.
    """
    start_local = datetime.combine(start_day, time(0, 0), tzinfo=MADRID_TZ)
    end_local = datetime.combine(end_day + timedelta(days=1), time(0, 0), tzinfo=MADRID_TZ)
    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc = end_local.astimezone(ZoneInfo("UTC"))
    return (
        start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


# ---------------------------------------------------------
# ESIOS fetch / parse
# ---------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=1800)
def fetch_esios_indicator(
    indicator_id: int,
    start_utc: str,
    end_utc: str,
    token: str,
    time_agg: str = "sum",
) -> pd.DataFrame:
    """
    Fetch one ESIOS indicator at hourly granularity.

    Important:
    - For price, never sum values; filter to España/geo_id=3 if available and average duplicates.
    - For generation/demand, sum duplicate hourly rows after filtering geography.
    """
    url = f"{BASE}/indicators/{indicator_id}"
    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "time_trunc": "hour",
    }

    # Price is already €/MWh, do not request sum aggregation.
    if indicator_id != DAY_AHEAD_PRICE_ID:
        params["time_agg"] = time_agg

    r = requests.get(url, headers=esios_headers(token), params=params, timeout=90)
    if not r.ok:
        raise RuntimeError(
            f"ESIOS indicator {indicator_id} failed: HTTP {r.status_code}. "
            f"URL={r.url}. Body preview={r.text[:500]}"
        )

    values = r.json().get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime_madrid", "indicator_id", "value"])

    raw = pd.DataFrame(values)

    # Prefer national Spain series when ESIOS returns several geographies.
    if "geo_id" in raw.columns and (raw["geo_id"] == 3).any():
        raw = raw[raw["geo_id"] == 3].copy()
    elif "geo_name" in raw.columns:
        geo = raw["geo_name"].astype(str).str.strip().str.lower()
        mask = geo.isin(["españa", "espana"])
        if mask.any():
            raw = raw[mask].copy()

    dt_col = "datetime_utc" if "datetime_utc" in raw.columns else "datetime"
    if dt_col not in raw.columns:
        raise ValueError(f"Indicator {indicator_id}: no datetime column in response: {raw.columns.tolist()}")

    out = pd.DataFrame()
    out["datetime_utc"] = pd.to_datetime(raw[dt_col], utc=True, errors="coerce")
    out["datetime_madrid"] = out["datetime_utc"].dt.tz_convert("Europe/Madrid")
    out["indicator_id"] = indicator_id
    out["value"] = pd.to_numeric(raw["value"], errors="coerce")
    out = out.dropna(subset=["datetime_madrid", "value"])

    agg_func = "mean" if indicator_id == DAY_AHEAD_PRICE_ID else "sum"

    out = (
        out.groupby(["datetime_madrid", "indicator_id"], as_index=False)
           .agg(value=("value", agg_func))
           .sort_values("datetime_madrid")
           .reset_index(drop=True)
    )

    return out


def fetch_many_indicators(indicators: dict[str, int], start_utc: str, end_utc: str, token: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    progress = st.progress(0, text="Fetching ESIOS indicators...")
    items = list(indicators.items())

    for i, (name, ind_id) in enumerate(items, start=1):
        try:
            df = fetch_esios_indicator(
                indicator_id=ind_id,
                start_utc=start_utc,
                end_utc=end_utc,
                token=token,
                time_agg="sum",
            )
            if not df.empty:
                df["series"] = name
                frames.append(df)
        except Exception as exc:
            st.warning(f"Could not fetch {name} ({ind_id}): {exc}")

        progress.progress(i / len(items), text=f"Fetched {i}/{len(items)} ESIOS indicators")

    progress.empty()

    if not frames:
        return pd.DataFrame(columns=["datetime_madrid", "indicator_id", "value", "series"])

    return pd.concat(frames, ignore_index=True)


def build_wide(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    wide = (
        raw.pivot_table(
            index="datetime_madrid",
            columns="series",
            values="value",
            aggfunc="sum",
        )
        .reset_index()
        .sort_values("datetime_madrid")
    )
    wide.columns.name = None
    return wide


# ---------------------------------------------------------
# Bilateral netting and thermal gap
# ---------------------------------------------------------
def apply_bilateral_netting(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate net PBF by technology:
        net PBF = gross PBF - bilateral PBF
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

        if bilat_cols:
            out[bilat_col] = out[bilat_cols].sum(axis=1)
        else:
            out[bilat_col] = 0.0

        out[net_col] = out[gross_col] - out[bilat_col]

        # Avoid negative noise if bilaterals slightly exceed gross because of revisions/rounding.
        out[net_col] = out[net_col].clip(lower=0)

        gross_sum = out[gross_col].sum()
        bilat_sum = out[bilat_col].sum()
        net_sum = out[net_col].sum()

        diagnostics.append(
            {
                "technology": tech,
                "gross_indicator": gross_col,
                "bilateral_indicators": ", ".join(bilat_cols) if bilat_cols else "No bilateral mapped; assumed 0",
                "gross_mwh": gross_sum,
                "bilateral_mwh": bilat_sum,
                "net_mwh": net_sum,
                "bilateral_share_pct": (bilat_sum / gross_sum * 100) if gross_sum else pd.NA,
            }
        )

    return out, pd.DataFrame(diagnostics)


def calculate_thermal_gap(
    wide: pd.DataFrame,
    non_thermal_components: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Thermal gap based on net PBF:
        thermal_gap = Total scheduled demand PBF - sum(net PBF non-thermal techs)
    """
    netted, diag = apply_bilateral_netting(wide)
    out = netted.copy()

    # For chart readability, overwrite technology columns with net values.
    for tech in PBF_GROSS_COMPONENTS:
        net_col = f"{tech} net PBF"
        out[tech] = out[net_col] if net_col in out.columns else 0.0

    if "Total scheduled demand PBF" not in out.columns:
        out["Total scheduled demand PBF"] = 0.0

    non_thermal_net_cols = []
    for tech in non_thermal_components:
        col = f"{tech} net PBF"
        if col not in out.columns:
            out[col] = 0.0
        non_thermal_net_cols.append(col)

    out["non_thermal_mwh"] = out[non_thermal_net_cols].sum(axis=1)
    out["thermal_gap_mwh"] = out["Total scheduled demand PBF"] - out["non_thermal_mwh"]
    out["thermal_gap_mwh"] = out["thermal_gap_mwh"].clip(lower=0)

    out["date_madrid"] = out["datetime_madrid"].dt.date
    out["hour_madrid"] = out["datetime_madrid"].dt.hour
    out["datetime_label"] = out["datetime_madrid"].dt.strftime("%d-%b %H:%M")

    return out, diag


def calculate_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["month"] = tmp["datetime_madrid"].dt.strftime("%Y-%m")

    rows = []
    for month, g in tmp.groupby("month"):
        baseload = g["Day-ahead price"].mean() if "Day-ahead price" in g.columns else pd.NA

        if "Day-ahead price" in g.columns and g["thermal_gap_mwh"].sum() != 0:
            price_weighted_by_gap = (
                g["Day-ahead price"] * g["thermal_gap_mwh"]
            ).sum() / g["thermal_gap_mwh"].sum()
        else:
            price_weighted_by_gap = pd.NA

        rows.append(
            {
                "month": month,
                "avg_spot_price_eur_mwh": baseload,
                "avg_thermal_gap_mwh": g["thermal_gap_mwh"].mean(),
                "max_thermal_gap_mwh": g["thermal_gap_mwh"].max(),
                "min_thermal_gap_mwh": g["thermal_gap_mwh"].min(),
                "price_weighted_by_thermal_gap_eur_mwh": price_weighted_by_gap,
                "demand_pbf_mwh": g["Total scheduled demand PBF"].sum() if "Total scheduled demand PBF" in g.columns else pd.NA,
                "non_thermal_net_pbf_mwh": g["non_thermal_mwh"].sum(),
                "thermal_gap_mwh_sum": g["thermal_gap_mwh"].sum(),
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------
st.title("PBF net of bilaterals — thermal gap vs day-ahead price")
st.caption(
    "Gross PBF generation is reduced by Programa bilateral PBF indicators where mapped. "
    "Timestamps are requested in UTC and displayed in Europe/Madrid."
)

with st.expander("Indicator IDs used", expanded=False):
    st.markdown("**Core indicators**")
    st.write({"Day-ahead price": DAY_AHEAD_PRICE_ID, "Total scheduled demand PBF": DEMAND_PBF_ID})

    st.markdown("**Gross PBF generation indicators**")
    st.json(PBF_GROSS_COMPONENTS)

    st.markdown("**Bilateral PBF indicators**")
    st.json(PBF_BILATERAL_COMPONENTS)

    st.markdown("**Bilateral total sales diagnostic**")
    st.write(PBF_BILATERAL_TOTAL_SALES_ID)

col1, col2 = st.columns(2)
with col1:
    start_day = st.date_input("Start day", value=date(2026, 3, 1))
with col2:
    end_day = st.date_input("End day inclusive", value=date(2026, 3, 31))

non_thermal = st.multiselect(
    "Non-thermal net PBF components deducted from demand",
    options=list(PBF_GROSS_COMPONENTS.keys()),
    default=DEFAULT_NON_THERMAL,
)

stack_components = st.multiselect(
    "Conventional net PBF technologies to stack",
    options=list(PBF_GROSS_COMPONENTS.keys()),
    default=DEFAULT_CONVENTIONAL_STACK,
)

if end_day < start_day:
    st.error("End day must be >= start day.")
    st.stop()

start_utc, end_utc = madrid_date_to_api_range(start_day, end_day)

st.write(f"API UTC request window: `{start_utc}` → `{end_utc}`")
st.info("Charts use Europe/Madrid local time.")

run = st.button("Fetch PBF net bilateral thermal gap", type="primary", use_container_width=True)

if run:
    token = get_esios_token()

    indicators = {
        "Day-ahead price": DAY_AHEAD_PRICE_ID,
        "Total scheduled demand PBF": DEMAND_PBF_ID,
    }

    # Gross PBF technologies.
    indicators.update(PBF_GROSS_COMPONENTS)

    # Bilateral PBF indicators.
    for tech, bilat_map in PBF_BILATERAL_COMPONENTS.items():
        indicators.update(bilat_map)

    # Total bilateral sales diagnostic.
    indicators["Programa bilateral PBF Total Ventas"] = PBF_BILATERAL_TOTAL_SALES_ID

    raw = fetch_many_indicators(indicators, start_utc, end_utc, token)

    if raw.empty:
        st.warning("No ESIOS data returned.")
        st.stop()

    wide = build_wide(raw)
    df, bilateral_diag = calculate_thermal_gap(wide, non_thermal)
    monthly = calculate_monthly_stats(df)

    # -----------------------------------------------------
    # Main overlay chart
    # -----------------------------------------------------
    st.subheader("Overlay — net PBF conventional stack vs day-ahead price")
    st.caption(
        "Left axis: net PBF conventional generation in MWh/h. "
        "Right axis: day-ahead spot price in €/MWh."
    )

    stack_cols = [c for c in stack_components if c in df.columns]

    if not stack_cols:
        st.warning("Select at least one conventional technology to stack.")
    else:
        stack_df = df[["datetime_madrid"] + stack_cols].melt(
            id_vars=["datetime_madrid"],
            var_name="component",
            value_name="mwh",
        )
        stack_df["mwh"] = pd.to_numeric(stack_df["mwh"], errors="coerce").fillna(0.0)

        base_x = alt.X(
            "datetime_madrid:T",
            title="Madrid date and hour",
            axis=alt.Axis(format="%d-%b %H:%M", labelAngle=-45),
        )

        bars = (
            alt.Chart(stack_df)
            .mark_bar(opacity=0.88)
            .encode(
                x=base_x,
                y=alt.Y(
                    "mwh:Q",
                    title="Net PBF conventional generation (MWh/h)",
                    stack="zero",
                    axis=alt.Axis(titleColor="#111827", labelColor="#111827"),
                ),
                color=alt.Color(
                    "component:N",
                    title="Net PBF conventional technologies",
                    legend=alt.Legend(orient="right"),
                ),
                tooltip=[
                    alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                    alt.Tooltip("component:N", title="Technology"),
                    alt.Tooltip("mwh:Q", title="MWh/h", format=",.0f"),
                ],
            )
        )

        if "Day-ahead price" in df.columns:
            price_line = (
                alt.Chart(df)
                .mark_line(color="#2563EB", strokeWidth=3)
                .encode(
                    x=base_x,
                    y=alt.Y(
                        "Day-ahead price:Q",
                        title="Day-ahead price (€/MWh)",
                        axis=alt.Axis(
                            titleColor="#2563EB",
                            labelColor="#2563EB",
                            orient="right",
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                        alt.Tooltip("Day-ahead price:Q", title="Price €/MWh", format=",.2f"),
                    ],
                )
            )

            combined = alt.layer(bars, price_line).resolve_scale(y="independent").properties(height=460)
        else:
            combined = bars.properties(height=460)

        st.altair_chart(combined, use_container_width=True)

    # -----------------------------------------------------
    # Thermal gap chart
    # -----------------------------------------------------
    st.subheader("Calculated thermal gap — net PBF basis")
    gap_chart = (
        alt.Chart(df)
        .mark_line(color="black", strokeWidth=2.5)
        .encode(
            x=alt.X(
                "datetime_madrid:T",
                title="Madrid date and hour",
                axis=alt.Axis(format="%d-%b %H:%M", labelAngle=-45),
            ),
            y=alt.Y("thermal_gap_mwh:Q", title="Thermal gap (MWh/h)"),
            tooltip=[
                alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                alt.Tooltip("thermal_gap_mwh:Q", title="Thermal gap", format=",.0f"),
                alt.Tooltip("Total scheduled demand PBF:Q", title="Demand PBF", format=",.0f"),
                alt.Tooltip("non_thermal_mwh:Q", title="Net non-thermal PBF", format=",.0f"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(gap_chart, use_container_width=True)

    # -----------------------------------------------------
    # Scatter
    # -----------------------------------------------------
    if "Day-ahead price" in df.columns:
        st.subheader("Scatter — day-ahead price vs net PBF thermal gap")
        scatter = (
            alt.Chart(df)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("thermal_gap_mwh:Q", title="Thermal gap (MWh/h)"),
                y=alt.Y("Day-ahead price:Q", title="Day-ahead price (€/MWh)"),
                color=alt.Color("hour_madrid:O", title="Madrid hour"),
                tooltip=[
                    alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                    alt.Tooltip("thermal_gap_mwh:Q", title="Thermal gap", format=",.0f"),
                    alt.Tooltip("Day-ahead price:Q", title="Price €/MWh", format=",.2f"),
                    alt.Tooltip("hour_madrid:O", title="Hour"),
                ],
            )
            .properties(height=420)
        )
        st.altair_chart(scatter, use_container_width=True)

    # -----------------------------------------------------
    # Tables / diagnostics
    # -----------------------------------------------------
    st.subheader("Monthly stats")
    st.dataframe(monthly, use_container_width=True, hide_index=True)

    st.subheader("Bilateral netting diagnostics")
    st.caption(
        "Net PBF = gross PBF technology - Programa bilateral PBF technology. "
        "Technologies without a mapped bilateral indicator are assumed to have bilateral 0."
    )
    st.dataframe(bilateral_diag, use_container_width=True, hide_index=True)

    with st.expander("Price diagnostics", expanded=False):
        if "Day-ahead price" in df.columns:
            st.write(
                {
                    "min": float(df["Day-ahead price"].min()),
                    "avg": float(df["Day-ahead price"].mean()),
                    "max": float(df["Day-ahead price"].max()),
                }
            )
            st.dataframe(
                df[["datetime_madrid", "Day-ahead price"]]
                .sort_values("Day-ahead price", ascending=False)
                .head(20),
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Hourly data", expanded=False):
        st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("Raw fetched indicators", expanded=False):
        st.dataframe(raw, use_container_width=True, hide_index=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.download_button(
            "Download hourly CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"pbf_net_bilateral_thermal_gap_hourly_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with c2:
        st.download_button(
            "Download monthly stats CSV",
            monthly.to_csv(index=False).encode("utf-8"),
            file_name=f"pbf_net_bilateral_monthly_stats_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with c3:
        st.download_button(
            "Download bilateral diagnostics CSV",
            bilateral_diag.to_csv(index=False).encode("utf-8"),
            file_name=f"pbf_bilateral_netting_diagnostics_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with c4:
        st.download_button(
            "Download raw indicators CSV",
            raw.to_csv(index=False).encode("utf-8"),
            file_name=f"esios_raw_pbf_bilaterals_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )
