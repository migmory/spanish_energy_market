from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# =========================================================
# ESIOS P48 hourly thermal gap test
# =========================================================
# Auth:
#   local .env: ESIOS_TOKEN=xxxxxxxx
#   Streamlit Cloud Secrets: ESIOS_TOKEN = "xxxxxxxx"
# All charts convert API UTC timestamps to Europe/Madrid.
# =========================================================

st.set_page_config(page_title="ESIOS P48 thermal gap test", layout="wide")

if load_dotenv is not None:
    load_dotenv(override=True)

BASE = "https://api.esios.ree.es"
MADRID_TZ = ZoneInfo("Europe/Madrid")

DAY_AHEAD_PRICE_ID = 600
DEMAND_P48_ID = 10027

P48_COMPONENTS = {
    "Nuclear": 74,
    "Hydro UGH + non UGH": 10063,
    "Wind": 10010,
    "Solar PV": 84,
    "Solar thermal": 85,
    "Other renewables": 10013,
    "Coal": 10008,
    "Fuel-Gas": 10009,
    "Combined cycle GT": 79,
    "Natural gas": 81,
    "Cogeneration": 10011,
    "Non-renewable waste": 10012,
    "Pump consumption": 95,
}

DEFAULT_NON_THERMAL = [
    "Nuclear",
    "Hydro UGH + non UGH",
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Other renewables",
]

DEFAULT_THERMAL_STACK = [
    "Coal",
    "Fuel-Gas",
    "Combined cycle GT",
    "Natural gas",
    "Cogeneration",
    "Non-renewable waste",
]


def get_esios_token() -> str:
    token = ""
    try:
        token = str(st.secrets.get("ESIOS_TOKEN", "") or "")
    except Exception:
        token = ""
    if not token:
        token = os.getenv("ESIOS_TOKEN", "")
    token = token.strip().strip('"').strip("'")
    if not token:
        st.error("Missing ESIOS_TOKEN. Put it in .env locally or Streamlit Secrets.")
        st.stop()
    return token


def esios_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "Host": "api.esios.ree.es",
        "x-api-key": token,
        "User-Agent": "Mozilla/5.0",
    }


def madrid_date_to_api_range(start_day: date, end_day: date) -> tuple[str, str]:
    start_local = datetime.combine(start_day, time(0, 0), tzinfo=MADRID_TZ)
    end_local = datetime.combine(end_day + timedelta(days=1), time(0, 0), tzinfo=MADRID_TZ)
    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc = end_local.astimezone(ZoneInfo("UTC"))
    return start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


@st.cache_data(show_spinner=True, ttl=1800)
def fetch_esios_indicator(indicator_id: int, start_utc: str, end_utc: str, token: str, time_agg: str = "sum") -> pd.DataFrame:
    url = f"{BASE}/indicators/{indicator_id}"
    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "time_trunc": "hour",
        "time_agg": time_agg,
    }
    r = requests.get(url, headers=esios_headers(token), params=params, timeout=90)
    if not r.ok:
        raise RuntimeError(
            f"ESIOS indicator {indicator_id} failed: HTTP {r.status_code}. "
            f"URL={r.url}. Body preview={r.text[:500]}"
        )

    payload = r.json()
    values = payload.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime_madrid", "indicator_id", "value"])

    df = pd.DataFrame(values)
    dt_col = "datetime_utc" if "datetime_utc" in df.columns else "datetime"
    if dt_col not in df.columns:
        raise ValueError(f"Indicator {indicator_id}: no datetime column in response columns {df.columns.tolist()}")

    out = pd.DataFrame()
    out["datetime_utc"] = pd.to_datetime(df[dt_col], utc=True, errors="coerce")
    out["datetime_madrid"] = out["datetime_utc"].dt.tz_convert("Europe/Madrid")
    out["indicator_id"] = indicator_id
    out["value"] = pd.to_numeric(df["value"], errors="coerce")
    out = out.dropna(subset=["datetime_madrid", "value"])
    out = (
        out.groupby(["datetime_madrid", "indicator_id"], as_index=False)
           .agg(value=("value", "sum"))
           .sort_values("datetime_madrid")
    )
    return out


def fetch_many_indicators(indicators: dict[str, int], start_utc: str, end_utc: str, token: str) -> pd.DataFrame:
    frames = []
    progress = st.progress(0, text="Fetching ESIOS indicators...")
    items = list(indicators.items())
    for i, (name, ind_id) in enumerate(items, start=1):
        try:
            time_agg = "average" if ind_id == DAY_AHEAD_PRICE_ID else "sum"
            df = fetch_esios_indicator(ind_id, start_utc, end_utc, token, time_agg=time_agg)
            df["series"] = name
            frames.append(df)
        except Exception as exc:
            st.warning(f"Could not fetch {name} ({ind_id}): {exc}")
        progress.progress(i / len(items), text=f"Fetched {i}/{len(items)} ESIOS indicators")
    progress.empty()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_wide_balance(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    wide = (
        raw.pivot_table(index="datetime_madrid", columns="series", values="value", aggfunc="sum")
        .reset_index()
        .sort_values("datetime_madrid")
    )
    wide.columns.name = None
    return wide


def calculate_thermal_gap(wide: pd.DataFrame, non_thermal_components: list[str]) -> pd.DataFrame:
    out = wide.copy()
    for col in ["Total scheduled demand P48"] + non_thermal_components:
        if col not in out.columns:
            out[col] = 0.0
    out["non_thermal_mwh"] = out[non_thermal_components].sum(axis=1)
    out["thermal_gap_mwh"] = out["Total scheduled demand P48"] - out["non_thermal_mwh"]
    out["date_madrid"] = out["datetime_madrid"].dt.date
    out["hour_madrid"] = out["datetime_madrid"].dt.hour
    out["datetime_label"] = out["datetime_madrid"].dt.strftime("%d-%b %H:%M")
    return out


def calculate_monthly_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    tmp = df.copy()
    tmp["month"] = tmp["datetime_madrid"].dt.strftime("%Y-%m")
    rows = []
    for month, g in tmp.groupby("month"):
        baseload = g["Day-ahead price"].mean() if "Day-ahead price" in g.columns else pd.NA
        gap = g["thermal_gap_mwh"].clip(lower=0)
        if "Day-ahead price" in g.columns and gap.sum() != 0:
            gap_weighted_price = (g["Day-ahead price"] * gap).sum() / gap.sum()
        else:
            gap_weighted_price = pd.NA
        rows.append({
            "month": month,
            "avg_price_eur_mwh": baseload,
            "avg_thermal_gap_mwh": g["thermal_gap_mwh"].mean(),
            "max_thermal_gap_mwh": g["thermal_gap_mwh"].max(),
            "min_thermal_gap_mwh": g["thermal_gap_mwh"].min(),
            "price_weighted_by_thermal_gap_eur_mwh": gap_weighted_price,
            "demand_mwh": g.get("Total scheduled demand P48", pd.Series(dtype=float)).sum(),
            "non_thermal_mwh": g["non_thermal_mwh"].sum(),
            "thermal_gap_mwh_sum": g["thermal_gap_mwh"].sum(),
        })
    return pd.DataFrame(rows)


st.title("ESIOS P48 — hourly thermal gap vs day-ahead price")
st.caption(
    "Uses ESIOS API indicators. API timestamps are UTC; all charts convert to Europe/Madrid. "
    "Thermal gap definition is configurable below."
)

with st.expander("Indicator IDs used", expanded=False):
    st.write("Day-ahead price:", DAY_AHEAD_PRICE_ID)
    st.write("Total scheduled demand P48:", DEMAND_P48_ID)
    st.json(P48_COMPONENTS)

col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    start_day = st.date_input("Start day", value=date(2026, 3, 1))
with col2:
    end_day = st.date_input("End day inclusive", value=date(2026, 3, 31))
with col3:
    st.selectbox("Program", ["P48"], index=0, disabled=True)

non_thermal = st.multiselect(
    "Components deducted from demand to calculate thermal gap",
    options=list(P48_COMPONENTS.keys()),
    default=DEFAULT_NON_THERMAL,
)

show_stack_components = st.multiselect(
    "Conventional technologies to stack (left axis volumes)",
    options=list(P48_COMPONENTS.keys()),
    default=DEFAULT_THERMAL_STACK,
)

if end_day < start_day:
    st.error("End day must be >= start day.")
    st.stop()

start_utc, end_utc = madrid_date_to_api_range(start_day, end_day)
st.write(f"API UTC request window: `{start_utc}` → `{end_utc}`")
st.info("The charts below use Madrid local time on the x-axis.")

run = st.button("Fetch ESIOS P48 thermal gap", type="primary", use_container_width=True)

if run:
    token = get_esios_token()
    indicators = {"Day-ahead price": DAY_AHEAD_PRICE_ID, "Total scheduled demand P48": DEMAND_P48_ID}
    indicators.update(P48_COMPONENTS)

    raw = fetch_many_indicators(indicators, start_utc, end_utc, token)
    if raw.empty:
        st.warning("No ESIOS data returned.")
        st.stop()

    wide = build_wide_balance(raw)
    df = calculate_thermal_gap(wide, non_thermal)
    monthly = calculate_monthly_stats(df)

    st.subheader("Overlay — conventional volumes + day-ahead price")
    st.caption(
        "Eje Y izquierdo: volúmenes en MWh/h (barras apiladas por tecnología convencional + línea negra de hueco térmico). "
        "Eje Y derecho: precio day-ahead en €/MWh (línea azul)."
    )

    stack_cols = [c for c in show_stack_components if c in df.columns]
    if stack_cols:
        stack_df = df[["datetime_madrid", "thermal_gap_mwh"] + stack_cols].melt(
            id_vars=["datetime_madrid", "thermal_gap_mwh"],
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
            .mark_bar(opacity=0.85)
            .encode(
                x=base_x,
                y=alt.Y(
                    "mwh:Q",
                    title="Conventional volumes (MWh/h)",
                    stack="zero",
                    axis=alt.Axis(titleColor="#111827", labelColor="#111827"),
                ),
                color=alt.Color(
                    "component:N",
                    title="Conventional technologies",
                    legend=alt.Legend(orient="right"),
                ),
                tooltip=[
                    alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                    alt.Tooltip("component:N", title="Technology"),
                    alt.Tooltip("mwh:Q", title="MWh/h", format=",.0f"),
                ],
            )
        )

        gap_line = (
            alt.Chart(df)
            .mark_line(color="black", strokeDash=[6, 4], strokeWidth=2)
            .encode(
                x=base_x,
                y=alt.Y(
                    "thermal_gap_mwh:Q",
                    title="Conventional volumes (MWh/h)",
                    axis=alt.Axis(titleColor="#111827", labelColor="#111827"),
                ),
                tooltip=[
                    alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                    alt.Tooltip("thermal_gap_mwh:Q", title="Thermal gap", format=",.0f"),
                    alt.Tooltip("Total scheduled demand P48:Q", title="Demand P48", format=",.0f"),
                    alt.Tooltip("non_thermal_mwh:Q", title="Non-thermal", format=",.0f"),
                ],
            )
        )

        # Left axis only: volumes + thermal gap share the same scale
        left_layer = alt.layer(bars, gap_line)

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
            combined = alt.layer(left_layer, price_line).resolve_scale(y="independent").properties(height=450)
        else:
            combined = left_layer.properties(height=450)

        st.altair_chart(combined, use_container_width=True)

        st.markdown(
            "- **Barras apiladas**: volúmenes por tecnología convencional (**eje Y izquierdo**)\n"
            "- **Línea negra discontinua**: hueco térmico total calculado (**eje Y izquierdo**)\n"
            "- **Línea azul**: precio day-ahead (**eje Y derecho**)"
        )

    if "Day-ahead price" in df.columns:
        st.subheader("Scatter — price vs thermal gap")
        scatter = (
            alt.Chart(df)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("thermal_gap_mwh:Q", title="Thermal gap MWh/h"),
                y=alt.Y("Day-ahead price:Q", title="Day-ahead price €/MWh"),
                color=alt.Color("hour_madrid:O", title="Madrid hour"),
                tooltip=[
                    alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                    alt.Tooltip("thermal_gap_mwh:Q", title="Thermal gap", format=",.0f"),
                    alt.Tooltip("Day-ahead price:Q", title="€/MWh", format=",.2f"),
                    alt.Tooltip("hour_madrid:O", title="Hour"),
                ],
            )
            .properties(height=450)
        )
        st.altair_chart(scatter, use_container_width=True)

    st.subheader("P48 components")
    stack_cols = [c for c in show_stack_components if c in df.columns]
    if stack_cols:
        stack_df = df[["datetime_madrid"] + stack_cols].melt(
            id_vars="datetime_madrid",
            var_name="component",
            value_name="mwh",
        )
        stack_chart = (
            alt.Chart(stack_df)
            .mark_area(opacity=0.75)
            .encode(
                x=alt.X("datetime_madrid:T", title="Madrid date and hour", axis=alt.Axis(format="%d-%b %H:%M", labelAngle=-45)),
                y=alt.Y("mwh:Q", title="MWh/h"),
                color=alt.Color("component:N", title="Component"),
                tooltip=[
                    alt.Tooltip("datetime_madrid:T", title="Madrid time", format="%d-%b-%Y %H:%M"),
                    alt.Tooltip("component:N", title="Component"),
                    alt.Tooltip("mwh:Q", title="MWh/h", format=",.0f"),
                ],
            )
            .properties(height=360)
        )
        st.altair_chart(stack_chart, use_container_width=True)

    st.subheader("Monthly stats")
    st.dataframe(monthly, use_container_width=True, hide_index=True)

    with st.expander("Hourly data", expanded=False):
        st.dataframe(df, use_container_width=True, hide_index=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button(
            "Download hourly thermal gap CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"esios_p48_thermal_gap_hourly_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "Download monthly stats CSV",
            monthly.to_csv(index=False).encode("utf-8"),
            file_name=f"esios_p48_thermal_gap_monthly_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c3:
        st.download_button(
            "Download raw indicators CSV",
            raw.to_csv(index=False).encode("utf-8"),
            file_name=f"esios_raw_indicators_{start_day}_{end_day}.csv",
            mime="text/csv",
            use_container_width=True,
        )
