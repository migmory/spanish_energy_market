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
# Solarpark / UNITY API test
# Purpose: download 10-min power values and aggregate to 15-min production
# for 4 parks during May.
# =========================================================

st.set_page_config(page_title="Solarpark production test", layout="wide")

BASE = "https://portal.solarpark-online.com"
MADRID_TZ = ZoneInfo("Europe/Madrid")

if load_dotenv is not None:
    load_dotenv(override=True)

# Confirmed / inferred source IDs from /ifms/sources/values/v2 responses.
# These look like plant-level Power kW signals.
POWER_SOURCE_IDS = {
    "Carmona Central 36": "d19246ea-065e-11f0-980e-42010afa015a",
    "Carmona Central 36.1": "cc211458-0892-11f0-9eeb-42010afa015a",
    "Palma del Condado Solar 555": "7113ab0e-2726-11f0-b9a2-42010afa015a",
    # Guarroman: chosen because it has 10-min power-shaped values, max ~5,003 kW.
    # Similar source c60814c2 also tracks it but has small negative night noise.
    "Guarroman Solar 81": "e1e421b8-1382-11f0-85ad-42010afa015a",
}

ID_TO_SITE = {v: k for k, v in POWER_SOURCE_IDS.items()}


def get_cookie() -> str:
    """
    The portal request uses Cookie auth, not Authorization Bearer.

    Local .env:
        SOLARPARK_COOKIE='IFMSCK=...'

    Streamlit Cloud Secrets:
        SOLARPARK_COOKIE = "IFMSCK=..."
    """
    cookie = ""
    try:
        cookie = str(st.secrets.get("SOLARPARK_COOKIE", "") or "")
    except Exception:
        cookie = ""

    if not cookie:
        cookie = os.getenv("SOLARPARK_COOKIE", "")

    if not cookie:
        st.error(
            "Missing SOLARPARK_COOKIE. Put it in local .env or Streamlit Secrets. "
            "Example: SOLARPARK_COOKIE='IFMSCK=xxxxxxxx'"
        )
        st.stop()

    return str(cookie).strip().strip('"').strip("'")


def madrid_day_to_utc_str(d: date, end_of_day: bool = False) -> str:
    """
    The browser request sends UTC timestamps.
    For Spain local day boundaries:
      local 2026-05-01 00:00 Europe/Madrid -> UTC string
      local 2026-06-01 00:00 Europe/Madrid -> UTC string
    """
    dt_local = datetime.combine(d, time(0, 0), tzinfo=MADRID_TZ)
    if end_of_day:
        dt_local = dt_local + timedelta(days=1)
    dt_utc = dt_local.astimezone(ZoneInfo("UTC"))
    return dt_utc.strftime("%Y%m%dT%H%M%SZ")


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

    r = requests.post(
        url,
        params=params,
        json=source_ids,
        headers=headers,
        timeout=120,
    )

    if not r.ok:
        raise RuntimeError(
            f"Solarpark request failed: HTTP {r.status_code}. "
            f"URL={r.url}. Body preview={r.text[:500]}"
        )

    return r.json()


def events_payload_to_power_df(payload: dict | list) -> pd.DataFrame:
    """
    Expected response:
      [{"events": [[timestamp_utc, value, source_id], ...]}]
    """
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

    df = df.dropna(subset=["datetime_utc", "site", "power_kw"]).copy()

    # Remove tiny negative night noise from some power signals.
    df["power_kw"] = df["power_kw"].clip(lower=0)

    return df[["datetime_utc", "datetime_madrid", "site", "source_id", "power_kw"]].sort_values(["site", "datetime_madrid"])


def power_10min_to_generation_15min(power_df: pd.DataFrame) -> pd.DataFrame:
    """
    The API events are 10-min. Convert average power kW to kWh per 10-min interval:
       kWh = kW * 10/60
    Then sum into 15-min buckets.
    """
    if power_df.empty:
        return pd.DataFrame(columns=["site", "datetime_madrid", "generation_kwh_15min"])

    tmp = power_df.copy()
    tmp["generation_kwh_10min"] = tmp["power_kw"] * (10.0 / 60.0)

    out = (
        tmp.set_index("datetime_madrid")
           .groupby("site")["generation_kwh_10min"]
           .resample("15min")
           .sum()
           .reset_index()
           .rename(columns={"generation_kwh_10min": "generation_kwh_15min"})
    )

    return out.sort_values(["site", "datetime_madrid"]).reset_index(drop=True)


def make_daily_summary(gen15: pd.DataFrame) -> pd.DataFrame:
    if gen15.empty:
        return pd.DataFrame(columns=["site", "date", "generation_mwh"])

    tmp = gen15.copy()
    tmp["date"] = tmp["datetime_madrid"].dt.date
    daily = (
        tmp.groupby(["site", "date"], as_index=False)
           .agg(generation_kwh=("generation_kwh_15min", "sum"))
    )
    daily["generation_mwh"] = daily["generation_kwh"] / 1000.0
    return daily[["site", "date", "generation_mwh"]]


def make_24h_average_profile(gen15: pd.DataFrame) -> pd.DataFrame:
    """
    Average 24h daily profile by site.

    The source data is 15-min generation (kWh per 15-min bucket).
    For each site and time-of-day bucket, calculate the average generation
    across all selected days.
    """
    if gen15.empty:
        return pd.DataFrame(columns=["site", "time_of_day", "hour_decimal", "avg_generation_kwh_15min", "avg_power_kw"])

    tmp = gen15.copy()
    tmp["time_of_day"] = tmp["datetime_madrid"].dt.strftime("%H:%M")
    tmp["hour_decimal"] = tmp["datetime_madrid"].dt.hour + tmp["datetime_madrid"].dt.minute / 60.0

    profile = (
        tmp.groupby(["site", "time_of_day", "hour_decimal"], as_index=False)
           .agg(
               avg_generation_kwh_15min=("generation_kwh_15min", "mean"),
               obs=("generation_kwh_15min", "count"),
           )
           .sort_values(["site", "hour_decimal"])
    )

    # Equivalent average power during each 15-min bucket:
    # kW = kWh / 0.25h
    profile["avg_power_kw"] = profile["avg_generation_kwh_15min"] / 0.25
    return profile


def make_month_summary(gen15: pd.DataFrame) -> pd.DataFrame:
    if gen15.empty:
        return pd.DataFrame(columns=["site", "generation_mwh", "obs_15min"])

    out = (
        gen15.groupby("site", as_index=False)
             .agg(
                 generation_kwh=("generation_kwh_15min", "sum"),
                 obs_15min=("generation_kwh_15min", "count"),
             )
    )
    out["generation_mwh"] = out["generation_kwh"] / 1000.0
    return out[["site", "generation_mwh", "obs_15min"]]


st.title("Solarpark / UNITY — May production test + 24h average profile")
st.caption(
    "Test using Cookie auth from the browser request. "
    "API source values appear at 10-min intervals; this page converts them to 15-min generation."
)

with st.expander("Configured power source IDs", expanded=False):
    st.json(POWER_SOURCE_IDS)

col1, col2 = st.columns(2)
with col1:
    start_day = st.date_input("Start day", value=date(2026, 5, 1))
with col2:
    end_day = st.date_input("End day inclusive", value=date(2026, 5, 31))

selected_sites = st.multiselect(
    "Sites",
    options=list(POWER_SOURCE_IDS.keys()),
    default=list(POWER_SOURCE_IDS.keys()),
)

if end_day < start_day:
    st.error("End day must be >= start day.")
    st.stop()

if not selected_sites:
    st.warning("Select at least one site.")
    st.stop()

selected_source_ids = [POWER_SOURCE_IDS[s] for s in selected_sites]

start_utc = madrid_day_to_utc_str(start_day)
# End is exclusive: next local midnight after selected end day.
end_utc = madrid_day_to_utc_str(end_day, end_of_day=True)

st.write(f"UTC request window: `{start_utc}` → `{end_utc}`")
st.info("For May 2026 in Europe/Madrid, local midnight is UTC 22:00 the previous day.")

run = st.button("Fetch production", type="primary", use_container_width=True)

if run:
    cookie = get_cookie()

    try:
        payload = fetch_power_values(start_utc, end_utc, cookie, selected_source_ids)
    except Exception as exc:
        st.error(f"Could not fetch Solarpark values: {exc}")
        st.stop()

    power_df = events_payload_to_power_df(payload)
    if power_df.empty:
        st.warning("No power events returned for the configured source IDs.")
        st.write("Raw payload preview:")
        st.json(payload if isinstance(payload, dict) else payload[:1])
        st.stop()

    gen15 = power_10min_to_generation_15min(power_df)
    daily = make_daily_summary(gen15)
    month_summary = make_month_summary(gen15)

    st.subheader("Monthly production summary")
    st.dataframe(month_summary, use_container_width=True, hide_index=True)
    st.caption("Below: average 24h profile calculated from all selected May days.")

    st.subheader("Average 24h generation profile by site")
    profile24 = make_24h_average_profile(gen15)

    metric = st.radio(
        "Profile metric",
        options=["Equivalent average power (kW)", "Average generation per 15-min bucket (kWh)"],
        horizontal=True,
        index=0,
    )

    if metric.startswith("Average generation"):
        y_field = "avg_generation_kwh_15min"
        y_title = "Avg generation per 15-min bucket (kWh)"
        tooltip_value = alt.Tooltip("avg_generation_kwh_15min:Q", title="Avg kWh/15min", format=",.2f")
    else:
        y_field = "avg_power_kw"
        y_title = "Equivalent average power (kW)"
        tooltip_value = alt.Tooltip("avg_power_kw:Q", title="Avg kW", format=",.0f")

    profile_chart = (
        alt.Chart(profile24)
        .mark_line(point=False, strokeWidth=3)
        .encode(
            x=alt.X(
                "hour_decimal:Q",
                title="Hour of day",
                scale=alt.Scale(domain=[0, 24]),
                axis=alt.Axis(values=list(range(0, 25, 2)), labelExpr="datum.value + ':00'"),
            ),
            y=alt.Y(f"{y_field}:Q", title=y_title),
            color=alt.Color("site:N", title="Site"),
            tooltip=[
                alt.Tooltip("site:N", title="Site"),
                alt.Tooltip("time_of_day:N", title="Time"),
                tooltip_value,
                alt.Tooltip("obs:Q", title="Obs", format=","),
            ],
        )
        .properties(height=460)
    )
    st.altair_chart(profile_chart, use_container_width=True)

    with st.expander("Show 24h profile data", expanded=False):
        st.dataframe(profile24, use_container_width=True, hide_index=True)

    st.subheader("Daily production")
    daily_chart = (
        alt.Chart(daily)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("generation_mwh:Q", title="Generation MWh/day"),
            color=alt.Color("site:N", title="Site"),
            tooltip=[
                alt.Tooltip("site:N", title="Site"),
                alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"),
                alt.Tooltip("generation_mwh:Q", title="MWh", format=",.2f"),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(daily_chart, use_container_width=True)

    st.subheader("15-min generation sample")
    st.dataframe(gen15.head(500), use_container_width=True, hide_index=True)

    csv = gen15.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download 15-min generation CSV",
        data=csv,
        file_name=f"solarpark_generation_15min_{start_day}_{end_day}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    with st.expander("Diagnostics", expanded=False):
        st.write("Power events rows:", len(power_df))
        st.write("15-min rows:", len(gen15))
        st.write("Source IDs returned:")
        st.write(sorted(power_df["source_id"].unique().tolist()))
        st.write("Max power by site:")
        st.dataframe(power_df.groupby("site", as_index=False)["power_kw"].max(), use_container_width=True, hide_index=True)
        st.write("Raw payload first object keys:")
        if isinstance(payload, list) and payload:
            st.write(list(payload[0].keys()))
        elif isinstance(payload, dict):
            st.write(list(payload.keys()))
