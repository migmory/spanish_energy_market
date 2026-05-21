from __future__ import annotations

import calendar
import json
import os
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass


# =========================================================
# ESIOS DEMAND TEST — Indicator 1293
# Monthly Peninsular demand in GWh + average MW
# =========================================================
st.set_page_config(page_title="ESIOS monthly demand test", layout="wide")

BASE_DIR = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(BASE_DIR / ".env")
    load_dotenv()

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE = "#1D4ED8"
ORANGE = "#EA580C"
GREY = "#64748B"
API_BASE = "https://api.esios.ree.es/indicators/1293"
PENINSULA_GEO_ID = "8741"


st.markdown(
    f"""
    <div style="
        padding:18px 22px;
        border-radius:18px;
        background:linear-gradient(90deg,{CORP_GREEN_DARK} 0%,{CORP_GREEN} 58%,#C7F3E2 100%);
        color:white;
        font-weight:900;
        font-size:1.55rem;
        margin-bottom:12px;
    ">🔎 ESIOS monthly demand test | Indicator 1293 — Península</div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "This page tests ESIOS indicator 1293 using x-api-key, geo_ids[]=8741. "
    "It shows monthly total demand in GWh and monthly average point demand in MW."
)


# =========================================================
# Helpers
# =========================================================
def get_secret_or_env(*names: str) -> str:
    for name in names:
        try:
            value = st.secrets.get(name)
            if value:
                return str(value)
        except Exception:
            pass
        value = os.getenv(name)
        if value:
            return str(value)
    return ""


def headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Content-Type": "application/json",
        "x-api-key": token,
        "User-Agent": "NexwellPower-Streamlit-ESIOS-Demand-Test/1.0",
    }


def first_scalar(value):
    """ESIOS sometimes returns scalar strings, lists, or small dicts depending on aggregation.
    Convert those safely before pandas datetime parsing."""
    if isinstance(value, list):
        if not value:
            return None
        return first_scalar(value[0])
    if isinstance(value, dict):
        for key in ["datetime", "datetime_utc", "date", "value"]:
            if key in value:
                return first_scalar(value.get(key))
        return None
    return value


def parse_esios_datetime_series(series: pd.Series) -> pd.Series:
    cleaned = series.map(first_scalar)
    try:
        return pd.to_datetime(cleaned, errors="coerce", utc=True, format="mixed")
    except TypeError:
        return pd.to_datetime(cleaned, errors="coerce", utc=True)


def request_esios_1293(
    *,
    token: str,
    start_day: date,
    end_day: date,
    time_agg: str,
    time_trunc: str = "month",
    geo_id: str = PENINSULA_GEO_ID,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    params: list[tuple[str, str]] = [
        ("start_date", datetime.combine(start_day, time(0, 0, 0)).strftime("%Y-%m-%dT%H:%M:%S")),
        ("end_date", datetime.combine(end_day, time(23, 55, 0)).strftime("%Y-%m-%dT%H:%M:%S")),
        ("time_trunc", time_trunc),
        ("time_agg", time_agg),
        ("geo_agg", "sum" if time_agg == "sum" else "avg"),
        ("locale", "es"),
        ("geo_ids[]", geo_id),
    ]
    response = requests.get(API_BASE, headers=headers(token), params=params, timeout=60)
    info = {
        "status_code": response.status_code,
        "url": response.url,
        "content_type": response.headers.get("content-type", ""),
        "response_chars": len(response.text or ""),
    }

    try:
        payload = response.json()
    except Exception:
        payload = {"non_json_body_preview": (response.text or "")[:2000]}

    if not response.ok:
        return pd.DataFrame(), {**info, "payload": payload}

    values = payload.get("indicator", {}).get("values", [])
    rows = []
    for item in values:
        rows.append(
            {
                "period_start": first_scalar(item.get("datetime") or item.get("datetime_utc") or item.get("date")),
                "value": first_scalar(item.get("value")),
                "geo_id": first_scalar(item.get("geo_id") or item.get("geoId") or item.get("geo_ids")),
                "geo_name": first_scalar(item.get("geo_name") or item.get("geoName")) or "Península",
                "datetime_utc": first_scalar(item.get("datetime_utc")),
                "tz_time": first_scalar(item.get("tz_time")),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df, {**info, "payload": payload}
    df["period_start"] = parse_esios_datetime_series(df["period_start"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["period_start", "value"]).copy()
    if df.empty:
        return df, {**info, "payload": payload, "parse_note": "Rows returned by API but datetime/value could not be parsed."}
    df["month"] = df["period_start"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None).dt.to_period("M").dt.to_timestamp()
    return df.sort_values("month").reset_index(drop=True), {**info, "payload": payload}


def days_in_month(ts: pd.Timestamp) -> int:
    return calendar.monthrange(int(ts.year), int(ts.month))[1]


def build_monthly_demand(token: str, start_day: date, end_day: date) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    # SUM: monthly sum of 5-min MW values. Energy MWh = sum(MW snapshots) / 12.
    sum_df, sum_info = request_esios_1293(
        token=token,
        start_day=start_day,
        end_day=end_day,
        time_agg="sum",
        time_trunc="month",
    )

    # AVG: average of 5-min MW values. This is the clean point average MW.
    avg_df, avg_info = request_esios_1293(
        token=token,
        start_day=start_day,
        end_day=end_day,
        time_agg="avg",
        time_trunc="month",
    )

    if sum_df.empty:
        return pd.DataFrame(), pd.DataFrame(), {"sum": sum_info, "avg": avg_info}

    out = sum_df[["month", "geo_id", "geo_name", "value"]].rename(columns={"value": "sum_5min_mw"})
    out["demand_mwh"] = out["sum_5min_mw"] / 12.0
    out["demand_gwh"] = out["demand_mwh"] / 1000.0

    if not avg_df.empty:
        avg_clean = avg_df[["month", "value"]].rename(columns={"value": "avg_mw_api"})
        out = out.merge(avg_clean, on="month", how="left")
    else:
        out["avg_mw_api"] = pd.NA

    # Fallback average if AVG endpoint fails: full-month equivalent average.
    # For current partial month, AVG endpoint is preferred because it reflects available data.
    out["hours_in_calendar_month"] = out["month"].map(lambda x: days_in_month(pd.Timestamp(x)) * 24)
    out["avg_mw_from_energy_full_month"] = out["demand_mwh"] / out["hours_in_calendar_month"]
    out["avg_mw"] = pd.to_numeric(out["avg_mw_api"], errors="coerce").combine_first(out["avg_mw_from_energy_full_month"])

    out["month_label"] = out["month"].dt.strftime("%b-%Y")
    out["prev_demand_gwh"] = out["demand_gwh"].shift(1)
    out["prev_avg_mw"] = out["avg_mw"].shift(1)
    out["demand_gwh_delta_pct"] = out["demand_gwh"] / out["prev_demand_gwh"] - 1
    out["avg_mw_delta_pct"] = out["avg_mw"] / out["prev_avg_mw"] - 1
    out["demand_gwh_delta_abs"] = out["demand_gwh"] - out["prev_demand_gwh"]
    out["avg_mw_delta_abs"] = out["avg_mw"] - out["prev_avg_mw"]

    diagnostics = pd.DataFrame(
        [
            {
                "request": "sum",
                "http": sum_info.get("status_code"),
                "response_chars": sum_info.get("response_chars"),
                "url": sum_info.get("url"),
            },
            {
                "request": "avg",
                "http": avg_info.get("status_code"),
                "response_chars": avg_info.get("response_chars"),
                "url": avg_info.get("url"),
            },
        ]
    )
    return out, diagnostics, {"sum": sum_info, "avg": avg_info}


def fmt_gwh(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.0f} GWh"


def fmt_mw(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.0f} MW"


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):+,.1%}"


def delta_html(v, unit: str) -> str:
    if v is None or pd.isna(v):
        return '<span style="color:#94A3B8;">→ n/a vs prev. month</span>'
    color = "#16A34A" if float(v) >= 0 else "#DC2626"
    arrow = "↑" if float(v) >= 0 else "↓"
    return f'<span style="color:{color}; font-weight:800;">{arrow} {fmt_pct(v)} vs prev. month</span>'


def section(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            margin-top:18px;
            margin-bottom:10px;
            padding:10px 14px;
            background:#F4FCF8;
            border-left:5px solid {CORP_GREEN};
            border-radius:8px;
            font-weight:850;
            color:#0F172A;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# Controls
# =========================================================
token = get_secret_or_env("ESIOS_TOKEN", "ESIOS_API_TOKEN", "REE_ESIOS_TOKEN")

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    start_day = st.date_input("Start date", value=date(2025, 1, 1))
with c2:
    end_day = st.date_input("End date", value=date(2026, 5, 31))
with c3:
    st.markdown("#### Token")
    if token:
        st.success("Token found.")
    else:
        st.error("No token found. Add ESIOS_TOKEN to Streamlit secrets or .env.")

st.caption(
    "Method: ESIOS 1293 returns 5-minute real demand values in MW. "
    "Monthly energy is calculated as sum(5-min MW values) / 12 = MWh, then / 1,000 = GWh. "
    "Average MW is requested directly with time_agg=avg."
)

run = st.button("Run monthly demand test", type="primary", use_container_width=True)

if run:
    if not token:
        st.stop()
    if start_day > end_day:
        st.error("Start date cannot be after end date.")
        st.stop()

    with st.spinner("Pulling ESIOS indicator 1293 monthly sum and avg..."):
        monthly, diagnostics, raw_infos = build_monthly_demand(token, start_day, end_day)

    section("Request diagnostics")
    st.dataframe(diagnostics, use_container_width=True, hide_index=True)

    if monthly.empty:
        st.error("No parseable ESIOS demand rows returned.")
        with st.expander("Raw response previews", expanded=False):
            st.code(json.dumps(raw_infos, ensure_ascii=False, indent=2)[:12000], language="json")
        st.stop()

    latest = monthly.dropna(subset=["demand_gwh"]).iloc[-1]
    prev = monthly.dropna(subset=["demand_gwh"]).iloc[-2] if len(monthly.dropna(subset=["demand_gwh"])) >= 2 else None

    section("Latest month quick read")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric(f"Total demand | {latest['month_label']}", fmt_gwh(latest["demand_gwh"]))
        st.markdown(delta_html(latest["demand_gwh_delta_pct"], "gwh"), unsafe_allow_html=True)
    with k2:
        st.metric(f"Average point demand | {latest['month_label']}", fmt_mw(latest["avg_mw"]))
        st.markdown(delta_html(latest["avg_mw_delta_pct"], "mw"), unsafe_allow_html=True)
    with k3:
        st.metric("Previous month demand", fmt_gwh(None if prev is None else prev["demand_gwh"]))
        st.caption("Monthly energy equivalent")
    with k4:
        st.metric("Previous month avg MW", fmt_mw(None if prev is None else prev["avg_mw"]))
        st.caption("Average 5-min point demand")

    section("Monthly demand chart")
    chart_df = monthly[["month", "month_label", "demand_gwh", "avg_mw"]].copy()

    bars = (
        alt.Chart(chart_df)
        .mark_bar(color=BLUE, opacity=0.78)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("demand_gwh:Q", title="Total demand (GWh)"),
            tooltip=[
                alt.Tooltip("month_label:N", title="Month"),
                alt.Tooltip("demand_gwh:Q", title="Demand GWh", format=",.0f"),
                alt.Tooltip("avg_mw:Q", title="Avg MW", format=",.0f"),
            ],
        )
    )

    line = (
        alt.Chart(chart_df)
        .mark_line(color=ORANGE, point=True, strokeWidth=3)
        .encode(
            x=alt.X("month:T"),
            y=alt.Y("avg_mw:Q", title="Average point demand (MW)"),
            tooltip=[
                alt.Tooltip("month_label:N", title="Month"),
                alt.Tooltip("avg_mw:Q", title="Avg MW", format=",.0f"),
            ],
        )
    )

    st.altair_chart(
        alt.layer(bars, line).resolve_scale(y="independent").properties(height=430),
        use_container_width=True,
    )

    section("Monthly table")
    table = monthly[
        [
            "month_label",
            "geo_id",
            "geo_name",
            "sum_5min_mw",
            "demand_gwh",
            "avg_mw",
            "demand_gwh_delta_abs",
            "demand_gwh_delta_pct",
            "avg_mw_delta_abs",
            "avg_mw_delta_pct",
        ]
    ].copy()
    table = table.rename(
        columns={
            "month_label": "Month",
            "geo_id": "Geo ID",
            "geo_name": "Geo",
            "sum_5min_mw": "Raw monthly sum of 5-min MW",
            "demand_gwh": "Demand total (GWh)",
            "avg_mw": "Average point demand (MW)",
            "demand_gwh_delta_abs": "Δ demand vs prev. (GWh)",
            "demand_gwh_delta_pct": "Δ demand vs prev. (%)",
            "avg_mw_delta_abs": "Δ avg MW vs prev. (MW)",
            "avg_mw_delta_pct": "Δ avg MW vs prev. (%)",
        }
    )
    st.dataframe(
        table.style.format(
            {
                "Raw monthly sum of 5-min MW": "{:,.0f}",
                "Demand total (GWh)": "{:,.0f}",
                "Average point demand (MW)": "{:,.0f}",
                "Δ demand vs prev. (GWh)": "{:+,.0f}",
                "Δ demand vs prev. (%)": "{:+.1%}",
                "Δ avg MW vs prev. (MW)": "{:+,.0f}",
                "Δ avg MW vs prev. (%)": "{:+.1%}",
            },
            na_rep="—",
        ),
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Raw payload previews", expanded=False):
        st.code(json.dumps(raw_infos["sum"].get("payload", {}), ensure_ascii=False, indent=2)[:10000], language="json")
        st.code(json.dumps(raw_infos["avg"].get("payload", {}), ensure_ascii=False, indent=2)[:10000], language="json")
