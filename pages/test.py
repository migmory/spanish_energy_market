from __future__ import annotations

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
# TEST — REE official demand + monthly peak GW
# =========================================================
st.set_page_config(page_title="REE official demand + peak test", layout="wide")

BASE_DIR = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(BASE_DIR / ".env")
    load_dotenv()

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE = "#1D4ED8"
ORANGE = "#EA580C"
RED = "#DC2626"
GREY = "#64748B"

REE_DEMAND_URL = "https://apidatos.ree.es/es/datos/demanda/evolucion"
ESIOS_1293_URL = "https://api.esios.ree.es/indicators/1293"
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
    ">🔎 REE official demand test | monthly GWh + max GW</div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "Recommended demand series for report KPIs: REE apidatos `demanda/evolucion`. "
    "This version also calculates monthly peak demand GW using hourly REE data and, if token is available, ESIOS 1293 max as a diagnostic."
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


def ree_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": "NexwellPower-REE-Demand-Test/1.1",
    }


def esios_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Content-Type": "application/json",
        "x-api-key": token,
        "User-Agent": "NexwellPower-REE-Demand-Test/1.1",
    }


def safe_json(response: requests.Response) -> dict[str, Any] | list[Any]:
    try:
        return response.json()
    except Exception:
        return {"non_json_body_preview": (response.text or "")[:5000]}


def first_scalar(value):
    if isinstance(value, list):
        if not value:
            return None
        return first_scalar(value[0])
    if isinstance(value, dict):
        for key in ["value", "datetime", "datetime_utc", "date", "name", "id"]:
            if key in value:
                return first_scalar(value.get(key))
        return None
    return value


def parse_dt_series(series: pd.Series) -> pd.Series:
    cleaned = series.map(first_scalar)
    try:
        return pd.to_datetime(cleaned, errors="coerce", utc=True, format="mixed")
    except TypeError:
        return pd.to_datetime(cleaned, errors="coerce", utc=True)


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


def fmt_gwh(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.1f} GWh"


def fmt_gw(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.2f} GW"


def fmt_mw(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.0f} MW"


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):+,.1%}"


def delta_html(v) -> str:
    if v is None or pd.isna(v):
        return '<span style="color:#94A3B8;">→ n/a vs prev. period</span>'
    color = "#16A34A" if float(v) >= 0 else "#DC2626"
    arrow = "↑" if float(v) >= 0 else "↓"
    return f'<span style="color:{color}; font-weight:800;">{arrow} {fmt_pct(v)} vs prev. period</span>'


# =========================================================
# REE demanda/evolucion parser
# =========================================================
def flatten_ree_demand_payload(payload: dict[str, Any], source_label: str, time_trunc: str) -> pd.DataFrame:
    rows = []
    included = payload.get("included", []) if isinstance(payload, dict) else []
    if isinstance(included, dict):
        included = [included]

    for item in included if isinstance(included, list) else []:
        if not isinstance(item, dict):
            continue
        attrs = item.get("attributes", {}) if isinstance(item.get("attributes"), dict) else {}
        title = attrs.get("title") or item.get("type") or source_label
        values = attrs.get("values") or item.get("values") or []
        if isinstance(values, dict):
            values = [values]

        for v in values if isinstance(values, list) else []:
            if not isinstance(v, dict):
                continue
            rows.append(
                {
                    "series": title,
                    "datetime": first_scalar(v.get("datetime") or v.get("date")),
                    "value": first_scalar(v.get("value")),
                    "percentage": first_scalar(v.get("percentage")),
                    "source": source_label,
                    "time_trunc": time_trunc,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["datetime"] = parse_dt_series(df["datetime"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    if df.empty:
        return df

    local_dt = df["datetime"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    df["local_datetime"] = local_dt
    df["period"] = local_dt.dt.to_period("M").dt.to_timestamp()
    return df.sort_values("local_datetime").reset_index(drop=True)


def fetch_ree_demanda_evolucion(
    start_day: date,
    end_day: date,
    *,
    geo_variant: str,
    time_trunc: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    params: list[tuple[str, str]] = [
        ("start_date", datetime.combine(start_day, time(0, 0)).strftime("%Y-%m-%dT%H:%M")),
        ("end_date", datetime.combine(end_day, time(23, 59)).strftime("%Y-%m-%dT%H:%M")),
        ("time_trunc", time_trunc),
    ]

    if geo_variant == "peninsular electric_system":
        params.extend(
            [
                ("geo_trunc", "electric_system"),
                ("geo_limit", "peninsular"),
                ("geo_ids", PENINSULA_GEO_ID),
            ]
        )
    elif geo_variant == "geo_ids only":
        params.append(("geo_ids", PENINSULA_GEO_ID))
    elif geo_variant == "no geo":
        pass

    response = requests.get(REE_DEMAND_URL, headers=ree_headers(), params=params, timeout=60)
    payload = safe_json(response)
    df = flatten_ree_demand_payload(payload, source_label=f"REE demanda/evolucion | {geo_variant}", time_trunc=time_trunc) if response.ok and isinstance(payload, dict) else pd.DataFrame()

    info = {
        "source": f"REE demanda/evolucion {time_trunc}",
        "geo_variant": geo_variant,
        "http": response.status_code,
        "url": response.url,
        "rows": int(len(df)),
        "response_chars": len(response.text or ""),
        "payload": payload if not response.ok or df.empty else None,
    }
    return df, info


def best_ree_series(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    all_df = pd.concat(frames, ignore_index=True)
    priority = {
        "REE demanda/evolucion | peninsular electric_system": 0,
        "REE demanda/evolucion | geo_ids only": 1,
        "REE demanda/evolucion | no geo": 2,
    }
    all_df["priority"] = all_df["source"].map(priority).fillna(99)
    best_source = all_df.sort_values("priority")["source"].iloc[0]
    best = all_df[all_df["source"] == best_source].copy()

    if best["series"].nunique() > 1:
        demand_like = best[best["series"].astype(str).str.contains("demanda", case=False, na=False)].copy()
        if not demand_like.empty:
            best = demand_like
    return best


def fetch_official_monthly_demand(start_day: date, end_day: date) -> tuple[pd.DataFrame, list[dict], dict[str, Any]]:
    variants = ["peninsular electric_system", "geo_ids only", "no geo"]
    frames = []
    infos = []
    raw = {}

    for variant in variants:
        try:
            df, info = fetch_ree_demanda_evolucion(start_day, end_day, geo_variant=variant, time_trunc="month")
            infos.append(info)
            if not df.empty:
                frames.append(df)
            if info.get("payload") is not None:
                raw[f"month | {variant}"] = info.get("payload")
        except Exception as exc:
            infos.append({"source": "REE month", "geo_variant": variant, "http": "ERROR", "url": "", "rows": 0, "error": str(exc)[:500]})

    best = best_ree_series(frames)
    if best.empty:
        return pd.DataFrame(), infos, raw

    # Monthly value is energy in MWh. Convert to GWh.
    out = (
        best.groupby(["period", "source"], as_index=False)
        .agg(raw_mwh=("value", "sum"))
        .sort_values("period")
        .reset_index(drop=True)
    )
    out["demand_gwh"] = out["raw_mwh"] / 1000.0
    out["hours_in_period"] = out["period"].dt.days_in_month * 24
    out["avg_demand_mw"] = out["demand_gwh"] * 1000.0 / out["hours_in_period"]
    return out, infos, raw


def fetch_ree_hourly_peak_gw(start_day: date, end_day: date) -> tuple[pd.DataFrame, list[dict], dict[str, Any]]:
    variants = ["peninsular electric_system", "geo_ids only", "no geo"]
    frames = []
    infos = []
    raw = {}

    for variant in variants:
        try:
            df, info = fetch_ree_demanda_evolucion(start_day, end_day, geo_variant=variant, time_trunc="hour")
            infos.append(info)
            if not df.empty:
                frames.append(df)
            if info.get("payload") is not None:
                raw[f"hour | {variant}"] = info.get("payload")
        except Exception as exc:
            infos.append({"source": "REE hour", "geo_variant": variant, "http": "ERROR", "url": "", "rows": 0, "error": str(exc)[:500]})

    best = best_ree_series(frames)
    if best.empty:
        return pd.DataFrame(), infos, raw

    # Hourly value from demanda/evolucion is energy in MWh for the hour.
    # For a one-hour period, MWh equals average MW over that hour.
    best["hourly_avg_mw"] = best["value"]
    best["hourly_avg_gw"] = best["hourly_avg_mw"] / 1000.0

    idx = best.groupby("period")["hourly_avg_gw"].idxmax()
    peak = best.loc[idx, ["period", "local_datetime", "hourly_avg_mw", "hourly_avg_gw", "source"]].copy()
    peak = peak.rename(columns={"local_datetime": "peak_hour"})
    return peak.sort_values("period").reset_index(drop=True), infos, raw


# =========================================================
# Optional ESIOS 1293 max diagnostic
# =========================================================
def fetch_esios_1293_monthly_max(token: str, start_day: date, end_day: date) -> tuple[pd.DataFrame, list[dict], dict[str, Any]]:
    if not token:
        return pd.DataFrame(), [], {}

    params = [
        ("start_date", datetime.combine(start_day, time(0, 0)).strftime("%Y-%m-%dT%H:%M:%S")),
        ("end_date", datetime.combine(end_day, time(23, 55)).strftime("%Y-%m-%dT%H:%M:%S")),
        ("time_trunc", "month"),
        ("time_agg", "max"),
        ("geo_agg", "max"),
        ("locale", "es"),
        ("geo_ids[]", PENINSULA_GEO_ID),
    ]
    response = requests.get(ESIOS_1293_URL, headers=esios_headers(token), params=params, timeout=60)
    payload = safe_json(response)
    values = payload.get("indicator", {}).get("values", []) if isinstance(payload, dict) else []

    rows = []
    for item in values if isinstance(values, list) else []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "datetime": first_scalar(item.get("datetime") or item.get("datetime_utc") or item.get("date")),
                "esios_1293_max_mw": first_scalar(item.get("value")),
                "geo_id": first_scalar(item.get("geo_id") or item.get("geoId") or item.get("geo_ids")),
                "geo_name": first_scalar(item.get("geo_name") or item.get("geoName")) or "Península",
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = parse_dt_series(df["datetime"])
        df["esios_1293_max_mw"] = pd.to_numeric(df["esios_1293_max_mw"], errors="coerce")
        df = df.dropna(subset=["datetime", "esios_1293_max_mw"]).copy()
        if not df.empty:
            df["period"] = df["datetime"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None).dt.to_period("M").dt.to_timestamp()
            df["esios_1293_max_gw"] = df["esios_1293_max_mw"] / 1000.0
            df = df[["period", "esios_1293_max_mw", "esios_1293_max_gw", "geo_id", "geo_name"]]

    info = {
        "source": "ESIOS 1293 monthly max",
        "geo_variant": "geo_ids[] 8741",
        "http": response.status_code,
        "url": response.url,
        "rows": int(len(df)),
        "response_chars": len(response.text or ""),
        "payload": payload if not response.ok or df.empty else None,
    }
    raw = {"ESIOS 1293 monthly max": info["payload"]} if info.get("payload") is not None else {}
    return df, [info], raw


def build_demand_with_peak(start_day: date, end_day: date, token: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    monthly, monthly_infos, monthly_raw = fetch_official_monthly_demand(start_day, end_day)
    hourly_peak, hourly_infos, hourly_raw = fetch_ree_hourly_peak_gw(start_day, end_day)
    esios_peak, esios_infos, esios_raw = fetch_esios_1293_monthly_max(token, start_day, end_day) if token else (pd.DataFrame(), [], {})

    diagnostics = pd.DataFrame(
        [{k: v for k, v in info.items() if k != "payload"} for info in (monthly_infos + hourly_infos + esios_infos)]
    )
    raw = {}
    raw.update(monthly_raw)
    raw.update(hourly_raw)
    raw.update(esios_raw)

    if monthly.empty and hourly_peak.empty and esios_peak.empty:
        return pd.DataFrame(), diagnostics, raw

    out = monthly.copy()

    # Ensure all optional columns exist even if the hourly/ESIOS requests return no rows.
    optional_defaults = {
        "peak_hour": pd.NaT,
        "hourly_avg_mw": pd.NA,
        "hourly_avg_gw": pd.NA,
        "esios_1293_max_mw": pd.NA,
        "esios_1293_max_gw": pd.NA,
    }

    if not hourly_peak.empty:
        out = out.merge(hourly_peak[["period", "peak_hour", "hourly_avg_mw", "hourly_avg_gw"]], on="period", how="outer")
    if not esios_peak.empty:
        out = out.merge(esios_peak[["period", "esios_1293_max_mw", "esios_1293_max_gw"]], on="period", how="outer")

    for col, default in optional_defaults.items():
        if col not in out.columns:
            out[col] = default

    # Ensure required monthly columns also exist if only hourly/ESIOS data came back.
    for col in ["demand_gwh", "avg_demand_mw", "raw_mwh"]:
        if col not in out.columns:
            out[col] = pd.NA
    if "source" not in out.columns:
        out["source"] = "REE demanda/evolucion"

    out = out.sort_values("period").reset_index(drop=True)
    out["prev_demand_gwh"] = pd.to_numeric(out["demand_gwh"], errors="coerce").shift(1)
    out["prev_avg_demand_mw"] = pd.to_numeric(out["avg_demand_mw"], errors="coerce").shift(1)
    out["prev_peak_hourly_gw"] = pd.to_numeric(out["hourly_avg_gw"], errors="coerce").shift(1)
    out["demand_delta_pct"] = pd.to_numeric(out["demand_gwh"], errors="coerce") / out["prev_demand_gwh"] - 1
    out["avg_mw_delta_pct"] = pd.to_numeric(out["avg_demand_mw"], errors="coerce") / out["prev_avg_demand_mw"] - 1
    out["peak_hourly_delta_pct"] = pd.to_numeric(out["hourly_avg_gw"], errors="coerce") / out["prev_peak_hourly_gw"] - 1
    out["period_label"] = pd.to_datetime(out["period"], errors="coerce").dt.strftime("%b-%Y")
    return out, diagnostics, raw


# =========================================================
# UI
# =========================================================
token = get_secret_or_env("ESIOS_TOKEN", "ESIOS_API_TOKEN", "REE_ESIOS_TOKEN")

c1, c2, c3 = st.columns(3)
with c1:
    start_day = st.date_input("Start date", value=date(2026, 1, 1))
with c2:
    end_day = st.date_input("End date", value=date(2026, 5, 31))
with c3:
    st.markdown("#### Optional ESIOS token")
    if token:
        st.success("Token found. ESIOS max diagnostic enabled.")
    else:
        st.info("No token found. REE demand + hourly peak still works.")

st.info(
    "Recommended report demand: REE `demanda/evolucion` monthly GWh. "
    "Monthly max GW is calculated from REE hourly `demanda/evolucion` as the highest hourly average demand in the month."
)

run = st.button("Run REE official demand + max GW test", type="primary", use_container_width=True)

if run:
    with st.spinner("Pulling REE monthly demand and hourly peak demand..."):
        demand, diagnostics, raw_payloads = build_demand_with_peak(start_day, end_day, token)

    section("Request diagnostics")
    st.dataframe(diagnostics, use_container_width=True, hide_index=True)

    if demand.empty:
        st.error("No demand rows returned.")
        if raw_payloads:
            with st.expander("Raw payload previews", expanded=True):
                st.code(json.dumps(raw_payloads, ensure_ascii=False, indent=2)[:20000], language="json")
        st.stop()

    section("Latest period quick read")
    latest = demand.dropna(subset=["period"]).iloc[-1]
    prev = demand.iloc[-2] if len(demand) >= 2 else None

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric(f"Demand total | {latest['period_label']}", fmt_gwh(latest.get("demand_gwh")))
        st.markdown(delta_html(latest.get("demand_delta_pct")), unsafe_allow_html=True)
    with k2:
        st.metric(f"Average demand | {latest['period_label']}", fmt_mw(latest.get("avg_demand_mw")))
        st.markdown(delta_html(latest.get("avg_mw_delta_pct")), unsafe_allow_html=True)
    with k3:
        st.metric(f"Max hourly demand | {latest['period_label']}", fmt_gw(latest.get("hourly_avg_gw")))
        st.markdown(delta_html(latest.get("peak_hourly_delta_pct")), unsafe_allow_html=True)
        if pd.notna(latest.get("peak_hour")):
            st.caption(f"Peak hour: {pd.Timestamp(latest['peak_hour']):%d-%b-%Y %H:%M}")
    with k4:
        st.metric("ESIOS 1293 max diagnostic", fmt_gw(latest.get("esios_1293_max_gw")))
        st.caption("Optional 5-min indicator max if token/API supports time_agg=max")

    section("Monthly demand and peak chart")
    plot = demand.copy()

    bars = (
        alt.Chart(plot)
        .mark_bar(color=BLUE, opacity=0.76)
        .encode(
            x=alt.X("period:T", title="Month"),
            y=alt.Y("demand_gwh:Q", title="Demand total (GWh)", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("period_label:N", title="Month"),
                alt.Tooltip("demand_gwh:Q", title="Demand GWh", format=",.1f"),
                alt.Tooltip("avg_demand_mw:Q", title="Average MW", format=",.0f"),
                alt.Tooltip("hourly_avg_gw:Q", title="Max hourly GW", format=",.2f"),
                alt.Tooltip("peak_hour:T", title="Peak hour", format="%d-%b-%Y %H:%M"),
            ],
        )
    )

    peak_line = (
        alt.Chart(plot)
        .mark_line(color=RED, point=True, strokeWidth=3)
        .encode(
            x=alt.X("period:T"),
            y=alt.Y("hourly_avg_gw:Q", title="Demand peak (GW)", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("period_label:N", title="Month"),
                alt.Tooltip("hourly_avg_gw:Q", title="Max hourly GW", format=",.2f"),
                alt.Tooltip("peak_hour:T", title="Peak hour", format="%d-%b-%Y %H:%M"),
            ],
        )
    )

    plot["avg_demand_mw_gw"] = pd.to_numeric(plot["avg_demand_mw"], errors="coerce") / 1000.0

    avg_line = (
        alt.Chart(plot)
        .mark_line(color=ORANGE, point=True, strokeDash=[6, 4], strokeWidth=2.5)
        .encode(
            x=alt.X("period:T"),
            y=alt.Y("avg_demand_mw_gw:Q", title="Demand peak / average (GW)", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("period_label:N", title="Month"),
                alt.Tooltip("avg_demand_mw:Q", title="Average MW", format=",.0f"),
            ],
        )
    )

    st.altair_chart(
        alt.layer(bars, peak_line, avg_line).resolve_scale(y="independent").properties(height=440),
        use_container_width=True,
    )

    section("Monthly table")
    table_cols = [
        "period_label",
        "source",
        "raw_mwh",
        "demand_gwh",
        "avg_demand_mw",
        "hourly_avg_gw",
        "peak_hour",
        "esios_1293_max_gw",
        "demand_delta_pct",
        "avg_mw_delta_pct",
        "peak_hourly_delta_pct",
    ]
    for col in table_cols:
        if col not in demand.columns:
            demand[col] = pd.NA
    table = demand[table_cols].copy()
    table = table.rename(
        columns={
            "period_label": "Month",
            "source": "Source",
            "raw_mwh": "Raw REE monthly value (MWh)",
            "demand_gwh": "Demand total (GWh)",
            "avg_demand_mw": "Average demand (MW)",
            "hourly_avg_gw": "Max hourly demand (GW)",
            "peak_hour": "Peak hour",
            "esios_1293_max_gw": "ESIOS 1293 max diagnostic (GW)",
            "demand_delta_pct": "Δ demand vs prev. (%)",
            "avg_mw_delta_pct": "Δ avg MW vs prev. (%)",
            "peak_hourly_delta_pct": "Δ max hourly GW vs prev. (%)",
        }
    )
    st.dataframe(
        table.style.format(
            {
                "Raw REE monthly value (MWh)": "{:,.0f}",
                "Demand total (GWh)": "{:,.1f}",
                "Average demand (MW)": "{:,.0f}",
                "Max hourly demand (GW)": "{:,.2f}",
                "ESIOS 1293 max diagnostic (GW)": "{:,.2f}",
                "Δ demand vs prev. (%)": "{:+.1%}",
                "Δ avg MW vs prev. (%)": "{:+.1%}",
                "Δ max hourly GW vs prev. (%)": "{:+.1%}",
            },
            na_rep="—",
        ),
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Interpretation", expanded=True):
        st.markdown(
            """
            - **Demand total (GWh):** REE `demanda/evolucion` monthly value, converted from MWh to GWh.
            - **Average demand (MW):** monthly GWh converted back to average MW across the month.
            - **Max hourly demand (GW):** highest hourly value from REE `demanda/evolucion` with `time_trunc=hour`.
              This is an hourly-average peak, not necessarily the absolute 5-minute instantaneous peak.
            - **ESIOS 1293 max diagnostic:** optional 5-minute indicator max if ESIOS accepts `time_agg=max`.
              Keep it as diagnostic until reconciled against REE.
            """
        )

    if raw_payloads:
        with st.expander("Raw payload previews", expanded=False):
            st.code(json.dumps(raw_payloads, ensure_ascii=False, indent=2)[:25000], language="json")
