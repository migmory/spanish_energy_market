import json
import os
from datetime import date, datetime, time
from typing import Any

import pandas as pd
import requests
import streamlit as st


# =========================================================
# ESIOS DEMAND TEST — Indicator 1293
# Goal: test period-specific demand pull and diagnose the Peninsular filter.
# =========================================================
st.set_page_config(page_title="ESIOS 1293 demand test", layout="wide")

st.markdown(
    """
    <div style="
        padding:18px 22px;
        border-radius:18px;
        background:linear-gradient(90deg,#0F766E 0%,#10B981 58%,#C7F3E2 100%);
        color:white;
        font-weight:900;
        font-size:1.55rem;
        margin-bottom:12px;
    ">🧪 ESIOS demand test | Indicator 1293</div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "Diagnostic page to test ESIOS indicator 1293 for a selected period and inspect which geo filter returns the Peninsular series."
)

API_BASE = "https://api.esios.ree.es/indicators/1293"


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


def build_headers(token: str, auth_style: str) -> dict[str, str]:
    if auth_style == 'Token token="..."':
        authorization = f'Token token="{token}"'
    elif auth_style == "Token token=...":
        authorization = f"Token token={token}"
    elif auth_style == "Bearer ...":
        authorization = f"Bearer {token}"
    else:
        authorization = token

    return {
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Content-Type": "application/json",
        "Authorization": authorization,
        "User-Agent": "NexwellPower-Streamlit-ESIOS-Diagnostic/1.0",
    }


def _safe_extract_values(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    e·sios indicator payloads are commonly:
      {"indicator": {"values": [...]}}
    This function also tolerates nested variants so the page keeps being useful
    if the wrapper changes slightly.
    """
    candidates = []
    if isinstance(payload, dict):
        indicator = payload.get("indicator")
        if isinstance(indicator, dict) and isinstance(indicator.get("values"), list):
            candidates = indicator["values"]
        elif isinstance(payload.get("values"), list):
            candidates = payload["values"]

    rows: list[dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "datetime": item.get("datetime") or item.get("datetime_utc") or item.get("date"),
                "value": item.get("value"),
                "geo_id": item.get("geo_id") or item.get("geoId"),
                "geo_name": item.get("geo_name") or item.get("geoName"),
                "tz_time": item.get("tz_time") or item.get("datetime_local"),
                "raw": item,
            }
        )
    return rows


def payload_to_df(payload: dict[str, Any]) -> pd.DataFrame:
    rows = _safe_extract_values(payload)
    if not rows:
        return pd.DataFrame(columns=["datetime", "value", "geo_id", "geo_name", "tz_time"])
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    if df.empty:
        return pd.DataFrame(columns=["datetime", "value", "geo_id", "geo_name", "tz_time"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    return df[["datetime", "month", "value", "geo_id", "geo_name", "tz_time"]].sort_values("datetime").reset_index(drop=True)


def request_variant(
    *,
    token: str,
    auth_style: str,
    start_dt: datetime,
    end_dt: datetime,
    geo_mode: str,
    geo_text: str,
    time_trunc: str,
    time_agg: str,
    geo_agg: str,
    include_locale: bool,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    params: list[tuple[str, str]] = [
        ("start_date", start_dt.strftime("%Y-%m-%dT%H:%M:%S")),
        ("end_date", end_dt.strftime("%Y-%m-%dT%H:%M:%S")),
        ("time_trunc", time_trunc),
        ("time_agg", time_agg),
        ("geo_agg", geo_agg),
    ]
    if include_locale:
        params.append(("locale", "es"))

    geo_ids = [x.strip() for x in geo_text.split(",") if x.strip()]
    if geo_mode == "geo_ids[]" and geo_ids:
        for geo_id in geo_ids:
            params.append(("geo_ids[]", geo_id))
    elif geo_mode == "geo_ids" and geo_ids:
        params.append(("geo_ids", ",".join(geo_ids)))
    elif geo_mode == "geo_id" and geo_ids:
        params.append(("geo_id", geo_ids[0]))
    # geo_mode == "no geo" intentionally sends no geoid filter.

    response = requests.get(
        API_BASE,
        headers=build_headers(token, auth_style),
        params=params,
        timeout=45,
    )
    info = {
        "status_code": response.status_code,
        "url": response.url,
        "content_type": response.headers.get("content-type", ""),
        "response_chars": len(response.text or ""),
    }
    try:
        payload = response.json()
    except Exception:
        payload = {"non_json_body_preview": (response.text or "")[:3000]}
    df = payload_to_df(payload) if response.ok else pd.DataFrame(columns=["datetime", "month", "value", "geo_id", "geo_name", "tz_time"])
    return payload, df, info


def style_diag(df: pd.DataFrame):
    if df.empty:
        return df
    return (
        df.style
        .set_properties(**{"font-size": "0.90rem", "padding": "6px 8px"})
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#0F766E"), ("color", "white"), ("font-weight", "800")]},
                {"selector": "td", "props": [("border-bottom", "1px solid #E2E8F0")]},
            ]
        )
    )


# =========================================================
# Controls
# =========================================================
token = get_secret_or_env("ESIOS_TOKEN", "ESIOS_API_TOKEN", "REE_ESIOS_TOKEN")

top_left, top_right = st.columns([1.2, 1.0])
with top_left:
    start_day = st.date_input("Start date", value=date(2025, 1, 1), key="esios_1293_start")
    end_day = st.date_input("End date", value=date(2026, 5, 31), key="esios_1293_end")
with top_right:
    st.markdown("#### Token")
    if token:
        st.success("Token found in `st.secrets` or environment variables.")
    else:
        st.error("No token found. Add `ESIOS_TOKEN` to Streamlit secrets or environment variables.")
    st.caption("The token is never printed in this page.")

c1, c2, c3, c4 = st.columns(4)
with c1:
    auth_style = st.selectbox(
        "Authorization header style",
        ['Token token="..."', "Token token=...", "Bearer ..."],
        index=0,
    )
with c2:
    geo_text = st.text_input(
        "Geo ID candidate(s)",
        value="8741",
        help="Editable candidate. The page also tests a no-geo request to inspect what the API returns.",
    )
with c3:
    time_trunc = st.selectbox("time_trunc", ["month", "day", "hour"], index=0)
with c4:
    aggregation_pack = st.selectbox(
        "Aggregation",
        ["time=sum | geo=sum", "time=avg | geo=avg", "time=sum | geo=avg"],
        index=0,
    )

if aggregation_pack == "time=avg | geo=avg":
    time_agg, geo_agg = "avg", "avg"
elif aggregation_pack == "time=sum | geo=avg":
    time_agg, geo_agg = "sum", "avg"
else:
    time_agg, geo_agg = "sum", "sum"

include_locale = st.checkbox("Include locale=es", value=True)

st.markdown("---")
run = st.button("Run ESIOS 1293 diagnostic matrix", type="primary", use_container_width=True)

if run:
    if not token:
        st.stop()
    if start_day > end_day:
        st.error("Start date cannot be after end date.")
        st.stop()

    start_dt = datetime.combine(start_day, time(0, 0, 0))
    end_dt = datetime.combine(end_day, time(23, 55, 0))

    variants = ["geo_ids[]", "geo_ids", "geo_id", "no geo"]
    diag_rows = []
    payloads: dict[str, dict[str, Any]] = {}
    frames: dict[str, pd.DataFrame] = {}

    with st.spinner("Calling ESIOS API variants..."):
        for geo_mode in variants:
            try:
                payload, df, info = request_variant(
                    token=token,
                    auth_style=auth_style,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    geo_mode=geo_mode,
                    geo_text=geo_text,
                    time_trunc=time_trunc,
                    time_agg=time_agg,
                    geo_agg=geo_agg,
                    include_locale=include_locale,
                )
                payloads[geo_mode] = payload
                frames[geo_mode] = df
                geo_names = []
                if not df.empty and "geo_name" in df.columns:
                    geo_names = sorted({str(x) for x in df["geo_name"].dropna().unique().tolist()})
                diag_rows.append(
                    {
                        "Variant": geo_mode,
                        "HTTP": info["status_code"],
                        "Rows": len(df),
                        "Geo names": ", ".join(geo_names[:5]),
                        "Response chars": info["response_chars"],
                        "URL": info["url"],
                    }
                )
            except Exception as exc:
                payloads[geo_mode] = {"exception": str(exc)}
                frames[geo_mode] = pd.DataFrame()
                diag_rows.append(
                    {
                        "Variant": geo_mode,
                        "HTTP": "ERROR",
                        "Rows": 0,
                        "Geo names": "",
                        "Response chars": 0,
                        "URL": str(exc),
                    }
                )

    diag = pd.DataFrame(diag_rows)
    st.markdown("### 1) Request diagnostics")
    st.dataframe(style_diag(diag), use_container_width=True)

    # Pick best variant: first successful with rows, preferring a geo-filtered request.
    chosen_variant = None
    for candidate in ["geo_ids[]", "geo_ids", "geo_id", "no geo"]:
        df = frames.get(candidate, pd.DataFrame())
        if df is not None and not df.empty:
            chosen_variant = candidate
            break

    if chosen_variant is None:
        st.error("No request variant returned parseable values. Expand the raw payloads below.")
    else:
        chosen = frames[chosen_variant].copy()
        st.success(f"Best parseable variant: `{chosen_variant}` with {len(chosen):,} rows.")
        st.markdown("### 2) Parsed values")
        st.dataframe(chosen, use_container_width=True)

        if "month" in chosen.columns:
            monthly = (
                chosen.groupby(["month", "geo_id", "geo_name"], dropna=False, as_index=False)["value"]
                .sum()
                .rename(columns={"value": "indicator_value_sum"})
            )
            st.markdown("### 3) Monthly aggregate check")
            st.dataframe(monthly, use_container_width=True)

            if not monthly.empty:
                chart_df = monthly.copy()
                chart_df["month"] = pd.to_datetime(chart_df["month"] + "-01", errors="coerce")
                chart_df = chart_df.dropna(subset=["month"])
                if not chart_df.empty:
                    st.line_chart(
                        chart_df.set_index("month")["indicator_value_sum"],
                        height=280,
                    )

    st.markdown("### 4) Raw payload preview")
    for variant in variants:
        with st.expander(f"Payload preview — {variant}", expanded=False):
            preview = json.dumps(payloads.get(variant, {}), ensure_ascii=False, indent=2)
            st.code(preview[:12000], language="json")

st.markdown("---")
st.caption(
    "Use this as a test bench first. Once the correct geo parameter is identified, the same request can be copied into Day Ahead / Monthly / Weekly production loaders."
)
