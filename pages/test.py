from __future__ import annotations

import os
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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
# TEST — Embalses.net + REE monthly average hourly demand profile
# =========================================================
st.set_page_config(page_title="Hydro reservoirs + demand profile test", layout="wide")

BASE_DIR = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(BASE_DIR / ".env", override=True)
    load_dotenv(override=True)

MADRID_TZ = ZoneInfo("Europe/Madrid")
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE = "#1D4ED8"
ORANGE = "#EA580C"

EMBALSES_URL = "https://www.embalses.net/"
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

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
    ">🧪 Test | Embalses + monthly demand hourly profile</div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "This test scrapes national reservoir levels from Embalses.net and builds a REE Península average hourly demand profile by month using `demanda/evolucion`."
)


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


def to_float_es(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    s = str(value).replace("\xa0", " ").strip()
    s = re.sub(r"[^\d,\.\-+]", "", s)
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def fmt_num(v, decimals=1, suffix=""):
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.{decimals}f}{suffix}"


def fmt_pct(v, decimals=1):
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.{decimals}f}%"


def delta_color_html(value, suffix="", decimals=1, good_when_up=True):
    if value is None or pd.isna(value):
        return '<span style="color:#94A3B8;">→ n/a</span>'
    v = float(value)
    positive = v >= 0
    good = positive if good_when_up else not positive
    color = "#16A34A" if good else "#DC2626"
    arrow = "↑" if positive else "↓"
    return f'<span style="color:{color};font-weight:800;">{arrow} {v:+,.{decimals}f}{suffix}</span>'


def safe_json(response: requests.Response):
    try:
        return response.json()
    except Exception:
        return {"non_json_body_preview": (response.text or "")[:4000]}


# =========================================================
# Embalses.net scraper
# =========================================================
def _clean_html_text(html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&sup3;", "3").replace("&#179;", "3")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _match_after(text: str, label_pattern: str, number_pattern: str = r"[-+]?\d{1,3}(?:\.\d{3})*(?:,\d+)?|[-+]?\d+(?:,\d+)?") -> float | None:
    pat = label_pattern + r"\s*[:\-]?\s*(" + number_pattern + r")"
    m = re.search(pat, text, flags=re.I)
    if not m:
        return None
    return to_float_es(m.group(1))


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_embalses_home() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    headers = {
        "User-Agent": "Mozilla/5.0 NexwellPower-Embalses-Test/1.0",
        "Accept": "text/html,application/xhtml+xml",
    }
    resp = requests.get(EMBALSES_URL, headers=headers, timeout=45)
    html = resp.text or ""
    text = _clean_html_text(html)

    summary = {
        "http": resp.status_code,
        "url": resp.url,
        "response_chars": len(html),
        "scrape_time": datetime.now(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "date": None,
        "stored_hm3": None,
        "stored_pct": None,
        "weekly_var_hm3": None,
        "weekly_var_pct": None,
        "capacity_hm3": None,
        "same_week_last_year": None,
        "same_week_last_year_hm3": None,
        "same_week_last_year_pct": None,
        "same_week_10y_avg_hm3": None,
        "same_week_10y_avg_pct": None,
    }

    m_date = re.search(r"Agua embalsada\s*\((\d{2}-\d{2}-\d{4})\)", text, flags=re.I)
    if m_date:
        summary["date"] = m_date.group(1)

    m_head = re.search(
        r"Agua embalsada\s*\([^)]+\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if m_head:
        summary["stored_hm3"] = to_float_es(m_head.group(1))
        summary["stored_pct"] = to_float_es(m_head.group(2))
    else:
        summary["stored_hm3"] = _match_after(text, r"Agua embalsada\s*\([^)]+\)")

    m_var = re.search(
        r"Variaci[oó]n semana Anterior\s*[:\-]?\s*([-+]?[0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([-+]?[0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if m_var:
        summary["weekly_var_hm3"] = to_float_es(m_var.group(1))
        summary["weekly_var_pct"] = to_float_es(m_var.group(2))

    summary["capacity_hm3"] = _match_after(text, r"Capacidad")

    m_ly = re.search(
        r"Misma Semana\s*\(\s*(\d{4})\s*\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if m_ly:
        summary["same_week_last_year"] = m_ly.group(1)
        summary["same_week_last_year_hm3"] = to_float_es(m_ly.group(2))
        summary["same_week_last_year_pct"] = to_float_es(m_ly.group(3))

    m_10y = re.search(
        r"Misma Semana\s*\(\s*Med\.\s*10\s*Años\s*\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if m_10y:
        summary["same_week_10y_avg_hm3"] = to_float_es(m_10y.group(1))
        summary["same_week_10y_avg_pct"] = to_float_es(m_10y.group(2))

    tables_out = []
    try:
        tables = pd.read_html(html, decimal=",", thousands=".")
        for i, t in enumerate(tables):
            tmp = t.copy()
            tmp.columns = [str(c).strip() for c in tmp.columns]
            tmp["table_id"] = i
            tables_out.append(tmp)
    except Exception:
        pass

    all_tables = pd.concat(tables_out, ignore_index=True) if tables_out else pd.DataFrame()

    bars = pd.DataFrame(
        [
            {"metric": "Current", "hm3": summary.get("stored_hm3"), "pct": summary.get("stored_pct")},
            {"metric": f"Same week LY ({summary.get('same_week_last_year', 'LY')})", "hm3": summary.get("same_week_last_year_hm3"), "pct": summary.get("same_week_last_year_pct")},
            {"metric": "Same week 10Y avg", "hm3": summary.get("same_week_10y_avg_hm3"), "pct": summary.get("same_week_10y_avg_pct")},
        ]
    ).dropna(subset=["hm3"], how="all")

    diagnostics = {
        "http": resp.status_code,
        "url": resp.url,
        "response_chars": len(html),
        "tables_found": len(tables_out),
        "text_preview": text[:3000],
    }
    return summary, bars, all_tables, diagnostics


# =========================================================
# REE demanda/evolucion hourly and monthly demand profile
# =========================================================
def parse_ree_included_series(payload: dict, value_field: str = "value") -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or item.get("id")
        for val in attrs.get("values", []) or []:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None)
            rows.append(
                {
                    "datetime": dt,
                    "title": str(title).strip(),
                    value_field: pd.to_numeric(val.get(value_field), errors="coerce"),
                }
            )
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_ree_demand_evolution(start_day: date, end_day: date, time_trunc: str = "hour") -> tuple[pd.DataFrame, dict[str, Any]]:
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": time_trunc,
        **REE_PENINSULAR_PARAMS,
    }
    url = f"{REE_API_BASE}/demanda/evolucion"
    resp = requests.get(url, params=params, timeout=60)
    payload = safe_json(resp)
    if not resp.ok or not isinstance(payload, dict):
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}

    df = parse_ree_included_series(payload, value_field="value")
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
        # Hourly value from demanda/evolucion is MWh in the hour; equivalent to average MW over that hour.
        df["hourly_avg_mw"] = df["value"]
        df["hourly_avg_gw"] = df["hourly_avg_mw"] / 1000.0
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
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
    }
    return df.sort_values("datetime").reset_index(drop=True), info


def month_bounds(d: date) -> tuple[date, date]:
    start = date(d.year, d.month, 1)
    if d.month == 12:
        end = date(d.year, 12, 31)
    else:
        end = date(d.year, d.month + 1, 1) - timedelta(days=1)
    return start, end


def previous_month_bounds(d: date) -> tuple[date, date]:
    first = date(d.year, d.month, 1)
    prev_end = first - timedelta(days=1)
    return month_bounds(prev_end)


def build_hourly_profile(hourly: pd.DataFrame, label: str) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "avg_gw", "min_gw", "max_gw", "label"])
    out = (
        hourly.groupby("hour", as_index=False)
        .agg(
            avg_gw=("hourly_avg_gw", "mean"),
            min_gw=("hourly_avg_gw", "min"),
            max_gw=("hourly_avg_gw", "max"),
            obs=("hourly_avg_gw", "count"),
        )
    )
    out["label"] = label
    return out


def build_daily_avg_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "avg_gw", "max_gw", "min_gw"])
    return (
        hourly.groupby("date", as_index=False)
        .agg(avg_gw=("hourly_avg_gw", "mean"), max_gw=("hourly_avg_gw", "max"), min_gw=("hourly_avg_gw", "min"))
    )


def build_monthly_summary(hourly: pd.DataFrame, month_df: pd.DataFrame | None = None) -> dict[str, Any]:
    summary = {"demand_gwh": None, "avg_demand_gw": None, "max_hourly_gw": None, "peak_hour": None, "days": None}
    if hourly is not None and not hourly.empty:
        summary["days"] = hourly["date"].nunique()
        summary["avg_demand_gw"] = hourly["hourly_avg_gw"].mean()
        summary["demand_gwh"] = hourly["hourly_avg_gw"].sum()  # GW * 1h = GWh
        idx = hourly["hourly_avg_gw"].idxmax()
        summary["max_hourly_gw"] = hourly.loc[idx, "hourly_avg_gw"]
        summary["peak_hour"] = hourly.loc[idx, "datetime"]

    # Keep the direct monthly API as diagnostic for completed/closed months.
    if month_df is not None and not month_df.empty:
        m = month_df.iloc[-1]
        if pd.notna(m.get("demand_gwh")):
            summary["demand_gwh_monthly_api"] = float(m["demand_gwh"])
            summary["avg_demand_gw_monthly_api"] = float(m["avg_demand_gw"])
    return summary


# =========================================================
# UI controls
# =========================================================
today = datetime.now(MADRID_TZ).date()
default_month_date = date(today.year, today.month, 1)

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    selected_month = st.date_input("Demand month to test", value=default_month_date)
with c2:
    compare_prev_month = st.checkbox("Compare with previous month", value=True)
with c3:
    end_cutoff = st.date_input(
        "End cut-off for selected month",
        value=today,
        help="For current month this can be today / latest available. For closed months leave month-end.",
    )

run = st.button("Run embalses + demand profile test", type="primary", use_container_width=True)

if run:
    section("1) Embalses.net national reservoir levels")
    with st.spinner("Scraping Embalses.net homepage..."):
        emb_summary, emb_bars, emb_tables, emb_diag = fetch_embalses_home()

    if emb_summary.get("http") != 200:
        st.error(f"Embalses.net returned HTTP {emb_summary.get('http')}")
    else:
        st.caption(f"Source: {emb_summary.get('url')} | scrape time: {emb_summary.get('scrape_time')}")

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Stored water", fmt_num(emb_summary.get("stored_hm3"), 0, " hm³"))
        st.caption(f"Date: {emb_summary.get('date') or '—'}")
    with k2:
        st.metric("Reservoir level", fmt_pct(emb_summary.get("stored_pct"), 2))
        st.markdown(delta_color_html(emb_summary.get("weekly_var_pct"), suffix=" pp weekly", decimals=2, good_when_up=True), unsafe_allow_html=True)
    with k3:
        st.metric("Capacity", fmt_num(emb_summary.get("capacity_hm3"), 0, " hm³"))
        st.caption("National total")
    with k4:
        st.metric("Weekly variation", fmt_num(emb_summary.get("weekly_var_hm3"), 0, " hm³"))
        st.markdown(delta_color_html(emb_summary.get("weekly_var_hm3"), suffix=" hm³", decimals=0, good_when_up=True), unsafe_allow_html=True)

    if not emb_bars.empty:
        bars = (
            alt.Chart(emb_bars.dropna(subset=["hm3"]))
            .mark_bar(size=52)
            .encode(
                x=alt.X("metric:N", title=None, sort=None),
                y=alt.Y("hm3:Q", title="Stored water (hm³)", scale=alt.Scale(zero=False)),
                color=alt.Color("metric:N", legend=None),
                tooltip=[
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("hm3:Q", title="hm³", format=",.0f"),
                    alt.Tooltip("pct:Q", title="%", format=",.2f"),
                ],
            )
            .properties(height=330)
        )
        st.altair_chart(bars, use_container_width=True)

    with st.expander("Embalses.net tables / diagnostics", expanded=False):
        st.json(emb_diag)
        if not emb_tables.empty:
            st.dataframe(emb_tables.head(250), use_container_width=True)
        else:
            st.info("No HTML tables parsed. Headline metrics were parsed from page text.")
        st.code(emb_diag.get("text_preview", ""), language="text")

    section("2) REE Península demand: monthly average hourly shape")
    start_sel, natural_end_sel = month_bounds(pd.Timestamp(selected_month).date())
    selected_end = min(pd.Timestamp(end_cutoff).date(), natural_end_sel)
    if selected_end < start_sel:
        selected_end = natural_end_sel

    if compare_prev_month:
        prev_start, prev_end = previous_month_bounds(start_sel)
    else:
        prev_start = prev_end = None

    with st.spinner("Pulling REE demanda/evolucion hourly data..."):
        selected_hourly, sel_info = fetch_ree_demand_evolution(start_sel, selected_end, time_trunc="hour")
        selected_monthly_api, sel_month_info = fetch_ree_demand_evolution(start_sel, selected_end, time_trunc="month")
        if compare_prev_month and prev_start is not None and prev_end is not None:
            prev_hourly, prev_info = fetch_ree_demand_evolution(prev_start, prev_end, time_trunc="hour")
            prev_monthly_api, prev_month_info = fetch_ree_demand_evolution(prev_start, prev_end, time_trunc="month")
        else:
            prev_hourly, prev_info, prev_monthly_api, prev_month_info = pd.DataFrame(), {}, pd.DataFrame(), {}

    if selected_hourly.empty:
        st.error("No hourly demand rows returned from REE demanda/evolucion.")
        st.json(sel_info)
    else:
        sel_label = f"{start_sel:%b-%Y}" if selected_end == natural_end_sel else f"{start_sel:%b-%Y} MTD to {selected_end:%d-%b}"
        prev_label = f"{prev_start:%b-%Y}" if compare_prev_month and prev_start is not None else "Previous month"

        sel_summary = build_monthly_summary(selected_hourly, selected_monthly_api)
        prev_summary = build_monthly_summary(prev_hourly, prev_monthly_api) if not prev_hourly.empty else {}

        demand_delta = avg_delta = peak_delta = None
        if prev_summary:
            if prev_summary.get("demand_gwh") not in [None, 0] and sel_summary.get("demand_gwh") is not None:
                demand_delta = sel_summary["demand_gwh"] / prev_summary["demand_gwh"] - 1
            if prev_summary.get("avg_demand_gw") not in [None, 0] and sel_summary.get("avg_demand_gw") is not None:
                avg_delta = sel_summary["avg_demand_gw"] / prev_summary["avg_demand_gw"] - 1
            if prev_summary.get("max_hourly_gw") not in [None, 0] and sel_summary.get("max_hourly_gw") is not None:
                peak_delta = sel_summary["max_hourly_gw"] / prev_summary["max_hourly_gw"] - 1

        d1, d2, d3, d4 = st.columns(4)
        with d1:
            st.metric(f"Demand total | {sel_label}", fmt_num(sel_summary.get("demand_gwh"), 1, " GWh"))
            st.markdown(delta_color_html(None if demand_delta is None else demand_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False), unsafe_allow_html=True)
        with d2:
            st.metric("Average hourly demand", fmt_num(sel_summary.get("avg_demand_gw"), 2, " GW"))
            st.markdown(delta_color_html(None if avg_delta is None else avg_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False), unsafe_allow_html=True)
        with d3:
            st.metric("Max hourly demand", fmt_num(sel_summary.get("max_hourly_gw"), 2, " GW"))
            st.markdown(delta_color_html(None if peak_delta is None else peak_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False), unsafe_allow_html=True)
        with d4:
            st.metric("Days included", fmt_num(sel_summary.get("days"), 0, " d"))
            if sel_summary.get("peak_hour") is not None:
                st.caption(f"Peak hour: {pd.Timestamp(sel_summary['peak_hour']):%d-%b %H:%M}")

        sel_profile = build_hourly_profile(selected_hourly, sel_label)
        profiles = [sel_profile]
        if compare_prev_month and not prev_hourly.empty:
            profiles.append(build_hourly_profile(prev_hourly, prev_label))
        profile_df = pd.concat(profiles, ignore_index=True)

        profile_chart = (
            alt.Chart(profile_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("avg_gw:Q", title="Average hourly demand (GW)", scale=alt.Scale(zero=False)),
                color=alt.Color("label:N", title="Month"),
                tooltip=[
                    alt.Tooltip("label:N", title="Month"),
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip("avg_gw:Q", title="Average GW", format=",.2f"),
                    alt.Tooltip("min_gw:Q", title="Min GW", format=",.2f"),
                    alt.Tooltip("max_gw:Q", title="Max GW", format=",.2f"),
                    alt.Tooltip("obs:Q", title="Hours obs.", format=",.0f"),
                ],
            )
            .properties(height=420)
            .configure_legend(orient="top", direction="horizontal")
        )
        st.altair_chart(profile_chart, use_container_width=True)

        st.caption(
            "Shape chart: hourly values from REE `demanda/evolucion` are grouped by hour of day and averaged across the selected month/cut-off."
        )

        daily_avg = build_daily_avg_profile(selected_hourly)
        if not daily_avg.empty:
            daily_chart = (
                alt.Chart(daily_avg)
                .mark_line(point=True, strokeWidth=2.5, color=BLUE)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("avg_gw:Q", title="Daily average demand (GW)", scale=alt.Scale(zero=False)),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"),
                        alt.Tooltip("avg_gw:Q", title="Avg GW", format=",.2f"),
                        alt.Tooltip("max_gw:Q", title="Max GW", format=",.2f"),
                        alt.Tooltip("min_gw:Q", title="Min GW", format=",.2f"),
                    ],
                )
                .properties(height=280)
            )
            st.altair_chart(daily_chart, use_container_width=True)

        section("3) Demand raw tables")
        st.dataframe(
            profile_df.style.format({"avg_gw": "{:,.2f}", "min_gw": "{:,.2f}", "max_gw": "{:,.2f}", "obs": "{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
        )

        with st.expander("REE request diagnostics", expanded=False):
            st.markdown("**Selected month hourly**")
            st.json(sel_info)
            st.markdown("**Selected month monthly API**")
            st.json(sel_month_info)
            if compare_prev_month:
                st.markdown("**Previous month hourly**")
                st.json(prev_info)
                st.markdown("**Previous month monthly API**")
                st.json(prev_month_info)
