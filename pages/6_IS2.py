# is2_generation_revenues_dashboard.py
# Streamlit dashboard for IS2 operational solar parks:
# - 15-min generation profile by site
# - solar sanity checks to prevent night generation / bad forward-fill issues
# - hourly day-ahead revenue calculation
# - professional revenue layout and diagnostics
#
# Run:
#   streamlit run is2_generation_revenues_dashboard.py

from __future__ import annotations

import io
import os
from typing import Optional, Iterable

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

MADRID_TZ = "Europe/Madrid"
UTC_TZ = "UTC"

st.set_page_config(page_title="IS2 generation and revenues", page_icon="⚡", layout="wide")

st.markdown(
    """
    <style>
        .main .block-container {padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1500px;}
        div[data-testid="metric-container"] {
            background: #ffffff; border: 1px solid #e6eaf0; padding: 14px 16px;
            border-radius: 14px; box-shadow: 0 1px 2px rgba(15,23,42,.04);
        }
        .ok-box {background:#ecfdf5;color:#065f46;border:1px solid #bbf7d0;border-radius:12px;padding:12px 14px;margin:8px 0 16px 0;}
        .warning-box {background:#fff8e6;color:#7a5200;border:1px solid #ffe1a6;border-radius:12px;padding:12px 14px;margin:8px 0 16px 0;}
        .danger-box {background:#fef2f2;color:#991b1b;border:1px solid #fecaca;border-radius:12px;padding:12px 14px;margin:8px 0 16px 0;}
    </style>
    """,
    unsafe_allow_html=True,
)


def norm_col(c: str) -> str:
    return str(c).strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_").replace(".", "_")


def find_col(cols: Iterable[str], candidates: list[str]) -> Optional[str]:
    mapping = {norm_col(c): c for c in cols}
    for cand in candidates:
        if norm_col(cand) in mapping:
            return mapping[norm_col(cand)]
    for c in cols:
        nc = norm_col(c)
        if any(norm_col(cand) in nc for cand in candidates):
            return c
    return None


def read_uploaded_table(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    raw = uploaded_file.read()
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(raw))
    if name.endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(raw))
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(raw), sep=";", decimal=",", encoding="latin1")
    if name.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(raw))
    raise ValueError("Unsupported file type. Use CSV, XLSX, XLS or Parquet.")


def clean_numeric(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    txt = s.astype(str).str.strip()
    # Handles European 1.234,56 and normal 1234.56 reasonably well.
    euro_mask = txt.str.contains(",", regex=False) & txt.str.contains(".", regex=False)
    txt = txt.where(~euro_mask, txt.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    txt = txt.where(euro_mask, txt.str.replace(",", ".", regex=False))
    return pd.to_numeric(txt.replace({"nan": np.nan, "None": np.nan, "": np.nan}), errors="coerce")


def to_madrid_datetime(series: pd.Series, source_tz: str) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    if getattr(dt.dt, "tz", None) is not None:
        return dt.dt.tz_convert(MADRID_TZ)
    if source_tz == "UTC":
        return dt.dt.tz_localize(UTC_TZ, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(MADRID_TZ)
    return dt.dt.tz_localize(source_tz, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(MADRID_TZ)


def normalize_generation_units(values: pd.Series, unit: str) -> pd.Series:
    v = clean_numeric(values)
    if unit == "kWh per 15-min":
        return v / 1000.0
    if unit == "MWh per 15-min":
        return v
    if unit == "kW average in 15-min":
        return v * 0.25 / 1000.0
    if unit == "MW average in 15-min":
        return v * 0.25
    raise ValueError("Unknown generation unit.")


def fmt_eur(x: float) -> str:
    return "—" if pd.isna(x) else f"€{x:,.0f}"


def fmt_mwh(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} MWh"


def fmt_price(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f} €/MWh"


def plot_layout(title: str, subtitle: str = "", height: int = 480) -> dict:
    text = f"<b>{title}</b><br><sup>{subtitle}</sup>" if subtitle else f"<b>{title}</b>"
    return dict(
        title=dict(text=text, x=0.01, xanchor="left"),
        height=height,
        margin=dict(l=55, r=40, t=78, b=50),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(size=12),
    )


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_indicator_600(token: str, start_date_iso: str, end_date_iso: str, geo_id: int = 3) -> pd.DataFrame:
    url = "https://api.esios.ree.es/indicators/600"
    headers = {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "Authorization": f"Token token={token}",
        "x-api-key": token,
    }
    params = {
        "start_date": start_date_iso,
        "end_date": end_date_iso,
        "time_trunc": "hour",
        "geo_ids[]": geo_id,
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    values = r.json().get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["hour_madrid", "price_eur_mwh", "price_source"])
    df = pd.DataFrame(values)
    dt_col = "datetime_utc" if "datetime_utc" in df.columns else "datetime"
    df["hour_madrid"] = pd.to_datetime(df[dt_col], errors="coerce", utc=True).dt.tz_convert(MADRID_TZ).dt.floor("h")
    df["price_eur_mwh"] = pd.to_numeric(df["value"], errors="coerce")
    if "geo_id" in df.columns:
        df = df[df["geo_id"].astype(str).eq(str(geo_id)) | df["geo_id"].isna()]
    df = df.dropna(subset=["hour_madrid", "price_eur_mwh"]).sort_values("hour_madrid").drop_duplicates("hour_madrid", keep="last")
    df["price_source"] = "eSIOS indicator 600 — geo_id 3 España"
    return df[["hour_madrid", "price_eur_mwh", "price_source"]]


def prepare_price_file(price_raw: pd.DataFrame, datetime_col: str, price_col: str, source_tz: str) -> pd.DataFrame:
    df = price_raw.copy()
    df["hour_madrid"] = to_madrid_datetime(df[datetime_col], source_tz).dt.floor("h")
    df["price_eur_mwh"] = clean_numeric(df[price_col])
    out = df.dropna(subset=["hour_madrid", "price_eur_mwh"]).sort_values("hour_madrid").drop_duplicates("hour_madrid", keep="last")
    out["price_source"] = "Uploaded price file"
    return out[["hour_madrid", "price_eur_mwh", "price_source"]]


def price_quality_message(price_df: pd.DataFrame) -> tuple[str, str]:
    if price_df.empty:
        return "danger", "No hourly prices loaded."
    p = price_df["price_eur_mwh"].dropna()
    if p.empty:
        return "danger", "Price series is empty after parsing."
    avg, pmin, pmax = p.mean(), p.min(), p.max()
    if avg > 200 or pmax > 600 or pmin < -200:
        return "warning", f"Average {avg:.1f} €/MWh, min {pmin:.1f}, max {pmax:.1f}. Check source/unit: this may not be the expected OMIE Spain hourly price."
    return "ok", f"Average {avg:.1f} €/MWh, min {pmin:.1f}, max {pmax:.1f}."


def prepare_generation(raw, dt_col, site_col, gen_col, source_tz, gen_unit, apply_night_clean, night_start_hour, night_end_hour):
    df = raw.copy()
    df["datetime_madrid"] = to_madrid_datetime(df[dt_col], source_tz)
    df["site"] = df[site_col].astype(str).str.strip()
    df["generation_mwh_15min_raw"] = normalize_generation_units(df[gen_col], gen_unit)
    df = df.dropna(subset=["datetime_madrid", "site", "generation_mwh_15min_raw"]).sort_values(["site", "datetime_madrid"])
    df["datetime_madrid"] = df["datetime_madrid"].dt.floor("15min")
    # Important: never forward-fill generation. Sum duplicates only.
    df = df.groupby(["site", "datetime_madrid"], as_index=False)["generation_mwh_15min_raw"].sum().sort_values(["site", "datetime_madrid"])
    hour_decimal = df["datetime_madrid"].dt.hour + df["datetime_madrid"].dt.minute / 60.0
    is_night = (hour_decimal >= night_start_hour) | (hour_decimal < night_end_hour)
    df["is_night_sanity_window"] = is_night
    df["night_generation_mwh_raw"] = np.where(is_night, df["generation_mwh_15min_raw"], 0.0)
    df["generation_mwh_15min"] = np.where(is_night, 0.0, df["generation_mwh_15min_raw"]) if apply_night_clean else df["generation_mwh_15min_raw"]
    df["generation_kwh_15min"] = df["generation_mwh_15min"] * 1000.0
    df["hour_madrid"] = df["datetime_madrid"].dt.floor("h")
    df["month"] = df["datetime_madrid"].dt.to_period("M").astype(str)
    diag = df.groupby("site", as_index=False).agg(
        rows=("generation_mwh_15min", "size"),
        start=("datetime_madrid", "min"),
        end=("datetime_madrid", "max"),
        raw_generation_mwh=("generation_mwh_15min_raw", "sum"),
        clean_generation_mwh=("generation_mwh_15min", "sum"),
        night_generation_mwh_raw=("night_generation_mwh_raw", "sum"),
    )
    diag["night_generation_pct_raw"] = np.where(diag["raw_generation_mwh"] > 0, diag["night_generation_mwh_raw"] / diag["raw_generation_mwh"] * 100, 0.0)
    return df, diag


def calculate_revenues(gen: pd.DataFrame, price: pd.DataFrame):
    merged = gen.merge(price[["hour_madrid", "price_eur_mwh", "price_source"]], on="hour_madrid", how="left")
    merged["revenue_eur"] = merged["generation_mwh_15min"] * merged["price_eur_mwh"]
    merged["is_priced"] = merged["price_eur_mwh"].notna()
    monthly = merged.groupby(["site", "month"], as_index=False).agg(
        generation_mwh=("generation_mwh_15min", "sum"),
        revenue_eur=("revenue_eur", "sum"),
        priced_intervals=("is_priced", "sum"),
        total_intervals=("is_priced", "size"),
    )
    monthly["missing_price_intervals"] = monthly["total_intervals"] - monthly["priced_intervals"]
    monthly["captured_price_eur_mwh"] = np.where(monthly["generation_mwh"] > 0, monthly["revenue_eur"] / monthly["generation_mwh"], np.nan)
    price_month = price.copy()
    price_month["month"] = price_month["hour_madrid"].dt.to_period("M").astype(str)
    baseload = price_month.groupby("month", as_index=False)["price_eur_mwh"].mean().rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    monthly = monthly.merge(baseload, on="month", how="left")
    monthly["capture_factor_pct"] = monthly["captured_price_eur_mwh"] / monthly["baseload_price_eur_mwh"] * 100
    annual = merged.assign(year=merged["datetime_madrid"].dt.year).groupby(["site", "year"], as_index=False).agg(
        generation_mwh=("generation_mwh_15min", "sum"),
        revenue_eur=("revenue_eur", "sum"),
        priced_intervals=("is_priced", "sum"),
        total_intervals=("is_priced", "size"),
    )
    annual["missing_price_intervals"] = annual["total_intervals"] - annual["priced_intervals"]
    annual["captured_price_eur_mwh"] = np.where(annual["generation_mwh"] > 0, annual["revenue_eur"] / annual["generation_mwh"], np.nan)
    price_year = price.copy()
    price_year["year"] = price_year["hour_madrid"].dt.year
    annual_base = price_year.groupby("year", as_index=False)["price_eur_mwh"].mean().rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    annual = annual.merge(annual_base, on="year", how="left")
    annual["capture_factor_pct"] = annual["captured_price_eur_mwh"] / annual["baseload_price_eur_mwh"] * 100
    return merged, monthly, annual


# Sidebar
st.sidebar.title("IS2 controls")
gen_file = st.sidebar.file_uploader("Upload 15-min generation file", type=["csv", "xlsx", "xls", "parquet"])
st.sidebar.markdown("---")
st.sidebar.subheader("Generation parsing")
source_tz_gen = st.sidebar.selectbox("Generation timestamp source timezone", ["UTC", "Europe/Madrid"], index=0)
gen_unit = st.sidebar.selectbox("Generation column unit", ["kWh per 15-min", "MWh per 15-min", "kW average in 15-min", "MW average in 15-min"], index=0)
apply_solar_night_clean = st.sidebar.checkbox("Set impossible night generation to zero", value=True)
ca, cb = st.sidebar.columns(2)
night_start_hour = ca.number_input("Night starts", min_value=18, max_value=24, value=22, step=1)
night_end_hour = cb.number_input("Night ends", min_value=0, max_value=9, value=5, step=1)
st.sidebar.markdown("---")
st.sidebar.subheader("Price source")
price_source = st.sidebar.radio("Hourly price source", ["Upload price file", "Fetch eSIOS indicator 600"], index=0)
price_file = None
source_tz_price = "Europe/Madrid"
esios_token = None
if price_source == "Upload price file":
    price_file = st.sidebar.file_uploader("Upload hourly OMIE/price file", type=["csv", "xlsx", "xls", "parquet"], key="price_upload")
    source_tz_price = st.sidebar.selectbox("Price timestamp source timezone", ["UTC", "Europe/Madrid"], index=1)
else:
    esios_token = st.sidebar.text_input("eSIOS token", value=os.getenv("ESIOS_TOKEN", ""), type="password")
st.sidebar.markdown("---")
chart_height = st.sidebar.slider("Chart height", 350, 800, 520, 20)
show_diagnostics = st.sidebar.checkbox("Show diagnostics", value=True)

# Main
st.title("IS2 generation and day-ahead revenues")
st.caption("15-min solar profile, hourly price join, revenue analytics. Timestamps displayed in Europe/Madrid.")

if gen_file is None:
    st.info("Upload the IS2 15-min generation file to start.")
    st.stop()

try:
    raw_gen = read_uploaded_table(gen_file)
except Exception as exc:
    st.error(f"Could not read generation file: {exc}")
    st.stop()

dt_guess = find_col(raw_gen.columns, ["datetime_madrid", "datetime_utc", "datetime", "timestamp", "date", "time"])
site_guess = find_col(raw_gen.columns, ["site", "asset", "plant", "park", "name", "installation"])
gen_guess = find_col(raw_gen.columns, ["generation_kwh", "generation", "energy", "kwh", "mwh", "value"])

with st.expander("Column mapping", expanded=False):
    c1, c2, c3 = st.columns(3)
    dt_col = c1.selectbox("Generation datetime column", raw_gen.columns, index=list(raw_gen.columns).index(dt_guess) if dt_guess in raw_gen.columns else 0)
    site_col = c2.selectbox("Site column", raw_gen.columns, index=list(raw_gen.columns).index(site_guess) if site_guess in raw_gen.columns else 0)
    gen_col = c3.selectbox("Generation value column", raw_gen.columns, index=list(raw_gen.columns).index(gen_guess) if gen_guess in raw_gen.columns else 0)

gen_df, gen_diag = prepare_generation(raw_gen, dt_col, site_col, gen_col, source_tz_gen, gen_unit, apply_solar_night_clean, int(night_start_hour), int(night_end_hour))

price_df = pd.DataFrame(columns=["hour_madrid", "price_eur_mwh", "price_source"])
if price_source == "Upload price file" and price_file is not None:
    raw_price = read_uploaded_table(price_file)
    price_dt_guess = find_col(raw_price.columns, ["datetime_madrid", "datetime_utc", "datetime", "timestamp", "date", "hour"])
    price_val_guess = find_col(raw_price.columns, ["price_eur_mwh", "price", "value", "precio", "eur_mwh", "€/mwh"])
    with st.expander("Price column mapping", expanded=False):
        p1, p2 = st.columns(2)
        price_dt_col = p1.selectbox("Price datetime column", raw_price.columns, index=list(raw_price.columns).index(price_dt_guess) if price_dt_guess in raw_price.columns else 0)
        price_col = p2.selectbox("Price value column", raw_price.columns, index=list(raw_price.columns).index(price_val_guess) if price_val_guess in raw_price.columns else 0)
    price_df = prepare_price_file(raw_price, price_dt_col, price_col, source_tz_price)
elif price_source == "Fetch eSIOS indicator 600":
    if esios_token:
        start_utc = gen_df["datetime_madrid"].min().tz_convert(UTC_TZ).floor("D")
        end_utc = gen_df["datetime_madrid"].max().tz_convert(UTC_TZ).ceil("D") + pd.Timedelta(days=1)
        try:
            with st.spinner("Fetching hourly prices from eSIOS indicator 600..."):
                price_df = fetch_esios_indicator_600(esios_token, start_utc.isoformat(), end_utc.isoformat(), geo_id=3)
        except Exception as exc:
            st.error(f"Could not fetch eSIOS prices: {exc}")
    else:
        st.warning("Add your eSIOS token in the sidebar or upload a price file.")

if not price_df.empty:
    min_hour, max_hour = gen_df["hour_madrid"].min(), gen_df["hour_madrid"].max()
    price_df = price_df[(price_df["hour_madrid"] >= min_hour) & (price_df["hour_madrid"] <= max_hour)]

# KPI row
total_gen = gen_df["generation_mwh_15min"].sum()
raw_night = gen_diag["night_generation_mwh_raw"].sum()
raw_total = gen_diag["raw_generation_mwh"].sum()
night_pct = raw_night / raw_total * 100 if raw_total else 0
m1, m2, m3, m4 = st.columns(4)
m1.metric("Clean generation", fmt_mwh(total_gen))
m2.metric("Raw night generation", fmt_mwh(raw_night), f"{night_pct:.2f}% raw")
m3.metric("Sites", f"{gen_df['site'].nunique():,.0f}")
m4.metric("15-min intervals", f"{len(gen_df):,.0f}")

if raw_night > max(0.005 * raw_total, 0.01):
    st.markdown(f'<div class="warning-box"><b>Solar sanity warning:</b> raw data has {raw_night:,.2f} MWh during the night window ({night_pct:.2f}% of raw generation). This normally means timezone shift or forward-fill. Cleaning: <b>{"night values set to zero" if apply_solar_night_clean else "night values kept"}</b>.</div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="ok-box"><b>Solar sanity check passed:</b> raw night generation is negligible.</div>', unsafe_allow_html=True)

# Generation profile
st.subheader("15-min generation profile")
site_options = sorted(gen_df["site"].unique())
selected_sites = st.multiselect("Sites to plot", site_options, default=site_options[: min(len(site_options), 6)])
plot_df = gen_df[gen_df["site"].isin(selected_sites)]
fig = go.Figure()
for site in selected_sites:
    sub = plot_df[plot_df["site"] == site]
    fig.add_trace(go.Scatter(x=sub["datetime_madrid"], y=sub["generation_kwh_15min"], mode="lines", name=site, line=dict(width=1.8), hovertemplate="%{x|%d-%b %H:%M}<br>%{y:,.1f} kWh/15-min<extra>" + site + "</extra>"))
fig.update_layout(**plot_layout("15-min generation profile", "Cleaned generation; no forward-fill; Europe/Madrid", chart_height))
fig.update_xaxes(title="Madrid date and hour", showgrid=False)
fig.update_yaxes(title="Generation (kWh / 15-min)", gridcolor="#e8edf3", zeroline=True, zerolinecolor="#94a3b8")
st.plotly_chart(fig, use_container_width=True)

# Revenues
st.subheader("Day-ahead revenues — operational parks")
if price_df.empty:
    st.markdown('<div class="danger-box">No price series loaded. Upload a clean OMIE Spain hourly price file or fetch eSIOS indicator 600 with token.</div>', unsafe_allow_html=True)
    st.stop()

status, msg = price_quality_message(price_df)
box_class = {"ok": "ok-box", "warning": "warning-box", "danger": "danger-box"}[status]
st.markdown(f'<div class="{box_class}"><b>Price diagnostics:</b> {msg}</div>', unsafe_allow_html=True)
revenues_df, monthly, annual = calculate_revenues(gen_df, price_df)
priced_ratio = revenues_df["is_priced"].mean() * 100 if len(revenues_df) else 0
total_revenue = revenues_df["revenue_eur"].sum(skipna=True)
captured_price = total_revenue / total_gen if total_gen > 0 else np.nan
baseload_price = price_df["price_eur_mwh"].mean()
capture_factor = captured_price / baseload_price * 100 if baseload_price and not pd.isna(captured_price) else np.nan
r1, r2, r3, r4 = st.columns(4)
r1.metric("Revenue", fmt_eur(total_revenue))
r2.metric("Captured price", fmt_price(captured_price))
r3.metric("Baseload price", fmt_price(baseload_price))
r4.metric("Capture factor", f"{capture_factor:.1f}%" if not pd.isna(capture_factor) else "—", f"{priced_ratio:.1f}% priced intervals")

hourly_gen = revenues_df.groupby("hour_madrid", as_index=False).agg(generation_mwh=("generation_mwh_15min", "sum"), revenue_eur=("revenue_eur", "sum"))
hourly = hourly_gen.merge(price_df[["hour_madrid", "price_eur_mwh"]], on="hour_madrid", how="left")
fig_rev = go.Figure()
fig_rev.add_trace(go.Bar(x=hourly["hour_madrid"], y=hourly["generation_mwh"], name="Generation", yaxis="y", opacity=0.75, hovertemplate="%{x|%d-%b %H:%M}<br>Generation: %{y:,.2f} MWh<extra></extra>"))
fig_rev.add_trace(go.Scatter(x=hourly["hour_madrid"], y=hourly["price_eur_mwh"], name="Day-ahead price", yaxis="y2", mode="lines", line=dict(width=2.4), hovertemplate="%{x|%d-%b %H:%M}<br>Price: %{y:,.2f} €/MWh<extra></extra>"))
fig_rev.update_layout(**plot_layout("Hourly generation and day-ahead price", "Generation aggregated from 15-min; price joined by Madrid hour", 460))
fig_rev.update_layout(yaxis=dict(title="Generation (MWh)", gridcolor="#e8edf3", zeroline=True, zerolinecolor="#94a3b8"), yaxis2=dict(title="Price (€/MWh)", overlaying="y", side="right", showgrid=False), bargap=0.05)
fig_rev.update_xaxes(title="Madrid date and hour", showgrid=False)
st.plotly_chart(fig_rev, use_container_width=True)

st.markdown("### Monthly revenue metrics")
monthly_display = monthly[["site", "month", "generation_mwh", "revenue_eur", "captured_price_eur_mwh", "baseload_price_eur_mwh", "capture_factor_pct", "priced_intervals", "missing_price_intervals"]].sort_values(["site", "month"])
st.dataframe(monthly_display.style.format({"generation_mwh": "{:,.2f}", "revenue_eur": "€{:,.0f}", "captured_price_eur_mwh": "{:,.2f}", "baseload_price_eur_mwh": "{:,.2f}", "capture_factor_pct": "{:,.1f}%", "priced_intervals": "{:,.0f}", "missing_price_intervals": "{:,.0f}"}), use_container_width=True, hide_index=True)

st.markdown("### Annual revenue metrics")
annual_display = annual[["site", "year", "generation_mwh", "revenue_eur", "captured_price_eur_mwh", "baseload_price_eur_mwh", "capture_factor_pct", "priced_intervals", "missing_price_intervals"]].sort_values(["site", "year"])
st.dataframe(annual_display.style.format({"generation_mwh": "{:,.2f}", "revenue_eur": "€{:,.0f}", "captured_price_eur_mwh": "{:,.2f}", "baseload_price_eur_mwh": "{:,.2f}", "capture_factor_pct": "{:,.1f}%", "priced_intervals": "{:,.0f}", "missing_price_intervals": "{:,.0f}"}), use_container_width=True, hide_index=True)

if show_diagnostics:
    st.markdown("### Diagnostics")
    with st.expander("Generation diagnostics", expanded=False):
        st.dataframe(gen_diag.style.format({"raw_generation_mwh": "{:,.3f}", "clean_generation_mwh": "{:,.3f}", "night_generation_mwh_raw": "{:,.3f}", "night_generation_pct_raw": "{:,.2f}%"}), use_container_width=True, hide_index=True)
    with st.expander("Price diagnostics", expanded=False):
        st.dataframe(price_df["price_eur_mwh"].describe().to_frame("price_eur_mwh").T, use_container_width=True)
        st.dataframe(price_df.head(50), use_container_width=True, hide_index=True)
    with st.expander("Revenue datetime diagnostics", expanded=False):
        diag = pd.DataFrame({"metric": ["Generation start", "Generation end", "Price start", "Price end", "Generation intervals", "Price hours", "Priced generation intervals", "Missing price intervals"], "value": [str(gen_df["datetime_madrid"].min()), str(gen_df["datetime_madrid"].max()), str(price_df["hour_madrid"].min()), str(price_df["hour_madrid"].max()), f"{len(gen_df):,}", f"{len(price_df):,}", f"{revenues_df['is_priced'].sum():,}", f"{(~revenues_df['is_priced']).sum():,}"]})
        st.dataframe(diag, use_container_width=True, hide_index=True)

st.markdown("### Download processed outputs")
d1, d2, d3 = st.columns(3)
d1.download_button("Download cleaned generation", data=gen_df.to_csv(index=False).encode("utf-8"), file_name="is2_cleaned_generation_15min.csv", mime="text/csv")
d2.download_button("Download revenue detail", data=revenues_df.to_csv(index=False).encode("utf-8"), file_name="is2_revenue_detail_15min.csv", mime="text/csv")
d3.download_button("Download monthly metrics", data=monthly_display.to_csv(index=False).encode("utf-8"), file_name="is2_monthly_revenue_metrics.csv", mime="text/csv")
