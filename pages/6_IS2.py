# is2_generation_revenues_corporate.py
# Corporate IS2 dashboard:
# - Uses the same Day Ahead price workbook convention as the Day Ahead page:
#     data/hourly_avg_price_since2021.xlsx -> sheet "prices_hourly_avg" -> columns datetime, price
# - Does NOT default to eSIOS indicator 600 for revenues, to avoid distorted baseload values.
# - 15-min solar generation remains 15-min; revenue join is done against hourly Madrid prices.
#
# Run:
#   streamlit run is2_generation_revenues_corporate.py

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


MADRID_TZ = "Europe/Madrid"
UTC_TZ = "UTC"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREY = "#4B5563"
CORP_LIGHT = "#F8FAFC"
CORP_BORDER = "#E5E7EB"
CORP_BLUE = "#1D4ED8"
CORP_ORANGE = "#D97706"


# =========================================================
# Page setup
# =========================================================
st.set_page_config(
    page_title="IS2 | Generation & Revenues",
    page_icon="⚡",
    layout="wide",
)

st.markdown(
    f"""
    <style>
        .main .block-container {{
            padding-top: 1.3rem;
            padding-bottom: 2.0rem;
            max-width: 1540px;
        }}

        h1 {{
            font-size: 2.05rem !important;
            font-weight: 800 !important;
            color: #111827 !important;
            letter-spacing: -0.02em;
        }}

        h2, h3 {{
            color: #111827 !important;
            letter-spacing: -0.01em;
        }}

        .corp-header {{
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 58%, #C7F0DD 100%);
            color: white;
            padding: 16px 22px;
            border-radius: 16px;
            font-weight: 850;
            font-size: 1.25rem;
            margin: 18px 0 14px 0;
            box-shadow: 0 3px 12px rgba(15,118,110,0.16);
        }}

        .subtle-card {{
            background: white;
            border: 1px solid {CORP_BORDER};
            border-radius: 16px;
            padding: 16px 18px;
            margin: 10px 0 16px 0;
            box-shadow: 0 1px 3px rgba(15,23,42,0.05);
        }}

        .small-muted {{
            color: #6B7280;
            font-size: 0.88rem;
        }}

        div[data-testid="metric-container"] {{
            background: white;
            border: 1px solid {CORP_BORDER};
            padding: 16px 18px;
            border-radius: 16px;
            box-shadow: 0 1px 3px rgba(15,23,42,0.05);
        }}

        div[data-testid="metric-container"] label {{
            color: #6B7280 !important;
            font-weight: 650 !important;
        }}

        div[data-testid="metric-container"] [data-testid="stMetricValue"] {{
            color: #111827 !important;
            font-weight: 800 !important;
        }}

        .ok-box {{
            background:#ECFDF5;
            color:#065F46;
            border:1px solid #BBF7D0;
            border-radius:14px;
            padding:13px 15px;
            margin:8px 0 16px 0;
        }}

        .warning-box {{
            background:#FFF8E6;
            color:#7A5200;
            border:1px solid #FFE1A6;
            border-radius:14px;
            padding:13px 15px;
            margin:8px 0 16px 0;
        }}

        .danger-box {{
            background:#FEF2F2;
            color:#991B1B;
            border:1px solid #FECACA;
            border-radius:14px;
            padding:13px 15px;
            margin:8px 0 16px 0;
        }}

        .price-source-pill {{
            display:inline-block;
            background:#EEF2FF;
            border:1px solid #C7D2FE;
            color:#3730A3;
            border-radius:999px;
            padding:6px 10px;
            font-size:0.85rem;
            font-weight:650;
            margin:4px 0 12px 0;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# Helpers
# =========================================================
def section_header(title: str) -> None:
    st.markdown(f'<div class="corp-header">{title}</div>', unsafe_allow_html=True)


def info_box(kind: str, text: str) -> None:
    cls = {"ok": "ok-box", "warning": "warning-box", "danger": "danger-box"}.get(kind, "warning-box")
    st.markdown(f'<div class="{cls}">{text}</div>', unsafe_allow_html=True)


def norm_col(c: str) -> str:
    return (
        str(c)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(".", "_")
        .replace("€", "eur")
    )


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


def clean_numeric(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")

    txt = s.astype(str).str.strip()
    # European numeric format: 1.234,56
    euro_mask = txt.str.contains(",", regex=False) & txt.str.contains(".", regex=False)
    txt = txt.where(~euro_mask, txt.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    # Decimal comma: 123,45
    txt = txt.where(euro_mask, txt.str.replace(",", ".", regex=False))
    return pd.to_numeric(txt.replace({"nan": np.nan, "None": np.nan, "": np.nan}), errors="coerce")


def read_table_from_upload(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.read()
    name = uploaded_file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(raw))
    if name.endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(raw))
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(raw), sep=";", decimal=",", encoding="latin1")
    if name.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(raw))
    raise ValueError("Unsupported file type. Use CSV, Excel or Parquet.")


def to_madrid_datetime(series: pd.Series, source_tz: str) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")

    # Aware timestamps: convert only.
    if getattr(dt.dt, "tz", None) is not None:
        return dt.dt.tz_convert(MADRID_TZ)

    # Naive timestamps: localize in declared source timezone.
    if source_tz == "UTC":
        return dt.dt.tz_localize(UTC_TZ, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(MADRID_TZ)

    return dt.dt.tz_localize(source_tz, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(MADRID_TZ)


def normalize_generation(values: pd.Series, unit: str) -> pd.Series:
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


def chart_layout(title: str, subtitle: str = "", height: int = 470) -> dict:
    title_text = f"<b>{title}</b><br><sup>{subtitle}</sup>" if subtitle else f"<b>{title}</b>"
    return dict(
        title=dict(text=title_text, x=0.01, xanchor="left"),
        height=height,
        margin=dict(l=58, r=58, t=78, b=52),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.85)",
        ),
        font=dict(size=12, color="#111827"),
    )


def style_table(df: pd.DataFrame):
    return (
        df.style
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", CORP_GREY),
                        ("color", "white"),
                        ("font-weight", "750"),
                        ("font-size", "13px"),
                        ("text-align", "center"),
                        ("padding", "9px 8px"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("font-size", "12px"),
                        ("padding", "7px 8px"),
                    ],
                },
            ]
        )
    )


# =========================================================
# Day Ahead price loading
# =========================================================
def candidate_base_dirs() -> list[Path]:
    here = Path(__file__).resolve()
    candidates = [
        here.parent,
        here.parent.parent,
        Path.cwd(),
        Path.cwd().parent,
    ]
    # Keep order, remove duplicates.
    out: list[Path] = []
    seen = set()
    for p in candidates:
        try:
            rp = p.resolve()
        except Exception:
            rp = p
        if str(rp) not in seen:
            out.append(rp)
            seen.add(str(rp))
    return out


def find_day_ahead_price_workbook() -> Optional[Path]:
    names = [
        "hourly_avg_price_since2021.xlsx",
        "hourly_avg_price_since2021.xls",
    ]
    subdirs = [
        Path("data"),
        Path("."),
        Path("pages") / "data",
    ]

    for base in candidate_base_dirs():
        for sub in subdirs:
            for name in names:
                p = base / sub / name
                if p.exists():
                    return p
    return None


@st.cache_data(show_spinner=False)
def load_day_ahead_workbook_prices(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    try:
        df = pd.read_excel(path, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(path, sheet_name=0)

    # Day Ahead convention uses datetime + price. Make this robust.
    dt_col = find_col(df.columns, ["datetime", "date", "timestamp", "hour"])
    price_col = find_col(df.columns, ["price", "precio", "value", "eur_mwh", "price_eur_mwh"])
    if dt_col is None or price_col is None:
        raise ValueError(
            f"Could not identify datetime/price columns in {path.name}. "
            f"Available columns: {list(df.columns)}"
        )

    out = pd.DataFrame()
    dt = pd.to_datetime(df[dt_col], errors="coerce")

    # Day Ahead workbook normally stores Madrid naive timestamps.
    if getattr(dt.dt, "tz", None) is not None:
        out["hour_madrid"] = dt.dt.tz_convert(MADRID_TZ).dt.floor("h")
    else:
        out["hour_madrid"] = dt.dt.tz_localize(MADRID_TZ, nonexistent="shift_forward", ambiguous="NaT").dt.floor("h")

    out["price_eur_mwh"] = clean_numeric(df[price_col])
    out = (
        out.dropna(subset=["hour_madrid", "price_eur_mwh"])
        .sort_values("hour_madrid")
        .drop_duplicates("hour_madrid", keep="last")
        .reset_index(drop=True)
    )
    out["price_source"] = f"Day Ahead workbook — {path.name}"
    return out[["hour_madrid", "price_eur_mwh", "price_source"]]


def prepare_uploaded_price_file(raw_price: pd.DataFrame, dt_col: str, price_col: str, source_tz: str) -> pd.DataFrame:
    out = pd.DataFrame()
    out["hour_madrid"] = to_madrid_datetime(raw_price[dt_col], source_tz).dt.floor("h")
    out["price_eur_mwh"] = clean_numeric(raw_price[price_col])
    out = (
        out.dropna(subset=["hour_madrid", "price_eur_mwh"])
        .sort_values("hour_madrid")
        .drop_duplicates("hour_madrid", keep="last")
        .reset_index(drop=True)
    )
    out["price_source"] = "Uploaded OMIE / Day Ahead price file"
    return out[["hour_madrid", "price_eur_mwh", "price_source"]]


def price_sanity(price_df: pd.DataFrame) -> tuple[str, str]:
    if price_df.empty:
        return "danger", "No hourly price rows loaded."

    p = price_df["price_eur_mwh"].dropna()
    if p.empty:
        return "danger", "Price column is empty after parsing."

    avg = p.mean()
    med = p.median()
    pmin = p.min()
    pmax = p.max()

    # Spanish day-ahead can spike, but a full-period baseload above 200 is usually a source issue.
    if avg > 200 or med > 180 or pmax > 700 or pmin < -300:
        return (
            "warning",
            f"<b>Price sanity warning:</b> baseload {avg:.1f} €/MWh, median {med:.1f}, "
            f"min {pmin:.1f}, max {pmax:.1f}. This usually means the revenue tab is not using the same "
            f"clean OMIE price series as Day Ahead, or the selected period/source is wrong.",
        )

    return (
        "ok",
        f"<b>Price sanity check passed:</b> baseload {avg:.1f} €/MWh, median {med:.1f}, "
        f"min {pmin:.1f}, max {pmax:.1f}.",
    )


# =========================================================
# Generation and revenue calculations
# =========================================================
def prepare_generation(
    raw: pd.DataFrame,
    dt_col: str,
    site_col: str,
    gen_col: str,
    source_tz: str,
    unit: str,
    zero_night: bool,
    night_start_hour: int,
    night_end_hour: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame()
    df["datetime_madrid"] = to_madrid_datetime(raw[dt_col], source_tz).dt.floor("15min")
    df["site"] = raw[site_col].astype(str).str.strip()
    df["generation_mwh_15min_raw"] = normalize_generation(raw[gen_col], unit)
    df = df.dropna(subset=["datetime_madrid", "site", "generation_mwh_15min_raw"])
    df = (
        df.groupby(["site", "datetime_madrid"], as_index=False)["generation_mwh_15min_raw"]
        .sum()
        .sort_values(["site", "datetime_madrid"])
    )

    # Do not forward-fill generation. Only optionally set impossible night solar values to zero.
    hour_decimal = df["datetime_madrid"].dt.hour + df["datetime_madrid"].dt.minute / 60.0
    is_night = (hour_decimal >= night_start_hour) | (hour_decimal < night_end_hour)

    df["is_night_window"] = is_night
    df["night_generation_mwh_raw"] = np.where(is_night, df["generation_mwh_15min_raw"], 0.0)
    df["generation_mwh_15min"] = np.where(is_night, 0.0, df["generation_mwh_15min_raw"]) if zero_night else df["generation_mwh_15min_raw"]
    df["generation_kwh_15min"] = df["generation_mwh_15min"] * 1000.0
    df["hour_madrid"] = df["datetime_madrid"].dt.floor("h")
    df["date"] = df["datetime_madrid"].dt.date
    df["month"] = df["datetime_madrid"].dt.to_period("M").astype(str)

    diag = (
        df.groupby("site", as_index=False)
        .agg(
            rows=("generation_mwh_15min", "size"),
            start=("datetime_madrid", "min"),
            end=("datetime_madrid", "max"),
            raw_generation_mwh=("generation_mwh_15min_raw", "sum"),
            clean_generation_mwh=("generation_mwh_15min", "sum"),
            raw_night_generation_mwh=("night_generation_mwh_raw", "sum"),
        )
    )
    diag["raw_night_generation_pct"] = np.where(
        diag["raw_generation_mwh"] > 0,
        diag["raw_night_generation_mwh"] / diag["raw_generation_mwh"] * 100,
        0.0,
    )
    return df, diag


def calculate_revenues(gen: pd.DataFrame, price: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    merged = gen.merge(
        price[["hour_madrid", "price_eur_mwh", "price_source"]],
        on="hour_madrid",
        how="left",
    )
    merged["is_priced"] = merged["price_eur_mwh"].notna()
    merged["revenue_eur"] = merged["generation_mwh_15min"] * merged["price_eur_mwh"]

    # Site-month metrics
    monthly = (
        merged.groupby(["site", "month"], as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_intervals=("is_priced", "sum"),
            total_intervals=("is_priced", "size"),
        )
    )
    monthly["missing_price_intervals"] = monthly["total_intervals"] - monthly["priced_intervals"]
    monthly["captured_price_eur_mwh"] = np.where(
        monthly["generation_mwh"] > 0,
        monthly["revenue_eur"] / monthly["generation_mwh"],
        np.nan,
    )

    price_month = price.copy()
    price_month["month"] = price_month["hour_madrid"].dt.to_period("M").astype(str)
    baseload_month = (
        price_month.groupby("month", as_index=False)["price_eur_mwh"]
        .mean()
        .rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    )
    monthly = monthly.merge(baseload_month, on="month", how="left")
    monthly["capture_factor_pct"] = monthly["captured_price_eur_mwh"] / monthly["baseload_price_eur_mwh"] * 100

    # Site-year metrics
    merged["year"] = merged["datetime_madrid"].dt.year
    annual = (
        merged.groupby(["site", "year"], as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_intervals=("is_priced", "sum"),
            total_intervals=("is_priced", "size"),
        )
    )
    annual["missing_price_intervals"] = annual["total_intervals"] - annual["priced_intervals"]
    annual["captured_price_eur_mwh"] = np.where(
        annual["generation_mwh"] > 0,
        annual["revenue_eur"] / annual["generation_mwh"],
        np.nan,
    )

    price_year = price.copy()
    price_year["year"] = price_year["hour_madrid"].dt.year
    baseload_year = (
        price_year.groupby("year", as_index=False)["price_eur_mwh"]
        .mean()
        .rename(columns={"price_eur_mwh": "baseload_price_eur_mwh"})
    )
    annual = annual.merge(baseload_year, on="year", how="left")
    annual["capture_factor_pct"] = annual["captured_price_eur_mwh"] / annual["baseload_price_eur_mwh"] * 100

    # Portfolio-month metrics
    portfolio = (
        merged.groupby("month", as_index=False)
        .agg(
            generation_mwh=("generation_mwh_15min", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            priced_intervals=("is_priced", "sum"),
            total_intervals=("is_priced", "size"),
        )
    )
    portfolio["captured_price_eur_mwh"] = np.where(
        portfolio["generation_mwh"] > 0,
        portfolio["revenue_eur"] / portfolio["generation_mwh"],
        np.nan,
    )
    portfolio = portfolio.merge(baseload_month, on="month", how="left")
    portfolio["capture_factor_pct"] = portfolio["captured_price_eur_mwh"] / portfolio["baseload_price_eur_mwh"] * 100

    return merged, monthly, annual, portfolio


def display_corporate_table(df: pd.DataFrame, table_type: str) -> None:
    out = df.copy()

    rename_map = {
        "site": "Site",
        "month": "Month",
        "year": "Year",
        "generation_mwh": "Generation (MWh)",
        "revenue_eur": "Revenue (€)",
        "captured_price_eur_mwh": "Captured price (€/MWh)",
        "baseload_price_eur_mwh": "Baseload price (€/MWh)",
        "capture_factor_pct": "Capture factor (%)",
        "priced_intervals": "Priced intervals",
        "total_intervals": "Total intervals",
        "missing_price_intervals": "Missing price intervals",
    }
    out = out.rename(columns=rename_map)

    ordered = [c for c in rename_map.values() if c in out.columns]
    out = out[ordered]

    fmt = {
        "Generation (MWh)": "{:,.2f}",
        "Revenue (€)": "€{:,.0f}",
        "Captured price (€/MWh)": "{:,.2f}",
        "Baseload price (€/MWh)": "{:,.2f}",
        "Capture factor (%)": "{:,.1f}%",
        "Priced intervals": "{:,.0f}",
        "Total intervals": "{:,.0f}",
        "Missing price intervals": "{:,.0f}",
    }

    st.dataframe(
        style_table(out).format({k: v for k, v in fmt.items() if k in out.columns}),
        use_container_width=True,
        hide_index=True,
    )


# =========================================================
# Sidebar
# =========================================================
st.sidebar.title("IS2 controls")

gen_file = st.sidebar.file_uploader(
    "Upload IS2 15-min generation file",
    type=["csv", "xlsx", "xls", "parquet"],
)

st.sidebar.markdown("---")
st.sidebar.subheader("Generation parsing")
source_tz_gen = st.sidebar.selectbox(
    "Generation timestamp source timezone",
    ["UTC", "Europe/Madrid"],
    index=0,
)
gen_unit = st.sidebar.selectbox(
    "Generation value unit",
    ["kWh per 15-min", "MWh per 15-min", "kW average in 15-min", "MW average in 15-min"],
    index=0,
)
zero_night = st.sidebar.checkbox(
    "Set impossible night generation to zero",
    value=True,
    help="Recommended for solar parks. This prevents timezone/ffill contamination from impacting revenues.",
)
n1, n2 = st.sidebar.columns(2)
night_start = n1.number_input("Night starts", min_value=18, max_value=24, value=22, step=1)
night_end = n2.number_input("Night ends", min_value=0, max_value=9, value=5, step=1)

st.sidebar.markdown("---")
st.sidebar.subheader("Price source")
price_mode = st.sidebar.radio(
    "Hourly day-ahead price source",
    [
        "Use Day Ahead workbook automatically",
        "Upload OMIE / Day Ahead price file",
    ],
    index=0,
    help="Default uses data/hourly_avg_price_since2021.xlsx, matching the Day Ahead page convention.",
)

uploaded_price_file = None
price_tz = "Europe/Madrid"
if price_mode == "Upload OMIE / Day Ahead price file":
    uploaded_price_file = st.sidebar.file_uploader(
        "Upload hourly price file",
        type=["csv", "xlsx", "xls", "parquet"],
        key="price_upload",
    )
    price_tz = st.sidebar.selectbox(
        "Price timestamp source timezone",
        ["Europe/Madrid", "UTC"],
        index=0,
    )

st.sidebar.markdown("---")
chart_height = st.sidebar.slider("Generation chart height", 360, 820, 520, 20)
show_diagnostics = st.sidebar.checkbox("Show diagnostics", value=True)


# =========================================================
# Main
# =========================================================
st.title("IS2 generation & day-ahead revenues")
st.caption(
    "15-min operational generation profile and hourly revenue calculation using the Day Ahead price series."
)

if gen_file is None:
    st.info("Upload the IS2 15-min generation file to start.")
    st.stop()

try:
    raw_gen = read_table_from_upload(gen_file)
except Exception as exc:
    st.error(f"Could not read generation file: {exc}")
    st.stop()

dt_guess = find_col(raw_gen.columns, ["datetime_madrid", "datetime_utc", "datetime", "timestamp", "date", "time"])
site_guess = find_col(raw_gen.columns, ["site", "asset", "plant", "park", "name", "installation"])
gen_guess = find_col(raw_gen.columns, ["generation_kwh", "generation", "energy", "kwh", "mwh", "value"])

with st.expander("Input column mapping", expanded=False):
    c1, c2, c3 = st.columns(3)
    dt_col = c1.selectbox(
        "Generation datetime column",
        raw_gen.columns,
        index=list(raw_gen.columns).index(dt_guess) if dt_guess in raw_gen.columns else 0,
    )
    site_col = c2.selectbox(
        "Site column",
        raw_gen.columns,
        index=list(raw_gen.columns).index(site_guess) if site_guess in raw_gen.columns else 0,
    )
    gen_col = c3.selectbox(
        "Generation value column",
        raw_gen.columns,
        index=list(raw_gen.columns).index(gen_guess) if gen_guess in raw_gen.columns else 0,
    )

gen_df, gen_diag = prepare_generation(
    raw_gen,
    dt_col=dt_col,
    site_col=site_col,
    gen_col=gen_col,
    source_tz=source_tz_gen,
    unit=gen_unit,
    zero_night=zero_night,
    night_start_hour=int(night_start),
    night_end_hour=int(night_end),
)

# Load prices
price_df = pd.DataFrame(columns=["hour_madrid", "price_eur_mwh", "price_source"])
price_source_label = "No price source loaded"

if price_mode == "Use Day Ahead workbook automatically":
    workbook = find_day_ahead_price_workbook()
    if workbook is None:
        info_box(
            "danger",
            "<b>Day Ahead workbook not found.</b> Expected something like "
            "<code>data/hourly_avg_price_since2021.xlsx</code>. Upload a price file instead.",
        )
    else:
        try:
            price_df = load_day_ahead_workbook_prices(str(workbook))
            price_source_label = str(workbook)
        except Exception as exc:
            info_box("danger", f"<b>Could not parse Day Ahead workbook:</b> {exc}")

else:
    if uploaded_price_file is not None:
        try:
            raw_price = read_table_from_upload(uploaded_price_file)
            p_dt_guess = find_col(raw_price.columns, ["datetime_madrid", "datetime_utc", "datetime", "timestamp", "date", "hour"])
            p_val_guess = find_col(raw_price.columns, ["price_eur_mwh", "price", "value", "precio", "eur_mwh", "mibgas", "omie"])
            with st.expander("Price column mapping", expanded=False):
                p1, p2 = st.columns(2)
                price_dt_col = p1.selectbox(
                    "Price datetime column",
                    raw_price.columns,
                    index=list(raw_price.columns).index(p_dt_guess) if p_dt_guess in raw_price.columns else 0,
                )
                price_col = p2.selectbox(
                    "Price value column",
                    raw_price.columns,
                    index=list(raw_price.columns).index(p_val_guess) if p_val_guess in raw_price.columns else 0,
                )
            price_df = prepare_uploaded_price_file(raw_price, price_dt_col, price_col, price_tz)
            price_source_label = uploaded_price_file.name
        except Exception as exc:
            info_box("danger", f"<b>Could not parse uploaded price file:</b> {exc}")
    else:
        info_box("warning", "Upload an OMIE / Day Ahead hourly price file to calculate revenues.")

# Restrict prices to generation period
if not price_df.empty:
    min_hour = gen_df["hour_madrid"].min()
    max_hour = gen_df["hour_madrid"].max()
    price_df = price_df[(price_df["hour_madrid"] >= min_hour) & (price_df["hour_madrid"] <= max_hour)].copy()

# =========================================================
# Top KPI cards
# =========================================================
section_header("Operational generation profile")

total_gen = gen_df["generation_mwh_15min"].sum()
raw_total = gen_diag["raw_generation_mwh"].sum()
raw_night = gen_diag["raw_night_generation_mwh"].sum()
raw_night_pct = raw_night / raw_total * 100 if raw_total > 0 else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Clean generation", fmt_mwh(total_gen))
k2.metric("Raw night generation", fmt_mwh(raw_night), f"{raw_night_pct:.2f}% raw")
k3.metric("Sites", f"{gen_df['site'].nunique():,.0f}")
k4.metric("15-min intervals", f"{len(gen_df):,.0f}")

if raw_night > max(0.005 * raw_total, 0.01):
    info_box(
        "warning",
        f"<b>Solar sanity warning:</b> raw generation contains {raw_night:,.2f} MWh during the configured "
        f"night window ({raw_night_pct:.2f}% of raw generation). "
        f"Cleaning applied: <b>{'night values set to zero' if zero_night else 'night values kept'}</b>.",
    )
else:
    info_box("ok", "<b>Solar sanity check passed:</b> raw night generation is negligible.")

# Generation plot
site_options = sorted(gen_df["site"].unique())
selected_sites = st.multiselect(
    "Sites to display",
    site_options,
    default=site_options[: min(6, len(site_options))],
)

plot_gen = gen_df[gen_df["site"].isin(selected_sites)].copy()
fig = go.Figure()
for site in selected_sites:
    sub = plot_gen[plot_gen["site"] == site]
    fig.add_trace(
        go.Scatter(
            x=sub["datetime_madrid"],
            y=sub["generation_kwh_15min"],
            mode="lines",
            name=site,
            line=dict(width=1.8),
            hovertemplate="%{x|%d-%b %H:%M}<br>%{y:,.1f} kWh/15-min<extra>" + site + "</extra>",
        )
    )

fig.update_layout(
    **chart_layout(
        "15-min generation profile",
        "No forward-fill applied; timestamps converted to Europe/Madrid",
        chart_height,
    )
)
fig.update_xaxes(title="Madrid date and hour", showgrid=False)
fig.update_yaxes(title="Generation (kWh / 15-min)", gridcolor="#E5E7EB", zeroline=True, zerolinecolor="#94A3B8")
st.plotly_chart(fig, use_container_width=True)


# =========================================================
# Revenues
# =========================================================
section_header("Day-ahead revenues — operational parks")

if price_df.empty:
    info_box(
        "danger",
        "No valid hourly price series is available for the generation period. "
        "Revenues are not calculated until the Day Ahead workbook or an uploaded OMIE price file is available.",
    )
    st.stop()

st.markdown(f'<span class="price-source-pill">Price source: {price_source_label}</span>', unsafe_allow_html=True)

status, msg = price_sanity(price_df)
info_box(status, msg)

revenues_df, monthly, annual, portfolio_month = calculate_revenues(gen_df, price_df)

priced_ratio = revenues_df["is_priced"].mean() * 100 if len(revenues_df) else 0.0
total_revenue = revenues_df["revenue_eur"].sum(skipna=True)
captured_price = total_revenue / total_gen if total_gen > 0 else np.nan
baseload_price = price_df["price_eur_mwh"].mean()
capture_factor = captured_price / baseload_price * 100 if baseload_price and not pd.isna(captured_price) else np.nan

r1, r2, r3, r4 = st.columns(4)
r1.metric("Portfolio revenue", fmt_eur(total_revenue))
r2.metric("Captured price", fmt_price(captured_price))
r3.metric("Baseload price", fmt_price(baseload_price))
r4.metric("Capture factor", f"{capture_factor:.1f}%" if not pd.isna(capture_factor) else "—", f"{priced_ratio:.1f}% priced intervals")

# Revenue chart: portfolio hourly generation + price
hourly_gen = (
    revenues_df.groupby("hour_madrid", as_index=False)
    .agg(
        generation_mwh=("generation_mwh_15min", "sum"),
        revenue_eur=("revenue_eur", "sum"),
    )
)
hourly = hourly_gen.merge(price_df[["hour_madrid", "price_eur_mwh"]], on="hour_madrid", how="left")

fig_rev = go.Figure()
fig_rev.add_trace(
    go.Bar(
        x=hourly["hour_madrid"],
        y=hourly["generation_mwh"],
        name="Generation",
        yaxis="y",
        marker=dict(color=CORP_GREEN),
        opacity=0.82,
        hovertemplate="%{x|%d-%b %H:%M}<br>Generation: %{y:,.2f} MWh<extra></extra>",
    )
)
fig_rev.add_trace(
    go.Scatter(
        x=hourly["hour_madrid"],
        y=hourly["price_eur_mwh"],
        name="Day-ahead price",
        yaxis="y2",
        mode="lines",
        line=dict(color=CORP_BLUE, width=2.6),
        hovertemplate="%{x|%d-%b %H:%M}<br>Price: %{y:,.2f} €/MWh<extra></extra>",
    )
)
fig_rev.update_layout(
    **chart_layout(
        "Hourly generation and day-ahead price",
        "Generation aggregated from 15-min intervals; hourly price from Day Ahead series",
        470,
    )
)
fig_rev.update_layout(
    yaxis=dict(
        title="Generation (MWh)",
        gridcolor="#E5E7EB",
        zeroline=True,
        zerolinecolor="#94A3B8",
    ),
    yaxis2=dict(
        title="Price (€/MWh)",
        overlaying="y",
        side="right",
        showgrid=False,
    ),
    bargap=0.04,
)
fig_rev.update_xaxes(title="Madrid date and hour", showgrid=False)
st.plotly_chart(fig_rev, use_container_width=True)

# Portfolio summary table first
st.markdown("#### Portfolio monthly summary")
display_corporate_table(portfolio_month.sort_values("month"), "portfolio")

tab_month, tab_annual, tab_detail = st.tabs(["Monthly by site", "Annual by site", "Hourly detail"])

with tab_month:
    display_corporate_table(monthly.sort_values(["site", "month"]), "monthly")

with tab_annual:
    display_corporate_table(annual.sort_values(["site", "year"]), "annual")

with tab_detail:
    detail_cols = [
        "datetime_madrid",
        "site",
        "generation_mwh_15min",
        "hour_madrid",
        "price_eur_mwh",
        "revenue_eur",
        "is_priced",
    ]
    detail = revenues_df[detail_cols].copy()
    detail = detail.rename(
        columns={
            "datetime_madrid": "15-min timestamp",
            "site": "Site",
            "generation_mwh_15min": "Generation (MWh)",
            "hour_madrid": "Price hour",
            "price_eur_mwh": "Price (€/MWh)",
            "revenue_eur": "Revenue (€)",
            "is_priced": "Priced",
        }
    )
    st.dataframe(
        style_table(detail.head(500)).format(
            {
                "Generation (MWh)": "{:,.4f}",
                "Price (€/MWh)": "{:,.2f}",
                "Revenue (€)": "€{:,.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Showing first 500 rows. Use the download buttons for the complete detail.")

# Diagnostics
if show_diagnostics:
    section_header("Diagnostics")

    with st.expander("Generation diagnostics", expanded=False):
        st.dataframe(
            style_table(gen_diag).format(
                {
                    "raw_generation_mwh": "{:,.3f}",
                    "clean_generation_mwh": "{:,.3f}",
                    "raw_night_generation_mwh": "{:,.3f}",
                    "raw_night_generation_pct": "{:,.2f}%",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("Price diagnostics", expanded=False):
        diag = price_df["price_eur_mwh"].describe().to_frame("price_eur_mwh").T
        st.dataframe(style_table(diag).format("{:,.2f}"), use_container_width=True)
        st.dataframe(price_df.head(80), use_container_width=True, hide_index=True)

    with st.expander("Revenue datetime diagnostics", expanded=False):
        diag = pd.DataFrame(
            {
                "Metric": [
                    "Generation start",
                    "Generation end",
                    "Price start",
                    "Price end",
                    "Generation 15-min intervals",
                    "Price hours",
                    "Priced generation intervals",
                    "Missing price intervals",
                    "Average price over matched period",
                ],
                "Value": [
                    str(gen_df["datetime_madrid"].min()),
                    str(gen_df["datetime_madrid"].max()),
                    str(price_df["hour_madrid"].min()),
                    str(price_df["hour_madrid"].max()),
                    f"{len(gen_df):,}",
                    f"{len(price_df):,}",
                    f"{revenues_df['is_priced'].sum():,}",
                    f"{(~revenues_df['is_priced']).sum():,}",
                    fmt_price(price_df["price_eur_mwh"].mean()),
                ],
            }
        )
        st.dataframe(style_table(diag), use_container_width=True, hide_index=True)

# Downloads
section_header("Downloads")
d1, d2, d3 = st.columns(3)
d1.download_button(
    "Cleaned 15-min generation",
    data=gen_df.to_csv(index=False).encode("utf-8"),
    file_name="is2_cleaned_generation_15min.csv",
    mime="text/csv",
)
d2.download_button(
    "Revenue detail",
    data=revenues_df.to_csv(index=False).encode("utf-8"),
    file_name="is2_revenue_detail_15min.csv",
    mime="text/csv",
)
d3.download_button(
    "Monthly revenue metrics",
    data=monthly.to_csv(index=False).encode("utf-8"),
    file_name="is2_monthly_revenue_metrics.csv",
    mime="text/csv",
)
