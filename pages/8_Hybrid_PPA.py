
"""
Hybrid PPA (Solar + BESS) — future fixed-for-floating settlement dashboard.

Workbook expected in: data/HPPA_wDEmand.xlsx

Data lineage:
- `monthly_summary`: monthly solar and hybrid captured prices / volumes / revenues.
- `dispatch`: hourly market prices and hybrid dispatch, used for baseload prices and
  the representative operational-day chart.
- `stats` (sheet 2): daily BESS economics, used to select the default representative day.

Settlement sign convention:
- Positive = payment to the PPA buyer / offtaker.
- Settlement = (hybrid captured price - fixed Hybrid PPA price) × contracted hybrid volume.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# -----------------------------------------------------------------------------
# Page setup and corporate palette
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Hybrid PPA (Solar + BESS)", layout="wide")

GREEN = "#1f8a5f"
GREEN_DARK = "#0f6b47"
GREEN_SOFT = "#e6f4ee"
GREEN_LIGHT = "#62b58f"
BLUE = "#285a84"
BLUE_SOFT = "#eaf1f7"
GOLD = "#d9aa2b"
GOLD_SOFT = "#fff7d9"
ORANGE = "#e8862e"
RED = "#cf4d4d"
INK = "#12332a"
MUTED = "#6b7f78"
GRID = "#edf3f0"

ETA_CH = 0.925
ETA_DIS = 0.925
RTE = ETA_CH * ETA_DIS  # 0.855625
BESS_POWER_MW = 1.0
BESS_DURATION_H = 4.0
BESS_CAPACITY_MWH = BESS_POWER_MW * BESS_DURATION_H
DOD = 1.0
CYCLES_PER_DAY = 1.0

st.markdown(
    f"""
    <style>
    .block-container {{
        padding-top: 1.05rem;
        max-width: 1850px;
        padding-left: 2.2rem;
        padding-right: 2.2rem;
    }}
    html, body, [class*="css"] {{
        font-family: "Inter", "Segoe UI", system-ui, sans-serif;
    }}
    .hp-hero {{
        background: linear-gradient(118deg, {GREEN_DARK} 0%, {GREEN} 58%, #35aa7a 100%);
        border-radius: 20px;
        padding: 28px 34px;
        color: white;
        box-shadow: 0 10px 28px rgba(15,107,71,.20);
        margin-bottom: 18px;
    }}
    .hp-pill {{
        display: inline-block;
        background: rgba(255,255,255,.94);
        color: {GREEN_DARK};
        padding: 5px 13px;
        border-radius: 999px;
        font-size: .73rem;
        font-weight: 900;
        letter-spacing: .075em;
        text-transform: uppercase;
        margin-bottom: 10px;
    }}
    .hp-hero h1 {{
        color: white;
        margin: 0;
        font-size: 2.05rem;
        font-weight: 900;
        letter-spacing: -.025em;
    }}
    .hp-hero p {{
        margin: 7px 0 0 0;
        color: #dcf1e8;
        max-width: 1050px;
        font-size: .96rem;
        line-height: 1.45;
    }}
    .hp-module {{
        margin: 24px 0 14px 0;
        padding: 18px 22px;
        border: 1px solid #cfe8dc;
        border-radius: 18px;
        background: linear-gradient(120deg, #fbfffd 0%, {GREEN_SOFT} 100%);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 18px;
        box-shadow: 0 6px 18px rgba(18,51,42,.07);
    }}
    .hp-module-title {{
        font-size: 1.28rem;
        font-weight: 900;
        color: {INK};
        letter-spacing: -.012em;
    }}
    .hp-module-sub {{
        margin-top: 4px;
        color: {MUTED};
        font-size: .9rem;
    }}
    .hp-module-tag {{
        border: 1.5px solid {GREEN_DARK};
        color: {GREEN_DARK};
        padding: 8px 15px;
        border-radius: 999px;
        font-size: .75rem;
        font-weight: 900;
        letter-spacing: .075em;
        text-transform: uppercase;
        white-space: nowrap;
    }}
    .hp-assumptions {{
        display: grid;
        grid-template-columns: repeat(6, minmax(0, 1fr));
        gap: 10px;
        margin: 12px 0 18px 0;
    }}
    .hp-assumption {{
        background: white;
        border: 1px solid #dce9e3;
        border-radius: 14px;
        padding: 12px 14px;
        box-shadow: 0 3px 10px rgba(18,51,42,.045);
        text-align: center;
    }}
    .hp-assumption .a-label {{
        color: {MUTED};
        font-size: .70rem;
        font-weight: 850;
        letter-spacing: .07em;
        text-transform: uppercase;
    }}
    .hp-assumption .a-value {{
        color: {INK};
        margin-top: 4px;
        font-size: 1.18rem;
        font-weight: 900;
    }}
    .hp-kpi {{
        background: white;
        border: 1px solid #e2ece7;
        border-radius: 18px;
        min-height: 142px;
        padding: 21px 22px;
        text-align: center;
        display: flex;
        flex-direction: column;
        justify-content: center;
        box-shadow: 0 4px 14px rgba(18,51,42,.06);
    }}
    .hp-kpi .k-label {{
        color: {MUTED};
        font-size: .75rem;
        font-weight: 850;
        letter-spacing: .07em;
        text-transform: uppercase;
    }}
    .hp-kpi .k-value {{
        margin-top: 5px;
        font-size: 2.8rem;
        font-weight: 950;
        line-height: 1;
        letter-spacing: -.045em;
        color: {INK};
    }}
    .hp-kpi .k-unit {{
        margin-left: 4px;
        color: {MUTED};
        font-size: 1.02rem;
        font-weight: 800;
        letter-spacing: 0;
    }}
    .hp-kpi .k-foot {{
        margin-top: 8px;
        color: {MUTED};
        font-size: .8rem;
        line-height: 1.3;
    }}
    .hp-kpi.positive .k-value {{ color: {GREEN_DARK}; }}
    .hp-kpi.negative .k-value {{ color: {RED}; }}
    .hp-kpi.gold .k-value {{ color: #9a7210; }}
    .hp-kpi.blue .k-value {{ color: {BLUE}; }}
    .hp-chart-title {{
        margin: 23px 0 5px 0;
        border-left: 5px solid {GREEN_DARK};
        padding-left: 10px;
        color: {INK};
        font-size: 1.04rem;
        font-weight: 900;
    }}
    .hp-chart-note {{
        margin: 0 0 9px 15px;
        color: {MUTED};
        font-size: .82rem;
    }}
    .hp-callout {{
        margin: 12px 0 6px 0;
        padding: 12px 16px;
        background: {GOLD_SOFT};
        color: #735b15;
        border: 1px solid #ead38a;
        border-radius: 13px;
        font-size: .86rem;
        font-weight: 650;
    }}
    .hp-table-note {{
        color: {MUTED};
        font-size: .82rem;
        margin: 3px 0 10px 0;
    }}
    .hp-print-card {{
        margin-top: 30px;
        padding: 18px 22px;
        border: 1px solid #cfe8dc;
        border-radius: 18px;
        background: linear-gradient(120deg, #fbfffd 0%, {GREEN_SOFT} 100%);
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 18px;
    }}
    .hp-print-title {{ color: {INK}; font-size: 1.22rem; font-weight: 900; }}
    .hp-print-sub {{ color: {MUTED}; font-size: .88rem; margin-top: 4px; }}
    .hp-print-seal {{
        color: {GREEN_DARK};
        border: 2px solid {GREEN_DARK};
        border-radius: 999px;
        padding: 9px 16px;
        font-size: .77rem;
        font-weight: 900;
        letter-spacing: .075em;
        white-space: nowrap;
    }}
    div[role="radiogroup"] {{ gap: 6px; }}
    div[role="radiogroup"] > label {{
        background: #f4f8f6;
        border: 1px solid #dbe7e1;
        border-radius: 999px;
        padding: 4px 14px 4px 8px;
    }}
    div[role="radiogroup"] > label:has(input:checked) {{
        background: {GREEN_SOFT};
        border-color: {GREEN};
        font-weight: 800;
    }}
    div[data-testid="stExpander"] {{
        border-radius: 14px;
        border: 1px solid #e2ece7;
    }}
    @media (max-width: 1050px) {{
        .hp-assumptions {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    }}
    @media (max-width: 700px) {{
        .hp-assumptions {{ grid-template-columns: 1fr 1fr; }}
        .hp-module {{ display: block; }}
        .hp-module-tag {{ display: inline-block; margin-top: 10px; }}
    }}
    @media print {{
        @page {{ size: A4 landscape; margin: 8mm; }}
        header, footer, [data-testid="stToolbar"], [data-testid="stSidebar"],
        [data-testid="stDecoration"], .stDeployButton {{ display: none !important; }}
        .block-container {{ max-width: 100% !important; padding: .3rem .5rem !important; }}
        button {{ display: none !important; }}
        .hp-hero, .hp-module, .hp-kpi, .hp-print-card {{
            box-shadow: none !important;
            break-inside: avoid;
        }}
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hp-hero">
      <span class="hp-pill">Future hedge · fixed-for-floating · solar + storage</span>
      <h1>Hybrid PPA (Solar + BESS)</h1>
      <p>
        Forward settlement view for a hybrid asset combining solar generation and a
        4-hour battery. Compare solar capture, baseload and the reshaped hybrid capture,
        review the hourly operating profile, and quantify the settlement against a fixed
        Hybrid PPA price.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Paths and utility functions
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_FILE_CANDIDATES = [
    DATA_DIR / "HPPA_wDEmand.xlsx",
    DATA_DIR / "HPPA_wDemand.xlsx",
    BASE_DIR / "HPPA_wDEmand.xlsx",
    BASE_DIR / "HPPA_wDemand.xlsx",
]


def resolve_data_file() -> Path | None:
    for candidate in DATA_FILE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def _excel_datetime(values: pd.Series) -> pd.Series:
    """Parse Excel serials, Python datetimes or text dates robustly."""
    if pd.api.types.is_datetime64_any_dtype(values):
        return pd.to_datetime(values, errors="coerce")

    numeric = pd.to_numeric(values, errors="coerce")
    parsed_numeric = pd.to_datetime(
        numeric,
        unit="D",
        origin="1899-12-30",
        errors="coerce",
    )
    parsed_text = pd.to_datetime(values, errors="coerce")
    return parsed_numeric.where(numeric.notna(), parsed_text)


def kpi_card(col, label: str, value: str, unit: str, foot: str, tone: str = "") -> None:
    col.markdown(
        f"""
        <div class="hp-kpi {tone}">
          <div class="k-label">{label}</div>
          <div class="k-value">{value}<span class="k-unit">{unit}</span></div>
          <div class="k-foot">{foot}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def chart_heading(title: str, note: str) -> None:
    st.markdown(f"<div class='hp-chart-title'>{title}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='hp-chart-note'>{note}</div>", unsafe_allow_html=True)


def style_chart(chart: alt.Chart | alt.LayerChart) -> alt.Chart | alt.LayerChart:
    return (
        chart.configure_axis(
            labelColor=MUTED,
            titleColor=MUTED,
            gridColor=GRID,
            domainColor="#dbe7e1",
            labelFontSize=11,
            titleFontSize=12,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            title=None,
            labelColor=INK,
            labelFontSize=11,
            symbolSize=120,
        )
        .configure_view(strokeWidth=0)
    )


def _period_x(granularity: str) -> alt.X:
    if granularity == "Annual":
        return alt.X(
            "period:N",
            title=None,
            sort=None,
            axis=alt.Axis(labelAngle=0, labelPadding=8),
        )
    return alt.X(
        "date:T",
        title=None,
        axis=alt.Axis(
            format="%b %y",
            labelAngle=-45,
            labelOverlap="greedy",
            tickCount=24,
            labelPadding=8,
        ),
    )


def _settlement_label(value: float, unit: str = "") -> str:
    if pd.isna(value):
        return ""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:+.1f}M{unit}"
    if abs(value) >= 1_000:
        return f"{value / 1_000:+.0f}k{unit}"
    return f"{value:+.1f}{unit}"


def settlement_bar_chart(
    df: pd.DataFrame,
    y_col: str,
    y_title: str,
    granularity: str,
    title: str,
    height: int = 390,
) -> alt.LayerChart:
    data = df.copy()
    data["bar_label"] = data[y_col].map(_settlement_label)
    x = _period_x(granularity)
    base = alt.Chart(data)

    bar_kwargs = {
        "cornerRadiusTopLeft": 5,
        "cornerRadiusTopRight": 5,
        "cornerRadiusBottomLeft": 5,
        "cornerRadiusBottomRight": 5,
        "opacity": .96,
    }
    if granularity == "Monthly":
        bar_kwargs["size"] = 10

    bars = base.mark_bar(**bar_kwargs).encode(
        x=x,
        y=alt.Y(f"{y_col}:Q", title=y_title),
        color=alt.condition(
            f"datum.{y_col} >= 0",
            alt.value(GREEN),
            alt.value(ORANGE),
        ),
        tooltip=[
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip(f"{y_col}:Q", title=y_title, format="+,.1f"),
        ],
    )
    zero = base.mark_rule(color="#9eb9ad", strokeWidth=1).encode(y=alt.datum(0))

    show_labels = granularity == "Annual"
    layers: list[alt.Chart] = [bars, zero]
    if show_labels:
        labels_pos = (
            base.transform_filter(f"datum.{y_col} >= 0")
            .mark_text(dy=-8, color="#000000", fontWeight="bold", fontSize=11)
            .encode(x=x, y=alt.Y(f"{y_col}:Q"), text="bar_label:N")
        )
        labels_neg = (
            base.transform_filter(f"datum.{y_col} < 0")
            .mark_text(dy=13, color="#000000", fontWeight="bold", fontSize=11)
            .encode(x=x, y=alt.Y(f"{y_col}:Q"), text="bar_label:N")
        )
        layers.extend([labels_pos, labels_neg])

    return alt.layer(*layers).properties(
        height=height,
        title=alt.TitleParams(title, anchor="start", color=INK, fontSize=15),
    )


# -----------------------------------------------------------------------------
# Workbook loaders
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner="Loading Hybrid PPA monthly results…")
def load_monthly_summary(path_str: str, mtime: float) -> pd.DataFrame:
    _ = mtime
    cols = [
        "Year",
        "month",
        "Hybrid_Profile_MWh",
        "Solar_Generation_MWh",
        "Solar_Revenue_EUR",
        "Hybrid_Revenue_EUR",
        "Captured Solar (€/MWh)",
        "Captured Hybrid (€/MWh)",
    ]
    df = pd.read_excel(path_str, sheet_name="monthly_summary", usecols=cols)
    df = df[df["month"].astype(str).str.upper() != "TOTAL"].copy()
    df["year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["date"] = pd.to_datetime(df["month"].astype(str) + "-01", errors="coerce")
    df = df.dropna(subset=["year", "date"]).copy()
    df["year"] = df["year"].astype(int)

    rename = {
        "Hybrid_Profile_MWh": "hybrid_volume_mwh",
        "Solar_Generation_MWh": "solar_generation_mwh",
        "Solar_Revenue_EUR": "solar_revenue_eur",
        "Hybrid_Revenue_EUR": "hybrid_revenue_eur",
        "Captured Solar (€/MWh)": "captured_solar",
        "Captured Hybrid (€/MWh)": "captured_hybrid",
    }
    df = df.rename(columns=rename)
    for col in rename.values():
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[
        [
            "year",
            "date",
            "month",
            "hybrid_volume_mwh",
            "solar_generation_mwh",
            "solar_revenue_eur",
            "hybrid_revenue_eur",
            "captured_solar",
            "captured_hybrid",
        ]
    ].sort_values("date")


@st.cache_data(show_spinner="Loading hourly hybrid dispatch — first load may take a few seconds…")
def load_dispatch(path_str: str, mtime: float) -> pd.DataFrame:
    path = Path(path_str)
    cache_path = path.with_name(f".{path.stem}_hybrid_dispatch.parquet")

    if cache_path.exists() and cache_path.stat().st_mtime >= mtime:
        try:
            return pd.read_parquet(cache_path)
        except Exception:
            pass

    cols = [
        "Date",
        "Hour",
        "omie_venta",
        "generacion",
        "g_to_grid",
        "g_to_batt",
        "grid_charge",
        "batt_for_sell",
        "soc",
        "hybrid profile (MWh)",
        "charge_mwh",
        "discharge_mwh",
        "timestamp",
        "month",
    ]
    df = pd.read_excel(path, sheet_name="dispatch", usecols=cols)

    df["date"] = _excel_datetime(df["Date"]).dt.normalize()
    df["timestamp_dt"] = _excel_datetime(df["timestamp"])
    bad_ts = df["timestamp_dt"].isna()
    if bad_ts.any():
        hour_num = pd.to_numeric(df.loc[bad_ts, "Hour"], errors="coerce").fillna(1)
        df.loc[bad_ts, "timestamp_dt"] = (
            df.loc[bad_ts, "date"] + pd.to_timedelta(hour_num - 1, unit="h")
        )

    numeric_cols = [
        "Hour",
        "omie_venta",
        "generacion",
        "g_to_grid",
        "g_to_batt",
        "grid_charge",
        "batt_for_sell",
        "soc",
        "hybrid profile (MWh)",
        "charge_mwh",
        "discharge_mwh",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["year"] = df["timestamp_dt"].dt.year.astype("Int64")
    df["month_key"] = df["timestamp_dt"].dt.to_period("M").astype(str)
    df = df.dropna(subset=["timestamp_dt", "date", "year", "omie_venta"]).copy()
    df["year"] = df["year"].astype(int)

    out = df[
        [
            "date",
            "timestamp_dt",
            "year",
            "month_key",
            "Hour",
            "omie_venta",
            "generacion",
            "g_to_grid",
            "g_to_batt",
            "grid_charge",
            "batt_for_sell",
            "soc",
            "hybrid profile (MWh)",
            "charge_mwh",
            "discharge_mwh",
        ]
    ].sort_values("timestamp_dt")

    try:
        out.to_parquet(cache_path, index=False)
    except Exception:
        pass
    return out


@st.cache_data(show_spinner="Loading daily BESS statistics…")
def load_daily_stats(path_str: str, mtime: float) -> pd.DataFrame:
    _ = mtime
    cols = ["Date", "Year", "Revenue BESS (€)", "hybrid profile (MWh)"]
    df = pd.read_excel(path_str, sheet_name="stats", usecols=cols)
    df["date"] = _excel_datetime(df["Date"]).dt.normalize()
    df["year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["bess_revenue_eur"] = pd.to_numeric(df["Revenue BESS (€)"], errors="coerce")
    df["hybrid_volume_mwh"] = pd.to_numeric(df["hybrid profile (MWh)"], errors="coerce")
    return df.dropna(subset=["date", "year"]).assign(year=lambda x: x["year"].astype(int))


@st.cache_data(show_spinner=False)
def monthly_baseload(dispatch: pd.DataFrame) -> pd.DataFrame:
    return (
        dispatch.groupby(["year", "month_key"], as_index=False)
        .agg(baseload=("omie_venta", "mean"), hours=("omie_venta", "count"))
        .rename(columns={"month_key": "month"})
    )


# -----------------------------------------------------------------------------
# Load and reconcile source data
# -----------------------------------------------------------------------------
data_path = resolve_data_file()
if data_path is None:
    st.error(
        "Workbook `HPPA_wDEmand.xlsx` was not found. Upload it to the repository's "
        "`data/` folder."
    )
    st.stop()

mtime = data_path.stat().st_mtime
monthly = load_monthly_summary(str(data_path), mtime)
dispatch = load_dispatch(str(data_path), mtime)
stats = load_daily_stats(str(data_path), mtime)

base_m = monthly_baseload(dispatch)
monthly = monthly.merge(base_m, on=["year", "month"], how="left", validate="one_to_one")
monthly["hybrid_premium_vs_baseload"] = monthly["captured_hybrid"] - monthly["baseload"]
monthly["hybrid_uplift_vs_solar"] = monthly["captured_hybrid"] - monthly["captured_solar"]

if monthly.empty:
    st.warning("No monthly hybrid results were found in the workbook.")
    st.stop()

MIN_YEAR = int(monthly["year"].min())
MAX_YEAR = int(monthly["year"].max())

# -----------------------------------------------------------------------------
# Controls
# -----------------------------------------------------------------------------
st.markdown(
    """
    <div class="hp-module">
      <div>
        <div class="hp-module-title">Hybrid PPA settlement configuration</div>
        <div class="hp-module-sub">
          Select the fixed price, project scale and a forward period of up to ten years.
        </div>
      </div>
      <div class="hp-module-tag">Offtaker settlement view</div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):
    c1, c2, c3, c4, c5 = st.columns([1.15, 1.0, 1.0, 1.0, 1.15])
    with c1:
        fixed_price = st.slider(
            "Hybrid PPA fixed price (€/MWh)",
            min_value=0.0,
            max_value=150.0,
            value=62.0,
            step=0.5,
        )
    with c2:
        project_multiplier = st.number_input(
            "Project scale vs source case",
            min_value=0.1,
            max_value=1000.0,
            value=1.0,
            step=0.5,
            help="1.0 reproduces the workbook's normalized project. Cash settlements scale linearly.",
        )
    with c3:
        contracted_share = st.slider(
            "Contracted hybrid volume",
            min_value=10,
            max_value=100,
            value=100,
            step=5,
            format="%d%%",
        )
    with c4:
        granularity = st.radio("View", ["Annual", "Monthly"], horizontal=True)
    with c5:
        start_options = list(range(MIN_YEAR, MAX_YEAR + 1))
        default_start = MIN_YEAR
        start_year = st.selectbox(
            "Start year",
            options=start_options,
            index=start_options.index(default_start),
        )

    max_tenor = min(10, MAX_YEAR - start_year + 1)
    tenor_years = st.slider(
        "Tenor (years)",
        min_value=1,
        max_value=max_tenor,
        value=max_tenor,
        step=1,
        help="The source workbook currently covers 2027–2040. The page limits each selected view to ten years.",
    )

end_year = start_year + tenor_years - 1
contracted_fraction = contracted_share / 100.0

st.markdown(
    f"""
    <div class="hp-assumptions">
      <div class="hp-assumption"><div class="a-label">Charging efficiency</div><div class="a-value">{ETA_CH:.1%}</div></div>
      <div class="hp-assumption"><div class="a-label">Discharging efficiency</div><div class="a-value">{ETA_DIS:.1%}</div></div>
      <div class="hp-assumption"><div class="a-label">Round-trip efficiency</div><div class="a-value">{RTE:.1%}</div></div>
      <div class="hp-assumption"><div class="a-label">Cycling limit</div><div class="a-value">1 cycle/day</div></div>
      <div class="hp-assumption"><div class="a-label">Battery duration</div><div class="a-value">4 hours</div></div>
      <div class="hp-assumption"><div class="a-label">Depth of discharge</div><div class="a-value">100%</div></div>
    </div>
    """,
    unsafe_allow_html=True,
)

selected_monthly = monthly[
    monthly["year"].between(start_year, end_year)
].copy()
if selected_monthly.empty:
    st.warning("No source data is available for the selected period.")
    st.stop()

selected_monthly["contracted_volume_mwh"] = (
    selected_monthly["hybrid_volume_mwh"] * project_multiplier * contracted_fraction
)
selected_monthly["settlement_eur_mwh"] = selected_monthly["captured_hybrid"] - fixed_price
selected_monthly["settlement_eur"] = (
    selected_monthly["settlement_eur_mwh"] * selected_monthly["contracted_volume_mwh"]
)
selected_monthly["fixed_revenue_eur"] = fixed_price * selected_monthly["contracted_volume_mwh"]
selected_monthly["period"] = selected_monthly["date"].dt.strftime("%Y-%m")

# Build annual values from revenues / volumes and hourly-weighted baseload.
annual_source = selected_monthly.assign(
    baseload_x_hours=selected_monthly["baseload"] * selected_monthly["hours"]
)
annual = (
    annual_source.groupby("year", as_index=False)
    .agg(
        solar_revenue_eur=("solar_revenue_eur", "sum"),
        hybrid_revenue_eur=("hybrid_revenue_eur", "sum"),
        solar_generation_mwh=("solar_generation_mwh", "sum"),
        hybrid_volume_mwh=("hybrid_volume_mwh", "sum"),
        contracted_volume_mwh=("contracted_volume_mwh", "sum"),
        settlement_eur=("settlement_eur", "sum"),
        fixed_revenue_eur=("fixed_revenue_eur", "sum"),
        baseload_x_hours=("baseload_x_hours", "sum"),
        hours=("hours", "sum"),
    )
)
annual["captured_solar"] = (
    annual["solar_revenue_eur"] / annual["solar_generation_mwh"].clip(lower=1e-9)
)
annual["captured_hybrid"] = (
    annual["hybrid_revenue_eur"] / annual["hybrid_volume_mwh"].clip(lower=1e-9)
)
annual["baseload"] = annual["baseload_x_hours"] / annual["hours"].clip(lower=1e-9)
annual["year"] = annual["year"].astype(int)
annual["hybrid_premium_vs_baseload"] = annual["captured_hybrid"] - annual["baseload"]
annual["hybrid_uplift_vs_solar"] = annual["captured_hybrid"] - annual["captured_solar"]
annual["settlement_eur_mwh"] = annual["captured_hybrid"] - fixed_price
annual["period"] = annual["year"].astype(str)
annual["date"] = pd.to_datetime(annual["year"].astype(str) + "-01-01")

view = annual.copy() if granularity == "Annual" else selected_monthly.copy()

# -----------------------------------------------------------------------------
# KPI block
# -----------------------------------------------------------------------------
weighted_hybrid = (
    selected_monthly["hybrid_revenue_eur"].sum()
    / max(selected_monthly["hybrid_volume_mwh"].sum(), 1e-9)
)
weighted_solar = (
    selected_monthly["solar_revenue_eur"].sum()
    / max(selected_monthly["solar_generation_mwh"].sum(), 1e-9)
)
weighted_baseload = (
    (selected_monthly["baseload"] * selected_monthly["hours"]).sum()
    / max(selected_monthly["hours"].sum(), 1e-9)
)
avg_settlement_mwh = weighted_hybrid - fixed_price
cumulative_settlement = selected_monthly["settlement_eur"].sum()
min_hybrid_premium = selected_monthly["hybrid_premium_vs_baseload"].min()

k1, k2, k3, k4 = st.columns(4)
kpi_card(
    k1,
    "Average hybrid captured price",
    f"{weighted_hybrid:.1f}",
    "€/MWh",
    f"{start_year}–{end_year} · volume-weighted",
    "positive",
)
kpi_card(
    k2,
    "Premium vs baseload",
    f"{weighted_hybrid - weighted_baseload:+.1f}",
    "€/MWh",
    f"Minimum monthly premium: {min_hybrid_premium:+.1f} €/MWh",
    "blue",
)
kpi_card(
    k3,
    "Average settlement to buyer",
    f"{avg_settlement_mwh:+.1f}",
    "€/MWh",
    f"Against a fixed price of {fixed_price:.1f} €/MWh",
    "positive" if avg_settlement_mwh >= 0 else "negative",
)
kpi_card(
    k4,
    "Cumulative settlement",
    f"{cumulative_settlement / 1e6:+.2f}",
    "M€",
    f"Scale {project_multiplier:g}× · {contracted_share}% contracted",
    "positive" if cumulative_settlement >= 0 else "negative",
)

# -----------------------------------------------------------------------------
# Three-curve price comparison
# -----------------------------------------------------------------------------
chart_heading(
    "Captured solar vs baseload vs captured hybrid",
    "The shaded green band is the hybrid premium over baseload. Source results show the hybrid capture above baseload in every displayed period.",
)

price_data = view.copy()
price_data["hybrid_premium_vs_baseload"] = (
    price_data["captured_hybrid"] - price_data["baseload"]
)
x = _period_x(granularity)
base = alt.Chart(price_data)

premium_band = base.mark_area(color=GREEN, opacity=.12).encode(
    x=x,
    y=alt.Y("baseload:Q", title="Price (€/MWh)"),
    y2="captured_hybrid:Q",
)

long_prices = price_data.melt(
    id_vars=["period", "date", "year", "hybrid_premium_vs_baseload"],
    value_vars=["captured_solar", "baseload", "captured_hybrid"],
    var_name="series_key",
    value_name="price_eur_mwh",
)
series_labels = {
    "captured_solar": "Captured solar",
    "baseload": "Baseload",
    "captured_hybrid": "Captured hybrid",
}
long_prices["Series"] = long_prices["series_key"].map(series_labels)

price_lines = alt.Chart(long_prices).mark_line(
    strokeWidth=3,
    point={"filled": True, "size": 50},
).encode(
    x=x,
    y=alt.Y("price_eur_mwh:Q", title="Price (€/MWh)"),
    color=alt.Color(
        "Series:N",
        scale=alt.Scale(
            domain=["Captured solar", "Baseload", "Captured hybrid"],
            range=[GOLD, BLUE, GREEN_DARK],
        ),
    ),
    strokeDash=alt.StrokeDash(
        "Series:N",
        scale=alt.Scale(
            domain=["Captured solar", "Baseload", "Captured hybrid"],
            range=[[2, 2], [6, 3], [1, 0]],
        ),
        legend=None,
    ),
    tooltip=[
        alt.Tooltip("period:N", title="Period"),
        alt.Tooltip("Series:N"),
        alt.Tooltip("price_eur_mwh:Q", title="€/MWh", format=".1f"),
        alt.Tooltip(
            "hybrid_premium_vs_baseload:Q",
            title="Hybrid premium vs baseload",
            format="+.1f",
        ),
    ],
)

price_chart = alt.layer(premium_band, price_lines).properties(
    height=440,
    title=alt.TitleParams(
        "Hybrid reshaping moves solar output into higher-value hours",
        anchor="start",
        color=INK,
        fontSize=15,
    ),
)
st.altair_chart(style_chart(price_chart), use_container_width=True)

if min_hybrid_premium <= 0:
    st.warning(
        "At least one source period does not show the hybrid capture above baseload. "
        "The chart displays the workbook values without forcing or clipping the result."
    )
else:
    st.markdown(
        f"""
        <div class="hp-callout">
          Across the selected period, captured hybrid remains above baseload in every month.
          The narrowest observed premium is <b>{min_hybrid_premium:+.1f} €/MWh</b>.
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------------------------
# Hybrid PPA settlement charts
# -----------------------------------------------------------------------------
chart_heading(
    "Hybrid captured price vs fixed Hybrid PPA price",
    "Green line = captured hybrid market price. Orange dashed line = fixed contract price. The vertical difference drives the financial settlement.",
)
contract_data = view.copy()
contract_data["fixed_price"] = fixed_price

hybrid_line = alt.Chart(contract_data).mark_line(
    color=GREEN_DARK,
    strokeWidth=3.2,
    point={"filled": True, "size": 55},
).encode(
    x=x,
    y=alt.Y("captured_hybrid:Q", title="Price (€/MWh)"),
    tooltip=[
        alt.Tooltip("period:N", title="Period"),
        alt.Tooltip("captured_hybrid:Q", title="Captured hybrid", format=".1f"),
        alt.Tooltip("fixed_price:Q", title="Hybrid PPA price", format=".1f"),
        alt.Tooltip("settlement_eur_mwh:Q", title="Settlement €/MWh", format="+.1f"),
    ],
)
fixed_line = alt.Chart(contract_data).mark_line(
    color=ORANGE,
    strokeWidth=2.7,
    strokeDash=[8, 5],
).encode(x=x, y=alt.Y("fixed_price:Q"))

contract_chart = alt.layer(hybrid_line, fixed_line).properties(
    height=390,
    title=alt.TitleParams(
        f"Hybrid capture versus {fixed_price:.1f} €/MWh fixed price",
        anchor="start",
        color=INK,
        fontSize=15,
    ),
)
st.altair_chart(style_chart(contract_chart), use_container_width=True)

chart_heading(
    "Settlement in €/MWh",
    "Positive bars represent a payment to the PPA buyer / offtaker; negative bars represent a payment by the buyer.",
)
st.altair_chart(
    style_chart(
        settlement_bar_chart(
            view,
            "settlement_eur_mwh",
            "Settlement to buyer (€/MWh)",
            granularity,
            "Hybrid PPA settlement per contracted MWh",
        )
    ),
    use_container_width=True,
)

chart_heading(
    "Settlement in €",
    "Cash settlement uses the workbook's hybrid exported profile, the selected project scale and the contracted-volume percentage.",
)
st.altair_chart(
    style_chart(
        settlement_bar_chart(
            view,
            "settlement_eur",
            "Settlement to buyer (€)",
            granularity,
            f"Hybrid PPA cash settlement · {project_multiplier:g}× source case · {contracted_share}% contracted",
            height=350,
        )
    ),
    use_container_width=True,
)

# -----------------------------------------------------------------------------
# Operational day — hourly dispatch
# -----------------------------------------------------------------------------
st.markdown(
    """
    <div class="hp-module">
      <div>
        <div class="hp-module-title">Representative hybrid operating day</div>
        <div class="hp-module-sub">
          Hourly solar output, battery charging / discharging, hybrid export and day-ahead price.
        </div>
      </div>
      <div class="hp-module-tag">Physical operations</div>
    </div>
    """,
    unsafe_allow_html=True,
)

available_dispatch = dispatch[dispatch["year"].between(start_year, end_year)].copy()
if available_dispatch.empty:
    st.info("No hourly dispatch is available for the selected period.")
else:
    min_day = available_dispatch["date"].min().date()
    max_day = available_dispatch["date"].max().date()

    stats_period = stats[stats["year"].between(start_year, end_year)].copy()
    if not stats_period.empty and stats_period["bess_revenue_eur"].notna().any():
        default_ts = stats_period.loc[stats_period["bess_revenue_eur"].idxmax(), "date"]
    else:
        # Select the highest-price-range day when stats do not cover the selected years.
        daily_range = (
            available_dispatch.groupby("date")["omie_venta"]
            .agg(lambda s: s.max() - s.min())
        )
        default_ts = daily_range.idxmax()

    default_date = min(max(default_ts.date(), min_day), max_day)
    op1, op2, op3 = st.columns([1.1, 1.1, 1.8])
    with op1:
        operating_day = st.date_input(
            "Operational day",
            value=default_date,
            min_value=min_day,
            max_value=max_day,
        )
    with op2:
        day_options = st.radio(
            "Display",
            ["Price + physical dispatch", "Physical dispatch only"],
            horizontal=False,
        )
    with op3:
        st.markdown(
            f"""
            <div style="padding-top:27px;color:{MUTED};font-size:.87rem;line-height:1.45;">
              Battery flow is shown as <b>positive when discharging</b> and
              <b>negative when charging</b>. The orange line is the selected
              Hybrid PPA price of <b>{fixed_price:.1f} €/MWh</b>.
            </div>
            """,
            unsafe_allow_html=True,
        )

    day = available_dispatch[
        available_dispatch["date"] == pd.Timestamp(operating_day)
    ].copy()
    if day.empty:
        st.warning("No hourly data is available for the selected day.")
    else:
        day["hour"] = pd.to_numeric(day["Hour"], errors="coerce")
        day["battery_flow_mwh"] = (
            day["discharge_mwh"].fillna(0) - day["charge_mwh"].fillna(0)
        )
        day["fixed_price"] = fixed_price
        day["hybrid_export_mwh"] = day["hybrid profile (MWh)"].fillna(0)
        day["solar_generation_mwh"] = day["generacion"].fillna(0)
        day["flow_type"] = np.where(
            day["battery_flow_mwh"] >= 0,
            "Discharge",
            "Charge",
        )

        hour_x = alt.X(
            "hour:O",
            title="Hour",
            axis=alt.Axis(labelAngle=0, values=list(range(1, 25))),
            sort=list(range(1, 25)),
        )

        price_layer = alt.Chart(day).mark_line(
            color=BLUE,
            strokeWidth=2.7,
            point={"filled": True, "size": 38},
        ).encode(
            x=hour_x,
            y=alt.Y("omie_venta:Q", title="Day-ahead price (€/MWh)"),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("omie_venta:Q", title="DA price €/MWh", format=".1f"),
                alt.Tooltip("solar_generation_mwh:Q", title="Solar MWh", format=".2f"),
                alt.Tooltip("battery_flow_mwh:Q", title="Battery flow MWh", format="+.2f"),
                alt.Tooltip("hybrid_export_mwh:Q", title="Hybrid export MWh", format="+.2f"),
            ],
        )
        fixed_layer = alt.Chart(day).mark_line(
            color=ORANGE,
            strokeDash=[7, 5],
            strokeWidth=2.1,
        ).encode(x=hour_x, y=alt.Y("fixed_price:Q"))

        solar_area = alt.Chart(day).mark_area(
            color=GOLD,
            opacity=.22,
            line={"color": GOLD, "strokeWidth": 1.5},
        ).encode(
            x=hour_x,
            y=alt.Y("solar_generation_mwh:Q", title="Energy flow (MWh)"),
        )
        flow_bars = alt.Chart(day).mark_bar(
            size=14,
            cornerRadiusTopLeft=3,
            cornerRadiusTopRight=3,
            cornerRadiusBottomLeft=3,
            cornerRadiusBottomRight=3,
        ).encode(
            x=hour_x,
            y=alt.Y("battery_flow_mwh:Q", title="Energy flow (MWh)"),
            color=alt.Color(
                "flow_type:N",
                scale=alt.Scale(
                    domain=["Charge", "Discharge"],
                    range=[RED, GREEN],
                ),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("flow_type:N", title="Battery mode"),
                alt.Tooltip("battery_flow_mwh:Q", title="Flow MWh", format="+.2f"),
            ],
        )
        hybrid_line_day = alt.Chart(day).mark_line(
            color=GREEN_DARK,
            strokeWidth=2.7,
            point={"filled": True, "size": 34},
        ).encode(
            x=hour_x,
            y=alt.Y("hybrid_export_mwh:Q", title="Energy flow (MWh)"),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("hybrid_export_mwh:Q", title="Hybrid export MWh", format="+.2f"),
            ],
        )
        zero_flow = alt.Chart(pd.DataFrame({"zero": [0]})).mark_rule(
            color="#a9bdb5",
            strokeWidth=1,
        ).encode(y="zero:Q")

        physical_layer = alt.layer(solar_area, flow_bars, hybrid_line_day, zero_flow)
        if day_options.startswith("Price"):
            op_chart = alt.layer(
                alt.layer(price_layer, fixed_layer),
                physical_layer,
            ).resolve_scale(y="independent")
        else:
            op_chart = physical_layer

        op_chart = op_chart.properties(
            height=460,
            title=alt.TitleParams(
                f"Hourly operation · {pd.Timestamp(operating_day).strftime('%d %b %Y')}",
                anchor="start",
                color=INK,
                fontSize=15,
                subtitle=[
                    "Yellow area = solar generation · red/green bars = battery charge/discharge · dark-green line = hybrid export"
                ],
                subtitleColor=MUTED,
                subtitleFontSize=11,
            ),
        )
        st.altair_chart(style_chart(op_chart), use_container_width=True)

        soc_chart = alt.Chart(day).mark_line(
            color="#4d665d",
            strokeDash=[6, 4],
            strokeWidth=2.2,
            point={"filled": True, "size": 28},
        ).encode(
            x=hour_x,
            y=alt.Y(
                "soc:Q",
                title="State of charge (MWh)",
                scale=alt.Scale(domain=[0, BESS_CAPACITY_MWH]),
            ),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("soc:Q", title="SoC MWh", format=".2f"),
            ],
        ).properties(height=125)
        st.altair_chart(style_chart(soc_chart), use_container_width=True)

# -----------------------------------------------------------------------------
# Settlement table and export
# -----------------------------------------------------------------------------
with st.expander("Hybrid PPA settlement table"):
    table = view.copy()
    table_cols = [
        "period",
        "captured_solar",
        "baseload",
        "captured_hybrid",
        "hybrid_premium_vs_baseload",
        "settlement_eur_mwh",
        "contracted_volume_mwh",
        "settlement_eur",
    ]
    table_out = table[table_cols].rename(
        columns={
            "period": "Period",
            "captured_solar": "Captured solar €/MWh",
            "baseload": "Baseload €/MWh",
            "captured_hybrid": "Captured hybrid €/MWh",
            "hybrid_premium_vs_baseload": "Hybrid premium vs baseload €/MWh",
            "settlement_eur_mwh": "Settlement to buyer €/MWh",
            "contracted_volume_mwh": "Contracted hybrid volume MWh",
            "settlement_eur": "Settlement to buyer €",
        }
    ).round(2)
    st.dataframe(table_out, use_container_width=True, hide_index=True)

    csv_bytes = table_out.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Download selected settlement table (CSV)",
        data=csv_bytes,
        file_name=f"hybrid_ppa_settlement_{start_year}_{end_year}_{granularity.lower()}.csv",
        mime="text/csv",
    )

# -----------------------------------------------------------------------------
# Methodology / data lineage
# -----------------------------------------------------------------------------
with st.expander("Methodology, settlement mechanics and data lineage"):
    st.markdown(
        f"""
- **Source workbook:** `data/HPPA_wDEmand.xlsx`.
- **Monthly / annual price curves:** solar and hybrid captured prices, revenues and
  volumes are read from `monthly_summary`. Baseload is the arithmetic hourly average
  of `omie_venta` from `dispatch`.
- **Sheet 2 (`stats`):** daily BESS revenue and hybrid volume are used to identify a
  representative high-value operating day within the selected period.
- **Hybrid profile:** the workbook defines it as `g_to_grid - grid_charge + batt_for_sell`.
- **Settlement:** `(captured hybrid price − Hybrid PPA fixed price) × contracted hybrid volume`.
  Positive means a payment **to the buyer / offtaker**.
- **Battery assumptions:** 1 MW / 4 MWh source case, 4-hour duration, 100% DoD,
  maximum 1 cycle/day, charging efficiency {ETA_CH:.1%}, discharging efficiency
  {ETA_DIS:.1%}, and RTE {RTE:.1%}. The workbook is undegraded unless its source
  assumptions are changed.
- **Project scale:** the cash settlement is multiplied by the selected scale factor;
  price metrics in €/MWh are unaffected.
- **Forward prices:** nominal prices from the workbook source case. Outputs are
  indicative and pre-fees.
        """
    )

# -----------------------------------------------------------------------------
# Print / PDF
# -----------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    """
    <div class="hp-print-card">
      <div>
        <div class="hp-print-title">Export the current Hybrid PPA view</div>
        <div class="hp-print-sub">
          Print or save as PDF using A4 landscape, minimum margins and background graphics enabled.
        </div>
      </div>
      <div class="hp-print-seal">NEXWELL POWER · HYBRID PPA</div>
    </div>
    """,
    unsafe_allow_html=True,
)
components.html(
    """
    <button onclick="window.parent.print()" style="
        width:100%; padding:14px 18px; border:0; border-radius:12px;
        background:#0f6b47; color:white; font-weight:850; font-size:15px;
        cursor:pointer; margin-top:10px;">
        Print / Save current page as PDF
    </button>
    """,
    height=70,
)
