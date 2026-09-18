"""
Hybrid PPA (Solar + BESS) settlements.

Data sources expected under data/:
- Historic values 2025-2026.csv
- aurora q2-26 capt_price_no_demand_2027-2036.xlsx
- baringa q2-26 captured hybrid_no demand_2027-2036.xlsx

The loader is tolerant to suffixes such as "(1)" or "(2)" and to minor
filename variations. Historical values are used through the last month
available in the CSV. From 2027 onwards the user selects Aurora or Baringa.

Settlement convention:
    settlement €/MWh = captured hybrid price - Hybrid PPA fixed price
Positive values represent payment to the buyer / offtaker.

Cash settlement uses a user-defined annual contracted Hybrid PPA volume,
allocated monthly in proportion to calendar days. This avoids inventing
historical production volumes that are not present in the historical CSV.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
import re

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


# =============================================================================
# PAGE SETUP / CORPORATE STYLE
# =============================================================================
st.set_page_config(page_title="Hybrid PPA Settlements", layout="wide")

GREEN = "#1f8a5f"
GREEN_DARK = "#0f6b47"
GREEN_SOFT = "#e6f4ee"
DASS_GREEN = "#198754"
ORANGE = "#e8862e"
GOLD = "#d9aa2b"
BLUE = "#285a84"
RED = "#cf4d4d"
INK = "#12332a"
MUTED = "#6b7f78"

st.markdown(
    f"""
    <style>
    .block-container {{
        padding-top: 1.1rem;
        max-width: 1850px;
        padding-left: 2.2rem;
        padding-right: 2.2rem;
    }}
    html, body, [class*="css"] {{
        font-family: "Inter","Segoe UI",system-ui,sans-serif;
    }}

    .nx-hero {{
        background: linear-gradient(120deg, {GREEN_DARK} 0%, {GREEN} 55%, #2fae79 100%);
        border-radius: 18px;
        padding: 26px 32px;
        color: #fff;
        margin-bottom: 18px;
        box-shadow: 0 8px 24px rgba(15,107,71,.18);
    }}
    .nx-hero h1 {{
        font-size: 2.0rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -.02em;
        color: #fff;
    }}
    .nx-hero p {{
        margin: 6px 0 0 0;
        color: #d9efe6;
        font-size: .95rem;
        max-width: 1050px;
    }}
    .nx-pill {{
        display:inline-block;
        background:#fff;
        color:{GREEN_DARK};
        font-weight:700;
        font-size:.75rem;
        padding:4px 12px;
        border-radius:999px;
        margin-bottom:10px;
        letter-spacing:.06em;
        text-transform:uppercase;
    }}

    .nx-module-banner {{
        margin: 26px 0 14px 0;
        padding: 18px 22px;
        border-radius: 18px;
        border: 1px solid #cfe8dc;
        background: linear-gradient(120deg, #f7fcfa 0%, #e6f4ee 100%);
        box-shadow: 0 6px 18px rgba(18,51,42,.07);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 18px;
    }}
    .nx-module-title {{
        font-size: 1.28rem;
        font-weight: 900;
        color: {INK};
        letter-spacing: -.01em;
    }}
    .nx-module-subtitle {{
        margin-top: 3px;
        color: {MUTED};
        font-size: .9rem;
    }}
    .nx-module-tag {{
        padding: 8px 14px;
        border-radius: 999px;
        border: 1.5px solid {GREEN_DARK};
        color: {GREEN_DARK};
        font-size: .76rem;
        font-weight: 900;
        letter-spacing: .07em;
        white-space: nowrap;
        text-transform: uppercase;
    }}

    .kpi {{
        background:#fff;
        border:1px solid #e5eeea;
        border-radius:18px;
        padding:22px 24px 20px 24px;
        box-shadow:0 3px 12px rgba(18,51,42,.06);
        min-height:138px;
        height:100%;
        text-align:center;
        display:flex;
        flex-direction:column;
        justify-content:center;
    }}
    .kpi .label {{
        color:{MUTED};
        font-size:.8rem;
        font-weight:700;
        text-transform:uppercase;
        letter-spacing:.07em;
    }}
    .kpi .value {{
        font-size:3.0rem;
        font-weight:800;
        letter-spacing:-.035em;
        line-height:1.05;
        margin-top:4px;
        color:{INK};
    }}
    .kpi .unit {{
        font-size:1.05rem;
        font-weight:700;
        color:{MUTED};
        margin-left:5px;
    }}
    .kpi .foot {{
        color:{MUTED};
        font-size:.8rem;
        margin-top:8px;
    }}
    .kpi.pos .value {{ color:{GREEN_DARK}; }}
    .kpi.neg .value {{ color:{RED}; }}
    .kpi.blue .value {{ color:{BLUE}; }}
    .kpi.gold .value {{ color:#9a7210; }}

    .nx-price-grid {{
        display:grid;
        grid-template-columns:repeat(3,minmax(0,1fr));
        gap:14px;
        margin:14px 0 10px 0;
    }}
    .nx-price-card {{
        background:linear-gradient(180deg,#fbfffc 0%,#e9f8ee 100%);
        border:1px solid #bfe0cb;
        border-radius:18px;
        padding:18px 20px;
        box-shadow:0 5px 16px rgba(18,51,42,.055);
    }}
    .nx-price-label {{
        font-size:.76rem;
        color:{MUTED};
        text-transform:uppercase;
        letter-spacing:.08em;
        font-weight:850;
    }}
    .nx-price-value {{
        margin-top:5px;
        font-size:2.75rem;
        line-height:.98;
        font-weight:950;
        letter-spacing:-.04em;
        color:{INK};
    }}
    .nx-price-value .unit {{
        font-size:1rem;
        letter-spacing:0;
        color:{MUTED};
        margin-left:4px;
        font-weight:850;
    }}
    .nx-price-foot {{
        margin-top:8px;
        color:{MUTED};
        font-size:.83rem;
        line-height:1.3;
    }}

    .nx-chart-title {{
        margin:20px 0 6px 0;
        padding-left:10px;
        border-left:5px solid {GREEN_DARK};
        color:{INK};
        font-size:1.02rem;
        font-weight:900;
    }}
    .nx-chart-note {{
        color:{MUTED};
        font-size:.82rem;
        margin:-2px 0 8px 15px;
    }}

    .nx-callout {{
        margin:10px 0 14px 0;
        padding:11px 15px;
        border-radius:12px;
        color:#16653f;
        background:rgba(183,235,199,.38);
        border:1px solid #bfe0cb;
        font-size:.86rem;
        font-weight:650;
    }}

    .nx-print-card {{
        margin-top:34px;
        padding:18px 22px;
        border:1px solid #cfe8dc;
        border-radius:18px;
        background:linear-gradient(120deg,#f7fcfa 0%,#e6f4ee 100%);
        display:flex;
        justify-content:space-between;
        align-items:center;
        gap:18px;
        box-shadow:0 3px 14px rgba(18,51,42,.06);
    }}
    .nx-print-title {{
        font-size:1.25rem;
        font-weight:850;
        color:{INK};
    }}
    .nx-print-subtitle {{
        font-size:.9rem;
        color:{MUTED};
        margin-top:4px;
    }}
    .nx-print-seal {{
        border:2px solid {GREEN_DARK};
        color:{GREEN_DARK};
        border-radius:999px;
        padding:10px 18px;
        font-size:.82rem;
        font-weight:900;
        letter-spacing:.08em;
        white-space:nowrap;
    }}

    div[role="radiogroup"] {{ gap:6px; }}
    div[role="radiogroup"] > label {{
        background:#f4f8f6;
        border:1px solid #dbe7e1;
        border-radius:999px;
        padding:4px 14px 4px 8px;
    }}
    div[role="radiogroup"] > label:has(input:checked) {{
        background:{GREEN_SOFT};
        border-color:{GREEN};
        font-weight:700;
    }}

    @media (max-width:900px) {{
        .nx-price-grid {{ grid-template-columns:1fr; }}
    }}

    @media print {{
        @page {{ size:A4 landscape; margin:8mm; }}
        header, footer, [data-testid="stToolbar"], [data-testid="stSidebar"],
        [data-testid="stDecoration"], .stDeployButton {{
            display:none !important;
        }}
        .block-container {{
            max-width:100% !important;
            padding:.3rem .5rem !important;
        }}
        button {{ display:none !important; }}
        .nx-hero, .nx-module-banner, .kpi, .nx-print-card {{
            break-inside:avoid;
            box-shadow:none !important;
        }}
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="nx-hero">
      <span class="nx-pill">Offtaker view · Hybrid PPA settlements</span>
      <h1>Hybrid PPA (Solar + BESS) Settlements</h1>
      <p>
        Historical captured prices for 2025-2026 and forward Hybrid PPA settlement
        scenarios for 2027-2036. Select Aurora Q2-26 or Baringa Q2-26 and compare
        captured solar, baseload and captured hybrid prices against a fixed Hybrid PPA price.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# =============================================================================
# PATHS AND FILE DISCOVERY
# =============================================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"


def _normalise_filename(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()


def resolve_data_file(
    preferred_names: list[str],
    required_tokens: list[str],
    suffixes: set[str],
) -> Path | None:
    """Resolve exact preferred names first, then tolerant keyword matching."""
    for name in preferred_names:
        p = DATA_DIR / name
        if p.exists():
            return p

    if not DATA_DIR.exists():
        return None

    tokens = [_normalise_filename(t) for t in required_tokens]
    candidates: list[Path] = []
    for p in DATA_DIR.iterdir():
        if not p.is_file() or p.suffix.lower() not in suffixes:
            continue
        normalised = _normalise_filename(p.name)
        if all(token in normalised for token in tokens):
            candidates.append(p)

    if not candidates:
        return None

    # Prefer the most recently modified matching file, useful when "(1)" / "(2)"
    # versions coexist in the repository.
    return max(candidates, key=lambda p: p.stat().st_mtime)


HIST_FILE = resolve_data_file(
    [
        "Historic values 2025-2026.csv",
        "Historic values 2025-2026(2).csv",
    ],
    ["historic", "2025", "2026"],
    {".csv"},
)

AURORA_FILE = resolve_data_file(
    [
        "aurora q2-26 capt_price_no_demand_2027-2036.xlsx",
        "aurora q2-26 capt_price_no_demand_2027-2036(1).xlsx",
    ],
    ["aurora", "q2", "26", "capt", "no", "demand", "2027", "2036"],
    {".xlsx", ".xls"},
)

BARINGA_FILE = resolve_data_file(
    [
        "baringa q2-26 captured hybrid_no demand_2027-2036.xlsx",
        "baringa q2-26 captured hybrid_no demand_2027-2036(2).xlsx",
    ],
    ["baringa", "q2", "26", "captured", "hybrid", "no", "demand", "2027", "2036"],
    {".xlsx", ".xls"},
)


# =============================================================================
# UI HELPERS
# =============================================================================
def kpi(col, label: str, value: str, unit: str = "", foot: str = "", tone: str = ""):
    col.markdown(
        f"""
        <div class="kpi {tone}">
          <div class="label">{label}</div>
          <div class="value">{value}<span class="unit">{unit}</span></div>
          <div class="foot">{foot}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def module_banner(title: str, subtitle: str, tag: str):
    st.markdown(
        f"""
        <div class="nx-module-banner">
          <div>
            <div class="nx-module-title">{title}</div>
            <div class="nx-module-subtitle">{subtitle}</div>
          </div>
          <div class="nx-module-tag">{tag}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def price_card(label: str, value: str, unit: str, foot: str) -> str:
    return f"""
    <div class="nx-price-card">
      <div class="nx-price-label">{label}</div>
      <div class="nx-price-value">{value}<span class="unit">{unit}</span></div>
      <div class="nx-price-foot">{foot}</div>
    </div>
    """


def price_grid(cards: list[str]):
    st.markdown("<div class='nx-price-grid'>" + "".join(cards) + "</div>", unsafe_allow_html=True)


def chart_heading(title: str, subtitle: str = ""):
    st.markdown(f"<div class='nx-chart-title'>{title}</div>", unsafe_allow_html=True)
    if subtitle:
        st.markdown(f"<div class='nx-chart-note'>{subtitle}</div>", unsafe_allow_html=True)


def style_chart(ch):
    return (
        ch.configure_axis(
            labelColor=MUTED,
            titleColor=MUTED,
            gridColor="#eef4f1",
            domainColor="#dbe7e1",
            labelFontSize=11,
            titleFontSize=12,
        )
        .configure_header(
            labelFontSize=13,
            labelFontWeight="bold",
            labelColor=INK,
            titleColor=MUTED,
        )
        .configure_view(strokeWidth=0)
    )


def _x_encoding(granularity: str):
    if granularity == "Annual":
        return alt.X("period:N", title=None, sort=None, axis=alt.Axis(labelAngle=0))
    return alt.X(
        "date:T",
        title=None,
        axis=alt.Axis(
            format="%b %y",
            labelAngle=-45,
            labelOverlap="greedy",
            tickCount=24,
            labelFontSize=10,
            labelPadding=8,
        ),
    )


def _bar_label(v: float, suffix: str = "") -> str:
    if pd.isna(v):
        return ""
    av = abs(v)
    if av >= 1_000_000:
        return f"{v / 1_000_000:+.1f}M{suffix}"
    if av >= 1_000:
        return f"{v / 1_000:+.0f}k{suffix}"
    return f"{v:+.1f}{suffix}"


def settlement_chart(
    df: pd.DataFrame,
    ycol: str,
    ytitle: str,
    granularity: str,
    title: str,
    height: int = 390,
):
    data = df.copy()
    data["bar_lbl"] = data[ycol].map(_bar_label)
    xenc = _x_encoding(granularity)
    base = alt.Chart(data)

    zero = base.mark_rule(color="#adc5bc").encode(y=alt.datum(0))
    bar_colour = alt.condition(
        f"datum.{ycol} >= 0",
        alt.value(GREEN),
        alt.value(ORANGE),
    )

    bar_kwargs = {
        "cornerRadiusTopLeft": 4,
        "cornerRadiusTopRight": 4,
        "cornerRadiusBottomLeft": 4,
        "cornerRadiusBottomRight": 4,
        "opacity": 1.0,
    }
    if granularity == "Monthly":
        bar_kwargs["size"] = 10

    bars = base.mark_bar(**bar_kwargs).encode(
        x=xenc,
        y=alt.Y(f"{ycol}:Q", title=ytitle),
        color=bar_colour,
        tooltip=[
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("captured_hybrid:Q", title="Captured hybrid €/MWh", format=".1f"),
            alt.Tooltip("fixed_price:Q", title="Fixed price €/MWh", format=".1f"),
            alt.Tooltip("settlement_eur_mwh:Q", title="Settlement €/MWh", format="+.1f"),
            alt.Tooltip("settlement_eur:Q", title="Settlement €", format=",.0f"),
        ],
    )

    layers = [bars, zero]
    if granularity == "Annual":
        pos_labels = (
            base.transform_filter(f"datum.{ycol} >= 0")
            .mark_text(dy=-8, fontSize=11, fontWeight="bold", color="#000000")
            .encode(x=xenc, y=alt.Y(f"{ycol}:Q"), text="bar_lbl:N")
        )
        neg_labels = (
            base.transform_filter(f"datum.{ycol} < 0")
            .mark_text(dy=13, fontSize=11, fontWeight="bold", color="#000000")
            .encode(x=xenc, y=alt.Y(f"{ycol}:Q"), text="bar_lbl:N")
        )
        layers.extend([pos_labels, neg_labels])

    return alt.layer(*layers).properties(
        height=height,
        title=alt.TitleParams(title, anchor="start", fontSize=15, color=INK),
    )


def market_vs_contract_chart(
    df: pd.DataFrame,
    granularity: str,
    title: str,
):
    data = df.copy()
    xenc = _x_encoding(granularity)
    base = alt.Chart(data)

    bars = base.mark_bar(
        cornerRadiusTopLeft=5,
        cornerRadiusTopRight=5,
        opacity=.88,
    ).encode(
        x=xenc,
        y=alt.Y("captured_hybrid:Q", title="€/MWh"),
        color=alt.value(GREEN),
        tooltip=[
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("captured_hybrid:Q", title="Captured hybrid €/MWh", format=".1f"),
            alt.Tooltip("fixed_price:Q", title="Fixed Hybrid PPA €/MWh", format=".1f"),
            alt.Tooltip("settlement_eur_mwh:Q", title="Settlement €/MWh", format="+.1f"),
        ],
    )

    line = base.mark_line(
        point={"filled": True, "size": 60},
        strokeWidth=3,
        color=ORANGE,
    ).encode(
        x=xenc,
        y=alt.Y("fixed_price:Q", title="€/MWh"),
    )

    return alt.layer(bars, line).properties(
        height=400,
        title=alt.TitleParams(
            title,
            anchor="start",
            fontSize=15,
            color=INK,
        ),
    )


# =============================================================================
# DATA LOADERS
# =============================================================================
@st.cache_data(show_spinner="Loading historical Hybrid PPA values...")
def load_historical(path_str: str, mtime: float) -> pd.DataFrame:
    _ = mtime
    raw = pd.read_csv(path_str)
    required = {"period", "series", "value"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(
            "Historical CSV is missing columns: " + ", ".join(sorted(missing))
        )

    raw["date"] = pd.to_datetime(raw["period"], errors="coerce")
    raw["value"] = pd.to_numeric(raw["value"], errors="coerce")
    raw = raw.dropna(subset=["date", "series", "value"]).copy()

    wanted = {
        "Baseload": "baseload",
        "Hybrid w/o demand": "captured_hybrid",
        "PV uncurtailed captured price": "captured_solar",
    }

    raw = raw[raw["series"].isin(wanted)].copy()
    wide = (
        raw.pivot_table(
            index="date",
            columns="series",
            values="value",
            aggfunc="first",
        )
        .rename(columns=wanted)
        .reset_index()
    )

    for col in ["baseload", "captured_hybrid", "captured_solar"]:
        if col not in wide.columns:
            wide[col] = np.nan

    wide["year"] = wide["date"].dt.year.astype(int)
    wide["month_num"] = wide["date"].dt.month.astype(int)
    wide["period"] = wide["date"].dt.strftime("%Y-%m")
    wide["data_source"] = "Historical outturn"
    wide["curve_source"] = "Historical"
    return wide[
        [
            "date",
            "year",
            "month_num",
            "period",
            "captured_solar",
            "baseload",
            "captured_hybrid",
            "data_source",
            "curve_source",
        ]
    ].sort_values("date")


@st.cache_data(show_spinner="Loading Aurora Q2-26 Hybrid PPA curve...")
def load_aurora(path_str: str, mtime: float) -> pd.DataFrame:
    _ = mtime
    cols = [
        "Year",
        "month",
        "Captured Solar (€/MWh)",
        "Baseload (€/MWh)",
        "Captured Hybrid (€/MWh)",
    ]
    df = pd.read_excel(path_str, sheet_name="monthly_summary", usecols=cols)
    df = df[df["month"].astype(str).str.upper() != "TOTAL"].copy()

    df["date"] = pd.to_datetime(
        df["month"].astype(str).str.slice(0, 7) + "-01",
        errors="coerce",
    )
    df["year"] = pd.to_numeric(df["Year"], errors="coerce")
    df = df.dropna(subset=["date", "year"]).copy()
    df["year"] = df["year"].astype(int)
    df["month_num"] = df["date"].dt.month.astype(int)

    df = df.rename(
        columns={
            "Captured Solar (€/MWh)": "captured_solar",
            "Baseload (€/MWh)": "baseload",
            "Captured Hybrid (€/MWh)": "captured_hybrid",
        }
    )
    for col in ["captured_solar", "baseload", "captured_hybrid"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["period"] = df["date"].dt.strftime("%Y-%m")
    df["data_source"] = "Aurora Q2-26"
    df["curve_source"] = "Aurora Q2-26"

    return df[
        [
            "date",
            "year",
            "month_num",
            "period",
            "captured_solar",
            "baseload",
            "captured_hybrid",
            "data_source",
            "curve_source",
        ]
    ].sort_values("date")


@st.cache_data(show_spinner="Loading Baringa Q2-26 Hybrid PPA curve...")
def load_baringa(path_str: str, mtime: float) -> pd.DataFrame:
    _ = mtime

    # The Baringa workbook stores its monthly summary in columns T:W
    # of the dispatch sheet:
    # T = Year, U = Month, V = PV captured, W = Baseload, X = Hybrid captured.
    raw = pd.read_excel(
        path_str,
        sheet_name="dispatch",
        usecols="T:X",
    )

    if raw.shape[1] < 5:
        raise ValueError("Baringa workbook monthly summary T:X was not found.")

    raw = raw.iloc[:, :5].copy()
    raw.columns = [
        "year",
        "month_num",
        "captured_solar",
        "baseload",
        "captured_hybrid",
    ]

    raw["year"] = pd.to_numeric(raw["year"], errors="coerce")
    raw["month_num"] = pd.to_numeric(raw["month_num"], errors="coerce")
    for col in ["captured_solar", "baseload", "captured_hybrid"]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")

    raw = raw.dropna(
        subset=["year", "month_num", "captured_hybrid"]
    ).copy()
    raw["year"] = raw["year"].astype(int)
    raw["month_num"] = raw["month_num"].astype(int)
    raw = raw[
        raw["year"].between(2027, 2036)
        & raw["month_num"].between(1, 12)
    ].copy()

    raw["date"] = pd.to_datetime(
        dict(year=raw["year"], month=raw["month_num"], day=1)
    )
    raw["period"] = raw["date"].dt.strftime("%Y-%m")
    raw["data_source"] = "Baringa Q2-26"
    raw["curve_source"] = "Baringa Q2-26"

    return raw[
        [
            "date",
            "year",
            "month_num",
            "period",
            "captured_solar",
            "baseload",
            "captured_hybrid",
            "data_source",
            "curve_source",
        ]
    ].drop_duplicates(["year", "month_num"]).sort_values("date")


def build_annual(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Annual settlement aggregation using the contracted settlement volume as weight.

    This is intentional: the historical CSV contains prices but no historical
    hybrid production volumes. The page therefore does not invent production
    weights. It aggregates annual settlement using the user's contracted volume,
    allocated by calendar days.
    """
    g = monthly.groupby("year", as_index=False)

    annual = g.agg(
        contracted_mwh=("contracted_mwh", "sum"),
        settlement_eur=("settlement_eur", "sum"),
        captured_solar_x_vol=("captured_solar_x_vol", "sum"),
        baseload_x_vol=("baseload_x_vol", "sum"),
        captured_hybrid_x_vol=("captured_hybrid_x_vol", "sum"),
        months=("month_num", "nunique"),
    )

    denom = annual["contracted_mwh"].clip(lower=1e-9)
    annual["captured_solar"] = annual["captured_solar_x_vol"] / denom
    annual["baseload"] = annual["baseload_x_vol"] / denom
    annual["captured_hybrid"] = annual["captured_hybrid_x_vol"] / denom
    annual["settlement_eur_mwh"] = annual["settlement_eur"] / denom
    annual["fixed_price"] = (
        monthly.groupby("year")["fixed_price"].first().reindex(annual["year"]).to_numpy()
    )
    annual["period"] = annual["year"].astype(str)
    annual["date"] = pd.to_datetime(annual["year"].astype(str) + "-01-01")
    annual["hybrid_premium_vs_baseload"] = (
        annual["captured_hybrid"] - annual["baseload"]
    )
    annual["hybrid_uplift_vs_solar"] = (
        annual["captured_hybrid"] - annual["captured_solar"]
    )
    return annual


# =============================================================================
# LOAD DATA
# =============================================================================
missing_files = []
if HIST_FILE is None:
    missing_files.append("Historic values 2025-2026.csv")
if AURORA_FILE is None:
    missing_files.append("Aurora Q2-26 captured-price workbook")
if BARINGA_FILE is None:
    missing_files.append("Baringa Q2-26 captured-hybrid workbook")

if missing_files:
    st.error(
        "Missing source file(s) in `data/`: "
        + ", ".join(missing_files)
        + ". The page accepts the uploaded filenames with or without (1)/(2) suffixes."
    )
    st.stop()

try:
    historical = load_historical(str(HIST_FILE), HIST_FILE.stat().st_mtime)
    aurora = load_aurora(str(AURORA_FILE), AURORA_FILE.stat().st_mtime)
    baringa = load_baringa(str(BARINGA_FILE), BARINGA_FILE.stat().st_mtime)
except Exception as exc:
    st.error(f"Could not load Hybrid PPA source data: {exc}")
    st.stop()

if historical.empty or aurora.empty or baringa.empty:
    st.error("One or more Hybrid PPA source datasets are empty.")
    st.stop()


# =============================================================================
# CONTROLS
# =============================================================================
module_banner(
    "Hybrid PPA settlement configuration",
    "Historical 2025-2026 plus selected forward captured-price curve for 2027-2036.",
    "Solar + BESS",
)

with st.container(border=True):
    c1, c2, c3, c4 = st.columns([1.25, 1.25, 1.15, 1.0])

    with c1:
        forward_source = st.radio(
            "Forward source (2027-2036)",
            ["Aurora Q2-26", "Baringa Q2-26"],
            horizontal=True,
        )

    with c2:
        fixed_price = st.slider(
            "Hybrid PPA fixed price (€/MWh)",
            min_value=0.0,
            max_value=150.0,
            value=62.0,
            step=0.5,
        )

    with c3:
        annual_volume_gwh = st.number_input(
            "Contracted Hybrid PPA volume (GWh/yr)",
            min_value=1.0,
            max_value=5000.0,
            value=100.0,
            step=10.0,
            help=(
                "Used only to convert €/MWh settlement into cash settlement. "
                "Monthly volume is allocated in proportion to calendar days."
            ),
        )

    with c4:
        granularity = st.radio(
            "View",
            ["Annual", "Monthly"],
            horizontal=True,
        )

selected_forward = aurora.copy() if forward_source.startswith("Aurora") else baringa.copy()

combined = pd.concat(
    [
        historical[historical["year"] <= 2026].copy(),
        selected_forward[selected_forward["year"] >= 2027].copy(),
    ],
    ignore_index=True,
).sort_values("date")

MIN_YEAR = int(combined["year"].min())
MAX_YEAR = int(combined["year"].max())

year_range = st.slider(
    "Settlement period",
    min_value=MIN_YEAR,
    max_value=MAX_YEAR,
    value=(MIN_YEAR, MAX_YEAR),
)

monthly = combined[
    combined["year"].between(year_range[0], year_range[1])
].copy()

if monthly.empty:
    st.warning("No Hybrid PPA source values are available in the selected period.")
    st.stop()

# Contracted annual volume is allocated by calendar days.
monthly["days_in_month"] = monthly["date"].dt.days_in_month.astype(float)
monthly["days_in_year"] = np.where(
    monthly["date"].dt.is_leap_year,
    366.0,
    365.0,
)
monthly["contracted_mwh"] = (
    annual_volume_gwh
    * 1000.0
    * monthly["days_in_month"]
    / monthly["days_in_year"]
)

monthly["fixed_price"] = fixed_price
monthly["settlement_eur_mwh"] = monthly["captured_hybrid"] - fixed_price
monthly["settlement_eur"] = (
    monthly["settlement_eur_mwh"] * monthly["contracted_mwh"]
)
monthly["hybrid_premium_vs_baseload"] = (
    monthly["captured_hybrid"] - monthly["baseload"]
)
monthly["hybrid_uplift_vs_solar"] = (
    monthly["captured_hybrid"] - monthly["captured_solar"]
)

monthly["captured_solar_x_vol"] = (
    monthly["captured_solar"] * monthly["contracted_mwh"]
)
monthly["baseload_x_vol"] = (
    monthly["baseload"] * monthly["contracted_mwh"]
)
monthly["captured_hybrid_x_vol"] = (
    monthly["captured_hybrid"] * monthly["contracted_mwh"]
)

annual = build_annual(monthly)
view = annual.copy() if granularity == "Annual" else monthly.copy()

price_grid(
    [
        price_card(
            "Hybrid PPA fixed price",
            f"{fixed_price:.1f}",
            "€/MWh",
            "Fixed contract price used for settlement.",
        ),
        price_card(
            "Forward source",
            forward_source.replace(" Q2-26", ""),
            "",
            "Historical 2025-2026 remains common to both forward cases.",
        ),
        price_card(
            "Contracted volume",
            f"{annual_volume_gwh:,.0f}",
            "GWh/yr",
            "Calendar-day shaped solely for cash-settlement conversion.",
        ),
    ]
)

st.markdown(
    f"""
    <div class="nx-callout">
      Forward configuration is <b>without grid demand / without grid charging</b>,
      matching both uploaded 2027-2036 source workbooks. Historical hybrid values use
      the <b>Hybrid w/o demand</b> series from the CSV.
    </div>
    """,
    unsafe_allow_html=True,
)


# =============================================================================
# KPIs
# =============================================================================
total_contract_mwh = monthly["contracted_mwh"].sum()
denom = max(total_contract_mwh, 1e-9)

weighted_hybrid = (
    monthly["captured_hybrid_x_vol"].sum() / denom
)
weighted_baseload = monthly["baseload_x_vol"].sum() / denom
weighted_solar = monthly["captured_solar_x_vol"].sum() / denom
avg_settlement = monthly["settlement_eur"].sum() / denom
cum_settlement = monthly["settlement_eur"].sum()

k1, k2, k3, k4 = st.columns(4)
kpi(
    k1,
    "Avg settlement to buyer",
    f"{avg_settlement:+.1f}",
    "€/MWh",
    f"{year_range[0]}-{year_range[1]} · contracted-volume weighted",
    "pos" if avg_settlement >= 0 else "neg",
)
kpi(
    k2,
    "Cumulative settlement",
    f"{cum_settlement / 1e6:+.2f}",
    "M€",
    f"{annual_volume_gwh:,.0f} GWh/yr contracted",
    "pos" if cum_settlement >= 0 else "neg",
)
kpi(
    k3,
    "Avg captured hybrid",
    f"{weighted_hybrid:.1f}",
    "€/MWh",
    f"{forward_source} from 2027",
    "blue",
)
kpi(
    k4,
    "Premium vs baseload",
    f"{weighted_hybrid - weighted_baseload:+.1f}",
    "€/MWh",
    f"Solar capture {weighted_solar:.1f} €/MWh",
    "gold",
)


# =============================================================================
# MARKET PRICE COMPARISON
# =============================================================================
chart_heading(
    "Captured solar vs baseload vs captured hybrid",
    "Historical values come from the 2025-2026 CSV. Forward values use the selected Q2-26 source.",
)

price_data = view.copy()
x = _x_encoding(granularity)

long_prices = price_data.melt(
    id_vars=["period", "date", "year"],
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

price_lines = (
    alt.Chart(long_prices)
    .mark_line(strokeWidth=3, point={"filled": True, "size": 48})
    .encode(
        x=x,
        y=alt.Y("price_eur_mwh:Q", title="Price (€/MWh)"),
        color=alt.Color(
            "Series:N",
            scale=alt.Scale(
                domain=["Captured solar", "Baseload", "Captured hybrid"],
                range=[GOLD, BLUE, GREEN_DARK],
            ),
            legend=alt.Legend(title=None, orient="top"),
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
        ],
    )
    .properties(
        height=430,
        title=alt.TitleParams(
            "Hybrid reshaping raises the captured value of the solar profile",
            anchor="start",
            fontSize=15,
            color=INK,
        ),
    )
)
st.altair_chart(style_chart(price_lines), use_container_width=True)


# =============================================================================
# SETTLEMENT
# =============================================================================
chart_heading(
    "Hybrid captured price vs fixed Hybrid PPA price",
    "Green bars = captured hybrid market price. Orange line = fixed Hybrid PPA price.",
)
st.altair_chart(
    style_chart(
        market_vs_contract_chart(
            view,
            granularity,
            "Hybrid market reference vs fixed Hybrid PPA price",
        )
    ),
    use_container_width=True,
)

chart_heading(
    "Settlement in €/MWh",
    "Positive values = payment to the buyer / offtaker. Negative values = payment by the buyer.",
)
st.altair_chart(
    style_chart(
        settlement_chart(
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
    "Cash settlement based on the selected annual contracted volume, allocated monthly by calendar days.",
)
st.altair_chart(
    style_chart(
        settlement_chart(
            view,
            "settlement_eur",
            "Settlement to buyer (€)",
            granularity,
            f"Hybrid PPA settlement in € · {annual_volume_gwh:,.0f} GWh/yr",
            height=340,
        )
    ),
    use_container_width=True,
)


# =============================================================================
# OPTIONAL AURORA / BARINGA COMPARISON
# =============================================================================
with st.expander("Compare Aurora Q2-26 vs Baringa Q2-26 forward captured hybrid"):
    comp = pd.concat(
        [
            aurora.assign(Provider="Aurora Q2-26"),
            baringa.assign(Provider="Baringa Q2-26"),
        ],
        ignore_index=True,
    )
    comp = comp[
        comp["year"].between(max(2027, year_range[0]), min(2036, year_range[1]))
    ].copy()

    if comp.empty:
        st.info("The selected period contains no forward years.")
    else:
        comp_chart = (
            alt.Chart(comp)
            .mark_line(strokeWidth=3, point={"filled": True, "size": 45})
            .encode(
                x=alt.X(
                    "date:T",
                    title=None,
                    axis=alt.Axis(
                        format="%b %y",
                        labelAngle=-45,
                        labelOverlap="greedy",
                    ),
                ),
                y=alt.Y(
                    "captured_hybrid:Q",
                    title="Captured hybrid (€/MWh)",
                ),
                color=alt.Color("Provider:N", legend=alt.Legend(title=None, orient="top")),
                tooltip=[
                    alt.Tooltip("period:N", title="Period"),
                    alt.Tooltip("Provider:N"),
                    alt.Tooltip(
                        "captured_hybrid:Q",
                        title="Captured hybrid €/MWh",
                        format=".1f",
                    ),
                ],
            )
            .properties(height=370)
        )
        st.altair_chart(style_chart(comp_chart), use_container_width=True)


# =============================================================================
# TABLE / DOWNLOAD
# =============================================================================
with st.expander("Hybrid PPA settlement table"):
    table_cols = [
        "period",
        "captured_solar",
        "baseload",
        "captured_hybrid",
        "hybrid_premium_vs_baseload",
        "fixed_price",
        "settlement_eur_mwh",
        "contracted_mwh",
        "settlement_eur",
    ]
    table_out = view[table_cols].rename(
        columns={
            "period": "Period",
            "captured_solar": "Captured solar €/MWh",
            "baseload": "Baseload €/MWh",
            "captured_hybrid": "Captured hybrid €/MWh",
            "hybrid_premium_vs_baseload": "Hybrid premium vs baseload €/MWh",
            "fixed_price": "Fixed Hybrid PPA €/MWh",
            "settlement_eur_mwh": "Settlement to buyer €/MWh",
            "contracted_mwh": "Contracted MWh",
            "settlement_eur": "Settlement to buyer €",
        }
    ).round(2)

    st.dataframe(
        table_out,
        use_container_width=True,
        hide_index=True,
    )

    csv_bytes = table_out.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Download selected settlement table (CSV)",
        data=csv_bytes,
        file_name=(
            f"hybrid_ppa_{forward_source.lower().replace(' ', '_')}_"
            f"{year_range[0]}_{year_range[1]}_{granularity.lower()}.csv"
        ),
        mime="text/csv",
    )


# =============================================================================
# DATA LINEAGE / METHODOLOGY
# =============================================================================
with st.expander("Where every number comes from"):
    st.markdown(
        f"""
- **Historical 2025-2026:** `{HIST_FILE.name}`. The page uses `Baseload`,
  `PV uncurtailed captured price` and `Hybrid w/o demand`.
- **Aurora forward 2027-2036:** `{AURORA_FILE.name}`. Monthly values are read
  from `monthly_summary`: captured solar, baseload and captured hybrid.
- **Baringa forward 2027-2036:** `{BARINGA_FILE.name}`. Monthly values are read
  from the embedded summary in columns T:X of `dispatch`.
- **Forward configuration:** both uploaded forward files are the **no-demand**
  case, so historical values are matched to `Hybrid w/o demand`.
- **Settlement:** `(captured hybrid price - fixed Hybrid PPA price) × contracted volume`.
  Positive values mean payment **to the buyer / offtaker**.
- **Cash-settlement volume:** the selected annual GWh is allocated by calendar days.
  This is a contract-volume assumption, not an inferred plant-production profile.
- **Annual view:** prices are weighted by the same contracted settlement volume.
  This avoids inventing historical generation weights, which are not present in the CSV.
        """
    )


# =============================================================================
# PRINT / PDF
# =============================================================================
st.markdown("---")
st.markdown(
    """
    <div class="nx-print-card">
      <div>
        <div class="nx-print-title">Export page to PDF</div>
        <div class="nx-print-subtitle">
          Print or save the current Hybrid PPA settlement view as PDF.
          Recommended format: <b>A4 landscape</b>, minimum margins and background graphics enabled.
        </div>
      </div>
      <div class="nx-print-seal">NEXWELL POWER · HYBRID PPA</div>
    </div>
    """,
    unsafe_allow_html=True,
)

components.html(
    """
    <button onclick="window.parent.print()" style="
        width:100%;
        padding:14px 18px;
        border:0;
        border-radius:12px;
        background:#0f6b47;
        color:white;
        font-weight:800;
        font-size:15px;
        cursor:pointer;
        margin-top:10px;">
        Print / Save current page as PDF
    </button>
    """,
    height=70,
)
