# pages/7_PPA_DASS_Settlements.py
"""
PPA & DASS Settlement Lab
=========================
Settlement analytics for two hedging products on Spanish solar + BESS:

1. Solar PV PPA  - "Fixed for floating" or "Floor + discount" structures,
   settled monthly or annually, in EUR/MWh and in EUR (using a user-defined
   annual PPA volume distributed across months with the solar monthly
   share distribution stored in the data workbook).
2. BESS Day-Ahead Spread Swap (DASS) - realised day-ahead TB4 revenue of a
   1 MW / 4 MWh battery (1 cycle/day max, 0.925 charge & discharge
   efficiency -> RTE ~0.856, undegraded) versus a fixed strike in
   kEUR/MW-yr, in EUR/MW and in EUR for a user-defined contracted capacity.

History runs 2021 -> Jun-2026 (OMIE outturn); the forward horizon
(2027-2040) uses the Aurora nominal hourly curve embedded in the workbook,
or an hourly price curve uploaded by the user (in which case the BESS
dispatch optimisation is re-run on the uploaded prices).
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config & styling (kept consistent with the rest of the app)
# ---------------------------------------------------------------------------
st.set_page_config(page_title="PPA & DASS Settlement", layout="wide")

st.markdown(
    """
    <style>
    .hero {
        background: linear-gradient(90deg, #effaf6 0%, #ffffff 100%);
        border: 1px solid #d9efe6;
        border-radius: 14px;
        padding: 18px 24px;
        margin-bottom: 14px;
    }
    .hero h1 { font-size: 1.55rem; margin: 0; }
    .hero p  { margin: 4px 0 0 0; color: #5c6b66; }
    .metric-note { color:#7a8a84; font-size:0.8rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
      <h1>🤝 PPA &amp; DASS Settlement Lab</h1>
      <p>Historical (2021 – Jun 2026) and forward (Aurora nominal, 2027 – 2040)
      settlements for a Solar PPA and a BESS Day-Ahead Spread Swap —
      monthly or annual, in €/MWh · €/MW and in €.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_FILE_CANDIDATES = [
    DATA_DIR / "PV___BESS_settlement_app.xlsx",
    DATA_DIR / "PV_BESS_settlement_app.xlsx",
    BASE_DIR / "PV___BESS_settlement_app.xlsx",
]

BESS_POWER_MW = 1.0          # contracted reference battery
BESS_CAPACITY_MWH = 4.0      # 4h duration
ETA_CH = 0.925               # charging efficiency
ETA_DIS = 0.925              # discharging efficiency  (RTE = 0.925^2 ~ 0.856)
E_CHARGE_GRID = BESS_CAPACITY_MWH / ETA_CH    # 4.3243 MWh bought from grid / cycle
E_DISCHARGE = BESS_CAPACITY_MWH * ETA_DIS     # 3.7000 MWh sold to grid / cycle
KEUR_PER_EURMWH = E_DISCHARGE * 365 / 1000 / (ETA_DIS)  # informative only

MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

GREEN = "#2e9e6b"
GREEN_SOFT = "#bfe6d4"
PURPLE = "#6f5fc6"
PURPLE_SOFT = "#d4cdf0"
ORANGE = "#e8862e"
RED = "#d1495b"


def resolve_data_file() -> Path | None:
    for p in DATA_FILE_CANDIDATES:
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner="Loading hourly price & solar dataset…")
def load_hourly(path_str: str) -> pd.DataFrame:
    """Hourly SPOT + solar profile, 2021-2040 (2026 partial)."""
    df = pd.read_excel(path_str, sheet_name=0, usecols="A:F")
    df.columns = ["date", "year", "month", "hour", "price", "solar"]
    df = df.dropna(subset=["year", "price"]).copy()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["day"] = pd.to_datetime(df["date"]).dt.day
    df["solar"] = df["solar"].fillna(0.0).astype(float)
    df["price"] = df["price"].astype(float)
    return df[["year", "month", "day", "hour", "price", "solar"]]


@st.cache_data(show_spinner=False)
def load_monthly_shares(path_str: str) -> pd.Series:
    """% of annual solar volume delivered in each month (col 'Monthly share distribution')."""
    raw = pd.read_excel(path_str, sheet_name=0, usecols="Z", nrows=14)
    shares = raw.iloc[:, 0].dropna().astype(float).to_numpy()[:12]
    shares = shares / shares.sum()  # normalise defensively
    return pd.Series(shares, index=range(1, 13), name="share")


@st.cache_data(show_spinner=False)
def typical_solar_shape(hourly: pd.DataFrame) -> pd.DataFrame:
    """Mean solar profile by (month, hour) from full history — used to weight
    capture prices on user-uploaded forward curves that carry no solar column."""
    hist = hourly[hourly["year"] <= 2025]
    return (hist.groupby(["month", "hour"])["solar"].mean()
                .reset_index().rename(columns={"solar": "solar_shape"}))


# ---------------------------------------------------------------------------
# BESS daily dispatch (TB4 with efficiencies) — vectorised per day
# ---------------------------------------------------------------------------
def _dispatch_day(prices: np.ndarray) -> tuple[float, float]:
    """One optimal cycle/day for a 1 MW / 4 MWh battery.
    Charge 4/0.925 MWh from grid in the cheapest hours (1 MW cap),
    discharge 4*0.925 MWh in the most expensive hours.
    Returns (revenue €/MW·day, TB4 spread €/MWh)."""
    n = len(prices)
    if n < 10:
        return np.nan, np.nan
    order = np.argsort(prices)

    charge = np.zeros(n)
    rem = E_CHARGE_GRID
    for i in order:
        take = min(BESS_POWER_MW, rem)
        charge[i] = take
        rem -= take
        if rem <= 1e-9:
            break

    discharge = np.zeros(n)
    rem = E_DISCHARGE
    for i in order[::-1]:
        take = min(BESS_POWER_MW, rem)
        discharge[i] = take
        rem -= take
        if rem <= 1e-9:
            break

    revenue = float(np.sum(discharge * prices) - np.sum(charge * prices))
    srt = np.sort(prices)
    tb4 = float(srt[-4:].mean() - srt[:4].mean())
    return revenue, tb4


@st.cache_data(show_spinner="Running BESS dispatch optimisation…")
def bess_daily_results(hourly: pd.DataFrame) -> pd.DataFrame:
    recs = []
    for (y, m, d), grp in hourly.groupby(["year", "month", "day"], sort=True):
        rev, tb4 = _dispatch_day(grp["price"].to_numpy(float))
        recs.append((y, m, d, rev, tb4))
    out = pd.DataFrame(recs, columns=["year", "month", "day", "rev_eur_mw", "tb4"])
    return out.dropna()


# ---------------------------------------------------------------------------
# Capture price computation (with optional exclusion of <=0 €/MWh hours)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def monthly_capture(hourly: pd.DataFrame, settle_at_nonpositive: bool) -> pd.DataFrame:
    df = hourly.copy()
    df["w"] = df["solar"]
    df["w_settle"] = np.where(df["price"] > 0, df["solar"], 0.0) \
        if not settle_at_nonpositive else df["solar"]

    def agg(g: pd.DataFrame) -> pd.Series:
        tot = g["w"].sum()
        settled = g["w_settle"].sum()
        cap = np.average(g["price"], weights=g["w_settle"]) if settled > 0 else np.nan
        cap_all = np.average(g["price"], weights=g["w"]) if tot > 0 else np.nan
        return pd.Series({
            "capture": cap,                       # €/MWh over *settled* volume
            "capture_all": cap_all,               # €/MWh over all generated volume
            "settled_share": settled / tot if tot > 0 else np.nan,
            "neg_hours_gen_share": 1 - settled / tot if tot > 0 else np.nan,
        })

    out = df.groupby(["year", "month"]).apply(agg, include_groups=False).reset_index()
    return out


# ---------------------------------------------------------------------------
# Uploaded forward curve handling
# ---------------------------------------------------------------------------
def parse_uploaded_curve(uploaded) -> pd.DataFrame | None:
    """Accepts CSV/XLSX with a datetime column + hourly price column.
    Returns year/month/day/hour/price."""
    try:
        if uploaded.name.lower().endswith((".xlsx", ".xls")):
            raw = pd.read_excel(uploaded)
        else:
            raw = pd.read_csv(uploaded)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not read the file: {exc}")
        return None

    dt_col, price_col = None, None
    for c in raw.columns:
        parsed = pd.to_datetime(raw[c], errors="coerce")
        if parsed.notna().mean() > 0.9:
            dt_col = c
            raw[c] = parsed
            break
    for c in raw.columns:
        if c == dt_col:
            continue
        vals = pd.to_numeric(raw[c], errors="coerce")
        if vals.notna().mean() > 0.9:
            price_col = c
            raw[c] = vals
            break
    if dt_col is None or price_col is None:
        st.error("The file needs one datetime column and one numeric hourly price column.")
        return None

    out = pd.DataFrame({
        "year": raw[dt_col].dt.year,
        "month": raw[dt_col].dt.month,
        "day": raw[dt_col].dt.day,
        "hour": raw[dt_col].dt.hour + 1,
        "price": raw[price_col].astype(float),
    }).dropna()
    out[["year", "month", "day"]] = out[["year", "month", "day"]].astype(int)
    return out


def curve_template_bytes() -> bytes:
    idx = pd.date_range("2027-01-01 00:00", "2027-12-31 23:00", freq="h")
    tmpl = pd.DataFrame({"datetime": idx, "price_eur_mwh": 60.0})
    buf = BytesIO()
    tmpl.to_csv(buf, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
data_path = resolve_data_file()
if data_path is None:
    st.error(
        "Data file `PV___BESS_settlement_app.xlsx` not found. "
        "Upload it to the repository `data/` folder."
    )
    st.stop()

hourly_base = load_hourly(str(data_path))
monthly_shares = load_monthly_shares(str(data_path))
solar_shape = typical_solar_shape(hourly_base)

HIST_MAX_YEAR = 2026
LAST_HIST_MONTH_2026 = int(hourly_base.loc[hourly_base.year == 2026, "month"].max())

# ---------------------------------------------------------------------------
# Controls
# ---------------------------------------------------------------------------
top1, top2, top3, top4 = st.columns([1.1, 1.3, 1.3, 1.6])
with top1:
    granularity = st.radio("View", ["Annual", "Monthly"], horizontal=True)
with top2:
    year_range = st.slider("Period", 2021, 2040, (2021, 2035))
with top3:
    settle_nonpos = st.toggle(
        "Settle at 0 / negative prices", value=True,
        help="OFF → hours with day-ahead price ≤ 0 €/MWh do not settle "
             "(hyperscaler-style non-settlement below zero).",
    )
with top4:
    curve_source = st.radio(
        "Forward curve (≥2027)", ["Aurora (workbook, nominal)", "Upload my own hourly curve"],
        horizontal=True,
    )

uploaded_curve = None
if curve_source.startswith("Upload"):
    up_col1, up_col2 = st.columns([2, 1])
    with up_col1:
        up = st.file_uploader(
            "Hourly forward price curve (CSV/XLSX: datetime + €/MWh)",
            type=["csv", "xlsx", "xls"],
        )
        if up is not None:
            uploaded_curve = parse_uploaded_curve(up)
    with up_col2:
        st.download_button(
            "⬇️ Template (CSV)", data=curve_template_bytes(),
            file_name="forward_curve_template.csv", mime="text/csv",
        )

# Build the working hourly dataset: history + chosen forward
if uploaded_curve is not None and not uploaded_curve.empty:
    fwd_years = sorted(uploaded_curve["year"].unique())
    hist_part = hourly_base[hourly_base["year"] <= HIST_MAX_YEAR].copy()
    fwd = uploaded_curve.merge(solar_shape, on=["month", "hour"], how="left")
    fwd["solar"] = fwd["solar_shape"].fillna(0.0)
    fwd = fwd[["year", "month", "day", "hour", "price", "solar"]]
    hourly = pd.concat([hist_part, fwd], ignore_index=True)
    st.info(
        f"Using **your uploaded curve** for {fwd_years[0]}–{fwd_years[-1]} "
        f"({len(uploaded_curve):,} hours). The BESS dispatch optimisation "
        "(1 cycle/day, η=0.925/0.925, 1 MW/4 MWh) is re-run on these prices; "
        "solar capture uses the historical month-hour solar shape."
    )
else:
    hourly = hourly_base

hourly = hourly[(hourly.year >= year_range[0]) & (hourly.year <= year_range[1])]
if hourly.empty:
    st.warning("No data in the selected period."); st.stop()

# ---------------------------------------------------------------------------
# Core computations
# ---------------------------------------------------------------------------
cap_m = monthly_capture(hourly, settle_nonpos)
bess_day = bess_daily_results(hourly)
bess_m = (bess_day.groupby(["year", "month"])
          .agg(rev_eur_mw=("rev_eur_mw", "sum"), tb4=("tb4", "mean"))
          .reset_index())

# ======================================================================
# 1) SOLAR PV PPA
# ======================================================================
st.markdown("## ☀️ Solar PV PPA")
c1, c2, c3, c4 = st.columns([1.3, 1, 1, 1])
with c1:
    ppa_type = st.selectbox("PPA structure", ["Fixed for floating", "Floor + discount"])
with c2:
    ppa_volume_gwh = st.number_input("PPA volume (GWh/yr)", 1.0, 5000.0, 100.0, 10.0)
with c3:
    strike = st.number_input("Strike (€/MWh)", 0.0, 300.0, 45.0, 1.0,
                             disabled=(ppa_type != "Fixed for floating"))
with c4:
    floor = st.number_input("Floor (€/MWh)", 0.0, 300.0, 30.0, 1.0,
                            disabled=(ppa_type != "Floor + discount"))
    discount = st.number_input("Discount to market (%)", 0.0, 100.0, 15.0, 1.0,
                               disabled=(ppa_type != "Floor + discount"))

ppa = cap_m.copy()
ppa["volume_mwh"] = ppa_volume_gwh * 1000.0 * ppa["month"].map(monthly_shares)
ppa["settled_mwh"] = ppa["volume_mwh"] * ppa["settled_share"].fillna(0)

if ppa_type == "Fixed for floating":
    ppa["contract_price"] = strike
else:
    ppa["contract_price"] = np.maximum(floor, ppa["capture"] * (1 - discount / 100.0))

# Buyer/offtaker view (as in the reference dashboard): settlement = market − contract
ppa["settle_eur_mwh"] = ppa["capture"] - ppa["contract_price"]
ppa["settle_eur"] = ppa["settle_eur_mwh"] * ppa["settled_mwh"]
ppa["period"] = ppa["year"].astype(str) + "-" + ppa["month"].map(lambda m: f"{m:02d}")

if granularity == "Annual":
    ppa_view = (ppa.groupby("year")
                .apply(lambda g: pd.Series({
                    "settle_eur": g["settle_eur"].sum(),
                    "settle_eur_mwh": (g["settle_eur"].sum() / g["settled_mwh"].sum())
                                       if g["settled_mwh"].sum() > 0 else np.nan,
                    "capture": (g["capture"] * g["settled_mwh"]).sum() / g["settled_mwh"].sum()
                               if g["settled_mwh"].sum() > 0 else np.nan,
                    "neg_hours_gen_share": g["neg_hours_gen_share"].mean(),
                }), include_groups=False)
                .reset_index())
    ppa_view["period"] = ppa_view["year"].astype(str)
else:
    ppa_view = ppa.copy()

avg_settle_mwh = (ppa_view["settle_eur"].sum() /
                  max(ppa["settled_mwh"].sum(), 1e-9))
m1, m2, m3 = st.columns(3)
m1.metric("Avg. settlement", f"{avg_settle_mwh:+.1f} €/MWh",
          help="Net settlement to the offtaker per settled MWh over the selected period.")
m2.metric("Cumulative settlement", f"{ppa_view['settle_eur'].sum()/1e6:+.2f} M€")
m3.metric("Avg. share of generation in ≤0 € hours",
          f"{100*ppa['neg_hours_gen_share'].mean():.1f} %")

ppa_chart = ppa_view.copy()
ppa_chart["sign"] = np.where(ppa_chart["settle_eur_mwh"] >= 0, "positive", "negative")
bars = (alt.Chart(ppa_chart)
        .mark_bar()
        .encode(
            x=alt.X("period:N", title=None, sort=None),
            y=alt.Y("settle_eur_mwh:Q", title="Settlement (€/MWh)"),
            color=alt.Color("sign:N", scale=alt.Scale(domain=["positive", "negative"],
                                                      range=[GREEN, ORANGE]), legend=None),
            tooltip=[
                alt.Tooltip("period:N", title="Period"),
                alt.Tooltip("capture:Q", title="Captured price €/MWh", format=".1f"),
                alt.Tooltip("settle_eur_mwh:Q", title="Settlement €/MWh", format="+.1f"),
                alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f"),
            ])
        .properties(height=280))
st.altair_chart(bars, use_container_width=True)

bars_eur = (alt.Chart(ppa_chart)
            .mark_bar()
            .encode(
                x=alt.X("period:N", title=None, sort=None),
                y=alt.Y("settle_eur:Q", title="Settlement (€)"),
                color=alt.Color("sign:N", scale=alt.Scale(domain=["positive", "negative"],
                                                          range=[GREEN, ORANGE]), legend=None),
                tooltip=[alt.Tooltip("period:N"),
                         alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")])
            .properties(height=240))
st.altair_chart(bars_eur, use_container_width=True)

with st.expander("PPA settlement table"):
    cols = (["period", "capture", "contract_price", "settle_eur_mwh",
             "volume_mwh", "settled_mwh", "settle_eur", "neg_hours_gen_share"]
            if granularity == "Monthly" else
            ["period", "capture", "settle_eur_mwh", "settle_eur", "neg_hours_gen_share"])
    st.dataframe(
        (ppa if granularity == "Monthly" else ppa_view)[cols]
        .rename(columns={
            "capture": "Captured price €/MWh",
            "contract_price": "Contract price €/MWh",
            "settle_eur_mwh": "Settlement €/MWh",
            "volume_mwh": "Volume MWh",
            "settled_mwh": "Settled MWh",
            "settle_eur": "Settlement €",
            "neg_hours_gen_share": "% gen in ≤0€ hours",
        }).round(2),
        use_container_width=True, hide_index=True,
    )

# ======================================================================
# 2) BESS DAY-AHEAD SPREAD SWAP
# ======================================================================
st.markdown("## 🔋 BESS Day-Ahead Spread Swap (DASS)")
d1, d2, d3 = st.columns([1.2, 1, 1.6])
with d1:
    dass_strike = st.number_input("DASS strike (k€/MW·yr)", 0.0, 300.0, 70.0, 1.0)
with d2:
    dass_mw = st.number_input("Contracted BESS capacity (MW)", 0.5, 1000.0, 10.0, 0.5)
with d3:
    eq_spread = dass_strike * 1000 / (E_DISCHARGE * 365)
    st.markdown(
        f"<div class='metric-note'>Strike ≈ <b>{eq_spread:.1f} €/MWh</b> of realised "
        f"TB4 spread (1 cycle/day, {E_DISCHARGE:.2f} MWh discharged per MW per day).</div>",
        unsafe_allow_html=True,
    )

bess = bess_m.copy()
days_in_month = (bess_day.groupby(["year", "month"])["day"].nunique()
                 .reset_index(name="ndays"))
bess = bess.merge(days_in_month, on=["year", "month"])
bess["strike_eur_mw"] = dass_strike * 1000.0 * bess["ndays"] / 365.0
bess["settle_eur_mw"] = bess["rev_eur_mw"] - bess["strike_eur_mw"]
bess["settle_eur"] = bess["settle_eur_mw"] * dass_mw
bess["period"] = bess["year"].astype(str) + "-" + bess["month"].map(lambda m: f"{m:02d}")

if granularity == "Annual":
    bess_view = (bess.groupby("year")
                 .agg(rev_eur_mw=("rev_eur_mw", "sum"),
                      strike_eur_mw=("strike_eur_mw", "sum"),
                      settle_eur_mw=("settle_eur_mw", "sum"),
                      settle_eur=("settle_eur", "sum"),
                      tb4=("tb4", "mean"))
                 .reset_index())
    bess_view["period"] = bess_view["year"].astype(str)
else:
    bess_view = bess.copy()

b1, b2, b3 = st.columns(3)
b1.metric("Avg. settlement (buyer)",
          f"{bess_view['settle_eur_mw'].mean()/1000:+.1f} k€/MW·{'yr' if granularity=='Annual' else 'mo'}")
b2.metric("Cumulative settlement", f"{bess_view['settle_eur'].sum()/1e6:+.2f} M€")
b3.metric("Avg. market TB4 spread", f"{bess_view['tb4'].mean():.1f} €/MWh")

bess_chart = bess_view.copy()
bess_chart["sign"] = np.where(bess_chart["settle_eur_mw"] >= 0, "positive", "negative")
scale_unit = 1000.0
bess_chart["settle_keur_mw"] = bess_chart["settle_eur_mw"] / scale_unit

settle_bars = (alt.Chart(bess_chart)
               .mark_bar()
               .encode(
                   x=alt.X("period:N", title=None, sort=None),
                   y=alt.Y("settle_keur_mw:Q", title="Settlement (k€/MW)"),
                   color=alt.Color("sign:N",
                                   scale=alt.Scale(domain=["positive", "negative"],
                                                   range=[PURPLE, RED]), legend=None),
                   tooltip=[
                       alt.Tooltip("period:N", title="Period"),
                       alt.Tooltip("rev_eur_mw:Q", title="BESS revenue €/MW", format=",.0f"),
                       alt.Tooltip("settle_keur_mw:Q", title="Settlement k€/MW", format="+.1f"),
                       alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f"),
                       alt.Tooltip("tb4:Q", title="TB4 spread €/MWh", format=".1f"),
                   ])
               .properties(height=280))

tb4_line = (alt.Chart(bess_chart)
            .mark_line(point=True, color=ORANGE, strokeDash=[5, 3])
            .encode(x=alt.X("period:N", sort=None),
                    y=alt.Y("tb4:Q", title="Market TB4 spread (€/MWh)"),
                    tooltip=[alt.Tooltip("period:N"),
                             alt.Tooltip("tb4:Q", title="TB4 €/MWh", format=".1f")]))

st.altair_chart(
    alt.layer(settle_bars, tb4_line).resolve_scale(y="independent"),
    use_container_width=True,
)
st.caption(
    "Bars: DASS settlement to the buyer (realised TB4 revenue − strike). "
    "Dashed line: average market TB4 spread (mean of 4 highest − 4 lowest day-ahead "
    "hourly prices per day) behind those revenues — the spread OMIE needs to print "
    "for the BESS to earn the shown revenue."
)

bars_eur2 = (alt.Chart(bess_chart)
             .mark_bar()
             .encode(
                 x=alt.X("period:N", title=None, sort=None),
                 y=alt.Y("settle_eur:Q", title=f"Settlement (€) — {dass_mw:.0f} MW"),
                 color=alt.Color("sign:N",
                                 scale=alt.Scale(domain=["positive", "negative"],
                                                 range=[PURPLE, RED]), legend=None),
                 tooltip=[alt.Tooltip("period:N"),
                          alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")])
             .properties(height=240))
st.altair_chart(bars_eur2, use_container_width=True)

with st.expander("DASS settlement table"):
    st.dataframe(
        bess_view[["period", "rev_eur_mw", "strike_eur_mw", "settle_eur_mw",
                   "settle_eur", "tb4"]]
        .rename(columns={
            "rev_eur_mw": "BESS revenue €/MW",
            "strike_eur_mw": "Strike €/MW",
            "settle_eur_mw": "Settlement €/MW",
            "settle_eur": "Settlement €",
            "tb4": "TB4 spread €/MWh",
        }).round(1),
        use_container_width=True, hide_index=True,
    )

# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    f"""
**Notes & assumptions**

- Forward prices are **nominal** (Aurora curve embedded in the workbook, or the uploaded curve).
- History covers **2021 → Jun-2026** (OMIE outturn); Jul–Dec 2026 is not available in the dataset,
  so 2026 annual figures are year-to-date.
- The DASS is modelled as a **max 1 cycle/day** day-ahead arbitrage of a **1 MW / 4 MWh** battery:
  charge {E_CHARGE_GRID:.2f} MWh from grid in the cheapest hours, discharge {E_DISCHARGE:.2f} MWh
  in the most expensive hours (η_ch = η_dis = 0.925 → **RTE ≈ 0.856**), **undegraded** over the horizon.
- Solar PPA volumes are distributed across months with the workbook's **monthly solar share distribution**;
  captured prices are solar-weighted averages of hourly day-ahead prices.
- With *Settle at 0/negative prices* off, hours with price ≤ 0 €/MWh neither settle nor count as settled
  volume (non-settlement below zero).
- Settlement sign convention: **positive = payment to the offtaker / swap buyer** (market above contract).
- All settlement values are indicative and pre-fees/collateral.
"""
)
