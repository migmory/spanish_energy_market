# pages/7_PPA_DASS_Settlements.py
"""
PPA & DASS Settlements — offtaker-facing settlement dashboard.

Data lineage (single source of truth = data/PV___BESS_settlement_app.xlsx):
- Hourly day-ahead price (col E) x hourly solar profile (col F) -> solar-weighted
  captured price per month. With "Settle at 0/negative" ON this reproduces the
  workbook's *Uncurtailed capture price* column exactly; OFF reproduces the
  *Curtailed* one (hours <=0 EUR/MWh are excluded from price and volume).
- BESS revenue & TB4 are computed here from the hourly prices (the workbook's
  own BESS/TB4 columns are empty): one cycle/day, charge 4/0.925 MWh in the
  cheapest hours, discharge 4*0.925 MWh in the priciest ones (RTE ~0.856).
  TB4 = daily mean(top-4) - mean(bottom-4), averaged per month.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(page_title="PPA & DASS Settlements", layout="wide")

GREEN = "#1f8a5f"
GREEN_DARK = "#0f6b47"
GREEN_SOFT = "#e6f4ee"
PURPLE = "#5b4bc4"
PURPLE_SOFT = "#eceafb"
ORANGE = "#e8862e"
RED = "#cf4d4d"
INK = "#12332a"
MUTED = "#6b7f78"

st.markdown(
    f"""
    <style>
    .block-container {{ padding-top: 1.2rem; max-width: 1500px; }}
    html, body, [class*="css"] {{ font-family: "Inter","Segoe UI",system-ui,sans-serif; }}

    .nx-hero {{
        background: linear-gradient(120deg, {GREEN_DARK} 0%, {GREEN} 55%, #2fae79 100%);
        border-radius: 18px; padding: 26px 32px; color: #ffffff; margin-bottom: 18px;
        box-shadow: 0 8px 24px rgba(15,107,71,.18);
    }}
    .nx-hero h1 {{ font-size: 1.9rem; font-weight: 800; margin: 0; letter-spacing:-.02em; color:#fff; }}
    .nx-hero p  {{ margin: 6px 0 0 0; color: #d9efe6; font-size: .95rem; max-width: 900px; }}
    .nx-pill {{
        display:inline-block; background:#ffffff; color:{GREEN_DARK}; font-weight:700;
        font-size:.75rem; padding:4px 12px; border-radius:999px; margin-bottom:10px;
        letter-spacing:.06em; text-transform:uppercase;
    }}

    .nx-section {{
        display:flex; align-items:center; gap:10px; margin: 26px 0 4px 0;
    }}
    .nx-dot {{ width:12px; height:12px; border-radius:4px; display:inline-block; }}
    .nx-section h2 {{ font-size:1.35rem; font-weight:800; color:{INK}; margin:0; letter-spacing:-.01em; }}
    .nx-sub {{ color:{MUTED}; font-size:.88rem; margin: 0 0 10px 22px; }}

    .kpi {{
        background:#ffffff; border:1px solid #e7efe b; border:1px solid #e5eeea;
        border-radius:16px; padding:18px 22px 16px 22px;
        box-shadow: 0 2px 10px rgba(18,51,42,.05); height:100%;
    }}
    .kpi .label {{ color:{MUTED}; font-size:.78rem; font-weight:700; text-transform:uppercase; letter-spacing:.07em; }}
    .kpi .value {{ font-size:2.5rem; font-weight:800; letter-spacing:-.03em; line-height:1.1; margin-top:2px; }}
    .kpi .unit  {{ font-size:1.05rem; font-weight:600; color:{MUTED}; margin-left:4px; }}
    .kpi .foot  {{ color:{MUTED}; font-size:.78rem; margin-top:6px; }}
    .pos {{ color:{GREEN_DARK}; }} .neg {{ color:{RED}; }} .neu {{ color:{INK}; }} .pur {{ color:{PURPLE}; }}

    .offtaker-badge {{
        display:inline-block; background:{GREEN_SOFT}; color:{GREEN_DARK};
        border:1px solid #cfe8dc; font-weight:700; font-size:.78rem;
        padding:5px 14px; border-radius:999px; letter-spacing:.03em;
    }}
    .offtaker-badge.dass {{ background:{PURPLE_SOFT}; color:{PURPLE}; border-color:#ddd8f5; }}

    div[data-testid="stExpander"] {{ border-radius: 14px; border:1px solid #e5eeea; }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="nx-hero">
      <span class="nx-pill">Offtaker view · settlements payable to the buyer</span>
      <h1>🛡️ PPA &amp; DASS Settlements</h1>
      <p>What the hedge would have paid you — and what it is expected to pay.
      Historical settlements 2021 – Jun 2026 on OMIE outturn, forward 2027 – 2040 on the
      Aurora nominal curve or your own price scenario. Monthly or annual, in €/MWh · k€/MW and in €.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Constants & paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_FILE_CANDIDATES = [
    DATA_DIR / "PV___BESS_settlement_app.xlsx",
    DATA_DIR / "PV_BESS_settlement_app.xlsx",
    BASE_DIR / "PV___BESS_settlement_app.xlsx",
]

BESS_POWER_MW = 1.0
BESS_CAPACITY_MWH = 4.0
ETA_CH = 0.925
ETA_DIS = 0.925                                  # RTE ~ 0.856
E_CHARGE_GRID = BESS_CAPACITY_MWH / ETA_CH       # 4.3243 MWh bought per cycle
E_DISCHARGE = BESS_CAPACITY_MWH * ETA_DIS        # 3.7000 MWh sold per cycle


def resolve_data_file() -> Path | None:
    for p in DATA_FILE_CANDIDATES:
        if p.exists():
            return p
    return None


def kpi(col, label: str, value: str, unit: str = "", foot: str = "", tone: str = "neu"):
    col.markdown(
        f"""
        <div class="kpi">
          <div class="label">{label}</div>
          <div class="value {tone}">{value}<span class="unit">{unit}</span></div>
          <div class="foot">{foot}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section(title: str, color: str, subtitle: str, badge: str, badge_cls: str = ""):
    st.markdown(
        f"""
        <div class="nx-section">
          <span class="nx-dot" style="background:{color};"></span>
          <h2>{title}</h2>
          <span class="offtaker-badge {badge_cls}">{badge}</span>
        </div>
        <p class="nx-sub">{subtitle}</p>
        """,
        unsafe_allow_html=True,
    )


def style_chart(ch: alt.Chart) -> alt.Chart:
    return (ch.configure_axis(labelColor=MUTED, titleColor=MUTED, gridColor="#eef4f1",
                              domainColor="#dbe7e1", labelFontSize=11, titleFontSize=12)
              .configure_view(strokeWidth=0))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner="Loading hourly price & solar dataset…")
def load_hourly(path_str: str) -> pd.DataFrame:
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
    raw = pd.read_excel(path_str, sheet_name=0, usecols="Z", nrows=14)
    shares = raw.iloc[:, 0].dropna().astype(float).to_numpy()[:12]
    shares = shares / shares.sum()
    return pd.Series(shares, index=range(1, 13), name="share")


@st.cache_data(show_spinner=False)
def typical_solar_shape(hourly: pd.DataFrame) -> pd.DataFrame:
    hist = hourly[hourly["year"] <= 2025]
    return (hist.groupby(["month", "hour"])["solar"].mean()
                .reset_index().rename(columns={"solar": "solar_shape"}))


# ---------------------------------------------------------------------------
# BESS dispatch & capture computation
# ---------------------------------------------------------------------------
def _dispatch_day(prices: np.ndarray) -> tuple[float, float]:
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
    return pd.DataFrame(recs, columns=["year", "month", "day",
                                       "rev_eur_mw", "tb4"]).dropna()


@st.cache_data(show_spinner=False)
def monthly_capture(hourly: pd.DataFrame, settle_at_nonpositive: bool) -> pd.DataFrame:
    df = hourly.copy()
    df["w"] = df["solar"]
    df["w_settle"] = df["solar"] if settle_at_nonpositive \
        else np.where(df["price"] > 0, df["solar"], 0.0)

    def agg(g: pd.DataFrame) -> pd.Series:
        tot = g["w"].sum()
        settled = g["w_settle"].sum()
        cap = np.average(g["price"], weights=g["w_settle"]) if settled > 0 else np.nan
        return pd.Series({
            "capture": cap,
            "settled_share": settled / tot if tot > 0 else np.nan,
            "neg_hours_gen_share": 1 - settled / tot if tot > 0 else np.nan,
        })

    return df.groupby(["year", "month"]).apply(agg, include_groups=False).reset_index()


# ---------------------------------------------------------------------------
# Uploaded forward curve
# ---------------------------------------------------------------------------
def parse_uploaded_curve(uploaded) -> pd.DataFrame | None:
    try:
        raw = (pd.read_excel(uploaded) if uploaded.name.lower().endswith((".xlsx", ".xls"))
               else pd.read_csv(uploaded))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not read the file: {exc}")
        return None
    dt_col, price_col = None, None
    for c in raw.columns:
        parsed = pd.to_datetime(raw[c], errors="coerce")
        if parsed.notna().mean() > 0.9:
            dt_col, raw[c] = c, parsed
            break
    for c in raw.columns:
        if c == dt_col:
            continue
        vals = pd.to_numeric(raw[c], errors="coerce")
        if vals.notna().mean() > 0.9:
            price_col, raw[c] = c, vals
            break
    if dt_col is None or price_col is None:
        st.error("The file needs one datetime column and one numeric hourly price column.")
        return None
    out = pd.DataFrame({
        "year": raw[dt_col].dt.year, "month": raw[dt_col].dt.month,
        "day": raw[dt_col].dt.day, "hour": raw[dt_col].dt.hour + 1,
        "price": raw[price_col].astype(float),
    }).dropna()
    out[["year", "month", "day"]] = out[["year", "month", "day"]].astype(int)
    return out


def curve_template_bytes() -> bytes:
    idx = pd.date_range("2027-01-01 00:00", "2027-12-31 23:00", freq="h")
    buf = BytesIO()
    pd.DataFrame({"datetime": idx, "price_eur_mwh": 60.0}).to_csv(buf, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
data_path = resolve_data_file()
if data_path is None:
    st.error("Data file `PV___BESS_settlement_app.xlsx` not found in `data/`.")
    st.stop()

hourly_base = load_hourly(str(data_path))
monthly_shares = load_monthly_shares(str(data_path))
solar_shape = typical_solar_shape(hourly_base)
HIST_MAX_YEAR = 2026

# ---------------------------------------------------------------------------
# Global controls
# ---------------------------------------------------------------------------
with st.container(border=True):
    top1, top2, top3, top4 = st.columns([1.0, 1.4, 1.2, 1.6])
    with top1:
        granularity = st.radio("View", ["Annual", "Monthly"], horizontal=True)
    with top2:
        year_range = st.slider("Period", 2021, 2040, (2021, 2035))
    with top3:
        settle_nonpos = st.toggle(
            "Settle at 0 / negative prices", value=True,
            help="ON → every generated MWh settles (workbook 'Uncurtailed' capture). "
                 "OFF → hours priced ≤ 0 €/MWh neither settle nor count as volume "
                 "(workbook 'Curtailed' capture).",
        )
    with top4:
        curve_source = st.radio(
            "Forward curve (≥ 2027)",
            ["Aurora (workbook, nominal)", "Upload my own hourly curve"],
            horizontal=True,
        )

uploaded_curve = None
if curve_source.startswith("Upload"):
    up_col1, up_col2 = st.columns([2, 1])
    with up_col1:
        up = st.file_uploader("Hourly forward price curve (CSV/XLSX: datetime + €/MWh)",
                              type=["csv", "xlsx", "xls"])
        if up is not None:
            uploaded_curve = parse_uploaded_curve(up)
    with up_col2:
        st.download_button("⬇️ Template (CSV)", data=curve_template_bytes(),
                           file_name="forward_curve_template.csv", mime="text/csv")

if uploaded_curve is not None and not uploaded_curve.empty:
    fwd_years = sorted(uploaded_curve["year"].unique())
    hist_part = hourly_base[hourly_base["year"] <= HIST_MAX_YEAR].copy()
    fwd = uploaded_curve.merge(solar_shape, on=["month", "hour"], how="left")
    fwd["solar"] = fwd["solar_shape"].fillna(0.0)
    fwd = fwd[["year", "month", "day", "hour", "price", "solar"]]
    hourly = pd.concat([hist_part, fwd], ignore_index=True)
    st.info(
        f"Using **your uploaded curve** for {fwd_years[0]}–{fwd_years[-1]} "
        f"({len(uploaded_curve):,} hours). BESS dispatch is re-optimised on these prices; "
        "solar capture uses the historical month-hour solar shape."
    )
else:
    hourly = hourly_base

hourly = hourly[(hourly.year >= year_range[0]) & (hourly.year <= year_range[1])]
if hourly.empty:
    st.warning("No data in the selected period.")
    st.stop()

cap_m = monthly_capture(hourly, settle_nonpos)
bess_day = bess_daily_results(hourly)
bess_m = (bess_day.groupby(["year", "month"])
          .agg(rev_eur_mw=("rev_eur_mw", "sum"), tb4=("tb4", "mean")).reset_index())

# ======================================================================
# 1) SOLAR PV PPA
# ======================================================================
section(
    "Solar PV PPA", GREEN,
    "Settlement to the offtaker = captured market price − contract price. "
    "Positive bars mean the market paid above your contract: cash flows to you.",
    "☀️ Offtaker receives when green",
)

with st.container(border=True):
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
ppa["settle_eur_mwh"] = ppa["capture"] - ppa["contract_price"]
ppa["settle_eur"] = ppa["settle_eur_mwh"] * ppa["settled_mwh"]
ppa["period"] = ppa["year"].astype(str) + "-" + ppa["month"].map(lambda m: f"{m:02d}")

if granularity == "Annual":
    ppa_view = (ppa.groupby("year")
                .apply(lambda g: pd.Series({
                    "settle_eur": g["settle_eur"].sum(),
                    "settle_eur_mwh": (g["settle_eur"].sum() / g["settled_mwh"].sum())
                                       if g["settled_mwh"].sum() > 0 else np.nan,
                    "capture": ((g["capture"] * g["settled_mwh"]).sum() / g["settled_mwh"].sum())
                               if g["settled_mwh"].sum() > 0 else np.nan,
                    "neg_hours_gen_share": g["neg_hours_gen_share"].mean(),
                }), include_groups=False).reset_index())
    ppa_view["period"] = ppa_view["year"].astype(str)
else:
    ppa_view = ppa.copy()

avg_settle_mwh = ppa_view["settle_eur"].sum() / max(ppa["settled_mwh"].sum(), 1e-9)
cum_ppa = ppa_view["settle_eur"].sum() / 1e6
tone_ppa = "pos" if avg_settle_mwh >= 0 else "neg"

k1, k2, k3, k4 = st.columns(4)
kpi(k1, "Avg settlement to offtaker", f"{avg_settle_mwh:+.1f}", "€/MWh",
    "Per settled MWh, selected period", tone_ppa)
kpi(k2, "Cumulative settlement", f"{cum_ppa:+.2f}", "M€",
    f"{ppa_volume_gwh:,.0f} GWh/yr PPA · {'settles ≤0€' if settle_nonpos else 'no settle ≤0€'}",
    tone_ppa)
kpi(k3, "Avg captured price", f"{np.nansum(ppa['capture']*ppa['settled_mwh'])/max(ppa['settled_mwh'].sum(),1e-9):.1f}",
    "€/MWh", "Solar-weighted market price", "neu")
kpi(k4, "Generation in ≤0 € hours", f"{100*ppa['neg_hours_gen_share'].mean():.1f}", "%",
    "Share of solar volume at non-positive prices", "neu")

st.markdown("")
ppa_chart = ppa_view.copy()
ppa_chart["sign"] = np.where(ppa_chart["settle_eur_mwh"] >= 0,
                             "Offtaker receives", "Offtaker pays")
zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="#c9d7d1").encode(y="y:Q")

bars = (alt.Chart(ppa_chart)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("period:N", title=None, sort=None),
            y=alt.Y("settle_eur_mwh:Q", title="Settlement to offtaker (€/MWh)"),
            color=alt.Color("sign:N",
                            scale=alt.Scale(domain=["Offtaker receives", "Offtaker pays"],
                                            range=[GREEN, ORANGE]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=[
                alt.Tooltip("period:N", title="Period"),
                alt.Tooltip("capture:Q", title="Captured price €/MWh", format=".1f"),
                alt.Tooltip("settle_eur_mwh:Q", title="Settlement €/MWh", format="+.1f"),
                alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f"),
            ])
        .properties(height=300, title=alt.TitleParams(
            "PPA settlement per MWh", anchor="start", fontSize=14, color=INK)))
st.altair_chart(style_chart(alt.layer(bars, zero)), use_container_width=True)

bars_eur = (alt.Chart(ppa_chart)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("period:N", title=None, sort=None),
                y=alt.Y("settle_eur:Q", title="Settlement to offtaker (€)"),
                color=alt.Color("sign:N",
                                scale=alt.Scale(domain=["Offtaker receives", "Offtaker pays"],
                                                range=[GREEN, ORANGE]), legend=None),
                tooltip=[alt.Tooltip("period:N"),
                         alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")])
            .properties(height=240, title=alt.TitleParams(
                f"PPA settlement in € — {ppa_volume_gwh:,.0f} GWh/yr, monthly solar shape",
                anchor="start", fontSize=14, color=INK)))
st.altair_chart(style_chart(alt.layer(bars_eur, zero)), use_container_width=True)

with st.expander("PPA settlement table"):
    tbl = (ppa if granularity == "Monthly" else ppa_view)
    cols = (["period", "capture", "contract_price", "settle_eur_mwh",
             "volume_mwh", "settled_mwh", "settle_eur", "neg_hours_gen_share"]
            if granularity == "Monthly" else
            ["period", "capture", "settle_eur_mwh", "settle_eur", "neg_hours_gen_share"])
    st.dataframe(tbl[cols].rename(columns={
        "capture": "Captured €/MWh", "contract_price": "Contract €/MWh",
        "settle_eur_mwh": "Settlement €/MWh", "volume_mwh": "Volume MWh",
        "settled_mwh": "Settled MWh", "settle_eur": "Settlement €",
        "neg_hours_gen_share": "% gen in ≤0€ h",
    }).round(2), use_container_width=True, hide_index=True)

# ======================================================================
# 2) BESS DASS
# ======================================================================
section(
    "BESS Day-Ahead Spread Swap (DASS)", PURPLE,
    "Settlement to the buyer = realised TB4 arbitrage revenue − fixed strike. "
    "The dashed line shows the market TB4 spread OMIE must print for the BESS to earn that revenue.",
    "🔋 Buyer receives when purple", "dass",
)

with st.container(border=True):
    d1, d2, d3 = st.columns([1.2, 1, 1.8])
    with d1:
        dass_strike = st.number_input("DASS strike (k€/MW·yr)", 0.0, 300.0, 70.0, 1.0)
    with d2:
        dass_mw = st.number_input("Contracted BESS capacity (MW)", 0.5, 1000.0, 10.0, 0.5)
    with d3:
        eq_spread = dass_strike * 1000 / (E_DISCHARGE * 365)
        st.markdown(
            f"<div style='color:{MUTED};font-size:.85rem;padding-top:28px;'>"
            f"Strike ≈ <b>{eq_spread:.1f} €/MWh</b> of realised TB4 spread "
            f"({E_DISCHARGE:.2f} MWh discharged per MW per day, 1 cycle/day).</div>",
            unsafe_allow_html=True,
        )

bess = bess_m.copy()
ndays = (bess_day.groupby(["year", "month"])["day"].nunique().reset_index(name="ndays"))
bess = bess.merge(ndays, on=["year", "month"])
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
                      tb4=("tb4", "mean")).reset_index())
    bess_view["period"] = bess_view["year"].astype(str)
else:
    bess_view = bess.copy()

avg_settle = bess_view["settle_eur_mw"].mean() / 1000
tone_dass = "pos" if avg_settle >= 0 else "neg"
k1, k2, k3, k4 = st.columns(4)
kpi(k1, "Avg settlement to buyer", f"{avg_settle:+.1f}",
    f"k€/MW·{'yr' if granularity == 'Annual' else 'mo'}",
    f"Strike {dass_strike:.0f} k€/MW·yr", tone_dass)
kpi(k2, "Cumulative settlement", f"{bess_view['settle_eur'].sum()/1e6:+.2f}", "M€",
    f"{dass_mw:.0f} MW contracted", tone_dass)
kpi(k3, "Avg BESS revenue", f"{bess_view['rev_eur_mw'].mean()/1000:.1f}",
    f"k€/MW·{'yr' if granularity == 'Annual' else 'mo'}",
    "1 MW/4 MWh · 1 cycle/day · RTE 0.856", "pur")
kpi(k4, "Avg market TB4 spread", f"{bess_view['tb4'].mean():.1f}", "€/MWh",
    "Mean of daily top-4 − bottom-4 prices", "neu")

st.markdown("")
bess_chart = bess_view.copy()
bess_chart["sign"] = np.where(bess_chart["settle_eur_mw"] >= 0,
                              "Buyer receives", "Buyer pays")
bess_chart["settle_keur_mw"] = bess_chart["settle_eur_mw"] / 1000.0

settle_bars = (alt.Chart(bess_chart)
               .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
               .encode(
                   x=alt.X("period:N", title=None, sort=None),
                   y=alt.Y("settle_keur_mw:Q", title="Settlement to buyer (k€/MW)"),
                   color=alt.Color("sign:N",
                                   scale=alt.Scale(domain=["Buyer receives", "Buyer pays"],
                                                   range=[PURPLE, RED]),
                                   legend=alt.Legend(title=None, orient="top")),
                   tooltip=[
                       alt.Tooltip("period:N", title="Period"),
                       alt.Tooltip("rev_eur_mw:Q", title="BESS revenue €/MW", format=",.0f"),
                       alt.Tooltip("settle_keur_mw:Q", title="Settlement k€/MW", format="+.1f"),
                       alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f"),
                       alt.Tooltip("tb4:Q", title="TB4 spread €/MWh", format=".1f"),
                   ])
               .properties(height=300, title=alt.TitleParams(
                   "DASS settlement vs market TB4 spread",
                   anchor="start", fontSize=14, color=INK)))
tb4_line = (alt.Chart(bess_chart)
            .mark_line(point={"filled": True, "size": 45}, color=ORANGE, strokeDash=[5, 3])
            .encode(x=alt.X("period:N", sort=None),
                    y=alt.Y("tb4:Q", title="Market TB4 spread (€/MWh)"),
                    tooltip=[alt.Tooltip("period:N"),
                             alt.Tooltip("tb4:Q", title="TB4 €/MWh", format=".1f")]))
st.altair_chart(
    style_chart(alt.layer(settle_bars, tb4_line).resolve_scale(y="independent")),
    use_container_width=True,
)

bars_eur2 = (alt.Chart(bess_chart)
             .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
             .encode(
                 x=alt.X("period:N", title=None, sort=None),
                 y=alt.Y("settle_eur:Q", title="Settlement to buyer (€)"),
                 color=alt.Color("sign:N",
                                 scale=alt.Scale(domain=["Buyer receives", "Buyer pays"],
                                                 range=[PURPLE, RED]), legend=None),
                 tooltip=[alt.Tooltip("period:N"),
                          alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")])
             .properties(height=240, title=alt.TitleParams(
                 f"DASS settlement in € — {dass_mw:.0f} MW contracted",
                 anchor="start", fontSize=14, color=INK)))
st.altair_chart(style_chart(alt.layer(bars_eur2, zero)), use_container_width=True)

with st.expander("DASS settlement table"):
    st.dataframe(
        bess_view[["period", "rev_eur_mw", "strike_eur_mw", "settle_eur_mw",
                   "settle_eur", "tb4"]]
        .rename(columns={"rev_eur_mw": "BESS revenue €/MW",
                         "strike_eur_mw": "Strike €/MW",
                         "settle_eur_mw": "Settlement €/MW",
                         "settle_eur": "Settlement €",
                         "tb4": "TB4 spread €/MWh"}).round(1),
        use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Methodology / data lineage
# ---------------------------------------------------------------------------
with st.expander("ℹ️ Where every number comes from (data lineage & reconciliation)"):
    st.markdown(
        f"""
- **Single data source**: `data/PV___BESS_settlement_app.xlsx`, hourly day-ahead price
  (2021 – Jun 2026 OMIE outturn; 2027 – 2040 Aurora **nominal**) and the hourly solar
  production profile.
- **Captured price** = solar-weighted average of hourly prices per month, computed from
  the hourly columns — *not* read from the workbook's summary table. Reconciliation:
  with *Settle at 0/negative* **ON** it matches the workbook's **Uncurtailed capture
  price** column exactly; **OFF** matches the **Curtailed** column exactly (hours
  ≤ 0 €/MWh are removed from both price and settled volume). If you compare a
  settlement against the *other* column, you will see a gap — that is the toggle,
  not a data mismatch.
- **BESS revenue & TB4** are computed here from the hourly prices: the workbook's own
  BESS-revenue and TB4 summary columns are empty, so there is no workbook figure to
  match. Definitions used: revenue = one optimal cycle/day buying
  {E_CHARGE_GRID:.2f} MWh in the cheapest hours and selling {E_DISCHARGE:.2f} MWh in the
  most expensive (η = 0.925/0.925 → RTE ≈ 0.856, undegraded);
  **TB4** = daily *(mean of 4 highest − mean of 4 lowest)* hourly prices, averaged over
  the days of the month/year. Revenue ≠ TB4 × energy exactly, because charging spills
  into a 5ᵗʰ hour (4.32 MWh > 4 h × 1 MW) and the 4ᵗʰ discharge hour is partial —
  small differences vs other TB4-based estimates (e.g. the LP optimiser in the BESS
  page, which allows different cycle limits) are expected.
- **PPA volume** = your annual GWh × the workbook's monthly solar share distribution.
- **Sign convention**: positive = payment **to the offtaker / swap buyer**
  (market above contract). 2026 figures are year-to-date (data through June).
  Values are indicative, pre-fees and collateral.
"""
    )
