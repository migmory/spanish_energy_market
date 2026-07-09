# pages/7_PPA_DASS_Settlements.py
"""
PPA & DASS Settlements — offtaker-facing settlement dashboard.

Data lineage (single source of truth = data/PV___BESS_settlement_app.xlsx):
- Captured price = solar-weighted hourly day-ahead price. Monthly figures match the
  workbook's monthly capture columns exactly; annual figures match the workbook's
  annual "Uncurtailed/Curtailed capture price" columns exactly because monthly PPA
  volumes follow each year's ACTUAL solar profile.
- BESS revenue & TB4 are computed here from the hourly prices (the workbook's own
  BESS/TB4 summary columns are empty). One cycle/day: buy 4/0.925 MWh in the cheapest
  hours, sell 4*0.925 MWh in the priciest (RTE ~0.856, undegraded).
- Performance: the xlsx is converted once to a parquet cache next to it; subsequent
  loads are near-instant. The BESS dispatch is fully vectorised.
"""

from __future__ import annotations

import os
import smtplib
import urllib.parse
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page setup & corporate styling
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
RTE = 0.925 * 0.925  # ~0.856

st.markdown(
    f"""
    <style>
    .block-container {{ padding-top: 1.1rem; max-width: 1550px; }}
    html, body, [class*="css"] {{ font-family: "Inter","Segoe UI",system-ui,sans-serif; }}

    .nx-hero {{
        background: linear-gradient(120deg, {GREEN_DARK} 0%, {GREEN} 55%, #2fae79 100%);
        border-radius: 18px; padding: 26px 32px; color: #fff; margin-bottom: 18px;
        box-shadow: 0 8px 24px rgba(15,107,71,.18);
    }}
    .nx-hero h1 {{ font-size: 2.0rem; font-weight: 800; margin: 0; letter-spacing:-.02em; color:#fff; }}
    .nx-hero p  {{ margin: 6px 0 0 0; color: #d9efe6; font-size: .95rem; max-width: 940px; }}
    .nx-pill {{
        display:inline-block; background:#fff; color:{GREEN_DARK}; font-weight:700;
        font-size:.75rem; padding:4px 12px; border-radius:999px; margin-bottom:10px;
        letter-spacing:.06em; text-transform:uppercase;
    }}

    .nx-section {{ display:flex; align-items:center; gap:10px; margin: 28px 0 4px 0; }}
    .nx-dot {{ width:13px; height:13px; border-radius:4px; display:inline-block; }}
    .nx-section h2 {{ font-size:1.45rem; font-weight:800; color:{INK}; margin:0; letter-spacing:-.01em; }}
    .nx-sub {{ color:{MUTED}; font-size:.9rem; margin: 0 0 12px 23px; }}

    .kpi {{
        background:#fff; border:1px solid #e5eeea; border-radius:18px;
        padding:20px 24px 18px 24px; box-shadow: 0 3px 12px rgba(18,51,42,.06); height:100%;
    }}
    .kpi .label {{ color:{MUTED}; font-size:.8rem; font-weight:700; text-transform:uppercase; letter-spacing:.07em; }}
    .kpi .value {{ font-size:3.1rem; font-weight:800; letter-spacing:-.035em; line-height:1.05; margin-top:4px; }}
    .kpi .unit  {{ font-size:1.15rem; font-weight:700; color:{MUTED}; margin-left:5px; }}
    .kpi .foot  {{ color:{MUTED}; font-size:.8rem; margin-top:8px; }}
    .pos {{ color:{GREEN_DARK}; }} .neg {{ color:{RED}; }} .neu {{ color:{INK}; }} .pur {{ color:{PURPLE}; }}

    .offtaker-badge {{
        display:inline-block; background:{GREEN_SOFT}; color:{GREEN_DARK};
        border:1px solid #cfe8dc; font-weight:700; font-size:.8rem;
        padding:5px 14px; border-radius:999px; letter-spacing:.03em;
    }}
    .offtaker-badge.dass {{ background:{PURPLE_SOFT}; color:{PURPLE}; border-color:#ddd8f5; }}

    /* segmented-control look for horizontal radios */
    div[role="radiogroup"] {{ gap: 6px; }}
    div[role="radiogroup"] > label {{
        background:#f4f8f6; border:1px solid #dbe7e1; border-radius:999px;
        padding:4px 14px 4px 8px; transition: all .15s ease;
    }}
    div[role="radiogroup"] > label:has(input:checked) {{
        background:{GREEN_SOFT}; border-color:{GREEN}; font-weight:700;
    }}
    /* slider accent */
    div[data-baseweb="slider"] div[role="slider"] {{ background:{GREEN_DARK}; }}
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
ETA_DIS = 0.925
E_CHARGE_GRID = BESS_CAPACITY_MWH / ETA_CH   # 4.3243 MWh bought / cycle
E_DISCHARGE = BESS_CAPACITY_MWH * ETA_DIS    # 3.7000 MWh sold / cycle
MONTH_ABBR = ["J", "F", "M", "A", "M2", "J2", "Jl", "Au", "S", "O", "N", "D"]
MONTH_LBL = {i + 1: l for i, l in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


def resolve_data_file() -> Path | None:
    for p in DATA_FILE_CANDIDATES:
        if p.exists():
            return p
    return None


def kpi(col, label: str, value: str, unit: str = "", foot: str = "", tone: str = "neu"):
    col.markdown(
        f"""<div class="kpi"><div class="label">{label}</div>
        <div class="value {tone}">{value}<span class="unit">{unit}</span></div>
        <div class="foot">{foot}</div></div>""",
        unsafe_allow_html=True,
    )


def section(title: str, color: str, subtitle: str, badge: str, badge_cls: str = ""):
    st.markdown(
        f"""<div class="nx-section"><span class="nx-dot" style="background:{color};"></span>
        <h2>{title}</h2><span class="offtaker-badge {badge_cls}">{badge}</span></div>
        <p class="nx-sub">{subtitle}</p>""",
        unsafe_allow_html=True,
    )


def style_chart(ch):
    return (ch.configure_axis(labelColor=MUTED, titleColor=MUTED, gridColor="#eef4f1",
                              domainColor="#dbe7e1", labelFontSize=11, titleFontSize=12)
              .configure_header(labelFontSize=13, labelFontWeight="bold", labelColor=INK,
                                titleColor=MUTED)
              .configure_view(strokeWidth=0))


# ---------------------------------------------------------------------------
# Fast data loading: xlsx -> parquet disk cache
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner="Loading dataset…")
def load_dataset(path_str: str, mtime: float) -> tuple[pd.DataFrame, pd.Series]:
    path = Path(path_str)
    pq = path.with_suffix(".parquet")
    sh = path.with_name(path.stem + "_shares.parquet")
    if pq.exists() and sh.exists() and pq.stat().st_mtime >= mtime:
        hourly = pd.read_parquet(pq)
        shares = pd.read_parquet(sh)["share"]
        shares.index = range(1, 13)
        return hourly, shares

    df = pd.read_excel(path, sheet_name=0, usecols="A:F")
    df.columns = ["date", "year", "month", "hour", "price", "solar"]
    df = df.dropna(subset=["year", "price"]).copy()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["day"] = pd.to_datetime(df["date"]).dt.day
    df["solar"] = df["solar"].fillna(0.0).astype(float)
    df["price"] = df["price"].astype(float)
    hourly = df[["year", "month", "day", "hour", "price", "solar"]].reset_index(drop=True)

    raw = pd.read_excel(path, sheet_name=0, usecols="Z", nrows=14)
    s = raw.iloc[:, 0].dropna().astype(float).to_numpy()[:12]
    shares = pd.Series(s / s.sum(), index=range(1, 13), name="share")

    try:  # persist for next cold start (works locally & on Streamlit Cloud runtime)
        hourly.to_parquet(pq, index=False)
        shares.rename("share").to_frame().to_parquet(sh)
    except Exception:  # noqa: BLE001 - read-only FS: just skip disk cache
        pass
    return hourly, shares


@st.cache_data(show_spinner=False)
def typical_solar_shape(hourly: pd.DataFrame) -> pd.DataFrame:
    hist = hourly[hourly["year"] <= 2025]
    return (hist.groupby(["month", "hour"])["solar"].mean()
                .reset_index().rename(columns={"solar": "solar_shape"}))


# ---------------------------------------------------------------------------
# BESS dispatch — vectorised (complete 24h days) + loop fallback for odd days
# ---------------------------------------------------------------------------
CH_W = np.array([1.0, 1.0, 1.0, 1.0, E_CHARGE_GRID - 4.0])   # 5 cheapest, partial last
DIS_W = np.array([E_DISCHARGE - 3.0, 1.0, 1.0, 1.0])          # 4 priciest, partial first


def _dispatch_day_loop(p: np.ndarray) -> tuple[float, float]:
    n = len(p)
    if n < 10:
        return np.nan, np.nan
    order = np.argsort(p)
    ch = np.zeros(n)
    rem = E_CHARGE_GRID
    for i in order:
        t = min(BESS_POWER_MW, rem)
        ch[i] = t
        rem -= t
        if rem <= 1e-9:
            break
    di = np.zeros(n)
    rem = E_DISCHARGE
    for i in order[::-1]:
        t = min(BESS_POWER_MW, rem)
        di[i] = t
        rem -= t
        if rem <= 1e-9:
            break
    srt = np.sort(p)
    return float((di * p).sum() - (ch * p).sum()), float(srt[-4:].mean() - srt[:4].mean())


@st.cache_data(show_spinner="Optimising BESS dispatch…")
def bess_daily_results(hourly: pd.DataFrame) -> pd.DataFrame:
    df = hourly.sort_values(["year", "month", "day", "hour"])
    sizes = df.groupby(["year", "month", "day"], sort=True).size()
    full_keys = sizes[sizes == 24].index
    odd_keys = sizes[sizes != 24].index

    df_idx = df.set_index(["year", "month", "day"])
    recs = []

    if len(full_keys):
        block = df_idx.loc[full_keys, "price"].to_numpy().reshape(-1, 24)
        s = np.sort(block, axis=1)
        rev = s[:, -4:] @ DIS_W - s[:, :5] @ CH_W
        tb4 = s[:, -4:].mean(axis=1) - s[:, :4].mean(axis=1)
        keys = np.array(list(full_keys))
        recs.append(pd.DataFrame({"year": keys[:, 0], "month": keys[:, 1],
                                  "day": keys[:, 2], "rev_eur_mw": rev, "tb4": tb4}))
    for key in odd_keys:
        rev, tb4 = _dispatch_day_loop(df_idx.loc[[key], "price"].to_numpy(float))
        recs.append(pd.DataFrame({"year": [key[0]], "month": [key[1]], "day": [key[2]],
                                  "rev_eur_mw": [rev], "tb4": [tb4]}))
    return pd.concat(recs, ignore_index=True).dropna()


@st.cache_data(show_spinner=False)
def monthly_capture(hourly: pd.DataFrame, settle_at_nonpositive: bool) -> pd.DataFrame:
    df = hourly.copy()
    df["w"] = df["solar"]
    df["w_settle"] = df["solar"] if settle_at_nonpositive \
        else np.where(df["price"] > 0, df["solar"], 0.0)
    df["pw"] = df["price"] * df["w_settle"]
    g = df.groupby(["year", "month"]).agg(pw=("pw", "sum"), ws=("w_settle", "sum"),
                                          w=("w", "sum")).reset_index()
    g["capture"] = np.where(g["ws"] > 0, g["pw"] / g["ws"], np.nan)
    g["settled_share"] = np.where(g["w"] > 0, g["ws"] / g["w"], np.nan)
    g["neg_hours_gen_share"] = 1 - g["settled_share"]
    g["solar_mwh"] = g["w"]  # relative units, used only for shaping volumes
    return g[["year", "month", "capture", "settled_share",
              "neg_hours_gen_share", "solar_mwh"]]


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
        "price": raw[price_col].astype(float)}).dropna()
    out[["year", "month", "day"]] = out[["year", "month", "day"]].astype(int)
    return out


def curve_template_bytes() -> bytes:
    idx = pd.date_range("2027-01-01 00:00", "2027-12-31 23:00", freq="h")
    tmpl = pd.DataFrame({"Datetime (hourly)": idx, "Price (€/MWh)": np.nan})
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        tmpl.to_excel(xw, index=False, sheet_name="forward_curve")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Chart builders (annual layered / monthly faceted by year)
# ---------------------------------------------------------------------------
def settlement_chart(df: pd.DataFrame, ycol: str, ytitle: str, label_col: str | None,
                     pos_lbl: str, neg_lbl: str, pos_c: str, neg_c: str,
                     granularity: str, title: str, tb4_line: bool = False,
                     tooltips: list | None = None, height: int = 300):
    df = df.copy()
    df["sign"] = np.where(df[ycol] >= 0, pos_lbl, neg_lbl)
    color = alt.Color("sign:N", scale=alt.Scale(domain=[pos_lbl, neg_lbl],
                                                range=[pos_c, neg_c]),
                      legend=alt.Legend(title=None, orient="top"))
    tt = tooltips or []

    if granularity == "Annual":
        base = alt.Chart(df)
        zero = base.mark_rule(color="#c9d7d1").encode(y=alt.datum(0))
        bars = base.mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
            x=alt.X("period:N", title=None, sort=None,
                    scale=alt.Scale(paddingInner=0.45), axis=alt.Axis(labelAngle=0)),
            y=alt.Y(f"{ycol}:Q", title=ytitle), color=color, tooltip=tt)
        layers = [bars, zero]
        if label_col:
            layers.append(base.mark_text(dy=-9, fontSize=11, fontWeight="bold",
                                         color=MUTED).encode(
                x=alt.X("period:N", sort=None), y=alt.Y(f"{ycol}:Q"),
                text=alt.Text(f"{label_col}:N")))
        chart = alt.layer(*layers)
        if tb4_line:
            line = alt.Chart(df).mark_line(point={"filled": True, "size": 110},
                                           color=ORANGE, strokeWidth=3, strokeDash=[6, 3]).encode(
                x=alt.X("period:N", sort=None),
                y=alt.Y("tb4:Q", title="Market TB4 spread (€/MWh)"),
                tooltip=[alt.Tooltip("period:N"),
                         alt.Tooltip("tb4:Q", title="TB4 €/MWh", format=".1f")])
            chart = alt.layer(chart, line).resolve_scale(y="independent")
        return chart.properties(height=height, title=alt.TitleParams(
            title, anchor="start", fontSize=15, color=INK))

    # Monthly → facet by year for a clear visual separation between years
    df["mon"] = df["month"].map(MONTH_LBL)
    base = alt.Chart(df)
    zero = base.mark_rule(color="#c9d7d1").encode(y=alt.datum(0))
    bars = base.mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
        x=alt.X("mon:N", title=None, sort=list(MONTH_LBL.values()),
                axis=alt.Axis(labelAngle=-90, labelFontSize=9)),
        y=alt.Y(f"{ycol}:Q", title=ytitle), color=color, tooltip=tt)
    layers = [bars, zero]
    if label_col:
        lbl_kw = dict(dy=-8, fontSize=11, fontWeight="bold", color=ORANGE, angle=270,
                      align="left") if label_col == "tb4_lbl" else \
                 dict(dy=-7, fontSize=9, color=MUTED, angle=270, align="left")
        layers.append(base.mark_text(**lbl_kw).encode(
            x=alt.X("mon:N", sort=list(MONTH_LBL.values())),
            y=alt.Y(f"{ycol}:Q"), text=alt.Text(f"{label_col}:N")))
    return (alt.layer(*layers)
            .properties(height=height, width=alt.Step(16))
            .facet(column=alt.Column("year:N", title=None,
                                     header=alt.Header(labelFontSize=13,
                                                       labelFontWeight="bold")),
                   spacing=6)
            .resolve_scale(x="independent")
            .properties(title=alt.TitleParams(title, anchor="start",
                                              fontSize=15, color=INK)))


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
data_path = resolve_data_file()
if data_path is None:
    st.error("Data file `PV___BESS_settlement_app.xlsx` not found in `data/`.")
    st.stop()

hourly_base, monthly_shares = load_dataset(str(data_path), data_path.stat().st_mtime)
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
                 "(workbook 'Curtailed' capture).")
    with top4:
        curve_source = st.radio("Forward curve (≥ 2027)",
                                ["Aurora (workbook)", "Upload my own curve"],
                                horizontal=True)

uploaded_curve = None
if curve_source.startswith("Upload"):
    up_col1, up_col2 = st.columns([2, 1])
    with up_col1:
        up = st.file_uploader("Hourly forward price curve (CSV/XLSX: datetime + €/MWh)",
                              type=["csv", "xlsx", "xls"])
        if up is not None:
            uploaded_curve = parse_uploaded_curve(up)
    with up_col2:
        st.download_button(
            "⬇️ Template (XLSX)", data=curve_template_bytes(),
            file_name="forward_curve_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if uploaded_curve is not None and not uploaded_curve.empty:
    fwd_years = sorted(uploaded_curve["year"].unique())
    hist_part = hourly_base[hourly_base["year"] <= HIST_MAX_YEAR].copy()
    fwd = uploaded_curve.merge(solar_shape, on=["month", "hour"], how="left")
    fwd["solar"] = fwd["solar_shape"].fillna(0.0)
    fwd = fwd[["year", "month", "day", "hour", "price", "solar"]]
    hourly = pd.concat([hist_part, fwd], ignore_index=True)
    st.info(f"Using **your uploaded curve** for {fwd_years[0]}–{fwd_years[-1]} "
            f"({len(uploaded_curve):,} hours). BESS dispatch is re-optimised on these "
            "prices; solar capture uses the historical month-hour solar shape.")
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
section("Solar PV PPA", GREEN,
        "Settlement to the offtaker = captured market price − contract price. "
        "Green bars: the market paid above your contract — cash flows to you. "
        "The small figures on the bars are the captured prices (€/MWh).",
        "☀️ Offtaker receives when green")

with st.container(border=True):
    c1, c2 = st.columns([1.1, 1.0])
    with c1:
        ppa_type = st.radio("PPA structure", ["Fixed for floating", "Floor + discount"],
                            horizontal=True)
        ppa_volume_gwh = st.number_input("PPA volume (GWh/yr)", 1.0, 5000.0, 100.0, 10.0)
    with c2:
        if ppa_type == "Fixed for floating":
            strike = st.slider("PPA strike price (€/MWh)", 0.0, 150.0, 30.0, 0.5)
            floor, discount = 0.0, 0.0
        else:
            strike = 0.0
            floor = st.slider("Floor (€/MWh)", 0.0, 150.0, 30.0, 0.5)
            discount = st.slider("Discount to market (%)", 0.0, 60.0, 15.0, 0.5)

ppa = cap_m.copy()
# Monthly volumes follow each year's ACTUAL solar profile → annual capture
# reproduces the workbook's annual capture column exactly.
ppa["yr_solar"] = ppa.groupby("year")["solar_mwh"].transform("sum")
ppa["volume_mwh"] = ppa_volume_gwh * 1000.0 * ppa["solar_mwh"] / ppa["yr_solar"]
ppa["settled_mwh"] = ppa["volume_mwh"] * ppa["settled_share"].fillna(0)
if ppa_type == "Fixed for floating":
    ppa["contract_price"] = strike
else:
    ppa["contract_price"] = np.maximum(floor, ppa["capture"] * (1 - discount / 100.0))
ppa["settle_eur_mwh"] = ppa["capture"] - ppa["contract_price"]
ppa["settle_eur"] = ppa["settle_eur_mwh"] * ppa["settled_mwh"]
ppa["period"] = ppa["year"].astype(str) + "-" + ppa["month"].map(lambda m: f"{m:02d}")

if granularity == "Annual":
    g = ppa.groupby("year")
    ppa_view = pd.DataFrame({
        "settle_eur": g["settle_eur"].sum(),
        "settled_mwh": g["settled_mwh"].sum(),
        "capture": g.apply(lambda x: (x["capture"] * x["settled_mwh"]).sum()
                           / max(x["settled_mwh"].sum(), 1e-9), include_groups=False),
        "neg_hours_gen_share": g["neg_hours_gen_share"].mean(),
    }).reset_index()
    ppa_view["settle_eur_mwh"] = ppa_view["settle_eur"] / ppa_view["settled_mwh"].clip(lower=1e-9)
    ppa_view["period"] = ppa_view["year"].astype(str)
    ppa_view["month"] = 1
else:
    ppa_view = ppa.copy()
ppa_view["cap_lbl"] = ppa_view["capture"].map(lambda v: f"{v:.0f}€" if pd.notna(v) else "")

avg_settle_mwh = ppa_view["settle_eur"].sum() / max(ppa["settled_mwh"].sum(), 1e-9)
avg_capture = (ppa["capture"] * ppa["settled_mwh"]).sum() / max(ppa["settled_mwh"].sum(), 1e-9)
tone_ppa = "pos" if avg_settle_mwh >= 0 else "neg"

k1, k2, k3, k4 = st.columns(4)
kpi(k1, "Avg settlement to offtaker", f"{avg_settle_mwh:+.1f}", "€/MWh",
    "Per settled MWh, selected period", tone_ppa)
kpi(k2, "Cumulative settlement", f"{ppa_view['settle_eur'].sum()/1e6:+.2f}", "M€",
    f"{ppa_volume_gwh:,.0f} GWh/yr · {'settles ≤0€' if settle_nonpos else 'no settle ≤0€'}",
    tone_ppa)
kpi(k3, "Avg captured price", f"{avg_capture:.1f}", "€/MWh",
    "Solar-weighted market price", "neu")
kpi(k4, "Generation in ≤0 € hours", f"{100*ppa['neg_hours_gen_share'].mean():.1f}", "%",
    "Share of solar volume at non-positive prices", "neu")

st.markdown("")
tt_ppa = [alt.Tooltip("period:N", title="Period"),
          alt.Tooltip("capture:Q", title="Captured €/MWh", format=".1f"),
          alt.Tooltip("settle_eur_mwh:Q", title="Settlement €/MWh", format="+.1f"),
          alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")]
st.altair_chart(style_chart(settlement_chart(
    ppa_view, "settle_eur_mwh", "Settlement to offtaker (€/MWh)", "cap_lbl",
    "Offtaker receives", "Offtaker pays", GREEN, ORANGE, granularity,
    "PPA settlement per MWh — captured price shown on each bar",
    tooltips=tt_ppa)), use_container_width=True)

st.altair_chart(style_chart(settlement_chart(
    ppa_view, "settle_eur", "Settlement to offtaker (€)", None,
    "Offtaker receives", "Offtaker pays", GREEN, ORANGE, granularity,
    f"PPA settlement in € — {ppa_volume_gwh:,.0f} GWh/yr shaped on actual solar profile",
    tooltips=[alt.Tooltip("period:N"),
              alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")],
    height=240)), use_container_width=True)

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
        "neg_hours_gen_share": "% gen in ≤0€ h"}).round(2),
        use_container_width=True, hide_index=True)

# ======================================================================
# 2) BESS DASS
# ======================================================================
section("BESS Day-Ahead Spread Swap (DASS)", PURPLE,
        "Settled as a CfD on BESS market revenues: monthly settlement = realised "
        "day-ahead revenue per MW − strike prorated by the days of the month over 365; "
        "annual settlement = realised annual revenue − full strike (e.g. 70 k€/MW·yr). "
        "Orange figures = the market TB4 spread (€/MWh) behind those revenues.",
        "🔋 Buyer receives when green", "dass")

with st.container(border=True):
    d1, d2, d3 = st.columns([1.4, 1.0, 1.4])
    with d1:
        dass_strike = st.slider("DASS strike (k€/MW·yr)", 0.0, 200.0, 70.0, 0.5)
    with d2:
        dass_mw = st.number_input("Contracted BESS capacity (MW)", 0.5, 1000.0, 1.0, 0.5)
    with d3:
        eq_spread = dass_strike * 1000 / (365 * BESS_CAPACITY_MWH * RTE)
        st.markdown(
            f"<div style='color:{MUTED};font-size:.87rem;padding-top:26px;'>"
            f"Strike ≈ <b>{eq_spread:.1f} €/MWh</b> OMIE TB4 spread equivalent "
            f"(strike ÷ 365 d × 4 h × 1 c/d × 0.856 RTE — approximation, as RTE "
            f"allocation between charge and discharge is not defined).</div>",
            unsafe_allow_html=True)

st.markdown(
    f"""<div style="background:{PURPLE_SOFT};border:1px solid #ddd8f5;border-radius:12px;
    padding:10px 16px;margin:6px 0 4px 0;color:{PURPLE};font-size:.85rem;font-weight:600;">
    ⚙️ BESS assumptions &nbsp;·&nbsp; 1 MW / 4 MWh &nbsp;·&nbsp; max 1 cycle/day
    &nbsp;·&nbsp; η<sub>ch</sub> = η<sub>dis</sub> = 0.925 → RTE ≈ 0.856 &nbsp;·&nbsp;
    charges {E_CHARGE_GRID:.2f} MWh / discharges {E_DISCHARGE:.2f} MWh per cycle
    &nbsp;·&nbsp; undegraded &nbsp;·&nbsp; forward prices nominal</div>""",
    unsafe_allow_html=True)

bess = bess_m.copy()
ndays = bess_day.groupby(["year", "month"])["day"].nunique().reset_index(name="ndays")
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
    bess_view["month"] = 1
else:
    bess_view = bess.copy()
bess_view["settle_keur_mw"] = bess_view["settle_eur_mw"] / 1000.0
bess_view["tb4_lbl"] = bess_view["tb4"].map(lambda v: f"{v:.0f}")

avg_settle = bess_view["settle_eur_mw"].mean() / 1000
tone_dass = "pos" if avg_settle >= 0 else "neg"
k1, k2, k3, k4 = st.columns(4)
kpi(k1, "Avg settlement to buyer", f"{avg_settle:+.1f}",
    f"k€/MW·{'yr' if granularity == 'Annual' else 'mo'}",
    f"Strike {dass_strike:.0f} k€/MW·yr ≈ {eq_spread:.0f} €/MWh TB4", tone_dass)
kpi(k2, "Cumulative settlement", f"{bess_view['settle_eur'].sum()/1e6:+.2f}", "M€",
    f"{dass_mw:.0f} MW contracted", tone_dass)
kpi(k3, "Avg BESS revenue", f"{bess_view['rev_eur_mw'].mean()/1000:.1f}",
    f"k€/MW·{'yr' if granularity == 'Annual' else 'mo'}",
    "1 MW/4 MWh · 1 cycle/day · RTE 0.856", "pur")
kpi(k4, "Avg market TB4 spread", f"{bess_view['tb4'].mean():.1f}", "€/MWh",
    "Mean of daily top-4 − bottom-4 prices", "neu")

st.markdown("")
tt_dass = [alt.Tooltip("period:N", title="Period"),
           alt.Tooltip("rev_eur_mw:Q", title="BESS revenue €/MW", format=",.0f"),
           alt.Tooltip("settle_keur_mw:Q", title="Settlement k€/MW", format="+.1f"),
           alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f"),
           alt.Tooltip("tb4:Q", title="TB4 spread €/MWh", format=".1f")]

if granularity == "Annual":
    ch = settlement_chart(bess_view, "settle_keur_mw", "Settlement to buyer (k€/MW)",
                          None, "Buyer receives", "Buyer pays", GREEN, RED,
                          "Annual", "DASS settlement vs market TB4 spread (dashed)",
                          tb4_line=True, tooltips=tt_dass)
else:
    ch = settlement_chart(bess_view, "settle_keur_mw", "Settlement to buyer (k€/MW)",
                          "tb4_lbl", "Buyer receives", "Buyer pays", GREEN, RED,
                          "Monthly", "DASS settlement — market TB4 spread (€/MWh) on each bar",
                          tooltips=tt_dass)
st.altair_chart(style_chart(ch), use_container_width=True)

st.altair_chart(style_chart(settlement_chart(
    bess_view, "settle_eur", "Settlement to buyer (€)", None,
    "Buyer receives", "Buyer pays", GREEN, RED, granularity,
    f"DASS settlement in € — {dass_mw:.0f} MW contracted",
    tooltips=[alt.Tooltip("period:N"),
              alt.Tooltip("settle_eur:Q", title="Settlement €", format=",.0f")],
    height=240)), use_container_width=True)

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
- **Single data source**: `data/PV___BESS_settlement_app.xlsx` — hourly day-ahead price
  (2021 – Jun 2026 OMIE outturn; 2027 – 2040 Aurora **nominal**) and hourly solar profile.
  On first run the workbook is converted to a parquet cache next to it, so subsequent
  loads are near-instant.
- **Captured price** = solar-weighted average of hourly prices, computed from the hourly
  columns. **Monthly** figures match the workbook's monthly capture columns exactly.
  **Annual** figures match the workbook's annual capture columns exactly, because monthly
  PPA volumes follow each year's **actual solar profile** (not a fixed average
  distribution). With *Settle at 0/negative* **ON** you reproduce the *Uncurtailed*
  columns; **OFF** the *Curtailed* ones (hours ≤ 0 €/MWh are removed from price and
  settled volume) — comparing against the other column will always show a gap.
- **BESS revenue & TB4** are computed here from the hourly prices (the workbook's own
  BESS/TB4 summary columns are empty). Revenue = one optimal cycle/day buying
  {E_CHARGE_GRID:.2f} MWh in the cheapest hours and selling {E_DISCHARGE:.2f} MWh in the
  most expensive (η = 0.925/0.925 → RTE ≈ 0.856, undegraded).
  **TB4** = daily *(mean of 4 highest − mean of 4 lowest)* prices, averaged per period.
  The strike-to-spread conversion is an approximation: strike ÷ (365 × 4 h × 1 c/d ×
  0.856 RTE), since the RTE split between charge and discharge is not defined.
- **Sign convention**: positive = payment **to the offtaker / swap buyer**.
  2026 figures are year-to-date (data through June). Values indicative, pre-fees.
"""
    )

# ---------------------------------------------------------------------------
# Contact — questions about the settlements go to Nexwell Power
# ---------------------------------------------------------------------------
CONTACT_TO = "mmoreno@nexwellpower.com"


def _smtp_config() -> dict | None:
    """SMTP credentials from st.secrets['smtp'] or environment variables
    (SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD). Returns None if absent."""
    cfg = {}
    try:
        cfg = dict(st.secrets.get("smtp", {}))
    except Exception:  # noqa: BLE001 - no secrets file
        cfg = {}
    host = cfg.get("host") or os.getenv("SMTP_HOST")
    if not host:
        return None
    return {
        "host": host,
        "port": int(cfg.get("port") or os.getenv("SMTP_PORT", 587)),
        "user": cfg.get("user") or os.getenv("SMTP_USER"),
        "password": cfg.get("password") or os.getenv("SMTP_PASSWORD"),
    }


def send_contact_email(sender: str, message: str) -> bool:
    cfg = _smtp_config()
    if cfg is None:
        return False
    msg = EmailMessage()
    msg["Subject"] = f"[PPA & DASS Settlements] Question from {sender}"
    msg["From"] = cfg["user"] or sender
    msg["To"] = CONTACT_TO
    msg["Reply-To"] = sender
    msg.set_content(
        f"Question submitted from the PPA & DASS Settlements app\n"
        f"From: {sender}\n\n{message}"
    )
    with smtplib.SMTP(cfg["host"], cfg["port"], timeout=15) as server:
        server.starttls()
        if cfg["user"] and cfg["password"]:
            server.login(cfg["user"], cfg["password"])
        server.send_message(msg)
    return True


section("Questions about these settlements?", ORANGE,
        "Leave your e-mail and your question — it goes straight to the Nexwell Power "
        "origination desk.", "✉️ We reply within 1 business day")

with st.container(border=True):
    with st.form("contact_form", clear_on_submit=False):
        f1, f2 = st.columns([1, 2])
        with f1:
            user_email = st.text_input("Your e-mail", placeholder="name@company.com")
        with f2:
            user_msg = st.text_area(
                "Your question",
                placeholder="e.g. Could you price a 10-year DASS on 20 MW starting 2027?",
                height=90)
        sent = st.form_submit_button("Send to Nexwell Power ➜", type="primary",
                                     use_container_width=True)

    if sent:
        if "@" not in user_email or "." not in user_email.split("@")[-1]:
            st.error("Please enter a valid e-mail address.")
        elif not user_msg.strip():
            st.error("Please write your question.")
        else:
            ok = False
            try:
                ok = send_contact_email(user_email.strip(), user_msg.strip())
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Direct send failed ({exc}).")
            if ok:
                st.success(f"✅ Sent! Your question is on its way to {CONTACT_TO}. "
                           "We will reply to your e-mail shortly.")
            else:
                # No SMTP configured on the server → hand off to the user's mail client
                subject = urllib.parse.quote(
                    f"[PPA & DASS Settlements] Question from {user_email.strip()}")
                body = urllib.parse.quote(
                    f"From: {user_email.strip()}\n\n{user_msg.strip()}")
                st.info("Direct sending is not configured on this server — click below "
                        "to send it from your own mail client (message pre-filled).")
                st.markdown(
                    f"<a href='mailto:{CONTACT_TO}?subject={subject}&body={body}' "
                    f"target='_blank' style='display:inline-block;background:{GREEN_DARK};"
                    f"color:#fff;font-weight:700;padding:10px 22px;border-radius:10px;"
                    f"text-decoration:none;'>📨 Open e-mail to {CONTACT_TO}</a>",
                    unsafe_allow_html=True)
