from __future__ import annotations

import io
import re
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode
import requests
import pandas as pd
import altair as alt
import streamlit as st

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="Test | Forward Monthly Closing", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OMIP_BASE_URL = "https://www.omip.pt/en/dados-mercado"
LIVE_CURRENT_MONTH_LOOKBACK_DAYS = 10
CACHE_FILES = [
    DATA_DIR / "omip_ES_EL_date_range2025_20260511.xlsx",
    DATA_DIR / "eex_forward_market.xlsx",
    DATA_DIR / "eex_forward_market.csv",
]

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE = "#1D4ED8"
SOLAR_YELLOW = "#F4C542"
TEXT_GREY = "#64748B"

TENORS = ["Y+1", "Y+2", "Q+1", "Q+2", "Q+3"]
TENOR_COLORS = {
    "Y+1": "#1D4ED8",
    "Y+2": "#0F766E",
    "Q+1": "#EA580C",
    "Q+2": "#7C3AED",
    "Q+3": "#DC2626",
}

def section(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
            color: white;
            padding: 14px 18px;
            border-radius: 14px;
            font-weight: 850;
            font-size: 1.35rem;
            margin-top: 8px;
            margin-bottom: 14px;
        ">🧪 {title}</div>
        """,
        unsafe_allow_html=True,
    )

def subsection(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            margin-top: 16px;
            margin-bottom: 8px;
            padding: 8px 12px;
            color: #0F172A;
            background: #F4FCF8;
            border-left: 5px solid {CORP_GREEN};
            font-size: 1.02rem;
            font-weight: 800;
            border-radius: 8px;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )

def chart_style(chart, height: int = 500):
    return (
        chart.properties(height=height)
        .configure_view(stroke="#E5E7EB", fill="white")
        .configure_axis(
            grid=True,
            gridColor="#E5E7EB",
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor="#111827",
            titleColor="#111827",
            labelFontSize=12,
            titleFontSize=13,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=12,
            titleFontSize=12,
            symbolStrokeWidth=3,
        )
    )

def norm(x) -> str:
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).replace("\xa0", " ")).strip()

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [norm(c).lower().replace(" ", "_") for c in out.columns]
    return out

def delivery_label(contract: str) -> str:
    c = norm(contract)
    m = re.search(r"\bYR-(\d{2})", c)
    if m:
        return f"YR{m.group(1)}"
    m = re.search(r"\bQ([1-4])-(\d{2})", c)
    if m:
        return f"Q{m.group(1)}-{m.group(2)}"
    return c

def infer_curve(sheet, instrument, contract) -> str | None:
    s = norm(sheet).lower()
    i = norm(instrument).upper()
    c = norm(contract).upper()
    if "solar" in s or i == "FTS" or c.startswith("FTS "):
        return "Solar"
    if "base" in s or i == "FTB" or c.startswith("FTB "):
        return "Baseload"
    return None


def omip_url(date_str: str, instrument: str) -> str:
    params = {
        "date": date_str,
        "product": "EL",
        "zone": "ES",
        "instrument": instrument,
    }
    return f"{OMIP_BASE_URL}?{urlencode(params)}"


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_tables_live(date_str: str, instrument: str) -> list[pd.DataFrame]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-GB,en;q=0.9,es;q=0.8",
        "Referer": "https://www.omip.pt/",
    }
    r = requests.get(omip_url(date_str, instrument), headers=headers, timeout=60)
    r.raise_for_status()
    return pd.read_html(io.StringIO(r.text))


def parse_float(value) -> float | None:
    s = norm(value)
    if not s or s.lower() in {"nan", "none", "n.a.", "na", "-", ""}:
        return None
    s = s.replace("€", "").replace("/MWh", "").replace("MWh", "").replace(" ", "")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def extract_live_contract_name(raw_text: str, instrument: str) -> str | None:
    text = norm(raw_text)
    m = re.search(rf"({re.escape(instrument)}\s+[A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)?)\s*$", text)
    if m:
        return norm(m.group(1))
    m = re.search(rf"({re.escape(instrument)}\s+.+)$", text)
    if m:
        tail = norm(m.group(1))
        tail = tail.split(" Transparency")[0].strip()
        return tail[:80]
    return None


def flatten_live_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            " | ".join([str(x) for x in tup if str(x) != "nan"]).strip()
            for tup in out.columns
        ]
    out.columns = [str(c).strip() for c in out.columns]
    return out


def parse_live_snapshot(asof: pd.Timestamp) -> pd.DataFrame:
    parts = []
    for curve, instrument in [("Baseload", "FTB"), ("Solar", "FTS")]:
        try:
            tables = fetch_tables_live(asof.strftime("%Y-%m-%d"), instrument)
        except Exception:
            tables = []
        rows = []
        for table in tables:
            df = flatten_live_columns(table)
            if df.empty:
                continue
            for _, row in df.iterrows():
                vals = list(row.values)
                texts = [norm(v) for v in vals]
                contract = None
                for txt in texts:
                    maybe = extract_live_contract_name(txt, instrument)
                    if maybe:
                        contract = maybe
                        break
                if not contract:
                    continue
                def val_at(pos: int):
                    return vals[pos] if pos < len(vals) else None
                d_price = parse_float(val_at(15))
                last_price = parse_float(val_at(7))
                best_bid = parse_float(val_at(3))
                best_ask = parse_float(val_at(4))
                d_minus_1 = parse_float(val_at(16))
                price = d_price
                if price is None:
                    price = last_price
                if price is None and best_bid is not None and best_ask is not None:
                    price = (best_bid + best_ask) / 2.0
                if price is None:
                    price = d_minus_1
                if price is None:
                    continue
                rows.append(
                    {
                        "market_date": asof.normalize(),
                        "sheet": curve,
                        "instrument": instrument,
                        "contract": contract,
                        "delivery_label": delivery_label(contract),
                        "curve": curve,
                        "price": price,
                        "curve_price": price,
                        "d_price": d_price,
                        "source": "live_omip",
                    }
                )
        if rows:
            parts.append(pd.DataFrame(rows))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def latest_live_snapshot_in_current_month(today_ts: pd.Timestamp) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    current_month = today_ts.to_period("M")
    for offset in range(0, LIVE_CURRENT_MONTH_LOOKBACK_DAYS + 1):
        candidate = (today_ts - pd.Timedelta(days=offset)).normalize()
        if candidate.to_period("M") != current_month:
            break
        snap = parse_live_snapshot(candidate)
        if not snap.empty:
            return snap, candidate
    return pd.DataFrame(), None


def enrich_cache_with_live_current_month(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    if df.empty:
        return df, "No live enrichment"
    today_ts = pd.Timestamp(date.today())
    cache_latest = pd.Timestamp(df["market_date"].max()).normalize()
    # Only refresh the current quotation month if the cache is stale versus today.
    if cache_latest.to_period("M") != today_ts.to_period("M") or cache_latest >= today_ts:
        return df, "Current month already covered by cache"
    live_snap, live_date = latest_live_snapshot_in_current_month(today_ts)
    if live_snap.empty or live_date is None:
        return df, "No live OMIP current-month snapshot found"
    keep = df[df["market_date"] != live_date].copy()
    out = pd.concat([keep, live_snap], ignore_index=True)
    return out, f"Live OMIP current-month snapshot added: {live_date.strftime('%Y-%m-%d')}"


@st.cache_data(show_spinner=False)
def load_cache() -> tuple[pd.DataFrame, str]:
    for path in CACHE_FILES:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".csv":
                raw = pd.read_csv(path)
            else:
                xls = pd.ExcelFile(path)
                sheet = "All curves" if "All curves" in xls.sheet_names else xls.sheet_names[0]
                raw = pd.read_excel(xls, sheet_name=sheet)
        except Exception:
            continue

        df = clean_cols(raw)
        rename = {
            "date": "market_date",
            "market_date": "market_date",
            "curve": "sheet",
            "sheet": "sheet",
            "instrument": "instrument",
            "contract_name": "contract",
            "contract": "contract",
            "delivery_label": "delivery_label",
            "curve_price": "curve_price",
            "d_price": "d_price",
            "d": "d_price",
        }
        df = df.rename(columns={c: rename[c] for c in df.columns if c in rename})

        if "market_date" not in df.columns or "contract" not in df.columns:
            continue
        for col in ["sheet", "instrument", "delivery_label", "curve_price", "d_price"]:
            if col not in df.columns:
                df[col] = None

        df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce").dt.normalize()
        df["contract"] = df["contract"].astype(str).map(norm)
        df["sheet"] = df["sheet"].astype(str).map(norm)
        df["instrument"] = df["instrument"].astype(str).map(norm)
        df["curve_price"] = pd.to_numeric(df["curve_price"], errors="coerce")
        df["d_price"] = pd.to_numeric(df["d_price"], errors="coerce")
        df["price"] = df["curve_price"].combine_first(df["d_price"])
        df["delivery_label"] = df["delivery_label"].astype(str).map(norm)
        needs = df["delivery_label"].isin(["", "nan", "none", "None"])
        df.loc[needs, "delivery_label"] = df.loc[needs, "contract"].map(delivery_label)
        df["curve"] = df.apply(lambda r: infer_curve(r["sheet"], r["instrument"], r["contract"]), axis=1)
        df = df.dropna(subset=["market_date", "price", "curve"])
        df = df[df["curve"].isin(["Baseload", "Solar"])]
        df = df[df["contract"].str.contains(r"^(?:FTB|FTS)\s+", regex=True, na=False)]
        if not df.empty:
            enriched, live_msg = enrich_cache_with_live_current_month(df.reset_index(drop=True))
            return enriched.reset_index(drop=True), f"{path.name} | {live_msg}"
    return pd.DataFrame(), "No cache file found"

def relative_labels(asof: pd.Timestamp) -> dict[str, str]:
    current_q = ((asof.month - 1) // 3) + 1
    labels = {
        "Y+1": f"YR{(asof.year + 1) % 100:02d}",
        "Y+2": f"YR{(asof.year + 2) % 100:02d}",
    }
    q = current_q
    y = asof.year
    for n in range(1, 4):
        q += 1
        if q > 4:
            q = 1
            y += 1
        labels[f"Q+{n}"] = f"Q{q}-{y % 100:02d}"
    return labels

def monthly_last_quote_dates(df: pd.DataFrame, months: int) -> pd.DataFrame:
    latest_month = df["market_date"].max().to_period("M")
    periods = pd.period_range(end=latest_month, periods=months, freq="M")
    work = df.copy()
    work["month"] = work["market_date"].dt.to_period("M")
    rows = []
    for period in periods:
        temp = work[work["month"] == period]
        rows.append(
            {
                "quote_month": period.to_timestamp(),
                "market_date": temp["market_date"].max() if not temp.empty else pd.NaT,
            }
        )
    return pd.DataFrame(rows)

def build_evolution(df: pd.DataFrame, months: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build a same-contract historical backtrace.

    The latest monthly close fixes the delivery mapping:
        Y+1, Y+2, Q+1, Q+2, Q+3
    The chart then tracks those exact delivery contracts backwards through the
    last close of each prior month.

    If the historical cache does not contain a required Q/Y contract for one of
    those monthly close dates, the page performs a cached OMIP live pull for that
    specific historical market date and fills the missing point when OMIP returns it.
    """
    quotes = monthly_last_quote_dates(df, months)
    valid_quotes = quotes.dropna(subset=["market_date"]).copy()
    if valid_quotes.empty:
        return pd.DataFrame(), pd.DataFrame()

    latest_market_date = pd.to_datetime(valid_quotes["market_date"].max(), errors="coerce")
    if pd.isna(latest_market_date):
        return pd.DataFrame(), pd.DataFrame()

    fixed_labels = relative_labels(latest_market_date)

    rows = []
    diagnostics = []
    for _, qrow in quotes.iterrows():
        quote_month = pd.Timestamp(qrow["quote_month"])
        market_date = pd.to_datetime(qrow["market_date"], errors="coerce")
        if pd.isna(market_date):
            diagnostics.append({
                "Quote month": quote_month.strftime("%b-%Y"),
                "Last market date": "—",
                "Status": "No data",
                "Historical OMIP backfill": "Not attempted",
                "Y+1": fixed_labels["Y+1"],
                "Y+2": fixed_labels["Y+2"],
                "Q+1": fixed_labels["Q+1"],
                "Q+2": fixed_labels["Q+2"],
                "Q+3": fixed_labels["Q+3"],
            })
            continue

        snap = df[df["market_date"] == market_date].copy()
        required_pairs = {(curve, label) for label in fixed_labels.values() for curve in ["Baseload", "Solar"]}
        existing_pairs = set(
            zip(
                snap["curve"].astype(str),
                snap["delivery_label"].astype(str),
            )
        ) if not snap.empty else set()
        missing_pairs_before = required_pairs - existing_pairs

        backfill_status = "Cache complete"
        if missing_pairs_before:
            live_snap = parse_live_snapshot(market_date)
            if not live_snap.empty:
                snap = pd.concat([snap, live_snap], ignore_index=True)
                snap = snap.drop_duplicates(subset=["curve", "delivery_label", "contract"], keep="last")
                existing_pairs_after = set(
                    zip(
                        snap["curve"].astype(str),
                        snap["delivery_label"].astype(str),
                    )
                )
                missing_pairs_after = required_pairs - existing_pairs_after
                filled_count = len(missing_pairs_before) - len(missing_pairs_after)
                backfill_status = f"OMIP date pull filled {filled_count}/{len(missing_pairs_before)} missing points"
            else:
                backfill_status = "OMIP date pull returned no usable rows"

        diagnostics.append({
            "Quote month": quote_month.strftime("%b-%Y"),
            "Last market date": market_date.strftime("%Y-%m-%d"),
            "Status": "Loaded",
            "Historical OMIP backfill": backfill_status,
            "Y+1": fixed_labels["Y+1"],
            "Y+2": fixed_labels["Y+2"],
            "Q+1": fixed_labels["Q+1"],
            "Q+2": fixed_labels["Q+2"],
            "Q+3": fixed_labels["Q+3"],
        })

        for tenor, label in fixed_labels.items():
            for curve in ["Baseload", "Solar"]:
                selected = snap[
                    (snap["curve"] == curve)
                    & (snap["delivery_label"].astype(str) == label)
                ].copy()
                if selected.empty:
                    rows.append(
                        {
                            "quote_month": quote_month,
                            "quote_month_label": quote_month.strftime("%b-%Y"),
                            "market_date": market_date,
                            "curve": curve,
                            "tenor": tenor,
                            "delivery_label": label,
                            "contract": None,
                            "price": None,
                        }
                    )
                    continue

                row = selected.iloc[0]
                rows.append(
                    {
                        "quote_month": quote_month,
                        "quote_month_label": quote_month.strftime("%b-%Y"),
                        "market_date": market_date,
                        "curve": curve,
                        "tenor": tenor,
                        "delivery_label": label,
                        "contract": row["contract"],
                        "price": float(row["price"]),
                    }
                )

    return pd.DataFrame(rows), pd.DataFrame(diagnostics)

def build_chart(evolution: pd.DataFrame):
    plot = evolution.dropna(subset=["price"]).copy()
    if plot.empty:
        return None
    plot["series"] = plot["curve"] + " | " + plot["tenor"]
    order = (
        plot[["quote_month", "quote_month_label"]]
        .drop_duplicates()
        .sort_values("quote_month")["quote_month_label"]
        .tolist()
    )
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X(
            "quote_month_label:N",
            sort=order,
            title="Quote month — last available market date in that month",
            axis=alt.Axis(labelAngle=-30),
        ),
        y=alt.Y("price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "tenor:N",
            title="Relative delivery",
            sort=TENORS,
            scale=alt.Scale(domain=TENORS, range=[TENOR_COLORS[t] for t in TENORS]),
        ),
        strokeDash=alt.StrokeDash(
            "curve:N",
            title="Curve",
            sort=["Baseload", "Solar"],
            scale=alt.Scale(domain=["Baseload", "Solar"], range=[[1, 0], [7, 3]]),
        ),
        shape=alt.Shape(
            "curve:N",
            title="Curve",
            sort=["Baseload", "Solar"],
            scale=alt.Scale(domain=["Baseload", "Solar"], range=["circle", "diamond"]),
        ),
        detail="series:N",
        tooltip=[
            alt.Tooltip("quote_month_label:N", title="Quote month"),
            alt.Tooltip("market_date:T", title="Last market date", format="%Y-%m-%d"),
            alt.Tooltip("curve:N", title="Curve"),
            alt.Tooltip("tenor:N", title="Relative delivery"),
            alt.Tooltip("delivery_label:N", title="Delivery contract"),
            alt.Tooltip("contract:N", title="OMIP contract"),
            alt.Tooltip("price:Q", title="Price €/MWh", format=",.2f"),
        ],
    ).properties(title="Last monthly OMIP quote evolution | Same delivery contract backtrace from latest monthly close")
    return chart_style(chart, height=520)

def latest_table(evolution: pd.DataFrame):
    if evolution.empty:
        return pd.DataFrame()
    latest_month = evolution["quote_month"].max()
    out = evolution[evolution["quote_month"] == latest_month].copy()
    out["Market date"] = pd.to_datetime(out["market_date"]).dt.strftime("%Y-%m-%d")
    out = out.rename(
        columns={
            "curve": "Curve",
            "tenor": "Relative delivery",
            "delivery_label": "Delivery label",
            "contract": "OMIP contract",
            "price": "Price €/MWh",
        }
    )
    return out[["Relative delivery", "Curve", "Delivery label", "OMIP contract", "Market date", "Price €/MWh"]]


def latest_relative_delivery_mapping(evolution: pd.DataFrame) -> pd.DataFrame:
    if evolution.empty:
        return pd.DataFrame()
    latest_month = evolution["quote_month"].max()
    snap = evolution[evolution["quote_month"] == latest_month].copy()
    cols = snap[["tenor", "delivery_label"]].drop_duplicates().rename(
        columns={"tenor": "Relative label", "delivery_label": "Actual delivery contract"}
    )
    return cols.sort_values("Relative label").reset_index(drop=True)


def style_table(df: pd.DataFrame):
    if df.empty:
        return df
    def row_style(row):
        if row["Curve"] == "Baseload":
            return ["background-color: #EEF5FF;"] * len(row)
        if row["Curve"] == "Solar":
            return ["background-color: #FFF7CC;"] * len(row)
        return [""] * len(row)
    return (
        df.style
        .format({"Price €/MWh": "{:,.2f}"})
        .apply(row_style, axis=1)
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#334155"), ("color", "white"), ("font-weight", "800")]},
                {"selector": "td", "props": [("padding", "7px 8px")]},
            ]
        )
    )

# =========================================================
# PAGE
# =========================================================
section("Forward monthly closing evolution")
st.caption(
    "Prototype for the Monthly Report forward block. "
    "Each monthly point uses the last available OMIP market date in that calendar month. "
    "Y+1, Y+2, Q+1, Q+2 and Q+3 are fixed using the latest monthly close, then traced backwards as the same delivery contracts."
)

cache, source_name = load_cache()
if cache.empty:
    st.error("No usable OMIP cache found in `/data`.")
    st.stop()

c1, c2 = st.columns([1, 2])
with c1:
    months = st.slider("Months to display", min_value=6, max_value=18, value=10, step=1)
with c2:
    st.info(f"Forward source: `{source_name}`")

evolution, diagnostics = build_evolution(cache, months)

mapping_df = latest_relative_delivery_mapping(evolution)
if not mapping_df.empty:
    latest_quote_date = pd.to_datetime(evolution["market_date"].max()).strftime("%Y-%m-%d")
    st.caption(
        f"Fixed delivery mapping from the latest monthly close ({latest_quote_date}): "
        + ", ".join(
            f"{row['Relative label']} = {row['Actual delivery contract']}"
            for _, row in mapping_df.iterrows()
        )
    )

m1, m2, m3 = st.columns(3)
m1.metric("Quote months loaded", int((diagnostics["Status"] == "Loaded").sum()) if not diagnostics.empty else 0)
m2.metric("Price points plotted", int(evolution["price"].notna().sum()) if not evolution.empty else 0)
m3.metric("Missing contract-month cells", int(evolution["price"].isna().sum()) if not evolution.empty else 0)

subsection("Evolution chart")
chart = build_chart(evolution)
if chart is not None:
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("No matching Y+1/Y+2/Q+1/Q+2/Q+3 quotations could be plotted.")

subsection("Latest monthly closing snapshot")
table = latest_table(evolution)
if not table.empty:
    st.dataframe(style_table(table), use_container_width=True, hide_index=True)
else:
    st.info("No latest snapshot available.")

with st.expander("Quote-date diagnostics"):
    st.dataframe(diagnostics, use_container_width=True, hide_index=True)

with st.expander("Underlying chart points"):
    show = evolution.copy()
    if not show.empty:
        show["quote_month"] = pd.to_datetime(show["quote_month"]).dt.strftime("%b-%Y")
        show["market_date"] = pd.to_datetime(show["market_date"]).dt.strftime("%Y-%m-%d")
    st.dataframe(show, use_container_width=True, hide_index=True)

st.caption(
    "The x-axis is the monthly quotation snapshot, not the delivery month. "
    "Delivery labels are frozen at the latest monthly close: e.g. if Q+1 = Q3-26 in May-2026, the line shows how Q3-26 quoted at the last close of Apr-2026, Mar-2026, etc. "
    "Where the local historical cache is missing those quarter contracts, the test page now attempts a cached OMIP pull for that historical month-end date."
)
