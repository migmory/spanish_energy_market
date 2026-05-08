from __future__ import annotations

import re
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="Forward Market", layout="wide")
st.title("Forward Market - OMIP")

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_DARK = "#D97706"
GREY_SHADE = "#F3F4F6"
OMIP_BASE_URL = "https://www.omip.pt/en/dados-mercado"

PRODUCTS = {
    "Power": "EL",
    "Natural Gas": "GN",
}

ZONES = {
    "Spain": "ES",
    "Portugal": "PT",
    "France": "FR",
    "Germany": "DE",
    "Spain-Portugal": "ESPT",
    "Portugal-Spain": "PTES",
}

# Codes observed/used by OMIP query string. Labels are user-facing.
INSTRUMENTS = {
    "SPEL Base Futures": "FTB",
    "SPEL Peak Futures": "FTP",
    "SPEL Base Forwards": "FWB",
    "SPEL Base Swaps": "SWB",
    "SPEL Solar Futures": "FTS",
}

MATURITIES = {
    "All": "ALL",
    "Day": "DAY",
    "Weekend": "WE",
    "Week": "WK",
    "Month": "MTH",
    "Quarter": "QTR",
    "Year": "YR",
    "PPA": "PPA",
}

TABLE_HEADER_FONT_PCT = "135%"
TABLE_BODY_FONT_PCT = "108%"


def section_header(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
            color: white;
            padding: 12px 18px;
            border-radius: 12px;
            font-weight: 800;
            font-size: 1.25rem;
            margin-top: 14px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def styled_df(df: pd.DataFrame):
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    fmt = {c: "{:,.2f}" for c in numeric_cols}
    return (
        df.style.format(fmt)
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#4B5563"),
                        ("color", "white"),
                        ("font-weight", "bold"),
                        ("font-size", TABLE_HEADER_FONT_PCT),
                        ("text-align", "center"),
                        ("padding", "10px 8px"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("font-size", TABLE_BODY_FONT_PCT),
                        ("padding", "6px 8px"),
                    ],
                },
            ]
        )
    )


def apply_common_chart_style(chart, height: int = 360):
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
            titleFontSize=14,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=12,
            titleFontSize=13,
            symbolStrokeWidth=3,
        )
    )


def parse_num(value) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"n.a.", "na", "nan", "none", "-"}:
        return None
    s = s.replace("€", "").replace("/MWh", "").replace(" ", "")
    # OMIP pages normally use a decimal dot, but support comma too.
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def contract_sort_key(contract: str) -> int:
    """YR-27 -> 2027, Q1-27 -> 202701, etc. Best effort."""
    if not isinstance(contract, str):
        return 999999
    m = re.search(r"YR-(\d{2})", contract)
    if m:
        return 2000 + int(m.group(1))
    m = re.search(r"Q([1-4])-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 10 + int(m.group(1))
    m = re.search(r"M(\d{1,2})-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 100 + int(m.group(1))
    m = re.search(r"WK(\d{1,2})-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 100 + int(m.group(1))
    m = re.search(r"(\d{2})$", contract)
    if m:
        return 2000 + int(m.group(1))
    return 999999


def build_omip_url(asof: date, product: str, zone: str, instrument: str, maturity: str) -> str:
    params = {
        "date": asof.isoformat(),
        "product": product,
        "zone": zone,
        "instrument": instrument,
        "maturity": maturity,
    }
    return f"{OMIP_BASE_URL}?{urlencode(params)}"


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_omip_html(asof: date, product: str, zone: str, instrument: str, maturity: str) -> tuple[str, str]:
    url = build_omip_url(asof, product, zone, instrument, maturity)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Streamlit OMIP forward dashboard)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resp = requests.get(url, headers=headers, timeout=(15, 60))
    resp.raise_for_status()
    return resp.text, url


def parse_omip_with_read_html(html: str) -> pd.DataFrame:
    try:
        tables = pd.read_html(html)
    except Exception:
        return pd.DataFrame()
    if not tables:
        return pd.DataFrame()

    # Choose the table that looks like the OMIP session table.
    for table in tables:
        flat_cols = [str(c).strip() for c in table.columns]
        lower_cols = " ".join(flat_cols).lower()
        if "contract" in lower_cols and ("d-1" in lower_cols or "reference" in lower_cols or "best bid" in lower_cols):
            df = table.copy()
            df.columns = [str(c).strip() for c in df.columns]
            return df
    return tables[0].copy()


def parse_omip_text_fallback(html: str, instrument_code: str) -> pd.DataFrame:
    """Fallback parser for the OMIP text layout returned by the public page.

    The page can be read as lines like:
    FTS YR-27 n.a. n.a. 0 n.a. n.a. n.a. 81 0 0 29.52 30.00
    """
    text = re.sub(r"<[^>]+>", "\n", html)
    text = re.sub(r"&nbsp;", " ", text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    rows: list[dict] = []
    pat = re.compile(rf"^({re.escape(instrument_code)})\s+([A-Z0-9\-]+)\s+(.*)$")
    for line in lines:
        m = pat.match(line)
        if not m:
            continue
        instr, contract_tail, rest = m.groups()
        contract = f"{instr} {contract_tail}"
        toks = rest.split()
        # Expected values after contract:
        # best_bid, best_ask, session_volume, last_price, last_time, last_volume,
        # open_interest, nr_contracts, otc_volume, D, D-1
        padded = toks + [None] * max(0, 11 - len(toks))
        row = {
            "Contract name": contract,
            "Best bid (€/MWh)": parse_num(padded[0]),
            "Best Ask (€/MWh)": parse_num(padded[1]),
            "Session volume (MWh)": parse_num(padded[2]),
            "Last price (€/MWh)": parse_num(padded[3]),
            "Last time": padded[4],
            "Last volume (MWh)": parse_num(padded[5]),
            "Open Interest": parse_num(padded[6]),
            "Nr of Contracts": parse_num(padded[7]),
            "OTC volume (MWh)": parse_num(padded[8]),
            "D (€/MWh)": parse_num(padded[9]),
            "D-1 (€/MWh)": parse_num(padded[10]),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def normalise_omip_df(raw: pd.DataFrame, html: str, instrument_code: str) -> pd.DataFrame:
    if raw.empty:
        raw = parse_omip_text_fallback(html, instrument_code)
    if raw.empty:
        return pd.DataFrame(columns=["contract", "reference_price", "d_minus_1", "best_bid", "best_ask", "last_price", "open_interest"])

    df = raw.copy()
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]

    def find_col(options: Iterable[str]) -> str | None:
        opts = [o.lower() for o in options]
        for col in df.columns:
            c = col.lower()
            if any(o in c for o in opts):
                return col
        return None

    contract_col = find_col(["contract name", "contract"])
    bid_col = find_col(["best bid"])
    ask_col = find_col(["best ask", "best offer"])
    last_col = find_col(["last price", "price (€/mwh)", "price"])
    d_col = None
    d1_col = None
    for col in df.columns:
        low = col.lower().strip()
        if low == "d (€/mwh)" or low == "d" or low.startswith("d "):
            d_col = col
        if low == "d-1 (€/mwh)" or low == "d-1" or "d-1" in low:
            d1_col = col
    oi_col = find_col(["open interest"])

    out = pd.DataFrame()
    out["contract"] = df[contract_col].astype(str) if contract_col else pd.Series(dtype=str)
    out["best_bid"] = df[bid_col].map(parse_num) if bid_col else None
    out["best_ask"] = df[ask_col].map(parse_num) if ask_col else None
    out["last_price"] = df[last_col].map(parse_num) if last_col else None
    out["reference_price"] = df[d_col].map(parse_num) if d_col else None
    out["d_minus_1"] = df[d1_col].map(parse_num) if d1_col else None
    out["open_interest"] = df[oi_col].map(parse_num) if oi_col else None

    out = out[out["contract"].str.contains(instrument_code, na=False)].copy()
    if out.empty and len(raw) > 0:
        # Some read_html outputs may omit the instrument in contract names.
        out = out.copy()
    out["sort_key"] = out["contract"].map(contract_sort_key)
    out["curve_price"] = out["reference_price"].combine_first(out["last_price"]).combine_first(
        (out["best_bid"] + out["best_ask"]) / 2
    )
    out = out.dropna(subset=["curve_price"], how="all").sort_values("sort_key").reset_index(drop=True)
    return out


def build_forward_curve_chart(df: pd.DataFrame, title: str):
    if df.empty:
        return None
    plot = df.copy()
    order = plot["contract"].tolist()
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
        x=alt.X("contract:N", sort=order, axis=alt.Axis(title=None, labelAngle=-35)),
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
            alt.Tooltip("reference_price:Q", title="D €/MWh", format=",.2f"),
            alt.Tooltip("d_minus_1:Q", title="D-1 €/MWh", format=",.2f"),
            alt.Tooltip("best_bid:Q", title="Best bid €/MWh", format=",.2f"),
            alt.Tooltip("best_ask:Q", title="Best ask €/MWh", format=",.2f"),
            alt.Tooltip("open_interest:Q", title="Open interest", format=",.0f"),
        ],
    ).properties(title=title, height=420)
    return apply_common_chart_style(chart, height=420)


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="omip_forward_curve")
    return output.getvalue()


section_header("OMIP live forward curve")
st.caption("Pulls the public OMIP market-results page and parses the displayed contracts. If the website layout changes, use the manual upload fallback below.")

left, mid, right = st.columns([1, 1, 1])
with left:
    asof = st.date_input("Market date", value=date(2026, 2, 3))
    product_label = st.selectbox("Product", list(PRODUCTS.keys()), index=0)
with mid:
    zone_label = st.selectbox("Zone", list(ZONES.keys()), index=0)
    instrument_label = st.selectbox("Instrument", list(INSTRUMENTS.keys()), index=list(INSTRUMENTS.keys()).index("SPEL Solar Futures"))
with right:
    maturity_label = st.selectbox("Maturity", list(MATURITIES.keys()), index=list(MATURITIES.keys()).index("Year"))
    pull = st.button("Pull OMIP prices", type="primary")

product = PRODUCTS[product_label]
zone = ZONES[zone_label]
instrument = INSTRUMENTS[instrument_label]
maturity = MATURITIES[maturity_label]
omip_url = build_omip_url(asof, product, zone, instrument, maturity)
st.markdown(f"[Open OMIP page]({omip_url})")

if pull:
    try:
        html, fetched_url = fetch_omip_html(asof, product, zone, instrument, maturity)
        raw = parse_omip_with_read_html(html)
        curve = normalise_omip_df(raw, html, instrument)
        if curve.empty:
            st.warning("OMIP page was reachable, but no curve rows could be parsed. Try a different instrument/maturity or use manual upload below.")
            with st.expander("Debug: first 4,000 characters of page text"):
                clean_text = re.sub(r"<[^>]+>", "\n", html)
                st.text(clean_text[:4000])
        else:
            st.success(f"Loaded {len(curve)} OMIP contracts from public page.")
            chart = build_forward_curve_chart(curve, f"OMIP {zone_label} {instrument_label} - {maturity_label} | {asof.isoformat()}")
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            st.dataframe(styled_df(curve.drop(columns=["sort_key"], errors="ignore")), use_container_width=True)
            st.download_button(
                "Download parsed OMIP curve as Excel",
                data=dataframe_to_excel_bytes(curve.drop(columns=["sort_key"], errors="ignore")),
                file_name=f"omip_{zone}_{instrument}_{maturity}_{asof.isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    except Exception as exc:
        st.error(f"Could not fetch/parse OMIP page: {exc}")
        st.info("The OMIP public page can still be opened in the browser from the link above. You can export/copy the table and upload it below.")

section_header("Manual upload fallback")
file = st.file_uploader("Upload OMIP CSV/XLSX export or copied table", type=["csv", "xlsx", "xls"])
if file is not None:
    try:
        if file.name.lower().endswith(".csv"):
            raw_upload = pd.read_csv(file)
        else:
            raw_upload = pd.read_excel(file)
        curve_upload = normalise_omip_df(raw_upload, "", instrument)
        if curve_upload.empty:
            st.warning("Could not identify OMIP curve columns in the uploaded file.")
            st.dataframe(raw_upload, use_container_width=True)
        else:
            chart = build_forward_curve_chart(curve_upload, f"Uploaded OMIP curve - {instrument_label} {maturity_label}")
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            st.dataframe(styled_df(curve_upload.drop(columns=["sort_key"], errors="ignore")), use_container_width=True)
    except Exception as exc:
        st.error(f"Could not parse upload: {exc}")
