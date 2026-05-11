from __future__ import annotations

import io
import re
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(page_title="Forward Market", layout="wide")
st.title("Forward Market - OMIP")

# =========================================================
# CONSTANTS / STYLE
# =========================================================
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_DARK = "#D97706"
GREY = "#6B7280"
RED = "#DC2626"

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

INSTRUMENTS = {
    "SPEL Base Futures": "FTB",
    "SPEL Peak Futures": "FTP",
    "SPEL Base Forwards": "FWB",
    "SPEL Base Swaps": "SWB",
    "SPEL Solar Futures": "FTS",
}

MATURITY_FILTERS = {
    "All": None,
    "Day": "DAY",
    "Weekend": "WE",
    "Week": "WK",
    "Month": "MTH",
    "Quarter": "QTR",
    "Year": "YR",
    "PPA": "PPA",
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-GB,en;q=0.9,es;q=0.8",
    "Referer": "https://www.omip.pt/",
}

WS_RE = re.compile(r"\s+", re.UNICODE)

TABLE_HEADER_FONT_PCT = "135%"
TABLE_BODY_FONT_PCT = "108%"


# =========================================================
# UI HELPERS
# =========================================================
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


def apply_common_chart_style(chart, height: int = 380):
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


# =========================================================
# OMIP PARSER - based on the working scripts uploaded
# =========================================================
def normalize_text_series(s: pd.Series) -> pd.Series:
    """Normalize NBSP/hidden spaces so 'FTS YR-27' matches reliably."""
    s = s.astype(str)
    s = s.str.replace("\xa0", " ", regex=False)
    s = s.str.replace("\u202f", " ", regex=False)
    s = s.str.replace("\ufeff", "", regex=False)
    return s.apply(lambda x: WS_RE.sub(" ", x).strip())


def normalize_text_value(value) -> str:
    if pd.isna(value):
        return ""
    return WS_RE.sub(" ", str(value).replace("\xa0", " ").replace("\u202f", " ").replace("\ufeff", "")).strip()


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " | ".join([str(x) for x in tup if str(x) != "nan" and not str(x).startswith("Unnamed")]).strip()
            for tup in df.columns
        ]
    df.columns = [normalize_text_value(c) for c in df.columns]
    return df


def pick_contract_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "contract name" in str(c).lower():
            return c
    for c in df.columns:
        if "contract" in str(c).lower():
            return c
    return df.columns[0]


def parse_num(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    s = normalize_text_value(value)
    if not s or s.lower() in {"n.a.", "n.a", "na", "nan", "none", "-", "--"}:
        return None
    s = s.replace("€", "").replace("/MWh", "").replace("MWh", "").replace("%", "")
    s = s.replace(" ", "")
    # Support both decimal comma and decimal dot.
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def build_omip_url(asof: date, product: str, zone: str, instrument: str, maturity: str | None = None) -> str:
    # IMPORTANT: the working scripts fetch without the maturity parameter.
    # We keep maturity optional and mostly use it as a post-filter because the public page
    # is more reliable when queried with date/product/zone/instrument only.
    params = {
        "date": asof.isoformat(),
        "product": product,
        "zone": zone,
        "instrument": instrument,
    }
    if maturity and maturity != "ALL":
        params["maturity"] = maturity
    return f"{OMIP_BASE_URL}?{urlencode(params)}"


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_omip_tables(asof: date, product: str, zone: str, instrument: str, include_maturity_param: bool, maturity_code: str | None) -> tuple[list[pd.DataFrame], str, str]:
    """Fetch OMIP tables using the robust method from the working scripts.

    The key details are:
    - requests.get with browser-like headers;
    - pd.read_html(io.StringIO(html));
    - query usually without maturity; maturity is then filtered locally.
    """
    maturity_for_url = maturity_code if include_maturity_param else None
    url = build_omip_url(asof, product, zone, instrument, maturity_for_url)
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=60)
    response.raise_for_status()
    html = response.text
    tables = pd.read_html(io.StringIO(html))
    return tables, html, url


def extract_contract_rows_from_table(df: pd.DataFrame, instrument: str) -> pd.DataFrame:
    df = flatten_columns(df)
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    if df.empty:
        return pd.DataFrame()

    contract_col = pick_contract_col(df)
    contract_series = normalize_text_series(df[contract_col])
    pattern = rf"^{re.escape(instrument)}\s"
    mask = contract_series.str.contains(pattern, regex=True, na=False)

    out = df.loc[mask].copy()
    if out.empty:
        return pd.DataFrame()

    out = out.rename(columns={contract_col: "contract_name"})
    out["contract_name"] = normalize_text_series(out["contract_name"])
    return out


def parse_omip_text_fallback(html: str, instrument: str) -> pd.DataFrame:
    """Fallback for OMIP pages where read_html produces difficult tables.

    It looks for text lines starting with FTB/FTS/etc., for example:
    FTS YR-27 n.a. n.a. 0 n.a. n.a. n.a. 81 0 0 29.52 30.00
    """
    text = re.sub(r"<[^>]+>", "\n", html)
    text = text.replace("&nbsp;", " ").replace("\xa0", " ")
    lines = [WS_RE.sub(" ", line).strip() for line in text.splitlines()]
    rows: list[dict] = []
    pat = re.compile(rf"^({re.escape(instrument)})\s+([A-Z0-9\-]+)\s+(.*)$")
    for line in lines:
        m = pat.match(line)
        if not m:
            continue
        instr, tail, rest = m.groups()
        toks = rest.split()
        padded = toks + [None] * max(0, 11 - len(toks))
        rows.append(
            {
                "contract_name": f"{instr} {tail}",
                "Best bid": padded[0],
                "Best Ask": padded[1],
                "Session volume": padded[2],
                "Last price": padded[3],
                "Last time": padded[4],
                "Last volume": padded[5],
                "Open Interest": padded[6],
                "Nr of Contracts": padded[7],
                "OTC volume": padded[8],
                "D": padded[9],
                "D-1": padded[10],
            }
        )
    return pd.DataFrame(rows)


def extract_contract_rows(tables: list[pd.DataFrame], html: str, instrument: str) -> pd.DataFrame:
    parts = []
    for table in tables:
        rows = extract_contract_rows_from_table(table, instrument)
        if not rows.empty:
            parts.append(rows)

    if parts:
        return pd.concat(parts, ignore_index=True)

    return parse_omip_text_fallback(html, instrument)


def find_col(df: pd.DataFrame, exact: list[str] | None = None, contains: list[str] | None = None) -> str | None:
    exact = [x.lower() for x in (exact or [])]
    contains = [x.lower() for x in (contains or [])]
    for col in df.columns:
        low = str(col).lower().strip()
        if low in exact:
            return col
    for col in df.columns:
        low = str(col).lower().strip()
        if any(x in low for x in contains):
            return col
    return None


def contract_sort_key(contract: str) -> int:
    if not isinstance(contract, str):
        return 999999999
    m = re.search(r"YR-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(1))) * 10000
    m = re.search(r"Q([1-4])-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1)) * 100
    m = re.search(r"M(\d{1,2})-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1))
    m = re.search(r"WK(\d{1,2})-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1))
    m = re.search(r"DAY(\d{1,2})-(\d{2})", contract)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1))
    m = re.search(r"(\d{2})$", contract)
    if m:
        return (2000 + int(m.group(1))) * 10000
    return 999999999


def get_maturity_from_contract(contract: str) -> str:
    if not isinstance(contract, str):
        return "Other"
    contract = contract.upper()
    if " YR-" in contract:
        return "YR"
    if " Q" in contract and re.search(r"Q[1-4]-\d{2}", contract):
        return "QTR"
    if " M" in contract and re.search(r"M\d{1,2}-\d{2}", contract):
        return "MTH"
    if " WK" in contract:
        return "WK"
    if " WE" in contract:
        return "WE"
    if " DAY" in contract:
        return "DAY"
    if " PPA" in contract:
        return "PPA"
    return "Other"


def normalise_omip_contracts(raw: pd.DataFrame, asof: date, sheet_name: str, instrument: str, maturity_filter: str | None) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(
            columns=[
                "date", "sheet", "instrument", "contract", "maturity", "curve_price", "D", "D-1", "best_bid", "best_ask",
                "last_price", "session_volume_mwh", "open_interest", "nr_contracts", "otc_volume_mwh", "sort_key"
            ]
        )

    df = flatten_columns(raw)
    if "contract_name" not in df.columns:
        ccol = pick_contract_col(df)
        df = df.rename(columns={ccol: "contract_name"})
    df["contract_name"] = normalize_text_series(df["contract_name"])

    best_bid_col = find_col(df, contains=["best bid"])
    best_ask_col = find_col(df, contains=["best ask", "best offer"])
    last_price_col = find_col(df, contains=["last price"])
    session_volume_col = find_col(df, contains=["session volume"])
    last_volume_col = find_col(df, contains=["last volume"])
    open_interest_col = find_col(df, contains=["open interest"])
    nr_contracts_col = find_col(df, contains=["nr of contracts", "number of contracts"])
    otc_volume_col = find_col(df, contains=["otc volume"])

    d_col = None
    d1_col = None
    for col in df.columns:
        low = str(col).lower().strip()
        # flattened MultiIndex often includes 'Settlement prices | D'.
        low_clean = low.replace("€", "").replace("/mwh", "")
        parts = [p.strip() for p in low_clean.split("|")]
        if low_clean == "d" or parts[-1] == "d" or low.startswith("d "):
            d_col = col
        if low_clean == "d-1" or parts[-1] == "d-1" or "d-1" in low:
            d1_col = col

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(asof)
    out["sheet"] = sheet_name
    out["instrument"] = instrument
    out["contract"] = df["contract_name"].astype(str)
    out["maturity"] = out["contract"].map(get_maturity_from_contract)

    out["best_bid"] = df[best_bid_col].map(parse_num) if best_bid_col else None
    out["best_ask"] = df[best_ask_col].map(parse_num) if best_ask_col else None
    out["last_price"] = df[last_price_col].map(parse_num) if last_price_col else None
    out["session_volume_mwh"] = df[session_volume_col].map(parse_num) if session_volume_col else None
    out["last_volume_mwh"] = df[last_volume_col].map(parse_num) if last_volume_col else None
    out["open_interest"] = df[open_interest_col].map(parse_num) if open_interest_col else None
    out["nr_contracts"] = df[nr_contracts_col].map(parse_num) if nr_contracts_col else None
    out["otc_volume_mwh"] = df[otc_volume_col].map(parse_num) if otc_volume_col else None
    out["D"] = df[d_col].map(parse_num) if d_col else None
    out["D-1"] = df[d1_col].map(parse_num) if d1_col else None

    mid_bid_ask = (out["best_bid"] + out["best_ask"]) / 2
    out["curve_price"] = out["D"].combine_first(out["last_price"]).combine_first(mid_bid_ask).combine_first(out["D-1"])
    out["sort_key"] = out["contract"].map(contract_sort_key)

    # Filter instrument code again in case the page carries multiple blocks.
    out = out[out["contract"].str.contains(rf"^{re.escape(instrument)}\s", regex=True, na=False)].copy()
    if maturity_filter:
        out = out[out["maturity"] == maturity_filter].copy()

    out = out.dropna(subset=["curve_price"], how="all")
    out = out.sort_values(["date", "sheet", "sort_key", "contract"]).reset_index(drop=True)
    return out


def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


@st.cache_data(show_spinner=True, ttl=1800)
def load_omip_curve_range(
    start_date: date,
    end_date: date,
    product: str,
    zone: str,
    instruments: tuple[tuple[str, str], ...],
    maturity_filter: str | None,
    include_maturity_param: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load OMIP contracts for a date range and one or more instruments.

    Returns (data, debug). Debug helps diagnose website layout issues.
    """
    all_parts: list[pd.DataFrame] = []
    debug_rows: list[dict] = []

    for sheet_name, instrument in instruments:
        for d in daterange(start_date, end_date):
            try:
                tables, html, url = fetch_omip_tables(d, product, zone, instrument, include_maturity_param, maturity_filter)
                raw_rows = extract_contract_rows(tables, html, instrument)
                parsed = normalise_omip_contracts(raw_rows, d, sheet_name, instrument, maturity_filter)
                if not parsed.empty:
                    all_parts.append(parsed)
                # Debug based on first table sample, matching the working range script.
                sample_first_col = ""
                first_col_name = ""
                n_rows_first_table = 0
                if tables:
                    tt = flatten_columns(tables[0])
                    n_rows_first_table = len(tt)
                    if len(tt.columns):
                        first_col_name = str(tt.columns[0])
                        sample_first_col = " || ".join(normalize_text_series(tt[tt.columns[0]]).head(8).tolist())
                debug_rows.append(
                    {
                        "date": d.isoformat(),
                        "sheet": sheet_name,
                        "instrument": instrument,
                        "url": url,
                        "tables_found": len(tables),
                        "rows_parsed": len(parsed),
                        "first_table_rows": n_rows_first_table,
                        "first_col_name": first_col_name,
                        "sample_first_col": sample_first_col,
                    }
                )
            except Exception as exc:
                debug_rows.append(
                    {
                        "date": d.isoformat(),
                        "sheet": sheet_name,
                        "instrument": instrument,
                        "url": build_omip_url(d, product, zone, instrument, maturity_filter if include_maturity_param else None),
                        "tables_found": 0,
                        "rows_parsed": 0,
                        "error": str(exc),
                    }
                )

    data = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()
    debug = pd.DataFrame(debug_rows)
    return data, debug


# =========================================================
# CHARTS / EXPORTS
# =========================================================
def build_curve_chart(df: pd.DataFrame, title: str):
    if df.empty:
        return None
    plot = df.copy()
    # For single-date data: contract curve.
    dates = sorted(plot["date"].dt.date.unique().tolist())
    if len(dates) == 1:
        order = plot.sort_values("sort_key")["contract"].drop_duplicates().tolist()
        chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("contract:N", sort=order, axis=alt.Axis(title=None, labelAngle=-35)),
            y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
            color=alt.Color("sheet:N", title="Curve", scale=alt.Scale(range=[BLUE_PRICE, YELLOW_DARK, GREY, RED])),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("sheet:N", title="Curve"),
                alt.Tooltip("contract:N", title="Contract"),
                alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
                alt.Tooltip("D:Q", title="D €/MWh", format=",.2f"),
                alt.Tooltip("D-1:Q", title="D-1 €/MWh", format=",.2f"),
                alt.Tooltip("best_bid:Q", title="Best bid €/MWh", format=",.2f"),
                alt.Tooltip("best_ask:Q", title="Best ask €/MWh", format=",.2f"),
                alt.Tooltip("open_interest:Q", title="Open interest", format=",.0f"),
            ],
        ).properties(title=title, height=420)
        return apply_common_chart_style(chart, height=420)

    # Multi-date data: evolution of selected contracts.
    contract_options = plot.sort_values("sort_key")["contract"].drop_duplicates().tolist()
    default_contracts = contract_options[: min(6, len(contract_options))]
    chosen_contracts = st.multiselect("Contracts to plot", contract_options, default=default_contracts)
    plot = plot[plot["contract"].isin(chosen_contracts)].copy()
    if plot.empty:
        return None
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=2.8).encode(
        x=alt.X("date:T", axis=alt.Axis(title=None, format="%d-%b")),
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        color=alt.Color("contract:N", title="Contract"),
        strokeDash=alt.StrokeDash("sheet:N", title="Curve"),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
            alt.Tooltip("sheet:N", title="Curve"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
            alt.Tooltip("D:Q", title="D €/MWh", format=",.2f"),
            alt.Tooltip("D-1:Q", title="D-1 €/MWh", format=",.2f"),
        ],
    ).properties(title=title, height=420)
    return apply_common_chart_style(chart, height=420)


def dataframe_to_excel_bytes(data: pd.DataFrame, debug: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if data.empty:
            pd.DataFrame({"info": ["No OMIP data parsed"]}).to_excel(writer, sheet_name="Data", index=False)
        else:
            for sheet_name, df in data.groupby("sheet"):
                df.drop(columns=["sort_key"], errors="ignore").to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)
            data.drop(columns=["sort_key"], errors="ignore").to_excel(writer, sheet_name="All_Data", index=False)
        debug.to_excel(writer, sheet_name="DEBUG", index=False)
    return output.getvalue()


# =========================================================
# MAIN UI
# =========================================================
section_header("OMIP live forward curve")
st.caption(
    "Uses the same robust extraction pattern as the working OMIP scripts: requests + browser headers + "
    "pd.read_html(io.StringIO(html)) + contract filtering by instrument code."
)

left, mid, right = st.columns(3)
with left:
    start_d = st.date_input("Start date", value=date(2026, 2, 3))
    end_d = st.date_input("End date", value=date(2026, 2, 3))
with mid:
    product_label = st.selectbox("Product", list(PRODUCTS.keys()), index=0)
    zone_label = st.selectbox("Zone", list(ZONES.keys()), index=0)
with right:
    maturity_label = st.selectbox("Maturity filter", list(MATURITY_FILTERS.keys()), index=list(MATURITY_FILTERS.keys()).index("Year"))
    include_maturity_param = st.checkbox("Send maturity parameter to OMIP URL", value=False, help="Usually leave off. The working extraction scripts fetch without maturity and filter locally.")

instrument_choice = st.radio(
    "Curves to pull",
    ["Baseload + Solar", "Baseload only", "Solar only", "Custom instrument"],
    horizontal=True,
    index=0,
)

if instrument_choice == "Baseload + Solar":
    selected_instruments = (("Baseload", "FTB"), ("Solar", "FTS"))
elif instrument_choice == "Baseload only":
    selected_instruments = (("Baseload", "FTB"),)
elif instrument_choice == "Solar only":
    selected_instruments = (("Solar", "FTS"),)
else:
    instr_label = st.selectbox("Instrument", list(INSTRUMENTS.keys()), index=list(INSTRUMENTS.keys()).index("SPEL Solar Futures"))
    selected_instruments = ((instr_label, INSTRUMENTS[instr_label]),)

product = PRODUCTS[product_label]
zone = ZONES[zone_label]
maturity_code = MATURITY_FILTERS[maturity_label]

if start_d > end_d:
    st.error("Start date must be before or equal to end date.")
    st.stop()

example_urls = [build_omip_url(start_d, product, zone, inst, maturity_code if include_maturity_param else None) for _, inst in selected_instruments]
st.markdown(" | ".join([f"[Open OMIP {name}]({url})" for (name, _), url in zip(selected_instruments, example_urls)]))

pull = st.button("Pull OMIP prices", type="primary")

if pull:
    data, debug = load_omip_curve_range(
        start_date=start_d,
        end_date=end_d,
        product=product,
        zone=zone,
        instruments=selected_instruments,
        maturity_filter=maturity_code,
        include_maturity_param=include_maturity_param,
    )

    if data.empty:
        st.warning("OMIP page(s) were reachable or attempted, but no curve rows could be parsed.")
        st.dataframe(debug, use_container_width=True)
    else:
        st.success(f"Loaded {len(data)} OMIP contract rows.")
        title = f"OMIP {zone_label} | {maturity_label} | {start_d.isoformat()}" + (f" to {end_d.isoformat()}" if start_d != end_d else "")
        chart = build_curve_chart(data, title)
        if chart is not None:
            st.altair_chart(chart, use_container_width=True)

        display_cols = [
            "date", "sheet", "contract", "maturity", "curve_price", "D", "D-1", "best_bid", "best_ask", "last_price",
            "session_volume_mwh", "open_interest", "nr_contracts", "otc_volume_mwh",
        ]
        display_cols = [c for c in display_cols if c in data.columns]
        st.dataframe(styled_df(data[display_cols]), use_container_width=True)

        st.download_button(
            "Download OMIP parsed data as Excel",
            data=dataframe_to_excel_bytes(data, debug),
            file_name=f"omip_{zone}_{product}_{start_d.isoformat()}_{end_d.isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        with st.expander("Debug extraction log"):
            st.dataframe(debug, use_container_width=True)

section_header("Manual upload fallback")
st.caption("Upload a CSV/XLSX copied or exported from OMIP. The same normalisation logic will be applied.")
upload = st.file_uploader("Upload OMIP CSV/XLSX", type=["csv", "xlsx", "xls"])
if upload is not None:
    try:
        if upload.name.lower().endswith(".csv"):
            raw_upload = pd.read_csv(upload)
        else:
            raw_upload = pd.read_excel(upload)
        sheet_name, instrument = selected_instruments[0]
        raw_rows = extract_contract_rows([raw_upload], "", instrument)
        if raw_rows.empty:
            # Maybe upload already has clean columns.
            raw_rows = raw_upload
        parsed_upload = normalise_omip_contracts(raw_rows, start_d, sheet_name, instrument, maturity_code)
        if parsed_upload.empty:
            st.warning("Could not identify OMIP curve columns in the uploaded file.")
            st.dataframe(raw_upload, use_container_width=True)
        else:
            chart = build_curve_chart(parsed_upload, f"Uploaded OMIP curve | {sheet_name} | {maturity_label}")
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            st.dataframe(styled_df(parsed_upload.drop(columns=["sort_key"], errors="ignore")), use_container_width=True)
    except Exception as exc:
        st.error(f"Could not parse upload: {exc}")
