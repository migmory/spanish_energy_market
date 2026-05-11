from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta
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

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_PRICE = "#FACC15"
GREY = "#6B7280"
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
    "Baseload - FTB": "FTB",
    "Solar - FTS": "FTS",
    "Peak - FTP": "FTP",
    "Base Forward - FWB": "FWB",
    "Base Swap - SWB": "SWB",
}

# User-facing maturity filters. OMIP page can be requested without maturity;
# the parser then filters the contracts we care about.
MATURITY_FILTERS = {
    "All": None,
    "Day": "D",
    "Weekend": "WE",
    "Week": "W",
    "Month": "M",
    "Quarter": "Q",
    "Year": "YR",
    "PPA": "PPA",
}

TABLE_HEADER_FONT_PCT = "135%"
TABLE_BODY_FONT_PCT = "108%"
WS_RE = re.compile(r"\s+", re.UNICODE)

# =========================================================
# DISPLAY HELPERS
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


def apply_common_chart_style(chart, height: int = 390):
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
# OMIP FETCH - SAME STYLE AS WORKING SCRIPT
# =========================================================
def omip_url(date_str: str, product: str, zone: str, instrument: str, include_maturity_param: bool = False, maturity: str | None = None) -> str:
    params = {
        "date": date_str,
        "product": product,
        "zone": zone,
        "instrument": instrument,
    }
    # The local scripts that worked did NOT pass maturity in the request.
    # Keep it optional for testing, but off by default.
    if include_maturity_param and maturity and maturity != "ALL":
        params["maturity"] = maturity
    return f"{OMIP_BASE_URL}?{urlencode(params)}"


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_tables(date_str: str, product: str, zone: str, instrument: str, include_maturity_param: bool = False, maturity: str | None = None) -> tuple[list[pd.DataFrame], str]:
    url = omip_url(date_str, product, zone, instrument, include_maturity_param, maturity)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-GB,en;q=0.9,es;q=0.8",
        "Referer": "https://www.omip.pt/",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    html = r.text

    # CLAVE: exactly as your working scripts do it.
    # On Streamlit Cloud, make sure lxml is in requirements.txt.
    # If lxml is not installed, pandas raises: ImportError: lxml not found.
    tables = pd.read_html(io.StringIO(html))
    return tables, url


def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def concat_tables(tables: list[pd.DataFrame]) -> pd.DataFrame:
    blocks = []
    for i, df in enumerate(tables, start=1):
        temp = df.copy()
        temp.insert(0, "table_id", i)
        blocks.append(temp)
        blocks.append(pd.DataFrame([{}]))
    return pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame()

# =========================================================
# PARSING
# =========================================================
def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace("\ufeff", "")
    s = WS_RE.sub(" ", s).strip()
    return s


def parse_num(value) -> float | None:
    s = normalize_str(value)
    if not s or s.lower() in {"n.a.", "na", "nan", "none", "-", ""}:
        return None
    s = s.replace("€", "").replace("/MWh", "").replace("MWh", "").replace(" ", "")
    # Decimal comma support; thousands comma support.
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            " | ".join([str(x) for x in tup if str(x) != "nan"]).strip()
            for tup in out.columns
        ]
    out.columns = [str(c).strip() for c in out.columns]
    return out


def extract_contract_name(raw_text: str, instrument: str) -> str | None:
    text = normalize_str(raw_text)
    # OMIP read_html often returns one long string:
    # "ISIN Code: ... Trading quotation: €/MWhFTS YR-27"
    m = re.search(rf"({re.escape(instrument)}\s+[A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)?)\s*$", text)
    if m:
        return normalize_str(m.group(1))
    # Fallback: any occurrence of instrument followed by text, stop before transparency if present.
    m = re.search(rf"({re.escape(instrument)}\s+.+)$", text)
    if m:
        tail = normalize_str(m.group(1))
        tail = tail.split(" Transparency")[0].strip()
        return tail[:80]
    return None


def contract_maturity(contract: str) -> str:
    c = normalize_str(contract)
    if re.search(rf"\bYR-\d{{2}}", c):
        return "YR"
    if re.search(rf"\bQ[1-4]-\d{{2}}", c) or re.search(rf"\bQ\s+Q[1-4]-\d{{2}}", c):
        return "Q"
    if re.search(rf"\bM\s+[A-Za-z]{{3}}-\d{{2}}", c):
        return "M"
    if re.search(rf"\bW[K]?\d{{1,2}}-\d{{2}}", c) or re.search(rf"\bW\s+W[K]?\d{{1,2}}-\d{{2}}", c):
        return "W"
    if re.search(rf"\bWE\b", c):
        return "WE"
    if re.search(rf"\bD\b", c):
        return "D"
    if "PPA" in c:
        return "PPA"
    return "Other"


def contract_sort_key(contract: str) -> int:
    c = normalize_str(contract)
    m = re.search(r"YR-(\d{2})", c)
    if m:
        return (2000 + int(m.group(1))) * 10000
    m = re.search(r"Q([1-4])-(\d{2})", c)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1)) * 1000
    m = re.search(r"M\s+([A-Za-z]{3})-(\d{2})", c)
    if m:
        months = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
        return (2000 + int(m.group(2))) * 10000 + months.get(m.group(1).title(), 99) * 100
    m = re.search(r"(?:W|WK)(\d{1,2})-(\d{2})", c)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1))
    m = re.search(r"(\d{2})$", c)
    if m:
        return (2000 + int(m.group(1))) * 10000
    return 99999999


def delivery_label_from_contract(contract: str) -> str:
    """Return a common delivery label shared by Baseload/Solar, e.g. FTB YR-27 and FTS YR-27 -> YR27."""
    c = normalize_str(contract)
    m = re.search(r"\bYR-(\d{2})", c)
    if m:
        return f"YR{m.group(1)}"
    m = re.search(r"\bQ([1-4])-(\d{2})", c)
    if m:
        return f"Q{m.group(1)}-{m.group(2)}"
    m = re.search(r"\bM\s+([A-Za-z]{3})-(\d{2})", c)
    if m:
        return f"{m.group(1).title()}-{m.group(2)}"
    m = re.search(r"\b(?:W|WK)(\d{1,2})-(\d{2})", c)
    if m:
        return f"WK{int(m.group(1)):02d}-{m.group(2)}"
    return re.sub(r"^(FTB|FTS|FTP|FWB|SWB)\s+", "", c).replace("-", "") or c


def parse_raw_tables_to_contracts(tables: list[pd.DataFrame], asof: date, sheet_name: str, instrument: str) -> pd.DataFrame:
    rows: list[dict] = []
    for table_id, table in enumerate(tables, start=1):
        df = flatten_columns(table)
        if df.empty:
            continue
        # Scan row by row. This is more robust than relying on headers because OMIP
        # has multi-level headers and rows with embedded ISIN metadata.
        for _, row in df.iterrows():
            values = list(row.values)
            texts = [normalize_str(v) for v in values]
            joined = " | ".join(texts)
            contract = None
            contract_col_pos = None
            for pos, txt in enumerate(texts):
                maybe = extract_contract_name(txt, instrument)
                if maybe:
                    contract = maybe
                    contract_col_pos = pos
                    break
            if not contract:
                continue

            # OMIP standard table positions after pandas read_html:
            # 0 Contract name; 3 Best bid; 4 Best Ask; 5 Session Vol; 7 Last Price;
            # 8 Last Time; 9 Last Vol; 11 Open Interest; 12 Nr Contracts; 13 OTC Vol;
            # 15 D; 16 D-1; 20 Transparency.
            def val_at(pos: int):
                return values[pos] if pos < len(values) else None

            best_bid = parse_num(val_at(3))
            best_ask = parse_num(val_at(4))
            session_volume = parse_num(val_at(5))
            last_price = parse_num(val_at(7))
            last_time = normalize_str(val_at(8)) or None
            last_volume = parse_num(val_at(9))
            open_interest = parse_num(val_at(11))
            nr_contracts = parse_num(val_at(12))
            otc_volume = parse_num(val_at(13))
            d_price = parse_num(val_at(15))
            d_minus_1 = parse_num(val_at(16))

            # If the table positions ever shift, try a weak fallback by taking the last
            # numeric values in the row. D and D-1 are usually the last two price-like cells.
            if d_price is None and d_minus_1 is None:
                nums = [parse_num(v) for v in values]
                nums = [x for x in nums if x is not None]
                if len(nums) >= 2:
                    d_price, d_minus_1 = nums[-2], nums[-1]
                elif len(nums) == 1:
                    d_price = nums[-1]

            curve_price = d_price
            if curve_price is None:
                curve_price = last_price
            if curve_price is None and best_bid is not None and best_ask is not None:
                curve_price = (best_bid + best_ask) / 2.0
            if curve_price is None:
                curve_price = d_minus_1

            rows.append(
                {
                    "market_date": asof,
                    "sheet": sheet_name,
                    "instrument": instrument,
                    "table_id": table_id,
                    "contract": contract,
                    "maturity": contract_maturity(contract),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "session_volume_mwh": session_volume,
                    "last_price": last_price,
                    "last_time": last_time,
                    "last_volume_mwh": last_volume,
                    "open_interest": open_interest,
                    "nr_contracts": nr_contracts,
                    "otc_volume_mwh": otc_volume,
                    "d_price": d_price,
                    "d_minus_1": d_minus_1,
                    "curve_price": curve_price,
                    "raw_contract_cell": normalize_str(values[contract_col_pos]) if contract_col_pos is not None else joined[:300],
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=[
            "market_date", "sheet", "instrument", "delivery_label", "contract", "maturity", "curve_price",
            "d_price", "d_minus_1", "best_bid", "best_ask", "last_price", "open_interest", "sort_key"
        ])
    out["sort_key"] = out["contract"].map(contract_sort_key)
    out["delivery_label"] = out["contract"].map(delivery_label_from_contract)
    out = out.drop_duplicates(subset=["market_date", "sheet", "instrument", "contract"], keep="last")
    return out.sort_values(["sheet", "sort_key", "contract"]).reset_index(drop=True)


def parse_working_excel_upload(file) -> pd.DataFrame:
    xls = pd.ExcelFile(file)
    parts = []
    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        if raw.empty:
            continue
        instrument = "FTS" if "solar" in sheet.lower() else "FTB" if "base" in sheet.lower() else None
        if instrument is None:
            # Try to infer from row text.
            joined = " ".join(raw.astype(str).fillna("").head(20).values.ravel().tolist())
            if "FTS" in joined:
                instrument = "FTS"
            elif "FTB" in joined:
                instrument = "FTB"
            else:
                continue
        # Since this is already Excel from concat_tables, pass it as a single table.
        parsed = parse_raw_tables_to_contracts([raw], date.today(), sheet, instrument)
        if not parsed.empty:
            parts.append(parsed)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def filter_maturity(df: pd.DataFrame, maturity_filter: str | None) -> pd.DataFrame:
    if df.empty or maturity_filter is None:
        return df
    return df[df["maturity"] == maturity_filter].copy()


def fetch_and_parse_day(asof: date, product: str, zone: str, instruments: dict[str, str], include_maturity_param: bool, maturity_param: str | None) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    parts = []
    debug_rows = []
    raw_tables_by_sheet: dict[str, pd.DataFrame] = {}
    date_str = asof.strftime("%Y-%m-%d")

    for sheet_name, instrument in instruments.items():
        try:
            tables, url = fetch_tables(date_str, product, zone, instrument, include_maturity_param, maturity_param)
            raw_concat = concat_tables(tables)
            raw_tables_by_sheet[sheet_name] = raw_concat
            parsed = parse_raw_tables_to_contracts(tables, asof, sheet_name, instrument)
            parts.append(parsed)
            debug_rows.append({
                "date": date_str,
                "sheet": sheet_name,
                "instrument": instrument,
                "url": url,
                "tables_found": len(tables),
                "raw_rows": int(sum(len(t) for t in tables)),
                "rows_parsed": len(parsed),
                "error": "",
            })
        except Exception as exc:
            debug_rows.append({
                "date": date_str,
                "sheet": sheet_name,
                "instrument": instrument,
                "url": omip_url(date_str, product, zone, instrument, include_maturity_param, maturity_param),
                "tables_found": 0,
                "raw_rows": 0,
                "rows_parsed": 0,
                "error": str(exc),
            })

    data = pd.concat([p for p in parts if p is not None and not p.empty], ignore_index=True) if any(not p.empty for p in parts) else pd.DataFrame()
    debug = pd.DataFrame(debug_rows)
    return data, debug, raw_tables_by_sheet


def fetch_and_parse_range(start_date: date, end_date: date, product: str, zone: str, instruments: dict[str, str], include_maturity_param: bool, maturity_param: str | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    parts = []
    debug_parts = []
    for d in daterange(start_date, end_date):
        data, debug, _ = fetch_and_parse_day(d, product, zone, instruments, include_maturity_param, maturity_param)
        if not data.empty:
            parts.append(data)
        if not debug.empty:
            debug_parts.append(debug)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    dbg = pd.concat(debug_parts, ignore_index=True) if debug_parts else pd.DataFrame()
    return out, dbg

# =========================================================
# CHARTS / EXPORT
# =========================================================
def build_curve_chart(df: pd.DataFrame, title: str):
    if df.empty:
        return None
    plot = df.copy()
    plot["series"] = plot["sheet"].astype(str)
    if "delivery_label" not in plot.columns:
        plot["delivery_label"] = plot["contract"].map(delivery_label_from_contract)
    order = (
        plot[["delivery_label", "sort_key"]]
        .drop_duplicates()
        .sort_values("sort_key")["delivery_label"]
        .tolist()
    )
    color_scale = alt.Scale(
        domain=["Baseload", "Solar"],
        range=[BLUE_PRICE, YELLOW_PRICE],
    )
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X(
            "delivery_label:N",
            sort=order,
            axis=alt.Axis(title="Delivery period", labelAngle=-35),
        ),
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title=None, scale=color_scale),
        tooltip=[
            alt.Tooltip("market_date:T", title="Market date", format="%Y-%m-%d"),
            alt.Tooltip("sheet:N", title="Curve"),
            alt.Tooltip("delivery_label:N", title="Delivery period"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
            alt.Tooltip("d_price:Q", title="D €/MWh", format=",.2f"),
            alt.Tooltip("d_minus_1:Q", title="D-1 €/MWh", format=",.2f"),
            alt.Tooltip("best_bid:Q", title="Best bid €/MWh", format=",.2f"),
            alt.Tooltip("best_ask:Q", title="Best ask €/MWh", format=",.2f"),
            alt.Tooltip("open_interest:Q", title="Open interest", format=",.0f"),
        ],
    ).properties(title=title, height=420)
    return apply_common_chart_style(chart, height=420)


def build_time_evolution_chart(df: pd.DataFrame, contract: str, title: str):
    if df.empty or not contract:
        return None
    plot = df[df["contract"] == contract].copy()
    if plot.empty:
        return None
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
        x=alt.X("market_date:T", title=None, axis=alt.Axis(format="%d-%b", labelAngle=0)),
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("market_date:T", title="Market date", format="%Y-%m-%d"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
            alt.Tooltip("d_price:Q", title="D €/MWh", format=",.2f"),
            alt.Tooltip("d_minus_1:Q", title="D-1 €/MWh", format=",.2f"),
        ],
    ).properties(title=title, height=380)
    return apply_common_chart_style(chart, height=380)


def dataframe_to_excel_bytes(data: pd.DataFrame, debug: pd.DataFrame | None = None, raw_tables: dict[str, pd.DataFrame] | None = None) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if data.empty:
            pd.DataFrame({"info": ["No parsed OMIP data"]}).to_excel(writer, sheet_name="info", index=False)
        else:
            data.drop(columns=["raw_contract_cell"], errors="ignore").to_excel(writer, sheet_name="All curves", index=False)
            for sheet in sorted(data["sheet"].dropna().unique().tolist()):
                data[data["sheet"] == sheet].drop(columns=["raw_contract_cell"], errors="ignore").to_excel(writer, sheet_name=str(sheet)[:31], index=False)
        if debug is not None and not debug.empty:
            debug.to_excel(writer, sheet_name="DEBUG", index=False)
        if raw_tables:
            for name, raw in raw_tables.items():
                if not raw.empty:
                    raw.to_excel(writer, sheet_name=f"RAW_{name}"[:31], index=False)
    return output.getvalue()

# =========================================================
# UI
# =========================================================
section_header("OMIP live forward curve")
st.caption(
    "Uses the same approach as your working Python scripts: requests.get(...) + pd.read_html(io.StringIO(html)). "
    "On Streamlit Cloud this requires lxml in requirements.txt."
)

with st.expander("Important deployment note", expanded=False):
    st.markdown(
        """
If the app shows an error like **`Import lxml failed`**, add this line to your `requirements.txt` and redeploy:

```txt
lxml
```

Your standalone `.py` works because your local environment has the HTML parser installed. Streamlit Cloud needs the same dependency.
        """
    )

c1, c2, c3 = st.columns(3)
with c1:
    mode = st.radio("Mode", ["Single date", "Date range"], horizontal=True)
    asof = st.date_input("Market date (OMIP published curve date)", value=date.today())
    st.caption("Market date = date used in the OMIP URL to retrieve the published forward curve, not the delivery year.")
    start_date = st.date_input("Start date", value=date.today() - timedelta(days=7))
    end_date = st.date_input("End date", value=date.today())
with c2:
    product_label = st.selectbox("Product", list(PRODUCTS.keys()), index=0)
    zone_label = st.selectbox("Zone", list(ZONES.keys()), index=0)
    maturity_label = st.selectbox("Maturity filter", list(MATURITY_FILTERS.keys()), index=list(MATURITY_FILTERS.keys()).index("Year"))
with c3:
    curve_choice = st.multiselect("Curves", ["Baseload", "Solar"], default=["Baseload", "Solar"])
    include_maturity_param = st.checkbox("Send maturity parameter to OMIP URL", value=False, help="Off by default because the scripts that work do not send maturity; they filter after reading the page.")
    pull = st.button("Pull OMIP prices", type="primary")

product = PRODUCTS[product_label]
zone = ZONES[zone_label]
maturity_filter = MATURITY_FILTERS[maturity_label]
maturity_param = "ALL" if maturity_filter is None else maturity_filter
selected_instruments = {}
if "Baseload" in curve_choice:
    selected_instruments["Baseload"] = "FTB"
if "Solar" in curve_choice:
    selected_instruments["Solar"] = "FTS"

sample_instrument = next(iter(selected_instruments.values()), "FTB")
st.markdown(f"[Open OMIP page]({omip_url(asof.strftime('%Y-%m-%d'), product, zone, sample_instrument, include_maturity_param, maturity_param)})")

if pull:
    if not selected_instruments:
        st.warning("Select at least one curve.")
    else:
        if mode == "Single date":
            data, debug, raw_tables = fetch_and_parse_day(asof, product, zone, selected_instruments, include_maturity_param, maturity_param)
        else:
            data, debug = fetch_and_parse_range(start_date, end_date, product, zone, selected_instruments, include_maturity_param, maturity_param)
            raw_tables = {}

        if not data.empty:
            data = filter_maturity(data, maturity_filter)

        if data.empty:
            st.warning("OMIP page(s) were reachable or attempted, but no curve rows could be parsed for the selected filters.")
            st.dataframe(debug, use_container_width=True)
            st.download_button(
                "Download debug Excel",
                data=dataframe_to_excel_bytes(data, debug, raw_tables),
                file_name=f"omip_debug_{zone}_{asof.isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.success(f"Loaded {len(data)} OMIP rows.")
            if mode == "Single date":
                chart = build_curve_chart(data, f"OMIP {zone_label} forward curve | Market date {asof.isoformat()} | {maturity_label}")
                if chart is not None:
                    st.altair_chart(chart, use_container_width=True)
            else:
                available_contracts = data.sort_values(["sheet", "sort_key"])["contract"].drop_duplicates().tolist()
                selected_contract = st.selectbox("Contract to plot over time", available_contracts)
                chart = build_time_evolution_chart(data, selected_contract, f"OMIP {selected_contract} evolution")
                if chart is not None:
                    st.altair_chart(chart, use_container_width=True)

            display_cols = [
                "market_date", "sheet", "instrument", "delivery_label", "contract", "maturity", "curve_price",
                "d_price", "d_minus_1", "best_bid", "best_ask", "last_price", "open_interest",
                "nr_contracts", "otc_volume_mwh", "session_volume_mwh",
            ]
            st.dataframe(styled_df(data[[c for c in display_cols if c in data.columns]]), use_container_width=True)
            with st.expander("Debug"):
                st.dataframe(debug, use_container_width=True)
            st.download_button(
                "Download OMIP parsed Excel",
                data=dataframe_to_excel_bytes(data, debug, raw_tables),
                file_name=f"omip_{zone}_{product}_{mode.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

section_header("Upload OMIP Excel fallback")
st.caption("Upload an Excel produced by your working script. The app will parse the Baseload / Solar sheets and chart the contracts.")
file = st.file_uploader("Upload OMIP_ES_EL_YYYY-MM-DD.xlsx", type=["xlsx", "xls"])
if file is not None:
    try:
        uploaded = parse_working_excel_upload(file)
        uploaded = filter_maturity(uploaded, maturity_filter)
        if uploaded.empty:
            st.warning("The uploaded Excel was read, but no contracts matched the selected maturity filter.")
        else:
            st.success(f"Parsed {len(uploaded)} rows from uploaded Excel.")
            chart = build_curve_chart(uploaded, f"Uploaded OMIP curve | {maturity_label}")
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            display_cols = [
                "sheet", "instrument", "delivery_label", "contract", "maturity", "curve_price", "d_price", "d_minus_1",
                "best_bid", "best_ask", "last_price", "open_interest", "nr_contracts"
            ]
            st.dataframe(styled_df(uploaded[[c for c in display_cols if c in uploaded.columns]]), use_container_width=True)
            st.download_button(
                "Download parsed upload",
                data=dataframe_to_excel_bytes(uploaded),
                file_name="omip_uploaded_parsed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    except Exception as exc:
        st.error(f"Could not parse uploaded OMIP Excel: {exc}")
