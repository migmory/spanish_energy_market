from __future__ import annotations

import html
import re
from datetime import date, timedelta
from html.parser import HTMLParser
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

# =========================================================
# PAGE CONFIG
# =========================================================
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

TABLE_HEADER_FONT_PCT = "135%"
TABLE_BODY_FONT_PCT = "108%"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-GB,en;q=0.9,es;q=0.8",
    "Referer": "https://www.omip.pt/",
}
WS_RE = re.compile(r"\s+", re.UNICODE)

# Expected OMIP columns after the contract name. The public table layout can move,
# but these names match the scripts that successfully extract OMIP with pd.read_html.
OMIP_VALUE_COLUMNS = [
    "Best bid (€/MWh)",
    "Best Ask (€/MWh)",
    "Session volume (MWh)",
    "Last price (€/MWh)",
    "Last time",
    "Last volume (MWh)",
    "Open Interest",
    "Nr of Contracts",
    "OTC volume (MWh)",
    "D (€/MWh)",
    "D-1 (€/MWh)",
]

# =========================================================
# STYLE HELPERS
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


def apply_common_chart_style(chart, height: int = 420):
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
# HTML TABLE PARSER WITHOUT lxml
# =========================================================
class SimpleTableParser(HTMLParser):
    """Tiny stdlib HTML table extractor.

    This avoids pandas.read_html(), so Streamlit Cloud does not need lxml.
    It extracts text from <table>/<tr>/<td>/<th> into a list of tables.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_table: list[list[str]] = []
        self._current_row: list[str] = []
        self._current_cell: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        tag = tag.lower()
        if tag == "table":
            self._in_table = True
            self._current_table = []
        elif self._in_table and tag == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_table and self._in_row and tag in {"td", "th"}:
            self._in_cell = True
            self._current_cell = []
        elif self._in_cell and tag in {"br", "p", "div", "span"}:
            self._current_cell.append(" ")

    def handle_endtag(self, tag: str):
        tag = tag.lower()
        if self._in_cell and tag in {"td", "th"}:
            value = normalize_scalar("".join(self._current_cell))
            self._current_row.append(value)
            self._current_cell = []
            self._in_cell = False
        elif self._in_table and self._in_row and tag == "tr":
            if any(x.strip() for x in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = []
            self._in_row = False
        elif self._in_table and tag == "table":
            if self._current_table:
                self.tables.append(self._current_table)
            self._current_table = []
            self._in_table = False

    def handle_data(self, data: str):
        if self._in_cell:
            self._current_cell.append(data)


def normalize_scalar(value) -> str:
    s = html.unescape(str(value))
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace("\ufeff", "")
    s = WS_RE.sub(" ", s).strip()
    return s


def normalize_text_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(normalize_scalar)


def tables_from_html_no_lxml(raw_html: str) -> list[pd.DataFrame]:
    parser = SimpleTableParser()
    parser.feed(raw_html)
    dfs: list[pd.DataFrame] = []

    for rows in parser.tables:
        if not rows:
            continue
        max_len = max(len(r) for r in rows)
        padded = [r + [""] * (max_len - len(r)) for r in rows]
        df = pd.DataFrame(padded)
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if not df.empty:
            dfs.append(df)
    return dfs

# =========================================================
# OMIP PARSING
# =========================================================
def build_omip_url(asof: date, product: str, zone: str, instrument: str, maturity: str | None = None) -> str:
    # Important: the working extraction scripts did not need maturity in the request.
    # We include it only if explicitly selected, but we also parse/filter after download.
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
def fetch_omip_html(asof: date, product: str, zone: str, instrument: str, maturity: str | None = None) -> tuple[str, str]:
    url = build_omip_url(asof, product, zone, instrument, maturity)
    resp = requests.get(url, headers=HEADERS, timeout=(15, 60))
    resp.raise_for_status()
    return resp.text, url


def parse_num(value) -> float | None:
    if value is None:
        return None
    s = normalize_scalar(value)
    if not s or s.lower() in {"n.a.", "n.a", "na", "nan", "none", "-", "—"}:
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


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [" | ".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in out.columns]
    else:
        out.columns = [str(c).strip() for c in out.columns]
    return out


def rows_to_structured_df(table: pd.DataFrame, instrument: str) -> pd.DataFrame:
    """Extract OMIP contract rows from a raw table-like dataframe.

    Works with both parsed tables with real headers and stdlib parser tables where
    the header may still be row 0.
    """
    if table.empty:
        return pd.DataFrame()

    df = flatten_columns(table).dropna(axis=1, how="all").dropna(axis=0, how="all").copy()
    if df.empty:
        return pd.DataFrame()

    # Normalize all cells to strings for robust matching.
    for c in df.columns:
        df[c] = df[c].map(normalize_scalar)

    rows = []
    patt = re.compile(rf"^{re.escape(instrument)}\s+", re.IGNORECASE)

    # Search each row for a cell that starts with FTB / FTS / etc.
    for _, row in df.iterrows():
        cells = [normalize_scalar(x) for x in row.tolist()]
        contract_idx = None
        for idx, cell in enumerate(cells):
            if patt.search(cell):
                contract_idx = idx
                break
        if contract_idx is None:
            continue

        contract_name = normalize_scalar(cells[contract_idx])
        values = cells[contract_idx + 1:]
        values = values + [None] * max(0, len(OMIP_VALUE_COLUMNS) - len(values))
        record = {"contract": contract_name}
        for col_name, value in zip(OMIP_VALUE_COLUMNS, values):
            record[col_name] = value
        rows.append(record)

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    return out


def parse_contract_lines_from_text(raw_html: str, instrument: str) -> pd.DataFrame:
    """Last-resort parser using page text lines, no lxml/bs4 required."""
    text = re.sub(r"<script.*?</script>", " ", raw_html, flags=re.I | re.S)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = html.unescape(text).replace("\xa0", " ").replace("\u202f", " ")
    lines = [normalize_scalar(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    rows = []
    patt = re.compile(rf"^({re.escape(instrument)})\s+([^\s]+)\s+(.*)$", re.I)
    for line in lines:
        m = patt.match(line)
        if not m:
            continue
        instr, tail, rest = m.groups()
        contract = f"{instr.upper()} {tail}"
        toks = rest.split()
        toks = toks + [None] * max(0, len(OMIP_VALUE_COLUMNS) - len(toks))
        record = {"contract": contract}
        for col_name, value in zip(OMIP_VALUE_COLUMNS, toks):
            record[col_name] = value
        rows.append(record)

    return pd.DataFrame(rows)


def contract_sort_key(contract: str) -> int:
    if not isinstance(contract, str):
        return 999999999
    c = contract.upper()
    m = re.search(r"YR-(\d{2})", c)
    if m:
        return (2000 + int(m.group(1))) * 10000
    m = re.search(r"Q([1-4])-(\d{2})", c)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1)) * 1000
    m = re.search(r"M(\d{1,2})-(\d{2})", c)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1)) * 100
    m = re.search(r"WK(\d{1,2})-(\d{2})", c)
    if m:
        return (2000 + int(m.group(2))) * 10000 + int(m.group(1))
    m = re.search(r"(\d{2})$", c)
    if m:
        return (2000 + int(m.group(1))) * 10000
    return 999999999


def maturity_matches(contract: str, maturity: str | None) -> bool:
    if maturity is None or maturity == "ALL":
        return True
    c = str(contract).upper()
    if maturity == "YR":
        return "YR-" in c
    if maturity == "QTR":
        return bool(re.search(r"\bQ[1-4]-", c))
    if maturity == "MTH":
        return bool(re.search(r"\bM\d{1,2}-", c))
    if maturity == "WK":
        return "WK" in c
    return maturity in c


def normalize_omip_contract_rows(raw: pd.DataFrame, instrument: str, maturity: str | None, market_date: date, sheet_name: str, url: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    out = raw.copy()
    out["contract"] = normalize_text_series(out["contract"])
    out = out[out["contract"].str.upper().str.startswith(f"{instrument} ", na=False)].copy()
    out = out[out["contract"].map(lambda x: maturity_matches(x, maturity))].copy()

    if out.empty:
        return pd.DataFrame()

    numeric_map = {
        "Best bid (€/MWh)": "best_bid",
        "Best Ask (€/MWh)": "best_ask",
        "Session volume (MWh)": "session_volume_mwh",
        "Last price (€/MWh)": "last_price",
        "Last volume (MWh)": "last_volume_mwh",
        "Open Interest": "open_interest",
        "Nr of Contracts": "nr_contracts",
        "OTC volume (MWh)": "otc_volume_mwh",
        "D (€/MWh)": "reference_price",
        "D-1 (€/MWh)": "d_minus_1",
    }

    for src, dst in numeric_map.items():
        out[dst] = out[src].map(parse_num) if src in out.columns else None

    if "Last time" in out.columns:
        out["last_time"] = out["Last time"].map(normalize_scalar)
    else:
        out["last_time"] = None

    out["curve_price"] = out["reference_price"].combine_first(out["last_price"]).combine_first(
        (out["best_bid"] + out["best_ask"]) / 2
    )
    out = out.dropna(subset=["curve_price"], how="all").copy()
    if out.empty:
        return pd.DataFrame()

    out.insert(0, "date", pd.to_datetime(market_date))
    out.insert(1, "sheet", sheet_name)
    out.insert(2, "instrument", instrument)
    out["url"] = url
    out["sort_key"] = out["contract"].map(contract_sort_key)
    keep_cols = [
        "date",
        "sheet",
        "instrument",
        "contract",
        "curve_price",
        "reference_price",
        "d_minus_1",
        "best_bid",
        "best_ask",
        "last_price",
        "last_time",
        "session_volume_mwh",
        "last_volume_mwh",
        "open_interest",
        "nr_contracts",
        "otc_volume_mwh",
        "url",
        "sort_key",
    ]
    return out[keep_cols].sort_values(["date", "sheet", "sort_key"]).reset_index(drop=True)


def parse_omip_page(raw_html: str, instrument: str, maturity: str | None, market_date: date, sheet_name: str, url: str) -> tuple[pd.DataFrame, dict]:
    debug = {"date": market_date.isoformat(), "sheet": sheet_name, "instrument": instrument, "url": url, "tables_found": 0, "rows_parsed": 0, "parser": "none", "error": ""}

    try:
        tables = tables_from_html_no_lxml(raw_html)
        debug["tables_found"] = len(tables)
        parts = []
        for table in tables:
            rows = rows_to_structured_df(table, instrument)
            if not rows.empty:
                parts.append(rows)
        if parts:
            raw_rows = pd.concat(parts, ignore_index=True)
            curve = normalize_omip_contract_rows(raw_rows, instrument, maturity, market_date, sheet_name, url)
            if not curve.empty:
                debug["rows_parsed"] = len(curve)
                debug["parser"] = "stdlib_html_table_parser"
                return curve, debug
    except Exception as exc:
        debug["error"] = f"table parser: {exc}"

    try:
        raw_rows = parse_contract_lines_from_text(raw_html, instrument)
        curve = normalize_omip_contract_rows(raw_rows, instrument, maturity, market_date, sheet_name, url)
        if not curve.empty:
            debug["rows_parsed"] = len(curve)
            debug["parser"] = "text_fallback_parser"
            return curve, debug
    except Exception as exc:
        debug["error"] = (debug.get("error", "") + f" | text parser: {exc}").strip(" |")

    return pd.DataFrame(), debug


def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_and_parse_one_day(asof: date, product: str, zone: str, instrument: str, maturity: str | None, sheet_name: str) -> tuple[pd.DataFrame, dict]:
    raw_html, url = fetch_omip_html(asof, product, zone, instrument, maturity)
    return parse_omip_page(raw_html, instrument, maturity, asof, sheet_name, url)


def build_forward_curve_chart(df: pd.DataFrame, title: str):
    if df.empty:
        return None
    plot = df.copy()
    plot["date_label"] = pd.to_datetime(plot["date"]).dt.strftime("%Y-%m-%d")
    plot["series"] = plot["sheet"] + " | " + plot["date_label"]
    # Use the most recent date for contract ordering.
    latest = plot[plot["date"] == plot["date"].max()].sort_values(["sheet", "sort_key"])
    order = latest["contract"].drop_duplicates().tolist()
    if not order:
        order = plot.sort_values("sort_key")["contract"].drop_duplicates().tolist()

    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("contract:N", sort=order, axis=alt.Axis(title=None, labelAngle=-35)),
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title=None),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
            alt.Tooltip("sheet:N", title="Curve"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
            alt.Tooltip("reference_price:Q", title="D €/MWh", format=",.2f"),
            alt.Tooltip("d_minus_1:Q", title="D-1 €/MWh", format=",.2f"),
            alt.Tooltip("best_bid:Q", title="Best bid €/MWh", format=",.2f"),
            alt.Tooltip("best_ask:Q", title="Best ask €/MWh", format=",.2f"),
            alt.Tooltip("open_interest:Q", title="Open interest", format=",.0f"),
        ],
    ).properties(title=title, height=430)
    return apply_common_chart_style(chart, height=430)


def dataframe_to_excel_bytes(curves: pd.DataFrame, debug: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if curves.empty:
            pd.DataFrame({"info": ["No parsed OMIP data"]}).to_excel(writer, index=False, sheet_name="OMIP")
        else:
            for sheet_name, df_sheet in curves.groupby("sheet"):
                df_sheet.drop(columns=["sort_key"], errors="ignore").to_excel(writer, index=False, sheet_name=str(sheet_name)[:31])
            curves.drop(columns=["sort_key"], errors="ignore").to_excel(writer, index=False, sheet_name="All curves")
        debug.to_excel(writer, index=False, sheet_name="DEBUG")
    return output.getvalue()

# =========================================================
# UI
# =========================================================
section_header("OMIP live forward curve")
st.caption(
    "Uses the same public OMIP page pattern as the working scripts, but avoids pandas.read_html/lxml. "
    "It parses HTML tables with a small built-in parser and falls back to text parsing."
)

left, mid, right = st.columns([1, 1, 1])
with left:
    start_date = st.date_input("Start date", value=date(2026, 2, 3))
    end_date = st.date_input("End date", value=date(2026, 2, 3))
with mid:
    product_label = st.selectbox("Product", list(PRODUCTS.keys()), index=0)
    zone_label = st.selectbox("Zone", list(ZONES.keys()), index=0)
with right:
    maturity_label = st.selectbox("Maturity filter", list(MATURITY_FILTERS.keys()), index=list(MATURITY_FILTERS.keys()).index("Year"))
    curve_choice = st.multiselect(
        "Curves to pull",
        options=["Baseload", "Solar"],
        default=["Baseload", "Solar"],
    )

product = PRODUCTS[product_label]
zone = ZONES[zone_label]
maturity = MATURITY_FILTERS[maturity_label]
instrument_map = {"Baseload": "FTB", "Solar": "FTS"}

example_url = build_omip_url(start_date, product, zone, instrument_map.get(curve_choice[0], "FTB") if curve_choice else "FTB", maturity)
st.markdown(f"[Open OMIP example page]({example_url})")

pull = st.button("Pull OMIP prices", type="primary")

if pull:
    if start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        st.stop()
    if not curve_choice:
        st.warning("Select at least one curve to pull.")
        st.stop()

    all_parts: list[pd.DataFrame] = []
    debug_rows: list[dict] = []
    progress_total = (end_date - start_date).days + 1
    progress = st.progress(0, text="Pulling OMIP data...")

    for i, d in enumerate(daterange(start_date, end_date), start=1):
        for sheet_name in curve_choice:
            instrument = instrument_map[sheet_name]
            try:
                df_day, dbg = fetch_and_parse_one_day(d, product, zone, instrument, maturity, sheet_name)
                debug_rows.append(dbg)
                if not df_day.empty:
                    all_parts.append(df_day)
            except Exception as exc:
                debug_rows.append({
                    "date": d.isoformat(),
                    "sheet": sheet_name,
                    "instrument": instrument,
                    "url": build_omip_url(d, product, zone, instrument, maturity),
                    "tables_found": 0,
                    "rows_parsed": 0,
                    "parser": "error",
                    "error": str(exc),
                })
        progress.progress(i / progress_total, text=f"Pulled {i}/{progress_total} day(s)")

    debug_df = pd.DataFrame(debug_rows)
    curves = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()

    if curves.empty:
        st.warning("OMIP page(s) were reachable or attempted, but no curve rows could be parsed.")
        st.dataframe(debug_df, use_container_width=True)
    else:
        st.success(f"Loaded {len(curves)} OMIP contract rows.")
        chart = build_forward_curve_chart(curves, f"OMIP {zone_label} forward curves | {start_date.isoformat()} to {end_date.isoformat()}")
        if chart is not None:
            st.altair_chart(chart, use_container_width=True)
        display_cols = [c for c in curves.columns if c not in {"sort_key"}]
        st.dataframe(styled_df(curves[display_cols]), use_container_width=True)

    st.download_button(
        "Download OMIP curves + DEBUG as Excel",
        data=dataframe_to_excel_bytes(curves, debug_df),
        file_name=f"omip_{zone}_{product}_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

section_header("Manual upload fallback")
st.caption("Upload a CSV/XLSX copied/exported from OMIP. This parser also avoids lxml.")
file = st.file_uploader("Upload OMIP CSV/XLSX export or copied table", type=["csv", "xlsx", "xls"])
if file is not None:
    try:
        if file.name.lower().endswith(".csv"):
            raw_upload = pd.read_csv(file)
        else:
            raw_upload = pd.read_excel(file)

        # Try both FTB and FTS against the uploaded file.
        parts = []
        for label, instr in {"Baseload": "FTB", "Solar": "FTS"}.items():
            raw_rows = rows_to_structured_df(raw_upload, instr)
            parsed = normalize_omip_contract_rows(raw_rows, instr, maturity, start_date, label, "uploaded_file")
            if not parsed.empty:
                parts.append(parsed)

        upload_curve = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if upload_curve.empty:
            st.warning("Could not identify OMIP curve rows in the uploaded file.")
            st.dataframe(raw_upload, use_container_width=True)
        else:
            chart = build_forward_curve_chart(upload_curve, "Uploaded OMIP curve")
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            st.dataframe(styled_df(upload_curve.drop(columns=["sort_key"], errors="ignore")), use_container_width=True)
    except Exception as exc:
        st.error(f"Could not parse upload: {exc}")
