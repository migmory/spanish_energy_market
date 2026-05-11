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
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OMIP_HIST_CACHE_FILENAME = "omip_ES_EL_date_range2025_20260511.xlsx"
# These are only fallbacks; the app now infers the actual date coverage from the Excel cache.
OMIP_HIST_CACHE_START = date(2025, 1, 1)
OMIP_HIST_CACHE_END = date(2026, 5, 11)


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
    return re.sub(r"^(?:FTB|FTS|FTP|FWB|SWB)\s+", "", c).replace("-", "") or c


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



def find_col_by_name(df: pd.DataFrame, patterns: list[str], exact: bool = False) -> str | None:
    """Find first column whose normalised name matches one of the patterns."""
    pats = [p.lower() for p in patterns]
    for col in df.columns:
        name = normalize_str(col).lower()
        if exact:
            if name in pats:
                return col
        else:
            if any(p in name for p in pats):
                return col
    return None


def parse_market_date_value(value, fallback: date | None = None) -> date | None:
    if value is None or pd.isna(value):
        return fallback
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.notna(ts):
            return ts.date()
    except Exception:
        pass
    return fallback


def instrument_from_contract_or_sheet(contract: str, sheet_name: str | None = None) -> tuple[str | None, str | None]:
    c = normalize_str(contract)
    if c.startswith("FTS"):
        return "FTS", "Solar"
    if c.startswith("FTB"):
        return "FTB", "Baseload"
    if c.startswith("FTP"):
        return "FTP", "Peak"
    if c.startswith("FWB"):
        return "FWB", "Base Forward"
    if c.startswith("SWB"):
        return "SWB", "Base Swap"
    sh = (sheet_name or "").lower()
    if "solar" in sh:
        return "FTS", "Solar"
    if "base" in sh:
        return "FTB", "Baseload"
    return None, None


def parse_direct_omip_contract_sheet(df_in: pd.DataFrame, sheet_name: str, fallback_market_date: date | None = None) -> pd.DataFrame:
    """Parse Excel sheets produced by OMIP_range_dataEXTRACT.py or previous app exports.

    Supports both:
    - normalised range sheets with columns like date, sheet, instrument, contract_name, D, D-1;
    - parsed app exports with columns like market_date, sheet, instrument, contract, curve_price;
    - raw OMIP sheets where the first contract column contains FTB/FTS rows.
    """
    if df_in is None or df_in.empty:
        return pd.DataFrame()

    df = flatten_columns(df_in)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if df.empty:
        return pd.DataFrame()

    # Ignore info/debug-only sheets.
    joined_cols = " ".join([normalize_str(c).lower() for c in df.columns])
    if sheet_name.lower().startswith("debug") or ("info" in joined_cols and len(df.columns) <= 2):
        return pd.DataFrame()

    contract_col = find_col_by_name(df, ["contract_name", "contract name", "contract"], exact=False)
    if contract_col is None:
        # Fallback: find a column that contains FTB/FTS-like values.
        for col in df.columns:
            sample = df[col].astype(str).map(normalize_str)
            if sample.str.contains(r"^(?:FTB|FTS|FTP|FWB|SWB)\s+", regex=True, na=False).any():
                contract_col = col
                break
    if contract_col is None:
        return pd.DataFrame()

    date_col = find_col_by_name(df, ["market_date", "market date", "date"], exact=False)
    sheet_col = find_col_by_name(df, ["sheet", "curve"], exact=True)
    instr_col = find_col_by_name(df, ["instrument"], exact=True)

    bid_col = find_col_by_name(df, ["best bid"], exact=False)
    ask_col = find_col_by_name(df, ["best ask", "best offer"], exact=False)
    last_col = find_col_by_name(df, ["last price"], exact=False)
    oi_col = find_col_by_name(df, ["open interest"], exact=False)
    nr_col = find_col_by_name(df, ["nr of contracts", "contracts"], exact=False)
    otc_col = find_col_by_name(df, ["otc volume"], exact=False)
    sess_col = find_col_by_name(df, ["session volume"], exact=False)

    # Prefer exact D / D-1 style columns, then looser matches.
    d_col = None
    d1_col = None
    for col in df.columns:
        nm = normalize_str(col).lower()
        if nm in {"d", "d (€/mwh)", "d eur/mwh", "d €/mwh"}:
            d_col = col
        if nm in {"d-1", "d-1 (€/mwh)", "d-1 eur/mwh", "d-1 €/mwh"} or "d-1" in nm:
            d1_col = col
    if d_col is None:
        d_col = find_col_by_name(df, ["reference_price", "reference price", "d_price", "d price", "settlement"], exact=False)
    if d1_col is None:
        d1_col = find_col_by_name(df, ["d_minus_1", "d minus 1"], exact=False)
    curve_col = find_col_by_name(df, ["curve_price", "curve price"], exact=False)

    rows = []
    for _, row in df.iterrows():
        contract = normalize_str(row.get(contract_col))
        if not re.match(r"^(?:FTB|FTS|FTP|FWB|SWB)\s+", contract):
            # Raw OMIP first column may include long metadata and the contract at the end.
            maybe_instr = normalize_str(row.get(instr_col)) if instr_col else None
            instr_guess = maybe_instr if maybe_instr in {"FTB", "FTS", "FTP", "FWB", "SWB"} else None
            if instr_guess:
                maybe = extract_contract_name(contract, instr_guess)
                contract = maybe or contract
        if not re.match(r"^(?:FTB|FTS|FTP|FWB|SWB)\s+", contract):
            continue

        instr, default_sheet = instrument_from_contract_or_sheet(contract, sheet_name)
        if instr_col and normalize_str(row.get(instr_col)) in {"FTB", "FTS", "FTP", "FWB", "SWB"}:
            instr = normalize_str(row.get(instr_col))
        sheet_value = normalize_str(row.get(sheet_col)) if sheet_col else ""
        sheet_value = sheet_value or default_sheet or sheet_name

        market_date = parse_market_date_value(row.get(date_col) if date_col else None, fallback=fallback_market_date)
        if market_date is None:
            # The date can sometimes be encoded in the file/sheet context; if unavailable, keep today's date.
            market_date = date.today()

        best_bid = parse_num(row.get(bid_col)) if bid_col else None
        best_ask = parse_num(row.get(ask_col)) if ask_col else None
        last_price = parse_num(row.get(last_col)) if last_col else None
        open_interest = parse_num(row.get(oi_col)) if oi_col else None
        nr_contracts = parse_num(row.get(nr_col)) if nr_col else None
        otc_volume = parse_num(row.get(otc_col)) if otc_col else None
        session_volume = parse_num(row.get(sess_col)) if sess_col else None
        d_price = parse_num(row.get(d_col)) if d_col else None
        d_minus_1 = parse_num(row.get(d1_col)) if d1_col else None
        curve_price = parse_num(row.get(curve_col)) if curve_col else None
        if curve_price is None:
            curve_price = d_price
        if curve_price is None:
            curve_price = last_price
        if curve_price is None and best_bid is not None and best_ask is not None:
            curve_price = (best_bid + best_ask) / 2
        if curve_price is None:
            curve_price = d_minus_1
        if curve_price is None:
            continue

        rows.append({
            "market_date": market_date,
            "sheet": sheet_value,
            "instrument": instr,
            "table_id": parse_num(row.get("table_id")) if "table_id" in df.columns else None,
            "contract": contract,
            "maturity": contract_maturity(contract),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "session_volume_mwh": session_volume,
            "last_price": last_price,
            "last_time": normalize_str(row.get("Last time")) if "Last time" in df.columns else None,
            "last_volume_mwh": None,
            "open_interest": open_interest,
            "nr_contracts": nr_contracts,
            "otc_volume_mwh": otc_volume,
            "d_price": d_price,
            "d_minus_1": d_minus_1,
            "curve_price": curve_price,
            "raw_contract_cell": contract,
            "source": "historical_cache" if fallback_market_date is None else "excel_upload",
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["market_date"] = pd.to_datetime(out["market_date"], errors="coerce").dt.date
    out["sort_key"] = out["contract"].map(contract_sort_key)
    out["delivery_label"] = out["contract"].map(delivery_label_from_contract)
    out = out.drop_duplicates(subset=["market_date", "sheet", "instrument", "contract"], keep="last")
    return out.sort_values(["market_date", "sheet", "sort_key", "contract"]).reset_index(drop=True)


def parse_normalised_all_curves_sheet(df_in: pd.DataFrame, source_label: str = "historical_cache") -> pd.DataFrame:
    """Fast path for the cached Excel uploaded to /data.

    The file omip_ES_EL_date_range2025_20260511.xlsx already has a normalised
    'All curves' sheet with columns such as market_date, sheet, instrument,
    contract, maturity, d_price, d_minus_1, curve_price, sort_key, delivery_label.
    Reading this sheet directly is much faster and avoids reparsing raw OMIP tables.
    """
    if df_in is None or df_in.empty:
        return pd.DataFrame()
    df = df_in.copy()
    df.columns = [normalize_str(c) for c in df.columns]
    required = {"market_date", "sheet", "instrument", "contract", "curve_price"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame()

    df["market_date"] = pd.to_datetime(df["market_date"], errors="coerce").dt.date
    df["sheet"] = df["sheet"].astype(str).map(normalize_str)
    df["instrument"] = df["instrument"].astype(str).map(normalize_str)
    df["contract"] = df["contract"].astype(str).map(normalize_str)

    numeric_cols = [
        "best_bid", "best_ask", "session_volume_mwh", "last_price", "last_volume_mwh",
        "open_interest", "nr_contracts", "otc_volume_mwh", "d_price", "d_minus_1",
        "curve_price", "sort_key",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "maturity" not in df.columns:
        df["maturity"] = df["contract"].map(contract_maturity)
    else:
        df["maturity"] = df["maturity"].astype(str).map(normalize_str)

    if "sort_key" not in df.columns:
        df["sort_key"] = df["contract"].map(contract_sort_key)
    if "delivery_label" not in df.columns:
        df["delivery_label"] = df["contract"].map(delivery_label_from_contract)
    else:
        df["delivery_label"] = df["delivery_label"].astype(str).map(normalize_str)

    for col in [
        "table_id", "best_bid", "best_ask", "session_volume_mwh", "last_price", "last_time",
        "last_volume_mwh", "open_interest", "nr_contracts", "otc_volume_mwh", "d_price",
        "d_minus_1",
    ]:
        if col not in df.columns:
            df[col] = None

    df["source"] = source_label
    out_cols = [
        "market_date", "sheet", "instrument", "table_id", "contract", "maturity",
        "best_bid", "best_ask", "session_volume_mwh", "last_price", "last_time",
        "last_volume_mwh", "open_interest", "nr_contracts", "otc_volume_mwh",
        "d_price", "d_minus_1", "curve_price", "sort_key", "delivery_label", "source",
    ]
    out = df[[c for c in out_cols if c in df.columns]].copy()
    out = out.dropna(subset=["market_date", "contract", "curve_price"])
    out = out[out["contract"].str.contains(r"^(?:FTB|FTS|FTP|FWB|SWB)\s+", regex=True, na=False)].copy()
    out = out.drop_duplicates(subset=["market_date", "sheet", "instrument", "contract"], keep="last")
    return out.sort_values(["market_date", "sheet", "sort_key", "contract"]).reset_index(drop=True)


def parse_omip_excel_file(path_or_file, default_market_date: date | None = None) -> pd.DataFrame:
    """Read an OMIP Excel cache/export and return the normalised contract dataset."""
    try:
        xls = pd.ExcelFile(path_or_file)
    except Exception:
        return pd.DataFrame()

    # Fast path for the cache file currently stored in /data. It has a ready-made
    # normalised All curves sheet, so do not parse the Baseload/Solar sheets again.
    if "All curves" in xls.sheet_names:
        try:
            all_curves = pd.read_excel(xls, sheet_name="All curves")
            parsed_all = parse_normalised_all_curves_sheet(
                all_curves,
                source_label="historical_cache" if default_market_date is None else "excel_upload",
            )
            if not parsed_all.empty:
                return parsed_all
        except Exception:
            pass

    parts = []
    for sheet in xls.sheet_names:
        try:
            raw = pd.read_excel(xls, sheet_name=sheet)
        except Exception:
            continue
        parsed = parse_direct_omip_contract_sheet(raw, sheet, fallback_market_date=default_market_date)
        if not parsed.empty:
            parts.append(parsed)
            continue

        # Fallback for Excel generated with concat_tables where headers may be shifted.
        try:
            raw_no_header = pd.read_excel(xls, sheet_name=sheet, header=None)
            instrument = "FTS" if "solar" in sheet.lower() else "FTB" if "base" in sheet.lower() else None
            if instrument is None:
                joined = " ".join(raw_no_header.astype(str).fillna("").head(20).values.ravel().tolist())
                instrument = "FTS" if "FTS" in joined else "FTB" if "FTB" in joined else None
            if instrument is not None:
                fallback = parse_raw_tables_to_contracts([raw_no_header], default_market_date or date.today(), sheet, instrument)
                if not fallback.empty:
                    fallback["source"] = "historical_cache" if default_market_date is None else "excel_upload"
                    parts.append(fallback)
        except Exception:
            pass

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def find_historical_omip_cache_files(product: str, zone: str) -> list[Path]:
    """Find OMIP historical cache files under /data.

    Primary expected file:
        data/omip_ES_EL_date_range2025_20260511.xlsx

    The function still has flexible fallbacks, but it always prefers the exact
    cache filename if present.
    """
    if not DATA_DIR.exists():
        return []

    preferred = DATA_DIR / OMIP_HIST_CACHE_FILENAME
    files: list[Path] = []
    if preferred.exists():
        # This is the vetted historical cache file. Use it alone so a one-day
        # debug/export workbook in /data does not contaminate the historical range.
        return [preferred]

    patterns = [
        f"*omip*{zone}*{product}*.xlsx",
        f"*OMIP*{zone}*{product}*.xlsx",
        "*omip*.xlsx",
        "*OMIP*.xlsx",
    ]
    for pat in patterns:
        files.extend(DATA_DIR.glob(pat))

    # De-duplicate, keep the exact preferred file first, then date_range files.
    unique = []
    seen = set()
    for f in files:
        if f not in seen:
            unique.append(f)
            seen.add(f)
    unique = sorted(
        unique,
        key=lambda x: (x.name != OMIP_HIST_CACHE_FILENAME, "date_range" not in x.name.lower(), x.name.lower()),
    )
    return unique


@st.cache_data(show_spinner=False, ttl=1800)
def load_historical_omip_cache(product: str, zone: str) -> pd.DataFrame:
    files = find_historical_omip_cache_files(product, zone)
    parts = []
    for f in files:
        parsed = parse_omip_excel_file(f)
        if not parsed.empty:
            parsed["source_file"] = f.name
            parts.append(parsed)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out["market_date"] = pd.to_datetime(out["market_date"], errors="coerce").dt.date
    out = out.dropna(subset=["market_date", "contract", "curve_price"])
    return out.drop_duplicates(subset=["market_date", "sheet", "instrument", "contract"], keep="last").reset_index(drop=True)


def fetch_and_parse_range_hybrid(start_date: date, end_date: date, product: str, zone: str, instruments: dict[str, str], include_maturity_param: bool, maturity_param: str | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use /data historical cache first, then fetch only missing dates from OMIP.

    The cache coverage is inferred from the Excel itself. With the current file
    omip_ES_EL_date_range2025_20260511.xlsx, this means 2025-01-01 to 2026-05-11.
    """
    selected_instr_codes = set(instruments.values())
    parts = []
    debug_parts = []

    cache = load_historical_omip_cache(product, zone)
    cache_used = pd.DataFrame()
    cache_start = None
    cache_end = None

    if not cache.empty:
        cache_start = min(cache["market_date"])
        cache_end = max(cache["market_date"])
        cache_tmp = cache.copy()
        cache_tmp = cache_tmp[
            (cache_tmp["market_date"] >= start_date)
            & (cache_tmp["market_date"] <= end_date)
            & (cache_tmp["instrument"].isin(selected_instr_codes))
            & (cache_tmp["market_date"] >= cache_start)
            & (cache_tmp["market_date"] <= cache_end)
        ].copy()
        if not cache_tmp.empty:
            cache_tmp["source"] = "historical_cache"
            parts.append(cache_tmp)
            cache_used = cache_tmp

    fetch_ranges: list[tuple[date, date]] = []
    if cache.empty or cache_start is None or cache_end is None:
        fetch_ranges.append((start_date, end_date))
    else:
        if start_date < cache_start:
            fetch_ranges.append((start_date, min(end_date, cache_start - timedelta(days=1))))
        if end_date > cache_end:
            fetch_ranges.append((max(start_date, cache_end + timedelta(days=1)), end_date))

    for a, b in fetch_ranges:
        if a <= b:
            data_live, debug_live = fetch_and_parse_range(a, b, product, zone, instruments, include_maturity_param, maturity_param)
            if not data_live.empty:
                data_live["source"] = "web_pull"
                parts.append(data_live)
            if not debug_live.empty:
                debug_parts.append(debug_live)

    cache_files = find_historical_omip_cache_files(product, zone)
    debug_rows = [{
        "date": f"{start_date.isoformat()} to {end_date.isoformat()}",
        "sheet": "historical_cache",
        "instrument": ",".join(sorted(selected_instr_codes)),
        "url": ", ".join([p.name for p in cache_files]) or "No cache file found in /data",
        "tables_found": None,
        "raw_rows": int(len(cache)) if not cache.empty else 0,
        "rows_parsed": int(len(cache_used)),
        "cache_start": cache_start.isoformat() if cache_start else None,
        "cache_end": cache_end.isoformat() if cache_end else None,
        "web_fetch_ranges": "; ".join([f"{a.isoformat()} to {b.isoformat()}" for a, b in fetch_ranges]),
        "error": "",
    }]
    debug_cache = pd.DataFrame(debug_rows)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    debug = pd.concat([debug_cache] + debug_parts, ignore_index=True) if debug_parts else debug_cache
    if not out.empty:
        out = out.drop_duplicates(subset=["market_date", "sheet", "instrument", "contract"], keep="last")
        out = out.sort_values(["market_date", "sheet", "sort_key", "contract"]).reset_index(drop=True)
    return out, debug

def parse_working_excel_upload(file) -> pd.DataFrame:
    parsed = parse_omip_excel_file(file, default_market_date=date.today())
    if not parsed.empty:
        parsed["source"] = "excel_upload"
    return parsed


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
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=True)),
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
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=True)),
        tooltip=[
            alt.Tooltip("market_date:T", title="Market date", format="%Y-%m-%d"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Curve price €/MWh", format=",.2f"),
            alt.Tooltip("d_price:Q", title="D €/MWh", format=",.2f"),
            alt.Tooltip("d_minus_1:Q", title="D-1 €/MWh", format=",.2f"),
        ],
    ).properties(title=title, height=380)
    return apply_common_chart_style(chart, height=380)




def build_time_evolution_by_delivery_chart(df: pd.DataFrame, delivery_label: str, title: str):
    """Plot Baseload and Solar price evolution over market_date for the same delivery period.

    Example: delivery_label='YR27' overlays FTB YR-27 and FTS YR-27 on the same x-axis.
    """
    if df.empty or not delivery_label:
        return None
    plot = df[df["delivery_label"] == delivery_label].copy()
    if plot.empty:
        return None

    plot["series"] = plot["sheet"].astype(str)
    color_scale = alt.Scale(
        domain=["Baseload", "Solar"],
        range=[BLUE_PRICE, YELLOW_PRICE],
    )
    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("market_date:T", title="Market date", axis=alt.Axis(format="%d-%b", labelAngle=0)),
        y=alt.Y("curve_price:Q", title="€/MWh", scale=alt.Scale(zero=True)),
        color=alt.Color("series:N", title=None, scale=color_scale),
        tooltip=[
            alt.Tooltip("market_date:T", title="Market date", format="%Y-%m-%d"),
            alt.Tooltip("sheet:N", title="Curve"),
            alt.Tooltip("delivery_label:N", title="Delivery period"),
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
    "In date-range mode, choose one delivery period (for example YR28) and the chart overlays Baseload and Solar. "
    "For date ranges, the app uses the OMIP Excel cache in /data first (currently omip_ES_EL_date_range2025_20260511.xlsx) and only pulls dates outside the cache from the web."
)

c1, c2, c3 = st.columns(3)
with c1:
    mode = st.radio("Mode", ["Single date", "Date range"], horizontal=True)

    if mode == "Single date":
        asof = st.date_input(
            "Market date (OMIP published curve date)",
            value=date.today(),
            key="omip_single_market_date",
        )
        start_date = None
        end_date = None
        st.caption("Market date = date used in the OMIP URL to retrieve the published forward curve, not the delivery year.")
    else:
        asof = None
        start_date = st.date_input(
            "Start market date",
            value=date.today() - timedelta(days=7),
            key="omip_range_start_date",
        )
        end_date = st.date_input(
            "End market date",
            value=date.today(),
            key="omip_range_end_date",
        )
        st.caption("Start/end market dates = publication dates used in the OMIP URL range.")

with c2:
    product_label = st.selectbox("Product", list(PRODUCTS.keys()), index=0)
    zone_label = st.selectbox("Zone", list(ZONES.keys()), index=0)
    maturity_label = st.selectbox("Maturity filter", list(MATURITY_FILTERS.keys()), index=list(MATURITY_FILTERS.keys()).index("Year"))
with c3:
    curve_choice = st.multiselect("Curves", ["Baseload", "Solar"], default=["Baseload", "Solar"])
    include_maturity_param = st.checkbox(
        "Send maturity parameter to OMIP URL",
        value=False,
        help="Off by default because the scripts that work do not send maturity; they filter after reading the page.",
    )
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
link_date = asof if mode == "Single date" else start_date
if link_date is not None:
    st.markdown(
        f"[Open OMIP page]({omip_url(link_date.strftime('%Y-%m-%d'), product, zone, sample_instrument, include_maturity_param, maturity_param)})"
    )

# Persist the latest pulled dataset in session_state. Without this, selecting a different
# delivery period triggers a Streamlit rerun and the chart disappears because the previous
# pull result only existed inside the button-click run.
if pull:
    if not selected_instruments:
        st.warning("Select at least one curve.")
    elif mode == "Date range" and (start_date is None or end_date is None or start_date > end_date):
        st.warning("Please select a valid date range.")
    else:
        if mode == "Single date":
            data, debug, raw_tables = fetch_and_parse_day(asof, product, zone, selected_instruments, include_maturity_param, maturity_param)
            result_label = f"Market date {asof.isoformat()}"
        else:
            data, debug = fetch_and_parse_range_hybrid(start_date, end_date, product, zone, selected_instruments, include_maturity_param, maturity_param)
            raw_tables = {}
            result_label = f"Market dates {start_date.isoformat()} to {end_date.isoformat()}"

        if not data.empty:
            data = filter_maturity(data, maturity_filter)

        st.session_state["omip_forward_result"] = {
            "data": data,
            "debug": debug,
            "raw_tables": raw_tables,
            "mode": mode,
            "zone_label": zone_label,
            "zone": zone,
            "product": product,
            "maturity_label": maturity_label,
            "result_label": result_label,
            "pulled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

result = st.session_state.get("omip_forward_result")
if result is not None:
    data = result.get("data", pd.DataFrame())
    debug = result.get("debug", pd.DataFrame())
    raw_tables = result.get("raw_tables", {})
    result_mode = result.get("mode", mode)
    result_zone_label = result.get("zone_label", zone_label)
    result_zone = result.get("zone", zone)
    result_product = result.get("product", product)
    result_maturity_label = result.get("maturity_label", maturity_label)
    result_label = result.get("result_label", "")

    if data.empty:
        st.warning("OMIP page(s) were reachable or attempted, but no curve rows could be parsed for the selected filters.")
        if debug is not None and not debug.empty:
            st.dataframe(debug, use_container_width=True)
        st.download_button(
            "Download debug Excel",
            data=dataframe_to_excel_bytes(data, debug, raw_tables),
            file_name=f"omip_debug_{result_zone}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.success(f"Loaded {len(data)} OMIP rows. Latest pull: {result_label}.")
        if result_mode == "Single date":
            chart = build_curve_chart(
                data,
                f"OMIP {result_zone_label} forward curve | {result_label} | {result_maturity_label}",
            )
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
        else:
            delivery_options = (
                data[["delivery_label", "sort_key"]]
                .drop_duplicates()
                .sort_values("sort_key")["delivery_label"]
                .tolist()
            )
            if delivery_options:
                selected_delivery = st.selectbox(
                    "Delivery period to plot over time",
                    delivery_options,
                    key="omip_delivery_period_over_time",
                )
                chart = build_time_evolution_by_delivery_chart(
                    data,
                    selected_delivery,
                    f"OMIP {result_zone_label} {selected_delivery} evolution | Baseload vs Solar",
                )
                if chart is not None:
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.warning("No data available for the selected delivery period.")
            else:
                st.warning("No delivery periods found in the pulled dataset.")

        display_cols = [
            "market_date", "sheet", "instrument", "delivery_label", "contract", "maturity", "curve_price",
            "d_price", "d_minus_1", "best_bid", "best_ask", "last_price", "open_interest",
            "nr_contracts", "otc_volume_mwh", "session_volume_mwh", "source", "source_file",
        ]
        st.dataframe(styled_df(data[[c for c in display_cols if c in data.columns]]), use_container_width=True)
        with st.expander("Debug"):
            if debug is not None and not debug.empty:
                st.dataframe(debug, use_container_width=True)
            else:
                st.info("No debug rows for the latest pull.")
        st.download_button(
            "Download OMIP parsed Excel",
            data=dataframe_to_excel_bytes(data, debug, raw_tables),
            file_name=f"omip_{result_zone}_{result_product}_{result_mode.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Select the OMIP parameters and click Pull OMIP prices.")

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
