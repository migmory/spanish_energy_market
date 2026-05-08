import os
import re
from datetime import date
from pathlib import Path

import altair as alt
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

# =========================================================
# ENV / CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Forward Market", layout="wide")

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

EEX_MARKET_DATA_HUB_URL = "https://www.eex.com/en/market-data/market-data-hub"
EEX_DATASOURCE_API_BASE = "https://api1.datasource.eex-group.com"
EEX_FORWARD_LOCAL_CSV = DATA_DIR / "eex_forward_market.csv"
EEX_FORWARD_LOCAL_XLSX = DATA_DIR / "eex_forward_market.xlsx"
EEX_FORWARD_LIVE_CACHE = DATA_DIR / "eex_forward_live_cache.csv"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
GREY_SHADE = "#F3F4F6"
TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"

st.markdown(
    """
    <style>
    html, body, [class*="css"] { font-size: 101% !important; }
    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 101% !important;
    }
    h1 { font-size: 2.0rem !important; }
    h2, h3 { font-size: 1.35rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


def section_header(title: str):
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


def subtle_subsection(title: str):
    st.markdown(
        f"""
        <div style="
            margin-top: 14px;
            margin-bottom: 8px;
            padding: 8px 0 4px 0;
            color: #1F2937;
            font-size: 1.05rem;
            font-weight: 700;
            border-bottom: 1px solid #E5E7EB;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def styled_df(df: pd.DataFrame, pct_cols: list[str] | None = None):
    pct_cols = pct_cols or []
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols]
    fmt = {c: "{:,.2f}" for c in numeric_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})
    styles = [
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
            "props": [("font-size", TABLE_BODY_FONT_PCT), ("padding", "6px 8px")],
        },
    ]
    return df.style.format(fmt).set_table_styles(styles)


def apply_common_chart_style(chart, height: int = 360):
    chart_dict = chart.to_dict()
    styled = chart if any(k in chart_dict for k in ["vconcat", "hconcat", "concat"]) else chart.properties(height=height)
    return (
        styled
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
# EEX helpers
# =========================================================
def _clean_col_name(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")


def _first_existing_col(columns: list[str], candidates: list[str]) -> str | None:
    col_set = set(columns)
    for c in candidates:
        if c in col_set:
            return c
    return None


def _parse_contract_sort_value(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT

    direct = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(direct):
        return pd.Timestamp(direct).normalize()

    s_up = s.upper().replace("_", " ").replace("-", " ").replace("/", " ")

    q_match = re.search(r"Q([1-4]).*?(20\d{2})", s_up) or re.search(r"(20\d{2}).*?Q([1-4])", s_up)
    if q_match:
        g1, g2 = q_match.groups()
        if g1.startswith("20"):
            year = int(g1)
            quarter = int(g2)
        else:
            quarter = int(g1)
            year = int(g2)
        return pd.Timestamp(year, (quarter - 1) * 3 + 1, 1)

    months = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    m_match = re.search(r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).*?(20\d{2}|\d{2})", s_up)
    if m_match:
        mon, yy = m_match.groups()
        year = int(yy) + 2000 if len(yy) == 2 else int(yy)
        return pd.Timestamp(year, months[mon], 1)

    y_match = re.search(r"(20\d{2})", s_up)
    if y_match:
        return pd.Timestamp(int(y_match.group(1)), 1, 1)

    return pd.NaT


def _eex_empty_forward_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "as_of_date", "product", "market_area", "load_type", "contract",
            "contract_sort", "price", "currency", "source",
        ]
    )


def normalize_eex_forward_market_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    cols_out = [
        "as_of_date", "product", "market_area", "load_type", "contract",
        "contract_sort", "price", "currency", "source",
    ]
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=cols_out)

    df = raw_df.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    columns = df.columns.tolist()

    date_col = _first_existing_col(columns, [
        "as_of_date", "trading_day", "trading_date", "trade_date", "business_date",
        "date", "settlement_date", "data_date", "timestamp", "tradedate",
    ])
    product_col = _first_existing_col(columns, [
        "product", "product_name", "instrument", "instrument_name", "contract_name",
        "name", "eex_product", "commodity", "longname", "root",
    ])
    market_col = _first_existing_col(columns, [
        "market_area", "market", "area", "country", "zone", "delivery_area", "hub", "marketarea",
    ])
    load_col = _first_existing_col(columns, [
        "load_type", "load", "profile", "base_peak", "baseload_peakload", "contract_type",
    ])
    contract_col = _first_existing_col(columns, [
        "contract", "delivery_period", "maturity", "maturity_date", "delivery", "delivery_start",
        "period", "expiry", "expiration", "contract_month", "contract_year", "product",
    ])
    price_col = _first_existing_col(columns, [
        "settlement_price", "settlement", "settle", "settle_price", "final_settlement_price",
        "last_price", "last", "price", "close", "closing_price", "px_last", "settlementprice", "lastprice",
    ])
    currency_col = _first_existing_col(columns, ["currency", "ccy", "unitofprices"])

    if contract_col is None or price_col is None:
        return pd.DataFrame(columns=cols_out)

    out = pd.DataFrame()
    out["as_of_date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT
    out["product"] = df[product_col].astype(str).str.strip() if product_col else "EEX forward"
    out["market_area"] = df[market_col].astype(str).str.strip() if market_col else ""
    out["load_type"] = df[load_col].astype(str).str.strip() if load_col else ""
    out["contract"] = df[contract_col].astype(str).str.strip()
    out["price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["currency"] = df[currency_col].astype(str).str.strip() if currency_col else "EUR/MWh"
    out["source"] = "EEX upload/local"

    combined_txt = (out["product"].fillna("") + " " + out["contract"].fillna("")).str.lower()
    out.loc[out["load_type"].isin(["", "nan", "None"]), "load_type"] = ""
    out.loc[out["load_type"].eq("") & combined_txt.str.contains("base|baseload", na=False), "load_type"] = "Baseload"
    out.loc[out["load_type"].eq("") & combined_txt.str.contains("peak|peakload", na=False), "load_type"] = "Peakload"
    out.loc[out["load_type"].eq(""), "load_type"] = "All"

    out["contract_sort"] = out["contract"].map(_parse_contract_sort_value)
    fallback_sort = pd.to_datetime(out["contract"], errors="coerce", dayfirst=True)
    out["contract_sort"] = out["contract_sort"].combine_first(fallback_sort)
    out = out.dropna(subset=["contract", "price"]).copy()
    out = out.sort_values(["product", "market_area", "load_type", "contract_sort", "contract"]).reset_index(drop=True)
    return out[cols_out]


def load_eex_forward_market_file(uploaded_file=None) -> pd.DataFrame:
    try:
        if uploaded_file is not None:
            name = uploaded_file.name.lower()
            raw = pd.read_excel(uploaded_file) if name.endswith((".xlsx", ".xls")) else pd.read_csv(uploaded_file)
            return normalize_eex_forward_market_df(raw)

        if EEX_FORWARD_LOCAL_XLSX.exists():
            return normalize_eex_forward_market_df(pd.read_excel(EEX_FORWARD_LOCAL_XLSX))
        if EEX_FORWARD_LOCAL_CSV.exists():
            return normalize_eex_forward_market_df(pd.read_csv(EEX_FORWARD_LOCAL_CSV))
    except Exception as exc:
        st.warning(f"Could not load local/uploaded EEX file: {exc}")
        return _eex_empty_forward_df()

    return _eex_empty_forward_df()


def _flatten_eex_datasource_json(payload) -> pd.DataFrame:
    rows = []
    if isinstance(payload, dict):
        results = payload.get("results", payload.get("result", payload.get("data", [])))
    else:
        results = payload

    if isinstance(results, dict):
        results = results.get("result", results.get("rows", results.get("data", [])))

    if isinstance(results, list):
        for item in results:
            if isinstance(item, dict) and "result" in item and isinstance(item["result"], list):
                rows.extend([x for x in item["result"] if isinstance(x, dict)])
            elif isinstance(item, dict):
                rows.append(item)
    return pd.DataFrame(rows)


def _read_eex_datasource_credentials() -> tuple[str | None, str | None]:
    user = (os.getenv("EEX_DATASOURCE_USER") or os.getenv("EEX_API_USER") or "").strip()
    password = (os.getenv("EEX_DATASOURCE_PASSWORD") or os.getenv("EEX_API_PASSWORD") or "").strip()
    return (user or None), (password or None)


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_eex_datasource_permissions_cached(user: str, password: str, commodity: str = "POWER") -> pd.DataFrame:
    url = f"{EEX_DATASOURCE_API_BASE}/getPermissions/json"
    resp = requests.get(
        url,
        params={"commodity": commodity.upper()},
        auth=(user, password),
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=(15, 60),
    )
    resp.raise_for_status()
    df = _flatten_eex_datasource_json(resp.json())
    if df.empty:
        return df
    df.columns = [_clean_col_name(c) for c in df.columns]
    return df


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_eex_datasource_derivatives_cached(
    user: str,
    password: str,
    trade_date: str,
    roots_csv: str,
    product_type: str = "futures",
) -> pd.DataFrame:
    roots = [r.strip() for r in str(roots_csv).replace(";", ",").split(",") if r.strip()]
    if not roots:
        return _eex_empty_forward_df()

    frames = []
    for i in range(0, len(roots), 3):
        chunk = roots[i:i + 3]
        url = f"{EEX_DATASOURCE_API_BASE}/getDerivatives/json"
        params = {
            "producttype": product_type.lower(),
            "tradedate": trade_date,
            "returntype": "results",
            "root": ",".join(chunk),
        }
        resp = requests.get(
            url,
            params=params,
            auth=(user, password),
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=(15, 90),
        )
        resp.raise_for_status()
        raw = _flatten_eex_datasource_json(resp.json())
        if not raw.empty:
            frames.append(raw)

    if not frames:
        return _eex_empty_forward_df()

    raw = pd.concat(frames, ignore_index=True)
    raw_renamed = raw.copy()
    rename_map = {}
    for c in raw_renamed.columns:
        cl = _clean_col_name(c)
        if cl == "tradedate":
            rename_map[c] = "trading_date"
        elif cl == "longname":
            rename_map[c] = "product_name"
        elif cl == "marketarea":
            rename_map[c] = "market_area"
        elif cl == "maturity":
            rename_map[c] = "maturity"
        elif cl == "deliveryperiod":
            rename_map[c] = "delivery_period"
        elif cl == "settlementprice":
            rename_map[c] = "settlement_price"
        elif cl == "lastprice":
            rename_map[c] = "last_price"
        elif cl == "unitofprices":
            rename_map[c] = "currency"
        elif cl == "product":
            rename_map[c] = "contract"
        elif cl == "root":
            rename_map[c] = "root"
    raw_renamed = raw_renamed.rename(columns=rename_map)

    if "maturity" in raw_renamed.columns:
        raw_renamed["contract"] = raw_renamed["maturity"].astype(str)
    elif "delivery_period" in raw_renamed.columns:
        raw_renamed["contract"] = raw_renamed["delivery_period"].astype(str)

    out = normalize_eex_forward_market_df(raw_renamed)
    if not out.empty:
        out["source"] = "EEX DataSource API"
        EEX_FORWARD_LIVE_CACHE.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(EEX_FORWARD_LIVE_CACHE, index=False)
    return out


def try_fetch_eex_market_data_hub_public() -> pd.DataFrame:
    """Best-effort HTML table pull.

    The EEX hub is often rendered client-side, so this will frequently return empty.
    The official DataSource API or file export is the robust route.
    """
    try:
        resp = requests.get(
            EEX_MARKET_DATA_HUB_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=(15, 45),
        )
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
    except Exception:
        return _eex_empty_forward_df()

    normalized_frames = []
    for tbl in tables:
        norm = normalize_eex_forward_market_df(tbl)
        if not norm.empty:
            norm["source"] = "EEX Market Data Hub public HTML"
            normalized_frames.append(norm)
    if not normalized_frames:
        return _eex_empty_forward_df()
    return pd.concat(normalized_frames, ignore_index=True).drop_duplicates().sort_values(["contract_sort", "contract"]).reset_index(drop=True)


def load_eex_live_cache() -> pd.DataFrame:
    if not EEX_FORWARD_LIVE_CACHE.exists():
        return _eex_empty_forward_df()
    try:
        df = pd.read_csv(EEX_FORWARD_LIVE_CACHE)
        df["as_of_date"] = pd.to_datetime(df.get("as_of_date"), errors="coerce")
        df["contract_sort"] = pd.to_datetime(df.get("contract_sort"), errors="coerce")
        df["price"] = pd.to_numeric(df.get("price"), errors="coerce")
        return df.dropna(subset=["contract", "price"]).reset_index(drop=True)
    except Exception:
        return _eex_empty_forward_df()


def build_forward_market_chart(forward_df: pd.DataFrame):
    if forward_df.empty:
        return None
    plot = forward_df.copy()
    plot["series"] = plot[["market_area", "load_type"]].fillna("").agg(" | ".join, axis=1).str.strip(" |")
    plot.loc[plot["series"].eq(""), "series"] = plot["product"]
    plot["contract_axis"] = plot["contract"]
    order = plot.sort_values(["contract_sort", "contract"])["contract_axis"].drop_duplicates().tolist()

    chart = alt.Chart(plot).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
        x=alt.X("contract_axis:N", sort=order, axis=alt.Axis(title="Delivery contract", labelAngle=-35)),
        y=alt.Y("price:Q", title="Forward price / settlement (EUR/MWh)"),
        color=alt.Color("series:N", title="Market / load"),
        tooltip=[
            alt.Tooltip("as_of_date:T", title="As of", format="%Y-%m-%d"),
            alt.Tooltip("product:N", title="Product"),
            alt.Tooltip("market_area:N", title="Market area"),
            alt.Tooltip("load_type:N", title="Load type"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("price:Q", title="Price", format=",.2f"),
            alt.Tooltip("currency:N", title="Currency"),
            alt.Tooltip("source:N", title="Source"),
        ],
    ).properties(height=390, title="EEX forward curve")
    return apply_common_chart_style(chart, height=390)


def render_forward_curve(df: pd.DataFrame, key_prefix: str):
    if df.empty:
        st.info("No EEX forward curve loaded yet.")
        return

    products = sorted(df["product"].dropna().astype(str).unique().tolist())
    markets = sorted(df["market_area"].dropna().astype(str).unique().tolist())
    loads = sorted(df["load_type"].dropna().astype(str).unique().tolist())

    c1, c2, c3 = st.columns(3)
    with c1:
        selected_products = st.multiselect("Products", products, default=products[: min(len(products), 3)], key=f"{key_prefix}_products")
    with c2:
        selected_markets = st.multiselect("Market areas", markets, default=markets, key=f"{key_prefix}_markets")
    with c3:
        selected_loads = st.multiselect("Load types", loads, default=loads, key=f"{key_prefix}_loads")

    plot_df = df[
        df["product"].astype(str).isin(selected_products)
        & df["market_area"].astype(str).isin(selected_markets)
        & df["load_type"].astype(str).isin(selected_loads)
    ].copy()

    chart = build_forward_market_chart(plot_df)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    st.dataframe(styled_df(plot_df.drop(columns=["contract_sort"], errors="ignore")), use_container_width=True)


# =========================================================
# PAGE
# =========================================================
st.title("Forward Market - EEX")
section_header("Forward market - EEX")
st.caption(
    "Best-effort public pull + official EEX DataSource API + upload/local file. "
    "For reliable automated forward curves, EEX DataSource credentials are usually required."
)

tab_live, tab_hub, tab_upload = st.tabs(["Live EEX pull", "EEX Market Data Hub", "Upload / local file"])

with tab_live:
    subtle_subsection("Option A - public EEX webpage pull, best effort")
    st.caption(
        "This tries to read tables directly present in the public EEX Market Data Hub HTML. "
        "If the data is loaded in the browser through MarketView, this may return no curve."
    )
    public_df = _eex_empty_forward_df()
    if st.button("Try public EEX Market Data Hub pull", key="try_eex_public_pull"):
        with st.spinner("Trying to read public EEX page..."):
            public_df = try_fetch_eex_market_data_hub_public()
        if public_df.empty:
            st.warning("No usable forward table was found in the public HTML. Use DataSource credentials below or upload an export.")
        else:
            st.success(f"Loaded {len(public_df):,} rows from public EEX HTML.")
            render_forward_curve(public_df, "public")

    subtle_subsection("Option B - official EEX DataSource API")
    st.caption(
        "Add EEX_DATASOURCE_USER and EEX_DATASOURCE_PASSWORD to your .env. "
        "These are the API credentials provided by EEX DataSource, not the webshop login."
    )
    eex_user, eex_password = _read_eex_datasource_credentials()
    api_ok = bool(eex_user and eex_password)
    if not api_ok:
        st.info("No EEX DataSource credentials detected in .env.")
        st.code("EEX_DATASOURCE_USER=EEX_1234\nEEX_DATASOURCE_PASSWORD=your_datasource_password", language="dotenv")
    else:
        st.success("EEX DataSource credentials detected.")

    c_api1, c_api2, c_api3 = st.columns([1, 1, 2])
    with c_api1:
        eex_trade_date = st.date_input("Trade date", value=date.today(), key="eex_trade_date")
    with c_api2:
        eex_product_type = st.selectbox("Product type", ["futures", "options"], index=0, key="eex_product_type")
    with c_api3:
        eex_roots = st.text_input(
            "EEX root codes",
            value="",
            placeholder="e.g. Spanish Power Base/Peak root codes from getPermissions",
            key="eex_roots",
        )

    live_df = _eex_empty_forward_df()
    if api_ok:
        col_perm, col_pull = st.columns([1, 1])
        with col_perm:
            if st.button("Show permissioned POWER roots", key="eex_permissions_btn"):
                try:
                    perms = fetch_eex_datasource_permissions_cached(eex_user, eex_password, "POWER")
                    if perms.empty:
                        st.warning("No permissioned POWER roots returned by EEX.")
                    else:
                        txt = perms.astype(str).agg(" ".join, axis=1).str.lower()
                        spanish = perms[txt.str.contains("span|spain|es", na=False)].copy()
                        st.markdown("Spanish-looking permission rows:" if not spanish.empty else "Permissioned POWER roots:")
                        st.dataframe(styled_df((spanish if not spanish.empty else perms).head(200)), use_container_width=True)
                except Exception as exc:
                    st.error(f"EEX permissions call failed: {exc}")
        with col_pull:
            if st.button("Pull EEX forward curve", key="eex_datasource_pull_btn"):
                if not eex_roots.strip():
                    st.warning("Enter one or more EEX root codes first. Use the permissions button to find the Spanish roots available under your subscription.")
                else:
                    try:
                        with st.spinner("Pulling EEX DataSource getDerivatives..."):
                            live_df = fetch_eex_datasource_derivatives_cached(
                                eex_user,
                                eex_password,
                                eex_trade_date.isoformat(),
                                eex_roots,
                                eex_product_type,
                            )
                        if live_df.empty:
                            st.warning("EEX API returned no usable curve for the selected roots/date.")
                        else:
                            st.success(f"Loaded {len(live_df):,} EEX rows. Cached to data/eex_forward_live_cache.csv.")
                    except Exception as exc:
                        st.error(f"EEX DataSource pull failed: {exc}")

    cached_live = load_eex_live_cache()
    if not cached_live.empty and live_df.empty:
        use_cache = st.checkbox("Use last cached EEX live pull", value=True, key="eex_use_live_cache")
        if use_cache:
            live_df = cached_live

    if not live_df.empty:
        render_forward_curve(live_df, "live")

with tab_hub:
    st.markdown(f"[Open EEX Market Data Hub]({EEX_MARKET_DATA_HUB_URL})")
    st.caption("If the embedded page does not load, open the link above. Some EEX/MarketView pages block embedding for security reasons.")
    try:
        components.html(
            f'<iframe src="{EEX_MARKET_DATA_HUB_URL}" width="100%" height="720" style="border:1px solid #E5E7EB; border-radius:10px;"></iframe>',
            height=740,
            scrolling=True,
        )
    except Exception:
        st.info("The EEX page could not be embedded. Use the link above instead.")

with tab_upload:
    st.markdown(
        "Upload a CSV/XLSX downloaded from EEX Market Data Hub / EEX DataSource. "
        "The dashboard will try to identify contract, settlement/price, product, market and load-type columns automatically."
    )
    uploaded_eex = st.file_uploader("Upload EEX forward curve export (CSV/XLSX)", type=["csv", "xlsx", "xls"], key="eex_forward_upload")
    upload_df = load_eex_forward_market_file(uploaded_eex)
    if upload_df.empty:
        st.info(
            "No EEX forward curve loaded yet. Upload an EEX export here, or save one as "
            "data/eex_forward_market.csv or data/eex_forward_market.xlsx."
        )
    else:
        render_forward_curve(upload_df, "upload")
