import os
import re
import stat
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

try:
    import paramiko
except Exception:
    paramiko = None

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="MIBGAS", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
LOCAL_FILE_PATTERN = "MIBGAS_Data_*.xlsx"
LOCAL_START_YEAR = 2021
LOCAL_END_YEAR = 2025
LIVE_YEAR = 2026
CACHE_FILE = DATA_DIR / "mibgas_2026_cache.csv"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
GREY_SHADE = "#F3F4F6"

TARGET_SHEET = "Trading Data PVB&VTP"

# =========================================================
# STYLE / HELPERS
# =========================================================
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


def normalize_col_name(col) -> str:
    if pd.isna(col):
        return ""
    s = str(col)
    s = s.replace("\xa0", " ").replace("\n", " ").strip().lower()
    repl = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "[": "", "]": "", "(": "", ")": "", "%": "pct", "/": "_", "-": "_", ".": "_",
    }
    for a, b in repl.items():
        s = s.replace(a, b)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_col_name(c) for c in out.columns]
    return out


def to_number(series: pd.Series) -> pd.Series:
    # Handles both already numeric columns and Spanish-formatted text numbers.
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    s = series.astype(str).str.strip()
    s = s.str.replace("€", "", regex=False)
    s = s.str.replace(" ", "", regex=False)
    s = s.str.replace("\xa0", "", regex=False)
    # If both thousand dot and decimal comma exist: 1.234,56 -> 1234.56
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        nc = normalize_col_name(c)
        if nc in cols:
            return nc
    return None

# =========================================================
# LOCAL EXCEL LOADING
# =========================================================
def read_mibgas_excel(path: Path) -> pd.DataFrame:
    """Read the relevant MIBGAS sheet only."""
    try:
        xls = pd.ExcelFile(path)
    except Exception as e:
        raise ValueError(f"cannot open Excel: {e}")

    # Prefer the correct sheet. Fall back to any sheet containing PVB&VTP.
    sheet = None
    if TARGET_SHEET in xls.sheet_names:
        sheet = TARGET_SHEET
    else:
        for s in xls.sheet_names:
            if "PVB" in str(s).upper() and "VTP" in str(s).upper():
                sheet = s
                break
    if sheet is None:
        raise ValueError(f"sheet '{TARGET_SHEET}' not found. Available sheets: {xls.sheet_names}")

    df = pd.read_excel(path, sheet_name=sheet)
    df = clean_columns(df)
    df["source_file"] = f"{path.name}/{sheet}"
    return standardize_raw_mibgas(df)


def standardize_raw_mibgas(df: pd.DataFrame) -> pd.DataFrame:
    """Return a standardized raw trading dataframe."""
    if df.empty:
        return pd.DataFrame()

    # Some SFTP files may arrive with a first blank row/header issue. Try to repair if product/trading_day missing.
    if "product" not in df.columns or "trading_day" not in df.columns:
        # Try using first row as header if it looks like header text.
        maybe = df.copy()
        if len(maybe) > 0:
            new_cols = [normalize_col_name(x) for x in maybe.iloc[0].tolist()]
            if "product" in new_cols and "trading_day" in new_cols:
                maybe = maybe.iloc[1:].copy()
                maybe.columns = new_cols
                if "source_file" not in maybe.columns and "source_file" in df.columns:
                    maybe["source_file"] = df["source_file"].iloc[0]
                df = maybe

    colmap = {
        "trading_day": first_col(df, ["Trading day", "trading_day"]),
        "product": first_col(df, ["Product", "product"]),
        "area": first_col(df, ["Area", "area"]),
        "place_of_delivery": first_col(df, ["Place of delivery", "place_of_delivery"]),
        "delivery_start": first_col(df, ["First Day Delivery", "first_day_delivery"]),
        "delivery_end": first_col(df, ["Last Day Delivery", "last_day_delivery"]),
        "reference_price": first_col(df, ["Reference Price [EUR/MWh]", "Daily Reference Price [EUR/MWh]", "reference_price_eur_mwh", "daily_reference_price_eur_mwh"]),
        "auction_price": first_col(df, ["Auction Price [EUR/MWh]", "Daily Auction Price [EUR/MWh]"]),
        "last_price": first_col(df, ["Last Price [EUR/MWh]", "Last Daily Price [EUR/MWh]"]),
        "eod_price": first_col(df, ["EOD Price [EUR/MWh]", "EOD Price"]),
        "bid": first_col(df, ["Bid [EUR/MWh]", "Bid"]),
        "ask": first_col(df, ["Ask [EUR/MWh]", "Ask"]),
        "volume": first_col(df, ["Volume Traded [MWh]", "Daily Volume Traded [MWh]", "Volume", "MWh"]),
    }

    required = ["trading_day", "product"]
    missing = [k for k in required if colmap[k] is None]
    if missing:
        raise ValueError(f"missing required columns {missing}. Columns found: {df.columns.tolist()}")

    out = pd.DataFrame()
    out["trading_day"] = pd.to_datetime(df[colmap["trading_day"]], dayfirst=True, errors="coerce")
    out["product"] = df[colmap["product"]].astype(str).str.strip()
    out["area"] = df[colmap["area"]].astype(str).str.strip() if colmap["area"] else None
    out["place_of_delivery"] = df[colmap["place_of_delivery"]].astype(str).str.strip() if colmap["place_of_delivery"] else None
    out["delivery_start"] = pd.to_datetime(df[colmap["delivery_start"]], dayfirst=True, errors="coerce") if colmap["delivery_start"] else pd.NaT
    out["delivery_end"] = pd.to_datetime(df[colmap["delivery_end"]], dayfirst=True, errors="coerce") if colmap["delivery_end"] else pd.NaT

    for out_col, key in [
        ("reference_price_eur_mwh", "reference_price"),
        ("auction_price_eur_mwh", "auction_price"),
        ("last_price_eur_mwh", "last_price"),
        ("eod_price_eur_mwh", "eod_price"),
        ("bid_eur_mwh", "bid"),
        ("ask_eur_mwh", "ask"),
        ("volume_traded_mwh", "volume"),
    ]:
        if colmap[key]:
            out[out_col] = to_number(df[colmap[key]])
        else:
            out[out_col] = pd.NA

    if "source_file" in df.columns:
        out["source_file"] = df["source_file"].astype(str)
    else:
        out["source_file"] = "unknown"

    out = out.dropna(subset=["trading_day"])
    out = out[out["product"].notna() & (out["product"].str.lower() != "nan")]
    return out.reset_index(drop=True)


@st.cache_data(show_spinner=True)
def load_local_history() -> tuple[pd.DataFrame, pd.DataFrame]:
    files = sorted(DATA_DIR.glob(LOCAL_FILE_PATTERN))
    logs = []
    frames = []

    for path in files:
        # Optional filter by year in filename, but keep if no year is found.
        m = re.search(r"(20\d{2})", path.name)
        if m:
            y = int(m.group(1))
            if y < LOCAL_START_YEAR or y > LOCAL_END_YEAR:
                continue
        try:
            df = read_mibgas_excel(path)
            frames.append(df)
            logs.append({"file": path.name, "status": "OK", "rows": len(df), "message": ""})
        except Exception as e:
            logs.append({"file": path.name, "status": "ERROR", "rows": 0, "message": str(e)})

    if frames:
        out = pd.concat(frames, ignore_index=True)
        out = out[out["trading_day"].dt.year.between(LOCAL_START_YEAR, LOCAL_END_YEAR)]
        out = out.drop_duplicates(subset=["trading_day", "product", "area"], keep="last")
        out = out.sort_values(["trading_day", "product"]).reset_index(drop=True)
    else:
        out = pd.DataFrame()

    return out, pd.DataFrame(logs)

# =========================================================
# SFTP LIVE 2026
# =========================================================
def get_secret(name: str, default=None):
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


def load_private_key():
    if paramiko is None:
        raise ValueError("paramiko is not installed. Add 'paramiko' to requirements.txt.")
    key_text = get_secret("MIBGAS_SFTP_KEY")
    if not key_text:
        return None
    key_file = StringIO(str(key_text))
    last_error = None
    key_loaders = []
    for key_name in ["Ed25519Key", "RSAKey", "ECDSAKey", "DSSKey"]:
        loader = getattr(paramiko, key_name, None)
        if loader is not None:
            key_loaders.append(loader)
    for loader in key_loaders:
        try:
            key_file.seek(0)
            return loader.from_private_key(key_file)
        except Exception as e:
            last_error = e
    raise ValueError(f"Could not load private key from Streamlit Secrets: {last_error}")


def connect_sftp():
    if paramiko is None:
        raise ValueError("paramiko is not installed. Add 'paramiko' to requirements.txt.")

    host = get_secret("MIBGAS_SFTP_HOST", "secureftpbucket.omie.es")
    port = int(get_secret("MIBGAS_SFTP_PORT", 22))
    user = get_secret("MIBGAS_SFTP_USER")
    password = get_secret("MIBGAS_SFTP_PASSWORD")
    key = load_private_key()

    if not user:
        raise ValueError("MIBGAS_SFTP_USER is missing in Streamlit Secrets.")
    if key is None and not password:
        raise ValueError("MIBGAS_SFTP_KEY or MIBGAS_SFTP_PASSWORD is missing in Streamlit Secrets.")

    transport = paramiko.Transport((host, port))
    if key is not None:
        transport.connect(username=user, pkey=key)
    else:
        transport.connect(username=user, password=password)
    return paramiko.SFTPClient.from_transport(transport), transport


def sftp_file_exists(sftp, path: str) -> bool:
    try:
        attr = sftp.stat(path)
        return stat.S_ISREG(attr.st_mode)
    except Exception:
        return False


def sftp_dir_exists(sftp, path: str) -> bool:
    try:
        attr = sftp.stat(path)
        return stat.S_ISDIR(attr.st_mode)
    except Exception:
        return False


def find_year_dir(sftp, year: int) -> str:
    configured = str(get_secret("MIBGAS_SFTP_BASE_PATH", "/MIBGAS")).rstrip("/")
    candidates = [
        f"{configured}/AGNO_{year}",
        f"/MIBGAS/AGNO_{year}",
        f"MIBGAS/AGNO_{year}",
        f"/secureftpbucket.omie.es/MIBGAS/AGNO_{year}",
        f"secureftpbucket.omie.es/MIBGAS/AGNO_{year}",
        f"/AGNO_{year}",
        f"AGNO_{year}",
    ]
    seen = set()
    for c in candidates:
        c = c.replace("//", "/")
        if c in seen:
            continue
        seen.add(c)
        if sftp_dir_exists(sftp, c):
            return c
    raise ValueError(f"Could not find AGNO_{year} directory. Tried: {candidates}")


def read_remote_excel_or_zip(sftp, remote_path: str, filename: str) -> pd.DataFrame:
    with sftp.open(remote_path, "rb") as f:
        content = f.read()
    lower = filename.lower()

    if lower.endswith((".xlsx", ".xls")):
        df = pd.read_excel(BytesIO(content), sheet_name=TARGET_SHEET)
        df = clean_columns(df)
        df["source_file"] = filename
        return standardize_raw_mibgas(df)

    if lower.endswith(".zip"):
        frames = []
        with zipfile.ZipFile(BytesIO(content)) as z:
            for inner in z.namelist():
                if inner.lower().endswith((".xlsx", ".xls")):
                    with z.open(inner) as g:
                        try:
                            df = pd.read_excel(BytesIO(g.read()), sheet_name=TARGET_SHEET)
                        except Exception:
                            continue
                    df = clean_columns(df)
                    df["source_file"] = f"{filename}/{inner}"
                    frames.append(standardize_raw_mibgas(df))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    return pd.DataFrame()


@st.cache_data(show_spinner=True, ttl=1800)
def load_live_2026() -> tuple[pd.DataFrame, str, pd.DataFrame]:
    """
    Load 2026 data from MIBGAS SFTP.

    WinSCP normally shows the files under:
        /secureftpbucket.omie.es/MIBGAS/AGNO_2026/XLS/

    Therefore this loader checks the year folder and common subfolders such as XLS and CSV.
    """
    rows = []
    frames = []
    sftp, transport = connect_sftp()
    try:
        year_dir = find_year_dir(sftp, LIVE_YEAR)
        candidate_dirs = [year_dir, f"{year_dir}/XLS", f"{year_dir}/CSV"]

        for remote_dir in candidate_dirs:
            try:
                items = sftp.listdir_attr(remote_dir)
            except Exception as e:
                rows.append({"filename": "", "remote_path": remote_dir, "status": "SKIPPED", "rows": 0, "message": str(e)})
                continue

            for item in items:
                if not stat.S_ISREG(item.st_mode):
                    continue
                filename = item.filename
                if not filename.lower().endswith((".xlsx", ".xls", ".zip")):
                    continue
                if "mibgas" not in filename.lower() and "gas" not in filename.lower():
                    continue

                remote_path = f"{remote_dir}/{filename}"
                try:
                    df = read_remote_excel_or_zip(sftp, remote_path, filename)
                    if not df.empty:
                        frames.append(df)
                    rows.append({"filename": filename, "remote_path": remote_path, "status": "OK", "rows": len(df), "message": ""})
                except Exception as e:
                    rows.append({"filename": filename, "remote_path": remote_path, "status": "ERROR", "rows": 0, "message": str(e)})
    finally:
        sftp.close()
        transport.close()

    if frames:
        out = pd.concat(frames, ignore_index=True)
        out = out[out["trading_day"].dt.year == LIVE_YEAR]
        out = out.drop_duplicates(subset=["trading_day", "product", "area", "delivery_start", "delivery_end"], keep="last")
        out = out.sort_values(["trading_day", "product"]).reset_index(drop=True)
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            out.to_csv(CACHE_FILE, index=False)
        except Exception:
            pass
        msg = f"2026 data loaded from MIBGAS SFTP ({len(out):,} rows)."
    else:
        out = pd.DataFrame()
        msg = "Connected to SFTP, but no 2026 MIBGAS trading files were loaded. Check AGNO_2026/XLS in Diagnostics."
    return out, msg, pd.DataFrame(rows)


def load_cached_2026() -> pd.DataFrame:
    if not CACHE_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(CACHE_FILE)
    for c in ["trading_day", "delivery_start", "delivery_end"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def load_all_data() -> tuple[pd.DataFrame, pd.DataFrame, str, pd.DataFrame]:
    hist, local_log = load_local_history()

    try:
        live, live_msg, sftp_log = load_live_2026()
    except Exception as e:
        cached = load_cached_2026()
        if not cached.empty:
            live, live_msg = cached, f"2026 SFTP data not loaded; using cache. Reason: {e}"
        else:
            live, live_msg = pd.DataFrame(), f"2026 SFTP data not loaded: {e}"
        sftp_log = pd.DataFrame()

    combined = pd.concat([hist, live], ignore_index=True) if not hist.empty or not live.empty else pd.DataFrame()
    if not combined.empty:
        combined = combined.sort_values(["trading_day", "product"]).reset_index(drop=True)
    return combined, local_log, live_msg, sftp_log

# =========================================================
# DATASETS
# =========================================================
def make_actuals(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Build actual daily gas prices.

    For GDAES_D+1 the economically relevant date for the x-axis is the delivery day,
    not the trading day. Therefore:
      - market_trading_day = original MIBGAS Trading day
      - trading_day = First Day Delivery, used by the existing chart/aggregation logic
    """
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    actuals = df[(df["product"] == "GDAES_D+1") & (df["area"].fillna("ES") == "ES")].copy()
    actuals["price"] = pd.to_numeric(actuals["reference_price_eur_mwh"], errors="coerce")
    actuals["market_trading_day"] = pd.to_datetime(actuals["trading_day"], errors="coerce")
    actuals["delivery_start"] = pd.to_datetime(actuals["delivery_start"], errors="coerce")

    # Use First Day Delivery as chart date. If missing, fall back to Trading day.
    actuals["trading_day"] = actuals["delivery_start"].combine_first(actuals["market_trading_day"])

    actuals = actuals.dropna(subset=["trading_day", "price"])
    actuals["series"] = "GDAES_D+1 Reference Price"
    return actuals[["trading_day", "market_trading_day", "product", "delivery_start", "delivery_end", "price", "volume_traded_mwh", "source_file", "series"]].sort_values("trading_day")


def make_forwards(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    forwards = df[(df["product"].isin(["GYES_Y+1", "GYES_Y+2"])) & (df["area"].fillna("ES") == "ES")].copy()

    # Your older files may have EOD Price. The newer MIBGAS file has Last Price instead.
    # Use EOD when present, otherwise Last Price, otherwise Reference Price.
    eod = pd.to_numeric(forwards.get("eod_price_eur_mwh"), errors="coerce") if "eod_price_eur_mwh" in forwards.columns else pd.Series(index=forwards.index, dtype=float)
    last = pd.to_numeric(forwards.get("last_price_eur_mwh"), errors="coerce") if "last_price_eur_mwh" in forwards.columns else pd.Series(index=forwards.index, dtype=float)
    ref = pd.to_numeric(forwards.get("reference_price_eur_mwh"), errors="coerce") if "reference_price_eur_mwh" in forwards.columns else pd.Series(index=forwards.index, dtype=float)
    forwards["price"] = eod.combine_first(last).combine_first(ref)
    forwards["price_source"] = "EOD Price"
    forwards.loc[eod.isna() & last.notna(), "price_source"] = "Last Price"
    forwards.loc[eod.isna() & last.isna() & ref.notna(), "price_source"] = "Reference Price"

    forwards = forwards.dropna(subset=["trading_day", "price"])
    forwards["series"] = forwards["product"] + " " + forwards["price_source"]
    return forwards[["trading_day", "product", "delivery_start", "delivery_end", "price", "price_source", "volume_traded_mwh", "source_file", "series"]].sort_values(["trading_day", "product"])

# =========================================================
# AGGREGATION / CHARTS
# =========================================================
def aggregate_price_series(df: pd.DataFrame, granularity: str, group_cols: list[str]) -> pd.DataFrame:
    """Aggregate price data to daily, weekly, monthly, annual, or rolling 30D average."""
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["trading_day"] = pd.to_datetime(tmp["trading_day"], errors="coerce")
    tmp["price"] = pd.to_numeric(tmp["price"], errors="coerce")
    tmp = tmp.dropna(subset=["trading_day", "price"])

    if tmp.empty:
        return pd.DataFrame()

    if granularity == "Daily":
        tmp["period"] = tmp["trading_day"].dt.normalize()
        out = (
            tmp.groupby(group_cols + ["period"], as_index=False)
            .agg(price=("price", "mean"), volume_traded_mwh=("volume_traded_mwh", "sum"))
            .sort_values(group_cols + ["period"])
        )
        out["period_label"] = out["period"].dt.strftime("%Y-%m-%d")
        return out

    if granularity == "Weekly":
        # Monday-start week; the timestamp represents the first day of the week.
        tmp["period"] = tmp["trading_day"].dt.to_period("W-SUN").dt.start_time
        out = (
            tmp.groupby(group_cols + ["period"], as_index=False)
            .agg(price=("price", "mean"), volume_traded_mwh=("volume_traded_mwh", "sum"))
            .sort_values(group_cols + ["period"])
        )
        out["period_label"] = out["period"].dt.strftime("Week of %d-%b-%Y")
        return out

    if granularity == "Monthly":
        tmp["period"] = tmp["trading_day"].dt.to_period("M").dt.to_timestamp()
        out = (
            tmp.groupby(group_cols + ["period"], as_index=False)
            .agg(price=("price", "mean"), volume_traded_mwh=("volume_traded_mwh", "sum"))
            .sort_values(group_cols + ["period"])
        )
        out["period_label"] = out["period"].dt.strftime("%b-%Y")
        return out

    if granularity == "Annual":
        tmp["year"] = tmp["trading_day"].dt.year
        out = (
            tmp.groupby(group_cols + ["year"], as_index=False)
            .agg(price=("price", "mean"), volume_traded_mwh=("volume_traded_mwh", "sum"))
            .sort_values(group_cols + ["year"])
        )
        out["period"] = pd.to_datetime(out["year"].astype(str) + "-01-01")
        out["period_label"] = out["year"].astype(str)
        return out

    if granularity == "Rolling 30D average":
        tmp["period"] = tmp["trading_day"].dt.normalize()
        daily = (
            tmp.groupby(group_cols + ["period"], as_index=False)
            .agg(price=("price", "mean"), volume_traded_mwh=("volume_traded_mwh", "sum"))
            .sort_values(group_cols + ["period"])
        )
        frames = []
        for _, g in daily.groupby(group_cols, dropna=False):
            g = g.sort_values("period").copy()
            g["price"] = g["price"].rolling(window=30, min_periods=1).mean()
            frames.append(g)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        out["period_label"] = out["period"].dt.strftime("%Y-%m-%d")
        return out

    return tmp


def build_price_chart(df: pd.DataFrame, granularity: str, title_y: str, color_field: str | None = None, color_scale=None):
    if df.empty:
        return None

    if granularity == "Annual":
        x_enc = alt.X("period_label:N", title=None, sort=sorted(df["period_label"].unique().tolist()), axis=alt.Axis(labelAngle=0))
    else:
        x_format = "%d-%b-%Y" if granularity in {"Daily", "Weekly", "Rolling 30D average"} else "%b-%Y"
        x_enc = alt.X("period:T", title=None, axis=alt.Axis(format=x_format, labelAngle=0))

    tooltip = [
        alt.Tooltip("period_label:N", title="Period"),
        alt.Tooltip("price:Q", title="Price €/MWh", format=",.2f"),
        alt.Tooltip("volume_traded_mwh:Q", title="Volume MWh", format=",.0f"),
    ]

    if color_field and color_field in df.columns:
        tooltip.insert(1, alt.Tooltip(f"{color_field}:N", title=color_field.replace("_", " ").title()))
        chart = (
            alt.Chart(df)
            .mark_line(point=True, strokeWidth=2.5)
            .encode(
                x=x_enc,
                y=alt.Y("price:Q", title=title_y),
                color=alt.Color(f"{color_field}:N", title="Product", scale=color_scale),
                tooltip=tooltip,
            )
        )
    else:
        chart = (
            alt.Chart(df)
            .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
            .encode(
                x=x_enc,
                y=alt.Y("price:Q", title=title_y),
                tooltip=tooltip,
            )
        )

    return apply_common_chart_style(chart, height=380)


def render_actuals_section(actuals_f: pd.DataFrame):
    st.subheader("Historical actuals - GDAES D+1 Reference Price by delivery day")
    granularity = st.radio(
        "Actuals granularity",
        options=["Daily", "Weekly", "Rolling 30D average", "Monthly", "Annual"],
        index=2,
        horizontal=True,
        key="actuals_granularity",
    )
    plot_df = aggregate_price_series(actuals_f, granularity, group_cols=["product"])
    c = build_price_chart(plot_df, granularity, "Reference Price €/MWh")
    if c is None:
        st.warning("No GDAES_D+1 Reference Price data found.")
    else:
        st.altair_chart(c, use_container_width=True)

    with st.expander("Show actuals data"):
        st.dataframe(actuals_f.sort_values("trading_day", ascending=False), use_container_width=True, hide_index=True)


def render_forwards_section(forwards_f: pd.DataFrame):
    st.subheader("Forward prices - GYES Y+1 and Y+2")
    st.caption("The chart uses EOD Price when available; otherwise Last Price; otherwise Reference Price.")
    granularity = st.radio(
        "Forwards granularity",
        options=["Daily", "Weekly", "Rolling 30D average", "Monthly", "Annual"],
        index=2,
        horizontal=True,
        key="forwards_granularity",
    )
    plot_df = aggregate_price_series(forwards_f, granularity, group_cols=["product"])
    color_scale = alt.Scale(domain=["GYES_Y+1", "GYES_Y+2"], range=[YELLOW_DARK, BLUE_PRICE])
    c = build_price_chart(plot_df, granularity, "Price €/MWh", color_field="product", color_scale=color_scale)
    if c is None:
        st.warning("No GYES_Y+1 / GYES_Y+2 data found.")
    else:
        st.altair_chart(c, use_container_width=True)

    with st.expander("Show forward data"):
        st.dataframe(forwards_f.sort_values(["trading_day", "product"], ascending=[False, True]), use_container_width=True, hide_index=True)
# =========================================================
# PAGE
# =========================================================
st.title("MIBGAS - Spain Gas Prices")
st.caption(
    "Historical files are loaded from `/data/MIBGAS_Data_*.xlsx` from 2021 to 2025. "
    "2026 files are loaded from MIBGAS SFTP when Streamlit Secrets are configured. Actuals use First Day Delivery on the x-axis."
)

section_header("MIBGAS market data")

refresh_col, status_col = st.columns([1, 4])
with refresh_col:
    if st.button("Refresh MIBGAS SFTP data"):
        load_live_2026.clear()
        load_local_history.clear()
        st.rerun()

raw, local_log, live_msg, sftp_log = load_all_data()
with status_col:
    st.caption(live_msg)

if not local_log.empty:
    errors = local_log[local_log["status"] == "ERROR"]
    if not errors.empty:
        with st.expander("Local file read warnings"):
            st.dataframe(errors, use_container_width=True, hide_index=True)

if raw.empty:
    st.warning("No MIBGAS data found. Check that files are uploaded as `data/MIBGAS_Data_2021.xlsx` ... `data/MIBGAS_Data_2025.xlsx`.")
    st.stop()

actuals = make_actuals(raw)
forwards = make_forwards(raw)

min_date = raw["trading_day"].min().date()
max_date = raw["trading_day"].max().date()
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
with col2:
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

actuals_f = actuals[(actuals["trading_day"].dt.date >= start_date) & (actuals["trading_day"].dt.date <= end_date)] if not actuals.empty else actuals
forwards_f = forwards[(forwards["trading_day"].dt.date >= start_date) & (forwards["trading_day"].dt.date <= end_date)] if not forwards.empty else forwards

if actuals_f.empty and forwards_f.empty:
    st.warning("Files were read, but no GDAES_D+1 actuals or GYES_Y+1/Y+2 forwards were found for the selected period.")

k1, k2, k3, k4 = st.columns(4)
if not actuals_f.empty:
    latest_a = actuals_f.sort_values("trading_day").iloc[-1]
    k1.metric("Latest GDAES D+1", f"{latest_a['price']:,.2f} €/MWh")
    k2.metric("Latest delivery day", latest_a["trading_day"].strftime("%Y-%m-%d"))
else:
    k1.metric("Latest GDAES D+1", "-")
    k2.metric("Latest delivery day", "-")

if not forwards_f.empty:
    latest_f_date = forwards_f["trading_day"].max()
    latest_forwards = forwards_f[forwards_f["trading_day"] == latest_f_date]
    y1 = latest_forwards.loc[latest_forwards["product"] == "GYES_Y+1", "price"]
    y2 = latest_forwards.loc[latest_forwards["product"] == "GYES_Y+2", "price"]
    k3.metric("Latest GYES Y+1", f"{float(y1.iloc[-1]):,.2f} €/MWh" if not y1.empty else "-")
    k4.metric("Latest GYES Y+2", f"{float(y2.iloc[-1]):,.2f} €/MWh" if not y2.empty else "-")
else:
    k3.metric("Latest GYES Y+1", "-")
    k4.metric("Latest GYES Y+2", "-")

st.markdown("---")

# Requested layout: one actuals chart, then one forwards chart underneath.
render_actuals_section(actuals_f)
render_forwards_section(forwards_f)

tab_raw, tab_diagnostics = st.tabs(["Raw data", "Diagnostics"])

with tab_raw:
    st.dataframe(raw.sort_values(["trading_day", "product"], ascending=[False, True]), use_container_width=True, hide_index=True)
    csv = raw.to_csv(index=False).encode("utf-8")
    st.download_button("Download combined MIBGAS data", csv, "mibgas_combined.csv", "text/csv")

with tab_diagnostics:
    st.write("Local files loaded from `/data`:")
    st.dataframe(local_log, use_container_width=True, hide_index=True)
    st.write("SFTP 2026 load status:")
    st.write(live_msg)
    if not sftp_log.empty:
        st.dataframe(sftp_log, use_container_width=True, hide_index=True)
    st.write("Secrets expected in Streamlit Cloud → App → Settings → Secrets:")
    secrets_example = 'MIBGAS_SFTP_HOST = "secureftp.mibgas.es"\nMIBGAS_SFTP_PORT = 22\nMIBGAS_SFTP_USER = "m.moreno"\nMIBGAS_SFTP_BASE_PATH = "/secureftpbucket.omie.es/MIBGAS"\n\nMIBGAS_SFTP_KEY = """\n-----BEGIN OPENSSH PRIVATE KEY-----\nPASTE_FULL_GASkey_CONTENT_HERE\n-----END OPENSSH PRIVATE KEY-----\n"""'
    st.code(secrets_example, language="toml")
