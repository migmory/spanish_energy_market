import os
import re
import stat
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import altair as alt
import pandas as pd
import requests
import streamlit as st
import pydeck as pdk

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
def _search_secret_case_insensitive(container, target_name: str):
    """Recursively find a secret by name, including nested TOML sections."""
    try:
        items = container.items()
    except Exception:
        return None

    target = str(target_name).strip().lower()
    for key, value in items:
        if str(key).strip().lower() == target and value not in (None, ""):
            return value

    for _, value in items:
        if hasattr(value, "items"):
            found = _search_secret_case_insensitive(value, target_name)
            if found not in (None, ""):
                return found
    return None


def get_secret(name: str, default=None):
    """Read top-level, nested, case-insensitive Streamlit secrets or env vars."""
    try:
        found = _search_secret_case_insensitive(st.secrets, name)
        if found not in (None, ""):
            return found
    except Exception:
        pass

    for env_name in (name, name.upper(), name.lower()):
        env_value = os.getenv(env_name)
        if env_value:
            return env_value
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
# GIE AGSI / ALSI INVENTORY API
# =========================================================
GIE_COUNTRY_OPTIONS = {
    "EU total": "eu",
    "Spain": "es",
    "Portugal": "pt",
    "France": "fr",
    "Germany": "de",
    "Italy": "it",
    "Netherlands": "nl",
    "Belgium": "be",
}

GIE_MAP_COUNTRIES = {
    "Spain": {"code": "es", "iso_alpha3": "ESP", "lat": 40.20, "lon": -3.50},
    "Portugal": {"code": "pt", "iso_alpha3": "PRT", "lat": 39.60, "lon": -8.00},
    "France": {"code": "fr", "iso_alpha3": "FRA", "lat": 46.30, "lon": 2.20},
    "Germany": {"code": "de", "iso_alpha3": "DEU", "lat": 51.10, "lon": 10.40},
    "Italy": {"code": "it", "iso_alpha3": "ITA", "lat": 42.80, "lon": 12.50},
    "Netherlands": {"code": "nl", "iso_alpha3": "NLD", "lat": 52.20, "lon": 5.30},
    "Belgium": {"code": "be", "iso_alpha3": "BEL", "lat": 50.70, "lon": 4.60},
    "Austria": {"code": "at", "iso_alpha3": "AUT", "lat": 47.60, "lon": 14.20},
    "Czechia": {"code": "cz", "iso_alpha3": "CZE", "lat": 49.80, "lon": 15.40},
    "Hungary": {"code": "hu", "iso_alpha3": "HUN", "lat": 47.10, "lon": 19.40},
    "Poland": {"code": "pl", "iso_alpha3": "POL", "lat": 52.10, "lon": 19.30},
    "Croatia": {"code": "hr", "iso_alpha3": "HRV", "lat": 45.30, "lon": 16.10},
    "Greece": {"code": "gr", "iso_alpha3": "GRC", "lat": 39.10, "lon": 22.30},
    "Lithuania": {"code": "lt", "iso_alpha3": "LTU", "lat": 55.20, "lon": 23.80},
    "Finland": {"code": "fi", "iso_alpha3": "FIN", "lat": 64.50, "lon": 26.00},
}

ALSI_MAP_COUNTRY_NAMES = [
    "Spain", "Portugal", "France", "Germany", "Italy", "Netherlands",
    "Belgium", "Poland", "Croatia", "Greece", "Lithuania", "Finland",
]



def get_gie_api_key(platform: str) -> str | None:
    """Use shared, platform-specific, aliased, or temporary-session GIE keys."""
    aliases = [
        "GIE_API_KEY", "GIE_KEY",
        f"{platform.upper()}_API_KEY", f"{platform.upper()}_KEY",
        "x-key", "X_KEY",
    ]
    for alias in aliases:
        key = get_secret(alias)
        if key:
            cleaned = str(key).strip().strip('"').strip("'")
            if cleaned:
                return cleaned

    session_key = st.session_state.get("gie_manual_api_key")
    return str(session_key).strip() if session_key else None


def _gie_payload_rows(payload) -> tuple[list[dict], int | None]:
    """Extract API rows and pagination metadata from either API response format."""
    if isinstance(payload, list):
        return payload, None
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response format from GIE API.")

    if payload.get("error"):
        raise ValueError(str(payload["error"]))

    rows = payload.get("data", payload.get("results", payload.get("items", [])))
    if isinstance(rows, dict):
        rows = rows.get("data", rows.get("results", rows.get("items", [])))
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise ValueError(
            "GIE API response does not contain a valid data array. "
            f"Top-level fields: {list(payload.keys())}"
        )

    last_page = payload.get("last_page", payload.get("lastPage"))
    try:
        last_page = int(last_page) if last_page is not None else None
    except (TypeError, ValueError):
        last_page = None
    return rows, last_page


def _request_gie_page(
    platform: str,
    country_code: str,
    start_date,
    end_date,
    page: int,
    api_key: str,
    api_mode: str | None = None,
) -> tuple[list[dict], int | None, str]:
    """Request AGSI via V2 and ALSI via its still-applicable V4 data endpoint."""
    platform = platform.lower()
    country_code = country_code.lower()
    country_code_api = country_code.upper()
    base_url = f"https://{platform}.gie.eu"
    headers = {
        "x-key": api_key,
        "Accept": "application/json",
        "User-Agent": "NexwellPower-Streamlit/1.0",
    }
    start_s = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_s = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    # API V2 uses type=eu for the EU aggregate and country=xx for countries.
    # Using country=eu can return an incomplete/empty schema on ALSI.
    v2_params = {
        "from": start_s,
        "to": end_s,
        "size": 300,
        "page": page,
    }
    if country_code == "eu":
        v2_params["type"] = "eu"
    else:
        # GIE documents country filters as two-character uppercase codes.
        v2_params["country"] = country_code_api

    candidates = {
        "v2": (f"{base_url}/api", v2_params),
        "legacy": (
            f"{base_url}/api/data/{country_code}",
            {"from": start_s, "till": end_s},
        ),
    }
    # IMPORTANT: the current common API documentation explicitly notes that
    # the older V4 data endpoint remains applicable to ALSI. The generic V2
    # endpoint can return valid rows and send-out values while leaving LNG
    # inventory and DTMI empty. Therefore ALSI must prefer /api/data/{code}.
    if api_mode in candidates:
        modes = [api_mode]
    elif platform == "alsi" and country_code == "eu":
        # The legacy EU aggregate is reliable for ALSI.
        modes = ["legacy", "v2"]
    elif platform == "alsi":
        # Country aggregates are documented on /api?country=XX.
        # /api/data/{country} can return rows with send-out but empty inventory.
        modes = ["v2", "legacy"]
    else:
        modes = ["v2", "legacy"]

    errors = []

    def _has_nonempty_value(value) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip() not in {"", "-", "--", "null", "None", "N/A", "n/a"}
        if isinstance(value, dict):
            return any(_has_nonempty_value(v) for v in value.values())
        if isinstance(value, (list, tuple)):
            return any(_has_nonempty_value(v) for v in value)
        return True

    def alsi_rows_have_inventory_values(rows: list[dict]) -> bool:
        if platform != "alsi" or not rows:
            return bool(rows)
        aliases = {
            "lnginventory", "inventory", "lngstock", "lngvolume", "dtmi",
            "declaredtotalmaxinventory", "maxinventory", "storagecapacity",
            "lngstoragecapacity", "inventorypct", "inventorypercentage",
            "filllevel", "inventorylevel",
        }
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                if normalize_col_name(key) in aliases and _has_nonempty_value(value):
                    return True
        return False

    for mode in modes:
        url, params = candidates[mode]
        try:
            response = requests.get(url, params=params, headers=headers, timeout=45)
            response.raise_for_status()
            payload = response.json()
            rows, last_page = _gie_payload_rows(payload)

            # A response containing only send-out is not sufficient for the
            # inventory KPIs/map. Try the alternative endpoint instead.
            if platform == "alsi" and rows and not alsi_rows_have_inventory_values(rows):
                errors.append(f"{mode}: rows returned but LNG inventory/DTMI values were empty")
                continue

            if rows or mode == modes[-1]:
                return rows, last_page, mode
        except Exception as exc:
            errors.append(f"{mode}: {exc}")

    if errors:
        raise ValueError(" | ".join(errors))
    return [], None, modes[-1]


@st.cache_data(show_spinner=False, ttl=3600)
def load_gie_inventory(
    platform: str,
    country_code: str,
    start_date,
    end_date,
    api_key: str,
) -> pd.DataFrame:
    """Download and standardise AGSI underground storage or ALSI LNG inventory data."""
    all_rows: list[dict] = []
    page = 1
    last_page = None
    api_mode = None

    while page <= 100:
        rows, reported_last_page, api_mode = _request_gie_page(
            platform=platform,
            country_code=country_code,
            start_date=start_date,
            end_date=end_date,
            page=page,
            api_key=api_key,
            api_mode=api_mode,
        )
        all_rows.extend(rows)

        # The legacy /api/data/{code} endpoint returns the entire selected
        # period in one call. API V2 is paginated.
        if api_mode == "legacy":
            break

        if reported_last_page is not None:
            last_page = reported_last_page
        if not rows:
            break
        if last_page is not None and page >= last_page:
            break
        if last_page is None and len(rows) < 300:
            break
        page += 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # ALSI still uses an older API schema in some endpoint/account combinations.
    # Canonicalise common field-name variants before calculating metrics.
    def coalesce_api_field(canonical: str, aliases: list[str]) -> None:
        """Fill a canonical field from aliases, even when it already exists but is null."""
        normalised = {normalize_col_name(c): c for c in df.columns}
        result = pd.Series(pd.NA, index=df.index, dtype="object")

        # Keep any valid canonical values first, then fill gaps from aliases.
        ordered_aliases = [canonical] + [a for a in aliases if normalize_col_name(a) != normalize_col_name(canonical)]
        seen_sources = set()
        for alias in ordered_aliases:
            source = normalised.get(normalize_col_name(alias))
            if source is None or source in seen_sources:
                continue
            seen_sources.add(source)
            candidate = df[source].copy()
            if not pd.api.types.is_numeric_dtype(candidate):
                candidate = candidate.astype("string").str.strip()
                candidate = candidate.replace({
                    "": pd.NA, "-": pd.NA, "--": pd.NA,
                    "None": pd.NA, "none": pd.NA,
                    "null": pd.NA, "NULL": pd.NA,
                    "N/A": pd.NA, "n/a": pd.NA,
                })
            result = result.combine_first(candidate)

        df[canonical] = result

    coalesce_api_field("lngInventory", [
        "lngInventory", "lng_inventory", "inventory", "lngStock", "lng_stock",
        "lngVolume", "lng_volume", "gasInStorage", "gas_in_storage",
    ])
    coalesce_api_field("dtmi", [
        "dtmi", "declaredTotalMaxInventory", "declared_max_inventory",
        "maxInventory", "max_inventory", "storageCapacity", "storage_capacity",
        "lngStorageCapacity", "lng_storage_capacity", "workingGasVolume",
    ])
    coalesce_api_field("inventory_pct", [
        "inventory_pct", "inventoryPercentage", "inventory_percentage",
        "inventoryFull", "inventory_full", "fillLevel", "fill_level",
        "stockLevel", "stock_level", "inventoryLevel", "inventory_level",
        "lngInventoryLevel", "full",
    ])
    coalesce_api_field("sendOut", [
        "sendOut", "send_out", "sendout", "dailySendOut", "daily_send_out",
    ])
    coalesce_api_field("dtrs", [
        "dtrs", "declaredTotalReferenceSendOut", "referenceSendOutCapacity",
        "reference_send_out_capacity", "sendOutCapacity", "send_out_capacity",
    ])

    date_col = next(
        (c for c in ["gasDayStart", "gasDayStartedOn", "gasDay", "gas_day", "date", "day"] if c in df.columns),
        None,
    )
    if date_col is None:
        raise ValueError(f"No gas-day date field found. Columns returned: {df.columns.tolist()}")

    df["gas_day"] = pd.to_datetime(df[date_col], errors="coerce")
    numeric_cols = [
        "gasInStorage", "consumption", "consumptionFull", "injection", "withdrawal",
        "workingGasVolume", "injectionCapacity", "withdrawalCapacity", "trend", "full",
        "lngInventory", "inventory_pct", "sendOut", "dtmi", "dtrs",
    ]

    def _extract_numeric_scalar(value):
        """Extract a numeric scalar from ALSI values, including nested JSON objects."""
        if value is None or value is pd.NA:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return value

        # Current GIE responses can wrap values in objects such as
        # {"value": "5373.25", ...}. Prefer common value keys first.
        if isinstance(value, dict):
            preferred_keys = (
                "value", "amount", "total", "current", "raw", "rawValue",
                "numericValue", "reportedValue", "inventory", "lngInventory",
                "dtmi", "sendOut", "dtrs", "percentage", "percent", "pct",
            )
            for key in preferred_keys:
                if key in value:
                    parsed = _extract_numeric_scalar(value[key])
                    if parsed is not None:
                        return parsed
            for nested in value.values():
                parsed = _extract_numeric_scalar(nested)
                if parsed is not None:
                    return parsed
            return None

        if isinstance(value, (list, tuple)):
            for nested in value:
                parsed = _extract_numeric_scalar(nested)
                if parsed is not None:
                    return parsed
            return None

        s = str(value).strip()
        if s in {"", "-", "--", "None", "none", "null", "NULL", "N/A", "n/a"}:
            return None
        s = re.sub(r"<[^>]+>", "", s)
        s = s.replace("%", "").replace("\u00a0", "").replace(" ", "")

        # Keep only a plausible signed decimal number if units or labels are present.
        matches = re.findall(r"[-+]?\d[\d.,]*", s)
        if not matches:
            return None
        s = matches[0]

        if "," in s and "." in s:
            # Decide decimal separator by the last occurrence.
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            parts = s.split(",")
            if len(parts[-1]) <= 3:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")

        try:
            return float(s)
        except (TypeError, ValueError):
            return None

    def gie_to_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.map(_extract_numeric_scalar), errors="coerce")

    for col in numeric_cols:
        if col in df.columns:
            df[col] = gie_to_numeric(df[col])

    if platform.lower() == "agsi":
        if "full" not in df.columns and {"gasInStorage", "workingGasVolume"}.issubset(df.columns):
            df["full"] = 100 * df["gasInStorage"] / df["workingGasVolume"].replace(0, pd.NA)
        if {"injection", "withdrawal"}.issubset(df.columns):
            df["net_flow_gwh_d"] = df["injection"] - df["withdrawal"]
    else:
        # ALSI V2 calls the stock field `inventory`; legacy responses may call it
        # `lngInventory`. The canonical field above combines both. Always fill
        # missing percentages from inventory / declared maximum inventory.
        if "inventory_pct" not in df.columns:
            df["inventory_pct"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        else:
            df["inventory_pct"] = pd.to_numeric(df["inventory_pct"], errors="coerce")

        if {"lngInventory", "dtmi"}.issubset(df.columns):
            derived_pct = 100 * df["lngInventory"] / df["dtmi"].replace(0, pd.NA)
            df["inventory_pct"] = df["inventory_pct"].combine_first(derived_pct)

    df = df.dropna(subset=["gas_day"])
    df = df.drop_duplicates(subset=["gas_day"], keep="last")
    return df.sort_values("gas_day").reset_index(drop=True)



@st.cache_data(show_spinner=False, ttl=3600)
def load_gie_map_snapshot(platform: str, end_date, api_key: str, metric_col: str) -> pd.DataFrame:
    """Load the latest non-null selected inventory metric for each map country."""
    end_ts = pd.Timestamp(end_date).normalize()
    start_ts = end_ts - pd.Timedelta(days=45)
    rows = []

    map_country_names = ALSI_MAP_COUNTRY_NAMES if platform.lower() == "alsi" else list(GIE_MAP_COUNTRIES)

    for country_name in map_country_names:
        meta = GIE_MAP_COUNTRIES[country_name]
        try:
            df = load_gie_inventory(
                platform=platform,
                country_code=meta["code"],
                start_date=start_ts,
                end_date=end_ts,
                api_key=api_key,
            )
            if df.empty or metric_col not in df.columns:
                continue
            valid = df[df[metric_col].notna()].sort_values("gas_day")
            if valid.empty:
                continue
            latest = valid.iloc[-1].copy()
            latest["country_label"] = country_name
            latest["country_code"] = meta["code"].upper()
            latest["iso_alpha3"] = meta["iso_alpha3"]
            latest["lat"] = meta["lat"]
            latest["lon"] = meta["lon"]
            rows.append(latest)
        except Exception:
            # Countries without data for a platform/date are simply omitted.
            continue

    return pd.DataFrame(rows)


def _add_map_style_columns(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    tmp = df.copy()
    vals = pd.to_numeric(tmp[metric_col], errors="coerce").fillna(0)
    max_val = max(float(vals.max()) if len(vals) else 0.0, 1.0)
    rel = (vals / max_val).clip(lower=0, upper=1)

    tmp["map_radius"] = 25_000 + rel.pow(0.7) * 140_000
    tmp["color_r"] = (255 - rel * 160).round().astype(int)
    tmp["color_g"] = (120 + rel * 110).round().astype(int)
    tmp["color_b"] = (90 + rel * 40).round().astype(int)
    tmp["fill_color"] = tmp[["color_r", "color_g", "color_b"]].values.tolist()
    return tmp


def build_gie_bubble_map(
    df: pd.DataFrame,
    metric_col: str,
    metric_title: str,
    suffix: str = "",
):
    if df.empty or metric_col not in df.columns:
        return None

    keep = [
        c for c in [
            "country_label", "country_code", "iso_alpha3", "lat", "lon", "gas_day", metric_col
        ] if c in df.columns
    ]
    tmp = df[keep].copy()
    tmp[metric_col] = pd.to_numeric(tmp[metric_col], errors="coerce")
    tmp = tmp.dropna(subset=[metric_col, "lat", "lon"])
    if tmp.empty:
        return None

    tmp = _add_map_style_columns(tmp, metric_col)
    tmp["gas_day_label"] = pd.to_datetime(tmp["gas_day"], errors="coerce").dt.strftime("%Y-%m-%d")
    tmp["value_label"] = tmp[metric_col].map(lambda x: f"{x:,.1f}{suffix}")
    tmp["map_label"] = tmp.apply(
        lambda r: f"{r['country_code']}\n{r['value_label']}", axis=1
    )

    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=tmp,
        get_position="[lon, lat]",
        get_radius="map_radius",
        get_fill_color="fill_color",
        get_line_color=[30, 41, 59],
        line_width_min_pixels=1,
        pickable=True,
        opacity=0.78,
        stroked=True,
    )
    labels = pdk.Layer(
        "TextLayer",
        data=tmp,
        get_position="[lon, lat]",
        get_text="map_label",
        get_size=15,
        get_color=[17, 24, 39],
        get_alignment_baseline="bottom",
        get_text_anchor="middle",
        get_pixel_offset="[0, -24]",
        pickable=False,
    )

    tooltip = {
        "html": (
            f"<b>{{country_label}}</b><br/>"
            f"{metric_title}: {{value_label}}<br/>"
            "Gas day: {gas_day_label}"
        ),
        "style": {"backgroundColor": "white", "color": "#111827"},
    }

    return pdk.Deck(
        map_provider="carto",
        map_style="light",
        initial_view_state=pdk.ViewState(
            latitude=48.5,
            longitude=10.5,
            zoom=3.35,
            pitch=0,
        ),
        layers=[scatter, labels],
        tooltip=tooltip,
    )


def _metric_text(value, decimals: int = 1, suffix: str = "") -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.{decimals}f}{suffix}"


def build_gie_seasonal_chart(df: pd.DataFrame, value_col: str, y_title: str, tooltip_title: str):
    if df.empty or value_col not in df.columns:
        return None
    tmp = df[["gas_day", value_col]].dropna().copy()
    if tmp.empty:
        return None

    tmp["year"] = tmp["gas_day"].dt.year.astype(str)
    tmp["actual_date"] = tmp["gas_day"].dt.strftime("%Y-%m-%d")
    tmp["season_date"] = pd.to_datetime("2000-" + tmp["gas_day"].dt.strftime("%m-%d"), errors="coerce")
    latest_year = tmp["year"].astype(int).max()

    chart = (
        alt.Chart(tmp)
        .mark_line(interpolate="monotone")
        .encode(
            x=alt.X("season_date:T", title=None, axis=alt.Axis(format="%b", labelAngle=0)),
            y=alt.Y(f"{value_col}:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=alt.Color("year:N", title="Year", sort="descending"),
            detail="year:N",
            strokeWidth=alt.condition(
                f"datum.year == '{latest_year}'",
                alt.value(3.5),
                alt.value(1.5),
            ),
            opacity=alt.condition(
                f"datum.year == '{latest_year}'",
                alt.value(1.0),
                alt.value(0.65),
            ),
            tooltip=[
                alt.Tooltip("actual_date:N", title="Gas day"),
                alt.Tooltip("year:N", title="Year"),
                alt.Tooltip(f"{value_col}:Q", title=tooltip_title, format=",.2f"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=390)


def build_gie_flow_chart(df: pd.DataFrame, platform: str):
    if df.empty:
        return None

    if platform == "agsi":
        flow_cols = [c for c in ["injection", "withdrawal"] if c in df.columns]
        labels = {"injection": "Injection", "withdrawal": "Withdrawal"}
        y_title = "GWh/d"
    else:
        flow_cols = [c for c in ["sendOut", "dtrs"] if c in df.columns]
        labels = {"sendOut": "Send-out", "dtrs": "Reference send-out capacity"}
        y_title = "GWh/d"

    if not flow_cols:
        return None

    tmp = df[["gas_day"] + flow_cols].melt(
        id_vars="gas_day",
        value_vars=flow_cols,
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])
    tmp["series"] = tmp["series"].map(labels).fillna(tmp["series"])

    chart = (
        alt.Chart(tmp)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("gas_day:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("value:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=alt.Color("series:N", title=None),
            tooltip=[
                alt.Tooltip("gas_day:T", title="Gas day", format="%Y-%m-%d"),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("value:Q", title=y_title, format=",.1f"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=310)


def render_gie_inventory_section():
    section_header("European gas inventories - GIE AGSI / ALSI")
    st.caption(
        "Daily underground gas storage and LNG terminal data. "
        "Source: Gas Infrastructure Europe (GIE), AGSI / ALSI Transparency Platforms."
    )

    c1, c2, c3, c4 = st.columns([1.25, 1.25, 1.25, 0.8])
    with c1:
        inventory_view = st.radio(
            "Dataset",
            options=["Underground storage (AGSI)", "LNG inventory (ALSI)"],
            horizontal=True,
            key="gie_inventory_view",
        )
    with c2:
        country_label = st.selectbox(
            "Geography",
            options=list(GIE_COUNTRY_OPTIONS.keys()),
            index=0,
            key="gie_country",
        )
    today = pd.Timestamp.today().date()
    default_start = pd.Timestamp(year=max(today.year - 3, 2011), month=1, day=1).date()
    with c3:
        gie_dates = st.date_input(
            "Inventory period",
            value=(default_start, today),
            min_value=pd.Timestamp("2011-01-01").date(),
            max_value=today,
            key="gie_inventory_dates",
        )
    with c4:
        st.write("")
        st.write("")
        if st.button("Refresh GIE data", key="refresh_gie_data"):
            load_gie_inventory.clear()
            load_gie_map_snapshot.clear()
            st.rerun()

    if not isinstance(gie_dates, (tuple, list)) or len(gie_dates) != 2:
        st.info("Select both a start and an end date.")
        return
    start_date, end_date = gie_dates
    if start_date > end_date:
        st.warning("The inventory start date must be before the end date.")
        return

    platform = "agsi" if "AGSI" in inventory_view else "alsi"
    api_key = get_gie_api_key(platform)
    if not api_key:
        st.warning(
            "The app is not detecting the GIE key in this Streamlit deployment. "
            "This version accepts top-level or nested Secrets and common key aliases."
        )
        manual_key = st.text_input(
            "Temporary GIE API key for this browser session",
            type="password",
            key="gie_manual_key_input",
            help="Use this only to test the connection. Keep the permanent key in Streamlit Secrets.",
        )
        if manual_key:
            st.session_state["gie_manual_api_key"] = manual_key.strip()
            st.rerun()
        with st.expander("Permanent Streamlit Secrets configuration"):
            st.code('GIE_API_KEY = "PASTE_YOUR_GIE_API_KEY_HERE"', language="toml")
            st.caption("Save it in this app's Secrets and reboot. Do not place it inside the multiline MIBGAS_SFTP_KEY value.")
        return

    try:
        with st.spinner(f"Loading {platform.upper()} inventory data..."):
            inventory = load_gie_inventory(
                platform=platform,
                country_code=GIE_COUNTRY_OPTIONS[country_label],
                start_date=start_date,
                end_date=end_date,
                api_key=api_key,
            )
    except Exception as exc:
        st.error(
            f"Could not load {platform.upper()} data: {exc}. "
            "The API key was detected, but the request or returned schema failed."
        )
        with st.expander("GIE connection diagnostics"):
            st.write({
                "platform": platform.upper(),
                "geography": country_label,
                "country_code": GIE_COUNTRY_OPTIONS[country_label],
                "start_date": str(start_date),
                "end_date": str(end_date),
                "api_key_detected": True,
            })
        return

    if inventory.empty:
        st.warning("The GIE API returned no inventory rows for the selected geography and period.")
        with st.expander("GIE request diagnostics"):
            st.write({
                "platform": platform.upper(),
                "geography": country_label,
                "country_code": GIE_COUNTRY_OPTIONS[country_label],
                "start_date": str(start_date),
                "end_date": str(end_date),
                "api_key_detected": bool(api_key),
            })
        return

    def latest_non_null(col: str):
        if col not in inventory.columns:
            return pd.NA
        valid = inventory.loc[inventory[col].notna(), ["gas_day", col]].sort_values("gas_day")
        return valid.iloc[-1][col] if not valid.empty else pd.NA

    m1, m2, m3, m4 = st.columns(4)

    if platform == "agsi":
        m1.metric("Storage filling level", _metric_text(latest_non_null("full"), 1, "%"))
        m2.metric("Gas in storage", _metric_text(latest_non_null("gasInStorage"), 1, " TWh"))
        m3.metric("Working gas volume", _metric_text(latest_non_null("workingGasVolume"), 1, " TWh"))
        m4.metric("Net daily flow", _metric_text(latest_non_null("net_flow_gwh_d"), 0, " GWh/d"))

        metric_label = st.radio(
            "Inventory chart metric",
            options=["Filling level (%)", "Gas in storage (TWh)"],
            horizontal=True,
            key="agsi_metric",
        )
        if metric_label.startswith("Filling"):
            value_col, y_title, tooltip_title = "full", "Storage filling level (%)", "Fill level %"
            map_suffix = "%"
        else:
            value_col, y_title, tooltip_title = "gasInStorage", "Gas in storage (TWh)", "Gas in storage TWh"
            map_suffix = " TWh"
        st.subheader("Europe map - underground gas storage")
    else:
        m1.metric("LNG inventory", _metric_text(latest_non_null("lngInventory"), 1, " thousand m³ LNG"))
        m2.metric("Tank filling level", _metric_text(latest_non_null("inventory_pct"), 1, "%"))
        m3.metric("Send-out", _metric_text(latest_non_null("sendOut"), 1, " GWh/d"))
        m4.metric("Declared max inventory", _metric_text(latest_non_null("dtmi"), 1, " thousand m³ LNG"))

        metric_label = st.radio(
            "Inventory chart metric",
            options=["Tank filling level (%)", "LNG inventory (thousand m³)"],
            horizontal=True,
            key="alsi_metric",
        )
        if metric_label.startswith("Tank"):
            value_col, y_title, tooltip_title = "inventory_pct", "LNG tank filling level (%)", "Fill level %"
            map_suffix = "%"
        else:
            value_col, y_title, tooltip_title = "lngInventory", "LNG inventory (thousand m³)", "LNG inventory"
            map_suffix = " thousand m³"
        st.subheader("Europe map - LNG inventory")

    try:
        with st.spinner("Loading Europe map snapshot..."):
            map_snapshot = load_gie_map_snapshot(
                platform=platform,
                end_date=end_date,
                api_key=api_key,
                metric_col=value_col,
            )
    except Exception:
        map_snapshot = pd.DataFrame()

    if not map_snapshot.empty and value_col in map_snapshot.columns:
        latest_map_day = pd.to_datetime(map_snapshot["gas_day"], errors="coerce").max()
        if pd.notna(latest_map_day):
            st.caption(f"Latest available map snapshot: {latest_map_day.strftime('%Y-%m-%d')}")
        map_deck = build_gie_bubble_map(
            map_snapshot,
            metric_col=value_col,
            metric_title=metric_label,
            suffix=map_suffix,
        )
        if map_deck is not None:
            st.pydeck_chart(map_deck, use_container_width=True, height=520)

        with st.expander("Show map snapshot values"):
            map_display_cols = [c for c in ["country_label", "gas_day", value_col] if c in map_snapshot.columns]
            display_df = map_snapshot[map_display_cols].copy().sort_values(value_col, ascending=False)
            rename_map = {value_col: metric_label}
            st.dataframe(display_df.rename(columns=rename_map), use_container_width=True, hide_index=True)
    else:
        returned_cols = ", ".join(sorted(map(str, inventory.columns.tolist())))
        st.info(
            "ALSI loaded the EU aggregate, but no valid country-level inventory values were returned for the map. "
            "The map queries /api?country=XX for each LNG country and ignores responses containing only send-out. "
            f"EU fields returned: {returned_cols}"
        )
        if platform == "alsi":
            raw_diag_cols = [c for c in ["gas_day", "inventory", "lngInventory", "dtmi", "inventory_pct", "sendOut", "dtrs"] if c in inventory.columns]
            if raw_diag_cols:
                with st.expander("ALSI raw-value diagnostics"):
                    diag = inventory[raw_diag_cols].tail(10).copy()
                    for c in diag.columns:
                        if c != "gas_day":
                            diag[c] = diag[c].map(lambda x: repr(x))
                    st.dataframe(diag, use_container_width=True, hide_index=True)

    st.subheader(f"{country_label} inventory - seasonal comparison")
    inventory_chart = build_gie_seasonal_chart(inventory, value_col, y_title, tooltip_title)
    if inventory_chart is not None:
        st.altair_chart(inventory_chart, use_container_width=True)
    else:
        st.warning(f"No values were returned for {metric_label}.")

    st.subheader("Daily operational flows")
    flow_chart = build_gie_flow_chart(inventory, platform)
    if flow_chart is not None:
        st.altair_chart(flow_chart, use_container_width=True)

    with st.expander("Show and download GIE inventory data"):
        display_cols = [
            c for c in [
                "gas_day", "name", "code", "status",
                "gasInStorage", "full", "workingGasVolume", "injection", "withdrawal", "net_flow_gwh_d",
                "lngInventory", "inventory_pct", "sendOut", "dtmi", "dtrs", "info",
            ] if c in inventory.columns
        ]
        st.dataframe(
            inventory[display_cols].sort_values("gas_day", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
        csv = inventory.to_csv(index=False).encode("utf-8")
        st.download_button(
            f"Download {platform.upper()} inventory CSV",
            data=csv,
            file_name=f"gie_{platform}_{GIE_COUNTRY_OPTIONS[country_label]}_{start_date}_{end_date}.csv",
            mime="text/csv",
            key=f"download_{platform}_inventory",
        )


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

st.markdown("---")
render_gie_inventory_section()

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
    secrets_example = 'GIE_API_KEY = "PASTE_YOUR_GIE_API_KEY_HERE"\n\nMIBGAS_SFTP_HOST = "secureftp.mibgas.es"\nMIBGAS_SFTP_PORT = 22\nMIBGAS_SFTP_USER = "m.moreno"\nMIBGAS_SFTP_BASE_PATH = "/secureftpbucket.omie.es/MIBGAS"\n\nMIBGAS_SFTP_KEY = """\n-----BEGIN OPENSSH PRIVATE KEY-----\nPASTE_FULL_GASkey_CONTENT_HERE\n-----END OPENSSH PRIVATE KEY-----\n"""'
    st.code(secrets_example, language="toml")
