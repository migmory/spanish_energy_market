import stat
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

try:
    import paramiko
except Exception:  # lets local historical view work even if paramiko is missing
    paramiko = None

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(page_title="MIBGAS", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

LOCAL_START_YEAR = 2021
LOCAL_END_YEAR = 2025
LIVE_YEAR = 2026

CACHE_FILE_2026 = DATA_DIR / "mibgas_2026_cache.csv"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
GREY_SHADE = "#F3F4F6"

# =========================================================
# STYLE HELPERS
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


def format_metric(value, suffix="", decimals=2):
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}{suffix}"

# =========================================================
# GENERIC NORMALIZATION HELPERS
# =========================================================
def normalize_text(value) -> str:
    """Robust text normalization for column names and product names."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().lower()
    repl = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "\n": " ", "\r": " ", "\t": " ",
    }
    for a, b in repl.items():
        text = text.replace(a, b)
    text = " ".join(text.split())
    text = text.replace("[", "").replace("]", "")
    text = text.replace("(", "").replace(")", "")
    text = text.replace("/", "_").replace("-", "_").replace(".", "_")
    text = text.replace(" ", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def parse_number_series(series: pd.Series) -> pd.Series:
    """Parse Spanish/European numeric strings while preserving already numeric values."""
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    txt = series.astype(str).str.strip()
    txt = txt.replace({"": None, "nan": None, "None": None, "NaN": None})
    txt = txt.str.replace("€", "", regex=False)
    txt = txt.str.replace("MWh", "", regex=False)
    txt = txt.str.replace(" ", "", regex=False)

    # If comma exists, assume European format: 1.234,56 -> 1234.56
    has_comma = txt.str.contains(",", na=False)
    european = txt.where(~has_comma, txt.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    return pd.to_numeric(european, errors="coerce")


def find_col(df: pd.DataFrame, possible_names: list[str]) -> str | None:
    norm_map = {normalize_text(c): c for c in df.columns}
    for name in possible_names:
        norm = normalize_text(name)
        if norm in norm_map:
            return norm_map[norm]
    return None


def standardize_mibgas_raw(raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Standardizes raw MIBGAS data to a compact schema.
    Required useful fields:
    - trading_day
    - product
    - area
    - daily_reference_price_eur_mwh
    - eod_price_eur_mwh
    - delivery_start
    - delivery_end
    - daily_volume_traded_mwh
    """
    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Drop fully empty rows/columns.
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if df.empty:
        return pd.DataFrame()

    col_trading_day = find_col(df, ["Trading day", "Trading Day", "Fecha", "Fecha negociacion", "Fecha negociación"])
    col_product = find_col(df, ["Product", "Producto", "Contract", "Contrato"])
    col_area = find_col(df, ["Area", "Área"])
    col_place = find_col(df, ["Place of delivery", "Place Delivery", "PVB/VTP"])
    col_delivery_start = find_col(df, ["First Day Delivery", "First delivery day", "Delivery start"])
    col_delivery_end = find_col(df, ["Last Day Delivery", "Last delivery day", "Delivery end"])

    col_daily_ref = find_col(
        df,
        [
            "Daily Reference Price [EUR/MWh]",
            "Daily Reference Price EUR/MWh",
            "Reference Price [EUR/MWh]",
            "Reference Price EUR/MWh",
            "Precio de Referencia Diario [EUR/MWh]",
        ],
    )
    col_eod = find_col(
        df,
        [
            "EOD Price [EUR/MWh]",
            "EOD Price EUR/MWh",
            "End of Day Price [EUR/MWh]",
            "Last Daily Price [EUR/MWh]",
            "Last Daily Price EUR/MWh",
            "Last Price [EUR/MWh]",
        ],
    )
    col_last_daily = find_col(df, ["Last Daily Price [EUR/MWh]", "Last Daily Price EUR/MWh"])
    col_daily_auction = find_col(df, ["Daily Auction Price [EUR/MWh]", "Daily Auction Price EUR/MWh"])
    col_volume = find_col(df, ["Daily Volume Traded [MWh]", "Daily Volume Traded MWh", "Volume", "Volumen"])

    if col_trading_day is None or col_product is None:
        # This is usually not the trading data sheet.
        return pd.DataFrame()

    out = pd.DataFrame()
    out["trading_day"] = pd.to_datetime(df[col_trading_day], dayfirst=True, errors="coerce")
    out["product"] = df[col_product].astype(str).str.strip()

    if col_area is not None:
        out["area"] = df[col_area].astype(str).str.strip()
    else:
        out["area"] = None

    if col_place is not None:
        out["place_of_delivery"] = df[col_place].astype(str).str.strip()
    else:
        out["place_of_delivery"] = None

    if col_delivery_start is not None:
        out["delivery_start"] = pd.to_datetime(df[col_delivery_start], dayfirst=True, errors="coerce")
    else:
        out["delivery_start"] = pd.NaT

    if col_delivery_end is not None:
        out["delivery_end"] = pd.to_datetime(df[col_delivery_end], dayfirst=True, errors="coerce")
    else:
        out["delivery_end"] = pd.NaT

    if col_daily_ref is not None:
        out["daily_reference_price_eur_mwh"] = parse_number_series(df[col_daily_ref])
    else:
        out["daily_reference_price_eur_mwh"] = pd.NA

    if col_eod is not None:
        out["eod_price_eur_mwh"] = parse_number_series(df[col_eod])
    else:
        out["eod_price_eur_mwh"] = pd.NA

    if col_last_daily is not None:
        out["last_daily_price_eur_mwh"] = parse_number_series(df[col_last_daily])
    else:
        out["last_daily_price_eur_mwh"] = pd.NA

    if col_daily_auction is not None:
        out["daily_auction_price_eur_mwh"] = parse_number_series(df[col_daily_auction])
    else:
        out["daily_auction_price_eur_mwh"] = pd.NA

    if col_volume is not None:
        out["daily_volume_traded_mwh"] = parse_number_series(df[col_volume])
    else:
        out["daily_volume_traded_mwh"] = pd.NA

    out["source_file"] = source_file
    out = out.dropna(subset=["trading_day"]).copy()

    # Clean obvious garbage product strings.
    out = out[out["product"].notna() & (out["product"].astype(str).str.lower() != "nan")].copy()

    return out.reset_index(drop=True)

# =========================================================
# LOCAL FILE LOADERS
# =========================================================
def read_excel_trading_sheets(path_or_bytes, source_file: str) -> pd.DataFrame:
    """Read the correct MIBGAS trading sheet. Fallback to all sheets if needed."""
    try:
        xls = pd.ExcelFile(path_or_bytes)
    except Exception as e:
        raise ValueError(f"Could not open Excel file: {e}")

    sheet_names = xls.sheet_names
    preferred = []
    for sh in sheet_names:
        sh_norm = normalize_text(sh)
        if "trading" in sh_norm and ("pvb" in sh_norm or "vtp" in sh_norm):
            preferred.append(sh)
    for sh in sheet_names:
        sh_norm = normalize_text(sh)
        if sh not in preferred and "trading" in sh_norm:
            preferred.append(sh)

    # Try preferred sheets first, then every sheet.
    ordered_sheets = preferred + [s for s in sheet_names if s not in preferred]

    frames = []
    errors = []
    for sheet in ordered_sheets:
        try:
            raw = pd.read_excel(path_or_bytes, sheet_name=sheet)
            std = standardize_mibgas_raw(raw, source_file=f"{source_file}/{sheet}")
            if not std.empty:
                frames.append(std)
        except Exception as e:
            errors.append(f"{sheet}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def read_csv_like_bytes(content: bytes, source_file: str) -> pd.DataFrame:
    frames = []
    for enc in ["utf-8", "latin1", "cp1252"]:
        for sep in [";", ",", "\t", "|"]:
            try:
                raw = pd.read_csv(BytesIO(content), sep=sep, encoding=enc, engine="python")
                if raw.shape[1] <= 1:
                    continue
                std = standardize_mibgas_raw(raw, source_file=source_file)
                if not std.empty:
                    frames.append(std)
                    return pd.concat(frames, ignore_index=True)
            except Exception:
                continue
    return pd.DataFrame()


def read_mibgas_file_from_bytes(content: bytes, filename: str) -> pd.DataFrame:
    lower = filename.lower()
    if lower.endswith((".xlsx", ".xls")):
        return read_excel_trading_sheets(BytesIO(content), filename)

    if lower.endswith(".zip"):
        frames = []
        with zipfile.ZipFile(BytesIO(content)) as z:
            for inner in z.namelist():
                if inner.endswith("/"):
                    continue
                inner_lower = inner.lower()
                if not inner_lower.endswith((".xlsx", ".xls", ".csv", ".txt")):
                    continue
                with z.open(inner) as f:
                    inner_content = f.read()
                std = read_mibgas_file_from_bytes(inner_content, f"{filename}/{inner}")
                if not std.empty:
                    frames.append(std)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if lower.endswith((".csv", ".txt")):
        return read_csv_like_bytes(content, filename)

    return pd.DataFrame()


@st.cache_data(show_spinner=True)
def load_local_mibgas_data() -> pd.DataFrame:
    """Load historical files from /data/MIBGAS_Data_*.xlsx for 2021-2025."""
    files = []
    for year in range(LOCAL_START_YEAR, LOCAL_END_YEAR + 1):
        files.extend(sorted(DATA_DIR.glob(f"MIBGAS_Data_{year}.xlsx")))
        files.extend(sorted(DATA_DIR.glob(f"MIBGAS_Data_{year}.xls")))
        files.extend(sorted(DATA_DIR.glob(f"MIBGAS_Data_{year}.csv")))

    frames = []
    read_errors = []

    for path in files:
        try:
            if path.suffix.lower() in [".xlsx", ".xls"]:
                df = read_excel_trading_sheets(path, path.name)
            else:
                df = read_csv_like_bytes(path.read_bytes(), path.name)

            if df.empty:
                read_errors.append(f"{path.name}: no usable trading sheet/data found")
                continue
            frames.append(df)
        except Exception as e:
            read_errors.append(f"{path.name}: {e}")

    if read_errors:
        st.session_state["mibgas_local_read_errors"] = read_errors
    else:
        st.session_state["mibgas_local_read_errors"] = []

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["trading_day"] = pd.to_datetime(out["trading_day"], errors="coerce")
    out = out.dropna(subset=["trading_day"])
    out = out[out["trading_day"].dt.year.between(LOCAL_START_YEAR, LOCAL_END_YEAR)].copy()
    out = out.drop_duplicates(subset=["trading_day", "product", "area", "source_file"], keep="last")
    return out.sort_values(["trading_day", "product"]).reset_index(drop=True)

# =========================================================
# SFTP HELPERS
# =========================================================
def load_private_key_from_secrets():
    if paramiko is None:
        raise ValueError("paramiko is not installed. Add paramiko to requirements.txt.")

    key_text = st.secrets.get("MIBGAS_SFTP_KEY", None)
    if not key_text:
        return None

    key_text = str(key_text).strip()

    # Do not support raw PuTTY keys in secrets. They must be converted to PEM/OpenSSH first.
    if key_text.startswith("PuTTY-User-Key-File"):
        raise ValueError("The key is still in PuTTY .ppk format. Convert it to PEM/OpenSSH before pasting it in Secrets.")

    loaders = [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]
    last_error = None
    for loader in loaders:
        try:
            return loader.from_private_key(StringIO(key_text))
        except Exception as e:
            last_error = e

    raise ValueError(f"Could not load private key from Streamlit Secrets: {last_error}")


def connect_mibgas_sftp():
    if paramiko is None:
        raise ValueError("paramiko is not installed. Add paramiko to requirements.txt.")

    host = st.secrets.get("MIBGAS_SFTP_HOST", None)
    port = int(st.secrets.get("MIBGAS_SFTP_PORT", 22))
    user = st.secrets.get("MIBGAS_SFTP_USER", None)
    password = st.secrets.get("MIBGAS_SFTP_PASSWORD", None)

    if not host:
        raise ValueError("MIBGAS_SFTP_HOST is missing in Streamlit Secrets.")
    if not user:
        raise ValueError("MIBGAS_SFTP_USER is missing in Streamlit Secrets.")

    host = str(host).strip().replace("sftp://", "").replace("/", "")
    user = str(user).strip()

    key = load_private_key_from_secrets()

    transport = paramiko.Transport((host, port))
    if key is not None:
        transport.connect(username=user, pkey=key)
    elif password:
        transport.connect(username=user, password=password)
    else:
        raise ValueError("No MIBGAS_SFTP_KEY or MIBGAS_SFTP_PASSWORD found in Streamlit Secrets.")

    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def mibgas_base_path() -> str:
    return str(st.secrets.get("MIBGAS_SFTP_BASE_PATH", "/secureftpbucket.omie.es/MIBGAS")).rstrip("/")


def is_sftp_file(attr) -> bool:
    return stat.S_ISREG(attr.st_mode)


def is_sftp_dir(attr) -> bool:
    return stat.S_ISDIR(attr.st_mode)


@st.cache_data(show_spinner=False, ttl=1800)
def list_remote_files_for_year(year: int = LIVE_YEAR) -> pd.DataFrame:
    """List files in AGNO_YYYY plus common subfolders like XLS/CSV."""
    base = mibgas_base_path()

    candidate_dirs = [
        f"{base}/AGNO_{year}",
        f"{base}/AGNO_{year}/XLS",
        f"{base}/AGNO_{year}/CSV",
        f"{base}/AGNO_{year}/TXT",
    ]

    # Also include common alternative base paths in case secrets only contain /MIBGAS.
    alternative_bases = [
        "/secureftpbucket.omie.es/MIBGAS",
        "/MIBGAS",
    ]
    for alt_base in alternative_bases:
        if alt_base.rstrip("/") != base:
            candidate_dirs.extend([
                f"{alt_base}/AGNO_{year}",
                f"{alt_base}/AGNO_{year}/XLS",
                f"{alt_base}/AGNO_{year}/CSV",
                f"{alt_base}/AGNO_{year}/TXT",
            ])

    rows = []
    checked_dirs = []
    errors = []

    sftp, transport = connect_mibgas_sftp()
    try:
        for remote_dir in candidate_dirs:
            if remote_dir in checked_dirs:
                continue
            checked_dirs.append(remote_dir)
            try:
                items = sftp.listdir_attr(remote_dir)
            except Exception as e:
                errors.append(f"{remote_dir}: {e}")
                continue

            for item in items:
                if not is_sftp_file(item):
                    continue
                filename = item.filename
                lower = filename.lower()
                if not lower.endswith((".xlsx", ".xls", ".csv", ".txt", ".zip")):
                    continue
                if "mibgas" not in lower and "gas" not in lower:
                    continue
                rows.append({
                    "year": year,
                    "filename": filename,
                    "remote_dir": remote_dir,
                    "remote_path": f"{remote_dir}/{filename}",
                    "size_bytes": item.st_size,
                    "modified": pd.to_datetime(item.st_mtime, unit="s", errors="coerce"),
                })
    finally:
        sftp.close()
        transport.close()

    st.session_state["mibgas_sftp_checked_dirs"] = checked_dirs
    st.session_state["mibgas_sftp_list_errors"] = errors

    if not rows:
        return pd.DataFrame(columns=["year", "filename", "remote_dir", "remote_path", "size_bytes", "modified"])

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["remote_path"])
        .sort_values(["modified", "filename"])
        .reset_index(drop=True)
    )


def read_remote_file_bytes(remote_path: str) -> bytes:
    sftp, transport = connect_mibgas_sftp()
    try:
        with sftp.open(remote_path, "rb") as f:
            return f.read()
    finally:
        sftp.close()
        transport.close()


@st.cache_data(show_spinner=True, ttl=1800)
def load_sftp_2026_data() -> pd.DataFrame:
    files = list_remote_files_for_year(LIVE_YEAR)
    if files.empty:
        return pd.DataFrame()

    frames = []
    errors = []
    for _, row in files.iterrows():
        remote_path = row["remote_path"]
        filename = row["filename"]
        try:
            content = read_remote_file_bytes(remote_path)
            df = read_mibgas_file_from_bytes(content, filename)
            if df.empty:
                errors.append(f"{filename}: no usable trading data found")
                continue
            frames.append(df)
        except Exception as e:
            errors.append(f"{filename}: {e}")

    st.session_state["mibgas_sftp_read_errors"] = errors

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["trading_day"] = pd.to_datetime(out["trading_day"], errors="coerce")
    out = out.dropna(subset=["trading_day"])
    out = out[out["trading_day"].dt.year == LIVE_YEAR].copy()
    out = out.drop_duplicates(subset=["trading_day", "product", "area", "source_file"], keep="last")

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out.to_csv(CACHE_FILE_2026, index=False)
    except Exception:
        pass

    return out.sort_values(["trading_day", "product"]).reset_index(drop=True)


def load_cached_2026_data() -> pd.DataFrame:
    if not CACHE_FILE_2026.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(CACHE_FILE_2026)
        df["trading_day"] = pd.to_datetime(df["trading_day"], errors="coerce")
        for col in [
            "daily_reference_price_eur_mwh", "eod_price_eur_mwh", "last_daily_price_eur_mwh",
            "daily_auction_price_eur_mwh", "daily_volume_traded_mwh",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["trading_day"])
    except Exception:
        return pd.DataFrame()

# =========================================================
# DATASET BUILDERS
# =========================================================
def load_all_raw_data() -> tuple[pd.DataFrame, str]:
    hist = load_local_mibgas_data()

    live_status = ""
    try:
        live = load_sftp_2026_data()
        if live.empty:
            live_status = "Connected to SFTP, but no 2026 MIBGAS trading files were loaded."
        else:
            live_status = f"Loaded {len(live):,} rows from 2026 SFTP."
    except Exception as e:
        live = load_cached_2026_data()
        if live.empty:
            live_status = f"2026 SFTP data not loaded: {e}"
        else:
            live_status = f"Using cached 2026 data because SFTP failed: {e}"

    combined = pd.concat([hist, live], ignore_index=True)
    if combined.empty:
        return combined, live_status

    combined["trading_day"] = pd.to_datetime(combined["trading_day"], errors="coerce")
    combined = combined.dropna(subset=["trading_day"])
    combined = combined.drop_duplicates(subset=["trading_day", "product", "area", "source_file"], keep="last")
    combined = combined.sort_values(["trading_day", "product"]).reset_index(drop=True)
    return combined, live_status


def build_actuals(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "price", "source_file"])

    tmp = raw.copy()
    tmp["product_norm"] = tmp["product"].astype(str).str.strip().str.upper()
    if "area" in tmp.columns:
        tmp["area_norm"] = tmp["area"].astype(str).str.strip().str.upper()
    else:
        tmp["area_norm"] = ""

    # Main actual daily product. Area filter is loose because some sheets may not have area.
    mask = tmp["product_norm"].eq("GDAES_D+1")
    mask &= tmp["area_norm"].isin(["ES", "", "NONE", "NAN"]) | tmp["area_norm"].isna()

    actuals = tmp[mask].copy()
    actuals["price"] = pd.to_numeric(actuals["daily_reference_price_eur_mwh"], errors="coerce")

    out = actuals[["trading_day", "product", "price", "source_file"]].rename(columns={"trading_day": "date"})
    out["series"] = "GDAES_D+1 Daily Reference Price"
    out = out.dropna(subset=["date", "price"]).sort_values("date")
    return out.drop_duplicates(subset=["date", "product"], keep="last").reset_index(drop=True)


def build_forwards(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "price", "delivery_start", "delivery_end", "source_file"])

    tmp = raw.copy()
    tmp["product_norm"] = tmp["product"].astype(str).str.strip().str.upper()
    if "area" in tmp.columns:
        tmp["area_norm"] = tmp["area"].astype(str).str.strip().str.upper()
    else:
        tmp["area_norm"] = ""

    mask = tmp["product_norm"].isin(["GYES_Y+1", "GYES_Y+2"])
    mask &= tmp["area_norm"].isin(["ES", "", "NONE", "NAN"]) | tmp["area_norm"].isna()
    fw = tmp[mask].copy()

    # Prefer EOD. If blank, fallback to last daily, then reference.
    fw["price"] = pd.to_numeric(fw["eod_price_eur_mwh"], errors="coerce")
    fw["price"] = fw["price"].combine_first(pd.to_numeric(fw.get("last_daily_price_eur_mwh"), errors="coerce"))
    fw["price"] = fw["price"].combine_first(pd.to_numeric(fw.get("daily_reference_price_eur_mwh"), errors="coerce"))

    out = fw[["trading_day", "product", "price", "delivery_start", "delivery_end", "source_file"]].rename(columns={"trading_day": "date"})
    out["series"] = out["product"].astype(str) + " EOD Price"
    out = out.dropna(subset=["date", "price"]).sort_values(["date", "product"])
    return out.drop_duplicates(subset=["date", "product"], keep="last").reset_index(drop=True)

# =========================================================
# GRANULARITY / CHARTS
# =========================================================
def apply_granularity(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "price"])

    group_cols = ["product"] if "product" in out.columns else []
    if "series" in out.columns:
        group_cols.append("series")

    if granularity == "Daily":
        out["plot_date"] = out["date"]
        out["plot_label"] = out["plot_date"].dt.strftime("%Y-%m-%d")
        return out

    if granularity == "Rolling 30D average":
        frames = []
        if group_cols:
            grouped = out.groupby(group_cols, dropna=False)
            for keys, g in grouped:
                g = g.sort_values("date").copy()
                g["price"] = g["price"].rolling(window=30, min_periods=1).mean()
                g["plot_date"] = g["date"]
                g["plot_label"] = g["plot_date"].dt.strftime("%Y-%m-%d")
                frames.append(g)
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        out = out.sort_values("date").copy()
        out["price"] = out["price"].rolling(window=30, min_periods=1).mean()
        out["plot_date"] = out["date"]
        out["plot_label"] = out["plot_date"].dt.strftime("%Y-%m-%d")
        return out

    if granularity == "Monthly":
        out["plot_date"] = out["date"].dt.to_period("M").dt.to_timestamp()
    elif granularity == "Annual":
        out["plot_date"] = out["date"].dt.to_period("Y").dt.to_timestamp()
    else:
        out["plot_date"] = out["date"]

    agg_cols = group_cols + ["plot_date"]
    aggregated = out.groupby(agg_cols, as_index=False).agg(price=("price", "mean"))
    if granularity == "Monthly":
        aggregated["plot_label"] = aggregated["plot_date"].dt.strftime("%b-%Y")
    elif granularity == "Annual":
        aggregated["plot_label"] = aggregated["plot_date"].dt.year.astype(str)
    else:
        aggregated["plot_label"] = aggregated["plot_date"].dt.strftime("%Y-%m-%d")
    return aggregated.sort_values("plot_date").reset_index(drop=True)


def build_actuals_chart(actuals: pd.DataFrame, granularity: str):
    plot = apply_granularity(actuals, granularity)
    if plot.empty:
        return None

    if granularity == "Annual":
        x_enc = alt.X("plot_label:N", title=None, sort=plot["plot_label"].tolist(), axis=alt.Axis(labelAngle=0))
    else:
        x_enc = alt.X("plot_date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0))

    chart = (
        alt.Chart(plot)
        .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
        .encode(
            x=x_enc,
            y=alt.Y("price:Q", title="€/MWh"),
            tooltip=[
                alt.Tooltip("plot_label:N", title="Period"),
                alt.Tooltip("price:Q", title="Price €/MWh", format=",.2f"),
                alt.Tooltip("product:N", title="Product"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=380)


def build_forwards_chart(forwards: pd.DataFrame, granularity: str):
    plot = apply_granularity(forwards, granularity)
    if plot.empty:
        return None

    if granularity == "Annual":
        x_enc = alt.X("plot_label:N", title=None, sort=sorted(plot["plot_label"].unique().tolist()), axis=alt.Axis(labelAngle=0))
    else:
        x_enc = alt.X("plot_date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0))

    chart = (
        alt.Chart(plot)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=x_enc,
            y=alt.Y("price:Q", title="€/MWh"),
            color=alt.Color("product:N", title="Product", scale=alt.Scale(range=[YELLOW_DARK, BLUE_PRICE, CORP_GREEN])),
            tooltip=[
                alt.Tooltip("plot_label:N", title="Period"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("price:Q", title="Price €/MWh", format=",.2f"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=380)

# =========================================================
# PAGE
# =========================================================
st.title("MIBGAS - Spain Gas Prices")
st.caption(
    "Historical files are loaded from `/data/MIBGAS_Data_*.xlsx` from 2021 to 2025. "
    "2026 files are loaded from MIBGAS SFTP when Streamlit Secrets are configured."
)

section_header("MIBGAS market data")

col_refresh, col_status = st.columns([1.1, 5])
with col_refresh:
    if st.button("Refresh MIBGAS SFTP data"):
        try:
            load_sftp_2026_data.clear()
            list_remote_files_for_year.clear()
        except Exception:
            pass
        st.rerun()

raw, live_status = load_all_raw_data()
with col_status:
    st.caption(live_status)

if raw.empty:
    st.warning("No MIBGAS data found. Upload files named `MIBGAS_Data_2021.xlsx` ... `MIBGAS_Data_2025.xlsx` to `/data`.")

    with st.expander("Diagnostics"):
        st.write("Local read errors:")
        st.write(st.session_state.get("mibgas_local_read_errors", []))
        st.write("SFTP checked dirs:")
        st.write(st.session_state.get("mibgas_sftp_checked_dirs", []))
        st.write("SFTP list errors:")
        st.write(st.session_state.get("mibgas_sftp_list_errors", []))
        st.write("SFTP read errors:")
        st.write(st.session_state.get("mibgas_sftp_read_errors", []))
    st.stop()

actuals = build_actuals(raw)
forwards = build_forwards(raw)

all_dates = []
if not actuals.empty:
    all_dates.extend([actuals["date"].min(), actuals["date"].max()])
if not forwards.empty:
    all_dates.extend([forwards["date"].min(), forwards["date"].max()])

if not all_dates:
    st.warning("The files were read, but no GDAES_D+1 actuals or GYES_Y+1/Y+2 forwards were found.")
    st.dataframe(raw.head(200), use_container_width=True, hide_index=True)
    st.stop()

min_date = pd.to_datetime(min(all_dates)).date()
max_date = pd.to_datetime(max(all_dates)).date()

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
with col2:
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

actuals_f = actuals[(actuals["date"].dt.date >= start_date) & (actuals["date"].dt.date <= end_date)].copy()
forwards_f = forwards[(forwards["date"].dt.date >= start_date) & (forwards["date"].dt.date <= end_date)].copy()

latest_actual = actuals_f.sort_values("date").iloc[-1] if not actuals_f.empty else None
latest_y1 = forwards_f[forwards_f["product"].str.upper() == "GYES_Y+1"].sort_values("date").iloc[-1] if not forwards_f[forwards_f["product"].str.upper() == "GYES_Y+1"].empty else None
latest_y2 = forwards_f[forwards_f["product"].str.upper() == "GYES_Y+2"].sort_values("date").iloc[-1] if not forwards_f[forwards_f["product"].str.upper() == "GYES_Y+2"].empty else None

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Latest GDAES D+1", f"{format_metric(latest_actual['price'] if latest_actual is not None else None, ' €/MWh')}")
kpi2.metric("Latest actual date", latest_actual["date"].strftime("%Y-%m-%d") if latest_actual is not None else "-")
kpi3.metric("Latest GYES Y+1", f"{format_metric(latest_y1['price'] if latest_y1 is not None else None, ' €/MWh')}")
kpi4.metric("Latest GYES Y+2", f"{format_metric(latest_y2['price'] if latest_y2 is not None else None, ' €/MWh')}")

st.markdown("---")

# Main actuals chart
st.subheader("Actuals - GDAES D+1 Daily Reference Price")
g1_col, _ = st.columns([1, 3])
with g1_col:
    gran_actuals = st.radio(
        "Actuals granularity",
        options=["Daily", "Rolling 30D average", "Monthly", "Annual"],
        index=0,
        horizontal=False,
    )
chart = build_actuals_chart(actuals_f, gran_actuals)
if chart is None:
    st.warning("No GDAES_D+1 actual data available for the selected period.")
else:
    st.altair_chart(chart, use_container_width=True)

# Main forwards chart
st.subheader("Forwards - GYES Y+1 / Y+2 EOD Price")
g2_col, _ = st.columns([1, 3])
with g2_col:
    gran_forwards = st.radio(
        "Forwards granularity",
        options=["Daily", "Rolling 30D average", "Monthly", "Annual"],
        index=0,
        horizontal=False,
    )
chart = build_forwards_chart(forwards_f, gran_forwards)
if chart is None:
    st.warning("No GYES_Y+1 / GYES_Y+2 forward data available for the selected period.")
else:
    st.altair_chart(chart, use_container_width=True)

# Data tabs
raw_tab, actuals_tab, forwards_tab, sftp_tab, diagnostics_tab = st.tabs([
    "Raw data", "Actuals data", "Forwards data", "SFTP files", "Diagnostics"
])

with raw_tab:
    st.dataframe(raw.sort_values(["trading_day", "product"], ascending=[False, True]), use_container_width=True, hide_index=True)

with actuals_tab:
    st.dataframe(actuals_f.sort_values("date", ascending=False), use_container_width=True, hide_index=True)
    st.download_button(
        "Download actuals CSV",
        actuals_f.to_csv(index=False).encode("utf-8"),
        file_name="mibgas_gdaes_d1_actuals.csv",
        mime="text/csv",
    )

with forwards_tab:
    st.dataframe(forwards_f.sort_values(["date", "product"], ascending=[False, True]), use_container_width=True, hide_index=True)
    st.download_button(
        "Download forwards CSV",
        forwards_f.to_csv(index=False).encode("utf-8"),
        file_name="mibgas_gyes_y1_y2_forwards.csv",
        mime="text/csv",
    )

with sftp_tab:
    st.write("Remote files detected for 2026:")
    try:
        remote_files = list_remote_files_for_year(LIVE_YEAR)
        st.dataframe(remote_files, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not list 2026 SFTP files: {e}")

with diagnostics_tab:
    st.write("Local read errors:")
    st.write(st.session_state.get("mibgas_local_read_errors", []))

    st.write("SFTP checked directories:")
    st.write(st.session_state.get("mibgas_sftp_checked_dirs", []))

    st.write("SFTP list errors:")
    st.write(st.session_state.get("mibgas_sftp_list_errors", []))

    st.write("SFTP read errors:")
    st.write(st.session_state.get("mibgas_sftp_read_errors", []))

    st.write("Products found:")
    if "product" in raw.columns:
        st.dataframe(pd.DataFrame({"product": sorted(raw["product"].dropna().astype(str).unique().tolist())}), use_container_width=True, hide_index=True)
