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
# PAGE CONFIG
# =========================================================
st.set_page_config(page_title="MIBGAS", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

LOCAL_START_YEAR = 2021
LIVE_YEAR = 2026

MIBGAS_CACHE_FILE = DATA_DIR / "mibgas_2026_cache.csv"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_DARK = "#D97706"
GREY_SHADE = "#F3F4F6"


# =========================================================
# DISPLAY HELPERS
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
# GENERIC PARSING HELPERS
# =========================================================
def normalize_column_name(col: str) -> str:
    return (
        str(col)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(".", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("[", "")
        .replace("]", "")
        .replace("%", "pct")
    )


def find_first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm_map = {normalize_column_name(c): c for c in df.columns}
    for cand in candidates:
        cand_norm = normalize_column_name(cand)
        if cand_norm in norm_map:
            return norm_map[cand_norm]
    return None


def parse_number_series(s: pd.Series) -> pd.Series:
    """
    Handles Spanish and English number formats:
    34,56 -> 34.56
    1.234,56 -> 1234.56
    1,234.56 -> 1234.56
    """
    txt = s.astype(str).str.strip()
    txt = txt.str.replace("€", "", regex=False)
    txt = txt.str.replace(" ", "", regex=False)
    txt = txt.str.replace("\u00a0", "", regex=False)

    # If both '.' and ',' appear, infer decimal separator from the last one.
    def clean_one(x: str) -> str:
        if x.lower() in {"nan", "none", "", "nat"}:
            return ""
        comma_pos = x.rfind(",")
        dot_pos = x.rfind(".")
        if comma_pos >= 0 and dot_pos >= 0:
            if comma_pos > dot_pos:
                # Spanish: 1.234,56
                return x.replace(".", "").replace(",", ".")
            # English: 1,234.56
            return x.replace(",", "")
        if comma_pos >= 0:
            return x.replace(".", "").replace(",", ".")
        return x

    return pd.to_numeric(txt.map(clean_one), errors="coerce")


def clean_product_name(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.replace(" ", "", regex=False)


def read_excel_all_sheets_from_bytes(content: bytes, source_file: str) -> pd.DataFrame:
    buffer = BytesIO(content)
    xls = pd.ExcelFile(buffer)
    frames = []
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(BytesIO(content), sheet_name=sheet)
            if not df.empty:
                df["source_file"] = f"{source_file}/{sheet}"
                frames.append(df)
        except Exception:
            pass
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def read_tabular_file_from_bytes(content: bytes, filename: str) -> pd.DataFrame:
    filename_lower = filename.lower()

    if filename_lower.endswith((".xlsx", ".xls")):
        return read_excel_all_sheets_from_bytes(content, filename)

    if filename_lower.endswith(".zip"):
        frames = []
        with zipfile.ZipFile(BytesIO(content)) as z:
            for inner_name in z.namelist():
                inner_lower = inner_name.lower()
                if not inner_lower.endswith((".xlsx", ".xls", ".csv", ".txt")):
                    continue
                with z.open(inner_name) as f:
                    inner_content = f.read()
                inner_df = read_tabular_file_from_bytes(inner_content, f"{filename}/{inner_name}")
                if not inner_df.empty:
                    frames.append(inner_df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    for enc in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        for sep in [";", ",", "\t", "|"]:
            try:
                df = pd.read_csv(
                    BytesIO(content),
                    sep=sep,
                    encoding=enc,
                    decimal=",",
                    engine="python",
                )
                if df.shape[1] > 1:
                    df["source_file"] = filename
                    return df
            except Exception:
                pass

    return pd.DataFrame()


def read_local_tabular_file(path: Path) -> pd.DataFrame:
    try:
        return read_tabular_file_from_bytes(path.read_bytes(), path.name)
    except Exception:
        return pd.DataFrame()


# =========================================================
# MIBGAS NORMALIZATION FOR YOUR FILE FORMAT
# =========================================================
def normalize_mibgas_raw(raw: pd.DataFrame, source_file: str = "unknown") -> pd.DataFrame:
    """
    Normalizes MIBGAS files like:
    Trading day | Product | Place of delivery | Area | First Day Delivery | Last Day Delivery |
    Daily Reference Price [EUR/MWh] | ... | EOD Price [EUR/MWh]
    """
    if raw.empty:
        return pd.DataFrame()

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Remove fully empty rows and columns.
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # Some files can have header rows not detected. Try to detect a row containing Trading day/Product.
    if "Trading day" not in df.columns and "Product" not in df.columns:
        for idx in range(min(10, len(df))):
            row_values = [str(x).strip() for x in df.iloc[idx].tolist()]
            joined = "|".join(row_values).lower()
            if "trading day" in joined and "product" in joined:
                new_cols = row_values
                df = df.iloc[idx + 1 :].copy()
                df.columns = new_cols
                df = df.dropna(how="all").dropna(axis=1, how="all")
                break

    trading_day_col = find_first_existing_col(df, ["Trading day", "Trading Day", "Fecha", "Date"])
    product_col = find_first_existing_col(df, ["Product", "Producto"])
    area_col = find_first_existing_col(df, ["Area", "Área"])
    pod_col = find_first_existing_col(df, ["Place of delivery", "Place Delivery", "PVB/VTP", "Hub"])
    first_delivery_col = find_first_existing_col(df, ["First Day Delivery", "First delivery", "Delivery start"])
    last_delivery_col = find_first_existing_col(df, ["Last Day Delivery", "Last delivery", "Delivery end"])

    daily_reference_col = find_first_existing_col(
        df,
        [
            "Daily Reference Price [EUR/MWh]",
            "Daily Reference Price",
            "Reference Price",
            "Precio Referencia Diario",
        ],
    )
    eod_col = find_first_existing_col(
        df,
        [
            "EOD Price [EUR/MWh]",
            "EOD Price",
            "End of Day Price",
            "Precio EOD",
        ],
    )
    daily_volume_col = find_first_existing_col(
        df,
        [
            "Daily Volume Traded [MWh]",
            "Daily Volume Traded",
            "Volume",
            "Volumen",
        ],
    )

    if trading_day_col is None or product_col is None:
        raise ValueError(f"Could not identify required columns. Columns found: {df.columns.tolist()}")

    out = pd.DataFrame()
    out["trading_day"] = pd.to_datetime(df[trading_day_col], dayfirst=True, errors="coerce")
    out["product"] = clean_product_name(df[product_col])

    if area_col is not None:
        out["area"] = df[area_col].astype(str).str.strip()
    else:
        out["area"] = pd.NA

    if pod_col is not None:
        out["place_of_delivery"] = df[pod_col].astype(str).str.strip()
    else:
        out["place_of_delivery"] = pd.NA

    if first_delivery_col is not None:
        out["delivery_start"] = pd.to_datetime(df[first_delivery_col], dayfirst=True, errors="coerce")
    else:
        out["delivery_start"] = pd.NaT

    if last_delivery_col is not None:
        out["delivery_end"] = pd.to_datetime(df[last_delivery_col], dayfirst=True, errors="coerce")
    else:
        out["delivery_end"] = pd.NaT

    if daily_reference_col is not None:
        out["daily_reference_price_eur_mwh"] = parse_number_series(df[daily_reference_col])
    else:
        out["daily_reference_price_eur_mwh"] = pd.NA

    if eod_col is not None:
        out["eod_price_eur_mwh"] = parse_number_series(df[eod_col])
    else:
        out["eod_price_eur_mwh"] = pd.NA

    if daily_volume_col is not None:
        out["daily_volume_traded_mwh"] = parse_number_series(df[daily_volume_col])
    else:
        out["daily_volume_traded_mwh"] = pd.NA

    if "source_file" in df.columns:
        out["source_file"] = df["source_file"].astype(str)
    else:
        out["source_file"] = source_file

    out = out.dropna(subset=["trading_day", "product"]).copy()
    return out.reset_index(drop=True)


# =========================================================
# LOCAL HISTORICAL LOADER: /data/MIBGAS_Data_2021.xlsx etc.
# =========================================================
@st.cache_data(show_spinner=True)
def load_local_mibgas_data() -> pd.DataFrame:
    """
    Reads files from /data whose name contains MIBGAS or gas.
    Expected: MIBGAS_Data_2021.xlsx ... MIBGAS_Data_2025.xlsx
    """
    if not DATA_DIR.exists():
        return pd.DataFrame()

    files = []
    for path in DATA_DIR.iterdir():
        if not path.is_file():
            continue
        if path.suffix.lower() not in [".xlsx", ".xls", ".csv", ".txt", ".zip"]:
            continue
        name = path.name.lower()
        if "mibgas" in name or re.search(r"(^|[_\- ])gas([_\- ]|$)", name):
            # Avoid reading the generated cache twice as historical raw data.
            if path.name == MIBGAS_CACHE_FILE.name:
                continue
            files.append(path)

    frames = []
    for path in sorted(files):
        try:
            raw = read_local_tabular_file(path)
            if raw.empty:
                continue
            norm = normalize_mibgas_raw(raw, source_file=path.name)
            frames.append(norm)
        except Exception as e:
            st.warning(f"Could not process local MIBGAS file {path.name}: {e}")

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["trading_day", "product"])
    out = out[
        (out["trading_day"].dt.year >= LOCAL_START_YEAR)
        & (out["trading_day"].dt.year < LIVE_YEAR)
    ].copy()
    out = out.sort_values(["trading_day", "product", "source_file"]).reset_index(drop=True)
    return out


# =========================================================
# SFTP / WINscp CONNECTION VIA STREAMLIT SECRETS
# =========================================================
def get_secret_value(name: str, default=None):
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


def load_mibgas_private_key_from_secrets():
    if paramiko is None:
        raise ImportError("paramiko is not installed. Add 'paramiko' to requirements.txt")

    key_text = get_secret_value("MIBGAS_SFTP_KEY")
    if not key_text:
        return None

    key_file = StringIO(str(key_text).strip())
    loaders = [
        paramiko.Ed25519Key,
        paramiko.RSAKey,
        paramiko.ECDSAKey,
        paramiko.DSSKey,
    ]

    last_error = None
    for loader in loaders:
        try:
            key_file.seek(0)
            return loader.from_private_key(key_file)
        except Exception as e:
            last_error = e

    raise ValueError(f"Could not load MIBGAS private key from secrets: {last_error}")


def connect_mibgas_sftp():
    if paramiko is None:
        raise ImportError("paramiko is not installed. Add 'paramiko' to requirements.txt")

    host = get_secret_value("MIBGAS_SFTP_HOST", "secureftpbucket.omie.es")
    port = int(get_secret_value("MIBGAS_SFTP_PORT", 22))
    user = get_secret_value("MIBGAS_SFTP_USER")
    password = get_secret_value("MIBGAS_SFTP_PASSWORD")
    key = load_mibgas_private_key_from_secrets()

    if not user:
        raise ValueError("MIBGAS_SFTP_USER is missing in Streamlit Secrets.")

    transport = paramiko.Transport((host, port))
    if key is not None:
        transport.connect(username=user, pkey=key)
    elif password:
        transport.connect(username=user, password=password)
    else:
        raise ValueError("No MIBGAS_SFTP_KEY or MIBGAS_SFTP_PASSWORD found in Streamlit Secrets.")

    return paramiko.SFTPClient.from_transport(transport), transport


def mibgas_remote_base_path() -> str:
    return str(get_secret_value("MIBGAS_SFTP_BASE_PATH", "/MIBGAS")).rstrip("/")


def is_sftp_file(attr) -> bool:
    return stat.S_ISREG(attr.st_mode)


@st.cache_data(show_spinner=False, ttl=1800)
def list_mibgas_remote_files_for_year(year: int = LIVE_YEAR) -> pd.DataFrame:
    base_path = mibgas_remote_base_path()
    year_path = f"{base_path}/AGNO_{year}"

    rows = []
    sftp, transport = connect_mibgas_sftp()
    try:
        for item in sftp.listdir_attr(year_path):
            if is_sftp_file(item):
                rows.append(
                    {
                        "year": year,
                        "filename": item.filename,
                        "remote_path": f"{year_path}/{item.filename}",
                        "size_bytes": item.st_size,
                        "modified": pd.to_datetime(item.st_mtime, unit="s", errors="coerce"),
                    }
                )
    finally:
        sftp.close()
        transport.close()

    if not rows:
        return pd.DataFrame(columns=["year", "filename", "remote_path", "size_bytes", "modified"])
    return pd.DataFrame(rows).sort_values(["modified", "filename"]).reset_index(drop=True)


def read_remote_file_bytes(remote_path: str) -> bytes:
    sftp, transport = connect_mibgas_sftp()
    try:
        with sftp.open(remote_path, "rb") as f:
            return f.read()
    finally:
        sftp.close()
        transport.close()


@st.cache_data(show_spinner=True, ttl=1800)
def load_mibgas_2026_from_sftp() -> pd.DataFrame:
    files_df = list_mibgas_remote_files_for_year(LIVE_YEAR)
    if files_df.empty:
        return pd.DataFrame()

    frames = []
    for _, row in files_df.iterrows():
        filename = row["filename"]
        remote_path = row["remote_path"]
        try:
            content = read_remote_file_bytes(remote_path)
            raw = read_tabular_file_from_bytes(content, filename)
            if raw.empty:
                continue
            norm = normalize_mibgas_raw(raw, source_file=filename)
            frames.append(norm)
        except Exception as e:
            st.warning(f"Could not process remote MIBGAS file {filename}: {e}")

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["trading_day", "product"])
    out = out[out["trading_day"].dt.year == LIVE_YEAR].copy()
    out = out.sort_values(["trading_day", "product", "source_file"]).reset_index(drop=True)

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out.to_csv(MIBGAS_CACHE_FILE, index=False)
    except Exception:
        pass

    return out


def load_cached_mibgas_2026() -> pd.DataFrame:
    if not MIBGAS_CACHE_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(MIBGAS_CACHE_FILE)
    for col in ["trading_day", "delivery_start", "delivery_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


@st.cache_data(show_spinner=True, ttl=900)
def load_all_mibgas_data() -> tuple[pd.DataFrame, str]:
    hist = load_local_mibgas_data()

    live_status = "2026 SFTP not loaded."
    try:
        live = load_mibgas_2026_from_sftp()
        live_status = "2026 data loaded from MIBGAS SFTP."
    except Exception as e:
        live = load_cached_mibgas_2026()
        if live.empty:
            live_status = f"Could not load 2026 from SFTP and no cache was found: {e}"
        else:
            live_status = f"Could not load 2026 from SFTP. Using local cache: {e}"

    combined = pd.concat([hist, live], ignore_index=True)
    if combined.empty:
        return combined, live_status

    combined["trading_day"] = pd.to_datetime(combined["trading_day"], errors="coerce")
    for col in ["delivery_start", "delivery_end"]:
        if col in combined.columns:
            combined[col] = pd.to_datetime(combined[col], errors="coerce")
    for col in ["daily_reference_price_eur_mwh", "eod_price_eur_mwh", "daily_volume_traded_mwh"]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    combined["product"] = clean_product_name(combined["product"])
    combined = combined.dropna(subset=["trading_day", "product"])
    combined = combined.sort_values(["trading_day", "product", "source_file"]).drop_duplicates(
        subset=["trading_day", "product", "area", "place_of_delivery", "delivery_start", "delivery_end"],
        keep="last",
    )
    return combined.reset_index(drop=True), live_status


# =========================================================
# DATASETS REQUESTED BY USER
# =========================================================
def build_actuals_gdaes_d1(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    df = raw.copy()
    df["product_clean"] = clean_product_name(df["product"])

    mask = df["product_clean"].eq("GDAES_D+1")
    if "area" in df.columns:
        mask = mask & (df["area"].isna() | df["area"].astype(str).str.upper().eq("ES"))

    out = df[mask].copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    out = out.rename(
        columns={
            "trading_day": "date",
            "daily_reference_price_eur_mwh": "price",
            "daily_volume_traded_mwh": "volume",
        }
    )
    out = out[["date", "product", "price", "volume", "source_file"]].copy()
    out["series"] = "GDAES D+1 - Daily Reference Price"
    out = out.dropna(subset=["date", "price"]).sort_values("date")
    out = out.drop_duplicates(subset=["date", "product"], keep="last")
    return out.reset_index(drop=True)


def build_forwards_gyes_y1_y2(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "delivery_start", "delivery_end", "price", "source_file"])

    df = raw.copy()
    df["product_clean"] = clean_product_name(df["product"])

    mask = df["product_clean"].isin(["GYES_Y+1", "GYES_Y+2"])
    if "area" in df.columns:
        mask = mask & (df["area"].isna() | df["area"].astype(str).str.upper().eq("ES"))

    out = df[mask].copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "product", "delivery_start", "delivery_end", "price", "source_file"])

    out = out.rename(
        columns={
            "trading_day": "date",
            "eod_price_eur_mwh": "price",
        }
    )
    out = out[["date", "product", "delivery_start", "delivery_end", "price", "source_file"]].copy()
    out["series"] = out["product"].astype(str) + " - EOD Price"
    out = out.dropna(subset=["date", "price"]).sort_values(["date", "product"])
    out = out.drop_duplicates(subset=["date", "product"], keep="last")
    return out.reset_index(drop=True)


# =========================================================
# CHARTS
# =========================================================
def build_actuals_chart(actuals: pd.DataFrame):
    if actuals.empty:
        return None

    chart = (
        alt.Chart(actuals)
        .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
        .encode(
            x=alt.X("date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="Daily Reference Price (€/MWh)"),
            tooltip=[
                alt.Tooltip("date:T", title="Trading day", format="%Y-%m-%d"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("price:Q", title="Daily Reference Price", format=",.2f"),
                alt.Tooltip("volume:Q", title="Daily Volume Traded (MWh)", format=",.0f"),
                alt.Tooltip("source_file:N", title="Source file"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=390)


def build_forward_chart(forwards: pd.DataFrame):
    if forwards.empty:
        return None

    color_scale = alt.Scale(domain=["GYES_Y+1", "GYES_Y+2"], range=[CORP_GREEN_DARK, YELLOW_DARK])

    chart = (
        alt.Chart(forwards)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="EOD Price (€/MWh)"),
            color=alt.Color("product:N", title="Product", scale=color_scale),
            tooltip=[
                alt.Tooltip("date:T", title="Trading day", format="%Y-%m-%d"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("delivery_start:T", title="First Day Delivery", format="%Y-%m-%d"),
                alt.Tooltip("delivery_end:T", title="Last Day Delivery", format="%Y-%m-%d"),
                alt.Tooltip("price:Q", title="EOD Price", format=",.2f"),
                alt.Tooltip("source_file:N", title="Source file"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=390)


def build_monthly_actuals_chart(actuals: pd.DataFrame):
    if actuals.empty:
        return None
    monthly = actuals.copy()
    monthly["month"] = monthly["date"].dt.to_period("M").dt.to_timestamp()
    monthly = monthly.groupby("month", as_index=False)["price"].mean()
    chart = (
        alt.Chart(monthly)
        .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
        .encode(
            x=alt.X("month:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="Monthly avg Daily Reference Price (€/MWh)"),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%b-%Y"),
                alt.Tooltip("price:Q", title="Monthly avg", format=",.2f"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=360)


# =========================================================
# PAGE
# =========================================================
st.title("MIBGAS - Spain Gas Prices")

st.caption(
    "Historical files are loaded from `/data` from 2021 to 2025. "
    "2026 files are loaded from the MIBGAS SFTP / WinSCP connection using Streamlit Secrets."
)

section_header("MIBGAS market data")

with st.expander("Required Streamlit Secrets", expanded=False):
    st.code(
        '''
MIBGAS_SFTP_HOST = "secureftpbucket.omie.es"
MIBGAS_SFTP_PORT = 22
MIBGAS_SFTP_USER = "m.moreno"
MIBGAS_SFTP_BASE_PATH = "/MIBGAS"

MIBGAS_SFTP_KEY = """
-----BEGIN OPENSSH PRIVATE KEY-----
PASTE_THE_FULL_GASkey_CONTENT_HERE
-----END OPENSSH PRIVATE KEY-----
"""
        '''.strip(),
        language="toml",
    )

col_refresh, col_status = st.columns([1.1, 3])
with col_refresh:
    if st.button("Refresh MIBGAS SFTP data"):
        load_mibgas_2026_from_sftp.clear()
        list_mibgas_remote_files_for_year.clear()
        load_all_mibgas_data.clear()
        st.rerun()

raw_all, live_status = load_all_mibgas_data()
with col_status:
    st.caption(live_status)

if raw_all.empty:
    st.warning(
        "No MIBGAS data found. Upload files such as `MIBGAS_Data_2021.xlsx` to `/data`, "
        "and configure Streamlit Secrets for 2026 SFTP data."
    )
    st.stop()

actuals_all = build_actuals_gdaes_d1(raw_all)
forwards_all = build_forwards_gyes_y1_y2(raw_all)

available_dates = []
if not actuals_all.empty:
    available_dates.extend(actuals_all["date"].dropna().tolist())
if not forwards_all.empty:
    available_dates.extend(forwards_all["date"].dropna().tolist())

if not available_dates:
    st.warning("The files were read, but no GDAES_D+1 actuals or GYES_Y+1/Y+2 forwards were found.")
    st.dataframe(raw_all.head(200), use_container_width=True, hide_index=True)
    st.stop()

min_date = pd.to_datetime(min(available_dates)).date()
max_date = pd.to_datetime(max(available_dates)).date()

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
with col2:
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

actuals = actuals_all[
    (actuals_all["date"].dt.date >= start_date) &
    (actuals_all["date"].dt.date <= end_date)
].copy()

forwards = forwards_all[
    (forwards_all["date"].dt.date >= start_date) &
    (forwards_all["date"].dt.date <= end_date)
].copy()

# KPIs
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
if not actuals.empty:
    latest_actual = actuals.sort_values("date").iloc[-1]
    kpi1.metric("Latest GDAES D+1", f"{latest_actual['price']:,.2f} €/MWh")
    kpi2.metric("Latest actual date", latest_actual["date"].strftime("%Y-%m-%d"))
else:
    kpi1.metric("Latest GDAES D+1", "-")
    kpi2.metric("Latest actual date", "-")

if not forwards.empty:
    latest_forward_date = forwards["date"].max()
    latest_forwards = forwards[forwards["date"] == latest_forward_date]
    y1 = latest_forwards.loc[latest_forwards["product"] == "GYES_Y+1", "price"]
    y2 = latest_forwards.loc[latest_forwards["product"] == "GYES_Y+2", "price"]
    kpi3.metric("Latest GYES Y+1", f"{y1.iloc[-1]:,.2f} €/MWh" if not y1.empty else "-")
    kpi4.metric("Latest GYES Y+2", f"{y2.iloc[-1]:,.2f} €/MWh" if not y2.empty else "-")
else:
    kpi3.metric("Latest GYES Y+1", "-")
    kpi4.metric("Latest GYES Y+2", "-")


tab_actuals, tab_forwards, tab_monthly, tab_data, tab_files = st.tabs(
    [
        "Actuals - GDAES D+1",
        "Forwards - GYES Y+1 / Y+2",
        "Monthly actuals",
        "Data",
        "SFTP files",
    ]
)

with tab_actuals:
    subtle_subsection("GDAES D+1 - Daily Reference Price")
    chart = build_actuals_chart(actuals)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No GDAES_D+1 Daily Reference Price data found for the selected period.")

with tab_forwards:
    subtle_subsection("GYES Y+1 and GYES Y+2 - EOD Price")
    chart = build_forward_chart(forwards)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No GYES_Y+1 / GYES_Y+2 EOD Price data found for the selected period.")

with tab_monthly:
    subtle_subsection("GDAES D+1 - Monthly average Daily Reference Price")
    chart = build_monthly_actuals_chart(actuals)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No monthly data available for the selected period.")

with tab_data:
    data_choice = st.radio(
        "Dataset",
        ["Actuals GDAES D+1", "Forwards GYES Y+1/Y+2", "Raw normalized data"],
        horizontal=True,
    )

    if data_choice == "Actuals GDAES D+1":
        table = actuals.copy()
    elif data_choice == "Forwards GYES Y+1/Y+2":
        table = forwards.copy()
    else:
        table = raw_all.copy()
        table = table[
            (table["trading_day"].dt.date >= start_date) &
            (table["trading_day"].dt.date <= end_date)
        ].copy()

    table_display = table.copy()
    for col in ["date", "trading_day", "delivery_start", "delivery_end"]:
        if col in table_display.columns:
            table_display[col] = pd.to_datetime(table_display[col], errors="coerce").dt.strftime("%Y-%m-%d")

    st.dataframe(table_display.sort_index(ascending=False), use_container_width=True, hide_index=True)

    csv = table.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download selected dataset as CSV",
        data=csv,
        file_name="mibgas_selected_data.csv",
        mime="text/csv",
    )

with tab_files:
    subtle_subsection("Local data files")
    local_files = []
    if DATA_DIR.exists():
        for path in sorted(DATA_DIR.iterdir()):
            if path.is_file() and path.suffix.lower() in [".xlsx", ".xls", ".csv", ".txt", ".zip"]:
                if "mibgas" in path.name.lower() or re.search(r"(^|[_\- ])gas([_\- ]|$)", path.name.lower()):
                    local_files.append(
                        {
                            "filename": path.name,
                            "size_kb": round(path.stat().st_size / 1024, 1),
                            "modified": pd.to_datetime(path.stat().st_mtime, unit="s", errors="coerce"),
                        }
                    )
    st.dataframe(pd.DataFrame(local_files), use_container_width=True, hide_index=True)

    subtle_subsection("Remote 2026 SFTP files")
    try:
        remote_files = list_mibgas_remote_files_for_year(LIVE_YEAR)
        st.dataframe(remote_files, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not list remote SFTP files: {e}")
