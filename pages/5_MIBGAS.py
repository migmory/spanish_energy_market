
import os
import re
import stat
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import altair as alt
import pandas as pd
import paramiko
import streamlit as st


# =========================================================
# CONFIG
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


# =========================================================
# COLUMN NORMALIZATION
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
    Handles Spanish decimals:
    34,56 -> 34.56
    1.234,56 -> 1234.56
    """
    txt = s.astype(str).str.strip()
    txt = txt.str.replace("€", "", regex=False)
    txt = txt.str.replace(" ", "", regex=False)
    txt = txt.str.replace(".", "", regex=False)
    txt = txt.str.replace(",", ".", regex=False)
    return pd.to_numeric(txt, errors="coerce")


def normalize_mibgas_prices(raw: pd.DataFrame, source_file: str = "unknown") -> pd.DataFrame:
    """
    Normalizes different possible MIBGAS Excel/CSV formats into:
    date, product, price, volume, source_file
    """
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    date_candidates = [
        "Fecha",
        "FECHA",
        "Fecha sesión",
        "Fecha Sesion",
        "Fecha negociación",
        "Fecha negociacion",
        "Trading date",
        "Date",
        "Gas day",
        "Día de gas",
        "Dia de gas",
        "delivery_date",
        "session_date",
    ]

    product_candidates = [
        "Producto",
        "PRODUCTO",
        "Product",
        "Contrato",
        "CONTRATO",
        "Instrumento",
        "Instrument",
    ]

    price_candidates = [
        "Precio",
        "PRECIO",
        "Price",
        "Precio medio",
        "Precio Medio",
        "Average price",
        "Precio referencia",
        "Precio de referencia",
        "Reference price",
        "Precio cierre",
        "Closing price",
        "Last price",
    ]

    volume_candidates = [
        "Volumen",
        "VOLUMEN",
        "Volume",
        "Cantidad",
        "Quantity",
        "Energia",
        "Energía",
        "MWh",
    ]

    date_col = find_first_existing_col(df, date_candidates)
    product_col = find_first_existing_col(df, product_candidates)
    price_col = find_first_existing_col(df, price_candidates)
    volume_col = find_first_existing_col(df, volume_candidates)

    # Infer date column if not found
    if date_col is None:
        for c in df.columns:
            parsed = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
            if parsed.notna().mean() > 0.5:
                date_col = c
                break

    # Infer price column if not found
    if price_col is None:
        numeric_scores = []
        for c in df.columns:
            if c == date_col:
                continue

            values = parse_number_series(df[c])
            valid_ratio = values.notna().mean()
            median_val = values.median()

            if valid_ratio > 0.5 and pd.notna(median_val) and -100 <= median_val <= 500:
                numeric_scores.append((c, valid_ratio))

        if numeric_scores:
            price_col = sorted(numeric_scores, key=lambda x: x[1], reverse=True)[0][0]

    if date_col is None or price_col is None:
        raise ValueError(
            "Could not identify date or price column. "
            f"Columns found: {df.columns.tolist()}"
        )

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")

    if product_col is not None:
        out["product"] = df[product_col].astype(str).str.strip()
    else:
        out["product"] = "MIBGAS"

    out["price"] = parse_number_series(df[price_col])

    if volume_col is not None:
        out["volume"] = parse_number_series(df[volume_col])
    else:
        out["volume"] = pd.NA

    if "source_file" in df.columns:
        out["source_file"] = df["source_file"].astype(str)
    else:
        out["source_file"] = source_file

    out = out.dropna(subset=["date", "price"]).copy()
    out = out.sort_values(["date", "product", "source_file"]).reset_index(drop=True)

    return out[["date", "product", "price", "volume", "source_file"]]


# =========================================================
# LOCAL HISTORICAL DATA FROM /data
# =========================================================
def read_local_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix in [".xlsx", ".xls"]:
        # Try all sheets because MIBGAS workbooks can have different sheet names.
        xls = pd.ExcelFile(path)
        frames = []

        for sheet in xls.sheet_names:
            try:
                df = pd.read_excel(path, sheet_name=sheet)
                if not df.empty:
                    df["source_file"] = f"{path.name}/{sheet}"
                    frames.append(df)
            except Exception:
                pass

        if frames:
            return pd.concat(frames, ignore_index=True)

        return pd.DataFrame()

    if suffix in [".csv", ".txt"]:
        for enc in ["utf-8", "latin1", "cp1252"]:
            for sep in [";", ",", "\t", "|"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc, decimal=",", engine="python")
                    if df.shape[1] > 1:
                        df["source_file"] = path.name
                        return df
                except Exception:
                    pass

    return pd.DataFrame()


@st.cache_data(show_spinner=True)
def load_mibgas_historical_from_data() -> pd.DataFrame:
    """
    Reads all Excel/CSV/TXT files in /data that seem related to MIBGAS/gas,
    from 2021 to 2025.
    """
    if not DATA_DIR.exists():
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    files = []
    for path in DATA_DIR.iterdir():
        name = path.name.lower()

        if path.suffix.lower() not in [".xlsx", ".xls", ".csv", ".txt"]:
            continue

        # Adjust this condition if your files have a different name.
        if any(key in name for key in ["mibgas", "gas"]):
            files.append(path)

    frames = []

    for path in files:
        try:
            raw = read_local_file(path)
            if raw.empty:
                continue

            norm = normalize_mibgas_prices(raw, source_file=path.name)
            frames.append(norm)

        except Exception as e:
            st.warning(f"Could not process local MIBGAS file {path.name}: {e}")

    if not frames:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "price"])

    out = out[
        (out["date"].dt.year >= LOCAL_START_YEAR) &
        (out["date"].dt.year < LIVE_YEAR)
    ].copy()

    out = out.drop_duplicates(subset=["date", "product"], keep="last")
    out = out.sort_values(["date", "product"]).reset_index(drop=True)

    return out


# =========================================================
# MIBGAS SFTP FROM STREAMLIT SECRETS
# =========================================================
def load_mibgas_private_key_from_secrets():
    key_text = st.secrets.get("MIBGAS_SFTP_KEY", None)

    if not key_text:
        return None

    key_file = StringIO(key_text)

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

    raise ValueError(f"Could not load MIBGAS private key: {last_error}")


def connect_mibgas_sftp():
    host = st.secrets.get("MIBGAS_SFTP_HOST", "secureftpbucket.omie.es")
    port = int(st.secrets.get("MIBGAS_SFTP_PORT", 22))
    user = st.secrets.get("MIBGAS_SFTP_USER", None)

    if not user:
        raise ValueError("MIBGAS_SFTP_USER not found in Streamlit Secrets.")

    key = load_mibgas_private_key_from_secrets()
    password = st.secrets.get("MIBGAS_SFTP_PASSWORD", None)

    transport = paramiko.Transport((host, port))

    if key is not None:
        transport.connect(username=user, pkey=key)
    elif password:
        transport.connect(username=user, password=password)
    else:
        raise ValueError("No MIBGAS SSH key or password found in Streamlit Secrets.")

    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def mibgas_remote_base_path() -> str:
    return st.secrets.get("MIBGAS_SFTP_BASE_PATH", "/MIBGAS").rstrip("/")


def is_sftp_file(attr) -> bool:
    return stat.S_ISREG(attr.st_mode)


@st.cache_data(show_spinner=False, ttl=1800)
def list_mibgas_remote_files_for_year(year: int) -> pd.DataFrame:
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


def read_mibgas_remote_file(content: bytes, filename: str) -> pd.DataFrame:
    filename_lower = filename.lower()
    buffer = BytesIO(content)

    if filename_lower.endswith((".xlsx", ".xls")):
        df = pd.read_excel(buffer)
        df["source_file"] = filename
        return df

    if filename_lower.endswith(".zip"):
        frames = []

        with zipfile.ZipFile(buffer) as z:
            for inner_name in z.namelist():
                inner_lower = inner_name.lower()

                if inner_lower.endswith((".csv", ".txt")):
                    with z.open(inner_name) as f:
                        inner_content = f.read()
                    inner_df = read_mibgas_remote_file(inner_content, inner_name)
                    inner_df["source_file"] = f"{filename}/{inner_name}"
                    frames.append(inner_df)

                elif inner_lower.endswith((".xlsx", ".xls")):
                    with z.open(inner_name) as f:
                        inner_df = pd.read_excel(BytesIO(f.read()))
                    inner_df["source_file"] = f"{filename}/{inner_name}"
                    frames.append(inner_df)

        if frames:
            return pd.concat(frames, ignore_index=True)

        return pd.DataFrame()

    for enc in ["utf-8", "latin1", "cp1252"]:
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


@st.cache_data(show_spinner=True, ttl=1800)
def load_mibgas_2026_from_sftp() -> pd.DataFrame:
    files_df = list_mibgas_remote_files_for_year(LIVE_YEAR)

    if files_df.empty:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    frames = []

    for _, row in files_df.iterrows():
        filename = row["filename"]
        remote_path = row["remote_path"]

        try:
            content = read_remote_file_bytes(remote_path)
            raw_df = read_mibgas_remote_file(content, filename)

            if raw_df.empty:
                continue

            norm_df = normalize_mibgas_prices(raw_df, source_file=filename)
            frames.append(norm_df)

        except Exception as e:
            st.warning(f"Could not process MIBGAS file {filename}: {e}")

    if not frames:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "price"])

    out = out[out["date"].dt.year == LIVE_YEAR].copy()
    out = out.drop_duplicates(subset=["date", "product"], keep="last")
    out = out.sort_values(["date", "product"]).reset_index(drop=True)

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out.to_csv(MIBGAS_CACHE_FILE, index=False)
    except Exception:
        pass

    return out


def load_cached_mibgas_2026() -> pd.DataFrame:
    if not MIBGAS_CACHE_FILE.exists():
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])

    df = pd.read_csv(MIBGAS_CACHE_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    else:
        df["volume"] = pd.NA
    if "product" not in df.columns:
        df["product"] = "MIBGAS"
    if "source_file" not in df.columns:
        df["source_file"] = "mibgas_2026_cache.csv"

    return df[["date", "product", "price", "volume", "source_file"]].dropna(subset=["date", "price"])


def load_mibgas_combined() -> pd.DataFrame:
    hist = load_mibgas_historical_from_data()

    try:
        live = load_mibgas_2026_from_sftp()
        live_status = "Live 2026 data loaded from MIBGAS SFTP."
    except Exception as e:
        st.error(f"Could not connect to MIBGAS SFTP: {e}")
        live = load_cached_mibgas_2026()
        live_status = "Using cached 2026 data."

    combined = pd.concat([hist, live], ignore_index=True)

    if combined.empty:
        return combined, live_status

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    combined["product"] = combined["product"].fillna("MIBGAS").astype(str)

    combined = combined.dropna(subset=["date", "price"]).copy()
    combined = combined.drop_duplicates(subset=["date", "product"], keep="last")
    combined = combined.sort_values(["date", "product"]).reset_index(drop=True)

    return combined, live_status


# =========================================================
# CHARTS
# =========================================================
def build_gas_price_chart(df: pd.DataFrame):
    if df.empty:
        return None

    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
        .encode(
            x=alt.X("date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="€/MWh"),
            color=alt.Color("product:N", title="Product"),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("price:Q", title="Price €/MWh", format=",.2f"),
                alt.Tooltip("volume:Q", title="Volume", format=",.2f"),
                alt.Tooltip("source_file:N", title="Source file"),
            ],
        )
    )

    return apply_common_chart_style(chart, height=380)


def build_monthly_gas_chart(df: pd.DataFrame):
    if df.empty:
        return None

    monthly = df.copy()
    monthly["month"] = monthly["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        monthly.groupby(["month", "product"], as_index=False)
        .agg(price=("price", "mean"))
        .sort_values(["month", "product"])
    )

    chart = (
        alt.Chart(monthly)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("month:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="Monthly average €/MWh"),
            color=alt.Color("product:N", title="Product"),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%b-%Y"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("price:Q", title="Avg price €/MWh", format=",.2f"),
            ],
        )
    )

    return apply_common_chart_style(chart, height=360)


# =========================================================
# PAGE
# =========================================================
st.title("MIBGAS - Spain Gas Prices")

section_header("MIBGAS gas price")

st.caption(
    "Historical data is loaded from Excel/CSV files in `/data` from 2021 to 2025. "
    "From 2026 onwards, data is loaded from the MIBGAS SFTP."
)

col_refresh, col_info = st.columns([1, 3])

with col_refresh:
    refresh = st.button("Refresh SFTP data")

if refresh:
    load_mibgas_2026_from_sftp.clear()
    list_mibgas_remote_files_for_year.clear()

gas_df, live_status = load_mibgas_combined()

with col_info:
    st.caption(live_status)

if gas_df.empty:
    st.warning(
        "No MIBGAS data found. Upload Excel/CSV files with 'mibgas' or 'gas' in the filename "
        "inside the `/data` folder, and configure Streamlit Secrets for SFTP."
    )
    st.stop()

min_date = gas_df["date"].min().date()
max_date = gas_df["date"].max().date()

col1, col2, col3 = st.columns([1, 1, 1.4])

with col1:
    start_date = st.date_input(
        "Start date",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
    )

with col2:
    end_date = st.date_input(
        "End date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

filtered = gas_df[
    (gas_df["date"].dt.date >= start_date) &
    (gas_df["date"].dt.date <= end_date)
].copy()

products = sorted(filtered["product"].dropna().unique().tolist())

with col3:
    selected_product = st.selectbox(
        "Product",
        options=["All"] + products,
        index=0,
    )

if selected_product != "All":
    filtered = filtered[filtered["product"] == selected_product].copy()

if filtered.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

latest = filtered.sort_values("date").iloc[-1]
avg_price = filtered["price"].mean()
min_price = filtered["price"].min()
max_price = filtered["price"].max()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Latest gas price", f"{latest['price']:,.2f} €/MWh")
kpi2.metric("Latest date", latest["date"].strftime("%Y-%m-%d"))
kpi3.metric("Average selected period", f"{avg_price:,.2f} €/MWh")
kpi4.metric("Range", f"{min_price:,.2f} - {max_price:,.2f} €/MWh")

tab_daily, tab_monthly, tab_data, tab_files = st.tabs(
    ["Daily price", "Monthly average", "Data", "SFTP files"]
)

with tab_daily:
    chart = build_gas_price_chart(filtered)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)

with tab_monthly:
    chart = build_monthly_gas_chart(filtered)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)

with tab_data:
    table = filtered.copy()
    table["date"] = table["date"].dt.strftime("%Y-%m-%d")

    table = table.rename(
        columns={
            "date": "Date",
            "product": "Product",
            "price": "Price (€/MWh)",
            "volume": "Volume",
            "source_file": "Source file",
        }
    )

    st.dataframe(
        table.sort_values("Date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name="mibgas_prices.csv",
        mime="text/csv",
    )

with tab_files:
    st.write("Remote files detected in MIBGAS SFTP for 2026:")

    try:
        files_df = list_mibgas_remote_files_for_year(LIVE_YEAR)
        st.dataframe(files_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not list SFTP files: {e}")
