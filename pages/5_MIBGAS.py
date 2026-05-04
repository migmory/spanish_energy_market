from __future__ import annotations

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
LOCAL_FILE_PATTERN = "MIBGAS_Data_*.xlsx"
TRADING_SHEET = "Trading Data PVB&VTP"
CACHE_FILE_2026 = DATA_DIR / "mibgas_2026_cache.csv"

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE_PRICE = "#1D4ED8"
YELLOW_DARK = "#D97706"


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


# =========================================================
# NORMALIZATION HELPERS
# =========================================================
def normalize_col(col: str) -> str:
    return (
        str(col)
        .replace("\n", " ")
        .replace("\xa0", " ")
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
        .replace("[", "")
        .replace("]", "")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("%", "pct")
        .replace(" ", "_")
    )


def get_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm_map = {normalize_col(c): c for c in df.columns}
    for cand in candidates:
        if normalize_col(cand) in norm_map:
            return norm_map[normalize_col(cand)]
    return None


def parse_number(s: pd.Series) -> pd.Series:
    txt = s.astype(str).str.strip()
    txt = txt.str.replace("€", "", regex=False)
    txt = txt.str.replace(" ", "", regex=False)
    txt = txt.str.replace("\u00a0", "", regex=False)

    def clean_one(x: str) -> str:
        if x.lower() in {"nan", "none", "", "nat"}:
            return ""
        comma = x.rfind(",")
        dot = x.rfind(".")
        if comma >= 0 and dot >= 0:
            # 1.234,56 or 1,234.56
            if comma > dot:
                return x.replace(".", "").replace(",", ".")
            return x.replace(",", "")
        if comma >= 0:
            return x.replace(".", "").replace(",", ".")
        return x

    return pd.to_numeric(txt.map(clean_one), errors="coerce")


def clean_product(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.replace(" ", "", regex=False)


def normalize_mibgas_raw(raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Standard output columns:
    trading_day, product, area, place_of_delivery, delivery_start, delivery_end,
    daily_reference_price_eur_mwh, eod_price_eur_mwh, last_price_eur_mwh,
    volume_mwh, source_file.
    """
    if raw.empty:
        return pd.DataFrame()

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # In case the header was read as a normal row.
    if "Trading day" not in df.columns or "Product" not in df.columns:
        for idx in range(min(15, len(df))):
            joined = "|".join(str(x).strip().lower() for x in df.iloc[idx].tolist())
            if "trading day" in joined and "product" in joined:
                df.columns = [str(x).strip() for x in df.iloc[idx].tolist()]
                df = df.iloc[idx + 1 :].copy().dropna(how="all").dropna(axis=1, how="all")
                break

    trading_col = get_col(df, ["Trading day", "Trading Day", "Fecha", "Date"])
    product_col = get_col(df, ["Product", "Producto"])
    area_col = get_col(df, ["Area", "Área"])
    pod_col = get_col(df, ["Place of delivery", "Place of Delivery", "PVB", "Hub"])
    first_col = get_col(df, ["First Day Delivery", "First delivery", "Delivery start"])
    last_col = get_col(df, ["Last Day Delivery", "Last delivery", "Delivery end"])

    # The files you uploaded use "Reference Price [EUR/MWh]", not "Daily Reference Price".
    ref_col = get_col(
        df,
        [
            "Daily Reference Price [EUR/MWh]",
            "Reference Price [EUR/MWh]",
            "Daily Reference Price",
            "Reference Price",
        ],
    )
    eod_col = get_col(
        df,
        [
            "EOD Price [EUR/MWh]",
            "EOD Price",
            "End of Day Price",
        ],
    )
    last_price_col = get_col(
        df,
        [
            "Last Daily Price [EUR/MWh]",
            "Last Price [EUR/MWh]",
            "Last Daily Price",
            "Last Price",
        ],
    )
    volume_col = get_col(
        df,
        [
            "Daily Volume Traded [MWh]",
            "Volume Traded [MWh]",
            "Volume Traded\n[MWh]",
            "Auction Volume Traded [MWh]",
            "Auction Volume Traded\u00a0[MWh]",
        ],
    )

    if trading_col is None or product_col is None:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["trading_day"] = pd.to_datetime(df[trading_col], dayfirst=True, errors="coerce")
    out["product"] = clean_product(df[product_col])
    out["area"] = df[area_col].astype(str).str.strip() if area_col else pd.NA
    out["place_of_delivery"] = df[pod_col].astype(str).str.strip() if pod_col else pd.NA
    out["delivery_start"] = pd.to_datetime(df[first_col], dayfirst=True, errors="coerce") if first_col else pd.NaT
    out["delivery_end"] = pd.to_datetime(df[last_col], dayfirst=True, errors="coerce") if last_col else pd.NaT
    out["daily_reference_price_eur_mwh"] = parse_number(df[ref_col]) if ref_col else pd.NA
    out["eod_price_eur_mwh"] = parse_number(df[eod_col]) if eod_col else pd.NA
    out["last_price_eur_mwh"] = parse_number(df[last_price_col]) if last_price_col else pd.NA
    out["volume_mwh"] = parse_number(df[volume_col]) if volume_col else pd.NA
    out["source_file"] = source_file

    out = out.dropna(subset=["trading_day", "product"]).copy()
    out = out[out["product"].ne("")]
    return out.reset_index(drop=True)


# =========================================================
# LOCAL DATA: /data/MIBGAS_Data_2021.xlsx ... 2025
# =========================================================
def read_local_mibgas_excel(path: Path) -> pd.DataFrame:
    """Reads only the useful sheet. This avoids false products from 'Regulated gas'."""
    try:
        xls = pd.ExcelFile(path)
        if TRADING_SHEET in xls.sheet_names:
            raw = pd.read_excel(path, sheet_name=TRADING_SHEET)
            return normalize_mibgas_raw(raw, f"{path.name}/{TRADING_SHEET}")

        # Fallback: read sheets whose name starts with Trading Data.
        frames = []
        for sheet in xls.sheet_names:
            if sheet.lower().startswith("trading data"):
                raw = pd.read_excel(path, sheet_name=sheet)
                norm = normalize_mibgas_raw(raw, f"{path.name}/{sheet}")
                if not norm.empty:
                    frames.append(norm)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not read local file {path.name}: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner=True)
def load_local_mibgas_data() -> pd.DataFrame:
    files = sorted(DATA_DIR.glob(LOCAL_FILE_PATTERN)) if DATA_DIR.exists() else []
    files = [p for p in files if p.suffix.lower() in [".xlsx", ".xls"]]

    frames = []
    for path in files:
        norm = read_local_mibgas_excel(path)
        if not norm.empty:
            frames.append(norm)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out[out["trading_day"].dt.year.between(LOCAL_START_YEAR, LIVE_YEAR - 1)].copy()
    out = out.sort_values(["trading_day", "product", "source_file"]).drop_duplicates(
        subset=["trading_day", "product", "area", "place_of_delivery", "delivery_start", "delivery_end"],
        keep="last",
    )
    return out.reset_index(drop=True)


# =========================================================
# 2026 SFTP DATA
# =========================================================
def get_secret(name: str, default=None):
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


def sftp_secrets_configured() -> tuple[bool, str]:
    if paramiko is None:
        return False, "`paramiko` is not installed. Add `paramiko` to requirements.txt."
    if not get_secret("MIBGAS_SFTP_USER"):
        return False, "`MIBGAS_SFTP_USER` is missing in Streamlit Secrets."
    if not get_secret("MIBGAS_SFTP_KEY") and not get_secret("MIBGAS_SFTP_PASSWORD"):
        return False, "`MIBGAS_SFTP_KEY` or `MIBGAS_SFTP_PASSWORD` is missing in Streamlit Secrets."
    return True, ""


def load_private_key_from_secret():
    key_text = get_secret("MIBGAS_SFTP_KEY")
    if not key_text:
        return None

    key_file = StringIO(str(key_text))
    loaders = [paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey, paramiko.DSSKey]
    last_error = None
    for loader in loaders:
        try:
            key_file.seek(0)
            return loader.from_private_key(key_file)
        except Exception as e:
            last_error = e
    raise ValueError(f"Could not load private key from Streamlit Secrets: {last_error}")


def connect_sftp():
    host = get_secret("MIBGAS_SFTP_HOST", "secureftpbucket.omie.es")
    port = int(get_secret("MIBGAS_SFTP_PORT", 22))
    user = get_secret("MIBGAS_SFTP_USER")
    password = get_secret("MIBGAS_SFTP_PASSWORD")
    key = load_private_key_from_secret()

    transport = paramiko.Transport((host, port))
    if key is not None:
        transport.connect(username=user, pkey=key)
    else:
        transport.connect(username=user, password=password)
    return paramiko.SFTPClient.from_transport(transport), transport


def possible_base_paths() -> list[str]:
    configured = str(get_secret("MIBGAS_SFTP_BASE_PATH", "")).strip().rstrip("/")
    paths = []
    if configured:
        paths.append(configured)
    # Different SFTP clients can show root differently.
    paths += ["/MIBGAS", "/secureftpbucket.omie.es/MIBGAS", "MIBGAS", "."]
    seen = set()
    out = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def list_year_files(sftp, year: int) -> tuple[list[dict], str]:
    last_error = None
    for base in possible_base_paths():
        year_path = f"{base.rstrip('/')}/AGNO_{year}" if base != "." else f"AGNO_{year}"
        try:
            rows = []
            for item in sftp.listdir_attr(year_path):
                if stat.S_ISREG(item.st_mode):
                    rows.append(
                        {
                            "filename": item.filename,
                            "remote_path": f"{year_path}/{item.filename}",
                            "modified": pd.to_datetime(item.st_mtime, unit="s", errors="coerce"),
                            "size_bytes": item.st_size,
                        }
                    )
            return rows, year_path
        except Exception as e:
            last_error = e
    raise FileNotFoundError(f"Could not find AGNO_{year} in SFTP. Last error: {last_error}")


def read_remote_bytes(remote_path: str) -> bytes:
    sftp, transport = connect_sftp()
    try:
        with sftp.open(remote_path, "rb") as f:
            return f.read()
    finally:
        sftp.close()
        transport.close()


def read_remote_mibgas_file(content: bytes, filename: str) -> pd.DataFrame:
    lower = filename.lower()

    if lower.endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(BytesIO(content))
        if TRADING_SHEET in xls.sheet_names:
            raw = pd.read_excel(BytesIO(content), sheet_name=TRADING_SHEET)
            return normalize_mibgas_raw(raw, f"{filename}/{TRADING_SHEET}")
        frames = []
        for sheet in xls.sheet_names:
            if sheet.lower().startswith("trading data"):
                raw = pd.read_excel(BytesIO(content), sheet_name=sheet)
                norm = normalize_mibgas_raw(raw, f"{filename}/{sheet}")
                if not norm.empty:
                    frames.append(norm)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if lower.endswith(".zip"):
        frames = []
        with zipfile.ZipFile(BytesIO(content)) as z:
            for inner in z.namelist():
                if inner.lower().endswith((".xlsx", ".xls", ".csv", ".txt")):
                    with z.open(inner) as f:
                        norm = read_remote_mibgas_file(f.read(), f"{filename}/{inner}")
                    if not norm.empty:
                        frames.append(norm)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # CSV/TXT fallback.
    for enc in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        for sep in [";", ",", "\t", "|"]:
            try:
                raw = pd.read_csv(BytesIO(content), sep=sep, encoding=enc, engine="python")
                if raw.shape[1] > 1:
                    return normalize_mibgas_raw(raw, filename)
            except Exception:
                pass
    return pd.DataFrame()


@st.cache_data(show_spinner=True, ttl=1800)
def load_mibgas_2026_from_sftp() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    ok, reason = sftp_secrets_configured()
    if not ok:
        raise ValueError(reason)

    sftp, transport = connect_sftp()
    try:
        file_rows, detected_path = list_year_files(sftp, LIVE_YEAR)
    finally:
        sftp.close()
        transport.close()

    files_df = pd.DataFrame(file_rows).sort_values(["modified", "filename"]).reset_index(drop=True)
    frames = []
    for _, row in files_df.iterrows():
        try:
            content = read_remote_bytes(row["remote_path"])
            norm = read_remote_mibgas_file(content, row["filename"])
            if not norm.empty:
                frames.append(norm)
        except Exception as e:
            st.warning(f"Could not process 2026 SFTP file {row['filename']}: {e}")

    if not frames:
        live = pd.DataFrame()
    else:
        live = pd.concat(frames, ignore_index=True)
        live = live[live["trading_day"].dt.year == LIVE_YEAR].copy()
        live = live.sort_values(["trading_day", "product", "source_file"]).drop_duplicates(
            subset=["trading_day", "product", "area", "place_of_delivery", "delivery_start", "delivery_end"],
            keep="last",
        )

    try:
        if not live.empty:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            live.to_csv(CACHE_FILE_2026, index=False)
    except Exception:
        pass

    return live.reset_index(drop=True), files_df, detected_path


def load_2026_cache() -> pd.DataFrame:
    if not CACHE_FILE_2026.exists():
        return pd.DataFrame()
    df = pd.read_csv(CACHE_FILE_2026)
    for c in ["trading_day", "delivery_start", "delivery_end"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in ["daily_reference_price_eur_mwh", "eod_price_eur_mwh", "last_price_eur_mwh", "volume_mwh"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(show_spinner=True, ttl=900)
def load_all_data() -> tuple[pd.DataFrame, str, pd.DataFrame]:
    hist = load_local_mibgas_data()
    status = "2026 SFTP not configured. Showing local historical files only."
    files_df = pd.DataFrame()

    try:
        live, files_df, detected_path = load_mibgas_2026_from_sftp()
        status = f"2026 data loaded from SFTP path `{detected_path}`."
    except Exception as e:
        live = load_2026_cache()
        if live.empty:
            status = f"2026 SFTP data not loaded: {e}"
        else:
            status = f"2026 SFTP failed; using cached 2026 file: {e}"

    combined = pd.concat([hist, live], ignore_index=True)
    if combined.empty:
        return combined, status, files_df

    combined["trading_day"] = pd.to_datetime(combined["trading_day"], errors="coerce")
    combined["product"] = clean_product(combined["product"])
    combined = combined.dropna(subset=["trading_day", "product"])
    combined = combined.sort_values(["trading_day", "product", "source_file"]).drop_duplicates(
        subset=["trading_day", "product", "area", "place_of_delivery", "delivery_start", "delivery_end"],
        keep="last",
    )
    return combined.reset_index(drop=True), status, files_df


# =========================================================
# DATASETS REQUESTED
# =========================================================
def build_actuals(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])
    df = raw.copy()
    df["product_clean"] = clean_product(df["product"])
    mask = df["product_clean"].eq("GDAES_D+1")
    if "area" in df.columns:
        mask &= df["area"].isna() | df["area"].astype(str).str.upper().eq("ES")
    out = df[mask].copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "product", "price", "volume", "source_file"])
    out = out.rename(columns={"trading_day": "date", "daily_reference_price_eur_mwh": "price", "volume_mwh": "volume"})
    out = out[["date", "product", "price", "volume", "source_file"]].dropna(subset=["date", "price"])
    out["series"] = "GDAES D+1 - Reference Price"
    return out.sort_values("date").drop_duplicates(subset=["date", "product"], keep="last").reset_index(drop=True)


def build_forwards(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "product", "delivery_start", "delivery_end", "price", "price_source", "source_file"])
    df = raw.copy()
    df["product_clean"] = clean_product(df["product"])
    mask = df["product_clean"].isin(["GYES_Y+1", "GYES_Y+2"])
    if "area" in df.columns:
        mask &= df["area"].isna() | df["area"].astype(str).str.upper().eq("ES")
    out = df[mask].copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "product", "delivery_start", "delivery_end", "price", "price_source", "source_file"])

    # Prefer explicit EOD price if available. In the current Excel format, use Last Price as EOD-equivalent; fallback to Reference Price.
    out["price"] = out["eod_price_eur_mwh"]
    out["price_source"] = "EOD Price"
    use_last = out["price"].isna() & out["last_price_eur_mwh"].notna()
    out.loc[use_last, "price"] = out.loc[use_last, "last_price_eur_mwh"]
    out.loc[use_last, "price_source"] = "Last Price"
    use_ref = out["price"].isna() & out["daily_reference_price_eur_mwh"].notna()
    out.loc[use_ref, "price"] = out.loc[use_ref, "daily_reference_price_eur_mwh"]
    out.loc[use_ref, "price_source"] = "Reference Price"

    out = out.rename(columns={"trading_day": "date"})
    out = out[["date", "product", "delivery_start", "delivery_end", "price", "price_source", "source_file"]]
    out = out.dropna(subset=["date", "price"])
    return out.sort_values(["date", "product"]).drop_duplicates(subset=["date", "product"], keep="last").reset_index(drop=True)


# =========================================================
# CHARTS
# =========================================================
def chart_actuals(df: pd.DataFrame):
    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
        .encode(
            x=alt.X("date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="€/MWh"),
            tooltip=[
                alt.Tooltip("date:T", title="Trading day", format="%Y-%m-%d"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("price:Q", title="Reference Price", format=",.2f"),
                alt.Tooltip("volume:Q", title="Volume MWh", format=",.0f"),
                alt.Tooltip("source_file:N", title="Source"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=380)


def chart_forwards(df: pd.DataFrame):
    color_scale = alt.Scale(domain=["GYES_Y+1", "GYES_Y+2"], range=[CORP_GREEN_DARK, YELLOW_DARK])
    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("date:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="€/MWh"),
            color=alt.Color("product:N", title="Product", scale=color_scale),
            tooltip=[
                alt.Tooltip("date:T", title="Trading day", format="%Y-%m-%d"),
                alt.Tooltip("product:N", title="Product"),
                alt.Tooltip("delivery_start:T", title="Delivery start", format="%Y-%m-%d"),
                alt.Tooltip("delivery_end:T", title="Delivery end", format="%Y-%m-%d"),
                alt.Tooltip("price:Q", title="Price", format=",.2f"),
                alt.Tooltip("price_source:N", title="Price source"),
                alt.Tooltip("source_file:N", title="Source"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=380)


def chart_monthly_actuals(df: pd.DataFrame):
    if df.empty:
        return None
    monthly = df.copy()
    monthly["month"] = monthly["date"].dt.to_period("M").dt.to_timestamp()
    monthly = monthly.groupby("month", as_index=False)["price"].mean()
    chart = (
        alt.Chart(monthly)
        .mark_line(point=True, strokeWidth=2.5, color=BLUE_PRICE)
        .encode(
            x=alt.X("month:T", title=None, axis=alt.Axis(format="%b-%Y", labelAngle=0)),
            y=alt.Y("price:Q", title="Monthly average €/MWh"),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%b-%Y"),
                alt.Tooltip("price:Q", title="Avg Reference Price", format=",.2f"),
            ],
        )
    )
    return apply_common_chart_style(chart, height=360)


# =========================================================
# PAGE
# =========================================================
st.title("MIBGAS - Spain Gas Prices")
st.caption(
    "Historical files are loaded from `/data/MIBGAS_Data_*.xlsx` from 2021 to 2025. "
    "2026 files are loaded from MIBGAS SFTP when Streamlit Secrets are configured."
)

section_header("MIBGAS market data")

col_refresh, col_status = st.columns([1.1, 3])
with col_refresh:
    if st.button("Refresh MIBGAS SFTP data"):
        load_mibgas_2026_from_sftp.clear()
        load_all_data.clear()
        st.rerun()

raw, status, sftp_files = load_all_data()
with col_status:
    st.caption(status)

if raw.empty:
    st.warning("No MIBGAS data found. Upload files named `MIBGAS_Data_2021.xlsx` ... `MIBGAS_Data_2025.xlsx` to `/data`.")
    st.stop()

actuals = build_actuals(raw)
forwards = build_forwards(raw)

if actuals.empty and forwards.empty:
    st.warning("The files were read, but no GDAES_D+1 actuals or GYES_Y+1/Y+2 forwards were found in the Trading Data PVB&VTP sheet.")
    st.dataframe(raw.head(200), use_container_width=True, hide_index=True)
    st.stop()

min_date = raw["trading_day"].min().date()
max_date = raw["trading_day"].max().date()

f1, f2 = st.columns(2)
with f1:
    start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
with f2:
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

if not actuals.empty:
    actuals = actuals[(actuals["date"].dt.date >= start_date) & (actuals["date"].dt.date <= end_date)].copy()
if not forwards.empty:
    forwards = forwards[(forwards["date"].dt.date >= start_date) & (forwards["date"].dt.date <= end_date)].copy()

k1, k2, k3, k4 = st.columns(4)
if not actuals.empty:
    latest_actual = actuals.sort_values("date").iloc[-1]
    k1.metric("Latest GDAES D+1", f"{latest_actual['price']:,.2f} €/MWh")
    k2.metric("Latest actual date", latest_actual["date"].strftime("%Y-%m-%d"))
else:
    k1.metric("Latest GDAES D+1", "-")
    k2.metric("Latest actual date", "-")

if not forwards.empty:
    latest_fw = forwards.sort_values("date").groupby("product", as_index=False).tail(1)
    y1 = latest_fw.loc[latest_fw["product"].eq("GYES_Y+1"), "price"]
    y2 = latest_fw.loc[latest_fw["product"].eq("GYES_Y+2"), "price"]
    k3.metric("Latest GYES Y+1", f"{y1.iloc[-1]:,.2f} €/MWh" if not y1.empty else "-")
    k4.metric("Latest GYES Y+2", f"{y2.iloc[-1]:,.2f} €/MWh" if not y2.empty else "-")
else:
    k3.metric("Latest GYES Y+1", "-")
    k4.metric("Latest GYES Y+2", "-")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Actuals - GDAES D+1",
    "Forwards - GYES Y+1 / Y+2",
    "Monthly actuals",
    "Data",
    "SFTP / diagnostics",
])

with tab1:
    subtle_subsection("GDAES D+1 - Reference Price")
    if actuals.empty:
        st.warning("No GDAES_D+1 Reference Price data found for the selected period.")
    else:
        st.altair_chart(chart_actuals(actuals), use_container_width=True)

with tab2:
    subtle_subsection("GYES Y+1 and Y+2 - Price")
    st.caption("The chart uses explicit EOD Price when present. If not present in the file format, it uses Last Price; if Last Price is missing, it falls back to Reference Price.")
    if forwards.empty:
        st.warning("No GYES_Y+1 / GYES_Y+2 price data found for the selected period.")
    else:
        st.altair_chart(chart_forwards(forwards), use_container_width=True)

with tab3:
    subtle_subsection("GDAES D+1 - Monthly average Reference Price")
    if actuals.empty:
        st.warning("No actuals available for the selected period.")
    else:
        st.altair_chart(chart_monthly_actuals(actuals), use_container_width=True)

with tab4:
    choice = st.radio("Dataset", ["Actuals GDAES D+1", "Forwards GYES Y+1/Y+2", "Raw normalized data"], horizontal=True)
    if choice == "Actuals GDAES D+1":
        table = actuals.copy()
    elif choice == "Forwards GYES Y+1/Y+2":
        table = forwards.copy()
    else:
        table = raw.copy()

    for c in ["date", "trading_day", "delivery_start", "delivery_end"]:
        if c in table.columns:
            table[c] = pd.to_datetime(table[c], errors="coerce").dt.strftime("%Y-%m-%d")
    st.dataframe(table.sort_values(table.columns[0], ascending=False), use_container_width=True, hide_index=True)
    st.download_button("Download selected dataset as CSV", table.to_csv(index=False).encode("utf-8"), "mibgas_dataset.csv", "text/csv")

with tab5:
    st.write("**Local files detected**")
    local_files = pd.DataFrame({"file": [p.name for p in sorted(DATA_DIR.glob(LOCAL_FILE_PATTERN))]}) if DATA_DIR.exists() else pd.DataFrame()
    st.dataframe(local_files, use_container_width=True, hide_index=True)

    st.write("**2026 SFTP files detected**")
    if sftp_files.empty:
        st.caption("No SFTP file list available. Configure Streamlit Secrets and click Refresh.")
    else:
        st.dataframe(sftp_files, use_container_width=True, hide_index=True)

    with st.expander("Secrets template"):
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
