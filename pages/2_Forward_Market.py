from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests
import streamlit as st

BASE_URL = "https://www.omip.pt/en/dados-mercado"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
TIMEOUT = 25
MAX_RETRIES = 3

DATA_DIR = Path("data")
REPO_XLSX = DATA_DIR / "omip_forward_history.xlsx"
REPO_CSV = DATA_DIR / "omip_forward_history.csv"
CACHE_FILE = DATA_DIR / "omip_forward_market_cache.csv"

# OJO:
# - Baseload en OMIP usa FWB (no FTB)
# - Solar usa FTS
# - En la fila, las dos últimas cifras suelen ser D y D-1.
#   Aquí cogemos D = penúltima cifra de la fila.
@dataclass(frozen=True)
class ContractSpec:
    page_instrument: str
    row_prefix: str
    contract: str
    label: str


CONTRACT_SPECS = [
    ContractSpec("FWB", "FWB", "YR-27", "Baseload YR27"),
    ContractSpec("FWB", "FWB", "YR-28", "Baseload YR28"),
    ContractSpec("FTS", "FTS", "YR-27", "Solar YR27"),
    ContractSpec("FTS", "FTS", "YR-28", "Solar YR28"),
]
EXPECTED_COLUMNS = ["date"] + [x.label for x in CONTRACT_SPECS]

st.set_page_config(page_title="OMIP Forward Market", layout="wide")
st.title("OMIP Forward Market | YR27 & YR28")
st.caption("Usa histórico del repo/cache y solo intenta descargar de OMIP las fechas que falten.")

def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=EXPECTED_COLUMNS)

def daterange(start: date, end: date, step_days: int) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=step_days)

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return empty_df()

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out = out.rename(
        columns={
            "Date": "date",
            "DATE": "date",
            "Baseload YR-27": "Baseload YR27",
            "Baseload YR-28": "Baseload YR28",
            "Solar YR-27": "Solar YR27",
            "Solar YR-28": "Solar YR28",
            "FWB YR27": "Baseload YR27",
            "FWB YR28": "Baseload YR28",
            "FTS YR27": "Solar YR27",
            "FTS YR28": "Solar YR28",
        }
    )

    if "date" not in out.columns:
        return empty_df()

    for col in EXPECTED_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    out = out[EXPECTED_COLUMNS].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])

    for col in EXPECTED_COLUMNS:
        if col != "date":
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return out

def load_seed_file() -> pd.DataFrame:
    try:
        if REPO_XLSX.exists():
            return normalize_df(pd.read_excel(REPO_XLSX))
        if REPO_CSV.exists():
            return normalize_df(pd.read_csv(REPO_CSV))
    except Exception:
        pass
    return empty_df()

def load_local_cache() -> pd.DataFrame:
    if not CACHE_FILE.exists():
        return empty_df()
    try:
        return normalize_df(pd.read_csv(CACHE_FILE))
    except Exception:
        return empty_df()

def load_existing_history() -> pd.DataFrame:
    seed = load_seed_file()
    cache = load_local_cache()
    return normalize_df(pd.concat([seed, cache], ignore_index=True))

def save_local_cache(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    normalize_df(df).to_csv(CACHE_FILE, index=False)

def merge_into_cache(imported_df: pd.DataFrame) -> int:
    current = load_existing_history()
    combined = normalize_df(pd.concat([current, normalize_df(imported_df)], ignore_index=True))
    save_local_cache(combined)
    return len(combined)

@st.cache_data(show_spinner=False, ttl=60 * 60 * 12)
def fetch_page(day_iso: str, instrument: str) -> str:
    target_date = datetime.strptime(day_iso, "%Y-%m-%d").date()
    url = f"{BASE_URL}?date={target_date.isoformat()}&product=EL&zone=ES&instrument={instrument}"
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en,en-US;q=0.9"}

    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    raise RuntimeError(f"No se pudo descargar {url}: {last_error}")

def parse_contract_value(html: str, row_prefix: str, contract: str) -> Optional[float]:
    if not html:
        return None

    # Busca la fila tipo "FWB YR-27 ..." o "FTS YR-27 ..."
    pattern = re.compile(
        rf"(?im)^\s*{re.escape(row_prefix)}\s+{re.escape(contract)}\s+.*$"
    )
    match = pattern.search(html)
    if not match:
        return None

    line = re.sub(r"\s+", " ", match.group(0)).strip()

    # Elimina el prefijo del contrato para no capturar el 27/28 de YR-27 / YR-28
    line_wo_contract = re.sub(
        rf"^\s*{re.escape(row_prefix)}\s+{re.escape(contract)}\s+",
        "",
        line,
        flags=re.IGNORECASE,
    )

    nums = re.findall(r"-?\d+(?:\.\d+)?", line_wo_contract)
    if len(nums) < 2:
        return None

    # En OMIP las dos últimas columnas visibles son D y D-1.
    # Queremos D = penúltimo valor
    try:
        return float(nums[-2])
    except Exception:
        return None

def download_missing_dates(missing_days: list[date]) -> tuple[pd.DataFrame, int]:
    if not missing_days:
        return empty_df(), 0

    rows: list[dict] = []
    failed_days = 0
    progress = st.progress(0, text="Descargando fechas nuevas desde OMIP...")

    for idx, day in enumerate(missing_days, start=1):
        pages = {}
        ok_any_page = False

        for instrument in {"FWB", "FTS"}:
            try:
                pages[instrument] = fetch_page(day.isoformat(), instrument)
                ok_any_page = True
            except Exception:
                pages[instrument] = ""

        if not ok_any_page:
            failed_days += 1
            progress.progress(idx / len(missing_days), text=f"Descargando fechas nuevas desde OMIP... {idx}/{len(missing_days)}")
            continue

        record = {"date": pd.Timestamp(day)}
        found_any = False

        for spec in CONTRACT_SPECS:
            value = parse_contract_value(pages.get(spec.page_instrument, ""), spec.row_prefix, spec.contract)
            record[spec.label] = value
            found_any = found_any or value is not None

        if found_any:
            rows.append(record)
        else:
            failed_days += 1

        progress.progress(idx / len(missing_days), text=f"Descargando fechas nuevas desde OMIP... {idx}/{len(missing_days)}")

    progress.empty()
    return normalize_df(pd.DataFrame(rows)), failed_days

def get_timeseries_incremental(start_date: date, end_date: date, step_days: int, update_from_omip: bool) -> tuple[pd.DataFrame, int, int]:
    existing = load_existing_history()
    existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
    existing = existing.dropna(subset=["date"])

    cached_dates = set(existing["date"].dt.date) if not existing.empty else set()
    requested_days = list(daterange(start_date, end_date, step_days))
    missing_days = [d for d in requested_days if d not in cached_dates]

    new_data = empty_df()
    failed_days = 0
    if update_from_omip and missing_days:
        new_data, failed_days = download_missing_dates(missing_days)

    combined = normalize_df(pd.concat([existing, new_data], ignore_index=True))
    if not combined.empty:
        save_local_cache(combined)

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date"])

    mask = (combined["date"].dt.date >= start_date) & (combined["date"].dt.date <= end_date)
    return combined.loc[mask].copy(), len(missing_days), failed_days

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    export_df = normalize_df(df).copy()
    export_df["date"] = export_df["date"].dt.date
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="OMIP")
    output.seek(0)
    return output.getvalue()

with st.sidebar:
    st.header("Configuración")
    default_start = date(date.today().year - 2, 1, 1)
    start_date = st.date_input("Desde", value=default_start)
    end_date = st.date_input("Hasta", value=date.today())

    frequency = st.selectbox("Frecuencia de muestreo", ["Diaria", "Semanal"], index=1)
    step_days = 1 if frequency == "Diaria" else 7

    use_forward_fill = st.checkbox("Rellenar huecos con último valor", value=True)
    update_from_omip = st.checkbox("Intentar actualizar faltantes desde OMIP", value=False)

    st.markdown("---")
    st.write(f"Seed del repo: `{REPO_XLSX}` o `{REPO_CSV}`")
    st.write(f"Cache local: `{CACHE_FILE}`")

    uploaded_file = st.file_uploader("Importar histórico manual (CSV/XLSX)", type=["csv", "xlsx"])
    if uploaded_file is not None:
        try:
            imported = pd.read_excel(uploaded_file) if uploaded_file.name.lower().endswith(".xlsx") else pd.read_csv(uploaded_file)
            total_rows = merge_into_cache(imported)
            st.success(f"Histórico importado. Filas guardadas: {total_rows}.")
        except Exception as exc:
            st.error(f"No pude leer el archivo: {exc}")

    if st.button("Borrar cache local"):
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
        st.success("Cache local borrada. Recarga la página.")

if start_date > end_date:
    st.error("La fecha inicial no puede ser mayor que la final.")
    st.stop()

with st.spinner("Leyendo histórico..."):
    df, missing_count, failed_days = get_timeseries_incremental(start_date, end_date, step_days, update_from_omip)

if df.empty:
    st.error("No hay datos válidos. Sube un histórico manual o revisa el seed del repo.")
    st.stop()

history_now = load_existing_history()
st.info(
    f"Fechas pedidas en el rango: {missing_count}. "
    f"Fechas guardadas entre seed + cache: {len(history_now)}. "
    f"Fechas que OMIP no devolvió bien en esta carga: {failed_days}."
)

st.warning(
    "Consejo: deja desactivado 'Intentar actualizar faltantes desde OMIP' hasta validar que los datos scrapeados cuadran con tu histórico."
)

plot_df = normalize_df(df).set_index("date")
full_index = pd.date_range(start=plot_df.index.min(), end=plot_df.index.max(), freq="D")
plot_df = plot_df.reindex(full_index)
plot_df.index.name = "date"
if use_forward_fill:
    plot_df = plot_df.ffill()

st.subheader("Evolución")
selected_series = st.multiselect(
    "Series a mostrar",
    options=list(plot_df.columns),
    default=list(plot_df.columns),
)
if selected_series:
    st.line_chart(plot_df[selected_series])
else:
    st.info("Selecciona al menos una serie.")

latest = normalize_df(df).sort_values("date").iloc[-1]
cols = st.columns(4)
for col, spec in zip(cols, CONTRACT_SPECS):
    value = latest.get(spec.label)
    col.metric(spec.label, f"{value:,.2f} €/MWh" if pd.notna(value) else "n.d.")

st.subheader("Tabla")
display_df = normalize_df(df).copy()
st.dataframe(display_df, use_container_width=True)

csv_bytes = display_df.to_csv(index=False).encode("utf-8")
xlsx_bytes = to_excel_bytes(display_df)

st.download_button(
    "Descargar CSV",
    data=csv_bytes,
    file_name=f"omip_forward_market_{start_date.isoformat()}_{end_date.isoformat()}.csv",
    mime="text/csv",
)

st.download_button(
    "Descargar XLSX",
    data=xlsx_bytes,
    file_name=f"omip_forward_market_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
