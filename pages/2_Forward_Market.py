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


@dataclass(frozen=True)
class ContractSpec:
    instrument: str
    contract: str
    label: str


CONTRACT_SPECS = [
    ContractSpec("FTB", "YR-27", "Baseload YR27"),
    ContractSpec("FTB", "YR-28", "Baseload YR28"),
    ContractSpec("FTS", "YR-27", "Solar YR27"),
    ContractSpec("FTS", "YR-28", "Solar YR28"),
]
EXPECTED_COLUMNS = ["date"] + [x.label for x in CONTRACT_SPECS]

st.set_page_config(page_title="OMIP Forward Market", layout="wide")
st.title("OMIP Forward Market | YR27 & YR28")
st.caption(
    "Lee primero un histórico del repo o del cache local y solo descarga de OMIP las fechas que falten."
)


def daterange(start: date, end: date, step_days: int) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=step_days)


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
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    raise RuntimeError(f"No se pudo descargar {url}: {last_error}")


_CONTRACT_LINE_RE_TEMPLATE = r"\b{instrument}\s+{contract}\b[^\n\r]*"
_NUMBER_RE = re.compile(r"(?<![A-Za-z])-?\d+(?:\.\d+)?")


def parse_contract_value(html: str, instrument: str, contract: str) -> Optional[float]:
    pattern = re.compile(
        _CONTRACT_LINE_RE_TEMPLATE.format(
            instrument=re.escape(instrument), contract=re.escape(contract)
        ),
        flags=re.IGNORECASE,
    )
    match = pattern.search(html)
    if not match:
        return None

    line = re.sub(r"\s+", " ", match.group(0)).strip()
    numbers = _NUMBER_RE.findall(line)
    if not numbers:
        return None
    return float(numbers[-1])



def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=EXPECTED_COLUMNS)



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
            "FTB YR27": "Baseload YR27",
            "FTB YR28": "Baseload YR28",
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

    return out.sort_values("date").drop_duplicates(subset=["date"], keep="last")



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
    combined = pd.concat([seed, cache], ignore_index=True)
    return normalize_df(combined)



def save_local_cache(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    normalize_df(df).to_csv(CACHE_FILE, index=False)



def merge_into_cache(imported_df: pd.DataFrame) -> int:
    current = load_existing_history()
    combined = pd.concat([current, normalize_df(imported_df)], ignore_index=True)
    combined = normalize_df(combined)
    save_local_cache(combined)
    return len(combined)



def download_missing_dates(missing_days: list[date]) -> pd.DataFrame:
    if not missing_days:
        return empty_df()

    rows: list[dict] = []
    progress = st.progress(0, text="Descargando solo fechas nuevas...")

    for idx, day in enumerate(missing_days, start=1):
        pages: dict[str, str] = {}
        for instrument in {"FTB", "FTS"}:
            try:
                pages[instrument] = fetch_page(day.isoformat(), instrument)
            except Exception:
                pages[instrument] = ""

        record = {"date": pd.Timestamp(day)}
        found_any = False
        for spec in CONTRACT_SPECS:
            value = parse_contract_value(
                pages.get(spec.instrument, ""), spec.instrument, spec.contract
            )
            record[spec.label] = value
            found_any = found_any or value is not None

        if found_any:
            rows.append(record)

        progress.progress(
            min(idx / max(len(missing_days), 1), 1.0),
            text=f"Descargando solo fechas nuevas... {idx}/{len(missing_days)}",
        )

    progress.empty()
    return normalize_df(pd.DataFrame(rows)) if rows else empty_df()



def get_timeseries_incremental(start_date: date, end_date: date, step_days: int) -> tuple[pd.DataFrame, int]:
    existing = normalize_df(load_existing_history())
    existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
    existing = existing.dropna(subset=["date"])

    cached_dates = set(existing["date"].dt.date) if not existing.empty else set()
    requested_days = list(daterange(start_date, end_date, step_days))
    missing_days = [d for d in requested_days if d not in cached_dates]

    new_data = download_missing_dates(missing_days)

    combined = pd.concat([existing, new_data], ignore_index=True)
    combined = normalize_df(combined)
    if combined.empty:
        return empty_df(), len(missing_days)

    save_local_cache(combined)

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    mask = (combined["date"].dt.date >= start_date) & (combined["date"].dt.date <= end_date)
    return combined.loc[mask].copy(), len(missing_days)



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

    frequency = st.selectbox(
        "Frecuencia de muestreo",
        options=["Diaria", "Semanal"],
        index=1,
    )
    step_days = 1 if frequency == "Diaria" else 7

    use_forward_fill = st.checkbox("Rellenar huecos con último valor", value=True)

    st.markdown("---")
    st.write(f"Histórico base buscado en: `{REPO_XLSX}` o `{REPO_CSV}`")
    st.write(f"Cache incremental local: `{CACHE_FILE}`")

    uploaded_file = st.file_uploader("Importar histórico manual (CSV/XLSX)", type=["csv", "xlsx"])
    if uploaded_file is not None:
        try:
            imported = pd.read_excel(uploaded_file) if uploaded_file.name.lower().endswith(".xlsx") else pd.read_csv(uploaded_file)
            final_rows = merge_into_cache(imported)
            st.success(f"Histórico importado. Filas totales guardadas: {final_rows}.")
        except Exception as exc:  # noqa: BLE001
            st.error(f"No pude leer el archivo: {exc}")

    if st.button("Borrar cache local"):
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
        st.success("Cache local borrada. Recarga la página.")

if start_date > end_date:
    st.error("La fecha inicial no puede ser mayor que la final.")
    st.stop()

with st.spinner("Leyendo histórico y completando fechas faltantes..."):
    df, missing_count = get_timeseries_incremental(start_date, end_date, step_days)

if df.empty:
    st.error("No he podido extraer datos para ese rango.")
    st.stop()

history_now = load_existing_history()
st.info(
    f"Fechas pedidas a OMIP en esta carga: {missing_count}. "
    f"Fechas disponibles entre seed + cache: {len(history_now)}."
)

full_index = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="D")
plot_df = normalize_df(df).set_index("date").reindex(full_index)
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

st.download_button(
    "Descargar CSV",
    data=display_df.to_csv(index=False).encode("utf-8"),
    file_name=f"omip_forward_market_{start_date.isoformat()}_{end_date.isoformat()}.csv",
    mime="text/csv",
)

st.download_button(
    "Descargar XLSX",
    data=to_excel_bytes(display_df),
    file_name=f"omip_forward_market_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
