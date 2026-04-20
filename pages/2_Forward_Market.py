from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


st.set_page_config(page_title="OMIP Forward Market", layout="wide")
st.title("OMIP Forward Market | YR27 & YR28")
st.caption(
    "Lee primero un histórico local y solo descarga de OMIP las fechas que falten."
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
    return pd.DataFrame(columns=["date"] + [x.label for x in CONTRACT_SPECS])


def load_local_cache() -> pd.DataFrame:
    if not CACHE_FILE.exists():
        return empty_df()
    try:
        df = pd.read_csv(CACHE_FILE, parse_dates=["date"])
        if "date" not in df.columns:
            return empty_df()
        keep_cols = ["date"] + [x.label for x in CONTRACT_SPECS]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = pd.NA
        return df[keep_cols].sort_values("date").drop_duplicates(subset=["date"])
    except Exception:
        return empty_df()


def save_local_cache(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = df.copy().sort_values("date").drop_duplicates(subset=["date"])
    out.to_csv(CACHE_FILE, index=False)



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

    if not rows:
        return empty_df()
    return pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"])



def get_timeseries_incremental(start_date: date, end_date: date, step_days: int) -> tuple[pd.DataFrame, int]:
    cached = load_local_cache()
    cached_dates = set(pd.to_datetime(cached["date"]).dt.date) if not cached.empty else set()

    requested_days = list(daterange(start_date, end_date, step_days))
    missing_days = [d for d in requested_days if d not in cached_dates]

    new_data = download_missing_dates(missing_days)

    combined = pd.concat([cached, new_data], ignore_index=True)
    if combined.empty:
        return empty_df(), len(missing_days)

    combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    save_local_cache(combined)

    mask = (combined["date"].dt.date >= start_date) & (combined["date"].dt.date <= end_date)
    return combined.loc[mask].copy(), len(missing_days)


with st.sidebar:
    st.header("Configuración")
    default_start = date(date.today().year - 2, 1, 1)
    start_date = st.date_input("Desde", value=default_start)
    end_date = st.date_input("Hasta", value=date.today())

    frequency = st.selectbox(
        "Frecuencia de muestreo",
        options=["Diaria", "Semanal"],
        index=1,
        help="La app guarda lo descargado y solo pide a OMIP las fechas que aún no tenga.",
    )
    step_days = 1 if frequency == "Diaria" else 7

    use_forward_fill = st.checkbox(
        "Rellenar huecos con último valor",
        value=True,
        help="Útil si algunos días no devuelven fila o si eliges muestreo semanal.",
    )

    st.markdown("---")
    st.markdown(f"**Cache local:** `{CACHE_FILE}`")
    if st.button("Borrar cache local"):
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
        st.success("Cache local borrada. Recarga la página.")

if start_date > end_date:
    st.error("La fecha inicial no puede ser mayor que la final.")
    st.stop()

range_days = (end_date - start_date).days + 1
if frequency == "Diaria" and range_days > 900:
    st.warning("El rango diario es grande. La primera carga puede tardar; luego reutiliza el cache local.")

with st.spinner("Leyendo cache local y completando fechas faltantes..."):
    df, missing_count = get_timeseries_incremental(start_date, end_date, step_days)

if df.empty:
    st.error("No he podido extraer datos para ese rango.")
    st.stop()

cached_now = load_local_cache()
st.info(
    f"Fechas pedidas a OMIP en esta carga: {missing_count}. "
    f"Fechas guardadas en cache local: {len(cached_now)}."
)

full_index = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="D")
plot_df = df.set_index("date").reindex(full_index)
plot_df.index.name = "date"

if use_forward_fill:
    plot_df = plot_df.ffill()

st.subheader("Evolución")
selected_series = st.multiselect(
    "Series a mostrar",
    options=list(plot_df.columns),
    default=list(plot_df.columns),
)

if not selected_series:
    st.info("Selecciona al menos una serie.")
else:
    st.line_chart(plot_df[selected_series])

latest = df.sort_values("date").iloc[-1]
col1, col2, col3, col4 = st.columns(4)
for col, spec in zip([col1, col2, col3, col4], CONTRACT_SPECS):
    value = latest.get(spec.label)
    col.metric(spec.label, f"{value:,.2f} €/MWh" if pd.notna(value) else "n.d.")

st.subheader("Tabla")
display_df = df.copy()
for c in display_df.columns:
    if c != "date":
        display_df[c] = pd.to_numeric(display_df[c], errors="coerce")
st.dataframe(display_df, use_container_width=True)

csv_bytes = display_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Descargar CSV",
    data=csv_bytes,
    file_name=f"omip_forward_market_{start_date.isoformat()}_{end_date.isoformat()}.csv",
    mime="text/csv",
)

with st.expander("Notas"):
    st.markdown(
        f"""
- Se usa un cache local en `{CACHE_FILE}`.
- Primero se leen los datos ya guardados.
- Solo se consulta OMIP para las fechas que faltan dentro del rango pedido.
- Esto reduce mucho el tiempo de carga después de la primera ejecución.
- Importante: si lo despliegas en **Streamlit Community Cloud**, el disco local no siempre es persistente entre reinicios/redeploys.
  En ese caso, para persistencia real conviene guardar el CSV en un bucket, base de datos o GitHub Releases/raw.
        """
    )
