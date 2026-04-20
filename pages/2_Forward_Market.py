from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


@dataclass(frozen=True)
class ContractSpec:
    instrument: str  # FTB or FTS
    contract: str    # YR-27, YR-28
    label: str       # Baseload YR27, Solar YR28, ...


CONTRACT_SPECS = [
    ContractSpec("FTB", "YR-27", "Baseload YR27"),
    ContractSpec("FTB", "YR-28", "Baseload YR28"),
    ContractSpec("FTS", "YR-27", "Solar YR27"),
    ContractSpec("FTS", "YR-28", "Solar YR28"),
]


st.set_page_config(page_title="OMIP Forward Market", layout="wide")
st.title("OMIP Forward Market | YR27 & YR28")
st.caption(
    "Serie histórica obtenida de la página pública de resultados de mercado de OMIP "
    "(instrumentos FTB baseload y FTS solar, zona ES)."
)


def daterange(start: date, end: date, step_days: int) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=step_days)


def build_url(day: date, instrument: str) -> str:
    return (
        f"{BASE_URL}?date={day.isoformat()}&product=EL&zone=ES&instrument={instrument}"
    )


@st.cache_data(show_spinner=False, ttl=60 * 60 * 12)
def fetch_page(day_iso: str, instrument: str) -> str:
    target_date = datetime.strptime(day_iso, "%Y-%m-%d").date()
    url = build_url(target_date, instrument)
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
    """
    Busca la línea tipo:
    FTB YR-27 n.a. n.a. 0 n.a. n.a. n.a. 2 17520 n.a. 56.60
    o
    FTS YR-28 n.a. n.a. 0 n.a. n.a. n.a. 26 0 0 29.62 29.65

    y devuelve el último valor numérico de la línea, que en la página pública
    corresponde a la referencia D-1 / settlement visible para ese día.

    Si prefieres usar la columna D en vez de D-1, cambia numbers[-1] por numbers[-2]
    cuando existan dos valores finales.
    """
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

    # En estas líneas OMIP suele dejar al final D y D-1. Para la serie diaria,
    # usamos el último valor visible de la fila por robustez.
    return float(numbers[-1])


@st.cache_data(show_spinner=False, ttl=60 * 60 * 12)
def fetch_timeseries(start_iso: str, end_iso: str, step_days: int) -> pd.DataFrame:
    start_date = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_iso, "%Y-%m-%d").date()

    rows: list[dict] = []
    all_days = list(daterange(start_date, end_date, step_days))

    progress = st.progress(0, text="Descargando datos OMIP...")

    for idx, day in enumerate(all_days, start=1):
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
            min(idx / max(len(all_days), 1), 1.0),
            text=f"Descargando datos OMIP... {idx}/{len(all_days)}",
        )

    progress.empty()

    if not rows:
        return pd.DataFrame(columns=["date"] + [x.label for x in CONTRACT_SPECS])

    df = pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"])
    return df


with st.sidebar:
    st.header("Configuración")
    default_start = date(date.today().year - 2, 1, 1)
    start_date = st.date_input("Desde", value=default_start)
    end_date = st.date_input("Hasta", value=date.today())

    frequency = st.selectbox(
        "Frecuencia de muestreo",
        options=["Diaria", "Semanal"],
        index=1,
        help=(
            "Diaria hace una petición por cada día del rango. "
            "Semanal reduce mucho el tiempo de carga."
        ),
    )
    step_days = 1 if frequency == "Diaria" else 7

    use_forward_fill = st.checkbox(
        "Rellenar huecos con último valor", value=True,
        help="Útil si algunos días no devuelven fila o si eliges muestreo semanal."
    )

    st.markdown("---")
    st.markdown("**Fuente pública OMIP**")
    st.code("instrument=FTB -> baseload\ninstrument=FTS -> solar")

if start_date > end_date:
    st.error("La fecha inicial no puede ser mayor que la final.")
    st.stop()

range_days = (end_date - start_date).days + 1
if frequency == "Diaria" and range_days > 900:
    st.warning(
        "El rango diario es grande y puede tardar bastante. "
        "Para histórico largo suele ir mejor semanal."
    )

with st.spinner("Construyendo serie histórica..."):
    df = fetch_timeseries(start_date.isoformat(), end_date.isoformat(), step_days)

if df.empty:
    st.error("No he podido extraer datos para ese rango. Prueba con menos rango o frecuencia semanal.")
    st.stop()

# Reindexado opcional para que la gráfica quede continua
full_index = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="D")
plot_df = df.set_index("date").reindex(full_index)
plot_df.index.name = "date"

if step_days == 7:
    # Mantener puntos semanales, pero con posibilidad de rellenar huecos visuales.
    if use_forward_fill:
        plot_df = plot_df.ffill()
else:
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
metric_cols = [col1, col2, col3, col4]
for col, spec in zip(metric_cols, CONTRACT_SPECS):
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

with st.expander("Notas técnicas"):
    st.markdown(
        """
- La app consulta la página pública de OMIP por fecha y por instrumento.
- `FTB` corresponde a **SPEL Base Futures** y `FTS` a **SPEL Solar Futures**.
- Para cada fecha busca las filas `YR-27` y `YR-28`.
- En el parseo se usa el **último valor numérico visible de la fila**. Si en tu validación prefieres usar la columna `D` en vez de `D-1`, cambia una línea en `parse_contract_value()`.
- La caché de Streamlit evita repetir llamadas durante 12 horas.
        """
    )
