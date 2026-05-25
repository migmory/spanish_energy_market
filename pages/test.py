"""
Streamlit test REE: potencia instalada SIN autoconsumo — v4.

Run:
    pip install streamlit pandas requests plotly
    streamlit run ree_potencia_instalada_test_v4.py

Qué corrige esta v4:
- La v3 intentaba parsear la tabla de tecnologías del HTML del informe, pero esos
  valores NO vienen en el HTML estático; la web los renderiza/inyecta por JS o por
  una fuente no expuesta en el HTML que recibe requests.
- Por eso salían filas absurdas como Solar FV = 230 MW o Potencia total = 0.
- Esta versión NO intenta parsear tablas ocultas. Solo extrae magnitudes que están
  explícitamente escritas en el texto del informe:
    * total nacional instalado a 31/12/año
    * total incluyendo autoconsumo, cuando aparece
    * renovables nacionales, cuando aparece
    * almacenamiento nacional, cuando aparece
    * solar FV peninsular, cuando aparece
    * eólica peninsular, cuando aparece
- Para 2026 mensual mantiene un "probe" de API. Si REE devuelve 500/400, se ve claro.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from typing import Any, Iterable

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


REPORT_URLS = {
    2025: "https://www.sistemaelectrico-ree.es/es/informe-del-sistema-electrico/potencia-instalada",
    2024: "https://www.sistemaelectrico-ree.es/es/2024/informe-del-sistema-electrico/potencia-instalada",
    2023: "https://www.sistemaelectrico-ree.es/es/2023/informe-del-sistema-electrico/potencia-instalada",
    2022: "https://www.sistemaelectrico-ree.es/es/2022/informe-del-sistema-electrico/potencia-instalada",
    2021: "https://www.sistemaelectrico-ree.es/es/2021/informe-del-sistema-electrico/potencia-instalada",
    2020: "https://www.sistemaelectrico-ree.es/es/2020/informe-del-sistema-electrico/potencia-instalada",
}

API_CANDIDATES = [
    "https://apidatos.ree.es/es/datos/generacion/potencia-instalada",
    "https://apidatos.ree.es/es/datos/generacion/potencia-instalada-generacion",
]


@dataclass
class FetchResult:
    ok: bool
    source: str
    df: pd.DataFrame
    raw: Any | None = None
    error: str | None = None


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        data = data.strip()
        if data:
            self.parts.append(data)

    def get_text(self) -> str:
        return "\n".join(self.parts)


def html_to_text(raw_html: str) -> str:
    parser = TextExtractor()
    parser.feed(raw_html)
    text = html.unescape(parser.get_text())
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text


def parse_es_number_to_mw(token: str | int | float | None, unit: str = "MW") -> float | None:
    """
    Convierte números españoles a MW.

    Ejemplos:
    - "142.558" + MW => 142558
    - "95,6" + GW => 95600
    - "10" + GW => 10000
    """
    if token is None:
        return None

    if isinstance(token, (int, float)):
        val = float(token)
    else:
        s = str(token).strip()
        s = s.replace("\xa0", "").replace(" ", "")

        if "." in s and "," in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        elif re.match(r"^-?\d{1,3}(\.\d{3})+$", s):
            s = s.replace(".", "")

        try:
            val = float(s)
        except ValueError:
            return None

    if unit.upper() == "GW":
        val *= 1000.0

    return val


def first_match_mw(text: str, patterns: list[tuple[str, str]]) -> tuple[float | None, str | None]:
    """
    patterns = [(regex, unit), ...]
    El regex debe tener un grupo con el número.
    """
    for pattern, unit in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            val = parse_es_number_to_mw(m.group(1), unit)
            if val is not None:
                return val, m.group(0)[:300]
    return None, None


def parse_report_metrics(text: str, year: int, url: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def add(metric: str, value_mw: float | None, source_text: str | None, confidence: str = "high") -> None:
        if value_mw is None:
            return
        rows.append(
            {
                "year": year,
                "date": pd.Timestamp(f"{year}-12-31"),
                "period": str(year),
                "metric": metric,
                "mw": round(float(value_mw), 3),
                "source_url": url,
                "source_text": source_text or "",
                "confidence": confidence,
            }
        )

    # Total nacional sin autoconsumo. Esta frase aparece clara en los informes.
    total, total_src = first_match_mw(
        text,
        [
            (
                rf"A 31 de diciembre de {year}.*?potencia instalada de\s+([0-9\.\,]+)\s*MW",
                "MW",
            ),
            (
                r"ha alcanzado.*?potencia instalada de\s+([0-9\.\,]+)\s*MW",
                "MW",
            ),
        ],
    )
    add("Total nacional sin autoconsumo visible", total, total_src)

    # Total nacional incluyendo autoconsumo: aparece explícito en 2025.
    total_auto, total_auto_src = first_match_mw(
        text,
        [
            (
                r"tenemos en cuenta las instalaciones de autoconsumo.*?asciende a\s+([0-9\.\,]+)\s*MW",
                "MW",
            ),
            (
                r"incluyendo autoconsumo.*?([0-9\.\,]+)\s*MW",
                "MW",
            ),
        ],
    )
    add("Total nacional incluyendo autoconsumo", total_auto, total_auto_src)

    # Renovables nacionales: aparece como X GW.
    renov, renov_src = first_match_mw(
        text,
        [
            (
                r"alcanzar una potencia instalada de fuentes de generación renovables de\s+([0-9\.\,]+)\s*GW",
                "GW",
            ),
            (
                r"potencia instalada de generación renovable.*?alcanz.*?([0-9\.\,]+)\s*GW",
                "GW",
            ),
        ],
    )
    add("Renovables nacional", renov, renov_src)

    # Almacenamiento nacional.
    storage, storage_src = first_match_mw(
        text,
        [
            (
                rf"potencia instalada de almacenamiento del sistema eléctrico español en {year} se sitúa en\s+([0-9\.\,]+)\s*MW",
                "MW",
            ),
            (
                r"almacenamiento del sistema eléctrico español.*?se sitúa en\s+([0-9\.\,]+)\s*MW",
                "MW",
            ),
        ],
    )
    add("Almacenamiento nacional", storage, storage_src)

    # Turbinación bombeo y baterías, si están en el texto.
    pumped, pumped_src = first_match_mw(
        text,
        [
            (
                r"de los cuales\s+([0-9\.\,]+)\s+corresponden a turbinación bombeo",
                "MW",
            )
        ],
    )
    add("Turbinación bombeo nacional", pumped, pumped_src)

    batteries, batteries_src = first_match_mw(
        text,
        [
            (
                r"turbinación bombeo y\s+([0-9\.\,]+)\s*MW a baterías",
                "MW",
            )
        ],
    )
    add("Baterías nacional", batteries, batteries_src)

    # Solar FV peninsular. No es nacional; lo marco claramente.
    pv_pen, pv_pen_src = first_match_mw(
        text,
        [
            (
                r"solar fotovoltaica.*?potencia instalada peninsular con\s+([0-9\.\,]+)\s*MW",
                "MW",
            ),
            (
                r"solar fotovoltaica.*?con\s+([0-9\.\,]+)\s*MW.*?potencia instalada peninsular",
                "MW",
            ),
        ],
    )
    add("Solar fotovoltaica peninsular", pv_pen, pv_pen_src)

    # Eólica peninsular.
    wind_pen, wind_pen_src = first_match_mw(
        text,
        [
            (
                r"potencia instalada eólica.*?con un total de\s+([0-9\.\,]+)\s*MW",
                "MW",
            )
        ],
    )
    add("Eólica peninsular", wind_pen, wind_pen_src)

    return pd.DataFrame(rows)


def fetch_report_year(year: int) -> FetchResult:
    url = REPORT_URLS.get(year)
    if not url:
        return FetchResult(False, f"report {year}", pd.DataFrame(), error=f"No hay URL configurada para {year}")

    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        if r.status_code != 200:
            return FetchResult(False, url, pd.DataFrame(), error=f"HTTP {r.status_code}: {r.text[:300]}")

        text = html_to_text(r.text)
        df = parse_report_metrics(text, year, url)

        if df.empty:
            return FetchResult(False, url, pd.DataFrame(), raw=text[:5000], error="Página descargada, pero sin métricas parseables.")

        return FetchResult(True, url, df, raw=text[:5000])

    except Exception as exc:
        return FetchResult(False, url, pd.DataFrame(), error=f"{type(exc).__name__}: {exc}")


def fetch_reports(start_year: int, end_year: int) -> FetchResult:
    dfs: list[pd.DataFrame] = []
    errors: list[str] = []

    for year in range(start_year, end_year + 1):
        res = fetch_report_year(year)
        if res.ok:
            dfs.append(res.df)
        else:
            errors.append(f"{year}: {res.error}")

    if dfs:
        return FetchResult(
            ok=True,
            source="Informes del sistema eléctrico REE",
            df=pd.concat(dfs, ignore_index=True),
            error="\n".join(errors) if errors else None,
        )

    return FetchResult(False, "Informes del sistema eléctrico REE", pd.DataFrame(), error="\n".join(errors))


def walk_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from walk_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


def parse_api_json(raw: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for node in walk_dicts(raw):
        attrs = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
        values = node.get("values") or attrs.get("values")

        if not isinstance(values, list):
            continue

        title = (
            node.get("title")
            or node.get("name")
            or attrs.get("title")
            or attrs.get("name")
            or node.get("type")
            or attrs.get("type")
            or "unknown"
        )

        for point in values:
            if not isinstance(point, dict):
                continue

            dt = point.get("datetime") or point.get("date")
            val = parse_es_number_to_mw(point.get("value"), "MW")

            if dt is None or val is None:
                continue

            rows.append({"datetime": dt, "metric": str(title), "mw": val})

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["datetime"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["date"]).copy()
    df["year"] = df["date"].dt.year
    df["period"] = df["date"].dt.to_period("M").astype(str)
    return df


def probe_monthly_api(start: str, end: str) -> FetchResult:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 streamlit-ree-test/0.4",
    }

    param_sets = [
        {"start_date": start, "end_date": end, "time_trunc": "month"},
        {
            "start_date": start,
            "end_date": end,
            "time_trunc": "month",
            "systemElectric": "nacional",
        },
        {
            "start_date": start,
            "end_date": end,
            "time_trunc": "month",
            "geo_trunc": "electric_system",
            "geo_limit": "nacional",
            "geo_ids": "8741",
        },
    ]

    errors: list[str] = []

    for endpoint in API_CANDIDATES:
        for params in param_sets:
            try:
                r = requests.get(endpoint, params=params, headers=headers, timeout=30)

                if r.status_code != 200:
                    errors.append(f"{endpoint} | {params} | HTTP {r.status_code}: {r.text[:250]}")
                    continue

                raw = r.json()
                df = parse_api_json(raw)

                if not df.empty:
                    return FetchResult(True, f"{endpoint} | params={params}", df, raw=raw)

                errors.append(f"{endpoint} | {params} | HTTP 200 pero sin series parseables")

            except Exception as exc:
                errors.append(f"{endpoint} | {params} | {type(exc).__name__}: {exc}")

    return FetchResult(False, "REData API monthly probe", pd.DataFrame(), error="\n".join(errors))


def validate_2025(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    checks: list[dict[str, Any]] = []
    d2025 = df[df["year"].eq(2025)]

    expected = {
        "Total nacional sin autoconsumo visible": 142_558,
        "Total nacional incluyendo autoconsumo": 150_809,
        "Almacenamiento nacional": 3_427,
        "Solar fotovoltaica peninsular": 40_952,
        "Eólica peninsular": 32_593,
    }

    for metric, exp in expected.items():
        row = d2025[d2025["metric"].eq(metric)]
        if row.empty:
            continue
        got = float(row.iloc[-1]["mw"])
        checks.append(
            {
                "metric": metric,
                "expected_mw": exp,
                "got_mw": got,
                "diff_mw": got - exp,
                "status": "OK" if abs(got - exp) < 1 else "REVISAR",
            }
        )

    return pd.DataFrame(checks)


def main() -> None:
    st.set_page_config(page_title="REE potencia instalada v4", layout="wide")
    st.title("REE potencia instalada — test sin autoconsumo v4")

    st.warning(
        "Importante: la tabla de tecnologías del informe no viene en el HTML estático. "
        "Esta versión evita parsear esa tabla y solo extrae datos explícitos del texto, "
        "para no generar números falsos."
    )

    with st.sidebar:
        st.header("Parámetros")
        start_year = st.number_input("Año inicio anual", min_value=2020, max_value=2025, value=2020)
        end_year = st.number_input("Año fin anual", min_value=2020, max_value=2025, value=2025)
        start_2026 = st.date_input("Inicio mensual 2026", value=date(2026, 1, 1))
        end_2026 = st.date_input("Fin mensual 2026", value=date(2026, 12, 31))
        show_sources = st.checkbox("Mostrar texto fuente de cada métrica", value=False)
        show_debug = st.checkbox("Mostrar debug API", value=True)

    if start_year > end_year:
        st.error("El año inicial no puede ser mayor que el año final.")
        return

    st.header("1) Anual desde texto de informes REE")

    annual_res = fetch_reports(int(start_year), int(end_year))

    if annual_res.ok:
        df = annual_res.df.copy()
        st.success(f"Datos extraídos desde: {annual_res.source}")

        display_cols = ["year", "metric", "mw", "confidence", "source_url"]
        if show_sources:
            display_cols.append("source_text")

        st.dataframe(df[display_cols], use_container_width=True)

        checks = validate_2025(df)
        if not checks.empty:
            st.subheader("Validación 2025")
            st.dataframe(checks, use_container_width=True)

        pivot = df.pivot_table(index="year", columns="metric", values="mw", aggfunc="last").reset_index()
        st.subheader("Vista pivote")
        st.dataframe(pivot, use_container_width=True)

        fig = px.line(
            df,
            x="year",
            y="mw",
            color="metric",
            markers=True,
            title="Métricas extraídas del texto del informe",
        )
        fig.update_layout(yaxis_title="MW", xaxis_title="Año")
        st.plotly_chart(fig, use_container_width=True)

        st.download_button(
            "Descargar anual CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="ree_potencia_instalada_informe_texto_v4.csv",
            mime="text/csv",
        )
    else:
        st.error("No se pudo extraer la parte anual.")
        st.code(annual_res.error or "Sin detalle", language="text")

    st.header("2) Mensual 2026 — probe API")

    monthly_start = f"{start_2026:%Y-%m-%d}T00:00"
    monthly_end = f"{end_2026:%Y-%m-%d}T23:59"
    monthly_res = probe_monthly_api(monthly_start, monthly_end)

    if monthly_res.ok:
        st.success(f"Mensual 2026 cargado desde API: {monthly_res.source}")
        st.dataframe(monthly_res.df, use_container_width=True)
        fig_m = px.line(
            monthly_res.df,
            x="period",
            y="mw",
            color="metric",
            markers=True,
            title="Potencia instalada mensual 2026 desde API",
        )
        st.plotly_chart(fig_m, use_container_width=True)
    else:
        st.info(
            "No se ha encontrado mensual 2026 por API pública para este widget. "
            "En tu prueba ya se veía HTTP 500/400, así que esto probablemente no está expuesto."
        )
        if show_debug:
            st.code(monthly_res.error or "Sin detalle", language="text")

    if show_debug:
        st.header("Debug")
        st.subheader("Errores de informes")
        st.code(annual_res.error or "Sin errores de informes", language="text")

        st.subheader("Endpoints API probados")
        st.code("\n".join(API_CANDIDATES), language="text")


if __name__ == "__main__":
    main()
