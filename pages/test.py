
"""
Streamlit test REE: potencia instalada SIN autoconsumo.

Run:
    pip install streamlit pandas requests plotly
    streamlit run ree_potencia_instalada_test_v3.py

Por qué esta v3:
- El endpoint público `apidatos.ree.es/es/datos/generacion/potencia-instalada`
  está devolviendo HTTP 500 para este widget.
- El endpoint `potencia-instalada-generacion` devuelve HTTP 400 porque no parece
  ser un widget API válido.
- Por tanto, la parte anual se obtiene de las páginas del Informe del Sistema
  de sistemaelectrico-ree.es.
- La parte mensual 2026 se deja como "probe" de API: si REE lo expone, carga;
  si no, lo enseña claramente como no disponible.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from typing import Any, Iterable

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


REPORT_TOTAL_2025_MW = 142_558
REPORT_SOLAR_FV_2025_MW = 41_660

# En la web de informe, 2025 está en la URL sin año.
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

TECH_ORDER = [
    "Hidráulica",
    "Hidroeólica",
    "Eólica",
    "Solar fotovoltaica",
    "Solar Fotovoltaica",
    "Solar térmica",
    "Solar Térmica",
    "Otras renovables",
    "Otras Renovables",
    "Residuos renovables",
    "Residuos Renovables",
    "Renovables",
    "Nuclear",
    "Carbón",
    "Motor diésel",
    "Motor Diésel",
    "Turbina de gas",
    "Turbina de Gas",
    "Turbina de vapor",
    "Turbina de Vapor",
    "Fuel + Gas",
    "Fuel+Gas",
    "Fuel",
    "Ciclo combinado",
    "Ciclo Combinado",
    "Cogeneración",
    "Residuos no renovables",
    "Residuos no Renovables",
    "No renovables",
    "No Renovables",
    "Generación",
    "Turbinación bombeo",
    "Turbinación Bombeo",
    "Baterías",
    "Almacenamiento",
    "Potencia total",
    "Total",
]


@dataclass
class FetchResult:
    ok: bool
    source: str
    df: pd.DataFrame
    raw: Any | None = None
    error: str | None = None


class TextExtractor(HTMLParser):
    """Extrae texto de HTML con separadores para no juntar celdas."""

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
    text = parser.get_text()
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    return text


def parse_spanish_number(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    txt = str(value).strip()
    if not txt:
        return None

    txt = txt.replace("\xa0", "").replace(" ", "")

    # 142.558 => 142558
    # 7,3 => 7.3
    # 1.234,56 => 1234.56
    if "." in txt and "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    elif "," in txt:
        txt = txt.replace(",", ".")
    elif re.match(r"^-?\d{1,3}(\.\d{3})+$", txt):
        txt = txt.replace(".", "")

    try:
        return float(txt)
    except ValueError:
        return None


def normalise_technology(name: str) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    fixes = {
        "Solar Fotovoltaica": "Solar fotovoltaica",
        "Solar Térmica": "Solar térmica",
        "Otras Renovables": "Otras renovables",
        "Residuos Renovables": "Residuos renovables",
        "Motor Diésel": "Motor diésel",
        "Turbina de Gas": "Turbina de gas",
        "Turbina de Vapor": "Turbina de vapor",
        "Ciclo Combinado": "Ciclo combinado",
        "Residuos no Renovables": "Residuos no renovables",
        "No Renovables": "No renovables",
        "Turbinación Bombeo": "Turbinación bombeo",
        "Fuel+Gas": "Fuel + Gas",
        "Total": "Potencia total",
    }
    return fixes.get(name, name)


def extract_report_headline_total(text: str, year: int) -> float | None:
    # Ejemplo 2025:
    # "... potencia instalada de 142.558 MW ..."
    # Ejemplo 2024:
    # "... potencia instalada de 132.343 MW."
    patterns = [
        r"potencia instalada de\s+([0-9\.\,]+)\s*MW",
        r"ha alcanzado.*?([0-9\.\,]+)\s*MW",
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return parse_spanish_number(m.group(1))

    return None


def find_row_segment(text: str, tech: str) -> str | None:
    """
    Busca el segmento de texto correspondiente a una tecnología.
    Funciona tanto si el HTMLParser separa celdas con saltos como si el chart
    aparece como texto compacto.
    """
    candidates = [tech]
    if tech == "Solar fotovoltaica":
        candidates.append("Solar Fotovoltaica")
    if tech == "Potencia total":
        candidates.append("Total")

    all_names = sorted(set(TECH_ORDER), key=len, reverse=True)

    for c in candidates:
        pos = text.find(c)
        if pos == -1:
            continue

        start = pos + len(c)
        end_positions = []
        for nxt in all_names:
            if nxt == c:
                continue
            p = text.find(nxt, start)
            if p != -1:
                end_positions.append(p)

        end = min(end_positions) if end_positions else min(len(text), start + 250)
        return text[start:end]

    return None


def extract_numbers_from_segment(segment: str) -> list[float]:
    """
    Extrae números españoles de un segmento.
    Si vienen con saltos/separadores, va bien.
    Si vienen compactados sin separador, se intenta una segunda estrategia simple.
    """
    # Caso normal: hay separadores entre celdas.
    tokens = re.findall(r"-?\d{1,3}(?:\.\d{3})*(?:,\d+)?|-?\d+(?:,\d+)?", segment)
    nums = [parse_spanish_number(t) for t in tokens]
    nums = [n for n in nums if n is not None]

    # Si sale un solo token enorme, probablemente el chart juntó celdas.
    # En ese caso no intentamos adivinar cada columna; es mejor no contaminar.
    if len(nums) <= 1:
        return []

    return nums


def parse_report_capacity_table(text: str, year: int) -> pd.DataFrame:
    """
    Extrae filas de la tabla "Potencia instalada a 31.12.YYYY".

    La tabla tiene normalmente:
    Sistema peninsular: MW, %
    Sistema no peninsular: MW, %
    Nacional: MW, %

    Por eso tomamos:
    - nums[0] = peninsular MW
    - nums[2] = no peninsular MW
    - nums[4] = nacional MW
    """
    # Acotamos al bloque de tabla para evitar coger mapas por CCAA.
    start_marker = f"Potencia instalada a 31.12.{year}"
    start = text.find(start_marker)
    block = text[start:] if start != -1 else text

    # Cortamos antes de variaciones/evolución para no mezclar con otros gráficos.
    cut_markers = [
        "Variaciones de potencia",
        "Evolución de la estructura",
        "Mapa de potencia",
        "Desglose de potencia",
    ]
    cuts = [block.find(m) for m in cut_markers if block.find(m) != -1]
    if cuts:
        block = block[: min(cuts)]

    rows: list[dict[str, Any]] = []

    seen = set()
    for tech in TECH_ORDER:
        tech_norm = normalise_technology(tech)
        if tech_norm in seen:
            continue
        seen.add(tech_norm)

        segment = find_row_segment(block, tech)
        if not segment:
            continue

        nums = extract_numbers_from_segment(segment)
        if len(nums) >= 5:
            rows.append(
                {
                    "year": year,
                    "date": pd.Timestamp(f"{year}-12-31"),
                    "technology": tech_norm,
                    "peninsular_mw": nums[0],
                    "non_peninsular_mw": nums[2],
                    "national_mw": nums[4],
                    "period": str(year),
                    "source": "report_table",
                }
            )

    df = pd.DataFrame(rows)

    # Fallback seguro para total anual desde el párrafo.
    headline_total = extract_report_headline_total(text, year)
    if headline_total is not None:
        if df.empty or not df["technology"].eq("Potencia total").any():
            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        [
                            {
                                "year": year,
                                "date": pd.Timestamp(f"{year}-12-31"),
                                "technology": "Potencia total",
                                "peninsular_mw": None,
                                "non_peninsular_mw": None,
                                "national_mw": headline_total,
                                "period": str(year),
                                "source": "report_headline",
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    return df


def fetch_report_year(year: int) -> FetchResult:
    url = REPORT_URLS.get(year)
    if not url:
        return FetchResult(
            ok=False,
            source=f"report {year}",
            df=pd.DataFrame(),
            error=f"No tengo URL configurada para {year}. Añádela en REPORT_URLS.",
        )

    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        if r.status_code != 200:
            return FetchResult(
                ok=False,
                source=url,
                df=pd.DataFrame(),
                error=f"HTTP {r.status_code}: {r.text[:300]}",
            )

        text = html_to_text(r.text)
        df = parse_report_capacity_table(text, year)
        if df.empty:
            return FetchResult(
                ok=False,
                source=url,
                df=pd.DataFrame(),
                raw=text[:4000],
                error="Página descargada, pero no pude parsear la tabla.",
            )

        df["url"] = url
        return FetchResult(ok=True, source=url, df=df, raw=text[:4000])

    except Exception as exc:
        return FetchResult(
            ok=False,
            source=url,
            df=pd.DataFrame(),
            error=f"{type(exc).__name__}: {exc}",
        )


def fetch_annual_reports(start_year: int, end_year: int) -> FetchResult:
    dfs = []
    errors = []

    for year in range(start_year, end_year + 1):
        res = fetch_report_year(year)
        if res.ok:
            dfs.append(res.df)
        else:
            errors.append(f"{year}: {res.error}")

    if dfs:
        return FetchResult(
            ok=True,
            source="sistemaelectrico-ree.es report pages",
            df=pd.concat(dfs, ignore_index=True),
            error="\n".join(errors) if errors else None,
        )

    return FetchResult(
        ok=False,
        source="sistemaelectrico-ree.es report pages",
        df=pd.DataFrame(),
        error="\n".join(errors) or "Sin datos",
    )


def walk_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


def parse_api_json(raw: dict[str, Any]) -> pd.DataFrame:
    rows = []

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
            val = parse_spanish_number(point.get("value"))
            if dt is None or val is None:
                continue
            rows.append({"datetime": dt, "technology": title, "national_mw": val})

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["datetime"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["date"]).copy()
    df["year"] = df["date"].dt.year
    df["period"] = df["date"].dt.to_period("M").astype(str)
    return df


def probe_monthly_2026_api(start: str, end: str) -> FetchResult:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 streamlit-ree-test/0.3",
    }

    param_sets = [
        {"start_date": start, "end_date": end, "time_trunc": "month"},
        {
            "start_date": start,
            "end_date": end,
            "time_trunc": "month",
            "geo_trunc": "electric_system",
            "geo_limit": "peninsular",
            "geo_ids": "8741",
        },
    ]

    errors = []

    for endpoint in API_CANDIDATES:
        for params in param_sets:
            try:
                r = requests.get(endpoint, params=params, headers=headers, timeout=30)
                if r.status_code != 200:
                    errors.append(
                        f"{endpoint} | {params} | HTTP {r.status_code}: {r.text[:300]}"
                    )
                    continue

                raw = r.json()
                df = parse_api_json(raw)
                if not df.empty:
                    return FetchResult(
                        ok=True,
                        source=f"{endpoint} | params={params}",
                        df=df,
                        raw=raw,
                    )

                errors.append(
                    f"{endpoint} | {params} | HTTP 200 pero sin series parseables."
                )

            except Exception as exc:
                errors.append(f"{endpoint} | {params} | {type(exc).__name__}: {exc}")

    return FetchResult(
        ok=False,
        source="REData API monthly probe",
        df=pd.DataFrame(),
        error="\n".join(errors),
    )


def validate_2025(df: pd.DataFrame) -> pd.DataFrame:
    checks = []
    if df.empty:
        return pd.DataFrame(checks)

    d2025 = df[df["year"] == 2025].copy()
    if d2025.empty:
        return pd.DataFrame(checks)

    total = d2025[d2025["technology"] == "Potencia total"]
    if not total.empty:
        got = float(total.iloc[-1]["national_mw"])
        checks.append(
            {
                "check": "Total 2025 sin autoconsumo",
                "expected_mw": REPORT_TOTAL_2025_MW,
                "got_mw": got,
                "diff_mw": got - REPORT_TOTAL_2025_MW,
                "status": "OK" if abs(got - REPORT_TOTAL_2025_MW) < 1 else "REVISAR",
            }
        )

    fv = d2025[d2025["technology"] == "Solar fotovoltaica"]
    if not fv.empty:
        got = float(fv.iloc[-1]["national_mw"])
        checks.append(
            {
                "check": "Solar FV 2025 sin autoconsumo",
                "expected_mw": REPORT_SOLAR_FV_2025_MW,
                "got_mw": got,
                "diff_mw": got - REPORT_SOLAR_FV_2025_MW,
                "status": "OK" if abs(got - REPORT_SOLAR_FV_2025_MW) < 1 else "REVISAR",
            }
        )

    return pd.DataFrame(checks)


def main() -> None:
    st.set_page_config(page_title="REE potencia instalada v3", layout="wide")
    st.title("REE potencia instalada — test sin autoconsumo")

    st.info(
        "Esta v3 no depende del endpoint de API que está devolviendo 500. "
        "Para la serie anual usa las páginas del Informe del Sistema."
    )

    with st.sidebar:
        st.header("Parámetros")
        start_year = st.number_input("Año inicio anual", min_value=2020, max_value=2025, value=2020)
        end_year = st.number_input("Año fin anual", min_value=2020, max_value=2025, value=2025)
        start_2026 = st.date_input("Inicio mensual 2026", value=date(2026, 1, 1))
        end_2026 = st.date_input("Fin mensual 2026", value=date(2026, 12, 31))
        show_debug = st.checkbox("Mostrar debug", value=True)

    st.header("1) Anual desde informes REE")

    annual_res = fetch_annual_reports(int(start_year), int(end_year))

    if annual_res.ok:
        annual_df = annual_res.df.copy()
        st.success(f"Datos anuales cargados desde: {annual_res.source}")
        st.dataframe(annual_df, use_container_width=True)

        checks = validate_2025(annual_df)
        if not checks.empty:
            st.subheader("Validación 2025")
            st.dataframe(checks, use_container_width=True)

        # Gráfico: solo nacional.
        chart_df = annual_df.copy()
        totals = chart_df[chart_df["technology"] == "Potencia total"]
        if len(totals) < len(chart_df):
            chart_df = chart_df[chart_df["technology"] != "Potencia total"]

        fig = px.line(
            chart_df,
            x="period",
            y="national_mw",
            color="technology",
            markers=True,
            title="Potencia instalada nacional por tecnología",
        )
        fig.update_layout(yaxis_title="MW", xaxis_title="Año")
        st.plotly_chart(fig, use_container_width=True)

        csv = annual_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Descargar anual CSV",
            data=csv,
            file_name="ree_potencia_instalada_anual_sin_autoconsumo.csv",
            mime="text/csv",
        )
    else:
        st.error("No se pudo cargar la parte anual.")
        st.code(annual_res.error or "Sin detalle", language="text")

    st.header("2) Mensual 2026 — probe API")

    monthly_start = f"{start_2026:%Y-%m-%d}T00:00"
    monthly_end = f"{end_2026:%Y-%m-%d}T23:59"

    monthly_res = probe_monthly_2026_api(monthly_start, monthly_end)

    if monthly_res.ok:
        st.success(f"Mensual 2026 cargado desde API: {monthly_res.source}")
        st.dataframe(monthly_res.df, use_container_width=True)

        fig_m = px.line(
            monthly_res.df,
            x="period",
            y="national_mw",
            color="technology",
            markers=True,
            title="Potencia instalada mensual 2026",
        )
        st.plotly_chart(fig_m, use_container_width=True)
    else:
        st.warning(
            "No he encontrado mensual 2026 en la API pública de REE para este widget. "
            "Esto es consistente con el HTTP 500/400 que estabas viendo."
        )
        st.code(monthly_res.error or "Sin detalle", language="text")

    if show_debug:
        st.header("Debug")
        st.subheader("Errores anual")
        st.code(annual_res.error or "Sin errores anuales", language="text")

        st.subheader("Endpoints API probados para mensual")
        st.code("\n".join(API_CANDIDATES), language="text")

        st.subheader("Errores mensual")
        st.code(monthly_res.error or "Sin errores mensuales", language="text")


if __name__ == "__main__":
    main()
