"""
Streamlit test: REE installed capacity (sin autoconsumo) annual to 2025 + monthly 2026.

Run:
    pip install streamlit pandas requests plotly beautifulsoup4 lxml
    streamlit run ree_potencia_instalada_test.py

Notes:
- The 2025 report page explicitly separates:
    * 142,558 MW = potencia instalada del sistema eléctrico español
      (generación + almacenamiento visible to system, "sin autoconsumo no visible")
    * 150,809 MW = total if including autoconsumo not visible
- This script tries the REData API first, then scrapes the report page as fallback.
- The 2026 monthly series is attempted via the same REData API family. If REE's endpoint
  changes or returns a different taxonomy, the app shows raw/debug output to adjust parsing.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from bs4 import BeautifulSoup


REPORT_URL = "https://www.sistemaelectrico-ree.es/es/informe-del-sistema-electrico/potencia-instalada"

# Public REData API candidate endpoints.
# Historically the browser page uses /datos/generacion/potencia-instalada-generacion,
# while the public API often uses /datos/generacion/potencia-instalada.
API_ENDPOINTS = [
    "https://apidatos.ree.es/es/datos/generacion/potencia-instalada",
    "https://apidatos.ree.es/es/datos/generacion/potencia-instalada-generacion",
]

EXPECTED_2025_TOTAL_SIN_AUTOCONSUMO_MW = 142_558  # from REE Informe del Sistema 2025 text/table


@dataclass
class FetchResult:
    ok: bool
    source: str
    df: pd.DataFrame
    raw: Any | None = None
    error: str | None = None


def _safe_float(x: Any) -> float | None:
    """Parse Spanish/English numeric strings into float."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if not s:
        return None

    # Remove units and NBSP; keep digits, signs, separators.
    s = s.replace("\xa0", " ")
    s = re.sub(r"[^0-9,\.\-]", "", s)

    # Spanish thousands/decimal heuristic:
    # "142.558" in REE text means 142558 MW, but "7,3" means 7.3.
    if "," in s and "." in s:
        # "1.234,56" -> "1234.56"
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # "7,3" -> "7.3"
        s = s.replace(",", ".")
    elif "." in s:
        # If a dot is followed by exactly 3 digits and no decimal pattern, treat as thousands.
        if re.match(r"^-?\d{1,3}(\.\d{3})+$", s):
            s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


def _walk_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    """Yield all dict nodes inside a JSON object."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_dicts(item)


def _extract_series_from_reddata_json(raw: dict[str, Any]) -> pd.DataFrame:
    """
    Generic parser for REData JSONAPI responses.

    It searches for nodes with a `values` list. Each value usually includes:
        datetime, value, percentage
    The parent node often includes title/type/groupId.
    """
    rows: list[dict[str, Any]] = []

    for node in _walk_dicts(raw):
        values = node.get("values")
        if not isinstance(values, list) or not values:
            continue

        attrs = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
        title = (
            node.get("title")
            or node.get("name")
            or attrs.get("title")
            or attrs.get("name")
            or node.get("type")
            or "unknown"
        )
        group = node.get("groupId") or node.get("group_id") or attrs.get("groupId")
        serie_type = node.get("type") or attrs.get("type")

        for v in values:
            if not isinstance(v, dict):
                continue
            dt = v.get("datetime") or v.get("date") or v.get("x")
            val = v.get("value") or v.get("y")
            pct = v.get("percentage")
            rows.append(
                {
                    "datetime": dt,
                    "technology": str(title).strip(),
                    "group": group,
                    "type": serie_type,
                    "mw": _safe_float(val),
                    "percentage": _safe_float(pct),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["datetime"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["date", "mw"]).copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.to_period("M").astype(str)
    return df


@st.cache_data(show_spinner=False, ttl=60 * 30)
def fetch_reddata_api(start: str, end: str, time_trunc: str) -> FetchResult:
    """
    Try multiple endpoint/parameter combinations. For national data, REData often works
    with no geo params; some widgets accept geo params.
    """
    param_sets = [
        {
            "start_date": start,
            "end_date": end,
            "time_trunc": time_trunc,
        },
        {
            "start_date": start,
            "end_date": end,
            "time_trunc": time_trunc,
            "geo_trunc": "electric_system",
            "geo_limit": "nacional",
            "geo_ids": "8741",
        },
        {
            "start_date": start,
            "end_date": end,
            "time_trunc": time_trunc,
            "geo_trunc": "electric_system",
            "geo_limit": "peninsular",
            "geo_ids": "8741",
        },
    ]

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 streamlit-ree-test/0.1",
    }

    errors: list[str] = []
    for endpoint in API_ENDPOINTS:
        for params in param_sets:
            try:
                r = requests.get(endpoint, params=params, headers=headers, timeout=30)
                if r.status_code != 200:
                    errors.append(f"{endpoint} {params} -> HTTP {r.status_code}: {r.text[:250]}")
                    continue

                raw = r.json()
                df = _extract_series_from_reddata_json(raw)
                if not df.empty:
                    df["api_endpoint"] = endpoint
                    df["api_params"] = json.dumps(params, ensure_ascii=False)
                    return FetchResult(ok=True, source=f"API: {endpoint}", df=df, raw=raw)

                errors.append(f"{endpoint} {params} -> JSON OK pero parser sin filas")
            except Exception as e:
                errors.append(f"{endpoint} {params} -> {type(e).__name__}: {e}")

    return FetchResult(ok=False, source="REData API", df=pd.DataFrame(), error="\n".join(errors))


@st.cache_data(show_spinner=False, ttl=60 * 60)
def scrape_report_2025_table() -> FetchResult:
    """
    Scrape the 2025 report page as a fallback.

    The static HTML/search result exposes the chart table in text. Depending on REE's
    rendering, the table can appear as HTML, JSON in scripts, or plain text. This is
    intentionally defensive.
    """
    try:
        r = requests.get(REPORT_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)

        rows: list[dict[str, Any]] = []

        # Fallback pattern for known 2025 table lines in text, e.g.
        # "Solar fotovoltaica40.95227,27088,641.66026,9"
        # We only need "Nacional MW", the 5th number in many rows. If parsing fails,
        # we at least extract the headline total.
        technologies = [
            "Hidráulica", "Hidroeólica", "Eólica", "Solar fotovoltaica", "Solar térmica",
            "Otras renovables", "Residuos renovables", "Renovables", "Nuclear", "Carbón",
            "Motor diésel", "Turbina de gas", "Turbina de vapor", "Fuel", "Ciclo combinado",
            "Cogeneración", "Residuos no renovables", "No renovables", "Generación",
            "Turbinación bombeo", "Baterías", "Almacenamiento", "Potencia total",
        ]

        for tech in technologies:
            # Capture a chunk after the tech label until next known tech or newline.
            # This is best-effort because REE's chart table is often rendered client-side.
            m = re.search(re.escape(tech) + r"\s*([0-9\.,\-\s]+)", text)
            if not m:
                continue
            nums = re.findall(r"-?\d+(?:[\.,]\d+)?", m.group(1))
            nums_float = [_safe_float(n) for n in nums]
            nums_float = [x for x in nums_float if x is not None]

            # In the report table order is normally:
            # Peninsular MW, %25/24, No peninsular MW, %25/24, Nacional MW, %25/24
            nacional_mw = nums_float[4] if len(nums_float) >= 5 else None
            if nacional_mw is not None:
                rows.append(
                    {
                        "date": pd.Timestamp("2025-12-31"),
                        "year": 2025,
                        "month": "2025-12",
                        "technology": tech,
                        "mw": nacional_mw,
                        "source_detail": "scraped_report_table_best_effort",
                    }
                )

        df = pd.DataFrame(rows)

        # Hard fallback: headline 142.558 MW
        if df.empty or not (df["technology"].eq("Potencia total").any()):
            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        [
                            {
                                "date": pd.Timestamp("2025-12-31"),
                                "year": 2025,
                                "month": "2025-12",
                                "technology": "Potencia total",
                                "mw": float(EXPECTED_2025_TOTAL_SIN_AUTOCONSUMO_MW),
                                "source_detail": "report_headline_142558_mw",
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        return FetchResult(ok=True, source=REPORT_URL, df=df, raw={"text_sample": text[:5000]})
    except Exception as e:
        return FetchResult(ok=False, source=REPORT_URL, df=pd.DataFrame(), error=f"{type(e).__name__}: {e}")


def normalise_installed_capacity(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """
    Clean names and keep likely installed capacity rows.

    We do NOT subtract autoconsumo here; instead we fetch the "generación/potencia-instalada"
    widget and validate the annual 2025 total against the report's "sin autoconsumo" total.
    If REE changes the widget to include BTM, the validation will show it.
    """
    if df.empty:
        return df

    out = df.copy()
    out["technology_clean"] = (
        out["technology"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # Remove clearly non-technology helper rows if API returns them.
    bad_terms = ["autoconsumo"]  # we want sin autoconsumo; explicit rows are excluded
    mask_bad = out["technology_clean"].str.lower().apply(lambda x: any(t in x for t in bad_terms))
    out = out.loc[~mask_bad].copy()

    # Normalize periods.
    if granularity == "year":
        out["period"] = out["year"].astype(str)
    else:
        out["period"] = out["month"]

    return out.sort_values(["date", "technology_clean"])


def split_total_and_tech(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, df
    total_mask = df["technology_clean"].str.lower().isin(
        ["potencia total", "total", "generación", "generacion"]
    )
    totals = df.loc[total_mask].copy()
    tech = df.loc[~total_mask].copy()
    return totals, tech


def validate_2025_against_report(df_annual: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if df_annual.empty:
        return pd.DataFrame(rows)

    # Look for a 2025 total. Prefer "Potencia total", else sum technologies if no total.
    d2025 = df_annual[df_annual["year"].eq(2025)].copy()
    if d2025.empty:
        return pd.DataFrame(rows)

    total_candidates = d2025[d2025["technology_clean"].str.lower().eq("potencia total")]
    if total_candidates.empty:
        total_candidates = d2025[d2025["technology_clean"].str.lower().isin(["total", "generación", "generacion"])]

    if not total_candidates.empty:
        got = float(total_candidates.sort_values("date").iloc[-1]["mw"])
        rows.append(
            {
                "check": "2025 total sin autoconsumo esperado por informe",
                "expected_mw": EXPECTED_2025_TOTAL_SIN_AUTOCONSUMO_MW,
                "got_mw": got,
                "diff_mw": got - EXPECTED_2025_TOTAL_SIN_AUTOCONSUMO_MW,
                "status": "OK" if abs(got - EXPECTED_2025_TOTAL_SIN_AUTOCONSUMO_MW) < 50 else "REVISAR",
            }
        )

    # Check PV too if available: report gives 41,660 MW national solar fotovoltaica.
    pv = d2025[d2025["technology_clean"].str.lower().eq("solar fotovoltaica")]
    if not pv.empty:
        got_pv = float(pv.sort_values("date").iloc[-1]["mw"])
        rows.append(
            {
                "check": "2025 solar FV informe nacional",
                "expected_mw": 41660,
                "got_mw": got_pv,
                "diff_mw": got_pv - 41660,
                "status": "OK" if abs(got_pv - 41660) < 50 else "REVISAR",
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(page_title="REE Potencia instalada test", layout="wide")
    st.title("Test REE potencia instalada — sin autoconsumo")

    st.markdown(
        """
        Objetivo: traer **potencia instalada anual hasta 2025** desde REE y probar
        **granularidad mensual 2026**. El test valida 2025 contra el informe:
        **142.558 MW sin autoconsumo no visible**.
        """
    )

    with st.sidebar:
        st.header("Parámetros")
        start_year = st.number_input("Año inicio anual", min_value=1990, max_value=2025, value=2020, step=1)
        end_year = st.number_input("Año fin anual", min_value=1990, max_value=2025, value=2025, step=1)
        start_2026 = st.date_input("Inicio mensual 2026", value=date(2026, 1, 1))
        end_2026 = st.date_input("Fin mensual 2026", value=date(2026, 5, 31))
        show_debug = st.checkbox("Mostrar debug/raw", value=True)

    annual_start = f"{int(start_year)}-01-01T00:00"
    annual_end = f"{int(end_year)}-12-31T23:59"
    monthly_start = f"{start_2026:%Y-%m-%d}T00:00"
    monthly_end = f"{end_2026:%Y-%m-%d}T23:59"

    st.subheader("1) Anual hasta 2025")
    with st.spinner("Consultando REData API anual..."):
        annual_res = fetch_reddata_api(annual_start, annual_end, "year")

    if annual_res.ok:
        annual = normalise_installed_capacity(annual_res.df, "year")
        st.success(f"Datos anuales obtenidos desde {annual_res.source}")
    else:
        st.warning("API anual no devolvió datos parseables. Probando scrape del informe 2025...")
        fallback = scrape_report_2025_table()
        annual = normalise_installed_capacity(fallback.df, "year") if fallback.ok else pd.DataFrame()
        if fallback.ok:
            st.info("Usando fallback scrape del informe 2025.")
        else:
            st.error(f"Fallback falló: {fallback.error}")

    if not annual.empty:
        st.dataframe(annual, use_container_width=True)

        checks = validate_2025_against_report(annual)
        if not checks.empty:
            st.subheader("Validación contra informe 2025")
            st.dataframe(checks, use_container_width=True)
            if checks["status"].eq("REVISAR").any():
                st.error("La serie no cuadra con el informe sin autoconsumo. Puede estar incluyendo BTM/autoconsumo o el parser tomó una fila incorrecta.")
            else:
                st.success("La serie cuadra razonablemente con el informe sin autoconsumo.")

        totals, tech = split_total_and_tech(annual)
        chart_df = tech if not tech.empty else annual
        fig = px.line(
            chart_df,
            x="period",
            y="mw",
            color="technology_clean",
            markers=True,
            title="Potencia instalada anual por tecnología / bloque",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("No se han podido obtener datos anuales.")

    st.subheader("2) Mensual 2026")
    with st.spinner("Consultando REData API mensual 2026..."):
        monthly_res = fetch_reddata_api(monthly_start, monthly_end, "month")

    if monthly_res.ok:
        monthly = normalise_installed_capacity(monthly_res.df, "month")
        st.success(f"Datos mensuales 2026 obtenidos desde {monthly_res.source}")
        st.dataframe(monthly, use_container_width=True)

        totals_m, tech_m = split_total_and_tech(monthly)
        chart_m = tech_m if not tech_m.empty else monthly
        fig_m = px.line(
            chart_m,
            x="period",
            y="mw",
            color="technology_clean",
            markers=True,
            title="Potencia instalada mensual 2026",
        )
        st.plotly_chart(fig_m, use_container_width=True)

        if not totals_m.empty:
            st.metric(
                "Última potencia total mensual encontrada",
                f"{totals_m.sort_values('date').iloc[-1]['mw']:,.0f} MW".replace(",", "."),
                help="Total tal como lo devuelve REE; revisar validación si sospechas inclusión de autoconsumo.",
            )
    else:
        st.error("No se han podido obtener datos mensuales 2026 desde la API.")
        st.code(monthly_res.error or "", language="text")

    if show_debug:
        st.subheader("Debug")
        st.markdown("**API endpoints probados**")
        st.code("\n".join(API_ENDPOINTS), language="text")

        if annual_res.error:
            st.markdown("**Errores anual API**")
            st.code(annual_res.error, language="text")

        if monthly_res.error:
            st.markdown("**Errores mensual API**")
            st.code(monthly_res.error, language="text")

        if annual_res.ok and annual_res.raw is not None:
            st.markdown("**Raw anual sample**")
            st.json(annual_res.raw, expanded=False)

        if monthly_res.ok and monthly_res.raw is not None:
            st.markdown("**Raw mensual sample**")
            st.json(monthly_res.raw, expanded=False)


if __name__ == "__main__":
    main()
