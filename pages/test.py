"""
Streamlit test REE: potencia instalada SIN autoconsumo.

Run:
    pip install streamlit pandas requests plotly
    streamlit run ree_potencia_instalada_test_v2.py

Cambios v2:
- Sin BeautifulSoup.
- Sin @st.cache_data, para evitar UnserializableReturnValueError.
- Valida 2025 contra 142.558 MW total y 41.660 MW solar FV del informe REE.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


REPORT_2025_TOTAL_SIN_AUTOCONSUMO_MW = 142_558
REPORT_2025_SOLAR_FV_SIN_AUTOCONSUMO_MW = 41_660

API_ENDPOINTS = [
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


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    txt = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not txt:
        return None

    if "." in txt and "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    elif "," in txt:
        txt = txt.replace(",", ".")

    try:
        return float(txt)
    except ValueError:
        return None


def walk_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


def extract_reddata_series(raw: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for node in walk_dicts(raw):
        attrs = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
        values = node.get("values") or attrs.get("values")

        if not isinstance(values, list) or not values:
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

        group_id = node.get("groupId") or node.get("group_id") or attrs.get("groupId")
        serie_type = node.get("type") or attrs.get("type")

        for point in values:
            if not isinstance(point, dict):
                continue

            dt = point.get("datetime") or point.get("date")
            mw = safe_float(point.get("value"))

            if dt is None or mw is None:
                continue

            rows.append(
                {
                    "datetime": dt,
                    "technology": str(title).strip(),
                    "group_id": group_id,
                    "serie_type": serie_type,
                    "mw": mw,
                    "percentage": safe_float(point.get("percentage")),
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["datetime"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["date", "mw"]).copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.to_period("M").astype(str)

    return df.sort_values(["date", "technology"]).reset_index(drop=True)


def fetch_reddata_api(start: str, end: str, time_trunc: str) -> FetchResult:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 streamlit-ree-test/0.2",
    }

    param_sets = [
        {"start_date": start, "end_date": end, "time_trunc": time_trunc},
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

    errors: list[str] = []

    for endpoint in API_ENDPOINTS:
        for params in param_sets:
            try:
                response = requests.get(endpoint, params=params, headers=headers, timeout=30)

                if response.status_code != 200:
                    errors.append(
                        f"{endpoint} | {params} | HTTP {response.status_code}: "
                        f"{response.text[:300]}"
                    )
                    continue

                raw = response.json()
                df = extract_reddata_series(raw)

                if not df.empty:
                    df["api_endpoint"] = endpoint
                    df["api_params"] = json.dumps(params, ensure_ascii=False)
                    return FetchResult(
                        ok=True,
                        source=f"{endpoint} | params={params}",
                        df=df,
                        raw=raw,
                    )

                errors.append(
                    f"{endpoint} | {params} | JSON recibido, pero no hay series parseables."
                )

            except Exception as exc:
                errors.append(f"{endpoint} | {params} | {type(exc).__name__}: {exc}")

    return FetchResult(
        ok=False,
        source="REData API",
        df=pd.DataFrame(),
        raw=None,
        error="\n".join(errors),
    )


def clean_capacity_df(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["technology_clean"] = (
        out["technology"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    )

    # Queremos SIN autoconsumo. Si aparece una fila explícita de autoconsumo, se elimina.
    # Si REE mete BTM dentro de Solar FV sin etiquetarlo, esta regla no puede restarlo.
    out = out[
        ~out["technology_clean"].str.lower().str.contains("autoconsumo", na=False)
    ].copy()

    out["period"] = out["year"].astype(str) if granularity == "year" else out["month"]

    return out.sort_values(["date", "technology_clean"]).reset_index(drop=True)


def find_total_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    total_names = ["potencia total", "total", "generación", "generacion"]

    return df[df["technology_clean"].str.lower().isin(total_names)].copy()


def validation_2025(df: pd.DataFrame) -> pd.DataFrame:
    checks: list[dict[str, Any]] = []

    if df.empty:
        return pd.DataFrame(checks)

    d2025 = df[df["year"] == 2025].copy()

    if d2025.empty:
        return pd.DataFrame(checks)

    totals = find_total_rows(d2025)

    if not totals.empty:
        got = float(totals.sort_values("date").iloc[-1]["mw"])
        checks.append(
            {
                "check": "Total 2025 sin autoconsumo según informe",
                "expected_mw": REPORT_2025_TOTAL_SIN_AUTOCONSUMO_MW,
                "got_mw": got,
                "diff_mw": got - REPORT_2025_TOTAL_SIN_AUTOCONSUMO_MW,
                "status": "OK" if abs(got - REPORT_2025_TOTAL_SIN_AUTOCONSUMO_MW) < 50 else "REVISAR",
            }
        )

    fv = d2025[d2025["technology_clean"].str.lower() == "solar fotovoltaica"]

    if not fv.empty:
        got_fv = float(fv.sort_values("date").iloc[-1]["mw"])
        checks.append(
            {
                "check": "Solar FV 2025 sin autoconsumo según informe",
                "expected_mw": REPORT_2025_SOLAR_FV_SIN_AUTOCONSUMO_MW,
                "got_mw": got_fv,
                "diff_mw": got_fv - REPORT_2025_SOLAR_FV_SIN_AUTOCONSUMO_MW,
                "status": "OK" if abs(got_fv - REPORT_2025_SOLAR_FV_SIN_AUTOCONSUMO_MW) < 50 else "REVISAR",
            }
        )

    return pd.DataFrame(checks)


def plot_capacity(df: pd.DataFrame, title: str):
    if df.empty:
        return None

    totals = find_total_rows(df)

    if len(totals) < len(df):
        chart_df = df.drop(index=totals.index, errors="ignore").copy()
    else:
        chart_df = df.copy()

    fig = px.line(
        chart_df,
        x="period",
        y="mw",
        color="technology_clean",
        markers=True,
        title=title,
    )
    fig.update_layout(yaxis_title="MW", xaxis_title="Periodo", legend_title="Tecnología")
    return fig


def main() -> None:
    st.set_page_config(page_title="REE potencia instalada test", layout="wide")
    st.title("Test REE potencia instalada — sin autoconsumo")

    st.markdown(
        """
        Objetivo: traer **potencia instalada anual hasta 2025** desde REE y probar
        **granularidad mensual 2026**.

        Validación:
        - **142.558 MW** = potencia instalada 2025 del sistema eléctrico español sin autoconsumo no visible.
        - **41.660 MW** = solar fotovoltaica 2025 del informe sin autoconsumo no visible.
        """
    )

    with st.sidebar:
        st.header("Parámetros")

        start_year = st.number_input(
            "Año inicio anual", min_value=1990, max_value=2025, value=2020, step=1
        )
        end_year = st.number_input(
            "Año fin anual", min_value=1990, max_value=2025, value=2025, step=1
        )
        start_2026 = st.date_input("Inicio mensual 2026", value=date(2026, 1, 1))
        end_2026 = st.date_input("Fin mensual 2026", value=date(2026, 12, 31))
        show_debug = st.checkbox("Mostrar debug", value=True)

    annual_start = f"{int(start_year)}-01-01T00:00"
    annual_end = f"{int(end_year)}-12-31T23:59"

    monthly_start = f"{start_2026:%Y-%m-%d}T00:00"
    monthly_end = f"{end_2026:%Y-%m-%d}T23:59"

    st.header("1) Anual hasta 2025")

    annual_res = fetch_reddata_api(start=annual_start, end=annual_end, time_trunc="year")

    if annual_res.ok:
        st.success(f"API anual OK: {annual_res.source}")
        annual_df = clean_capacity_df(annual_res.df, granularity="year")
        st.dataframe(annual_df, use_container_width=True)

        checks = validation_2025(annual_df)

        st.subheader("Validación 2025")
        if checks.empty:
            st.warning(
                "No he encontrado fila total o Solar FV para validar 2025. "
                "Mira el debug/raw para ajustar nombres."
            )
        else:
            st.dataframe(checks, use_container_width=True)
            if checks["status"].eq("REVISAR").any():
                st.error(
                    "La serie NO cuadra con el informe sin autoconsumo. "
                    "Puede estar incluyendo autoconsumo/BTM o el parser cogió una familia incorrecta."
                )
            else:
                st.success("La serie cuadra con el informe sin autoconsumo.")

        fig = plot_capacity(annual_df, "Potencia instalada anual por tecnología")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
    else:
        annual_df = pd.DataFrame()
        st.error("No se han podido obtener datos anuales.")
        st.code(annual_res.error or "Sin detalle de error", language="text")

    st.header("2) Mensual 2026")

    monthly_res = fetch_reddata_api(start=monthly_start, end=monthly_end, time_trunc="month")

    if monthly_res.ok:
        st.success(f"API mensual OK: {monthly_res.source}")
        monthly_df = clean_capacity_df(monthly_res.df, granularity="month")
        st.dataframe(monthly_df, use_container_width=True)

        totals = find_total_rows(monthly_df)
        if not totals.empty:
            latest = totals.sort_values("date").iloc[-1]
            st.metric(
                "Último total mensual encontrado",
                f"{latest['mw']:,.0f} MW".replace(",", "."),
                f"{latest['period']}",
            )

        fig_m = plot_capacity(monthly_df, "Potencia instalada mensual 2026 por tecnología")
        if fig_m is not None:
            st.plotly_chart(fig_m, use_container_width=True)
    else:
        monthly_df = pd.DataFrame()
        st.error("No se han podido obtener datos mensuales 2026.")
        st.code(monthly_res.error or "Sin detalle de error", language="text")

    if show_debug:
        st.header("Debug")

        st.subheader("Endpoints probados")
        st.code("\n".join(API_ENDPOINTS), language="text")

        st.subheader("Resultado anual")
        st.write({"ok": annual_res.ok, "source": annual_res.source, "rows": len(annual_res.df)})
        if annual_res.error:
            st.code(annual_res.error, language="text")
        if annual_res.raw is not None:
            st.json(annual_res.raw, expanded=False)

        st.subheader("Resultado mensual 2026")
        st.write({"ok": monthly_res.ok, "source": monthly_res.source, "rows": len(monthly_res.df)})
        if monthly_res.error:
            st.code(monthly_res.error, language="text")
        if monthly_res.raw is not None:
            st.json(monthly_res.raw, expanded=False)


if __name__ == "__main__":
    main()
