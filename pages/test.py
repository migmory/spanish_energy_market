"""
Streamlit: REE potencia instalada 2026 — Península, Solar FV, sin autoconsumo.

Run:
    pip install streamlit pandas requests plotly openpyxl
    streamlit run ree_potencia_instalada_2026_peninsular_solar_v6.py

Qué hace:
- Descarga los Excel mensuales de REE de Producción:
    02-produccion-{mes}-2026.xlsx
- Lee la hoja "Data 6".
- Extrae dos bloques de potencia instalada peninsular:
    1) autoconsumo
    2) total incluyendo autoconsumo
- Calcula:
    sin_autoconsumo = total_incl_autoconsumo - autoconsumo
- Filtra SOLO "Solar fotovoltaica".
- Grafica la evolución mensual 2026 para Península.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


BASE = "https://www.ree.es/sites/default/files/11_PUBLICACIONES/Documentos"

MONTHS_ES = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
}

MONTH_NAME_TO_NUM = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

PROD_URL_TEMPLATE = BASE + "/02-produccion-{month}-{year}.xlsx"

TECH_FIX = {
    "Solar Fotovoltaica": "Solar fotovoltaica",
    "Solar fotovoltaica": "Solar fotovoltaica",
    "Solar Térmica": "Solar térmica",
    "Otras Renovables": "Otras renovables",
    "Residuos Renovables": "Residuos renovables",
    "Residuos no Renovables": "Residuos no renovables",
    "Ciclo Combinado": "Ciclo combinado",
    "Turbina de Gas": "Turbina de gas",
    "Turbina de Vapor": "Turbina de vapor",
    "Motores diesel": "Motor diésel",
    "Motores diésel": "Motor diésel",
    "Motor diésel": "Motor diésel",
    "Total": "Total",
}


@dataclass
class DownloadResult:
    ok: bool
    url: str
    content: bytes | None = None
    error: str | None = None


def normalize_tech(x: Any) -> str:
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return TECH_FIX.get(s, s)


def parse_month_label(label: Any) -> pd.Timestamp | None:
    """
    Convierte etiquetas tipo '2026 Febrero' a Timestamp fin de mes.
    """
    if not isinstance(label, str):
        return None

    m = re.match(r"^\s*(\d{4})\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)\s*$", label)
    if not m:
        return None

    year = int(m.group(1))
    month_name = m.group(2).strip().lower()
    month_name = (
        month_name.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
    )
    month = MONTH_NAME_TO_NUM.get(month_name)
    if not month:
        return None

    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)


def download_excel(url: str) -> DownloadResult:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=45)
        if r.status_code != 200:
            return DownloadResult(False, url, error=f"HTTP {r.status_code}: {r.text[:250]}")
        return DownloadResult(True, url, content=r.content)
    except Exception as exc:
        return DownloadResult(False, url, error=f"{type(exc).__name__}: {exc}")


def read_xlsx(content: bytes, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=None, engine="openpyxl")


def extract_block(
    df: pd.DataFrame,
    header_row: int,
    first_data_row: int,
    last_data_row: int,
    block_type: str,
    source_url: str,
) -> pd.DataFrame:
    """
    Extrae un bloque de la hoja Data 6:
    - fila header_row: columnas tipo '2026 Febrero'
    - filas data: tecnologías
    """
    month_cols: list[tuple[int, pd.Timestamp]] = []

    for col in range(1, df.shape[1]):
        dt = parse_month_label(df.iat[header_row, col])
        if dt is not None:
            month_cols.append((col, dt))

    rows: list[dict[str, Any]] = []

    for r in range(first_data_row, last_data_row + 1):
        tech_raw = df.iat[r, 0]
        if pd.isna(tech_raw):
            continue

        tech = normalize_tech(tech_raw)

        for col, dt in month_cols:
            val = df.iat[r, col]
            if pd.isna(val):
                continue

            try:
                mw = float(val)
            except Exception:
                continue

            # Evita columnas auxiliares/porcentajes.
            if tech == "Total" and mw < 100:
                continue

            rows.append(
                {
                    "system": "Península",
                    "date": dt,
                    "period": dt.strftime("%Y-%m"),
                    "technology": tech,
                    "block": block_type,
                    "mw": mw,
                    "source_url": source_url,
                }
            )

    return pd.DataFrame(rows)


def parse_peninsula_production_excel(content: bytes, source_url: str) -> pd.DataFrame:
    """
    Hoja Data 6:
    - bloque autoconsumo: encabezado fila Excel 6, datos filas 8-17
    - bloque total incl. autoconsumo: encabezado fila Excel 21, datos filas 23-38
    En pandas (0-based):
    - header_row=5, first_data_row=7, last_data_row=16
    - header_row=20, first_data_row=22, last_data_row=37
    """
    df = read_xlsx(content, "Data 6")

    auto = extract_block(
        df=df,
        header_row=5,
        first_data_row=7,
        last_data_row=16,
        block_type="autoconsumo",
        source_url=source_url,
    )

    total = extract_block(
        df=df,
        header_row=20,
        first_data_row=22,
        last_data_row=37,
        block_type="total_incl_autoconsumo",
        source_url=source_url,
    )

    out = pd.concat([auto, total], ignore_index=True)
    out = out[out["system"].eq("Península")].copy()
    return out


def compute_solar_without_autoconsumo(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Solar FV peninsular sin autoconsumo:
    total_incl_autoconsumo - autoconsumo
    """
    if raw.empty:
        return raw

    solar = raw[raw["technology"].eq("Solar fotovoltaica")].copy()

    total = solar[solar["block"].eq("total_incl_autoconsumo")].copy()
    auto = solar[solar["block"].eq("autoconsumo")].copy()

    total = total.rename(columns={"mw": "mw_total_incl_autoconsumo"})
    auto = auto.rename(columns={"mw": "mw_autoconsumo"})

    keys = ["system", "date", "period", "technology"]

    merged = total[keys + ["mw_total_incl_autoconsumo", "source_url"]].merge(
        auto[keys + ["mw_autoconsumo"]],
        on=keys,
        how="left",
    )

    merged["mw_autoconsumo"] = merged["mw_autoconsumo"].fillna(0.0)
    merged["mw_sin_autoconsumo"] = (
        merged["mw_total_incl_autoconsumo"] - merged["mw_autoconsumo"]
    )

    merged.loc[merged["mw_sin_autoconsumo"].abs() < 1e-9, "mw_sin_autoconsumo"] = 0.0

    merged = merged.sort_values(["date"]).reset_index(drop=True)
    return merged


def get_month_data(year: int, month: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Descarga un boletín mensual de Producción y devuelve:
    - raw peninsular
    - solar pv sin autoconsumo
    - errores
    """
    month_slug = MONTHS_ES[month]
    url = PROD_URL_TEMPLATE.format(month=month_slug, year=year)

    errors: list[str] = []

    dl = download_excel(url)
    if not dl.ok or not dl.content:
        errors.append(f"{month_slug} {year}: {dl.error}")
        return pd.DataFrame(), pd.DataFrame(), errors

    try:
        raw = parse_peninsula_production_excel(dl.content, dl.url)
        clean = compute_solar_without_autoconsumo(raw)
        raw = raw[raw["date"].dt.year.eq(year)].copy()
        clean = clean[clean["date"].dt.year.eq(year)].copy()
        return raw, clean, errors
    except Exception as exc:
        errors.append(f"{month_slug} {year}: parse error {type(exc).__name__}: {exc}")
        return pd.DataFrame(), pd.DataFrame(), errors


def get_2026_until(month_to_try: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    raw_all = []
    clean_all = []
    errors = []

    for m in range(1, month_to_try + 1):
        raw, clean, err = get_month_data(2026, m)
        if not raw.empty:
            raw_all.append(raw)
        if not clean.empty:
            clean_all.append(clean)
        errors.extend(err)

    raw_df = pd.concat(raw_all, ignore_index=True) if raw_all else pd.DataFrame()
    clean_df = pd.concat(clean_all, ignore_index=True) if clean_all else pd.DataFrame()

    # Cada boletín trae histórico; nos quedamos con el último fichero para cada mes.
    if not clean_df.empty:
        clean_df = clean_df.sort_values(["date", "source_url"]).drop_duplicates(
            subset=["system", "period", "technology"],
            keep="last",
        )
        clean_df = clean_df.sort_values(["date"]).reset_index(drop=True)

    return raw_df, clean_df, errors


def main() -> None:
    st.set_page_config(page_title="REE Península Solar FV 2026", layout="wide")
    st.title("REE potencia instalada 2026 — Península, Solar FV")

    st.markdown(
        """
        Esta app usa los **Excel mensuales de Producción** de REE y se queda **solo con Península** y
        **solo con Solar fotovoltaica**.

        Cálculo:
        - **Solar FV total publicada**
        - **Solar FV autoconsumo**
        - **Solar FV sin autoconsumo = total publicada - autoconsumo**
        """
    )

    with st.sidebar:
        st.header("Parámetros")
        month_to_try = st.selectbox(
            "Hasta qué mes de 2026 intentar descargar",
            options=list(MONTHS_ES.keys()),
            index=min(date.today().month, 12) - 1,
            format_func=lambda m: MONTHS_ES[m].capitalize(),
        )
        show_raw = st.checkbox("Mostrar bloques raw", value=False)
        show_errors = st.checkbox("Mostrar errores / meses no publicados", value=True)

    with st.spinner("Descargando y parseando boletines mensuales REE..."):
        raw_df, clean_df, errors = get_2026_until(int(month_to_try))

    if clean_df.empty:
        st.error("No se ha podido extraer Solar FV peninsular 2026.")
        if errors:
            st.code("\n".join(errors), language="text")
        return

    st.success("Datos extraídos correctamente para Península / Solar fotovoltaica.")

    st.subheader("Tabla limpia")
    st.dataframe(clean_df, use_container_width=True)

    latest = clean_df.sort_values("date").iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Solar FV total publicada",
        f"{latest['mw_total_incl_autoconsumo']:,.0f} MW".replace(",", "."),
        latest["period"],
    )
    c2.metric(
        "Solar FV autoconsumo",
        f"{latest['mw_autoconsumo']:,.0f} MW".replace(",", "."),
        latest["period"],
    )
    c3.metric(
        "Solar FV sin autoconsumo",
        f"{latest['mw_sin_autoconsumo']:,.0f} MW".replace(",", "."),
        latest["period"],
    )

    # Gráfico principal: solar FV peninsular.
    chart_long = clean_df.melt(
        id_vars=["period", "date", "system", "technology"],
        value_vars=[
            "mw_total_incl_autoconsumo",
            "mw_autoconsumo",
            "mw_sin_autoconsumo",
        ],
        var_name="serie",
        value_name="mw",
    )

    chart_long["serie"] = chart_long["serie"].map(
        {
            "mw_total_incl_autoconsumo": "Solar FV total publicada",
            "mw_autoconsumo": "Solar FV autoconsumo",
            "mw_sin_autoconsumo": "Solar FV sin autoconsumo",
        }
    )

    fig = px.line(
        chart_long,
        x="period",
        y="mw",
        color="serie",
        markers=True,
        title="Península — Solar fotovoltaica instalada 2026",
    )
    fig.update_layout(yaxis_title="MW", xaxis_title="Mes")
    st.plotly_chart(fig, use_container_width=True)

    # Gráfico opcional: solo la serie sin autoconsumo.
    st.subheader("Serie solo sin autoconsumo")
    fig2 = px.line(
        clean_df,
        x="period",
        y="mw_sin_autoconsumo",
        markers=True,
        title="Península — Solar fotovoltaica sin autoconsumo 2026",
    )
    fig2.update_layout(yaxis_title="MW sin autoconsumo", xaxis_title="Mes")
    st.plotly_chart(fig2, use_container_width=True)

    st.download_button(
        "Descargar CSV",
        data=clean_df.to_csv(index=False).encode("utf-8"),
        file_name="ree_peninsula_solar_fv_2026_sin_autoconsumo_v6.csv",
        mime="text/csv",
    )

    if show_raw:
        st.subheader("Raw blocks — Península")
        st.dataframe(raw_df[raw_df["technology"].eq("Solar fotovoltaica")], use_container_width=True)

    if show_errors and errors:
        st.subheader("Errores / meses no publicados")
        st.code("\n".join(errors), language="text")


if __name__ == "__main__":
    main()
