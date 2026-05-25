"""
Streamlit test REE: potencia instalada 2026 SIN autoconsumo — v5.

Run:
    pip install streamlit pandas requests plotly openpyxl
    streamlit run ree_potencia_instalada_2026_v5.py

Idea:
- La API pública del widget de potencia instalada devuelve 500/400.
- Para 2026, REE sí publica los Excel mensuales de boletines.
- Este script descarga:
    02-produccion-{mes}-2026.xlsx
    03-sistemas-no-peninsulares-{mes}-2026.xlsx
- Extrae la hoja Data 6 / Dat_02:
    bloque autoconsumo
    bloque potencia total
- Calcula:
    sin_autoconsumo = total_publicado - autoconsumo
- Devuelve:
    Península
    Baleares
    Canarias
    Nacional = Península + Baleares + Canarias

Limitación:
- Ceuta/Melilla aparecen en producción/energía, pero no en los bloques históricos de potencia
  mensual Dat_02 usados para Baleares/Canarias. Su impacto es pequeño, pero si necesitas
  "nacional estricto REE", habría que incorporar Ceuta/Melilla por otra fuente.
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

TECH_FIX = {
    "Solar Fotovoltaica": "Solar fotovoltaica",
    "Solar Térmica": "Solar térmica",
    "Otras Renovables": "Otras renovables",
    "Residuos Renovables": "Residuos renovables",
    "Residuos no Renovables": "Residuos no renovables",
    "Ciclo Combinado": "Ciclo combinado",
    "Turbina de Gas": "Turbina de gas",
    "Turbina de Vapor": "Turbina de vapor",
    "Motor diésel": "Motor diésel",
    "Motores diesel": "Motor diésel",
    "Motores diésel": "Motor diésel",
    "Total": "Total",
}

PROD_URL_TEMPLATE = BASE + "/02-produccion-{month}-{year}.xlsx"
SNP_URL_TEMPLATE = BASE + "/03-sistemas-no-peninsulares-{month}-{year}.xlsx"


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
    '2026 Febrero' -> Timestamp('2026-02-28') aprox month end.
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
    system: str,
    block_type: str,
    source_url: str,
) -> pd.DataFrame:
    """
    Extrae un bloque tipo:
        fila header_row: Mes | 2025 Febrero | ... | 2026 Febrero
        filas data: tecnología | MW...
    Índices 0-based.
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

            # Evita columnas auxiliares donde aparece un porcentaje junto al último mes.
            # Para MW totales, valores absurdamente pequeños en Total pueden ser porcentajes.
            if tech == "Total" and mw < 100:
                continue

            rows.append(
                {
                    "system": system,
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
    Hoja Data 6 del Excel de Producción:
    - filas 6-17: autoconsumo Península
    - filas 21-38: total Península incluyendo autoconsumo
    Ojo: pandas 0-based => fila Excel 6 es índice 5.
    """
    df = read_xlsx(content, "Data 6")

    auto = extract_block(
        df,
        header_row=5,       # Excel row 6
        first_data_row=7,   # Excel row 8
        last_data_row=16,   # Excel row 17
        system="Península",
        block_type="autoconsumo",
        source_url=source_url,
    )

    total = extract_block(
        df,
        header_row=20,      # Excel row 21
        first_data_row=22,  # Excel row 23
        last_data_row=37,   # Excel row 38
        system="Península",
        block_type="total_incl_autoconsumo",
        source_url=source_url,
    )

    return pd.concat([auto, total], ignore_index=True)


def parse_snp_excel(content: bytes, source_url: str) -> pd.DataFrame:
    """
    Hoja Dat_02 del Excel SNP:
    Baleares:
      - filas 6-13 auto
      - filas 17-31 total
    Canarias:
      - filas 39-46 auto
      - filas 50-64 total
    Todo en índices 0-based.
    """
    df = read_xlsx(content, "Dat_02")

    blocks = [
        # Baleares autoconsumo
        dict(header_row=5, first_data_row=7, last_data_row=12, system="Baleares", block_type="autoconsumo"),
        # Baleares total incluyendo autoconsumo
        dict(header_row=16, first_data_row=18, last_data_row=30, system="Baleares", block_type="total_incl_autoconsumo"),
        # Canarias autoconsumo
        dict(header_row=38, first_data_row=40, last_data_row=45, system="Canarias", block_type="autoconsumo"),
        # Canarias total incluyendo autoconsumo
        dict(header_row=49, first_data_row=51, last_data_row=63, system="Canarias", block_type="total_incl_autoconsumo"),
    ]

    out = []
    for b in blocks:
        out.append(extract_block(df, source_url=source_url, **b))

    return pd.concat(out, ignore_index=True)


def compute_without_autoconsumo(raw: pd.DataFrame) -> pd.DataFrame:
    """
    total_incl_autoconsumo - autoconsumo por sistema/mes/tecnología.
    Si una tecnología no existe en autoconsumo, autoconsumo = 0.
    """
    if raw.empty:
        return raw

    total = raw[raw["block"].eq("total_incl_autoconsumo")].copy()
    auto = raw[raw["block"].eq("autoconsumo")].copy()

    keys = ["system", "date", "period", "technology"]

    total = total.rename(columns={"mw": "mw_total_incl_autoconsumo"})
    auto = auto.rename(columns={"mw": "mw_autoconsumo"})

    merged = total[keys + ["mw_total_incl_autoconsumo", "source_url"]].merge(
        auto[keys + ["mw_autoconsumo"]],
        on=keys,
        how="left",
    )

    merged["mw_autoconsumo"] = merged["mw_autoconsumo"].fillna(0.0)
    merged["mw_sin_autoconsumo"] = (
        merged["mw_total_incl_autoconsumo"] - merged["mw_autoconsumo"]
    )

    # Evita -0.000000 por redondeos.
    merged.loc[merged["mw_sin_autoconsumo"].abs() < 1e-9, "mw_sin_autoconsumo"] = 0.0

    return merged.sort_values(["date", "system", "technology"]).reset_index(drop=True)


def aggregate_national(clean: pd.DataFrame) -> pd.DataFrame:
    """
    Suma Península + Baleares + Canarias.
    """
    if clean.empty:
        return clean

    value_cols = ["mw_total_incl_autoconsumo", "mw_autoconsumo", "mw_sin_autoconsumo"]
    nat = (
        clean.groupby(["date", "period", "technology"], as_index=False)[value_cols]
        .sum()
        .assign(system="Nacional parcial")
    )
    nat["source_url"] = "Suma Península + Baleares + Canarias desde boletines mensuales REE"

    cols = ["system", "date", "period", "technology"] + value_cols + ["source_url"]
    return nat[cols].sort_values(["date", "technology"]).reset_index(drop=True)


def get_month_data(year: int, month: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Descarga producción y SNP para un mes/año y devuelve:
    raw_blocks, clean_without_autoconsumo, errores
    """
    month_slug = MONTHS_ES[month]

    prod_url = PROD_URL_TEMPLATE.format(month=month_slug, year=year)
    snp_url = SNP_URL_TEMPLATE.format(month=month_slug, year=year)

    errors: list[str] = []
    raw_parts: list[pd.DataFrame] = []

    prod = download_excel(prod_url)
    if prod.ok and prod.content:
        try:
            raw_parts.append(parse_peninsula_production_excel(prod.content, prod.url))
        except Exception as exc:
            errors.append(f"Producción {month_slug} {year}: parse error {type(exc).__name__}: {exc}")
    else:
        errors.append(f"Producción {month_slug} {year}: {prod.error}")

    snp = download_excel(snp_url)
    if snp.ok and snp.content:
        try:
            raw_parts.append(parse_snp_excel(snp.content, snp.url))
        except Exception as exc:
            errors.append(f"SNP {month_slug} {year}: parse error {type(exc).__name__}: {exc}")
    else:
        errors.append(f"SNP {month_slug} {year}: {snp.error}")

    if not raw_parts:
        return pd.DataFrame(), pd.DataFrame(), errors

    raw = pd.concat(raw_parts, ignore_index=True)
    clean = compute_without_autoconsumo(raw)
    nat = aggregate_national(clean)
    clean_all = pd.concat([clean, nat], ignore_index=True)

    # Nos quedamos solo con meses del año solicitado.
    clean_all = clean_all[clean_all["date"].dt.year.eq(year)].copy()
    raw = raw[raw["date"].dt.year.eq(year)].copy()

    return raw, clean_all, errors


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

    # Quitar duplicados: cada boletín trae histórico desde 2025; nos quedamos con el último fichero
    # que contiene cada periodo/tecnología/sistema.
    if not clean_df.empty:
        clean_df = clean_df.sort_values(["date", "source_url"]).drop_duplicates(
            subset=["system", "period", "technology"],
            keep="last",
        )
        clean_df = clean_df.sort_values(["date", "system", "technology"]).reset_index(drop=True)

    return raw_df, clean_df, errors


def main() -> None:
    st.set_page_config(page_title="REE potencia instalada 2026 v5", layout="wide")
    st.title("REE potencia instalada 2026 — sin autoconsumo v5")

    st.markdown(
        """
        Esta versión usa los **Excel mensuales de boletines REE** en vez del endpoint API que devuelve 500/400.
        Calcula **sin autoconsumo** como:

        `potencia total publicada - potencia autoconsumo`

        Fuente base:
        - `02-produccion-{mes}-2026.xlsx` para Península.
        - `03-sistemas-no-peninsulares-{mes}-2026.xlsx` para Baleares y Canarias.
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
        system_filter = st.multiselect(
            "Sistema",
            ["Nacional parcial", "Península", "Baleares", "Canarias"],
            default=["Nacional parcial", "Península"],
        )
        show_raw = st.checkbox("Mostrar bloques raw", value=False)
        show_errors = st.checkbox("Mostrar errores/URLs no disponibles", value=True)

    with st.spinner("Descargando y parseando boletines REE..."):
        raw_df, clean_df, errors = get_2026_until(int(month_to_try))

    if clean_df.empty:
        st.error("No se ha podido extraer ningún dato mensual 2026 desde los Excel.")
        if errors:
            st.code("\n".join(errors), language="text")
        return

    st.success("Datos 2026 extraídos desde boletines mensuales REE.")

    filtered = clean_df[clean_df["system"].isin(system_filter)].copy()

    st.subheader("Potencia instalada sin autoconsumo")
    st.dataframe(filtered, use_container_width=True)

    latest_period = filtered["period"].max()
    latest = filtered[(filtered["period"].eq(latest_period)) & (filtered["technology"].eq("Total"))].copy()

    if not latest.empty:
        st.subheader(f"Último total disponible: {latest_period}")
        cols = st.columns(len(latest))
        for col, (_, row) in zip(cols, latest.iterrows()):
            col.metric(
                row["system"],
                f"{row['mw_sin_autoconsumo']:,.0f} MW".replace(",", "."),
                help=(
                    f"Total incl. autoconsumo: {row['mw_total_incl_autoconsumo']:,.0f} MW; "
                    f"Autoconsumo: {row['mw_autoconsumo']:,.0f} MW"
                ).replace(",", "."),
            )

    chart_df = filtered[filtered["technology"].ne("Total")].copy()
    fig = px.line(
        chart_df,
        x="period",
        y="mw_sin_autoconsumo",
        color="technology",
        facet_row="system" if len(system_filter) > 1 else None,
        markers=True,
        title="Potencia instalada 2026 sin autoconsumo por tecnología",
    )
    fig.update_layout(yaxis_title="MW sin autoconsumo", xaxis_title="Mes")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Comparativa total: incl. autoconsumo vs sin autoconsumo")
    comp = filtered[filtered["technology"].eq("Total")].copy()
    comp_long = comp.melt(
        id_vars=["system", "period", "technology"],
        value_vars=["mw_total_incl_autoconsumo", "mw_autoconsumo", "mw_sin_autoconsumo"],
        var_name="serie",
        value_name="mw",
    )
    fig2 = px.line(
        comp_long,
        x="period",
        y="mw",
        color="serie",
        line_dash="system",
        markers=True,
        title="Total instalado: total publicado, autoconsumo y sin autoconsumo",
    )
    fig2.update_layout(yaxis_title="MW", xaxis_title="Mes")
    st.plotly_chart(fig2, use_container_width=True)

    st.download_button(
        "Descargar CSV limpio",
        data=clean_df.to_csv(index=False).encode("utf-8"),
        file_name="ree_potencia_instalada_2026_sin_autoconsumo_v5.csv",
        mime="text/csv",
    )

    if show_raw:
        st.subheader("Raw blocks")
        st.dataframe(raw_df, use_container_width=True)

    if show_errors and errors:
        st.subheader("Errores / meses no publicados todavía")
        st.code("\n".join(errors), language="text")

    st.caption(
        "Nota: 'Nacional parcial' suma Península + Baleares + Canarias. "
        "Ceuta/Melilla no están incorporadas en este cálculo desde estos bloques históricos."
    )


if __name__ == "__main__":
    main()
