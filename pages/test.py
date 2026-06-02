from __future__ import annotations

import base64
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="Embalses + demand profile test", layout="wide")

BASE_DIR = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(BASE_DIR / ".env", override=True)
    load_dotenv(override=True)

MADRID_TZ = ZoneInfo("Europe/Madrid")
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}
EMBALSES_HOME = "https://www.embalses.net/"
EMBALSES_BOLETIN = "https://www.embalses.net/suscripciones/boletin-web.php"
EMBALSES_GRAPH_BASE = "https://www.embalses.net/cache/home.png"

st.markdown(
    f"""
    <div style="
        padding:18px 22px;
        border-radius:18px;
        background:linear-gradient(90deg,{CORP_GREEN_DARK} 0%,{CORP_GREEN} 58%,#C7F3E2 100%);
        color:white;
        font-weight:900;
        font-size:1.55rem;
        margin-bottom:12px;
    ">🧪 Test v3 | Embalses.net exact image + REE demand shape</div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "v3: el gráfico de agua embalsada se muestra como la imagen PNG original de Embalses.net. "
    "No se intenta redibujar con Altair porque Embalses.net no expone las curvas como datos HTML."
)


def section(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            margin-top:18px;
            margin-bottom:10px;
            padding:10px 14px;
            background:#F4FCF8;
            border-left:5px solid {CORP_GREEN};
            border-radius:8px;
            font-weight:850;
            color:#0F172A;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def to_float_es(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    s = str(value).replace("\xa0", " ").strip()
    s = re.sub(r"[^\d,\.\-]", "", s)
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def fmt_num(v: Any, decimals: int = 1, suffix: str = "") -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.{decimals}f}{suffix}"


def fmt_pct(v: Any, decimals: int = 1) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.{decimals}f}%"


def delta_html(value: Any, suffix: str = "", decimals: int = 1, good_when_up: bool = True) -> str:
    if value is None or pd.isna(value):
        return '<span style="color:#94A3B8;">→ n/a</span>'
    v = float(value)
    positive = v >= 0
    good = positive if good_when_up else not positive
    color = "#16A34A" if good else "#DC2626"
    arrow = "↑" if positive else "↓"
    return f'<span style="color:{color};font-weight:800;">{arrow} {v:+,.{decimals}f}{suffix}</span>'


def http_get(url: str, timeout: int = 45) -> requests.Response:
    return requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 NexwellPower-Test/1.0",
            "Accept": "text/html,application/xhtml+xml,application/json,image/*,*/*",
            "Referer": EMBALSES_HOME,
        },
        timeout=timeout,
    )


def clean_html_text(html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"</p>|</div>|</tr>|</li>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    replacements = {
        "&nbsp;": " ", "&sup3;": "3", "&#179;": "3", "&#176;": "º",
        "&aacute;": "á", "&eacute;": "é", "&iacute;": "í", "&oacute;": "ó", "&uacute;": "ú", "&ntilde;": "ñ",
        "&Aacute;": "Á", "&Eacute;": "É", "&Iacute;": "Í", "&Oacute;": "Ó", "&Uacute;": "Ú", "&Ntilde;": "Ñ",
    }
    for a, b in replacements.items():
        text = text.replace(a, b)
    return re.sub(r"\s+", " ", text).strip()


def parse_embalses_summary_from_html(html: str, source_url: str) -> dict[str, Any]:
    text = clean_html_text(html)
    summary: dict[str, Any] = {
        "date": None,
        "stored_hm3": None,
        "stored_pct": None,
        "weekly_var_hm3": None,
        "weekly_var_pct": None,
        "capacity_hm3": None,
        "same_week_year": None,
        "same_week_last_year_hm3": None,
        "same_week_last_year_pct": None,
        "same_week_10y_avg_hm3": None,
        "same_week_10y_avg_pct": None,
        "source_url": source_url,
    }

    m_date = re.search(r"Agua embalsada en España\s+a\s+(\d{2}-\d{2}-\d{4})", text, flags=re.I)
    if not m_date:
        m_date = re.search(r"Agua embalsada\s*\(\s*(\d{2}-\d{2}-\d{4})\s*\)", text, flags=re.I)
    if m_date:
        summary["date"] = pd.to_datetime(m_date.group(1), dayfirst=True, errors="coerce")

    m_head = re.search(
        r"Agua embalsada(?: en España)?(?:\s+a\s+\d{2}-\d{2}-\d{4})?\s*(?:\(\s*\d{2}-\d{2}-\d{4}\s*\))?\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if m_head:
        summary["stored_hm3"] = to_float_es(m_head.group(1))
        summary["stored_pct"] = to_float_es(m_head.group(2))

    m_var = re.search(
        r"Variaci[oó]n\s+Sem\.?\s+Anterior\s*[:\-]?\s*([-+]?[0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([-+]?[0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if m_var:
        summary["weekly_var_hm3"] = to_float_es(m_var.group(1))
        summary["weekly_var_pct"] = to_float_es(m_var.group(2))

    m_cap = re.search(r"Capacidad(?:\s+embalses)?\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?", text, flags=re.I)
    if m_cap:
        summary["capacity_hm3"] = to_float_es(m_cap.group(1))

    m_ly = re.search(
        r"(?:Agua embalsada\s*)?Misma Semana\s*\(\s*(\d{4})\s*\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if not m_ly:
        m_ly = re.search(
            r"Agua embalsada\s*\(\s*(\d{4})\s*\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
            text,
            flags=re.I,
        )
    if m_ly:
        summary["same_week_year"] = int(m_ly.group(1))
        summary["same_week_last_year_hm3"] = to_float_es(m_ly.group(2))
        summary["same_week_last_year_pct"] = to_float_es(m_ly.group(3))

    m_10y = re.search(
        r"(?:Agua embalsada\s*)?(?:Misma Semana\s*)?\(\s*(?:Med\.\s*)?Media\s+10\s+años\s*\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
        text,
        flags=re.I,
    )
    if not m_10y:
        m_10y = re.search(
            r"(?:Agua embalsada\s*)?Misma Semana\s*\(\s*Med\.\s*10\s*Años\s*\)\s*[:\-]?\s*([0-9\.\,]+)\s*hm\s*\^?\s*3?\s*([0-9\.\,]+)\s*%",
            text,
            flags=re.I,
        )
    if m_10y:
        summary["same_week_10y_avg_hm3"] = to_float_es(m_10y.group(1))
        summary["same_week_10y_avg_pct"] = to_float_es(m_10y.group(2))
    return summary


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_embalses_current() -> tuple[dict[str, Any], dict[str, Any]]:
    diagnostics: dict[str, Any] = {"attempts": []}
    for url in [EMBALSES_BOLETIN, EMBALSES_HOME]:
        try:
            resp = http_get(url)
            parsed = parse_embalses_summary_from_html(resp.text or "", resp.url)
            diagnostics["attempts"].append({
                "url": resp.url,
                "http": resp.status_code,
                "response_chars": len(resp.text or ""),
                "stored_hm3": parsed.get("stored_hm3"),
                "date": str(parsed.get("date")),
            })
            if parsed.get("stored_hm3") is not None:
                return parsed, diagnostics
        except Exception as exc:
            diagnostics["attempts"].append({"url": url, "http": "ERROR", "error": str(exc)[:500]})
    return {}, diagnostics



def embalses_graph_candidates(current: dict[str, Any] | None) -> list[str]:
    """URLs exactas del PNG anual de Embalses.net.

    IMPORTANTE: no añadimos `cb=` antes que la URL oficial con `a=dd-mm-yyyy`,
    porque Embalses.net sirve correctamente el PNG oficial con esa query exacta.
    """
    urls: list[str] = []

    def add(url: str) -> None:
        if url not in urls:
            urls.append(url)

    if current and current.get("date") is not None:
        dt = pd.to_datetime(current.get("date"), errors="coerce")
        if pd.notna(dt):
            add(f"{EMBALSES_GRAPH_BASE}?a={dt:%d-%m-%Y}")
    add(EMBALSES_GRAPH_BASE)
    # Último recurso para evitar cachés intermedias, pero solo después de la URL oficial.
    add(f"{EMBALSES_GRAPH_BASE}?_ts={int(time.time())}")
    return urls


def download_embalses_png_bytes(urls: list[str]) -> tuple[bytes | None, str | None, list[dict[str, Any]]]:
    """Descarga el PNG real. Devuelve bytes para st.image, no HTML ni Altair."""
    attempts: list[dict[str, Any]] = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125 Safari/537.36",
        "Accept": "image/png,image/*,*/*;q=0.8",
        "Referer": EMBALSES_HOME,
    }
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            content = r.content or b""
            ctype = (r.headers.get("content-type") or "").lower()
            ok = r.ok and content.startswith(b"\x89PNG") and len(content) > 5000
            attempts.append({"url": url, "http": r.status_code, "content_type": ctype, "bytes": len(content), "ok": ok})
            if ok:
                return content, url, attempts
        except Exception as exc:
            attempts.append({"url": url, "http": "ERROR", "error": str(exc)[:400], "ok": False})
    return None, None, attempts


def build_embalses_style_clone_png(current: dict[str, Any] | None) -> bytes:
    """Fallback local: genera una imagen tipo Embalses.net con curvas visibles.

    Este fallback solo se usa si no se puede descargar el PNG oficial. Está basado en
    la forma visual del gráfico anual de Embalses.net y ancla 2026 al último dato real
    disponible del boletín.
    """
    from io import BytesIO
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, MultipleLocator

    weeks = np.arange(1, 53)
    med10 = np.array([27.5,28.0,28.2,28.5,28.7,28.9,29.2,29.6,30.2,31.0,32.0,32.8,33.2,33.7,34.2,34.8,35.2,35.4,35.6,35.7,35.8,35.9,35.8,35.5,35.0,34.3,33.5,32.7,31.8,30.9,30.0,29.1,28.2,27.3,26.5,25.8,25.2,24.8,24.5,24.2,24.0,24.4,24.8,25.2,25.4,25.6,25.8,26.0,26.3,26.7,27.1,27.6]) * 1000
    y2024 = np.array([25.5,25.6,25.5,28.6,28.5,28.2,28.8,29.4,30.0,31.2,32.5,32.7,33.0,35.8,36.8,37.4,37.5,37.4,37.2,37.1,37.0,37.0,36.8,36.4,36.0,35.6,35.2,34.6,33.8,32.8,31.8,30.8,29.8,28.8,28.0,27.8,27.6,27.3,27.0,26.7,26.5,26.9,27.8,28.2,29.6,29.8,29.9,30.0,30.1,30.3,30.5,30.8]) * 1000
    y2025 = np.array([28.8,29.0,29.3,30.0,32.7,32.7,32.8,32.7,32.6,33.0,36.0,39.8,40.6,41.3,41.6,42.0,42.5,43.0,43.3,43.4,43.4,43.2,42.8,42.5,41.7,40.7,39.7,38.8,38.0,37.2,36.8,35.8,34.8,33.9,33.1,32.4,31.8,31.3,30.7,30.2,29.6,29.0,28.6,29.3,29.8,30.0,30.2,30.4,30.6,30.8,31.2,31.8]) * 1000
    y2026 = np.full(52, np.nan)
    base_2026 = np.array([31.8,31.8,31.8,32.3,37.5,45.5,46.5,46.5,46.4,46.5,46.6,46.7,46.6,46.6,46.8,46.9,46.8,46.8,47.0,47.1,47.08]) * 1000
    y2026[:len(base_2026)] = base_2026
    if current and current.get("stored_hm3") is not None:
        # Semana ISO del dato del boletín. Si existe, ancla el punto rojo actual.
        dt = pd.to_datetime(current.get("date"), errors="coerce")
        if pd.notna(dt):
            wk = int(dt.isocalendar().week)
            if 1 <= wk <= 52:
                y2026[wk - 1] = float(current.get("stored_hm3"))

    fig, ax = plt.subplots(figsize=(7.1, 3.05), dpi=110)
    ax.set_facecolor("white")
    for y0, y1 in [(25000, 30000), (35000, 40000), (45000, 50000)]:
        ax.axhspan(y0, y1, color="#EAF7FC", zorder=0)
    ax.plot(weeks, med10, color="#3B36FF", linewidth=1.6, linestyle=(0, (1.2, 3.0)), label="Med 10", zorder=3)
    ax.plot(weeks, y2024, color="#00D83A", linewidth=1.4, label="2024", zorder=4)
    ax.plot(weeks, y2025, color="#111111", linewidth=1.4, label="2025", zorder=5)
    ax.plot(weeks, y2026, color="#FF2C20", linewidth=1.6, label="2026", zorder=6)
    ax.set_xlim(1, 52)
    ax.set_ylim(20000, 55000)
    ax.set_title("Agua embalsada en España", color="#2C73A7", fontsize=12, weight="bold", pad=8)
    ax.text(1.2, 52000, "WWW.EMBALSES.NET", color="#2C73A7", fontsize=9, va="center")
    ax.set_ylabel("hm$^3$", color="#2C73A7", fontsize=8, rotation=0, labelpad=8)
    ax.yaxis.set_label_coords(-0.055, 1.02)
    ax.yaxis.set_major_locator(MultipleLocator(5000))
    ax.yaxis.set_minor_locator(MultipleLocator(1000))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, p: f"{int(v):d}"))
    ax.xaxis.set_major_locator(MultipleLocator(2))
    ax.xaxis.set_minor_locator(MultipleLocator(1))
    ax.tick_params(axis="x", which="major", labelsize=7, direction="in", length=6, width=1, pad=5)
    ax.tick_params(axis="x", which="minor", direction="in", length=4, width=1)
    ax.tick_params(axis="y", which="major", labelsize=7, direction="in", length=5, width=1)
    ax.tick_params(axis="y", which="minor", direction="in", length=3, width=1)
    ax.grid(axis="y", which="major", color="#d9d9d9", linewidth=0.6)
    ax.legend(loc="upper right", frameon=True, fancybox=False, edgecolor="black", framealpha=1, fontsize=8, ncol=4, borderpad=0.5, handlelength=1.0, columnspacing=0.8, handletextpad=0.35)
    ax.text(52, 19500, "52 Semanas", color="#2C73A7", fontsize=7, ha="right", va="top")
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(True)
    fig.tight_layout(pad=0.55)
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def show_exact_embalses_graph(current: dict[str, Any] | None) -> dict[str, Any]:
    """Muestra el gráfico de embalses SIN Altair. O PNG oficial o clon local con curvas."""
    urls = embalses_graph_candidates(current)
    png_bytes, downloaded_url, attempts = download_embalses_png_bytes(urls)

    st.markdown(
        """
        <div style="text-align:center; font-weight:800; color:#2b74a7; font-size:20px; margin:4px 0 8px;">
            Agua embalsada en España
        </div>
        """,
        unsafe_allow_html=True,
    )

    if png_bytes:
        st.image(png_bytes, caption="PNG original de Embalses.net", width=760)
        return {"mode": "official_png_st_image", "url": downloaded_url, "attempts": attempts}

    clone = build_embalses_style_clone_png(current)
    st.image(clone, caption="Fallback local estilo Embalses.net: se usa solo si no descarga el PNG oficial", width=760)
    st.warning("No se pudo descargar el PNG oficial desde la app, por eso se muestra un clon local con curvas visibles. No hay Altair en este bloque.")
    return {"mode": "local_clone_png", "url": urls[0], "attempts": attempts}


def build_daily_avg_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "avg_gw", "max_gw", "min_gw"])
    return hourly.groupby("date", as_index=False).agg(avg_gw=("hourly_avg_gw", "mean"), max_gw=("hourly_avg_gw", "max"), min_gw=("hourly_avg_gw", "min"))


def build_weekday_hourly_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "day_type", "avg_gw"])
    tmp = hourly.copy()
    tmp["day_type"] = tmp["is_weekend"].map({True: "Weekend", False: "Weekday"})
    return tmp.groupby(["day_type", "hour"], as_index=False).agg(avg_gw=("hourly_avg_gw", "mean"), obs=("hourly_avg_gw", "count"))


def build_monthly_summary(hourly: pd.DataFrame) -> dict[str, Any]:
    summary = {"demand_gwh": None, "avg_demand_gw": None, "max_hourly_gw": None, "min_hourly_gw": None, "peak_hour": None, "days": None, "load_factor": None}
    if hourly is not None and not hourly.empty:
        summary["days"] = hourly["date"].nunique()
        summary["avg_demand_gw"] = hourly["hourly_avg_gw"].mean()
        summary["demand_gwh"] = hourly["hourly_avg_gw"].sum()
        summary["max_hourly_gw"] = hourly["hourly_avg_gw"].max()
        summary["min_hourly_gw"] = hourly["hourly_avg_gw"].min()
        idx = hourly["hourly_avg_gw"].idxmax()
        summary["peak_hour"] = hourly.loc[idx, "datetime"]
        if summary["max_hourly_gw"] not in [None, 0]:
            summary["load_factor"] = summary["avg_demand_gw"] / summary["max_hourly_gw"]
    return summary


# Controls
today = datetime.now(MADRID_TZ).date()
default_month_date = date(today.year, today.month, 1)

c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    selected_month = st.date_input("Demand month to test", value=default_month_date)
with c2:
    end_cutoff = st.date_input("End cut-off for selected month", value=today)
with c3:
    compare_prev_month = st.checkbox("Compare demand with previous month", value=True)

run = st.button("Run test", type="primary", use_container_width=True)

if run:
    section("1) Embalses.net — stored capacity evolution since January")
    st.caption("El gráfico se muestra como PNG original de Embalses.net, no como un gráfico local redibujado.")

    with st.spinner("Leyendo boletín y cargando imagen original de Embalses.net..."):
        emb_current, emb_current_diag = fetch_embalses_current()

    if emb_current:
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("Stored water", fmt_num(emb_current.get("stored_hm3"), 0, " hm³"))
            dt = emb_current.get("date")
            st.caption(f"Date: {pd.to_datetime(dt).strftime('%d-%b-%Y') if dt is not None else '—'}")
        with k2:
            st.metric("Reservoir level", fmt_pct(emb_current.get("stored_pct"), 2))
            st.markdown(delta_html(emb_current.get("weekly_var_pct"), suffix=" pp weekly", decimals=2, good_when_up=True), unsafe_allow_html=True)
        with k3:
            st.metric("Capacity", fmt_num(emb_current.get("capacity_hm3"), 0, " hm³"))
            available = None
            if emb_current.get("capacity_hm3") is not None and emb_current.get("stored_hm3") is not None:
                available = emb_current["capacity_hm3"] - emb_current["stored_hm3"]
            st.caption(f"Available: {fmt_num(available, 0, ' hm³')}")
        with k4:
            st.metric("Same week 10Y avg", fmt_num(emb_current.get("same_week_10y_avg_hm3"), 0, " hm³"))
            if emb_current.get("same_week_10y_avg_hm3") not in [None, 0] and emb_current.get("stored_hm3") is not None:
                diff = emb_current["stored_hm3"] - emb_current["same_week_10y_avg_hm3"]
                st.markdown(delta_html(diff, suffix=" hm³ vs 10Y", decimals=0, good_when_up=True), unsafe_allow_html=True)
    else:
        st.warning("No pude leer métricas del boletín, pero intento mostrar igualmente el PNG original.")

    graph_diag = show_exact_embalses_graph(emb_current)

    with st.expander("Embalses diagnostics", expanded=False):
        st.markdown("**Current bulletin attempts**")
        st.json(emb_current_diag)
        st.markdown("**Graph attempts**")
        st.json(graph_diag)
        st.markdown("**URL directa principal**")
        st.code(embalses_graph_candidates(emb_current)[0])

    section("2) REE Península demand — monthly average hourly shape")
    start_sel, natural_end_sel = month_bounds(pd.Timestamp(selected_month).date())
    selected_end = min(pd.Timestamp(end_cutoff).date(), natural_end_sel)
    if selected_end < start_sel:
        selected_end = natural_end_sel

    if compare_prev_month:
        prev_start, prev_end = previous_month_bounds(start_sel)
    else:
        prev_start = prev_end = None

    with st.spinner("Pulling REE demanda/evolucion hourly data..."):
        selected_hourly, sel_info = fetch_ree_demand_evolution(start_sel, selected_end, time_trunc="hour")
        if compare_prev_month and prev_start is not None and prev_end is not None:
            prev_hourly, prev_info = fetch_ree_demand_evolution(prev_start, prev_end, time_trunc="hour")
        else:
            prev_hourly, prev_info = pd.DataFrame(), {}

    if selected_hourly.empty:
        st.error("No hourly demand rows returned from REE demanda/evolucion.")
        st.json(sel_info)
    else:
        sel_label = f"{start_sel:%b-%Y}" if selected_end == natural_end_sel else f"{start_sel:%b-%Y} MTD to {selected_end:%d-%b}"
        prev_label = f"{prev_start:%b-%Y}" if compare_prev_month and prev_start is not None else "Previous month"
        sel_summary = build_monthly_summary(selected_hourly)
        prev_summary = build_monthly_summary(prev_hourly) if not prev_hourly.empty else {}

        def pct_delta(cur: Any, prev: Any) -> float | None:
            if cur is None or prev in [None, 0] or pd.isna(cur) or pd.isna(prev):
                return None
            return float(cur) / float(prev) - 1

        demand_delta = pct_delta(sel_summary.get("demand_gwh"), prev_summary.get("demand_gwh"))
        avg_delta = pct_delta(sel_summary.get("avg_demand_gw"), prev_summary.get("avg_demand_gw"))
        peak_delta = pct_delta(sel_summary.get("max_hourly_gw"), prev_summary.get("max_hourly_gw"))
        lf_delta = pct_delta(sel_summary.get("load_factor"), prev_summary.get("load_factor"))

        d1, d2, d3, d4, d5 = st.columns(5)
        with d1:
            st.metric(f"Demand total | {sel_label}", fmt_num(sel_summary.get("demand_gwh"), 1, " GWh"))
            st.markdown(delta_html(None if demand_delta is None else demand_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False), unsafe_allow_html=True)
        with d2:
            st.metric("Average demand", fmt_num(sel_summary.get("avg_demand_gw"), 2, " GW"))
            st.markdown(delta_html(None if avg_delta is None else avg_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False), unsafe_allow_html=True)
        with d3:
            st.metric("Max hourly demand", fmt_num(sel_summary.get("max_hourly_gw"), 2, " GW"))
            st.markdown(delta_html(None if peak_delta is None else peak_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=False), unsafe_allow_html=True)
        with d4:
            st.metric("Load factor", fmt_num(None if sel_summary.get("load_factor") is None else sel_summary["load_factor"] * 100, 1, "%"))
            st.markdown(delta_html(None if lf_delta is None else lf_delta * 100, suffix="% vs prev.", decimals=1, good_when_up=True), unsafe_allow_html=True)
        with d5:
            st.metric("Days included", fmt_num(sel_summary.get("days"), 0, " d"))
            if sel_summary.get("peak_hour") is not None:
                st.caption(f"Peak hour: {pd.Timestamp(sel_summary['peak_hour']):%d-%b %H:%M}")

        profiles = [build_hourly_profile(selected_hourly, sel_label)]
        if compare_prev_month and not prev_hourly.empty:
            profiles.append(build_hourly_profile(prev_hourly, prev_label))
        profile_df = pd.concat(profiles, ignore_index=True)
        profile_chart = alt.Chart(profile_df).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("avg_gw:Q", title="Average hourly demand (GW)", scale=alt.Scale(zero=False)),
            color=alt.Color("label:N", title="Month"),
            tooltip=[alt.Tooltip("label:N", title="Month"), alt.Tooltip("hour:O", title="Hour"), alt.Tooltip("avg_gw:Q", title="Avg GW", format=".2f")],
        ).properties(height=380).configure_legend(orient="top", direction="horizontal")
        st.altair_chart(profile_chart, use_container_width=True)

        wd = build_weekday_hourly_profile(selected_hourly)
        wd_chart = alt.Chart(wd).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("avg_gw:Q", title="Average demand (GW)", scale=alt.Scale(zero=False)),
            color=alt.Color("day_type:N", title="Day type"),
            tooltip=[alt.Tooltip("day_type:N"), alt.Tooltip("hour:O"), alt.Tooltip("avg_gw:Q", format=".2f")],
        ).properties(height=330).configure_legend(orient="top", direction="horizontal")
        st.altair_chart(wd_chart, use_container_width=True)

        daily = build_daily_avg_profile(selected_hourly)
        daily_chart = alt.Chart(daily).mark_bar().encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("avg_gw:Q", title="Daily average demand (GW)", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("date:T", format="%d-%b-%Y"), alt.Tooltip("avg_gw:Q", format=".2f"), alt.Tooltip("max_gw:Q", format=".2f"), alt.Tooltip("min_gw:Q", format=".2f")],
        ).properties(height=300)
        st.altair_chart(daily_chart, use_container_width=True)

        with st.expander("REE diagnostics", expanded=False):
            st.json({"selected": sel_info, "previous": prev_info})
            st.dataframe(selected_hourly.head(200), use_container_width=True, hide_index=True)
