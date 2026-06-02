from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

st.set_page_config(page_title="Embalses + demand profile test v3", layout="wide")

BASE_DIR = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(BASE_DIR / ".env", override=True)
    load_dotenv(override=True)

MADRID_TZ = ZoneInfo("Europe/Madrid")
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
BLUE = "#1D4ED8"
RED = "#DC2626"

EMBALSES_HOME = "https://www.embalses.net/"
EMBALSES_BOLETIN = "https://www.embalses.net/suscripciones/boletin-web.php"
EMBALSES_ARCHIVE_BASE = "https://www.embalses.net/archive/index.php/"
REE_API_BASE = "https://apidatos.ree.es/es/datos"
REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

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
    ">🧪 Test v3 | Embalses style + REE demand shape</div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "Embalses.net is scraped as a public webpage source, so this page includes diagnostics and fallbacks. "
    "v3 adds an Embalses.net-style weekly chart: Med 10 + previous two years + current year. "
    "REE demand uses `demanda/evolucion` for Península."
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


def fmt_num(v, decimals=1, suffix=""):
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.{decimals}f}{suffix}"


def fmt_pct(v, decimals=1):
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.{decimals}f}%"


def delta_html(value, suffix="", decimals=1, good_when_up=True):
    if value is None or pd.isna(value):
        return '<span style="color:#94A3B8;">→ n/a</span>'
    v = float(value)
    positive = v >= 0
    good = positive if good_when_up else not positive
    color = "#16A34A" if good else "#DC2626"
    arrow = "↑" if positive else "↓"
    return f'<span style="color:{color};font-weight:800;">{arrow} {v:+,.{decimals}f}{suffix}</span>'


def safe_json(response: requests.Response):
    try:
        return response.json()
    except Exception:
        return {"non_json_body_preview": (response.text or "")[:4000]}


def http_get(url: str, timeout: int = 45) -> requests.Response:
    return requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 NexwellPower-Test/1.0",
            "Accept": "text/html,application/xhtml+xml,application/json",
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
def fetch_embalses_current() -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    urls = [EMBALSES_BOLETIN, EMBALSES_HOME]
    best: dict[str, Any] = {}
    tables = pd.DataFrame()
    diagnostics: dict[str, Any] = {"attempts": []}
    for url in urls:
        try:
            resp = http_get(url)
            parsed = parse_embalses_summary_from_html(resp.text or "", resp.url)
            diagnostics["attempts"].append({"url": resp.url, "http": resp.status_code, "response_chars": len(resp.text or ""), "stored_hm3": parsed.get("stored_hm3"), "date": str(parsed.get("date"))})
            if parsed.get("stored_hm3") is not None:
                best = parsed
                try:
                    html_tables = pd.read_html(resp.text or "", decimal=",", thousands=".")
                    frames = []
                    for i, t in enumerate(html_tables):
                        tmp = t.copy()
                        tmp.columns = [str(c).strip() for c in tmp.columns]
                        tmp["table_id"] = i
                        frames.append(tmp)
                    tables = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                except Exception:
                    tables = pd.DataFrame()
                break
        except Exception as exc:
            diagnostics["attempts"].append({"url": url, "http": "ERROR", "error": str(exc)[:500]})
    return best, tables, diagnostics


def archive_page_urls(max_pages: int) -> list[str]:
    urls = [urljoin(EMBALSES_ARCHIVE_BASE, "f-367.html")]
    for p in range(2, max_pages + 1):
        urls.extend([urljoin(EMBALSES_ARCHIVE_BASE, f"f-367-p-{p}.html"), urljoin(EMBALSES_ARCHIVE_BASE, f"f-367-p-{p}.php")])
    return urls


def extract_thread_links(index_html: str, base_url: str) -> list[str]:
    links = set()
    for href in re.findall(r'href=["\']([^"\']+)["\']', index_html, flags=re.I):
        if re.search(r"(?:^|/)t-\d+\.html", href) or re.search(r"index\.php/t-\d+\.html", href):
            links.add(urljoin(base_url, href))
    return sorted(links)


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_embalses_historical_from_archive(max_index_pages: int = 8, max_threads: int = 80) -> tuple[pd.DataFrame, pd.DataFrame]:
    diagnostics = []
    thread_links: list[str] = []
    for url in archive_page_urls(max_index_pages):
        try:
            resp = http_get(url, timeout=30)
            row = {"stage": "index", "url": resp.url, "http": resp.status_code, "response_chars": len(resp.text or ""), "links_found": 0}
            if resp.ok:
                links = extract_thread_links(resp.text or "", resp.url)
                row["links_found"] = len(links)
                thread_links.extend(links)
            diagnostics.append(row)
        except Exception as exc:
            diagnostics.append({"stage": "index", "url": url, "http": "ERROR", "error": str(exc)[:500], "links_found": 0})
    thread_links = sorted(set(thread_links), reverse=True)[:max_threads]
    rows = []
    for url in thread_links:
        try:
            resp = http_get(url, timeout=30)
            parsed = parse_embalses_summary_from_html(resp.text or "", resp.url)
            diagnostics.append({"stage": "thread", "url": resp.url, "http": resp.status_code, "response_chars": len(resp.text or ""), "date": str(parsed.get("date")), "stored_hm3": parsed.get("stored_hm3")})
            if parsed.get("date") is not None and parsed.get("stored_hm3") is not None:
                rows.append(parsed)
        except Exception as exc:
            diagnostics.append({"stage": "thread", "url": url, "http": "ERROR", "error": str(exc)[:500]})
    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "stored_hm3"]).copy()
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        df["year"] = df["date"].dt.year
        df["week"] = df["date"].dt.isocalendar().week.astype(int)
        df["available_hm3"] = pd.to_numeric(df["capacity_hm3"], errors="coerce") - pd.to_numeric(df["stored_hm3"], errors="coerce")
        df["available_pct"] = 100.0 - pd.to_numeric(df["stored_pct"], errors="coerce")
    return df, pd.DataFrame(diagnostics)


def build_embalses_chart_df(hist: pd.DataFrame, current: dict[str, Any], since: date) -> pd.DataFrame:
    rows = []
    if hist is not None and not hist.empty:
        tmp = hist[pd.to_datetime(hist["date"]).dt.date >= since].copy()
        for _, r in tmp.iterrows():
            rows.append({"date": r["date"], "series": "Current year", "hm3": r.get("stored_hm3"), "pct": r.get("stored_pct")})
            rows.append({"date": r["date"], "series": "Same week 2025", "hm3": r.get("same_week_last_year_hm3"), "pct": r.get("same_week_last_year_pct")})
            rows.append({"date": r["date"], "series": "10Y average", "hm3": r.get("same_week_10y_avg_hm3"), "pct": r.get("same_week_10y_avg_pct")})
    if current and current.get("date") is not None and current.get("stored_hm3") is not None:
        cdate = pd.to_datetime(current.get("date"), errors="coerce")
        if pd.notna(cdate) and cdate.date() >= since:
            rows.extend([
                {"date": cdate, "series": "Current year", "hm3": current.get("stored_hm3"), "pct": current.get("stored_pct")},
                {"date": cdate, "series": "Same week 2025", "hm3": current.get("same_week_last_year_hm3"), "pct": current.get("same_week_last_year_pct")},
                {"date": cdate, "series": "10Y average", "hm3": current.get("same_week_10y_avg_hm3"), "pct": current.get("same_week_10y_avg_pct")},
            ])
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["hm3"] = pd.to_numeric(out["hm3"], errors="coerce")
    out["pct"] = pd.to_numeric(out["pct"], errors="coerce")
    return out.dropna(subset=["date", "series", "hm3"]).sort_values(["date", "series"]).drop_duplicates(subset=["date", "series"], keep="last").reset_index(drop=True)



def build_embalses_net_style_df(hist: pd.DataFrame, current: dict[str, Any], current_year: int) -> pd.DataFrame:
    """Return weekly series in the visual style of Embalses.net: Med 10, Y-2, Y-1 and Y."""
    display_years = [current_year - 2, current_year - 1, current_year]
    rows: list[dict[str, Any]] = []

    def add_observation(dt_value: Any, stored_hm3: Any, stored_pct: Any, source: str) -> None:
        dt = pd.to_datetime(dt_value, errors="coerce")
        hm3 = pd.to_numeric(stored_hm3, errors="coerce")
        if pd.isna(dt) or pd.isna(hm3):
            return
        year = int(dt.year)
        if year not in display_years:
            return
        rows.append({
            "date": dt,
            "week": int(dt.isocalendar().week),
            "series": str(year),
            "hm3": float(hm3),
            "pct": pd.to_numeric(stored_pct, errors="coerce"),
            "source": source,
            "sort_key": display_years.index(year) + 1,
        })

    def add_med10(dt_value: Any, med_hm3: Any, med_pct: Any, source: str) -> None:
        dt = pd.to_datetime(dt_value, errors="coerce")
        hm3 = pd.to_numeric(med_hm3, errors="coerce")
        if pd.isna(dt) or pd.isna(hm3):
            return
        rows.append({
            "date": dt,
            "week": int(dt.isocalendar().week),
            "series": "Med 10",
            "hm3": float(hm3),
            "pct": pd.to_numeric(med_pct, errors="coerce"),
            "source": source,
            "sort_key": 0,
        })

    if hist is not None and not hist.empty:
        tmp = hist.copy()
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
        tmp = tmp.dropna(subset=["date"])
        for _, r in tmp.iterrows():
            add_observation(r.get("date"), r.get("stored_hm3"), r.get("stored_pct"), "archive")
            # Embalses.net archive pages usually include the same-week 10-year average on each bulletin.
            add_med10(r.get("date"), r.get("same_week_10y_avg_hm3"), r.get("same_week_10y_avg_pct"), "archive_10y")

    if current and current.get("date") is not None:
        add_observation(current.get("date"), current.get("stored_hm3"), current.get("stored_pct"), "current")
        add_med10(current.get("date"), current.get("same_week_10y_avg_hm3"), current.get("same_week_10y_avg_pct"), "current_10y")

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["week"] = pd.to_numeric(out["week"], errors="coerce").astype("Int64")
    out["hm3"] = pd.to_numeric(out["hm3"], errors="coerce")
    out["pct"] = pd.to_numeric(out["pct"], errors="coerce")
    out = out.dropna(subset=["week", "series", "hm3"]).copy()
    out["week"] = out["week"].astype(int)

    # If the site does not expose enough Med 10 rows, build a fallback average by ISO week from the available archive.
    med_weeks = set(out.loc[out["series"].eq("Med 10"), "week"].dropna().astype(int).tolist())
    if hist is not None and not hist.empty:
        tmp = hist.copy()
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
        tmp["stored_hm3"] = pd.to_numeric(tmp.get("stored_hm3"), errors="coerce")
        tmp["stored_pct"] = pd.to_numeric(tmp.get("stored_pct"), errors="coerce")
        tmp = tmp.dropna(subset=["date", "stored_hm3"])
        tmp["week"] = tmp["date"].dt.isocalendar().week.astype(int)
        tmp["year"] = tmp["date"].dt.year.astype(int)
        hist_avg = tmp[~tmp["year"].isin(display_years)].groupby("week", as_index=False).agg(hm3=("stored_hm3", "mean"), pct=("stored_pct", "mean"))
        missing_avg = hist_avg[~hist_avg["week"].isin(med_weeks)].copy()
        if not missing_avg.empty:
            missing_avg["date"] = pd.NaT
            missing_avg["series"] = "Med 10"
            missing_avg["source"] = "fallback_avg"
            missing_avg["sort_key"] = 0
            out = pd.concat([out, missing_avg[["date", "week", "series", "hm3", "pct", "source", "sort_key"]]], ignore_index=True)

    order = ["Med 10", str(current_year - 2), str(current_year - 1), str(current_year)]
    out["series"] = pd.Categorical(out["series"], categories=order, ordered=True)
    return out.sort_values(["sort_key", "series", "week", "date"]).drop_duplicates(subset=["series", "week"], keep="last").reset_index(drop=True)


def make_embalses_net_style_chart(style_df: pd.DataFrame, y_field: str = "hm3") -> alt.Chart:
    if style_df is None or style_df.empty:
        return alt.Chart(pd.DataFrame())
    y_title = "hm³" if y_field == "hm3" else "%"
    y_vals = pd.to_numeric(style_df[y_field], errors="coerce").dropna()
    y_min = max(0, int((y_vals.min() // 5000) * 5000)) if y_field == "hm3" and not y_vals.empty else None
    y_max = int(((y_vals.max() // 5000) + 1) * 5000) if y_field == "hm3" and not y_vals.empty else None
    if y_field != "hm3":
        y_min = max(0, int((y_vals.min() // 5) * 5)) if not y_vals.empty else None
        y_max = min(100, int(((y_vals.max() // 5) + 1) * 5)) if not y_vals.empty else None

    order = [str(s) for s in style_df["series"].cat.categories] if hasattr(style_df["series"], "cat") else list(style_df["series"].dropna().astype(str).unique())
    current_year = max([int(s) for s in order if str(s).isdigit()] or [datetime.now(MADRID_TZ).year])
    color_domain = ["Med 10", str(current_year - 2), str(current_year - 1), str(current_year)]
    color_range = ["#1E22FF", "#00E846", "#111111", "#FF2A1F"]
    dash_range = [[2, 6], [1, 0], [1, 0], [1, 0]]

    base = alt.Chart(style_df).encode(
        x=alt.X("week:Q", title="52 Semanas", scale=alt.Scale(domain=[1, 52]), axis=alt.Axis(values=list(range(1, 53, 2)), labelAngle=0, grid=False)),
        y=alt.Y(f"{y_field}:Q", title=y_title, scale=alt.Scale(domain=[y_min, y_max], zero=False), axis=alt.Axis(grid=True, tickCount=8)),
        color=alt.Color("series:N", title=None, scale=alt.Scale(domain=color_domain, range=color_range), legend=alt.Legend(orient="top", direction="horizontal", fillColor="#F7F7F7", strokeColor="#222", padding=8)),
        strokeDash=alt.StrokeDash("series:N", scale=alt.Scale(domain=color_domain, range=dash_range), legend=None),
        order=alt.Order("week:Q"),
        tooltip=[alt.Tooltip("series:N", title="Serie"), alt.Tooltip("week:Q", title="Semana", format=".0f"), alt.Tooltip(f"{y_field}:Q", title=y_title, format=",.0f"), alt.Tooltip("pct:Q", title="%", format=",.2f")],
    )

    layers = []
    if y_min is not None and y_max is not None and y_max > y_min:
        step = 5000 if y_field == "hm3" else 5
        bands = []
        i = 0
        y = y_min
        while y < y_max:
            if i % 2 == 0:
                bands.append({"y0": y, "y1": min(y + step, y_max)})
            y += step
            i += 1
        if bands:
            layers.append(alt.Chart(pd.DataFrame(bands)).mark_rect(color="#EAF6FB", opacity=0.85).encode(y="y0:Q", y2="y1:Q"))

    layers.append(base.mark_line(interpolate="monotone", strokeWidth=2.6))
    return alt.layer(*layers).properties(title="Agua embalsada en España", height=430).configure_view(stroke="#222222").configure_axis(labelFontSize=12, titleFontSize=12).configure_title(anchor="middle", fontSize=20, color="#40779D")


def parse_ree_included_series(payload: dict, value_field: str = "value") -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or item.get("id")
        for val in attrs.get("values", []) or []:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue
            dt = dt.tz_convert("Europe/Madrid").tz_localize(None)
            rows.append({"datetime": dt, "title": str(title).strip(), value_field: pd.to_numeric(val.get(value_field), errors="coerce")})
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_ree_demand_evolution(start_day: date, end_day: date, time_trunc: str = "hour") -> tuple[pd.DataFrame, dict[str, Any]]:
    params = {"start_date": f"{start_day.isoformat()}T00:00", "end_date": f"{end_day.isoformat()}T23:59", "time_trunc": time_trunc, **REE_PENINSULAR_PARAMS}
    url = f"{REE_API_BASE}/demanda/evolucion"
    resp = requests.get(url, params=params, timeout=60)
    payload = safe_json(resp)
    if not resp.ok or not isinstance(payload, dict):
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}
    df = parse_ree_included_series(payload, value_field="value")
    if df.empty:
        return pd.DataFrame(), {"http": resp.status_code, "url": resp.url, "rows": 0, "payload_preview": payload}
    if df["title"].nunique() > 1:
        demand_like = df[df["title"].astype(str).str.contains("demanda", case=False, na=False)].copy()
        if not demand_like.empty:
            df = demand_like
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    if time_trunc == "hour":
        df["hourly_avg_mw"] = df["value"]
        df["hourly_avg_gw"] = df["hourly_avg_mw"] / 1000.0
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df["weekday"] = df["datetime"].dt.day_name()
        df["is_weekend"] = df["datetime"].dt.weekday >= 5
    elif time_trunc == "month":
        df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        df["demand_mwh"] = df["value"]
        df["demand_gwh"] = df["demand_mwh"] / 1000.0
        df["avg_demand_gw"] = df["demand_gwh"] / (df["month"].dt.days_in_month * 24)
    info = {"http": resp.status_code, "url": resp.url, "rows": int(len(df)), "title_values": ", ".join(sorted(df["title"].dropna().astype(str).unique().tolist())[:5]), "payload_preview": None}
    return df.sort_values("datetime").reset_index(drop=True), info


def month_bounds(d: date) -> tuple[date, date]:
    start = date(d.year, d.month, 1)
    end = date(d.year, 12, 31) if d.month == 12 else date(d.year, d.month + 1, 1) - timedelta(days=1)
    return start, end


def previous_month_bounds(d: date) -> tuple[date, date]:
    first = date(d.year, d.month, 1)
    return month_bounds(first - timedelta(days=1))


def build_hourly_profile(hourly: pd.DataFrame, label: str) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "avg_gw", "min_gw", "max_gw", "label"])
    out = hourly.groupby("hour", as_index=False).agg(avg_gw=("hourly_avg_gw", "mean"), min_gw=("hourly_avg_gw", "min"), max_gw=("hourly_avg_gw", "max"), obs=("hourly_avg_gw", "count"))
    out["label"] = label
    return out


def build_weekday_hourly_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["hour", "day_type", "avg_gw"])
    tmp = hourly.copy()
    tmp["day_type"] = tmp["is_weekend"].map({True: "Weekend", False: "Weekday"})
    return tmp.groupby(["day_type", "hour"], as_index=False).agg(avg_gw=("hourly_avg_gw", "mean"), obs=("hourly_avg_gw", "count"))


def build_daily_avg_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "avg_gw", "max_gw", "min_gw"])
    return hourly.groupby("date", as_index=False).agg(avg_gw=("hourly_avg_gw", "mean"), max_gw=("hourly_avg_gw", "max"), min_gw=("hourly_avg_gw", "min"))


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


today = datetime.now(MADRID_TZ).date()
default_month_date = date(today.year, today.month, 1)

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
with c1:
    selected_month = st.date_input("Demand month to test", value=default_month_date)
with c2:
    end_cutoff = st.date_input("End cut-off for selected month", value=today)
with c3:
    archive_pages = st.slider("Embalses archive index pages to scan", min_value=1, max_value=20, value=8)
with c4:
    max_threads = st.slider("Max Embalses archive threads", min_value=10, max_value=200, value=80, step=10)
compare_prev_month = st.checkbox("Compare demand with previous month", value=True)

run = st.button("Run test", type="primary", use_container_width=True)

if run:
    section("1) Embalses.net — stored capacity evolution since January")
    st.caption("Target series: current stored water, same week 2025 and same week 10-year average.")

    with st.spinner("Scraping Embalses.net current bulletin and archive pages..."):
        emb_current, emb_tables, emb_current_diag = fetch_embalses_current()
        emb_hist, emb_archive_diag = fetch_embalses_historical_from_archive(max_index_pages=int(archive_pages), max_threads=int(max_threads))

    jan_start = date(today.year, 1, 1)
    chart_df = build_embalses_chart_df(emb_hist, emb_current, since=jan_start)
    embalses_style_df = build_embalses_net_style_df(emb_hist, emb_current, current_year=today.year)

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

    if embalses_style_df.empty:
        st.warning("Could not build the Embalses.net-style weekly series from archive. Check diagnostics below.")
    else:
        st.altair_chart(make_embalses_net_style_chart(embalses_style_df, y_field="hm3"), use_container_width=True)
        st.caption("Style v3: dotted blue Med 10, green previous-2 year, black previous year and red current year, with ISO weeks on the x-axis.")

    if chart_df.empty:
        st.warning("Could not build the legacy Embalses historical series from archive. Check diagnostics below.")
    else:
        tab_hm3, tab_pct, tab_available, tab_style_data = st.tabs(["Legacy stored hm³", "Legacy stored %", "Available capacity", "Style data"])
        with tab_hm3:
            ch = alt.Chart(chart_df).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("date:T", title="Week"),
                y=alt.Y("hm3:Q", title="Stored water (hm³)", scale=alt.Scale(zero=False)),
                color=alt.Color("series:N", title="Series"),
                tooltip=[alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"), alt.Tooltip("series:N"), alt.Tooltip("hm3:Q", title="hm³", format=",.0f"), alt.Tooltip("pct:Q", title="%", format=",.2f")],
            ).properties(height=420).configure_legend(orient="top", direction="horizontal")
            st.altair_chart(ch, use_container_width=True)
        with tab_pct:
            ch_pct = alt.Chart(chart_df.dropna(subset=["pct"])).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("date:T", title="Week"),
                y=alt.Y("pct:Q", title="Stored water (%)", scale=alt.Scale(zero=False)),
                color=alt.Color("series:N", title="Series"),
                tooltip=[alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"), alt.Tooltip("series:N"), alt.Tooltip("pct:Q", title="%", format=",.2f"), alt.Tooltip("hm3:Q", title="hm³", format=",.0f")],
            ).properties(height=420).configure_legend(orient="top", direction="horizontal")
            st.altair_chart(ch_pct, use_container_width=True)
        with tab_available:
            cap = emb_current.get("capacity_hm3") if emb_current else None
            if cap is not None and pd.notna(cap):
                avail_df = chart_df.copy()
                avail_df["available_hm3"] = float(cap) - avail_df["hm3"]
                avail_df["available_pct"] = 100.0 - avail_df["pct"]
                ch_av = alt.Chart(avail_df.dropna(subset=["available_hm3"])).mark_line(point=True, strokeWidth=3).encode(
                    x=alt.X("date:T", title="Week"),
                    y=alt.Y("available_hm3:Q", title="Available capacity (hm³)", scale=alt.Scale(zero=False)),
                    color=alt.Color("series:N", title="Series"),
                    tooltip=[alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"), alt.Tooltip("series:N"), alt.Tooltip("available_hm3:Q", title="Available hm³", format=",.0f"), alt.Tooltip("available_pct:Q", title="Available %", format=",.2f")],
                ).properties(height=420).configure_legend(orient="top", direction="horizontal")
                st.altair_chart(ch_av, use_container_width=True)
            else:
                st.info("Capacity could not be parsed, so available capacity cannot be calculated.")
        with tab_style_data:
            style_pivot = embalses_style_df.pivot_table(index="week", columns="series", values="hm3", aggfunc="last") if not embalses_style_df.empty else pd.DataFrame()
            st.dataframe(style_pivot, use_container_width=True)
            display_df = chart_df.pivot_table(index="date", columns="series", values=["hm3", "pct"], aggfunc="last")
            st.markdown("**Legacy table**")
            st.dataframe(display_df, use_container_width=True)

    with st.expander("Embalses diagnostics", expanded=False):
        st.markdown("**Current bulletin attempts**")
        st.json(emb_current_diag)
        st.markdown("**Archive diagnostics**")
        st.dataframe(emb_archive_diag, use_container_width=True, hide_index=True)
        if not emb_tables.empty:
            st.markdown("**Current page parsed HTML tables**")
            st.dataframe(emb_tables.head(250), use_container_width=True)

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

        def pct_delta(cur, prev):
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
            tooltip=[alt.Tooltip("label:N", title="Month"), alt.Tooltip("hour:O", title="Hour"), alt.Tooltip("avg_gw:Q", title="Average GW", format=",.2f"), alt.Tooltip("min_gw:Q", title="Min GW", format=",.2f"), alt.Tooltip("max_gw:Q", title="Max GW", format=",.2f"), alt.Tooltip("obs:Q", title="Hours obs.", format=",.0f")],
        ).properties(height=420).configure_legend(orient="top", direction="horizontal")
        st.altair_chart(profile_chart, use_container_width=True)

        weekday_profile = build_weekday_hourly_profile(selected_hourly)
        if not weekday_profile.empty:
            weekday_chart = alt.Chart(weekday_profile).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("avg_gw:Q", title="Average hourly demand (GW)", scale=alt.Scale(zero=False)),
                color=alt.Color("day_type:N", title="Day type"),
                tooltip=[alt.Tooltip("day_type:N", title="Day type"), alt.Tooltip("hour:O", title="Hour"), alt.Tooltip("avg_gw:Q", title="Average GW", format=",.2f"), alt.Tooltip("obs:Q", title="Obs.", format=",.0f")],
            ).properties(height=330).configure_legend(orient="top", direction="horizontal")
            st.altair_chart(weekday_chart, use_container_width=True)

        daily_avg = build_daily_avg_profile(selected_hourly)
        if not daily_avg.empty:
            daily_chart = alt.Chart(daily_avg).mark_line(point=True, strokeWidth=2.5, color=BLUE).encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("avg_gw:Q", title="Daily average demand (GW)", scale=alt.Scale(zero=False)),
                tooltip=[alt.Tooltip("date:T", title="Date", format="%d-%b-%Y"), alt.Tooltip("avg_gw:Q", title="Avg GW", format=",.2f"), alt.Tooltip("max_gw:Q", title="Max GW", format=",.2f"), alt.Tooltip("min_gw:Q", title="Min GW", format=",.2f")],
            ).properties(height=280)
            st.altair_chart(daily_chart, use_container_width=True)

        with st.expander("REE demand raw profile table / diagnostics", expanded=False):
            st.dataframe(profile_df.style.format({"avg_gw": "{:,.2f}", "min_gw": "{:,.2f}", "max_gw": "{:,.2f}", "obs": "{:,.0f}"}), use_container_width=True, hide_index=True)
            st.markdown("**Selected month hourly**")
            st.json(sel_info)
            if compare_prev_month:
                st.markdown("**Previous month hourly**")
                st.json(prev_info)
