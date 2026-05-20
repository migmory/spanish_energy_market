from __future__ import annotations

import json
import re
from datetime import date
from html import unescape
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    alt.data_transformers.disable_max_rows()
except Exception:
    pass

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Test | REE Autoconsumo Source Discovery", layout="wide")

REE_AUTOCONSUMO_PAGE = "https://www.ree.es/es/datos/autoconsumo"
REE_GENERACION_PAGE = "https://www.ree.es/es/datos/generacion"
REE_API_BASE = "https://apidatos.ree.es/es/datos"

PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREEN_LIGHT = "#D1FAE5"
BLUE = "#1D4ED8"
BLUE_DARK = "#1E3A8A"
YELLOW = "#FBBF24"
YELLOW_DARK = "#D97706"
ORANGE = "#EA580C"
RED = "#DC2626"
GREY = "#6B7280"
GRID = "#E5E7EB"
TEXT = "#111827"
WHITE = "#FFFFFF"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NexwellEnergyMarketApp/1.0)",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}

# Candidate public API paths. These are deliberately broad probes:
# the page will show which ones work and which ones do not.
CANDIDATE_WIDGETS = [
    # Existing public endpoint that has been returning 500 in prior tests
    ("generacion", "potencia-instalada"),

    # Autoconsumo guesses / likely naming families
    ("autoconsumo", "potencia-instalada"),
    ("autoconsumo", "potencia-autoconsumo"),
    ("autoconsumo", "potencia-instalada-autoconsumo"),
    ("autoconsumo", "estructura-potencia-autoconsumo"),
    ("autoconsumo", "potencia-total-autoconsumo"),
    ("autoconsumo", "capacidad-autoconsumo"),
    ("autoconsumo", "potencia-autoconsumo-total"),
    ("autoconsumo", "potencia-instalaciones-autoconsumo"),

    # Sometimes widgets are placed under generation even if conceptually autoconsumo
    ("generacion", "potencia-autoconsumo"),
    ("generacion", "potencia-instalada-autoconsumo"),
    ("generacion", "estructura-potencia-autoconsumo"),
]

TIME_TRUNCS = ["month", "year"]

# =========================================================
# STYLE
# =========================================================
def section(title: str, icon: str = "🧪") -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
            color: white;
            padding: 14px 18px;
            border-radius: 14px;
            font-weight: 850;
            font-size: 1.35rem;
            margin-top: 8px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">{icon} {title}</div>
        """,
        unsafe_allow_html=True,
    )


def subsection(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            margin-top: 16px;
            margin-bottom: 8px;
            padding: 8px 12px;
            color: #0F172A;
            background: #F4FCF8;
            border-left: 5px solid {CORP_GREEN};
            font-size: 1.02rem;
            font-weight: 800;
            border-radius: 8px;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def card_note(text: str, tone: str = "info") -> None:
    if tone == "warn":
        bg, border, color = "#FFF7ED", "#FDBA74", "#9A3412"
    elif tone == "ok":
        bg, border, color = "#ECFDF5", "#6EE7B7", "#065F46"
    else:
        bg, border, color = "#EFF6FF", "#93C5FD", "#1D4ED8"
    st.markdown(
        f"""
        <div style="
            border: 1px solid {border};
            background: {bg};
            color: {color};
            border-radius: 12px;
            padding: 12px 14px;
            margin: 8px 0 12px 0;
            font-size: 0.95rem;
        ">{text}</div>
        """,
        unsafe_allow_html=True,
    )


def chart_style(chart, height: int = 420):
    return (
        chart.properties(height=height)
        .configure_view(stroke="#E5E7EB", fill="white")
        .configure_axis(
            grid=True,
            gridColor="#E5E7EB",
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor="#111827",
            titleColor="#111827",
            labelFontSize=12,
            titleFontSize=13,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=12,
            titleFontSize=12,
            symbolStrokeWidth=3,
        )
    )


# =========================================================
# LOW-LEVEL HELPERS
# =========================================================
def short_text(value: str, limit: int = 500) -> str:
    s = re.sub(r"\s+", " ", str(value or "")).strip()
    return s[:limit] + ("…" if len(s) > limit else "")


def normalize_text(value: str) -> str:
    s = unescape(str(value or "")).strip().lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }
    for a, b in replacements.items():
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


def absolute_url(base_url: str, candidate: str) -> str:
    return urljoin(base_url, candidate)


def is_probable_js_url(url: str) -> bool:
    lower = url.lower()
    return lower.endswith(".js") or ".js?" in lower


def extract_script_urls(html: str, base_url: str) -> list[str]:
    raw = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.I)
    urls = [absolute_url(base_url, x) for x in raw]
    return sorted(dict.fromkeys(urls))


def extract_link_urls(html: str, base_url: str) -> list[str]:
    raw = re.findall(r'<link[^>]+href=["\']([^"\']+)["\']', html, flags=re.I)
    urls = [absolute_url(base_url, x) for x in raw]
    return sorted(dict.fromkeys(urls))


def find_strings_of_interest(text: str) -> list[str]:
    snippets: list[str] = []
    patterns = [
        r".{0,80}autoconsumo.{0,160}",
        r".{0,80}potencia.{0,160}",
        r".{0,80}apidatos\.ree\.es.{0,180}",
        r".{0,80}/es/datos/.{0,180}",
        r".{0,80}\.json.{0,160}",
        r".{0,80}widget.{0,160}",
    ]
    seen = set()
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I | re.S):
            s = short_text(m.group(0), 280)
            key = normalize_text(s)
            if key and key not in seen:
                seen.add(key)
                snippets.append(s)
    return snippets[:200]


def extract_urls_from_text(text: str) -> list[str]:
    candidates = re.findall(r'https?://[^\s"\'<>\\]+', text)
    candidates += re.findall(r'["\'](/[^"\']*(?:autocons|potencia|datos|api)[^"\']*)["\']', text, flags=re.I)
    out = []
    for c in candidates:
        c = c.strip().rstrip(";,)")
        out.append(c)
    return sorted(dict.fromkeys(out))


# =========================================================
# PAGE / JS DISCOVERY
# =========================================================
@st.cache_data(show_spinner=False, ttl=1800)
def fetch_text(url: str) -> tuple[int | None, str, str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        return int(response.status_code), response.headers.get("content-type", ""), response.text
    except Exception as exc:
        return None, "", str(exc)


def discover_page_assets(page_url: str) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    status, content_type, html = fetch_text(page_url)
    page_diag = pd.DataFrame(
        [
            {
                "page_url": page_url,
                "http_status": status,
                "content_type": content_type,
                "html_chars": len(html or ""),
                "html_preview": short_text(html, 240),
            }
        ]
    )
    script_urls = extract_script_urls(html, page_url)
    link_urls = extract_link_urls(html, page_url)
    js_urls = [u for u in script_urls + link_urls if is_probable_js_url(u)]
    assets_rows = []
    snippets = []
    for js_url in sorted(dict.fromkeys(js_urls)):
        js_status, js_content_type, js_text = fetch_text(js_url)
        asset_snippets = find_strings_of_interest(js_text) if js_status and 200 <= js_status < 300 else []
        discovered_urls = extract_urls_from_text(js_text) if asset_snippets else []
        snippets.extend(asset_snippets)
        assets_rows.append(
            {
                "asset_url": js_url,
                "http_status": js_status,
                "content_type": js_content_type,
                "chars": len(js_text or ""),
                "interesting_snippets": len(asset_snippets),
                "discovered_urls": ", ".join(discovered_urls[:12]),
            }
        )
    assets_df = pd.DataFrame(assets_rows)
    return page_diag, assets_df, snippets


def candidate_urls_from_discovery(assets_df: pd.DataFrame, snippets: list[str]) -> list[str]:
    urls: list[str] = []
    if not assets_df.empty and "discovered_urls" in assets_df.columns:
        for cell in assets_df["discovered_urls"].dropna().astype(str):
            for part in [x.strip() for x in cell.split(",") if x.strip()]:
                urls.append(part)
    for s in snippets:
        urls.extend(extract_urls_from_text(s))
    # Canonicalize obvious relative paths
    normalized = []
    for u in urls:
        if u.startswith("/"):
            normalized.append(absolute_url("https://www.ree.es", u))
        else:
            normalized.append(u)
    return sorted(dict.fromkeys(normalized))


# =========================================================
# PUBLIC API PROBING
# =========================================================
@st.cache_data(show_spinner=False, ttl=900)
def probe_api_endpoint(
    category: str,
    widget: str,
    start_day_iso: str,
    end_day_iso: str,
    time_trunc: str,
    peninsular: bool,
) -> dict:
    params = {
        "start_date": f"{start_day_iso}T00:00",
        "end_date": f"{end_day_iso}T23:59",
        "time_trunc": time_trunc,
    }
    if peninsular:
        params.update(PENINSULAR_PARAMS)

    endpoint = f"{REE_API_BASE}/{category}/{widget}"
    try:
        response = requests.get(endpoint, params=params, headers=HEADERS, timeout=60)
        body = response.text
        result = {
            "category": category,
            "widget": widget,
            "time_trunc": time_trunc,
            "scope": "Peninsular" if peninsular else "No geo params",
            "http_status": int(response.status_code),
            "ok": bool(response.ok),
            "url": response.url,
            "content_type": response.headers.get("content-type", ""),
            "body_preview": short_text(body, 260),
            "included_items": None,
            "data_items": None,
            "parsed_rows": 0,
            "titles": "",
        }
        try:
            payload = response.json()
            included = payload.get("included", []) or []
            result["included_items"] = len(included)
            result["data_items"] = len(payload.get("data", []) or [])
            rows = parse_api_payload(payload)
            result["parsed_rows"] = len(rows)
            if not rows.empty:
                result["titles"] = ", ".join(sorted(rows["title"].dropna().astype(str).unique().tolist()))[:700]
        except Exception:
            pass
        return result
    except Exception as exc:
        return {
            "category": category,
            "widget": widget,
            "time_trunc": time_trunc,
            "scope": "Peninsular" if peninsular else "No geo params",
            "http_status": None,
            "ok": False,
            "url": endpoint + "?" + urlencode(params),
            "content_type": "",
            "body_preview": str(exc),
            "included_items": None,
            "data_items": None,
            "parsed_rows": 0,
            "titles": "",
        }


def parse_api_payload(payload: dict) -> pd.DataFrame:
    rows = []
    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = str(attrs.get("title") or item.get("id") or "").strip()
        for value in attrs.get("values", []) or []:
            dt = pd.to_datetime(value.get("datetime"), utc=True, errors="coerce")
            val = pd.to_numeric(value.get("value"), errors="coerce")
            if pd.isna(dt) or pd.isna(val):
                continue
            rows.append(
                {
                    "datetime": dt.tz_convert("Europe/Madrid").tz_localize(None),
                    "title": title,
                    "value": float(val),
                }
            )
    return pd.DataFrame(rows)


def run_candidate_probe_matrix(start_day: date, end_day: date) -> pd.DataFrame:
    rows = []
    for category, widget in CANDIDATE_WIDGETS:
        for time_trunc in TIME_TRUNCS:
            for peninsular in [True, False]:
                rows.append(
                    probe_api_endpoint(
                        category=category,
                        widget=widget,
                        start_day_iso=start_day.isoformat(),
                        end_day_iso=end_day.isoformat(),
                        time_trunc=time_trunc,
                        peninsular=peninsular,
                    )
                )
    return pd.DataFrame(rows)


def probe_discovered_urls(
    urls: list[str],
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    rows = []
    for raw_url in urls[:80]:
        url = raw_url
        parsed = urlparse(url)
        if not parsed.scheme:
            continue

        # If discovered URL appears to be an apidatos call, enrich it with date params.
        query = parse_qs(parsed.query)
        if "apidatos.ree.es" in parsed.netloc:
            query.setdefault("start_date", [f"{start_day.isoformat()}T00:00"])
            query.setdefault("end_date", [f"{end_day.isoformat()}T23:59"])
            query.setdefault("time_trunc", ["month"])
            # Probe both as-is and with peninsular if absent
            url = parsed._replace(query=urlencode({k: v[-1] for k, v in query.items()})).geturl()

        status, content_type, body = fetch_text(url)
        parsed_rows = 0
        titles = ""
        try:
            payload = json.loads(body)
            data_rows = parse_api_payload(payload)
            parsed_rows = len(data_rows)
            if not data_rows.empty:
                titles = ", ".join(sorted(data_rows["title"].dropna().astype(str).unique().tolist()))[:700]
        except Exception:
            pass

        rows.append(
            {
                "discovered_url": url,
                "http_status": status,
                "content_type": content_type,
                "parsed_rows": parsed_rows,
                "titles": titles,
                "body_preview": short_text(body, 220),
            }
        )
    return pd.DataFrame(rows)


# =========================================================
# CHART PREVIEW
# =========================================================
def working_payload_preview(results: pd.DataFrame, start_day: date, end_day: date) -> tuple[pd.DataFrame, alt.Chart | None]:
    if results.empty:
        return pd.DataFrame(), None
    ok = results[results["parsed_rows"].fillna(0).astype(float) > 0].copy()
    if ok.empty:
        return pd.DataFrame(), None

    first = ok.iloc[0]
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": first["time_trunc"],
    }
    if first["scope"] == "Peninsular":
        params.update(PENINSULAR_PARAMS)

    endpoint = f"{REE_API_BASE}/{first['category']}/{first['widget']}"
    try:
        response = requests.get(endpoint, params=params, headers=HEADERS, timeout=60)
        payload = response.json()
        df = parse_api_payload(payload)
    except Exception:
        return pd.DataFrame(), None

    if df.empty:
        return df, None
    df["month"] = pd.to_datetime(df["datetime"]).dt.to_period("M").dt.to_timestamp()
    df = (
        df.groupby(["month", "title"], as_index=False)["value"]
        .sum()
        .sort_values(["month", "title"])
        .reset_index(drop=True)
    )
    chart = alt.Chart(df).mark_line(point=True, strokeWidth=2.5).encode(
        x=alt.X("month:T", title="Month", axis=alt.Axis(format="%b-%Y", labelAngle=-35)),
        y=alt.Y("value:Q", title="Value"),
        color=alt.Color("title:N", title="Series"),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%b-%Y"),
            alt.Tooltip("title:N", title="Series"),
            alt.Tooltip("value:Q", title="Value", format=",.2f"),
        ],
    ).properties(title=f"First working public-API probe: {first['category']}/{first['widget']}")
    return df, chart_style(chart, height=430)


# =========================================================
# PAGE
# =========================================================
section("REE autoconsumo source discovery — new test", "🔎")
st.caption(
    "Objective: identify the data source behind REE's Autoconsumo installed-capacity chart, "
    "because the public `generacion/potencia-instalada` endpoint is returning HTTP 500 in our app tests."
)

c1, c2 = st.columns(2)
with c1:
    start_day = st.date_input(
        "Probe start date",
        value=date(2026, 1, 1),
        min_value=date(2025, 1, 1),
        max_value=date.today(),
    )
with c2:
    end_day = st.date_input(
        "Probe end date",
        value=date.today(),
        min_value=date(2025, 1, 1),
        max_value=date.today(),
    )

if start_day > end_day:
    st.error("Start date must be before end date.")
    st.stop()

st.markdown(
    """
    **What this test does**
    1. Downloads the official REE Autoconsumo page.
    2. Reads its linked JavaScript assets.
    3. Searches those assets for strings/URLs containing `autoconsumo`, `potencia`, `apidatos`, `widget`, or `.json`.
    4. Probes a matrix of likely public API endpoints.
    5. Shows any endpoint that returns parseable data.
    """
)

run = st.button("Run autoconsumo source discovery", type="primary")
if not run:
    st.info("Press the button to run the source-discovery test.")
    st.stop()

# ---------------------------------------------------------
# A) Official page / asset discovery
# ---------------------------------------------------------
subsection("1) Official REE page and JavaScript asset discovery")
with st.spinner("Fetching the official REE Autoconsumo page and linked JS assets..."):
    page_diag, assets_df, snippets = discover_page_assets(REE_AUTOCONSUMO_PAGE)
    discovered_urls = candidate_urls_from_discovery(assets_df, snippets)

st.dataframe(page_diag, use_container_width=True, hide_index=True)

if assets_df.empty:
    card_note(
        "No JavaScript assets were detected from the page HTML in this environment. "
        "The public API probes below will still run.",
        tone="warn",
    )
else:
    st.dataframe(
        assets_df.sort_values(["interesting_snippets", "chars"], ascending=[False, False]),
        use_container_width=True,
        hide_index=True,
    )

with st.expander("Show snippets found inside page JS assets"):
    if snippets:
        st.dataframe(pd.DataFrame({"snippet": snippets}), use_container_width=True, hide_index=True)
    else:
        st.info("No relevant snippets were extracted from linked JS assets.")

with st.expander("Show URLs discovered from page JS assets"):
    if discovered_urls:
        st.dataframe(pd.DataFrame({"url": discovered_urls}), use_container_width=True, hide_index=True)
    else:
        st.info("No candidate URLs were extracted from page JS assets.")

# ---------------------------------------------------------
# B) Probe public APIs
# ---------------------------------------------------------
subsection("2) Public API candidate matrix")
with st.spinner("Probing candidate REE API endpoints..."):
    probe_df = run_candidate_probe_matrix(start_day, end_day)

summary_cols = [
    "category",
    "widget",
    "scope",
    "time_trunc",
    "http_status",
    "ok",
    "included_items",
    "data_items",
    "parsed_rows",
]
st.dataframe(
    probe_df[summary_cols].sort_values(["parsed_rows", "http_status"], ascending=[False, True]),
    use_container_width=True,
    hide_index=True,
)

working = probe_df[probe_df["parsed_rows"].fillna(0).astype(float) > 0].copy()
if working.empty:
    card_note(
        "No candidate public endpoint returned parseable rows. "
        "This strengthens the case that the web chart is fed by another route or by a frontend-internal API.",
        tone="warn",
    )
else:
    card_note(
        f"{len(working)} public-API probe(s) returned parseable rows.",
        tone="ok",
    )
    st.dataframe(
        working[
            [
                "category",
                "widget",
                "scope",
                "time_trunc",
                "http_status",
                "parsed_rows",
                "titles",
                "url",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

with st.expander("Show failure diagnostics for API probes"):
    failures = probe_df[probe_df["parsed_rows"].fillna(0).astype(float) == 0].copy()
    st.dataframe(
        failures[
            [
                "category",
                "widget",
                "scope",
                "time_trunc",
                "http_status",
                "body_preview",
                "url",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

# ---------------------------------------------------------
# C) Probe discovered URLs from JS assets
# ---------------------------------------------------------
subsection("3) Probe URLs discovered from REE frontend assets")
if not discovered_urls:
    st.info("No discovered URLs available to probe.")
else:
    with st.spinner("Probing URLs discovered from the frontend assets..."):
        discovered_probe_df = probe_discovered_urls(discovered_urls, start_day, end_day)
    st.dataframe(
        discovered_probe_df.sort_values(["parsed_rows", "http_status"], ascending=[False, True]),
        use_container_width=True,
        hide_index=True,
    )
    discovered_working = discovered_probe_df[
        discovered_probe_df["parsed_rows"].fillna(0).astype(float) > 0
    ]
    if not discovered_working.empty:
        card_note(
            f"{len(discovered_working)} discovered frontend URL(s) returned parseable JSON rows.",
            tone="ok",
        )

# ---------------------------------------------------------
# D) Preview chart if any candidate public endpoint works
# ---------------------------------------------------------
subsection("4) Preview of first working public-API payload")
preview_df, preview_chart = working_payload_preview(probe_df, start_day, end_day)
if preview_chart is None:
    st.info("No working public endpoint was found, so no preview chart is available.")
else:
    st.altair_chart(preview_chart, use_container_width=True)
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

st.caption(
    "Once this test identifies the actual source behind REE's live Autoconsumo / installed-capacity chart, "
    "we can build the production parser and feed Day Ahead with live 2026 Peninsular utility-scale PV capacity."
)
