from __future__ import annotations

from datetime import date
from urllib.parse import urlencode
import json
import re

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Test | REE installed-capacity API diagnostics", layout="wide")

REE_BASE = "https://apidatos.ree.es/es/datos"
WIDGET = "generacion/potencia-instalada"

PENINSULAR = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

# This test intentionally probes several variants.
# It is not declaring that all combinations are officially valid;
# the goal is to see which one REE currently accepts in the deployed app environment.
GEO_VARIANTS = {
    "Peninsular params used elsewhere in the app": PENINSULAR,
    "No geo params": {},
}

TIME_TRUNCS = ["month", "year"]

def section(title: str) -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #0F766E 0%, #10B981 55%, #C7F0DD 100%);
            color: white;
            padding: 14px 18px;
            border-radius: 14px;
            font-weight: 850;
            font-size: 1.25rem;
            margin-top: 8px;
            margin-bottom: 14px;
        ">🧪 {title}</div>
        """,
        unsafe_allow_html=True,
    )

def short_text(text: str, limit: int = 360) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text[:limit] + ("…" if len(text) > limit else "")

@st.cache_data(show_spinner=False, ttl=900)
def run_probe(start_day: date, end_day: date, time_trunc: str, geo_name: str, geo_params_json: str) -> dict:
    geo_params = json.loads(geo_params_json)
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": time_trunc,
        **geo_params,
    }
    url = f"{REE_BASE}/{WIDGET}?{urlencode(params)}"
    try:
        resp = requests.get(
            f"{REE_BASE}/{WIDGET}",
            params=params,
            timeout=60,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        body = resp.text
        out = {
            "geo_variant": geo_name,
            "time_trunc": time_trunc,
            "start": start_day.isoformat(),
            "end": end_day.isoformat(),
            "http_status": resp.status_code,
            "ok": bool(resp.ok),
            "content_type": resp.headers.get("content-type", ""),
            "url": resp.url,
            "body_preview": short_text(body),
            "included_items": None,
            "data_items": None,
            "parsed_rows": 0,
            "titles": "",
        }
        try:
            payload = resp.json()
            out["included_items"] = len(payload.get("included", []) or [])
            out["data_items"] = len(payload.get("data", []) or [])
            rows = []
            titles = []
            for item in payload.get("included", []) or []:
                attrs = item.get("attributes", {}) or {}
                title = str(attrs.get("title") or item.get("id") or "").strip()
                if title:
                    titles.append(title)
                for val in attrs.get("values", []) or []:
                    dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
                    value = pd.to_numeric(val.get("value"), errors="coerce")
                    if pd.notna(dt) and pd.notna(value):
                        rows.append((title, dt, value))
            out["parsed_rows"] = len(rows)
            out["titles"] = ", ".join(sorted(set(titles)))[:700]
        except Exception:
            pass
        return out
    except Exception as exc:
        return {
            "geo_variant": geo_name,
            "time_trunc": time_trunc,
            "start": start_day.isoformat(),
            "end": end_day.isoformat(),
            "http_status": None,
            "ok": False,
            "content_type": "",
            "url": url,
            "body_preview": str(exc),
            "included_items": None,
            "data_items": None,
            "parsed_rows": 0,
            "titles": "",
        }

def probe_matrix(end_day: date) -> pd.DataFrame:
    month_start = date(end_day.year, end_day.month, 1)
    previous_month_end = month_start - pd.Timedelta(days=1)
    windows = [
        ("Selected month MTD", month_start, end_day),
        ("Selected month full calendar month", month_start, pd.Timestamp(month_start).to_period("M").end_time.date()),
        ("Previous closed month", previous_month_end.replace(day=1), previous_month_end.date() if hasattr(previous_month_end, "date") else previous_month_end),
        ("Jan-to-selected-end 2026", date(2026, 1, 1), end_day),
    ]
    rows = []
    for window_name, start_day, finish_day in windows:
        start_day = pd.Timestamp(start_day).date()
        finish_day = pd.Timestamp(finish_day).date()
        for trunc in TIME_TRUNCS:
            for geo_name, geo_params in GEO_VARIANTS.items():
                result = run_probe(start_day, finish_day, trunc, geo_name, json.dumps(geo_params, sort_keys=True))
                result["window"] = window_name
                rows.append(result)
    return pd.DataFrame(rows)

section("REE installed-capacity API diagnostics")
st.caption(
    "This test isolates the REE public endpoint used for installed capacity and shows which exact "
    "date/geo/time-trunc combinations return data versus HTTP 500."
)

end_day = st.date_input(
    "Reference end date",
    value=date.today(),
    min_value=date(2026, 1, 1),
    max_value=date.today(),
)

if st.button("Run REE diagnostic matrix", type="primary"):
    st.session_state["ree_probe_run"] = True

if not st.session_state.get("ree_probe_run", False):
    st.info("Press the button to execute the diagnostic matrix.")
    st.stop()

with st.spinner("Testing REE installed-capacity endpoint variants..."):
    df = probe_matrix(end_day)

st.subheader("1) Result matrix")
show_cols = [
    "window",
    "geo_variant",
    "time_trunc",
    "http_status",
    "ok",
    "included_items",
    "data_items",
    "parsed_rows",
]
st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

ok_rows = df[df["parsed_rows"] > 0].copy()
error_rows = df[df["parsed_rows"] == 0].copy()

if not ok_rows.empty:
    st.success("At least one API variant returned parseable installed-capacity rows.")
    st.subheader("2) Working variants")
    st.dataframe(
        ok_rows[["window", "geo_variant", "time_trunc", "http_status", "parsed_rows", "titles", "url"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.error("No tested public-API variant returned parseable installed-capacity rows.")

st.subheader("3) Failure diagnostics")
st.dataframe(
    error_rows[["window", "geo_variant", "time_trunc", "http_status", "body_preview", "url"]],
    use_container_width=True,
    hide_index=True,
)

with st.expander("Show full diagnostic payload table"):
    st.dataframe(df, use_container_width=True, hide_index=True)

st.caption(
    "Interpretation: if the REE website renders May-2026 installed-capacity charts while all public "
    "apidatos variants above return 500/no rows, the website is likely using a different backend path, "
    "a cached/precomputed source, or an API variant not exposed by the public endpoint used here."
)
