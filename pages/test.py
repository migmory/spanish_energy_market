from datetime import date
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="REE Installed Capacity Test", layout="wide")
st.title("REE Installed Capacity - 2026 API Test")

REE_API_BASE = "https://apidatos.ree.es/es/datos"

REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}

LOCAL_MIX_TECH_MAP = {
    "Hidráulica": "Hydro",
    "Hidroeólica": "Other renewables",
    "Turbinación bombeo": "Pumped hydro",
    "Nuclear": "Nuclear",
    "Carbón": "Coal",
    "Fuel + Gas": "Fuel + Gas",
    "Turbina de vapor": "Steam turbine",
    "Ciclo combinado": "CCGT",
    "Eólica": "Wind",
    "Solar fotovoltaica": "Solar PV",
    "Solar térmica": "Solar thermal",
    "Otras renovables": "Other renewables",
    "Cogeneración": "CHP",
    "Residuos no renovables": "Other non-renewables",
    "Residuos renovables": "Biomass",
    "Biogás": "Biogas",
    "Biomasa": "Biomass",
}


def fetch_installed_capacity(start_day: date, end_day: date):
    url = f"{REE_API_BASE}/generacion/potencia-instalada"
    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "month",
        **REE_PENINSULAR_PARAMS,
    }

    resp = requests.get(url, params=params, timeout=60)

    debug_info = {
        "status_code": resp.status_code,
        "url_called": resp.url,
        "content_type": resp.headers.get("Content-Type"),
        "response_preview": resp.text[:5000],
    }

    try:
        payload = resp.json()
        debug_info["json_ok"] = True
        debug_info["json_error"] = None
    except Exception as e:
        payload = None
        debug_info["json_ok"] = False
        debug_info["json_error"] = f"{type(e).__name__}: {e}"

    return resp, payload, debug_info


def parse_ree_included_series(payload: dict) -> pd.DataFrame:
    rows = []

    for item in payload.get("included", []) or []:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or item.get("id")

        for val in attrs.get("values", []) or []:
            dt = pd.to_datetime(val.get("datetime"), utc=True, errors="coerce")
            if pd.isna(dt):
                continue

            dt = dt.tz_convert("Europe/Madrid").tz_localize(None)

            rows.append(
                {
                    "datetime": dt,
                    "title": str(title).strip(),
                    "value": pd.to_numeric(val.get("value"), errors="coerce"),
                }
            )

    return pd.DataFrame(rows)


def postprocess_capacity(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "capacity_mw"])

    out = df.copy()
    out["datetime"] = (
        pd.to_datetime(out["datetime"], errors="coerce")
        .dt.to_period("M")
        .dt.to_timestamp()
    )
    out["technology"] = out["title"].map(
        lambda x: LOCAL_MIX_TECH_MAP.get(str(x).strip(), str(x).strip())
    )
    out["capacity_mw"] = pd.to_numeric(out["value"], errors="coerce")

    out = out.dropna(subset=["datetime", "technology", "capacity_mw"]).copy()

    return (
        out[["datetime", "technology", "capacity_mw"]]
        .sort_values(["datetime", "technology"])
        .reset_index(drop=True)
    )


st.markdown("### Test parameters")

c1, c2 = st.columns(2)

with c1:
    start_day = st.date_input("Start date", value=date(2026, 1, 1))

with c2:
    end_day = st.date_input("End date", value=date.today())

run = st.button("Run REE test", type="primary")

if run:
    try:
        resp, payload, debug_info = fetch_installed_capacity(start_day, end_day)

        st.markdown("### 1. Raw HTTP debug")
        st.write(debug_info)

        if not resp.ok:
            st.error("REE returned an HTTP error.")
            st.stop()

        if payload is None:
            st.error("REE did not return valid JSON.")
            st.stop()

        st.markdown("### 2. Top-level JSON structure")
        st.write(
            {
                "top_level_keys": list(payload.keys()),
                "included_items": len(payload.get("included", []) or []),
                "has_data_key": "data" in payload,
            }
        )

        st.markdown("### 3. Raw JSON")
        st.json(payload)

        parsed = parse_ree_included_series(payload)

        st.markdown("### 4. Parsed raw series")
        st.write(
            {
                "parsed_rows": len(parsed),
                "unique_titles": (
                    sorted(parsed["title"].dropna().unique().tolist())
                    if not parsed.empty
                    else []
                ),
            }
        )
        st.dataframe(parsed, use_container_width=True)

        processed = postprocess_capacity(parsed)

        st.markdown("### 5. Final installed-capacity dataframe")
        st.write(
            {
                "final_rows": len(processed),
                "months_returned": (
                    sorted(processed["datetime"].dt.strftime("%Y-%m").unique().tolist())
                    if not processed.empty
                    else []
                ),
                "technologies_returned": (
                    sorted(processed["technology"].dropna().unique().tolist())
                    if not processed.empty
                    else []
                ),
            }
        )
        st.dataframe(processed, use_container_width=True)

        if processed.empty:
            st.warning(
                "REE answered with valid JSON, but no installed-capacity rows survived the parser."
            )
        else:
            st.success("REE installed-capacity data was returned and parsed correctly.")

    except Exception as e:
        st.error(f"Test failed with exception: {type(e).__name__}: {e}")
