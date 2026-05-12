from datetime import date
import requests
import streamlit as st

st.set_page_config(page_title="REE Installed Capacity Window Test", layout="wide")
st.title("REE Installed Capacity - Window Test")

REE_API_BASE = "https://apidatos.ree.es/es/datos"

REE_PENINSULAR_PARAMS = {
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741",
}


def test_window(label: str, start_day: date, end_day: date):
    url = f"{REE_API_BASE}/generacion/potencia-instalada"

    params = {
        "start_date": f"{start_day.isoformat()}T00:00",
        "end_date": f"{end_day.isoformat()}T23:59",
        "time_trunc": "month",
        **REE_PENINSULAR_PARAMS,
    }

    try:
        resp = requests.get(url, params=params, timeout=60)

        st.subheader(label)

        st.write(
            {
                "status_code": resp.status_code,
                "content_type": resp.headers.get("Content-Type"),
                "url_called": resp.url,
            }
        )

        try:
            payload = resp.json()

            st.success("Valid JSON returned")

            st.write(
                {
                    "top_level_keys": list(payload.keys()),
                    "included_items": len(payload.get("included", []) or []),
                }
            )

            st.markdown("#### JSON preview")
            st.json(payload)

        except Exception as e:
            st.error(f"No valid JSON: {type(e).__name__}: {e}")

            st.markdown("#### Response preview")
            st.code(resp.text[:3000])

    except Exception as e:
        st.error(f"Request failed: {type(e).__name__}: {e}")


st.markdown("### Run endpoint window tests")

if st.button("Run window tests", type="primary"):
    test_window(
        "1. Jan-Apr 2026",
        date(2026, 1, 1),
        date(2026, 4, 30),
    )

    test_window(
        "2. Jan-12 May 2026",
        date(2026, 1, 1),
        date(2026, 5, 12),
    )

    test_window(
        "3. April 2026 only",
        date(2026, 4, 1),
        date(2026, 4, 30),
    )
