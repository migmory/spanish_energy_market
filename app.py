import streamlit as st
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOGO_PATH = BASE_DIR / "data" / "nexwell-power-.jpg"

if LOGO_PATH.exists():
    st.sidebar.image(str(LOGO_PATH), width=140)

st.set_page_config(
    page_title="Spanish Energy Market App",
    layout="wide",
    initial_sidebar_state="expanded",
)

pg = st.navigation(
    {
        "Overview": [
            st.Page("pages/0_Home.py", title="Home", icon="🏠"),
        ],
        "Reports": [
            st.Page("pages/0_Monthly_Market_Report.py", title="Monthly Market Report", icon="📊"),
            st.Page("pages/0_Weekly_Market_Report.py", title="Weekly Market Report", icon="🗓️"),
        ],
        "Markets & Analytics": [
            st.Page("pages/1_Day_Ahead.py", title="Day Ahead", icon="⚡"),
            st.Page("pages/2_Forward_Market.py", title="Forward Market", icon="📈"),
            st.Page("pages/3_BESS.py", title="BESS", icon="🔋"),
            st.Page("pages/5_MIBGAS.py", title="MIBGAS", icon="🔥"),
            st.Page("pages/9_Day-Ahead_forecast.py", title="Day-Ahead forecast", icon="🎯"),
            st.Page("pages/test.py", title="test", icon="🧪"),
            st.Page("pages/test2.py", title="test2", icon="🧪"),
        ],
        "Operational Parks": [
            st.Page("pages/6_IS2.py", title="IS2", icon="☀️"),
        ],
        "Hedging": [
            st.Page("pages/7_PPA_DASS_Settlements.py", title="Solar PPA & DASS Settlements", icon="🛡️"),
            st.Page("pages/8_Hybrid_PPA.py", title="Hybrid PPA (solar + BESS) Settlements", icon="🔰"),
        ],
    }
)

pg.run()
