import streamlit as st
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOGO_PATH = BASE_DIR / "data" / "nexwell-power-.jpg"

if LOGO_PATH.exists():
    st.sidebar.image(str(LOGO_PATH), width=140)   # prueba 140, 160 o 180

st.set_page_config(
    page_title="Spanish Energy Market App",
    layout="wide",
    initial_sidebar_state="expanded"
)

pg = st.navigation([
    st.Page("pages/0_Home.py", title="Home", icon="🏠"),
    st.Page("pages/1_Day_Ahead.py", title="Day Ahead", icon="⚡"),
    st.Page("pages/2_Forward_Market.py", title="Forward Market", icon="📈"),
    st.Page("pages/3_BESS.py", title="BESS", icon="🔋"),
    st.Page("pages/4_Email_Report.py", title="Email Report", icon="📧"),
    st.Page("pages/5_MIBGAS.py", title="MIBGAS", icon="🔥"),
    st.Page("pages/test.py", title="test", icon="🧪"),
])

pg.run()
