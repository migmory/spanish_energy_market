import streamlit as st
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOGO_PATH = BASE_DIR / "data" / "nexwell-power.jpg"

if LOGO_PATH.exists():
    st.logo(str(LOGO_PATH))
st.set_page_config(
    page_title="Spanish Energy Market App",
    layout="wide",
    initial_sidebar_state="expanded"
)

pg = st.navigation([
    st.Page("pages/0_Home.py", title="Home"),
    st.Page("pages/1_Day_Ahead.py", title="Day Ahead"),
    st.Page("pages/2_Forward_Market.py", title="Forward Market"),
    st.Page("pages/3_BESS.py", title="BESS"),
    st.Page("pages/4_Email_Report.py", title="Email Report"),
    st.Page("pages/5_MIBGAS.py", title="MIBGAS"),
    st.Page("pages/test.py", title="test"),
])

pg.run()
