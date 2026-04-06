import streamlit as st

st.set_page_config(
    page_title="Spanish Energy Market App",
    layout="wide",
    initial_sidebar_state="expanded"
)

pg = st.navigation([
    st.Page("app.py", title="Home"),
    st.Page("pages/1_Day_Ahead.py", title="Day Ahead"),
    st.Page("pages/2_Forward_Market.py", title="Forward Market"),
    st.Page("pages/3_BESS.py", title="BESS"),
])

pg.run()
