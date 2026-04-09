import streamlit as st

pwd = st.text_input("Password", type="password")

if pwd != st.secrets["email_admin_password"]:
    st.stop()
