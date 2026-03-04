import streamlit as st
from components import setup_page, page_header, narrative

st.set_page_config(
    page_title="Ed-Fi Interoperability Lakehouse",
    page_icon="🏫",
    layout="wide",
)

setup_page()

page_header(
    "Ed-Fi Interoperability Lakehouse",
    "A unified analytics layer for multi-district K-12 data",
)

narrative(
    "Select <b>Executive Overview</b> from the sidebar to explore the dashboard, "
    "or navigate directly to any analytics page."
)

st.info("👈 Use the sidebar navigation to explore the dashboard.")
