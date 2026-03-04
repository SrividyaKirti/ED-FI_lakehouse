import streamlit as st
from components import inject_css, sidebar_branding

st.set_page_config(
    page_title="Ed-Fi Interoperability Lakehouse",
    page_icon="🏫",
    layout="wide",
)

inject_css()

# --- Sidebar ---
with st.sidebar:
    sidebar_branding()
    st.markdown("---")

    # District filter (shared across all pages via session_state)
    district = st.selectbox(
        "Filter by District",
        ["All Districts", "Grand Bend ISD (Ed-Fi)", "Riverside USD (OneRoster)"],
    )

    district_filter = {
        "All Districts": None,
        "Grand Bend ISD (Ed-Fi)": "Grand Bend ISD",
        "Riverside USD (OneRoster)": "Riverside USD",
    }[district]

    source_filter = {
        "All Districts": None,
        "Grand Bend ISD (Ed-Fi)": "edfi",
        "Riverside USD (OneRoster)": "oneroster",
    }[district]

    st.session_state["district_filter"] = district_filter
    st.session_state["source_filter"] = source_filter

# --- Landing page content ---
from components import page_header, narrative

page_header(
    "Ed-Fi Interoperability Lakehouse",
    "A unified analytics layer for multi-district K-12 data",
)

narrative(
    "Select <b>Executive Overview</b> from the sidebar to explore the dashboard, "
    "or navigate directly to any analytics page."
)

st.info("👈 Use the sidebar navigation to explore the dashboard.")
