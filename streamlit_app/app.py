import streamlit as st

st.set_page_config(
    page_title="Ed-Fi Interoperability Lakehouse",
    page_icon="🏫",
    layout="wide",
)

# --- Sidebar ---
with st.sidebar:
    st.title("Ed-Fi Interoperability Lakehouse")
    st.caption("A unified analytics layer for multi-district K-12 data")
    st.markdown("---")

    # District filter
    district = st.selectbox(
        "District",
        ["All Districts", "Grand Bend ISD (Ed-Fi)", "Riverside USD (OneRoster)"],
    )

    district_filter = {
        "All Districts": None,
        "Grand Bend ISD (Ed-Fi)": "Grand Bend ISD",
        "Riverside USD (OneRoster)": "Riverside USD",
    }[district]

    # Also store the source-system mapping for filtering tables without district_name
    source_filter = {
        "All Districts": None,
        "Grand Bend ISD (Ed-Fi)": "edfi",
        "Riverside USD (OneRoster)": "oneroster",
    }[district]

    st.session_state["district_filter"] = district_filter
    st.session_state["source_filter"] = source_filter

    st.markdown("---")
    st.markdown("**Built by:** Vidya")
    st.markdown("**Tech:** PySpark | dbt | DuckDB | Streamlit")

# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs([
    "Classroom Insights",
    "District Intelligence",
    "Data Quality Simulator",
    "Pipeline & Governance",
])

with tab1:
    from tabs.classroom_insights import render
    render()

with tab2:
    from tabs.district_intelligence import render as render_di
    render_di()

with tab3:
    from tabs.dq_simulator import render as render_dq
    render_dq()

with tab4:
    from tabs.pipeline_governance import render as render_pg
    render_pg()
