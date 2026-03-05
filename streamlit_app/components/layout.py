"""Page structure helpers: headers, sections, narratives, sidebar branding."""

import streamlit as st
from components.theme import inject_css
from components.navigation import init_nav_state


def page_header(title: str, subtitle: str) -> None:
    """Render a styled page header with title and subtitle."""
    st.markdown(
        f"""
        <div class="page-header">
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section(title: str, description: str | None = None) -> None:
    """Render a section header with colored left border."""
    desc_html = f"<p>{description}</p>" if description else ""
    st.markdown(
        f"""
        <div class="section-header">
            <h3>{title}</h3>
            {desc_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def narrative(text: str) -> None:
    """Render styled narrative text explaining a chart or section."""
    st.markdown(
        f'<div class="narrative">{text}</div>',
        unsafe_allow_html=True,
    )


def setup_page() -> None:
    """Set up shared page elements: CSS, sidebar filters, and footer.

    Call this at the top of every page (including app.py) so the sidebar
    is consistent regardless of which page the user navigates to.
    """
    inject_css()
    init_nav_state()

    with st.sidebar:
        # ── District filter ──
        from db import query

        districts = query(
            "SELECT DISTINCT district_name FROM gold.dim_school "
            "WHERE district_name IS NOT NULL ORDER BY district_name"
        )["district_name"].tolist()

        selected_district = st.selectbox(
            "District",
            options=["All Districts"] + districts,
            index=0,
            key="sidebar_district",
        )
        if selected_district == "All Districts":
            st.session_state["nav_district"] = None
        else:
            st.session_state["nav_district"] = selected_district

        # ── School filter (filtered by district) ──
        if st.session_state.get("nav_district"):
            d = st.session_state["nav_district"]
            schools = query(
                f"SELECT school_id, school_name FROM gold.dim_school "
                f"WHERE district_name = '{d}' ORDER BY school_name"
            )
        else:
            schools = query(
                "SELECT school_id, school_name FROM gold.dim_school "
                "WHERE school_name IS NOT NULL ORDER BY school_name"
            )

        school_options = ["All Schools"] + schools["school_name"].tolist()
        selected_school = st.selectbox(
            "School",
            options=school_options,
            index=0,
            key="sidebar_school",
        )
        if selected_school == "All Schools":
            st.session_state["nav_school_id"] = None
            st.session_state["nav_school_name"] = None
        else:
            row = schools[schools["school_name"] == selected_school].iloc[0]
            st.session_state["nav_school_id"] = row["school_id"]
            st.session_state["nav_school_name"] = selected_school

        # ── Subject filter ──
        st.selectbox(
            "Subject",
            options=["All", "Math", "ELA", "Science"],
            index=0,
            key="subject_filter",
        )

        # ── Footer ──
        st.markdown(
            """
            <div class="sidebar-footer">
                <a href="https://github.com/SrividyaKirti/ED-FI_lakehouse" target="_blank">
                    <img src="https://img.shields.io/badge/GitHub-Repo-white?logo=github" alt="GitHub Repo">
                </a>
                <div style="margin-top: 0.5rem;">
                    <small><b>Built by</b> Vidya Kirti</small><br>
                    <small>PySpark &bull; dbt &bull; DuckDB &bull; Airflow &bull; Streamlit</small>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
