"""Page structure helpers: headers, sections, narratives, sidebar branding."""

import streamlit as st
from components.theme import inject_css


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
    """Set up shared page elements: CSS, sidebar branding, and footer.

    Call this at the top of every page (including app.py) so the sidebar
    is consistent regardless of which page the user navigates to.
    """
    inject_css()

    # Default filter values (all districts) so pages don't break
    if "district_filter" not in st.session_state:
        st.session_state["district_filter"] = None
    if "source_filter" not in st.session_state:
        st.session_state["source_filter"] = None

    with st.sidebar:
        # -- Fixed footer with GitHub + attribution --
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
