"""Page structure helpers: headers, sections, narratives, sidebar branding."""

import streamlit as st


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


def sidebar_branding() -> None:
    """Render sidebar branding: title at top, project info fixed at bottom."""
    st.markdown("### Ed-Fi Lakehouse")
    st.caption("Interoperability Analytics Demo")


def sidebar_footer() -> None:
    """Render fixed footer at bottom of sidebar with GitHub link and attribution."""
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
