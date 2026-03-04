"""Design tokens and global CSS for the Ed-Fi Lakehouse dashboard."""

import streamlit as st

COLORS: dict[str, str] = {
    "primary": "#0D7377",
    "primary_light": "#14919B",
    "secondary": "#6C5CE7",
    "background": "#FAFBFC",
    "surface": "#FFFFFF",
    "surface_alt": "#F0F4F8",
    "text_primary": "#1A202C",
    "text_secondary": "#4A5568",
    "success": "#38A169",
    "warning": "#ED8936",
    "danger": "#E53E3E",
    "info": "#3182CE",
    "neutral": "#CBD5E0",
}


def inject_css() -> None:
    """Inject global CSS styles into the Streamlit app.

    Call this once at the top of the main app file, after ``set_page_config``.
    """
    st.markdown(
        f"""
        <style>
        /* ── Page layout ────────────────────────────────────── */
        .block-container {{
            padding-top: 1.5rem;
            padding-bottom: 1.5rem;
            padding-left: 2rem;
            padding-right: 2rem;
        }}

        /* ── Clean header ───────────────────────────────────── */
        header[data-testid="stHeader"] {{
            background: transparent;
        }}

        /* ── Sidebar styling ────────────────────────────────── */
        section[data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {COLORS["primary"]} 0%, #0a5c5f 100%);
        }}

        section[data-testid="stSidebar"] * {{
            color: #FFFFFF !important;
        }}

        section[data-testid="stSidebar"] a {{
            color: #FFFFFF !important;
            text-decoration: underline;
            opacity: 0.9;
        }}

        section[data-testid="stSidebar"] a:hover {{
            opacity: 1;
        }}

        /* Sidebar select-box text needs to stay dark for readability */
        section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] span {{
            color: {COLORS["text_primary"]} !important;
        }}

        /* ── Metric card ────────────────────────────────────── */
        .metric-card {{
            background: {COLORS["surface"]};
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
            transition: box-shadow 0.2s ease, transform 0.2s ease;
        }}

        .metric-card:hover {{
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            transform: translateY(-2px);
        }}

        .metric-value {{
            font-size: 2rem;
            font-weight: 700;
            color: {COLORS["text_primary"]};
            line-height: 1.2;
        }}

        .metric-label {{
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: {COLORS["text_secondary"]};
            margin-bottom: 0.25rem;
        }}

        .metric-delta {{
            font-size: 0.85rem;
            font-weight: 600;
            margin-top: 0.25rem;
        }}

        .metric-delta.positive {{
            color: {COLORS["success"]};
        }}

        .metric-delta.negative {{
            color: {COLORS["danger"]};
        }}

        .metric-delta.neutral {{
            color: {COLORS["text_secondary"]};
        }}

        /* ── Insight cards ──────────────────────────────────── */
        .insight-card {{
            border-radius: 8px;
            padding: 1rem 1.25rem;
            margin-bottom: 0.75rem;
            border-left: 4px solid;
        }}

        .insight-card.info {{
            border-left-color: {COLORS["info"]};
            background: #EBF8FF;
        }}

        .insight-card.success {{
            border-left-color: {COLORS["success"]};
            background: #F0FFF4;
        }}

        .insight-card.warning {{
            border-left-color: {COLORS["warning"]};
            background: #FFFAF0;
        }}

        .insight-card.danger {{
            border-left-color: {COLORS["danger"]};
            background: #FFF5F5;
        }}

        /* ── Section header ─────────────────────────────────── */
        .section-header {{
            border-left: 4px solid {COLORS["primary"]};
            padding-left: 0.75rem;
            margin-bottom: 1rem;
        }}

        .section-header h3 {{
            font-size: 1.15rem;
            font-weight: 700;
            color: {COLORS["text_primary"]};
            margin: 0;
        }}

        .section-header p {{
            font-size: 0.85rem;
            color: {COLORS["text_secondary"]};
            margin: 0.15rem 0 0 0;
        }}

        /* ── Page header ────────────────────────────────────── */
        .page-header {{
            margin-bottom: 1.5rem;
        }}

        .page-header h1 {{
            font-size: 1.75rem;
            font-weight: 700;
            color: {COLORS["text_primary"]};
            margin: 0;
        }}

        .page-header p {{
            font-size: 0.95rem;
            color: {COLORS["text_secondary"]};
            margin: 0.25rem 0 0 0;
        }}

        /* ── Narrative text ─────────────────────────────────── */
        .narrative {{
            font-size: 0.95rem;
            line-height: 1.6;
            color: {COLORS["text_secondary"]};
            margin-bottom: 1rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
