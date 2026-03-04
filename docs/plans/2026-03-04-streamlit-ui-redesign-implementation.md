# Streamlit UI Redesign — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Transform the Streamlit app from a tab-based prototype into a polished multipage app with a custom component library, education-themed styling, and narrative-driven analytics.

**Architecture:** Multipage Streamlit app using the `pages/` directory convention. A `components/` package provides reusable UI helpers (metric cards, insight callouts, chart theming, page layout). Global CSS injected via `app.py`. All existing tab logic migrated to standalone page files with enhanced narratives and dynamic insights.

**Tech Stack:** Streamlit 1.41.0, Plotly 6.0.0, DuckDB 1.2.0, pandas, custom CSS via `st.markdown(unsafe_allow_html=True)`

**Design Doc:** `docs/plans/2026-03-04-streamlit-ui-redesign.md`

**Key data points (from actual DB):**
- 350 students, 14 schools, 23 CCSS Math standards
- 22 quarantined records, 94.1% pass rate
- Risk levels: 12 Medium, 130 Low
- Mastery: Meeting 220, Exceeding 94, Developing 160, Needs Intervention 30
- Silver: ~40K records, Gold: ~43K records

---

## Task 1: Create Component Library — Theme & CSS

**Files:**
- Create: `streamlit_app/components/__init__.py`
- Create: `streamlit_app/components/theme.py`

**Step 1: Create `components/theme.py` with design tokens and CSS**

```python
"""Global theme and CSS injection for the Ed-Fi Lakehouse app."""

import streamlit as st

# Design tokens
COLORS = {
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
}


def inject_css():
    """Inject global CSS styles. Call once in app.py."""
    st.markdown(
        """
        <style>
        /* --- Page layout --- */
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }

        /* --- Sidebar styling --- */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0D7377 0%, #0a5c5f 100%);
        }
        [data-testid="stSidebar"] * {
            color: #ffffff !important;
        }
        [data-testid="stSidebar"] .stSelectbox label {
            color: #e0f2f1 !important;
        }
        [data-testid="stSidebar"] a {
            color: #b2dfdb !important;
            text-decoration: none;
        }
        [data-testid="stSidebar"] a:hover {
            color: #ffffff !important;
            text-decoration: underline;
        }

        /* --- Metric card styling --- */
        .metric-card {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.25rem;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06);
            transition: box-shadow 0.2s;
        }
        .metric-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        .metric-card .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: #1A202C;
            line-height: 1.2;
        }
        .metric-card .metric-label {
            font-size: 0.85rem;
            color: #4A5568;
            margin-top: 0.25rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .metric-card .metric-delta {
            font-size: 0.8rem;
            margin-top: 0.25rem;
        }
        .metric-card .metric-delta.positive { color: #38A169; }
        .metric-card .metric-delta.negative { color: #E53E3E; }
        .metric-card .metric-delta.neutral { color: #4A5568; }

        /* --- Insight cards --- */
        .insight-card {
            border-radius: 8px;
            padding: 1rem 1.25rem;
            margin: 1rem 0;
            border-left: 4px solid;
        }
        .insight-card.info {
            background: #EBF8FF;
            border-left-color: #3182CE;
        }
        .insight-card.success {
            background: #F0FFF4;
            border-left-color: #38A169;
        }
        .insight-card.warning {
            background: #FFFAF0;
            border-left-color: #ED8936;
        }
        .insight-card.danger {
            background: #FFF5F5;
            border-left-color: #E53E3E;
        }
        .insight-card .insight-title {
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 0.25rem;
        }
        .insight-card .insight-body {
            font-size: 0.9rem;
            color: #4A5568;
            line-height: 1.5;
        }

        /* --- Section headers --- */
        .section-header {
            border-left: 4px solid #0D7377;
            padding-left: 1rem;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }
        .section-header h3 {
            font-size: 1.25rem;
            font-weight: 600;
            color: #1A202C;
            margin: 0;
        }
        .section-header p {
            font-size: 0.9rem;
            color: #4A5568;
            margin: 0.25rem 0 0 0;
        }

        /* --- Page header --- */
        .page-header {
            margin-bottom: 2rem;
        }
        .page-header h1 {
            font-size: 1.75rem;
            font-weight: 700;
            color: #1A202C;
            margin: 0;
        }
        .page-header p {
            font-size: 1.05rem;
            color: #4A5568;
            margin: 0.5rem 0 0 0;
        }

        /* --- Narrative text --- */
        .narrative {
            font-size: 0.95rem;
            color: #4A5568;
            line-height: 1.6;
            margin: 0.75rem 0;
            padding: 0.5rem 0;
        }

        /* --- Hide default Streamlit header padding for cleaner look --- */
        .stApp > header {
            background-color: transparent;
        }

        /* --- Styled containers --- */
        [data-testid="stVerticalBlock"] > div:has(> .metric-card) {
            gap: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
```

**Step 2: Create `components/__init__.py`**

```python
"""Reusable UI components for the Ed-Fi Lakehouse app."""

from components.theme import COLORS, inject_css
```

This will be extended in subsequent tasks as we add more component modules.

**Step 3: Verify CSS injection works**

Run: `cd streamlit_app && ../.venv/bin/streamlit run app.py`

Temporarily add `from components.theme import inject_css; inject_css()` at the top of `app.py` (after `set_page_config`) and verify the sidebar gets the teal gradient. Then revert — we'll do the full `app.py` rewrite in Task 3.

**Step 4: Commit**

```bash
git add streamlit_app/components/__init__.py streamlit_app/components/theme.py
git commit -m "feat: add theme module with design tokens and global CSS"
```

---

## Task 2: Create Component Library — Cards, Charts, Layout

**Files:**
- Create: `streamlit_app/components/cards.py`
- Create: `streamlit_app/components/charts.py`
- Create: `streamlit_app/components/layout.py`
- Modify: `streamlit_app/components/__init__.py`

**Step 1: Create `components/cards.py`**

```python
"""Card components: metric tiles, insight callouts, stat rows."""

import streamlit as st


def metric_card(label: str, value: str, delta: str | None = None, delta_direction: str = "neutral") -> None:
    """Render a styled metric card with optional delta indicator."""
    delta_html = ""
    if delta:
        delta_class = delta_direction  # "positive", "negative", or "neutral"
        delta_html = f'<div class="metric-delta {delta_class}">{delta}</div>'

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def insight_card(title: str, body: str, severity: str = "info") -> None:
    """Render a colored insight callout. severity: info/success/warning/danger."""
    st.markdown(
        f"""
        <div class="insight-card {severity}">
            <div class="insight-title">{title}</div>
            <div class="insight-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_row(metrics: list[dict]) -> None:
    """Render a horizontal row of metric cards.

    Each dict: {"label": str, "value": str, "delta": str|None, "delta_direction": str}
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            metric_card(
                label=m["label"],
                value=m["value"],
                delta=m.get("delta"),
                delta_direction=m.get("delta_direction", "neutral"),
            )
```

**Step 2: Create `components/charts.py`**

```python
"""Plotly chart theming and color constants."""

import plotly.graph_objects as go
from components.theme import COLORS

# Named color sequences for consistent chart styling
MASTERY_COLORS = {
    "Exceeding": "#38A169",
    "Meeting": "#6C5CE7",
    "Developing": "#ED8936",
    "Needs Intervention": "#E53E3E",
}

RISK_COLORS = {
    "High": "#E53E3E",
    "Medium": "#ED8936",
    "Low": "#38A169",
}

DISTRICT_COLORS = {
    "Grand Bend ISD": "#0D7377",
    "Riverside USD": "#6C5CE7",
}

LAYER_COLORS = {
    "Bronze": "#ED8936",
    "Silver": "#A0AEC0",
    "Gold": "#D69E2E",
    "Quarantined": "#E53E3E",
}

CURRICULUM_COLORS = {
    "A": "#0D7377",
    "B": "#6C5CE7",
}


def apply_theme(fig: go.Figure) -> go.Figure:
    """Apply consistent styling to a Plotly figure."""
    fig.update_layout(
        font=dict(family="Inter, system-ui, sans-serif", color=COLORS["text_primary"]),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#E2E8F0",
            borderwidth=1,
            font=dict(size=12),
        ),
        xaxis=dict(
            gridcolor="#EDF2F7",
            linecolor="#E2E8F0",
            zerolinecolor="#E2E8F0",
        ),
        yaxis=dict(
            gridcolor="#EDF2F7",
            linecolor="#E2E8F0",
            zerolinecolor="#E2E8F0",
        ),
    )
    fig.update_layout(
        modebar=dict(
            bgcolor="rgba(0,0,0,0)",
            color="#A0AEC0",
            activecolor=COLORS["primary"],
        ),
    )
    return fig
```

**Step 3: Create `components/layout.py`**

```python
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
    """Render sidebar branding: project info, GitHub link, tech stack."""
    st.markdown("### Ed-Fi Lakehouse")
    st.caption("Interoperability Analytics Demo")
    st.markdown("---")

    st.markdown(
        "[![GitHub](https://img.shields.io/badge/GitHub-Repo-white?logo=github)]"
        "(https://github.com/SrividyaKirti/ED-FI_lakehouse)"
    )

    st.markdown("---")
    st.markdown("**Built by** Vidya Kirti")
    st.markdown(
        "<small>PySpark &bull; dbt &bull; DuckDB<br>"
        "Airflow &bull; Streamlit</small>",
        unsafe_allow_html=True,
    )
```

**Step 4: Update `components/__init__.py` to re-export everything**

```python
"""Reusable UI components for the Ed-Fi Lakehouse app."""

from components.theme import COLORS, inject_css
from components.cards import metric_card, insight_card, stat_row
from components.charts import (
    apply_theme,
    MASTERY_COLORS,
    RISK_COLORS,
    DISTRICT_COLORS,
    LAYER_COLORS,
    CURRICULUM_COLORS,
)
from components.layout import page_header, section, narrative, sidebar_branding
```

**Step 5: Commit**

```bash
git add streamlit_app/components/
git commit -m "feat: add cards, charts, and layout component modules"
```

---

## Task 3: Update Config and Rewrite app.py for Multipage

**Files:**
- Modify: `streamlit_app/.streamlit/config.toml`
- Modify: `streamlit_app/app.py`

**Step 1: Update `.streamlit/config.toml`**

```toml
[browser]
gatherUsageStats = false

[theme]
primaryColor = "#0D7377"
backgroundColor = "#FAFBFC"
secondaryBackgroundColor = "#F0F4F8"
textColor = "#1A202C"
```

**Step 2: Rewrite `app.py`**

The app.py becomes the entry point that:
- Sets page config
- Injects global CSS
- Renders sidebar (branding + filters)
- Shows a redirect to Executive Overview

```python
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
# When the user navigates to a page from the sidebar, that page renders instead.
# This is the default "home" content.
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
```

**Step 3: Verify the multipage skeleton works**

Run: `cd streamlit_app && ../.venv/bin/streamlit run app.py`

Check: Sidebar should show teal gradient, branding section, GitHub badge, district filter, and the landing page content. No pages in sidebar yet (we haven't created the `pages/` dir).

**Step 4: Commit**

```bash
git add streamlit_app/.streamlit/config.toml streamlit_app/app.py
git commit -m "feat: rewrite app.py for multipage with themed sidebar"
```

---

## Task 4: Create Executive Overview Page

**Files:**
- Create: `streamlit_app/pages/1_📊_Executive_Overview.py`

**Step 1: Create the Executive Overview page**

This is the flagship landing page. It queries all key metrics dynamically and presents them with narrative context.

```python
"""Executive Overview — the first thing visitors see."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from db import query
from components import (
    page_header, section, narrative, stat_row, insight_card, apply_theme,
    MASTERY_COLORS, DISTRICT_COLORS, LAYER_COLORS, inject_css,
)

inject_css()

page_header(
    "Executive Overview",
    "Real-time analytics across two interoperable school districts",
)

# --- KPI Row ---
try:
    students = query("SELECT COUNT(*) as n FROM gold.dim_student")["n"].iloc[0]
    schools = query("SELECT COUNT(DISTINCT school_id) as n FROM gold.dim_school")["n"].iloc[0]
    standards = query("SELECT COUNT(DISTINCT standard_code) as n FROM gold.dim_standard")["n"].iloc[0]
    quarantined = query("SELECT COUNT(*) as n FROM gold.fact_dq_quarantine_log")["n"].iloc[0]
    pass_rate = round((1 - quarantined / max(students + quarantined, 1)) * 100, 1)

    risk_df = query("SELECT risk_level, COUNT(*) as cnt FROM gold.agg_early_warning GROUP BY risk_level")
    at_risk = risk_df[risk_df["risk_level"].isin(["High", "Medium"])]["cnt"].sum()
except Exception as exc:
    st.error(f"Failed to load overview metrics: {exc}")
    st.stop()

stat_row([
    {"label": "Students", "value": f"{students:,}"},
    {"label": "Schools", "value": str(schools)},
    {"label": "Standards Tracked", "value": str(standards)},
    {"label": "Pipeline Pass Rate", "value": f"{pass_rate}%", "delta": "All DQ checks", "delta_direction": "positive"},
    {"label": "At-Risk Students", "value": str(at_risk), "delta": f"{at_risk / students * 100:.1f}% of total", "delta_direction": "negative" if at_risk > 0 else "neutral"},
])

# --- What This Project Demonstrates ---
insight_card(
    "What This Project Demonstrates",
    "A unified analytics layer built on two incompatible data standards "
    "— Ed-Fi XML and OneRoster CSV — harmonized through a Bronze→Silver→Gold "
    "lakehouse pipeline. The dashboards present meaningful, actionable analytics "
    "on top of this unified layer.",
    severity="info",
)

# --- Mastery Distribution ---
section("Mastery Distribution", "How students perform across all standards")

narrative(
    "This donut chart shows the distribution of student mastery levels "
    "across all assessed standards. Each student-standard pair is counted once "
    "at its latest assessment level."
)

try:
    mastery_df = query("""
        SELECT mastery_level, COUNT(*) as count
        FROM (
            SELECT student_id, standard_code, mastery_level,
                   ROW_NUMBER() OVER (PARTITION BY student_id, standard_code ORDER BY assessment_count DESC) rn
            FROM gold.fact_student_mastery_daily
        ) sub WHERE rn = 1
        GROUP BY mastery_level
    """)

    level_order = ["Exceeding", "Meeting", "Developing", "Needs Intervention"]
    mastery_df["mastery_level"] = mastery_df["mastery_level"].astype("category")
    mastery_df["mastery_level"] = mastery_df["mastery_level"].cat.set_categories(level_order)
    mastery_df = mastery_df.sort_values("mastery_level")

    fig = go.Figure(data=[go.Pie(
        labels=mastery_df["mastery_level"],
        values=mastery_df["count"],
        hole=0.55,
        marker=dict(colors=[MASTERY_COLORS.get(l, "#ccc") for l in mastery_df["mastery_level"]]),
        textinfo="label+percent",
        textfont=dict(size=13),
        hovertemplate="%{label}: %{value} student-standards<br>%{percent}<extra></extra>",
    )])
    fig.update_layout(
        showlegend=False,
        height=380,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    fig = apply_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

    total_pairs = mastery_df["count"].sum()
    exceeding_pct = mastery_df[mastery_df["mastery_level"] == "Exceeding"]["count"].sum() / total_pairs * 100
    intervention_pct = mastery_df[mastery_df["mastery_level"] == "Needs Intervention"]["count"].sum() / total_pairs * 100

    insight_card(
        f"{exceeding_pct:.0f}% of student-standard pairs are at Exceeding level",
        f"{intervention_pct:.0f}% still need intervention — these are the focus areas for targeted re-teaching.",
        severity="success" if intervention_pct < 15 else "warning",
    )
except Exception as exc:
    st.error(f"Failed to load mastery distribution: {exc}")

# --- Pipeline Health ---
section("Pipeline Health", "Record flow through the lakehouse layers")

narrative(
    "Data flows from raw source files through Bronze (ingestion), "
    "Silver (cleaning and typing), and Gold (analytics-ready) layers. "
    "Records that fail validation are quarantined rather than dropped."
)

try:
    # Count records per layer
    silver_edfi = sum(
        query(f"SELECT COUNT(*) as n FROM silver_edfi.{t}")["n"].iloc[0]
        for t in ["students", "schools", "sections", "attendance", "assessment_results",
                   "enrollments", "grades", "standards", "staff", "section_associations"]
    )
    silver_or = sum(
        query(f"SELECT COUNT(*) as n FROM silver_oneroster.{t}")["n"].iloc[0]
        for t in ["users", "orgs", "classes", "courses", "enrollments",
                   "results", "academic_sessions", "demographics", "line_items"]
    )
    silver_total = silver_edfi + silver_or
    gold_total = sum(
        query(f"SELECT COUNT(*) as n FROM gold.{t}")["n"].iloc[0]
        for t in ["dim_student", "dim_school", "dim_standard", "dim_section",
                   "dim_misconception_pattern", "fact_student_mastery_daily",
                   "fact_assessment_responses", "fact_attendance_daily",
                   "agg_early_warning", "agg_district_comparison"]
    )

    layers = ["Silver", "Gold", "Quarantined"]
    values = [silver_total, gold_total, quarantined]
    colors = [LAYER_COLORS["Silver"], LAYER_COLORS["Gold"], LAYER_COLORS["Quarantined"]]

    fig = go.Figure(data=[go.Bar(
        x=values,
        y=layers,
        orientation="h",
        marker_color=colors,
        text=[f"{v:,}" for v in values],
        textposition="auto",
        textfont=dict(size=14, color="white"),
    )])
    fig.update_layout(
        height=200,
        xaxis_title="Records",
        yaxis=dict(autorange="reversed"),
        showlegend=False,
    )
    fig = apply_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

    insight_card(
        f"{pass_rate}% pipeline pass rate",
        f"Only {quarantined} records quarantined out of {students + quarantined} "
        f"processed enrollments. All quarantined records are preserved for audit — nothing is silently dropped.",
        severity="success",
    )
except Exception as exc:
    st.error(f"Failed to load pipeline health: {exc}")

# --- Architecture at a Glance ---
section("Architecture at a Glance")

dot = """
digraph arch {
    rankdir=LR;
    node [style=filled, fontsize=11, shape=box, fontname="Inter"];
    edge [fontsize=10, fontname="Inter"];

    edfi [label="Ed-Fi XML", fillcolor="#E8F5E9", fontcolor="#1A202C"];
    oneroster [label="OneRoster CSV", fillcolor="#E8F5E9", fontcolor="#1A202C"];
    pyspark [label="PySpark\\n(Bronze→Silver)", fillcolor="#FFF3E0", fontcolor="#1A202C", shape=ellipse];
    dbt [label="dbt\\n(Silver→Gold)", fillcolor="#FFF3E0", fontcolor="#1A202C", shape=ellipse];
    gold [label="Gold Layer\\n(DuckDB)", fillcolor="#F3E5F5", fontcolor="#1A202C"];
    app [label="Streamlit\\nDashboard", fillcolor="#E3F2FD", fontcolor="#1A202C", shape=doubleoctagon];

    edfi -> pyspark;
    oneroster -> pyspark;
    pyspark -> dbt;
    dbt -> gold;
    gold -> app;
}
"""
st.graphviz_chart(dot, use_container_width=True)
```

**Step 2: Verify**

Run: `cd streamlit_app && ../.venv/bin/streamlit run app.py`

Check: Sidebar should show "Executive Overview" in navigation. Click it. Should see KPIs, donut chart, pipeline health bar, architecture diagram, all with styled components.

**Step 3: Commit**

```bash
git add "streamlit_app/pages/1_📊_Executive_Overview.py"
git commit -m "feat: add Executive Overview landing page with KPIs and pipeline health"
```

---

## Task 5: Migrate Classroom Insights to Multipage

**Files:**
- Create: `streamlit_app/pages/2_🎓_Classroom_Insights.py`

**Context:** Migrate from `tabs/classroom_insights.py`. Keep all existing SQL and chart logic, but wrap with components (page_header, section, narrative, insight_card, apply_theme). Add dynamic insight cards after each chart section. Add KPI summary row at top.

**Step 1: Create the page file**

The page should:
1. Call `inject_css()`
2. Add `page_header()`
3. Add KPI summary row (avg mastery, above/below mastery %, intervention count)
4. For each section: `section()` → `narrative()` → chart → `insight_card()` with dynamic data
5. Apply `apply_theme()` to all Plotly figures
6. Use `MASTERY_COLORS` and `RISK_COLORS` from components.charts
7. Keep the existing `_source_clause()` and `_district_clause()` helpers
8. Keep all SQL queries unchanged

Key enhancements over original:
- KPI row at top with `stat_row()`
- `narrative()` before each chart explaining what to look for
- `insight_card()` after mastery heatmap: dynamically identify the standard with the most students below mastery
- `insight_card()` after early warning scatter: count of students with both low attendance (<80%) AND low mastery (<60%)
- `insight_card()` after misconception clusters: highlight the most common misconception
- `apply_theme()` on both Plotly charts
- Use themed colors: `MASTERY_COLORS` for heatmap legend, `RISK_COLORS` for scatter

**Step 2: Verify**

Run the app and check the Classroom Insights page. All 4 sections should render with narratives and insight cards.

**Step 3: Commit**

```bash
git add "streamlit_app/pages/2_🎓_Classroom_Insights.py"
git commit -m "feat: migrate Classroom Insights to multipage with narratives and insights"
```

---

## Task 6: Migrate District Intelligence to Multipage

**Files:**
- Create: `streamlit_app/pages/3_🏫_District_Intelligence.py`

**Context:** Migrate from `tabs/district_intelligence.py`. Same pattern: keep SQL, wrap with components, add narratives and insights.

Key enhancements:
- KPI row: total students, total schools, districts compared
- `narrative()` before enrollment summary
- Enrollment summary uses `metric_card()` inside styled containers instead of raw `st.metric()`
- `insight_card()` after cross-district comparison: dynamically state which district leads on more standards
- `insight_card()` after curriculum effectiveness: call out which version performs better
- `apply_theme()` on all Plotly figures
- Use `DISTRICT_COLORS` and `CURRICULUM_COLORS`

**Step 1: Create the page, verify, commit**

```bash
git add "streamlit_app/pages/3_🏫_District_Intelligence.py"
git commit -m "feat: migrate District Intelligence to multipage with narratives and insights"
```

---

## Task 7: Migrate Data Quality Lab to Multipage

**Files:**
- Create: `streamlit_app/pages/4_🔬_Data_Quality_Lab.py`

**Context:** Migrate from `tabs/dq_simulator.py`. The interactive simulator logic stays the same.

Key enhancements:
- `page_header()` with "Data Quality Lab" title
- `section()` dividers between simulator sections
- `narrative()` explaining the purpose: "This interactive lab demonstrates the pipeline's DQ validation. Inject errors into sample records and observe how the validation gate catches and quarantines them."
- Processing results use `stat_row()` instead of raw `st.metric()`
- `insight_card()` after processing: summarize what was caught
- `insight_card()` for the actual quarantine records section
- `apply_theme()` on any Plotly figures (if we add a bar chart for quarantine breakdown)
- Replace the plain markdown list of quarantine-by-rule with a small horizontal bar chart

**Step 1: Create the page, verify, commit**

```bash
git add "streamlit_app/pages/4_🔬_Data_Quality_Lab.py"
git commit -m "feat: migrate Data Quality Lab to multipage with styled components"
```

---

## Task 8: Migrate Pipeline & Governance to Multipage

**Files:**
- Create: `streamlit_app/pages/5_🛡️_Pipeline_Governance.py`

**Context:** Migrate from `tabs/pipeline_governance.py`.

Key enhancements:
- `page_header()` with governance framing
- DQ Scorecard: `stat_row()` for top metrics, styled per-rule cards
- `insight_card(success)`: "All PII fields hashed at the Silver layer — zero raw PII in Gold"
- PII Compliance: styled checklist with green checkmark HTML instead of markdown `[+]`
- Data Lineage: `narrative()` before diagram, `section()` headers
- `apply_theme()` where applicable

**Step 1: Create the page, verify, commit**

```bash
git add "streamlit_app/pages/5_🛡️_Pipeline_Governance.py"
git commit -m "feat: migrate Pipeline & Governance to multipage with styled components"
```

---

## Task 9: Cleanup — Delete Old Tabs, Final Polish

**Files:**
- Delete: `streamlit_app/tabs/classroom_insights.py`
- Delete: `streamlit_app/tabs/district_intelligence.py`
- Delete: `streamlit_app/tabs/dq_simulator.py`
- Delete: `streamlit_app/tabs/pipeline_governance.py`
- Delete: `streamlit_app/tabs/__init__.py`
- Modify: `streamlit_app/components/.gitkeep` — Delete if present

**Step 1: Delete old tabs directory**

```bash
rm -rf streamlit_app/tabs/
rm -f streamlit_app/components/.gitkeep
```

**Step 2: Full app smoke test**

Run: `cd streamlit_app && ../.venv/bin/streamlit run app.py`

Verify each page:
1. Sidebar: teal gradient, GitHub badge, district filter, page navigation
2. Executive Overview: KPIs, donut chart, pipeline health, architecture
3. Classroom Insights: KPIs, heatmap, early warning, misconceptions, dependency chain
4. District Intelligence: enrollment cards, cross-district bar, curriculum effectiveness
5. Data Quality Lab: simulator works, processing results styled
6. Pipeline & Governance: scorecard, PII panel, lineage diagram

Test the district filter on each page.

**Step 3: Commit**

```bash
git rm -r streamlit_app/tabs/
git add -A streamlit_app/
git commit -m "chore: remove old tabs directory and finalize multipage migration"
```

---

## Task 10: Deploy and Verify on Streamlit Cloud

**Step 1: Push to GitHub**

```bash
git push origin main
```

**Step 2: Verify the live deployment**

Visit: https://ed-filakehouse-oaendihhv7wjec2w2rkhog.streamlit.app/

Streamlit Cloud should auto-redeploy. Verify:
- App loads without errors
- All 5 pages accessible from sidebar
- GitHub link in sidebar works
- District filter works across pages
- All charts render correctly
- CSS theming applied (teal sidebar, styled cards)
