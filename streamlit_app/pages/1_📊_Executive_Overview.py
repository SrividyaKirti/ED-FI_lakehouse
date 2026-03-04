"""Executive Overview — flagship landing page for the Ed-Fi Lakehouse dashboard."""

import sys
import os

import streamlit as st
import plotly.graph_objects as go

# Ensure the streamlit_app directory is on the import path so that
# ``components`` and ``db`` resolve correctly regardless of cwd.
_app_dir = os.path.join(os.path.dirname(__file__), "..")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

from components import (  # noqa: E402
    setup_page,
    page_header,
    section,
    narrative,
    insight_card,
    stat_row,
    apply_theme,
    MASTERY_COLORS,
    LAYER_COLORS,
)
from db import query  # noqa: E402

setup_page()

# ── Page header ─────────────────────────────────────────────────────
page_header(
    "Executive Overview",
    "Real-time analytics across two interoperable school districts",
)

# ── KPI metrics ─────────────────────────────────────────────────────
try:
    students = int(query("SELECT COUNT(*) AS n FROM gold.dim_student").iloc[0, 0])
    schools = int(
        query("SELECT COUNT(DISTINCT school_id) AS n FROM gold.dim_school").iloc[0, 0]
    )
    standards = int(
        query(
            "SELECT COUNT(DISTINCT standard_code) AS n FROM gold.dim_standard"
        ).iloc[0, 0]
    )
    quarantined = int(
        query("SELECT COUNT(*) AS n FROM gold.fact_dq_quarantine_log").iloc[0, 0]
    )
    pass_rate = round(
        (1 - quarantined / max(students + quarantined, 1)) * 100, 1
    )
    at_risk = int(
        query(
            "SELECT COUNT(*) AS n FROM gold.agg_early_warning "
            "WHERE risk_level IN ('High', 'Medium')"
        ).iloc[0, 0]
    )
except Exception as exc:
    st.error(f"Failed to load KPI data: {exc}")
    st.stop()

stat_row(
    [
        {"label": "Total Students", "value": f"{students:,}"},
        {"label": "Schools", "value": f"{schools:,}"},
        {"label": "Standards Tracked", "value": f"{standards:,}"},
        {"label": "DQ Pass Rate", "value": f"{pass_rate}%"},
        {"label": "At-Risk Students", "value": f"{at_risk:,}"},
    ]
)

# ── "What This Project Demonstrates" ───────────────────────────────
insight_card(
    "What This Project Demonstrates",
    "A unified analytics layer built on two incompatible data standards "
    "— Ed-Fi XML and OneRoster CSV — harmonized through a Bronze→Silver→Gold "
    "lakehouse pipeline. The dashboards present meaningful, actionable analytics "
    "on top of this unified layer.",
    severity="info",
)

# ── Mastery Distribution (donut chart) ──────────────────────────────
section("Mastery Distribution", "How students perform across all standards")

narrative(
    "Each student-standard pair is counted once using the record with the "
    "highest assessment count. The donut chart below shows the overall "
    "distribution of mastery levels across both districts."
)

try:
    mastery_df = query(
        """
        SELECT mastery_level, COUNT(*) AS count
        FROM (
            SELECT student_id, standard_code, mastery_level,
                   ROW_NUMBER() OVER (
                       PARTITION BY student_id, standard_code
                       ORDER BY assessment_count DESC
                   ) AS rn
            FROM gold.fact_student_mastery_daily
        ) sub
        WHERE rn = 1
        GROUP BY mastery_level
        """
    )
except Exception as exc:
    st.error(f"Failed to load mastery data: {exc}")
    st.stop()

# Build the donut chart with consistent color ordering
mastery_order = ["Exceeding", "Meeting", "Developing", "Needs Intervention"]
mastery_df = mastery_df.set_index("mastery_level").reindex(mastery_order).reset_index()
mastery_df = mastery_df.dropna(subset=["count"])

fig_mastery = go.Figure(
    go.Pie(
        labels=mastery_df["mastery_level"],
        values=mastery_df["count"],
        hole=0.55,
        marker=dict(
            colors=[MASTERY_COLORS[lvl] for lvl in mastery_df["mastery_level"]]
        ),
        textinfo="label+percent",
        hovertemplate="%{label}: %{value:,} students (%{percent})<extra></extra>",
    )
)
fig_mastery.update_layout(title_text="Mastery Level Distribution", height=420)
apply_theme(fig_mastery)
st.plotly_chart(fig_mastery, use_container_width=True)

# Dynamic insight from mastery data
total_mastery = int(mastery_df["count"].sum())
exceeding_count = int(
    mastery_df.loc[mastery_df["mastery_level"] == "Exceeding", "count"].values[0]
)
intervention_count = int(
    mastery_df.loc[
        mastery_df["mastery_level"] == "Needs Intervention", "count"
    ].values[0]
)
exceeding_pct = round(exceeding_count / max(total_mastery, 1) * 100, 1)
intervention_pct = round(intervention_count / max(total_mastery, 1) * 100, 1)

insight_card(
    "Mastery Snapshot",
    f"<b>{exceeding_pct}%</b> of student-standard pairs are at the "
    f"<em>Exceeding</em> level, while <b>{intervention_pct}%</b> require "
    f"intervention — representing {intervention_count:,} student-standard "
    f"combinations that may need targeted support.",
    severity="success" if intervention_pct < 10 else "warning",
)

# ── Pipeline Health (horizontal bar chart) ──────────────────────────
section("Pipeline Health", "Record flow through the lakehouse layers")

narrative(
    "Data flows through three layers: <b>Bronze</b> (raw ingestion), "
    "<b>Silver</b> (cleaned and standardized by PySpark), and <b>Gold</b> "
    "(analytics-ready tables built by dbt). Records that fail data-quality "
    "checks are quarantined rather than silently dropped."
)

try:
    # Silver Ed-Fi tables
    silver_edfi_tables = [
        "students", "schools", "sections", "attendance",
        "assessment_results", "enrollments", "grades", "standards",
        "staff", "section_associations",
    ]
    silver_edfi_total = 0
    for tbl in silver_edfi_tables:
        count = int(
            query(f"SELECT COUNT(*) AS n FROM silver_edfi.{tbl}").iloc[0, 0]
        )
        silver_edfi_total += count

    # Silver OneRoster tables
    silver_or_tables = [
        "users", "orgs", "classes", "courses", "enrollments",
        "results", "academic_sessions", "demographics", "line_items",
    ]
    silver_or_total = 0
    for tbl in silver_or_tables:
        count = int(
            query(f"SELECT COUNT(*) AS n FROM silver_oneroster.{tbl}").iloc[0, 0]
        )
        silver_or_total += count

    silver_total = silver_edfi_total + silver_or_total

    # Gold tables (excluding quarantine log)
    gold_tables = [
        "dim_student", "dim_school", "dim_standard", "dim_section",
        "dim_misconception_pattern", "fact_student_mastery_daily",
        "fact_assessment_responses", "fact_attendance_daily",
        "agg_early_warning", "agg_district_comparison",
    ]
    gold_total = 0
    for tbl in gold_tables:
        count = int(query(f"SELECT COUNT(*) AS n FROM gold.{tbl}").iloc[0, 0])
        gold_total += count

except Exception as exc:
    st.error(f"Failed to load pipeline metrics: {exc}")
    st.stop()

layers = ["Silver", "Gold", "Quarantined"]
counts = [silver_total, gold_total, quarantined]
colors = [LAYER_COLORS["Silver"], LAYER_COLORS["Gold"], LAYER_COLORS["Quarantined"]]

fig_pipeline = go.Figure(
    go.Bar(
        y=layers,
        x=counts,
        orientation="h",
        marker=dict(color=colors),
        text=[f"{c:,}" for c in counts],
        textposition="outside",
        hovertemplate="%{y}: %{x:,} records<extra></extra>",
    )
)
fig_pipeline.update_layout(
    title_text="Records per Pipeline Layer",
    xaxis_title="Record Count",
    yaxis_title="",
    height=300,
    yaxis=dict(autorange="reversed"),
)
apply_theme(fig_pipeline)
st.plotly_chart(fig_pipeline, use_container_width=True)

insight_card(
    "Pipeline Throughput",
    f"<b>{pass_rate}%</b> of ingested records passed data-quality checks. "
    f"Only <b>{quarantined:,}</b> records were quarantined out of "
    f"{students + quarantined:,} student records processed.",
    severity="success" if pass_rate >= 95 else "warning",
)

# ── Architecture at a Glance (Graphviz DOT diagram) ────────────────
section("Architecture at a Glance")

dot_source = """
digraph architecture {
    rankdir=LR;
    node [shape=box, style="filled,rounded", fontname="DM Sans, sans-serif",
          fontsize=11, fontcolor="#1A202C", penwidth=0];
    edge [color="#A0AEC0", arrowsize=0.7];

    // Source nodes
    edfi  [label="Ed-Fi XML",      fillcolor="#E8F5E9"];
    oneroster [label="OneRoster CSV", fillcolor="#E8F5E9"];

    // Processing nodes
    pyspark [label="PySpark\\n(Bronze → Silver)", fillcolor="#FFF3E0"];
    dbt     [label="dbt\\n(Silver → Gold)",       fillcolor="#FFF3E0"];

    // Output nodes
    gold [label="Gold Layer\\n(DuckDB)", fillcolor="#F3E5F5"];
    app  [label="Streamlit\\nDashboard", fillcolor="#E3F2FD"];

    // Edges
    edfi      -> pyspark;
    oneroster -> pyspark;
    pyspark   -> dbt;
    dbt       -> gold;
    gold      -> app;
}
"""

st.graphviz_chart(dot_source)
