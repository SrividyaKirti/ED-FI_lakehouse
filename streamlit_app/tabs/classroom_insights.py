"""Tab 1 -- Classroom Insights.

Surfaces Kiddom-style mastery views, misconception clusters,
standards dependency chains, and early-warning indicators.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db import query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_clause(alias: str = "m") -> str:
    """Return a SQL WHERE clause fragment that filters by source system.

    SECURITY: src is always from a constrained st.selectbox ('edfi'/'oneroster'/None),
    never from free-text input. DuckDB connection is read-only.
    """
    src = st.session_state.get("source_filter")
    if src:
        return f" AND {alias}._source_system = '{src}'"
    return ""


def _district_clause(alias: str = "sch") -> str:
    """Return a SQL WHERE/AND clause fragment for district filtering.

    Because Riverside USD rows have district_name = NULL in the gold layer,
    we fall back to _source_system filtering when the district filter is set.
    """
    src = st.session_state.get("source_filter")
    if src:
        return f" AND {alias}._source_system = '{src}'"
    return ""


# ---------------------------------------------------------------------------
# 1. Mastery Heatmap
# ---------------------------------------------------------------------------

def _render_mastery_heatmap() -> None:
    st.subheader("Mastery Heatmap")
    st.caption(
        "Latest mastery score per student per standard. "
        "Uses Kiddom's 'max value' method -- the highest score a student "
        "has achieved on each standard is carried forward."
    )

    source_filter = _source_clause("m")

    sql = f"""
        SELECT m.student_id, m.standard_code, m.max_score_to_date, m.mastery_level
        FROM gold.fact_student_mastery_daily m
        INNER JOIN (
            SELECT student_id, standard_code, MAX(date_key) as max_date
            FROM gold.fact_student_mastery_daily
            WHERE 1=1 {_source_clause("fact_student_mastery_daily")}
            GROUP BY student_id, standard_code
        ) latest
            ON m.student_id = latest.student_id
            AND m.standard_code = latest.standard_code
            AND (m.date_key = latest.max_date OR (m.date_key IS NULL AND latest.max_date IS NULL))
        WHERE 1=1 {source_filter}
        LIMIT 500
    """

    try:
        df = query(sql)
    except Exception as exc:
        st.error(f"Failed to load mastery data: {exc}")
        return

    if df.empty:
        st.info("No mastery data available for the selected filter.")
        return

    # De-duplicate: keep highest max_score_to_date per student+standard
    df = (
        df.sort_values("max_score_to_date", ascending=False)
        .drop_duplicates(subset=["student_id", "standard_code"], keep="first")
    )

    # Pivot to matrix: students (rows) x standards (cols)
    pivot = df.pivot_table(
        index="student_id",
        columns="standard_code",
        values="max_score_to_date",
        aggfunc="max",
    )

    # Sort columns by grade level embedded in standard code
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)

    # Color scale matching mastery levels from fact_student_mastery_daily.sql:
    # Needs Intervention (<50) red, Developing (50-69) orange,
    # Meeting (70-89) purple, Exceeding (>=90) green
    color_scale = [
        [0.0, "#d32f2f"],    # red -- Needs Intervention
        [0.499, "#d32f2f"],
        [0.50, "#fb8c00"],   # orange -- Developing
        [0.699, "#fb8c00"],
        [0.70, "#7b1fa2"],   # purple -- Meeting
        [0.899, "#7b1fa2"],
        [0.90, "#388e3c"],   # green -- Exceeding
        [1.0, "#388e3c"],
    ]

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[c.split(".")[-1] for c in pivot.columns],
            y=[sid[-6:] for sid in pivot.index],
            colorscale=color_scale,
            zmin=0,
            zmax=100,
            colorbar=dict(
                title="Score",
                tickvals=[25, 60, 80, 95],
                ticktext=["Needs Intervention", "Developing", "Meeting", "Exceeding"],
            ),
            hovertemplate=(
                "Student: %{y}<br>Standard: %{x}<br>Score: %{z}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        xaxis_title="Standard",
        yaxis_title="Student (last 6 chars)",
        height=max(350, len(pivot) * 14),
        margin=dict(l=100, r=20, t=30, b=80),
    )

    st.plotly_chart(fig, use_container_width=True)

    # Legend below chart
    legend_cols = st.columns(4)
    labels = [
        ("Needs Intervention", "0 -- 49", "#d32f2f"),
        ("Developing", "50 -- 69", "#fb8c00"),
        ("Meeting", "70 -- 89", "#7b1fa2"),
        ("Exceeding", "90 -- 100", "#388e3c"),
    ]
    for col, (label, rng, color) in zip(legend_cols, labels):
        col.markdown(
            f"<span style='color:{color}; font-weight:bold;'>{label}</span> ({rng})",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# 2. Misconception Clusters
# ---------------------------------------------------------------------------

def _render_misconception_clusters() -> None:
    st.subheader("Misconception Clusters")
    st.caption(
        "Detected wrong-answer patterns grouped by standard, "
        "with affected student counts and suggested re-teach strategies."
    )

    source_filter = _source_clause("r")

    sql = f"""
        SELECT r.standard_code,
               r.misconception_tag,
               r.misconception_description,
               r.suggested_reteach,
               COUNT(DISTINCT r.student_id) as affected_students,
               COUNT(*) as total_occurrences
        FROM gold.fact_assessment_responses r
        WHERE r.misconception_tag IS NOT NULL
          AND r.is_correct = false
          {source_filter}
        GROUP BY r.standard_code, r.misconception_tag,
                 r.misconception_description, r.suggested_reteach
        ORDER BY affected_students DESC
    """

    try:
        df = query(sql)
    except Exception as exc:
        st.error(f"Failed to load misconception data: {exc}")
        return

    if df.empty:
        st.info("No misconception patterns detected for the selected filter.")
        return

    for _, row in df.iterrows():
        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(f"**{row['standard_code']}** -- `{row['misconception_tag']}`")
                st.markdown(row["misconception_description"])
            with c2:
                st.metric("Affected Students", int(row["affected_students"]))
                st.metric("Occurrences", int(row["total_occurrences"]))

            st.markdown(f"**Suggested Re-teach:** {row['suggested_reteach']}")


# ---------------------------------------------------------------------------
# 3. Standards Dependency Chain
# ---------------------------------------------------------------------------

def _render_standards_dependency() -> None:
    st.subheader("Standards Dependency Chain")
    st.caption(
        "Prerequisite graph from the CCSS math standards. "
        "Node colors reflect class-average mastery: "
        "green (>=85), yellow (70-84), red (<70), gray (no data)."
    )

    source_filter = _source_clause("m")

    try:
        standards = query(
            "SELECT standard_code, prerequisite_standard_code, grade_level "
            "FROM gold.dim_standard ORDER BY grade_level"
        )

        # Average mastery per standard (latest per student, then avg)
        avg_sql = f"""
            SELECT standard_code, AVG(max_score_to_date) as avg_score
            FROM (
                SELECT m.student_id, m.standard_code, m.max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY m.student_id, m.standard_code
                           ORDER BY m.assessment_count DESC
                       ) as rn
                FROM gold.fact_student_mastery_daily m
                WHERE 1=1 {source_filter}
            ) sub
            WHERE rn = 1
            GROUP BY standard_code
        """
        avg_mastery = query(avg_sql)
    except Exception as exc:
        st.error(f"Failed to load standards data: {exc}")
        return

    score_map = dict(zip(avg_mastery["standard_code"], avg_mastery["avg_score"]))

    def _node_color(code: str) -> str:
        score = score_map.get(code)
        if score is None:
            return "#cccccc"
        if score >= 85:
            return "#388e3c"
        if score >= 70:
            return "#fbc02d"
        return "#d32f2f"

    def _short(code: str) -> str:
        """Shorten e.g. CCSS.MATH.K.CC.A.1 -> K.CC.A.1"""
        parts = code.split(".")
        return ".".join(parts[2:]) if len(parts) > 2 else code

    # Build Graphviz DOT
    lines = ["digraph standards {", "  rankdir=LR;", "  node [style=filled, fontsize=10];"]
    for _, row in standards.iterrows():
        code = row["standard_code"]
        color = _node_color(code)
        score = score_map.get(code)
        label = _short(code)
        if score is not None:
            label += f"\\n({score:.0f})"
        lines.append(f'  "{_short(code)}" [label="{label}", fillcolor="{color}", fontcolor="white"];')

    for _, row in standards.iterrows():
        prereq = row["prerequisite_standard_code"]
        if prereq:
            lines.append(f'  "{_short(prereq)}" -> "{_short(row["standard_code"])}";')

    lines.append("}")
    dot = "\n".join(lines)

    st.graphviz_chart(dot, use_container_width=True)


# ---------------------------------------------------------------------------
# 4. Early Warning
# ---------------------------------------------------------------------------

def _render_early_warning() -> None:
    st.subheader("Early Warning System")
    st.caption(
        "Students plotted by attendance rate vs. average mastery score, "
        "colored by risk level. High-risk students appear in the table below."
    )

    # Early warning only has oneroster students currently;
    # still apply source filter via join to dim_student
    src = st.session_state.get("source_filter")
    join_clause = ""
    if src:
        join_clause = f"INNER JOIN gold.dim_student s ON ew.student_id = s.student_id AND s._source_system = '{src}'"
    else:
        join_clause = "LEFT JOIN gold.dim_student s ON ew.student_id = s.student_id"

    sql = f"""
        SELECT ew.student_id,
               ew.avg_mastery_score,
               ew.attendance_rate,
               ew.count_below_developing,
               ew.standards_assessed,
               ew.declining_trend,
               ew.risk_level,
               s.grade_level,
               s.school_id
        FROM gold.agg_early_warning ew
        {join_clause}
        ORDER BY ew.risk_level DESC, ew.avg_mastery_score ASC
    """

    try:
        df = query(sql)
    except Exception as exc:
        st.error(f"Failed to load early-warning data: {exc}")
        return

    if df.empty:
        st.info("No early-warning data available for the selected filter.")
        return

    # Color mapping
    color_map = {"High": "#d32f2f", "Medium": "#fb8c00", "Low": "#388e3c"}

    fig = px.scatter(
        df,
        x="attendance_rate",
        y="avg_mastery_score",
        color="risk_level",
        color_discrete_map=color_map,
        hover_data=["student_id", "grade_level", "count_below_developing"],
        labels={
            "attendance_rate": "Attendance Rate",
            "avg_mastery_score": "Avg Mastery Score",
            "risk_level": "Risk Level",
        },
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

    # Summary metrics
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Students", len(df))
    high_count = len(df[df["risk_level"] == "High"])
    med_count = len(df[df["risk_level"] == "Medium"])
    m2.metric("Medium Risk", med_count)
    m3.metric("High Risk", high_count)

    # Table of at-risk students
    at_risk = df[df["risk_level"].isin(["High", "Medium"])].copy()
    if not at_risk.empty:
        st.markdown("**At-Risk Students**")
        display_cols = [
            "student_id", "risk_level", "avg_mastery_score",
            "attendance_rate", "count_below_developing",
            "standards_assessed", "declining_trend", "grade_level",
        ]
        st.dataframe(
            at_risk[display_cols].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No students are currently flagged as at-risk.")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def render() -> None:
    st.header("Classroom Insights")
    _render_mastery_heatmap()
    st.markdown("---")
    _render_misconception_clusters()
    st.markdown("---")
    _render_standards_dependency()
    st.markdown("---")
    _render_early_warning()
