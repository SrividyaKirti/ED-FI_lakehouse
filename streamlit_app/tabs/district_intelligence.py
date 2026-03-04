"""Tab 2 -- District Intelligence.

Enrollment summaries, cross-district standard comparisons,
and curriculum-version effectiveness analysis.
"""

import streamlit as st
import plotly.express as px
import pandas as pd
from db import query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_filter() -> str | None:
    return st.session_state.get("source_filter")


# ---------------------------------------------------------------------------
# 1. Enrollment Summary
# ---------------------------------------------------------------------------

def _render_enrollment_summary() -> None:
    st.subheader("Enrollment Summary")

    src = _source_filter()

    try:
        # Per-district aggregates
        where = f"WHERE s._source_system = '{src}'" if src else ""
        sql = f"""
            SELECT
                CASE
                    WHEN s._source_system = 'edfi' THEN 'Grand Bend ISD'
                    ELSE 'Riverside USD'
                END as district,
                s._source_system,
                COUNT(DISTINCT s.student_id) as student_count,
                COUNT(DISTINCT s.school_id) as school_count,
                ROUND(AVG(s.grade_level), 1) as avg_grade_level,
                MIN(s.grade_level) as min_grade,
                MAX(s.grade_level) as max_grade
            FROM gold.dim_student s
            {where}
            GROUP BY s._source_system
            ORDER BY district
        """
        df = query(sql)
    except Exception as exc:
        st.error(f"Failed to load enrollment summary: {exc}")
        return

    if df.empty:
        st.info("No enrollment data available.")
        return

    cols = st.columns(len(df))
    for col, (_, row) in zip(cols, df.iterrows()):
        with col:
            st.markdown(f"### {row['district']}")
            st.markdown(f"_Source: {row['_source_system']}_")
            m1, m2, m3 = st.columns(3)
            m1.metric("Students", int(row["student_count"]))
            m2.metric("Schools", int(row["school_count"]))
            m3.metric("Avg Grade", row["avg_grade_level"])
            st.caption(f"Grades {int(row['min_grade'])} -- {int(row['max_grade'])}")


# ---------------------------------------------------------------------------
# 2. Cross-District Standard Comparison
# ---------------------------------------------------------------------------

def _render_cross_district_comparison() -> None:
    st.subheader("Cross-District Standard Comparison")
    st.caption(
        "Mastery percentage by standard, compared across districts. "
        "Mastery is defined as scoring 70 or above."
    )

    src = _source_filter()

    try:
        # agg_district_comparison has district_name = NULL for oneroster.
        # Re-derive from fact tables joined to dim_student for accurate
        # district labeling.
        where = f"WHERE s._source_system = '{src}'" if src else ""
        sql = f"""
            SELECT
                m.standard_code,
                CASE
                    WHEN s._source_system = 'edfi' THEN 'Grand Bend ISD'
                    ELSE 'Riverside USD'
                END as district_name,
                COUNT(DISTINCT m.student_id) as student_count,
                ROUND(AVG(m.max_score_to_date), 1) as avg_score,
                ROUND(
                    SUM(CASE WHEN m.max_score_to_date >= 70 THEN 1 ELSE 0 END) * 100.0
                    / COUNT(*), 1
                ) as mastery_pct
            FROM (
                SELECT m2.student_id, m2.standard_code, m2.max_score_to_date, m2._source_system,
                       ROW_NUMBER() OVER (
                           PARTITION BY m2.student_id, m2.standard_code
                           ORDER BY m2.assessment_count DESC
                       ) as rn
                FROM gold.fact_student_mastery_daily m2
            ) m
            INNER JOIN gold.dim_student s ON m.student_id = s.student_id
            {where}
            WHERE m.rn = 1
            GROUP BY m.standard_code, s._source_system
            HAVING COUNT(DISTINCT m.student_id) >= 3
            ORDER BY m.standard_code
        """
        # Fix: the outer WHERE conflicts with the INNER JOIN / sub WHERE
        # Rebuild with proper syntax:
        source_and = f"AND s._source_system = '{src}'" if src else ""
        sql = f"""
            SELECT
                m.standard_code,
                CASE
                    WHEN s._source_system = 'edfi' THEN 'Grand Bend ISD'
                    ELSE 'Riverside USD'
                END as district_name,
                COUNT(DISTINCT m.student_id) as student_count,
                ROUND(AVG(m.max_score_to_date), 1) as avg_score,
                ROUND(
                    SUM(CASE WHEN m.max_score_to_date >= 70 THEN 1 ELSE 0 END) * 100.0
                    / COUNT(*), 1
                ) as mastery_pct
            FROM (
                SELECT m2.student_id, m2.standard_code, m2.max_score_to_date, m2._source_system,
                       ROW_NUMBER() OVER (
                           PARTITION BY m2.student_id, m2.standard_code
                           ORDER BY m2.assessment_count DESC
                       ) as rn
                FROM gold.fact_student_mastery_daily m2
            ) m
            INNER JOIN gold.dim_student s
                ON m.student_id = s.student_id
                {source_and}
            WHERE m.rn = 1
            GROUP BY m.standard_code, s._source_system
            HAVING COUNT(DISTINCT m.student_id) >= 3
            ORDER BY m.standard_code
        """
        df = query(sql)
    except Exception as exc:
        st.error(f"Failed to load district comparison data: {exc}")
        return

    if df.empty:
        st.info("No comparison data available.")
        return

    # Shorten standard codes for x-axis
    df["standard_short"] = df["standard_code"].apply(
        lambda c: ".".join(c.split(".")[2:]) if len(c.split(".")) > 2 else c
    )

    fig = px.bar(
        df,
        x="standard_short",
        y="mastery_pct",
        color="district_name",
        barmode="group",
        labels={
            "standard_short": "Standard",
            "mastery_pct": "Mastery %",
            "district_name": "District",
        },
        hover_data=["standard_code", "student_count", "avg_score"],
        color_discrete_map={
            "Grand Bend ISD": "#1565c0",
            "Riverside USD": "#7b1fa2",
        },
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        yaxis_range=[0, 100],
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# 3. Curriculum Effectiveness
# ---------------------------------------------------------------------------

def _render_curriculum_effectiveness() -> None:
    st.subheader("Curriculum Effectiveness")
    st.caption(
        "Comparing curriculum Version A vs Version B. "
        "Average mastery scores are computed for students enrolled "
        "in sections using each curriculum version."
    )

    src = _source_filter()
    source_and = f"AND sec._source_system = '{src}'" if src else ""

    sql = f"""
        SELECT
            sec.curriculum_version,
            sec.course_name,
            COUNT(DISTINCT m.student_id) as student_count,
            ROUND(AVG(m.max_score_to_date), 1) as avg_mastery,
            ROUND(
                SUM(CASE WHEN m.max_score_to_date >= 70 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 1
            ) as mastery_pct
        FROM gold.dim_section sec
        INNER JOIN gold.dim_student s
            ON sec.school_id = s.school_id
        INNER JOIN (
            SELECT m2.student_id, m2.standard_code, m2.max_score_to_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m2.student_id, m2.standard_code
                       ORDER BY m2.assessment_count DESC
                   ) as rn
            FROM gold.fact_student_mastery_daily m2
        ) m ON s.student_id = m.student_id AND m.rn = 1
        WHERE sec.curriculum_version IN ('A', 'B')
          {source_and}
        GROUP BY sec.curriculum_version, sec.course_name
        ORDER BY sec.course_name, sec.curriculum_version
    """

    try:
        df = query(sql)
    except Exception as exc:
        st.error(f"Failed to load curriculum data: {exc}")
        return

    if df.empty:
        st.info("No curriculum comparison data available.")
        return

    # Summary bar chart: Version A vs B overall
    summary = (
        df.groupby("curriculum_version")
        .agg(
            total_students=("student_count", "sum"),
            avg_mastery=("avg_mastery", "mean"),
            avg_mastery_pct=("mastery_pct", "mean"),
        )
        .reset_index()
    )

    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(
            summary,
            x="curriculum_version",
            y="avg_mastery",
            color="curriculum_version",
            labels={
                "curriculum_version": "Curriculum Version",
                "avg_mastery": "Avg Mastery Score",
            },
            color_discrete_map={"A": "#1565c0", "B": "#388e3c"},
            text="avg_mastery",
        )
        fig.update_layout(
            showlegend=False,
            height=350,
            yaxis_range=[0, 100],
        )
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig2 = px.bar(
            summary,
            x="curriculum_version",
            y="avg_mastery_pct",
            color="curriculum_version",
            labels={
                "curriculum_version": "Curriculum Version",
                "avg_mastery_pct": "Mastery Rate %",
            },
            color_discrete_map={"A": "#1565c0", "B": "#388e3c"},
            text="avg_mastery_pct",
        )
        fig2.update_layout(
            showlegend=False,
            height=350,
            yaxis_range=[0, 100],
        )
        fig2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        st.plotly_chart(fig2, use_container_width=True)

    # Detail table by course
    st.markdown("**Breakdown by Course**")
    st.dataframe(
        df.rename(columns={
            "curriculum_version": "Version",
            "course_name": "Course",
            "student_count": "Students",
            "avg_mastery": "Avg Score",
            "mastery_pct": "Mastery %",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def render() -> None:
    st.header("District Intelligence")
    _render_enrollment_summary()
    st.markdown("---")
    _render_cross_district_comparison()
    st.markdown("---")
    _render_curriculum_effectiveness()
