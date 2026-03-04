"""District Intelligence -- enrollment, standards performance, and curriculum
effectiveness across districts.

Migrated from ``tabs/district_intelligence.py`` with enhanced narratives,
dynamic insight cards, and design-system theming.
"""

import sys
import os

_app_dir = os.path.join(os.path.dirname(__file__), "..")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

import streamlit as st  # noqa: E402
import plotly.express as px  # noqa: E402
import pandas as pd  # noqa: E402
from db import query  # noqa: E402
from components import (  # noqa: E402
    inject_css,
    page_header,
    section,
    narrative,
    stat_row,
    metric_card,
    insight_card,
    apply_theme,
    DISTRICT_COLORS,
    CURRICULUM_COLORS,
)


# ── CSS injection (each page must call this independently) ────────────
inject_css()


# ── Page header ───────────────────────────────────────────────────────
page_header(
    "District Intelligence",
    "Enrollment, standards performance, and curriculum effectiveness across districts",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_filter() -> str | None:
    return st.session_state.get("source_filter")


# ---------------------------------------------------------------------------
# KPI Summary Row
# ---------------------------------------------------------------------------

try:
    _total_students = int(
        query("SELECT COUNT(*) AS n FROM gold.dim_student").iloc[0, 0]
    )
    _total_schools = int(
        query("SELECT COUNT(DISTINCT school_id) AS n FROM gold.dim_school").iloc[0, 0]
    )
except Exception as exc:
    st.error(f"Failed to load KPI data: {exc}")
    st.stop()

stat_row(
    [
        {"label": "Total Students", "value": f"{_total_students:,}"},
        {"label": "Total Schools", "value": f"{_total_schools:,}"},
        {"label": "Districts", "value": "2"},
    ]
)


# ---------------------------------------------------------------------------
# 1. Enrollment Summary
# ---------------------------------------------------------------------------

section("Enrollment Summary")
narrative(
    "Per-district enrollment breakdown showing student counts, school coverage, "
    "and grade ranges."
)

src = _source_filter()

try:
    where = f"WHERE s._source_system = '{src}'" if src else ""
    _enrollment_sql = f"""
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
    enrollment_df = query(_enrollment_sql)
except Exception as exc:
    st.error(f"Failed to load enrollment summary: {exc}")
    enrollment_df = pd.DataFrame()

if enrollment_df.empty:
    st.info("No enrollment data available.")
else:
    cols = st.columns(len(enrollment_df))
    for col, (_, row) in zip(cols, enrollment_df.iterrows()):
        with col:
            st.markdown(f"### {row['district']}")
            st.markdown(f"_Source: {row['_source_system']}_")
            m1, m2, m3 = st.columns(3)
            with m1:
                metric_card("Students", str(int(row["student_count"])))
            with m2:
                metric_card("Schools", str(int(row["school_count"])))
            with m3:
                metric_card("Avg Grade", str(row["avg_grade_level"]))
            st.caption(
                f"Grades {int(row['min_grade'])} -- {int(row['max_grade'])}"
            )


# ---------------------------------------------------------------------------
# 2. Cross-District Standard Comparison
# ---------------------------------------------------------------------------

section("Cross-District Standard Comparison")
narrative(
    "Mastery percentage by standard, compared across districts. "
    "Mastery is defined as scoring 70 or above."
)

src = _source_filter()

try:
    # source_filter is always from a constrained st.selectbox, never free-text input.
    source_and = f"AND s._source_system = '{src}'" if src else ""
    _comparison_sql = f"""
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
            SELECT m2.student_id, m2.standard_code, m2.max_score_to_date,
                   m2._source_system,
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
    comparison_df = query(_comparison_sql)
except Exception as exc:
    st.error(f"Failed to load district comparison data: {exc}")
    comparison_df = pd.DataFrame()

if comparison_df.empty:
    st.info("No comparison data available.")
else:
    # Shorten standard codes for x-axis
    comparison_df["standard_short"] = comparison_df["standard_code"].apply(
        lambda c: ".".join(c.split(".")[2:]) if len(c.split(".")) > 2 else c
    )

    fig = px.bar(
        comparison_df,
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
        color_discrete_map=DISTRICT_COLORS,
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        yaxis_range=[0, 100],
    )
    apply_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

    # Dynamic insight: which district leads on more standards
    pivoted = comparison_df.pivot(
        index="standard_code", columns="district_name", values="mastery_pct"
    )
    district_names = [d for d in DISTRICT_COLORS if d in pivoted.columns]

    if len(district_names) == 2:
        d1, d2 = district_names[0], district_names[1]
        # Only count standards where both districts have data
        both = pivoted.dropna(subset=[d1, d2])
        d1_leads = int((both[d1] > both[d2]).sum())
        d2_leads = int((both[d2] > both[d1]).sum())
        ties = int(len(both) - d1_leads - d2_leads)

        if d1_leads > d2_leads:
            leader, leader_n = d1, d1_leads
        elif d2_leads > d1_leads:
            leader, leader_n = d2, d2_leads
        else:
            leader, leader_n = None, 0

        if leader:
            body = (
                f"<b>{leader}</b> leads on <b>{leader_n}</b> of "
                f"{len(both)} shared standards."
            )
            if ties > 0:
                body += f" The districts are tied on {ties} standard{'s' if ties != 1 else ''}."
            insight_card("District Comparison", body, severity="info")
        else:
            insight_card(
                "District Comparison",
                f"Both districts are evenly matched, each leading on "
                f"<b>{d1_leads}</b> of {len(both)} shared standards.",
                severity="info",
            )
    else:
        insight_card(
            "District Comparison",
            "Only one district has sufficient data for comparison.",
            severity="info",
        )


# ---------------------------------------------------------------------------
# 3. Curriculum Effectiveness
# ---------------------------------------------------------------------------

section("Curriculum Effectiveness")
narrative(
    "Comparing curriculum Version A vs Version B across all course sections."
)

src = _source_filter()
source_and = f"AND sec._source_system = '{src}'" if src else ""

_curriculum_sql = f"""
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
    curriculum_df = query(_curriculum_sql)
except Exception as exc:
    st.error(f"Failed to load curriculum data: {exc}")
    curriculum_df = pd.DataFrame()

if curriculum_df.empty:
    st.info("No curriculum comparison data available.")
else:
    # Summary bar chart: Version A vs B overall
    summary = (
        curriculum_df.groupby("curriculum_version")
        .agg(
            total_students=("student_count", "sum"),
            avg_mastery=("avg_mastery", "mean"),
            avg_mastery_pct=("mastery_pct", "mean"),
        )
        .reset_index()
    )

    c1, c2 = st.columns(2)

    with c1:
        fig_avg = px.bar(
            summary,
            x="curriculum_version",
            y="avg_mastery",
            color="curriculum_version",
            labels={
                "curriculum_version": "Curriculum Version",
                "avg_mastery": "Avg Mastery Score",
            },
            color_discrete_map=CURRICULUM_COLORS,
            text="avg_mastery",
        )
        fig_avg.update_layout(
            showlegend=False,
            height=350,
            yaxis_range=[0, 100],
        )
        fig_avg.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        apply_theme(fig_avg)
        st.plotly_chart(fig_avg, use_container_width=True)

    with c2:
        fig_pct = px.bar(
            summary,
            x="curriculum_version",
            y="avg_mastery_pct",
            color="curriculum_version",
            labels={
                "curriculum_version": "Curriculum Version",
                "avg_mastery_pct": "Mastery Rate %",
            },
            color_discrete_map=CURRICULUM_COLORS,
            text="avg_mastery_pct",
        )
        fig_pct.update_layout(
            showlegend=False,
            height=350,
            yaxis_range=[0, 100],
        )
        fig_pct.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        apply_theme(fig_pct)
        st.plotly_chart(fig_pct, use_container_width=True)

    # Detail table by course
    st.markdown("**Breakdown by Course**")
    st.dataframe(
        curriculum_df.rename(columns={
            "curriculum_version": "Version",
            "course_name": "Course",
            "student_count": "Students",
            "avg_mastery": "Avg Score",
            "mastery_pct": "Mastery %",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Dynamic insight: which curriculum version has higher overall avg mastery
    version_a = summary.loc[summary["curriculum_version"] == "A"]
    version_b = summary.loc[summary["curriculum_version"] == "B"]

    if not version_a.empty and not version_b.empty:
        avg_a = float(version_a["avg_mastery"].iloc[0])
        avg_b = float(version_b["avg_mastery"].iloc[0])

        if avg_a > avg_b:
            leader_v, leader_score = "A", avg_a
            trailer_v, trailer_score = "B", avg_b
        elif avg_b > avg_a:
            leader_v, leader_score = "B", avg_b
            trailer_v, trailer_score = "A", avg_a
        else:
            leader_v = None

        if leader_v:
            diff = round(leader_score - trailer_score, 1)
            insight_card(
                "Curriculum Comparison",
                f"Version <b>{leader_v}</b> has a higher overall average mastery "
                f"score (<b>{leader_score:.1f}</b>) compared to Version "
                f"{trailer_v} ({trailer_score:.1f}), a difference of "
                f"<b>{diff}</b> points.",
                severity="info",
            )
        else:
            insight_card(
                "Curriculum Comparison",
                f"Both curriculum versions have identical average mastery scores "
                f"(<b>{avg_a:.1f}</b>).",
                severity="info",
            )
