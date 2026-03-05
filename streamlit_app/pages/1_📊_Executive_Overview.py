"""Executive Overview — drill-down hub for the Ed-Fi Lakehouse dashboard.

Levels: district -> school -> grade -> section -> student
Each level shows KPIs, charts, and a table to drill deeper.
"""

import sys
import os

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

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
    divider,
    inline_filters,
    breadcrumb,
    back_button,
    drill_into,
    subject_where,
    school_where,
    district_where,
    MASTERY_COLORS,
    RISK_COLORS,
    DISTRICT_COLORS,
    SUBJECT_COLORS,
)
from db import query  # noqa: E402

setup_page()

level = st.session_state.get("nav_level", "district")


# ── Helpers ────────────────────────────────────────────────────────────
def _grade_label(g) -> str:
    """Convert numeric grade to readable label."""
    if g is None or pd.isna(g):
        return "Unknown"
    g = int(g)
    return "Kindergarten" if g == 0 else f"Grade {g}"


def _safe_pct(num, denom) -> float:
    """Safe percentage calculation."""
    if denom is None or denom == 0:
        return 0.0
    return round(num / denom * 100, 1)


def _empty_guard(df: pd.DataFrame, label: str) -> bool:
    """Show warning and return True if the dataframe is empty."""
    if df is None or df.empty:
        st.warning(f"No data available for {label}.")
        return True
    return False


# =====================================================================
# DISTRICT LEVEL (default)
# =====================================================================
def render_district():
    """Top-level overview across all districts."""
    page_header(
        "Executive Overview",
        "K\u20135 elementary standards mastery across two interoperable school districts "
        "\u2014 Grand Bend ISD (Ed-Fi) and Riverside USD (OneRoster)",
    )

    inline_filters()

    # ── KPI row ──────────────────────────────────────────────────────
    try:
        subj_filter = subject_where("subject")
        students = int(query("SELECT COUNT(*) AS n FROM gold.dim_student").iloc[0, 0])
        schools = int(
            query("SELECT COUNT(DISTINCT school_id) AS n FROM gold.dim_school").iloc[0, 0]
        )
        standards = int(
            query(
                f"SELECT COUNT(DISTINCT standard_code) AS n FROM gold.dim_standard WHERE 1=1{subj_filter}"
            ).iloc[0, 0]
        )
        subjects = int(
            query("SELECT COUNT(DISTINCT subject) AS n FROM gold.dim_standard").iloc[0, 0]
        )
        quarantined = int(
            query("SELECT COUNT(*) AS n FROM gold.fact_dq_quarantine_log").iloc[0, 0]
        )
        total_processed = students + quarantined
        pass_rate = _safe_pct(total_processed - quarantined, total_processed)
    except Exception as exc:
        st.error(f"Failed to load KPI data: {exc}")
        st.stop()

    stat_row(
        [
            {"label": "Total Students", "value": f"{students:,}"},
            {"label": "Schools", "value": f"{schools:,}"},
            {"label": "Standards Tracked", "value": f"{standards:,}"},
            {"label": "Subjects", "value": f"{subjects:,}"},
            {"label": "DQ Pass Rate", "value": f"{pass_rate}%"},
        ]
    )

    divider()

    # ── District Mastery Comparison (stacked) ─────────────────────────
    section(
        "District Mastery Comparison",
        "Percentage of students Meeting or Exceeding standard, by subject",
    )

    try:
        mastery_df = query(
            f"""
            SELECT
                sch.district_name,
                std.subject,
                ROUND(AVG(CASE WHEN m.mastery_level IN ('Meeting', 'Exceeding')
                          THEN 1.0 ELSE 0.0 END) * 100, 1) AS mastery_pct
            FROM (
                SELECT student_id, standard_code, mastery_level,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m
            JOIN gold.dim_student stu ON stu.student_id = m.student_id
            JOIN gold.dim_school sch ON sch.school_id = stu.school_id
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE m.rn = 1{subject_where('std.subject')}
            GROUP BY sch.district_name, std.subject
            ORDER BY sch.district_name, std.subject
            """
        )

        overall_df = query(
            f"""
            SELECT
                std.subject,
                ROUND(AVG(CASE WHEN m.mastery_level IN ('Meeting', 'Exceeding')
                          THEN 1.0 ELSE 0.0 END) * 100, 1) AS mastery_pct
            FROM (
                SELECT student_id, standard_code, mastery_level,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE m.rn = 1{subject_where('std.subject')}
            GROUP BY std.subject
            """
        )
    except Exception as exc:
        st.error(f"Failed to load district comparison: {exc}")
        mastery_df = pd.DataFrame()
        overall_df = pd.DataFrame()

    if not _empty_guard(mastery_df, "district mastery comparison"):
        districts_list = mastery_df["district_name"].unique().tolist()

        for district in districts_list:
            d_df = mastery_df[mastery_df["district_name"] == district].copy()
            color = DISTRICT_COLORS.get(district, "#0D7377")

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=d_df["subject"],
                x=d_df["mastery_pct"],
                orientation="h",
                marker_color=color,
                text=d_df["mastery_pct"].apply(lambda v: f"{v:.1f}%"),
                textposition="outside",
                name=district,
            ))

            # Add overall average reference line per subject
            if not overall_df.empty:
                for _, row in overall_df.iterrows():
                    fig.add_vline(
                        x=row["mastery_pct"],
                        line_dash="dash",
                        line_color="#A0AEC0",
                        line_width=1,
                        annotation_text=f"Overall {row['mastery_pct']:.1f}%"
                        if row["subject"] == overall_df.iloc[0]["subject"] else None,
                        annotation_position="top",
                    )

            fig.update_layout(
                title=dict(text=district, font=dict(size=14)),
                height=180,
                xaxis=dict(range=[0, 105], title="% Meeting or Exceeding"),
                yaxis=dict(title=""),
                margin=dict(l=80, r=40, t=40, b=30),
                showlegend=False,
            )
            apply_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

        narrative(
            "Each bar shows the percentage of student-standard pairs rated "
            "<b>Meeting</b> or <b>Exceeding</b>. The dashed line marks the "
            "overall average across both districts for comparison."
        )

    divider()

    # ── At-Risk Student Spotlight ──────────────────────────────────────
    section("At-Risk Student Spotlight", "Students flagged by the early warning system")

    try:
        risk_df = query(
            """
            SELECT risk_level, COUNT(*) AS cnt
            FROM gold.agg_early_warning
            GROUP BY risk_level
            """
        )
    except Exception as exc:
        st.error(f"Failed to load risk data: {exc}")
        risk_df = pd.DataFrame()

    if not _empty_guard(risk_df, "risk data"):
        high = int(risk_df.loc[risk_df["risk_level"] == "High", "cnt"].sum())
        medium = int(risk_df.loc[risk_df["risk_level"] == "Medium", "cnt"].sum())
        low = int(risk_df.loc[risk_df["risk_level"] == "Low", "cnt"].sum())

        if high > 0:
            insight_card(
                "High-Risk Students",
                f"<b>{high}</b> student(s) flagged as <b>High</b> risk and "
                f"<b>{medium}</b> as <b>Medium</b> risk \u2014 primarily due to "
                f"low mastery scores and attendance below 90%. "
                f"<b>{low}</b> student(s) are Low risk. "
                f"See the <b>Early Warning</b> page for details.",
                severity="danger",
            )
        else:
            insight_card(
                "Risk Summary",
                f"<b>{medium}</b> student(s) at Medium risk, <b>{low}</b> at Low risk. "
                f"No students currently flagged as High risk.",
                severity="success",
            )

    divider()

    # ── Top / Bottom Performing Standards ──────────────────────────────
    section(
        "Standards Performance Highlights",
        "Highest and lowest mastery standards across all schools",
    )

    try:
        std_perf = query(
            f"""
            SELECT
                m.standard_code,
                std.subject,
                std.grade_level,
                ROUND(AVG(m.max_score_to_date), 1) AS avg_score
            FROM (
                SELECT student_id, standard_code, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE m.rn = 1{subject_where('std.subject')}
            GROUP BY m.standard_code, std.subject, std.grade_level
            ORDER BY avg_score DESC
            """
        )
    except Exception as exc:
        st.error(f"Failed to load standards performance: {exc}")
        std_perf = pd.DataFrame()

    if not _empty_guard(std_perf, "standards performance"):
        top3 = std_perf.head(3)
        bottom3 = std_perf.tail(3).iloc[::-1]  # lowest first

        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**Highest Mastery**")
            for _, r in top3.iterrows():
                gl = r["grade_level"]
                grade_str = "K" if gl == 0 else str(int(gl)) if pd.notna(gl) else "?"
                st.markdown(
                    f"<div class='insight-card success' style='padding:0.6rem 0.8rem;margin-bottom:0.4rem'>"
                    f"<span style='font-weight:600'>{r['subject']}</span> &middot; "
                    f"Grade {grade_str} &mdash; "
                    f"<code>{r['standard_code']}</code> &nbsp; <b>{r['avg_score']}%</b></div>",
                    unsafe_allow_html=True,
                )
        with col_r:
            st.markdown("**Standards Needing Attention**")
            for _, r in bottom3.iterrows():
                gl = r["grade_level"]
                grade_str = "K" if gl == 0 else str(int(gl)) if pd.notna(gl) else "?"
                st.markdown(
                    f"<div class='insight-card danger' style='padding:0.6rem 0.8rem;margin-bottom:0.4rem'>"
                    f"<span style='font-weight:600'>{r['subject']}</span> &middot; "
                    f"Grade {grade_str} &mdash; "
                    f"<code>{r['standard_code']}</code> &nbsp; <b>{r['avg_score']}%</b></div>",
                    unsafe_allow_html=True,
                )

    divider()

    # ── Explore by School ────────────────────────────────────────────
    section("Explore by School", "Select a school to drill down into grade and section analytics")

    try:
        school_df = query(
            f"""
            SELECT
                sch.school_id,
                sch.school_name,
                sch.district_name,
                sch.school_type,
                COUNT(DISTINCT stu.student_id) AS students,
                ROUND(AVG(m.max_score_to_date), 1) AS avg_score
            FROM gold.dim_school sch
            LEFT JOIN gold.dim_student stu ON stu.school_id = sch.school_id
            LEFT JOIN (
                SELECT student_id, standard_code, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            LEFT JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE 1=1{subject_where('std.subject')}
            GROUP BY sch.school_id, sch.school_name, sch.district_name, sch.school_type
            ORDER BY sch.district_name, sch.school_name
            """
        )
    except Exception as exc:
        st.error(f"Failed to load school data: {exc}")
        school_df = pd.DataFrame()

    if not _empty_guard(school_df, "schools"):
        display_df = school_df[["school_name", "district_name", "school_type", "students", "avg_score"]].copy()
        display_df.columns = ["School", "District", "Type", "Students", "Avg Score"]

        selected_school = st.selectbox(
            "Select a school to explore",
            options=["-- Choose --"] + school_df["school_name"].tolist(),
            key="exec_school_select",
        )

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        if selected_school != "-- Choose --":
            row = school_df[school_df["school_name"] == selected_school].iloc[0]
            drill_into(
                "school",
                nav_school_id=row["school_id"],
                nav_school_name=row["school_name"],
                nav_district=row["district_name"],
            )
            st.rerun()


# =====================================================================
# SCHOOL LEVEL
# =====================================================================
def render_school():
    """School-level view: KPIs, grade performance, drill into grades."""
    breadcrumb()
    back_button()

    school_id = st.session_state.get("nav_school_id")
    school_name = st.session_state.get("nav_school_name", "School")

    page_header(school_name, "School performance overview")

    # ── KPI row ──────────────────────────────────────────────────────
    try:
        stu_count = int(
            query(
                f"SELECT COUNT(DISTINCT student_id) AS n FROM gold.dim_student WHERE school_id = '{school_id}'"
            ).iloc[0, 0]
        )
        grades_served = int(
            query(
                f"SELECT COUNT(DISTINCT grade_level) AS n FROM gold.dim_student "
                f"WHERE school_id = '{school_id}' AND grade_level IS NOT NULL"
            ).iloc[0, 0]
        )
        avg_mastery = query(
            f"""
            SELECT ROUND(AVG(m.max_score_to_date), 1) AS avg_score
            FROM gold.dim_student stu
            JOIN (
                SELECT student_id, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            WHERE stu.school_id = '{school_id}'
            """
        ).iloc[0, 0]
        avg_mastery = round(float(avg_mastery), 1) if avg_mastery is not None and not pd.isna(avg_mastery) else 0.0

        att_df = query(
            f"""
            SELECT
                ROUND(
                    SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0), 1
                ) AS att_rate
            FROM gold.fact_attendance_daily
            WHERE school_id = '{school_id}'
            """
        )
        att_rate = round(float(att_df.iloc[0, 0]), 1) if att_df.iloc[0, 0] is not None and not pd.isna(att_df.iloc[0, 0]) else 0.0
    except Exception as exc:
        st.error(f"Failed to load school KPIs: {exc}")
        st.stop()

    stat_row(
        [
            {"label": "Students", "value": f"{stu_count:,}"},
            {"label": "Grades Served", "value": str(grades_served)},
            {"label": "Avg Mastery", "value": f"{avg_mastery}"},
            {"label": "Attendance Rate", "value": f"{att_rate}%"},
        ]
    )

    # ── Grade performance chart ──────────────────────────────────────
    section("Performance by Grade", "Average mastery score per grade and subject")

    try:
        grade_perf = query(
            f"""
            SELECT
                stu.grade_level,
                std.subject,
                ROUND(AVG(m.max_score_to_date), 1) AS avg_score
            FROM gold.dim_student stu
            JOIN (
                SELECT student_id, standard_code, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE stu.school_id = '{school_id}'
              AND stu.grade_level IS NOT NULL
              {subject_where('std.subject')}
            GROUP BY stu.grade_level, std.subject
            ORDER BY stu.grade_level, std.subject
            """
        )
    except Exception as exc:
        st.error(f"Failed to load grade performance: {exc}")
        grade_perf = pd.DataFrame()

    if not _empty_guard(grade_perf, "grade performance"):
        grade_perf["grade_label"] = grade_perf["grade_level"].apply(_grade_label)
        fig_grade = px.bar(
            grade_perf,
            x="grade_label",
            y="avg_score",
            color="subject",
            barmode="group",
            color_discrete_map=SUBJECT_COLORS,
            labels={"avg_score": "Avg Score", "grade_label": "Grade", "subject": "Subject"},
            text="avg_score",
        )
        fig_grade.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig_grade.update_layout(
            height=400,
            yaxis=dict(range=[0, 105]),
            xaxis=dict(categoryorder="array", categoryarray=[
                _grade_label(g) for g in sorted(grade_perf["grade_level"].unique())
            ]),
        )
        apply_theme(fig_grade)
        st.plotly_chart(fig_grade, use_container_width=True)

    # ── Drill into Grade ─────────────────────────────────────────────
    section("Drill into Grade", "Select a grade to explore further")

    try:
        grade_table = query(
            f"""
            SELECT
                stu.grade_level,
                COUNT(DISTINCT stu.student_id) AS students,
                ROUND(AVG(m.max_score_to_date), 1) AS avg_score
            FROM gold.dim_student stu
            LEFT JOIN (
                SELECT student_id, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            WHERE stu.school_id = '{school_id}'
              AND stu.grade_level IS NOT NULL
            GROUP BY stu.grade_level
            ORDER BY stu.grade_level
            """
        )
    except Exception as exc:
        st.error(f"Failed to load grade table: {exc}")
        grade_table = pd.DataFrame()

    if not _empty_guard(grade_table, "grades"):
        grade_table["grade_label"] = grade_table["grade_level"].apply(_grade_label)
        display_gt = grade_table[["grade_label", "students", "avg_score"]].copy()
        display_gt.columns = ["Grade", "Students", "Avg Score"]

        selected_grade = st.selectbox(
            "Select a grade",
            options=["-- Choose --"] + grade_table["grade_label"].tolist(),
            key="exec_grade_select",
        )

        st.dataframe(display_gt, use_container_width=True, hide_index=True)

        if selected_grade != "-- Choose --":
            row = grade_table[grade_table["grade_label"] == selected_grade].iloc[0]
            drill_into(
                "grade",
                nav_grade=int(row["grade_level"]),
            )
            st.rerun()


# =====================================================================
# GRADE LEVEL
# =====================================================================
def render_grade():
    """Grade-level view: KPIs, section comparison, drill into sections."""
    breadcrumb()
    back_button()

    school_id = st.session_state.get("nav_school_id")
    grade = st.session_state.get("nav_grade")
    grade_label = _grade_label(grade)

    page_header(grade_label, f"Performance within {st.session_state.get('nav_school_name', 'school')}")

    # ── KPI row ──────────────────────────────────────────────────────
    try:
        stu_count = int(
            query(
                f"SELECT COUNT(DISTINCT student_id) AS n FROM gold.dim_student "
                f"WHERE school_id = '{school_id}' AND grade_level = {grade}"
            ).iloc[0, 0]
        )
        sec_count = int(
            query(
                f"SELECT COUNT(DISTINCT section_id) AS n FROM gold.dim_section "
                f"WHERE school_id = '{school_id}' AND grade_level = {grade}"
            ).iloc[0, 0]
        )
        avg_score_df = query(
            f"""
            SELECT ROUND(AVG(m.max_score_to_date), 1) AS avg_score
            FROM gold.dim_student stu
            JOIN (
                SELECT student_id, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            WHERE stu.school_id = '{school_id}' AND stu.grade_level = {grade}
            """
        )
        avg_score = round(float(avg_score_df.iloc[0, 0]), 1) if avg_score_df.iloc[0, 0] is not None and not pd.isna(avg_score_df.iloc[0, 0]) else 0.0
    except Exception as exc:
        st.error(f"Failed to load grade KPIs: {exc}")
        st.stop()

    stat_row(
        [
            {"label": "Students", "value": f"{stu_count:,}"},
            {"label": "Sections", "value": f"{sec_count:,}"},
            {"label": "Avg Score", "value": f"{avg_score}"},
        ]
    )

    # ── Section comparison chart ─────────────────────────────────────
    section("Section Comparison", "Average mastery score by section")

    try:
        sec_perf = query(
            f"""
            SELECT
                sec.section_id,
                sec.course_name,
                sec.subject,
                ROUND(AVG(m.max_score_to_date), 1) AS avg_score,
                COUNT(DISTINCT stu.student_id) AS students
            FROM gold.dim_section sec
            JOIN gold.dim_student stu ON stu.school_id = sec.school_id
                AND stu.grade_level = sec.grade_level
            JOIN (
                SELECT student_id, standard_code, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
                AND std.subject = sec.subject
            WHERE sec.school_id = '{school_id}'
              AND sec.grade_level = {grade}
              {subject_where('sec.subject')}
            GROUP BY sec.section_id, sec.course_name, sec.subject
            ORDER BY sec.course_name
            """
        )
    except Exception as exc:
        st.error(f"Failed to load section performance: {exc}")
        sec_perf = pd.DataFrame()

    if not _empty_guard(sec_perf, "section performance"):
        fig_sec = px.bar(
            sec_perf,
            x="course_name",
            y="avg_score",
            color="subject",
            color_discrete_map=SUBJECT_COLORS,
            labels={"avg_score": "Avg Score", "course_name": "Section", "subject": "Subject"},
            text="avg_score",
        )
        fig_sec.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig_sec.update_layout(
            height=400,
            yaxis=dict(range=[0, 105]),
            xaxis=dict(tickangle=-45),
        )
        apply_theme(fig_sec)
        st.plotly_chart(fig_sec, use_container_width=True)

    # ── Drill into Section ───────────────────────────────────────────
    section("Drill into Section", "Select a section to explore")

    try:
        sec_table = query(
            f"""
            SELECT
                sec.section_id,
                sec.course_name,
                sec.subject,
                sec.curriculum_version,
                COUNT(DISTINCT stu.student_id) AS students
            FROM gold.dim_section sec
            LEFT JOIN gold.dim_student stu ON stu.school_id = sec.school_id
                AND stu.grade_level = sec.grade_level
            WHERE sec.school_id = '{school_id}'
              AND sec.grade_level = {grade}
              {subject_where('sec.subject')}
            GROUP BY sec.section_id, sec.course_name, sec.subject, sec.curriculum_version
            ORDER BY sec.course_name
            """
        )
    except Exception as exc:
        st.error(f"Failed to load section table: {exc}")
        sec_table = pd.DataFrame()

    if not _empty_guard(sec_table, "sections"):
        display_st = sec_table[["course_name", "subject", "curriculum_version", "students"]].copy()
        display_st.columns = ["Section", "Subject", "Curriculum", "Students"]

        selected_section = st.selectbox(
            "Select a section",
            options=["-- Choose --"] + sec_table["course_name"].tolist(),
            key="exec_section_select",
        )

        st.dataframe(display_st, use_container_width=True, hide_index=True)

        if selected_section != "-- Choose --":
            row = sec_table[sec_table["course_name"] == selected_section].iloc[0]
            drill_into(
                "section",
                nav_section_id=row["section_id"],
                nav_section_name=row["course_name"],
            )
            st.rerun()


# =====================================================================
# SECTION LEVEL
# =====================================================================
def render_section():
    """Section-level view: KPIs, student list, drill into student."""
    breadcrumb()
    back_button()

    section_id = st.session_state.get("nav_section_id")
    section_name = st.session_state.get("nav_section_name", "Section")

    page_header(section_name, "Section roster and performance")

    # ── Look up section details ──────────────────────────────────────
    try:
        sec_info = query(
            f"SELECT subject, grade_level, school_id FROM gold.dim_section WHERE section_id = '{section_id}' LIMIT 1"
        )
        if sec_info.empty:
            st.error("Section not found.")
            st.stop()
        sec_subject = sec_info.iloc[0]["subject"]
        sec_grade = sec_info.iloc[0]["grade_level"]
        sec_school = sec_info.iloc[0]["school_id"]
    except Exception as exc:
        st.error(f"Failed to load section info: {exc}")
        st.stop()

    # ── Get students in this section (via school + grade) ────────────
    try:
        student_list = query(
            f"""
            SELECT
                stu.student_id,
                stu.grade_level,
                ROUND(AVG(m.max_score_to_date), 1) AS avg_score,
                COUNT(DISTINCT m.standard_code) AS standards_assessed
            FROM gold.dim_student stu
            JOIN (
                SELECT student_id, standard_code, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
            ) m ON m.student_id = stu.student_id AND m.rn = 1
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE stu.school_id = '{sec_school}'
              AND stu.grade_level = {sec_grade}
              AND std.subject = '{sec_subject}'
            GROUP BY stu.student_id, stu.grade_level
            ORDER BY avg_score ASC
            """
        )
    except Exception as exc:
        st.error(f"Failed to load student list: {exc}")
        student_list = pd.DataFrame()

    # ── KPI row ──────────────────────────────────────────────────────
    stu_count = len(student_list) if not student_list.empty else 0
    avg_sec_score = round(float(student_list["avg_score"].mean()), 1) if stu_count > 0 else 0.0

    stat_row(
        [
            {"label": "Students", "value": f"{stu_count:,}"},
            {"label": "Avg Score", "value": f"{avg_sec_score}"},
            {"label": "Subject", "value": sec_subject},
        ]
    )

    # ── Student performance table ────────────────────────────────────
    section("Student Roster", "Select a student to view detailed performance")

    if not _empty_guard(student_list, "students in this section"):
        display_sl = student_list[["student_id", "avg_score", "standards_assessed"]].copy()
        display_sl.columns = ["Student ID", "Avg Score", "Standards Assessed"]

        selected_student = st.selectbox(
            "Select a student",
            options=["-- Choose --"] + student_list["student_id"].tolist(),
            key="exec_student_select",
        )

        st.dataframe(display_sl, use_container_width=True, hide_index=True)

        if selected_student != "-- Choose --":
            drill_into("student", nav_student_id=selected_student)
            st.rerun()


# =====================================================================
# STUDENT LEVEL
# =====================================================================
def render_student():
    """Student-level view: KPIs, per-standard mastery, assessment details."""
    breadcrumb()
    back_button()

    student_id = st.session_state.get("nav_student_id")

    page_header(f"Student {student_id}", "Individual performance profile")

    # ── KPI row ──────────────────────────────────────────────────────
    try:
        avg_score_df = query(
            f"""
            SELECT ROUND(AVG(max_score_to_date), 1) AS avg_score
            FROM (
                SELECT student_id, standard_code, max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
                WHERE student_id = '{student_id}'
            ) sub
            WHERE rn = 1
            """
        )
        avg_score = round(float(avg_score_df.iloc[0, 0]), 1) if avg_score_df.iloc[0, 0] is not None and not pd.isna(avg_score_df.iloc[0, 0]) else 0.0

        risk_df = query(
            f"SELECT risk_level, attendance_rate FROM gold.agg_early_warning WHERE student_id = '{student_id}' LIMIT 1"
        )
        if not risk_df.empty:
            risk_level = risk_df.iloc[0]["risk_level"]
            att_rate = round(float(risk_df.iloc[0]["attendance_rate"]) * 100, 1) if risk_df.iloc[0]["attendance_rate"] is not None else 0.0
        else:
            risk_level = "N/A"
            att_rate = 0.0
    except Exception as exc:
        st.error(f"Failed to load student KPIs: {exc}")
        st.stop()

    risk_severity = {"High": "danger", "Medium": "warning", "Low": "success"}.get(risk_level, "info")

    stat_row(
        [
            {"label": "Avg Score", "value": f"{avg_score}"},
            {"label": "Risk Level", "value": risk_level},
            {"label": "Attendance Rate", "value": f"{att_rate}%"},
        ]
    )

    if risk_level in ("High", "Medium"):
        insight_card(
            "At-Risk Student",
            f"This student has a <b>{risk_level}</b> risk level. "
            f"Average mastery score is <b>{avg_score}</b> with an attendance rate of <b>{att_rate}%</b>.",
            severity=risk_severity,
        )

    # ── Per-standard mastery bar chart ───────────────────────────────
    section("Mastery by Standard", "Score for each standard assessed")

    try:
        std_mastery = query(
            f"""
            SELECT m.standard_code, m.max_score_to_date AS score, m.mastery_level,
                   std.subject
            FROM (
                SELECT student_id, standard_code, max_score_to_date, mastery_level,
                       ROW_NUMBER() OVER (
                           PARTITION BY student_id, standard_code
                           ORDER BY assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily
                WHERE student_id = '{student_id}'
            ) m
            JOIN gold.dim_standard std ON std.standard_code = m.standard_code
            WHERE m.rn = 1
            ORDER BY std.subject, m.max_score_to_date ASC
            """
        )
    except Exception as exc:
        st.error(f"Failed to load standard mastery: {exc}")
        std_mastery = pd.DataFrame()

    if not _empty_guard(std_mastery, "standard mastery"):
        fig_std = px.bar(
            std_mastery,
            y="standard_code",
            x="score",
            color="mastery_level",
            orientation="h",
            color_discrete_map=MASTERY_COLORS,
            labels={"score": "Score", "standard_code": "Standard", "mastery_level": "Level"},
            text="score",
        )
        fig_std.update_traces(texttemplate="%{text:.0f}", textposition="outside")
        fig_std.update_layout(
            height=max(350, len(std_mastery) * 35),
            xaxis=dict(range=[0, 105]),
            yaxis=dict(autorange="reversed"),
            showlegend=True,
            legend=dict(title="Mastery Level"),
        )
        apply_theme(fig_std)
        st.plotly_chart(fig_std, use_container_width=True)

    # ── Assessment detail table ──────────────────────────────────────
    section("Assessment Responses", "Question-by-question detail")

    try:
        assess_df = query(
            f"""
            SELECT
                r.assessment_id,
                r.standard_code,
                r.question_number,
                r.correct_answer,
                r.student_answer,
                r.is_correct,
                r.score,
                r.misconception_tag
            FROM gold.fact_assessment_responses r
            WHERE r.student_id = '{student_id}'
            ORDER BY r.assessment_id, r.question_number
            """
        )
    except Exception as exc:
        st.error(f"Failed to load assessment responses: {exc}")
        assess_df = pd.DataFrame()

    if not _empty_guard(assess_df, "assessment responses"):
        display_ad = assess_df.copy()
        display_ad["is_correct"] = display_ad["is_correct"].map({True: "Correct", False: "Incorrect"})
        display_ad.columns = [
            "Assessment", "Standard", "Q#", "Correct Ans",
            "Student Ans", "Result", "Score", "Misconception"
        ]
        st.dataframe(display_ad, use_container_width=True, hide_index=True)

        # Summary insight
        total_q = len(assess_df)
        correct_q = int(assess_df["is_correct"].sum())
        accuracy = _safe_pct(correct_q, total_q)
        misconceptions = int(assess_df["misconception_tag"].notna().sum())

        insight_card(
            "Assessment Summary",
            f"Answered <b>{correct_q}</b> of <b>{total_q}</b> questions correctly (<b>{accuracy}%</b> accuracy). "
            f"<b>{misconceptions}</b> response(s) flagged with misconception patterns.",
            severity="success" if accuracy >= 75 else "warning",
        )


# =====================================================================
# Router
# =====================================================================
_RENDERERS = {
    "district": render_district,
    "school": render_school,
    "grade": render_grade,
    "section": render_section,
    "student": render_student,
}

renderer = _RENDERERS.get(level, render_district)
renderer()
