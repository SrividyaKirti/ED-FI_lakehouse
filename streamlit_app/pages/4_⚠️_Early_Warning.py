"""Early Warning & Attendance — risk identification and attendance analytics."""

import sys
import os

_app_dir = os.path.join(os.path.dirname(__file__), "..")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

import streamlit as st  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import pandas as pd  # noqa: E402

from db import query  # noqa: E402
from components import (  # noqa: E402
    setup_page,
    inline_filters,
    page_header,
    section,
    narrative,
    stat_row,
    insight_card,
    apply_theme,
    RISK_COLORS,
    school_where,
    district_where,
)

setup_page()
inline_filters()

page_header(
    "Early Warning & Attendance",
    "Identify at-risk students and surface attendance patterns across schools",
)

# ── Sidebar filter helpers ─────────────────────────────────────────
# Build WHERE clauses that respect sidebar selections.
# agg_early_warning has school_id but not district_name, so we join
# to dim_school when a district filter is active.
_school_filter = school_where("ew.school_id")
_district_filter = district_where("s.district_name")

# We need the join only when a filter references dim_school columns.
_needs_school_join = bool(_district_filter)


def _ew_from_clause() -> str:
    """Return the FROM clause for early-warning queries."""
    base = "gold.agg_early_warning ew"
    if _needs_school_join:
        base += " JOIN gold.dim_school s ON ew.school_id = s.school_id"
    return base


def _ew_where() -> str:
    """Combined WHERE for early-warning queries."""
    return f"WHERE 1=1{_school_filter}{_district_filter}"


# ── 1. KPI Metrics ────────────────────────────────────────────────
section("Key Indicators")

try:
    kpi = query(f"""
        SELECT
            COUNT(*) FILTER (WHERE ew.risk_level = 'High')   AS at_risk,
            ROUND(AVG(ew.attendance_rate) * 100, 1)          AS avg_attendance,
            COUNT(*) FILTER (WHERE ew.count_below_developing >= 2) AS declining_mastery,
            COUNT(DISTINCT ew.school_id)                     AS schools
        FROM {_ew_from_clause()}
        {_ew_where()}
    """)

    at_risk = int(kpi["at_risk"].iloc[0])
    avg_att = float(kpi["avg_attendance"].iloc[0]) if pd.notna(kpi["avg_attendance"].iloc[0]) else 0.0
    declining = int(kpi["declining_mastery"].iloc[0])
    school_count = int(kpi["schools"].iloc[0])

    stat_row([
        {"label": "High-Risk Students", "value": f"{at_risk:,}"},
        {"label": "Avg Attendance Rate", "value": f"{avg_att}%"},
        {"label": "Declining Mastery (2+)", "value": f"{declining:,}"},
        {"label": "Schools Represented", "value": f"{school_count:,}"},
    ])

    if at_risk > 0:
        total_students = int(query(f"""
            SELECT COUNT(*) AS n FROM {_ew_from_clause()} {_ew_where()}
        """).iloc[0, 0])
        at_risk_pct = round(at_risk / max(total_students, 1) * 100, 1)
        insight_card(
            "Risk Summary",
            f"<b>{at_risk_pct}%</b> of students ({at_risk:,} of {total_students:,}) "
            f"are classified as <em>High</em> risk. "
            f"<b>{declining:,}</b> students have two or more standards below Developing.",
            severity="danger" if at_risk_pct > 20 else "warning",
        )
except Exception as exc:
    st.error(f"Failed to load KPI data: {exc}")
    st.stop()

# ── 2. Risk Distribution ──────────────────────────────────────────
section("Risk Distribution", "Students grouped by risk level")

narrative(
    "The chart below shows how students are distributed across risk tiers. "
    "High-risk students have low attendance <em>and</em> low mastery scores; "
    "medium-risk students fall short on one dimension."
)

try:
    risk_df = query(f"""
        SELECT ew.risk_level, COUNT(*) AS count
        FROM {_ew_from_clause()}
        {_ew_where()}
        GROUP BY ew.risk_level
    """)

    if risk_df.empty:
        st.info("No risk data available for the selected filters.")
    else:
        risk_order = ["High", "Medium", "Low"]
        risk_df["risk_level"] = pd.Categorical(
            risk_df["risk_level"], categories=risk_order, ordered=True
        )
        risk_df = risk_df.sort_values("risk_level")

        fig_risk = go.Figure(
            go.Pie(
                labels=risk_df["risk_level"],
                values=risk_df["count"],
                hole=0.5,
                marker=dict(
                    colors=[RISK_COLORS.get(r, "#CBD5E0") for r in risk_df["risk_level"]]
                ),
                textinfo="label+percent",
                hovertemplate="%{label}: %{value:,} students (%{percent})<extra></extra>",
            )
        )
        fig_risk.update_layout(title_text="Risk Level Distribution", height=400)
        apply_theme(fig_risk)
        st.plotly_chart(fig_risk, use_container_width=True)

except Exception as exc:
    st.error(f"Failed to load risk distribution: {exc}")

# ── 3. Risk Factor Scatter ────────────────────────────────────────
section(
    "Risk Factor Scatter",
    "Attendance rate vs. mastery score — each dot is a student",
)

narrative(
    "Students in the lower-left quadrant have both low attendance and low "
    "mastery, placing them at highest risk. Reference lines mark the 90% "
    "attendance threshold and a mastery score of 70."
)

try:
    scatter_df = query(f"""
        SELECT
            ew.student_id,
            ew.risk_level,
            ROUND(ew.attendance_rate * 100, 1)  AS attendance_pct,
            ROUND(ew.avg_mastery_score, 1)      AS mastery_score,
            ew.count_below_developing
        FROM {_ew_from_clause()}
        {_ew_where()}
    """)

    if scatter_df.empty:
        st.info("No student-level data available for the selected filters.")
    else:
        # Ensure risk_level ordering for the legend
        scatter_df["risk_level"] = pd.Categorical(
            scatter_df["risk_level"],
            categories=["High", "Medium", "Low"],
            ordered=True,
        )

        fig_scatter = px.scatter(
            scatter_df,
            x="attendance_pct",
            y="mastery_score",
            color="risk_level",
            size="count_below_developing",
            size_max=18,
            color_discrete_map=RISK_COLORS,
            hover_data={
                "student_id": True,
                "attendance_pct": ":.1f",
                "mastery_score": ":.1f",
                "count_below_developing": True,
                "risk_level": True,
            },
            labels={
                "attendance_pct": "Attendance Rate (%)",
                "mastery_score": "Avg Mastery Score",
                "risk_level": "Risk Level",
                "count_below_developing": "Standards Below Developing",
            },
        )

        # Quadrant reference lines
        fig_scatter.add_hline(
            y=70, line_dash="dash", line_color="#A0AEC0",
            annotation_text="Mastery = 70", annotation_position="top left",
        )
        fig_scatter.add_vline(
            x=90, line_dash="dash", line_color="#A0AEC0",
            annotation_text="Attendance = 90%", annotation_position="top right",
        )

        fig_scatter.update_layout(
            title_text="Attendance vs. Mastery by Risk Level",
            height=500,
        )
        apply_theme(fig_scatter)
        st.plotly_chart(fig_scatter, use_container_width=True)

except Exception as exc:
    st.error(f"Failed to load risk scatter data: {exc}")

# ── 4. Attendance Patterns ────────────────────────────────────────
section("Attendance Patterns by School", "Status breakdown across schools")

narrative(
    "Stacked bars show the volume of each attendance status per school. "
    "Schools with a disproportionate share of absences or tardies may "
    "warrant closer investigation."
)

try:
    # Build attendance query with school/district filters
    att_from = "gold.fact_attendance_daily a JOIN gold.dim_school s ON a.school_id = s.school_id"
    att_where = f"WHERE 1=1{school_where('a.school_id')}{district_where('s.district_name')}"

    att_df = query(f"""
        SELECT
            s.school_name,
            a.attendance_status,
            COUNT(*) AS count
        FROM {att_from}
        {att_where}
        GROUP BY s.school_name, a.attendance_status
        ORDER BY s.school_name
    """)

    if att_df.empty:
        st.info("No attendance data available for the selected filters.")
    else:
        status_colors = {
            "Present": "#38A169",
            "Absent": "#E53E3E",
            "Tardy": "#ED8936",
            "Excused Absence": "#3182CE",
        }

        fig_att = px.bar(
            att_df,
            x="school_name",
            y="count",
            color="attendance_status",
            color_discrete_map=status_colors,
            barmode="stack",
            labels={
                "school_name": "School",
                "count": "Records",
                "attendance_status": "Status",
            },
        )
        fig_att.update_layout(
            title_text="Attendance Status by School",
            height=450,
            xaxis_tickangle=-30,
        )
        apply_theme(fig_att)
        st.plotly_chart(fig_att, use_container_width=True)

        # Compute absence rate insight
        total_att = int(att_df["count"].sum())
        absent_count = int(
            att_df.loc[att_df["attendance_status"] == "Absent", "count"].sum()
        )
        absent_pct = round(absent_count / max(total_att, 1) * 100, 1)

        insight_card(
            "Attendance Insight",
            f"Across the selected scope, <b>{absent_pct}%</b> of daily records "
            f"are unexcused absences ({absent_count:,} of {total_att:,} records).",
            severity="success" if absent_pct < 5 else "warning",
        )

except Exception as exc:
    st.error(f"Failed to load attendance data: {exc}")

# ── 5. Instructional Grouping Table ───────────────────────────────
section(
    "Instructional Grouping",
    "Student roster sorted by risk level for targeted intervention planning",
)

narrative(
    "Use this table to identify students who need immediate support. "
    "High-risk students appear first. Filter or sort columns to build "
    "intervention groups."
)

try:
    roster_df = query(f"""
        SELECT
            ew.student_id,
            s.school_name,
            ew.risk_level,
            ROUND(ew.attendance_rate * 100, 1)  AS attendance_pct,
            ROUND(ew.avg_mastery_score, 1)      AS avg_mastery_score,
            ew.count_below_developing
        FROM gold.agg_early_warning ew
        JOIN gold.dim_school s ON ew.school_id = s.school_id
        WHERE 1=1{school_where('ew.school_id')}{district_where('s.district_name')}
        ORDER BY
            CASE ew.risk_level
                WHEN 'High'   THEN 1
                WHEN 'Medium' THEN 2
                WHEN 'Low'    THEN 3
                ELSE 4
            END,
            ew.attendance_rate ASC
    """)

    if roster_df.empty:
        st.info("No students match the selected filters.")
    else:
        roster_df = roster_df.rename(columns={
            "student_id": "Student ID",
            "school_name": "School",
            "risk_level": "Risk Level",
            "attendance_pct": "Attendance %",
            "avg_mastery_score": "Avg Mastery",
            "count_below_developing": "Below Developing",
        })

        st.dataframe(
            roster_df,
            use_container_width=True,
            hide_index=True,
            height=400,
        )

        high_count = int((roster_df["Risk Level"] == "High").sum())
        med_count = int((roster_df["Risk Level"] == "Medium").sum())

        insight_card(
            "Grouping Summary",
            f"<b>{high_count}</b> students are in the High-risk group and "
            f"<b>{med_count}</b> in Medium-risk. Consider forming small-group "
            f"intervention cohorts based on shared risk factors.",
            severity="danger" if high_count > 10 else "info",
        )

except Exception as exc:
    st.error(f"Failed to load student roster: {exc}")
