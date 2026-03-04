"""Classroom Insights -- student mastery tracking and early intervention.

Migrated from ``tabs/classroom_insights.py`` with enhanced narratives,
dynamic insight cards, and design-system theming.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import pandas as pd  # noqa: E402
from db import query  # noqa: E402
from components import (  # noqa: E402
    inject_css,
    page_header,
    section,
    narrative,
    stat_row,
    insight_card,
    apply_theme,
    MASTERY_COLORS,
    RISK_COLORS,
)


# ── CSS injection (each page must call this independently) ────────────
inject_css()


# ── Page header ───────────────────────────────────────────────────────
page_header(
    "Classroom Insights",
    "Student mastery tracking and early intervention",
)


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
# KPI Summary Row
# ---------------------------------------------------------------------------

try:
    _kpi_sql = f"""
        SELECT
            AVG(max_score_to_date) AS avg_mastery,
            COUNT(CASE WHEN max_score_to_date >= 70 THEN 1 END) * 100.0
                / NULLIF(COUNT(*), 0) AS pct_above,
            COUNT(CASE WHEN max_score_to_date < 70 THEN 1 END) * 100.0
                / NULLIF(COUNT(*), 0) AS pct_below,
            COUNT(CASE WHEN mastery_level = 'Needs Intervention' THEN 1 END)
                AS intervention_count
        FROM (
            SELECT student_id, standard_code, max_score_to_date, mastery_level,
                   ROW_NUMBER() OVER (
                       PARTITION BY student_id, standard_code
                       ORDER BY assessment_count DESC
                   ) AS rn
            FROM gold.fact_student_mastery_daily
            WHERE 1=1 {_source_clause("fact_student_mastery_daily")}
        ) sub
        WHERE rn = 1
    """
    _kpi = query(_kpi_sql)
    _avg_mastery = round(float(_kpi["avg_mastery"].iloc[0]), 1)
    _pct_above = round(float(_kpi["pct_above"].iloc[0]), 1)
    _pct_below = round(float(_kpi["pct_below"].iloc[0]), 1)
    _intervention_n = int(_kpi["intervention_count"].iloc[0])
except Exception as exc:
    st.error(f"Failed to load KPI data: {exc}")
    st.stop()

stat_row(
    [
        {"label": "Avg Mastery Score", "value": f"{_avg_mastery}"},
        {"label": "% Above Mastery", "value": f"{_pct_above}%"},
        {"label": "% Below Mastery", "value": f"{_pct_below}%"},
        {
            "label": "Needs Intervention",
            "value": f"{_intervention_n:,}",
        },
    ]
)


# ---------------------------------------------------------------------------
# 1. Mastery Heatmap
# ---------------------------------------------------------------------------

section("Mastery Heatmap")
narrative(
    "This heatmap shows each student's latest score per standard. "
    "Red cells signal where targeted intervention is needed."
)

source_filter = _source_clause("m")

_heatmap_sql = f"""
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
    heatmap_df = query(_heatmap_sql)
except Exception as exc:
    st.error(f"Failed to load mastery data: {exc}")
    st.stop()

if heatmap_df.empty:
    st.info("No mastery data available for the selected filter.")
else:
    # De-duplicate: keep highest max_score_to_date per student+standard
    heatmap_df = (
        heatmap_df.sort_values("max_score_to_date", ascending=False)
        .drop_duplicates(subset=["student_id", "standard_code"], keep="first")
    )

    # Pivot to matrix: students (rows) x standards (cols)
    pivot = heatmap_df.pivot_table(
        index="student_id",
        columns="standard_code",
        values="max_score_to_date",
        aggfunc="max",
    )

    # Sort columns by grade level embedded in standard code
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)

    # Color scale using design-system tokens:
    # Needs Intervention (<50) danger, Developing (50-69) warning,
    # Meeting (70-89) secondary, Exceeding (>=90) success
    color_scale = [
        [0.0, MASTERY_COLORS["Needs Intervention"]],   # danger
        [0.499, MASTERY_COLORS["Needs Intervention"]],
        [0.50, MASTERY_COLORS["Developing"]],           # warning
        [0.699, MASTERY_COLORS["Developing"]],
        [0.70, MASTERY_COLORS["Meeting"]],              # secondary
        [0.899, MASTERY_COLORS["Meeting"]],
        [0.90, MASTERY_COLORS["Exceeding"]],            # success
        [1.0, MASTERY_COLORS["Exceeding"]],
    ]

    fig_heatmap = go.Figure(
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
    fig_heatmap.update_layout(
        xaxis_title="Standard",
        yaxis_title="Student (last 6 chars)",
        height=max(350, len(pivot) * 14),
        margin=dict(l=100, r=20, t=30, b=80),
    )
    apply_theme(fig_heatmap)
    st.plotly_chart(fig_heatmap, use_container_width=True)

    # Legend below chart
    legend_cols = st.columns(4)
    labels = [
        ("Needs Intervention", "0 -- 49", MASTERY_COLORS["Needs Intervention"]),
        ("Developing", "50 -- 69", MASTERY_COLORS["Developing"]),
        ("Meeting", "70 -- 89", MASTERY_COLORS["Meeting"]),
        ("Exceeding", "90 -- 100", MASTERY_COLORS["Exceeding"]),
    ]
    for col, (label, rng, color) in zip(legend_cols, labels):
        col.markdown(
            f"<span style='color:{color}; font-weight:bold;'>{label}</span> ({rng})",
            unsafe_allow_html=True,
        )

    # Dynamic insight: standard with the most students below mastery threshold
    below_mastery = heatmap_df[heatmap_df["max_score_to_date"] < 70]
    if not below_mastery.empty:
        worst_standard = (
            below_mastery.groupby("standard_code")["student_id"]
            .nunique()
            .sort_values(ascending=False)
            .head(1)
        )
        worst_code = worst_standard.index[0]
        worst_count = int(worst_standard.iloc[0])
        insight_card(
            "Highest-Need Standard",
            f"<b>{worst_code}</b> has <b>{worst_count}</b> students scoring below "
            f"the mastery threshold (70). Consider prioritizing re-teach resources "
            f"for this standard.",
            severity="warning",
        )
    else:
        insight_card(
            "Strong Mastery",
            "All students are at or above the mastery threshold across all "
            "standards in the current view.",
            severity="success",
        )


# ---------------------------------------------------------------------------
# 2. Early Warning System
# ---------------------------------------------------------------------------

section("Early Warning System")
narrative(
    "Students plotted by attendance rate vs. mastery score. "
    "The bottom-left quadrant represents the highest-risk students."
)

# Early warning only has oneroster students currently;
# still apply source filter via join to dim_student
_ew_src = st.session_state.get("source_filter")
if _ew_src:
    _ew_join = (
        f"INNER JOIN gold.dim_student s ON ew.student_id = s.student_id "
        f"AND s._source_system = '{_ew_src}'"
    )
else:
    _ew_join = "LEFT JOIN gold.dim_student s ON ew.student_id = s.student_id"

_ew_sql = f"""
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
    {_ew_join}
    ORDER BY ew.risk_level DESC, ew.avg_mastery_score ASC
"""

try:
    ew_df = query(_ew_sql)
except Exception as exc:
    st.error(f"Failed to load early-warning data: {exc}")
    st.stop()

if ew_df.empty:
    st.info("No early-warning data available for the selected filter.")
else:
    fig_ew = px.scatter(
        ew_df,
        x="attendance_rate",
        y="avg_mastery_score",
        color="risk_level",
        color_discrete_map=RISK_COLORS,
        hover_data=["student_id", "grade_level", "count_below_developing"],
        labels={
            "attendance_rate": "Attendance Rate",
            "avg_mastery_score": "Avg Mastery Score",
            "risk_level": "Risk Level",
        },
    )
    fig_ew.update_layout(height=450)
    apply_theme(fig_ew)
    st.plotly_chart(fig_ew, use_container_width=True)

    # Summary metrics using stat_row instead of raw st.metric
    _total_students = len(ew_df)
    _high_count = len(ew_df[ew_df["risk_level"] == "High"])
    _med_count = len(ew_df[ew_df["risk_level"] == "Medium"])

    stat_row(
        [
            {"label": "Total Students", "value": f"{_total_students:,}"},
            {"label": "Medium Risk", "value": f"{_med_count:,}"},
            {"label": "High Risk", "value": f"{_high_count:,}"},
        ]
    )

    # Table of at-risk students
    at_risk = ew_df[ew_df["risk_level"].isin(["High", "Medium"])].copy()
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

    # Dynamic insight: students with BOTH attendance < 80% AND mastery < 60%
    _dual_risk = ew_df[
        (ew_df["attendance_rate"] < 80) & (ew_df["avg_mastery_score"] < 60)
    ]
    _dual_count = len(_dual_risk)
    if _dual_count > 0:
        insight_card(
            "Dual-Risk Alert",
            f"<b>{_dual_count}</b> student{'s' if _dual_count != 1 else ''} "
            f"{'have' if _dual_count != 1 else 'has'} both attendance below 80% "
            f"and mastery below 60%. These students are at the highest risk and "
            f"need immediate, coordinated intervention.",
            severity="danger",
        )
    else:
        insight_card(
            "No Dual-Risk Students",
            "No students currently have both attendance below 80% and mastery "
            "below 60% -- a positive sign for early intervention efforts.",
            severity="success",
        )


# ---------------------------------------------------------------------------
# 3. Misconception Clusters
# ---------------------------------------------------------------------------

section("Misconception Clusters")
narrative(
    "Common wrong-answer patterns that reveal systematic misunderstandings, "
    "grouped by standard."
)

_misc_source_filter = _source_clause("r")

_misc_sql = f"""
    SELECT r.standard_code,
           r.misconception_tag,
           r.misconception_description,
           r.suggested_reteach,
           COUNT(DISTINCT r.student_id) as affected_students,
           COUNT(*) as total_occurrences
    FROM gold.fact_assessment_responses r
    WHERE r.misconception_tag IS NOT NULL
      AND r.is_correct = false
      {_misc_source_filter}
    GROUP BY r.standard_code, r.misconception_tag,
             r.misconception_description, r.suggested_reteach
    ORDER BY affected_students DESC
"""

try:
    misc_df = query(_misc_sql)
except Exception as exc:
    st.error(f"Failed to load misconception data: {exc}")
    st.stop()

if misc_df.empty:
    st.info("No misconception patterns detected for the selected filter.")
else:
    # Dynamic insight at top: most common misconception
    top_misc = misc_df.iloc[0]
    insight_card(
        "Most Common Misconception",
        f"<b>{top_misc['misconception_tag']}</b> in standard "
        f"<b>{top_misc['standard_code']}</b> affects "
        f"<b>{int(top_misc['affected_students'])}</b> students "
        f"with {int(top_misc['total_occurrences'])} total occurrences. "
        f"{top_misc['misconception_description']}",
        severity="warning",
    )

    for _, row in misc_df.iterrows():
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
# 4. Standards Dependency Chain
# ---------------------------------------------------------------------------

section("Standards Dependency Chain")
narrative(
    "Prerequisite graph of CCSS math standards. Node colors reflect "
    "class-average mastery."
)

_dep_source_filter = _source_clause("m")

try:
    standards = query(
        "SELECT standard_code, prerequisite_standard_code, grade_level "
        "FROM gold.dim_standard ORDER BY grade_level"
    )

    # Average mastery per standard (latest per student, then avg)
    _avg_sql = f"""
        SELECT standard_code, AVG(max_score_to_date) as avg_score
        FROM (
            SELECT m.student_id, m.standard_code, m.max_score_to_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.student_id, m.standard_code
                       ORDER BY m.assessment_count DESC
                   ) as rn
            FROM gold.fact_student_mastery_daily m
            WHERE 1=1 {_dep_source_filter}
        ) sub
        WHERE rn = 1
        GROUP BY standard_code
    """
    avg_mastery = query(_avg_sql)
except Exception as exc:
    st.error(f"Failed to load standards data: {exc}")
    st.stop()

score_map = dict(zip(avg_mastery["standard_code"], avg_mastery["avg_score"]))


def _node_color(code: str) -> str:
    score = score_map.get(code)
    if score is None:
        return "#cccccc"
    if score >= 85:
        return MASTERY_COLORS["Exceeding"]     # #38A169 (success)
    if score >= 70:
        return MASTERY_COLORS["Developing"]    # #ED8936 (warning)
    return MASTERY_COLORS["Needs Intervention"]  # #E53E3E (danger)


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
    lines.append(
        f'  "{_short(code)}" [label="{label}", fillcolor="{color}", fontcolor="white"];'
    )

for _, row in standards.iterrows():
    prereq = row["prerequisite_standard_code"]
    if prereq:
        lines.append(f'  "{_short(prereq)}" -> "{_short(row["standard_code"])}";')

lines.append("}")
dot = "\n".join(lines)

st.graphviz_chart(dot, use_container_width=True)
