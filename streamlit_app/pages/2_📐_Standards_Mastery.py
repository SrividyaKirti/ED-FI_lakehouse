"""Standards Mastery -- standards-level analytics with curriculum prerequisite graph.

Sections:
1. KPIs (standards tracked, avg mastery %, students assessed, standards meeting target)
2. Mastery Distribution donut chart
3. Performance by Standard heatmap
4. Standard Deep Dive (selectbox -> histogram + district comparison)
5. Curriculum Graph (Graphviz DAG of prerequisite chains)
"""

import sys
import os

_app_dir = os.path.join(os.path.dirname(__file__), "..")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

import streamlit as st  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import graphviz  # noqa: E402
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
    breadcrumb,
    back_button,
    subject_where,
    school_where,
    district_where,
    MASTERY_COLORS,
    SUBJECT_COLORS,
    COLORS,
)

setup_page()
inline_filters()

# ── Page header ─────────────────────────────────────────────────────
page_header(
    "Standards Mastery",
    "Standards-level performance analytics and curriculum prerequisite mapping",
)
breadcrumb()
back_button()

# ── Shared SQL filter fragments ─────────────────────────────────────
subj_filter = subject_where("s.subject")
dist_filter = district_where("sch.district_name")
school_filter = school_where("st.school_id")

# ---------------------------------------------------------------------------
# 1. KPI Summary Row
# ---------------------------------------------------------------------------

try:
    # Standards tracked (respects subject filter)
    _std_sql = f"""
        SELECT COUNT(*) AS n
        FROM gold.dim_standard s
        WHERE 1=1 {subj_filter}
    """
    standards_tracked = int(query(_std_sql).iloc[0, 0])

    # Avg mastery % and students assessed (respects all filters)
    _mastery_kpi_sql = f"""
        SELECT
            ROUND(AVG(sub.max_score_to_date), 1) AS avg_mastery,
            COUNT(DISTINCT sub.student_id) AS students_assessed
        FROM (
            SELECT m.student_id, m.standard_code, m.max_score_to_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.student_id, m.standard_code
                       ORDER BY m.assessment_count DESC
                   ) AS rn
            FROM gold.fact_student_mastery_daily m
            JOIN gold.dim_standard s ON m.standard_code = s.standard_code
            JOIN gold.dim_student st ON m.student_id = st.student_id
            JOIN gold.dim_school sch ON st.school_id = sch.school_id
            WHERE 1=1 {subj_filter} {dist_filter} {school_filter}
        ) sub
        WHERE sub.rn = 1
    """
    _kpi = query(_mastery_kpi_sql)
    avg_mastery = float(_kpi["avg_mastery"].iloc[0]) if _kpi["avg_mastery"].iloc[0] is not None else 0.0
    students_assessed = int(_kpi["students_assessed"].iloc[0])

    # Standards meeting target (>70% avg mastery)
    _target_sql = f"""
        SELECT COUNT(*) AS n
        FROM (
            SELECT sub.standard_code, AVG(sub.max_score_to_date) AS avg_score
            FROM (
                SELECT m.student_id, m.standard_code, m.max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY m.student_id, m.standard_code
                           ORDER BY m.assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily m
                JOIN gold.dim_standard s ON m.standard_code = s.standard_code
                JOIN gold.dim_student st ON m.student_id = st.student_id
                JOIN gold.dim_school sch ON st.school_id = sch.school_id
                WHERE 1=1 {subj_filter} {dist_filter} {school_filter}
            ) sub
            WHERE sub.rn = 1
            GROUP BY sub.standard_code
            HAVING AVG(sub.max_score_to_date) > 70
        ) meeting
    """
    standards_meeting = int(query(_target_sql).iloc[0, 0])

except Exception as exc:
    st.error(f"Failed to load KPI data: {exc}")
    st.stop()

stat_row(
    [
        {"label": "Standards Tracked", "value": f"{standards_tracked:,}"},
        {"label": "Avg Mastery %", "value": f"{avg_mastery:.1f}%"},
        {"label": "Students Assessed", "value": f"{students_assessed:,}"},
        {"label": "Meeting Target (>70%)", "value": f"{standards_meeting:,}"},
    ]
)

# Dynamic insight from KPIs
if standards_tracked > 0:
    pct_meeting = round(standards_meeting / standards_tracked * 100, 1)
    severity = "success" if pct_meeting >= 70 else ("warning" if pct_meeting >= 50 else "danger")
    insight_card(
        "Target Coverage",
        f"<b>{pct_meeting}%</b> of tracked standards ({standards_meeting} of "
        f"{standards_tracked}) have an average mastery score above 70%. "
        f"<b>{students_assessed:,}</b> students have been assessed across the "
        f"selected filters.",
        severity=severity,
    )


# ---------------------------------------------------------------------------
# 2. Mastery Distribution (Donut Chart)
# ---------------------------------------------------------------------------

section(
    "Mastery Distribution",
    "Student counts by mastery level across all standards",
)

narrative(
    "Each student-standard pair is counted once (using the record with the "
    "highest assessment count). The donut chart shows the distribution of "
    "mastery levels for the current filter selection."
)

try:
    _dist_sql = f"""
        SELECT sub.mastery_level, COUNT(*) AS count
        FROM (
            SELECT m.student_id, m.standard_code, m.mastery_level,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.student_id, m.standard_code
                       ORDER BY m.assessment_count DESC
                   ) AS rn
            FROM gold.fact_student_mastery_daily m
            JOIN gold.dim_standard s ON m.standard_code = s.standard_code
            JOIN gold.dim_student st ON m.student_id = st.student_id
            JOIN gold.dim_school sch ON st.school_id = sch.school_id
            WHERE 1=1 {subj_filter} {dist_filter} {school_filter}
        ) sub
        WHERE sub.rn = 1
        GROUP BY sub.mastery_level
    """
    mastery_dist = query(_dist_sql)
except Exception as exc:
    st.error(f"Failed to load mastery distribution: {exc}")
    st.stop()

if mastery_dist.empty:
    st.info("No mastery data available for the selected filters.")
else:
    mastery_order = ["Exceeding", "Meeting", "Developing", "Needs Intervention"]
    mastery_dist = (
        mastery_dist.set_index("mastery_level")
        .reindex(mastery_order)
        .reset_index()
        .dropna(subset=["count"])
    )

    fig_donut = go.Figure(
        go.Pie(
            labels=mastery_dist["mastery_level"],
            values=mastery_dist["count"],
            hole=0.55,
            marker=dict(
                colors=[MASTERY_COLORS[lvl] for lvl in mastery_dist["mastery_level"]]
            ),
            textinfo="label+percent",
            hovertemplate="%{label}: %{value:,} students (%{percent})<extra></extra>",
        )
    )
    fig_donut.update_layout(title_text="Mastery Level Distribution", height=420)
    apply_theme(fig_donut)
    st.plotly_chart(fig_donut, use_container_width=True)

    # Dynamic insight
    total_pairs = int(mastery_dist["count"].sum())
    meeting_exceeding = mastery_dist[
        mastery_dist["mastery_level"].isin(["Meeting", "Exceeding"])
    ]["count"].sum()
    me_pct = round(float(meeting_exceeding) / max(total_pairs, 1) * 100, 1)

    intervention_row = mastery_dist[mastery_dist["mastery_level"] == "Needs Intervention"]
    intervention_n = int(intervention_row["count"].iloc[0]) if not intervention_row.empty else 0
    intervention_pct = round(intervention_n / max(total_pairs, 1) * 100, 1)

    insight_card(
        "Mastery Snapshot",
        f"<b>{me_pct}%</b> of student-standard pairs are Meeting or Exceeding, "
        f"while <b>{intervention_pct}%</b> ({intervention_n:,} pairs) need intervention.",
        severity="success" if me_pct >= 70 else "warning",
    )


# ---------------------------------------------------------------------------
# 3. Performance by Standard (Heatmap)
# ---------------------------------------------------------------------------

section(
    "Performance by Standard",
    "Student counts per standard broken down by mastery level",
)

narrative(
    "The heatmap below shows how many students fall into each mastery level "
    "for every tracked standard. Darker cells indicate higher student counts. "
    "Look for standards with heavy concentration in the Needs Intervention column."
)

try:
    _heatmap_sql = f"""
        SELECT sub.standard_code, sub.mastery_level, COUNT(*) AS student_count
        FROM (
            SELECT m.student_id, m.standard_code, m.mastery_level,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.student_id, m.standard_code
                       ORDER BY m.assessment_count DESC
                   ) AS rn
            FROM gold.fact_student_mastery_daily m
            JOIN gold.dim_standard s ON m.standard_code = s.standard_code
            JOIN gold.dim_student st ON m.student_id = st.student_id
            JOIN gold.dim_school sch ON st.school_id = sch.school_id
            WHERE 1=1 {subj_filter} {dist_filter} {school_filter}
        ) sub
        WHERE sub.rn = 1
        GROUP BY sub.standard_code, sub.mastery_level
        ORDER BY sub.standard_code
    """
    heatmap_df = query(_heatmap_sql)
except Exception as exc:
    st.error(f"Failed to load heatmap data: {exc}")
    st.stop()

if heatmap_df.empty:
    st.info("No data available for the performance heatmap.")
else:
    # Pivot: standards (rows) x mastery levels (columns)
    pivot = heatmap_df.pivot_table(
        index="standard_code",
        columns="mastery_level",
        values="student_count",
        aggfunc="sum",
        fill_value=0,
    )
    # Reorder columns in logical order
    col_order = [c for c in mastery_order if c in pivot.columns]
    pivot = pivot[col_order]

    # Shorten standard codes for display
    def _short_code(code: str) -> str:
        parts = code.split(".")
        return ".".join(parts[2:]) if len(parts) > 2 else code

    short_labels = [_short_code(c) for c in pivot.index]

    fig_heatmap = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=col_order,
            y=short_labels,
            colorscale="YlOrRd",
            reversescale=True,
            text=pivot.values,
            texttemplate="%{text}",
            hovertemplate=(
                "Standard: %{y}<br>Level: %{x}<br>Students: %{z}<extra></extra>"
            ),
            colorbar=dict(title="Count"),
        )
    )
    fig_heatmap.update_layout(
        title_text="Students per Standard by Mastery Level",
        xaxis_title="Mastery Level",
        yaxis_title="Standard",
        height=max(400, len(pivot) * 28),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=120, r=20, t=50, b=60),
    )
    apply_theme(fig_heatmap)
    st.plotly_chart(fig_heatmap, use_container_width=True)

    # Identify the standard with the most students needing intervention
    if "Needs Intervention" in pivot.columns:
        worst_std = pivot["Needs Intervention"].idxmax()
        worst_count = int(pivot.loc[worst_std, "Needs Intervention"])
        insight_card(
            "Highest-Need Standard",
            f"<b>{worst_std}</b> has the most students ({worst_count:,}) at the "
            f"Needs Intervention level. Consider prioritizing re-teach resources "
            f"for this standard.",
            severity="warning",
        )


# ---------------------------------------------------------------------------
# 4. Standard Deep Dive
# ---------------------------------------------------------------------------

section(
    "Standard Deep Dive",
    "Select a standard to view its score distribution and cross-district comparison",
)

try:
    _standards_list_sql = f"""
        SELECT s.standard_code, s.standard_description, s.subject
        FROM gold.dim_standard s
        WHERE 1=1 {subj_filter}
        ORDER BY s.subject, s.standard_code
    """
    all_standards = query(_standards_list_sql)
except Exception as exc:
    st.error(f"Failed to load standards list: {exc}")
    st.stop()

if all_standards.empty:
    st.info("No standards available for the selected subject filter.")
else:
    # Build display labels: "CCSS.MATH.K.CC.A.1 -- Count to 100..."
    all_standards["display"] = (
        all_standards["standard_code"] + "  --  " + all_standards["standard_description"].str[:60]
    )
    selected_display = st.selectbox(
        "Choose a standard",
        options=all_standards["display"].tolist(),
        key="deep_dive_standard",
    )
    selected_code = all_standards.loc[
        all_standards["display"] == selected_display, "standard_code"
    ].iloc[0]

    # ── Score distribution histogram ──────────────────────────────────
    try:
        _hist_sql = f"""
            SELECT sub.max_score_to_date AS score
            FROM (
                SELECT m.student_id, m.standard_code, m.max_score_to_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY m.student_id, m.standard_code
                           ORDER BY m.assessment_count DESC
                       ) AS rn
                FROM gold.fact_student_mastery_daily m
                JOIN gold.dim_student st ON m.student_id = st.student_id
                JOIN gold.dim_school sch ON st.school_id = sch.school_id
                WHERE m.standard_code = '{selected_code}'
                  {dist_filter} {school_filter}
            ) sub
            WHERE sub.rn = 1
        """
        hist_df = query(_hist_sql)
    except Exception as exc:
        st.error(f"Failed to load score distribution: {exc}")
        hist_df = None

    if hist_df is not None and not hist_df.empty:
        col_hist, col_comp = st.columns(2)

        with col_hist:
            st.markdown(f"**Score Distribution: {selected_code}**")
            fig_hist = px.histogram(
                hist_df,
                x="score",
                nbins=20,
                color_discrete_sequence=[COLORS["primary"]],
                labels={"score": "Score", "count": "Students"},
            )
            fig_hist.update_layout(
                xaxis_title="Score",
                yaxis_title="Number of Students",
                height=380,
                bargap=0.05,
            )
            # Add vertical line at mastery threshold
            fig_hist.add_vline(
                x=70, line_dash="dash", line_color=MASTERY_COLORS["Meeting"],
                annotation_text="Mastery (70)",
                annotation_position="top right",
            )
            apply_theme(fig_hist)
            st.plotly_chart(fig_hist, use_container_width=True)

        # ── District comparison bar chart ─────────────────────────────
        with col_comp:
            st.markdown(f"**District Comparison: {selected_code}**")
            try:
                _comp_sql = f"""
                    SELECT district_name, avg_score, mastery_pct, student_count
                    FROM gold.agg_district_comparison
                    WHERE standard_code = '{selected_code}'
                    ORDER BY district_name
                """
                comp_df = query(_comp_sql)
            except Exception as exc:
                st.error(f"Failed to load district comparison: {exc}")
                comp_df = None

            if comp_df is not None and not comp_df.empty:
                fig_comp = go.Figure()
                fig_comp.add_trace(
                    go.Bar(
                        x=comp_df["district_name"],
                        y=comp_df["avg_score"],
                        name="Avg Score",
                        marker_color=COLORS["primary"],
                        text=[f"{v:.1f}" for v in comp_df["avg_score"]],
                        textposition="outside",
                        hovertemplate=(
                            "%{x}<br>Avg Score: %{y:.1f}<extra></extra>"
                        ),
                    )
                )
                fig_comp.add_trace(
                    go.Bar(
                        x=comp_df["district_name"],
                        y=comp_df["mastery_pct"] * 100,
                        name="Mastery %",
                        marker_color=COLORS["secondary"],
                        text=[f"{v * 100:.1f}%" for v in comp_df["mastery_pct"]],
                        textposition="outside",
                        hovertemplate=(
                            "%{x}<br>Mastery: %{y:.1f}%<extra></extra>"
                        ),
                    )
                )
                fig_comp.update_layout(
                    barmode="group",
                    yaxis_title="Score / Percentage",
                    height=380,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                # Reference line at 70
                fig_comp.add_hline(
                    y=70, line_dash="dash", line_color=MASTERY_COLORS["Meeting"],
                    annotation_text="Target (70)",
                    annotation_position="bottom right",
                )
                apply_theme(fig_comp)
                st.plotly_chart(fig_comp, use_container_width=True)
            else:
                st.info("No district comparison data available for this standard.")

        # Summary stats for the selected standard
        median_score = hist_df["score"].median()
        below_70 = len(hist_df[hist_df["score"] < 70])
        total = len(hist_df)
        below_pct = round(below_70 / max(total, 1) * 100, 1)

        insight_card(
            f"Deep Dive: {selected_code}",
            f"Median score is <b>{median_score:.0f}</b> across <b>{total:,}</b> "
            f"students assessed. <b>{below_pct}%</b> ({below_70:,}) are below the "
            f"mastery threshold of 70.",
            severity="success" if below_pct < 30 else ("warning" if below_pct < 50 else "danger"),
        )
    elif hist_df is not None:
        st.info(f"No assessment data found for {selected_code} with the current filters.")


# ---------------------------------------------------------------------------
# 5. Curriculum Graph (Graphviz DAG)
# ---------------------------------------------------------------------------

section(
    "Curriculum Prerequisite Graph",
    "Prerequisite chain of standards colored by average mastery",
)

narrative(
    "Each node represents a standard. Edges show prerequisite relationships "
    "(A -> B means A is a prerequisite of B). Node colors reflect class-average "
    "mastery: <span style='color:#38A169; font-weight:bold;'>green</span> for "
    "Meeting/Exceeding, <span style='color:#ED8936; font-weight:bold;'>orange</span> "
    "for Developing, and <span style='color:#E53E3E; font-weight:bold;'>red</span> "
    "for Needs Intervention."
)

try:
    _graph_std_sql = f"""
        SELECT s.standard_code, s.prerequisite_standard_code,
               s.grade_level, s.domain, s.subject
        FROM gold.dim_standard s
        WHERE 1=1 {subj_filter}
        ORDER BY s.grade_level, s.standard_code
    """
    graph_standards = query(_graph_std_sql)

    # Average mastery per standard (latest per student, then avg) -- with filters
    _avg_mastery_sql = f"""
        SELECT sub.standard_code, AVG(sub.max_score_to_date) AS avg_score
        FROM (
            SELECT m.student_id, m.standard_code, m.max_score_to_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.student_id, m.standard_code
                       ORDER BY m.assessment_count DESC
                   ) AS rn
            FROM gold.fact_student_mastery_daily m
            JOIN gold.dim_student st ON m.student_id = st.student_id
            JOIN gold.dim_school sch ON st.school_id = sch.school_id
            WHERE 1=1 {dist_filter} {school_filter}
        ) sub
        WHERE sub.rn = 1
        GROUP BY sub.standard_code
    """
    avg_mastery_df = query(_avg_mastery_sql)
except Exception as exc:
    st.error(f"Failed to load curriculum graph data: {exc}")
    st.stop()

if graph_standards.empty:
    st.info("No standards available for the curriculum graph with the current filters.")
else:
    score_map = dict(
        zip(avg_mastery_df["standard_code"], avg_mastery_df["avg_score"])
    )

    def _node_color(code: str) -> str:
        """Return a fill color based on average mastery score."""
        score = score_map.get(code)
        if score is None:
            return COLORS["neutral"]
        if score >= 70:
            return MASTERY_COLORS["Exceeding"] if score >= 90 else MASTERY_COLORS["Meeting"]
        if score >= 50:
            return MASTERY_COLORS["Developing"]
        return MASTERY_COLORS["Needs Intervention"]

    def _font_color(code: str) -> str:
        """Return white for dark backgrounds, dark for light ones."""
        score = score_map.get(code)
        if score is None:
            return COLORS["text_primary"]
        return "white"

    def _short(code: str) -> str:
        """Shorten e.g. CCSS.MATH.K.CC.A.1 -> K.CC.A.1 or NGSS.2-PS1-1 -> 2-PS1-1."""
        parts = code.split(".")
        if len(parts) > 2:
            return ".".join(parts[2:])
        # NGSS format: NGSS.2-PS1-1
        if code.startswith("NGSS."):
            return code[5:]
        return code

    # Build graphviz Digraph
    dot = graphviz.Digraph(
        "curriculum",
        graph_attr={
            "rankdir": "LR",
            "bgcolor": "transparent",
            "fontname": "DM Sans, sans-serif",
            "nodesep": "0.4",
            "ranksep": "0.6",
        },
        node_attr={
            "shape": "box",
            "style": "filled,rounded",
            "fontname": "DM Sans, sans-serif",
            "fontsize": "10",
            "penwidth": "0",
            "margin": "0.15,0.08",
        },
        edge_attr={
            "color": "#A0AEC0",
            "arrowsize": "0.7",
        },
    )

    # Group by grade_level for subgraph clusters
    grades = sorted(graph_standards["grade_level"].unique())
    for grade in grades:
        grade_rows = graph_standards[graph_standards["grade_level"] == grade]
        grade_label = "Kindergarten" if grade == 0 else f"Grade {grade}"
        with dot.subgraph(name=f"cluster_g{grade}") as sub:
            sub.attr(label=grade_label, style="dashed", color="#A0AEC0", fontsize="11")
            for _, row in grade_rows.iterrows():
                code = row["standard_code"]
                score = score_map.get(code)
                label = _short(code)
                if score is not None:
                    label += f"\n({score:.0f})"
                sub.node(
                    _short(code),
                    label=label,
                    fillcolor=_node_color(code),
                    fontcolor=_font_color(code),
                )

    # Add edges for prerequisite relationships
    for _, row in graph_standards.iterrows():
        prereq = row["prerequisite_standard_code"]
        if prereq:
            dot.edge(_short(prereq), _short(row["standard_code"]))

    st.graphviz_chart(dot, use_container_width=True)

    # Legend for node colors
    legend_cols = st.columns(4)
    legend_items = [
        ("Exceeding (90+)", MASTERY_COLORS["Exceeding"]),
        ("Meeting (70-89)", MASTERY_COLORS["Meeting"]),
        ("Developing (50-69)", MASTERY_COLORS["Developing"]),
        ("Needs Intervention (<50)", MASTERY_COLORS["Needs Intervention"]),
    ]
    for col, (label, color) in zip(legend_cols, legend_items):
        col.markdown(
            f"<span style='color:{color}; font-weight:bold;'>&#9632;</span> {label}",
            unsafe_allow_html=True,
        )

    # Identify weak links in the prerequisite chain
    weak_standards = [
        code for code in graph_standards["standard_code"]
        if score_map.get(code, 100) < 50
    ]
    if weak_standards:
        weak_list = ", ".join(f"<b>{s}</b>" for s in weak_standards[:5])
        insight_card(
            "Weak Links in Prerequisite Chain",
            f"The following standards have average mastery below 50%: {weak_list}. "
            f"Students struggling with these prerequisites will likely struggle with "
            f"dependent standards too. Consider targeted intervention.",
            severity="danger",
        )
    else:
        # Check for developing-level standards
        developing_standards = [
            code for code in graph_standards["standard_code"]
            if 50 <= score_map.get(code, 100) < 70
        ]
        if developing_standards:
            insight_card(
                "Standards at Developing Level",
                f"<b>{len(developing_standards)}</b> standard(s) have average mastery "
                f"in the Developing range (50-69%). Monitor these closely as they may "
                f"become blockers for dependent standards.",
                severity="warning",
            )
        else:
            insight_card(
                "Strong Prerequisite Coverage",
                "All standards in the prerequisite chain are at or above the "
                "Meeting threshold. Students have a solid foundation for "
                "progression through the curriculum.",
                severity="success",
            )
