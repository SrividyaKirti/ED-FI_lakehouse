"""Misconception Analysis -- cataloged misconception patterns, wrong-answer
distributions, assessment accuracy by standard, and reteach strategy reference.

Surfaces actionable insights from ``gold.dim_misconception_pattern`` and
``gold.fact_assessment_responses`` so that instructional coaches and teachers
can identify, understand, and address the most common student misconceptions.
"""

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
    page_header,
    section,
    narrative,
    stat_row,
    insight_card,
    apply_theme,
    MASTERY_COLORS,
    SUBJECT_COLORS,
    subject_where,
    school_where,
    district_where,
)

setup_page()

# -- Page header ---------------------------------------------------------------
page_header(
    "Misconception Analysis",
    "Cataloged misconception patterns, wrong-answer distributions, and "
    "Atlas-style reteach recommendations",
)


# ---------------------------------------------------------------------------
# Shared filter fragments
# ---------------------------------------------------------------------------

def _filters() -> str:
    """Build combined WHERE fragments from sidebar selections.

    The helpers return strings like `` AND col = 'val'`` (with leading AND),
    so we start with ``WHERE 1=1`` to make concatenation safe.
    """
    return (
        subject_where("ds.subject")
        + school_where("st.school_id")
        + district_where("sc.district_name")
    )


def _filters_responses_only() -> str:
    """Filters applicable directly on fact_assessment_responses joined to
    dim_standard (aliased ds) and dim_student / dim_school."""
    return (
        subject_where("ds.subject")
        + school_where("st.school_id")
        + district_where("sc.district_name")
    )


# ---------------------------------------------------------------------------
# 1. KPI Summary Row
# ---------------------------------------------------------------------------

try:
    _filter_sql = _filters()

    _kpi_sql = f"""
        WITH filtered_responses AS (
            SELECT far.*
            FROM gold.fact_assessment_responses far
            INNER JOIN gold.dim_standard ds
                ON far.standard_code = ds.standard_code
            INNER JOIN gold.dim_student st
                ON far.student_id = st.student_id
            INNER JOIN gold.dim_school sc
                ON st.school_id = sc.school_id
            WHERE 1=1 {_filter_sql}
        )
        SELECT
            (SELECT COUNT(*) FROM gold.dim_misconception_pattern) AS misconceptions_cataloged,
            (SELECT COUNT(DISTINCT misconception_indicator)
             FROM filtered_responses
             WHERE misconception_indicator IS NOT NULL
               AND misconception_indicator != '') AS patterns_detected,
            (SELECT COUNT(DISTINCT student_id)
             FROM filtered_responses
             WHERE misconception_indicator IS NOT NULL
               AND misconception_indicator != '') AS students_affected,
            (SELECT COUNT(DISTINCT suggested_reteach)
             FROM gold.dim_misconception_pattern
             WHERE suggested_reteach IS NOT NULL) AS reteach_strategies
    """
    kpi = query(_kpi_sql)
    _misconceptions_cataloged = int(kpi["misconceptions_cataloged"].iloc[0])
    _patterns_detected = int(kpi["patterns_detected"].iloc[0])
    _students_affected = int(kpi["students_affected"].iloc[0])
    _reteach_strategies = int(kpi["reteach_strategies"].iloc[0])
except Exception as exc:
    st.error(f"Failed to load KPI data: {exc}")
    st.stop()

stat_row(
    [
        {"label": "Misconceptions Cataloged", "value": f"{_misconceptions_cataloged}"},
        {"label": "Wrong-Answer Patterns Detected", "value": f"{_patterns_detected}"},
        {"label": "Students Affected", "value": f"{_students_affected:,}"},
        {"label": "Reteach Strategies", "value": f"{_reteach_strategies}"},
    ]
)


# ---------------------------------------------------------------------------
# 2. Misconception Clusters
# ---------------------------------------------------------------------------

section("Misconception Clusters")
narrative(
    "Each card below represents a cataloged misconception pattern. "
    "Expand a card to see the full description, wrong-answer pattern, "
    "and how many students in the current filter have exhibited it."
)

try:
    _cluster_sql = f"""
        SELECT
            mp.misconception_id,
            mp.pattern_label,
            mp.description,
            mp.wrong_answer_pattern,
            mp.standard_code,
            mp.suggested_reteach,
            COALESCE(agg.student_count, 0) AS student_count
        FROM gold.dim_misconception_pattern mp
        LEFT JOIN (
            SELECT
                far.misconception_indicator,
                COUNT(DISTINCT far.student_id) AS student_count
            FROM gold.fact_assessment_responses far
            INNER JOIN gold.dim_standard ds
                ON far.standard_code = ds.standard_code
            INNER JOIN gold.dim_student st
                ON far.student_id = st.student_id
            INNER JOIN gold.dim_school sc
                ON st.school_id = sc.school_id
            WHERE far.misconception_indicator IS NOT NULL
              AND far.misconception_indicator != ''
              {_filters_responses_only()}
            GROUP BY far.misconception_indicator
        ) agg ON mp.pattern_label = agg.misconception_indicator
        ORDER BY agg.student_count DESC NULLS LAST, mp.pattern_label
    """
    clusters_df = query(_cluster_sql)
except Exception as exc:
    st.error(f"Failed to load misconception clusters: {exc}")
    clusters_df = pd.DataFrame()

if clusters_df.empty:
    st.info("No misconception patterns found in the catalog.")
else:
    # Show a high-level insight about the most common misconception
    top = clusters_df.iloc[0]
    if top["student_count"] > 0:
        insight_card(
            "Most Common Misconception",
            f"<b>{top['pattern_label'].replace('_', ' ').title()}</b> "
            f"affects <b>{int(top['student_count']):,}</b> student(s) "
            f"on standard <b>{top['standard_code']}</b>.",
            severity="warning",
        )

    for _, row in clusters_df.iterrows():
        label = row["pattern_label"].replace("_", " ").title()
        count = int(row["student_count"])
        badge = f"  ({count} student{'s' if count != 1 else ''})" if count > 0 else "  (no students in current filter)"

        with st.expander(f"{label}{badge}", expanded=False):
            st.markdown(f"**Standard:** `{row['standard_code']}`")
            st.markdown(f"**Description:** {row['description']}")
            st.markdown(f"**Wrong-Answer Pattern:** `{row['wrong_answer_pattern']}`")
            if row["suggested_reteach"]:
                st.markdown(f"**Suggested Reteach:** {row['suggested_reteach']}")


# ---------------------------------------------------------------------------
# 3. Assessment Accuracy by Standard
# ---------------------------------------------------------------------------

section("Assessment Accuracy by Standard")
narrative(
    "Accuracy rate (correct answers / total answers) for each standard, "
    "sorted from lowest to highest. Standards with the lowest accuracy "
    "are prime candidates for misconception investigation."
)

try:
    _accuracy_sql = f"""
        SELECT
            far.standard_code,
            ds.subject,
            COUNT(*) AS total_responses,
            SUM(CASE WHEN far.is_correct THEN 1 ELSE 0 END) AS correct_responses,
            ROUND(
                SUM(CASE WHEN far.is_correct THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 1
            ) AS accuracy_pct
        FROM gold.fact_assessment_responses far
        INNER JOIN gold.dim_standard ds
            ON far.standard_code = ds.standard_code
        INNER JOIN gold.dim_student st
            ON far.student_id = st.student_id
        INNER JOIN gold.dim_school sc
            ON st.school_id = sc.school_id
        WHERE 1=1 {_filters_responses_only()}
        GROUP BY far.standard_code, ds.subject
        HAVING COUNT(*) >= 10
        ORDER BY accuracy_pct ASC
    """
    accuracy_df = query(_accuracy_sql)
except Exception as exc:
    st.error(f"Failed to load accuracy data: {exc}")
    accuracy_df = pd.DataFrame()

if accuracy_df.empty:
    st.info("No assessment response data available for the current filters.")
else:
    # Shorten standard codes for display
    accuracy_df["standard_short"] = accuracy_df["standard_code"].apply(
        lambda c: ".".join(c.split(".")[2:]) if len(c.split(".")) > 2 else c
    )

    fig_accuracy = px.bar(
        accuracy_df,
        y="standard_short",
        x="accuracy_pct",
        color="subject",
        orientation="h",
        labels={
            "standard_short": "Standard",
            "accuracy_pct": "Accuracy %",
            "subject": "Subject",
        },
        hover_data=["standard_code", "total_responses", "correct_responses"],
        color_discrete_map=SUBJECT_COLORS,
    )
    fig_accuracy.update_layout(
        height=max(400, len(accuracy_df) * 28),
        xaxis_range=[0, 100],
        yaxis=dict(autorange="reversed"),
    )
    apply_theme(fig_accuracy)
    st.plotly_chart(fig_accuracy, use_container_width=True)

    # Insight: lowest-accuracy standard
    lowest = accuracy_df.iloc[0]
    insight_card(
        "Lowest Accuracy Standard",
        f"<b>{lowest['standard_code']}</b> ({lowest['subject']}) has an accuracy "
        f"rate of only <b>{lowest['accuracy_pct']}%</b> across "
        f"<b>{int(lowest['total_responses']):,}</b> responses.",
        severity="danger",
    )


# ---------------------------------------------------------------------------
# 4. Common Wrong Answers
# ---------------------------------------------------------------------------

section("Common Wrong Answers")
narrative(
    "Select a standard to see how student answers distribute compared to "
    "the correct answer. Peaks on incorrect options reveal systematic "
    "misconceptions worth addressing."
)

try:
    _standards_with_responses_sql = f"""
        SELECT DISTINCT far.standard_code
        FROM gold.fact_assessment_responses far
        INNER JOIN gold.dim_standard ds
            ON far.standard_code = ds.standard_code
        INNER JOIN gold.dim_student st
            ON far.student_id = st.student_id
        INNER JOIN gold.dim_school sc
            ON st.school_id = sc.school_id
        WHERE 1=1 {_filters_responses_only()}
        ORDER BY far.standard_code
    """
    available_standards = query(_standards_with_responses_sql)["standard_code"].tolist()
except Exception as exc:
    st.error(f"Failed to load available standards: {exc}")
    available_standards = []

if not available_standards:
    st.info("No standards with assessment responses for the current filters.")
else:
    selected_standard = st.selectbox(
        "Select a Standard",
        options=available_standards,
        key="misconception_standard_select",
    )

    if selected_standard:
        try:
            _wrong_answer_sql = f"""
                SELECT
                    far.student_answer,
                    far.correct_answer,
                    COUNT(*) AS frequency,
                    CASE
                        WHEN far.student_answer = far.correct_answer THEN 'Correct'
                        ELSE 'Incorrect'
                    END AS answer_type
                FROM gold.fact_assessment_responses far
                INNER JOIN gold.dim_standard ds
                    ON far.standard_code = ds.standard_code
                INNER JOIN gold.dim_student st
                    ON far.student_id = st.student_id
                INNER JOIN gold.dim_school sc
                    ON st.school_id = sc.school_id
                WHERE far.standard_code = '{selected_standard}'
                  {_filters_responses_only()}
                GROUP BY far.student_answer, far.correct_answer,
                         CASE
                            WHEN far.student_answer = far.correct_answer THEN 'Correct'
                            ELSE 'Incorrect'
                         END
                ORDER BY frequency DESC
            """
            wrong_df = query(_wrong_answer_sql)
        except Exception as exc:
            st.error(f"Failed to load answer distribution: {exc}")
            wrong_df = pd.DataFrame()

        if wrong_df.empty:
            st.info(f"No response data available for {selected_standard}.")
        else:
            correct_val = wrong_df["correct_answer"].iloc[0]
            st.markdown(f"**Correct answer:** `{correct_val}`")

            answer_colors = {"Correct": "#38A169", "Incorrect": "#E53E3E"}

            fig_answers = px.bar(
                wrong_df,
                x="student_answer",
                y="frequency",
                color="answer_type",
                labels={
                    "student_answer": "Student Answer",
                    "frequency": "Frequency",
                    "answer_type": "",
                },
                color_discrete_map=answer_colors,
            )
            fig_answers.update_layout(
                height=400,
                xaxis_title="Student Answer",
                yaxis_title="Response Count",
                legend_title_text="",
            )
            apply_theme(fig_answers)
            st.plotly_chart(fig_answers, use_container_width=True)

            # Show misconception tag breakdown for this standard
            try:
                _tag_sql = f"""
                    SELECT
                        far.misconception_tag,
                        COUNT(*) AS occurrences,
                        COUNT(DISTINCT far.student_id) AS students
                    FROM gold.fact_assessment_responses far
                    INNER JOIN gold.dim_student st
                        ON far.student_id = st.student_id
                    INNER JOIN gold.dim_school sc
                        ON st.school_id = sc.school_id
                    WHERE far.standard_code = '{selected_standard}'
                      AND far.misconception_tag IS NOT NULL
                      AND far.misconception_tag != ''
                      {school_where("st.school_id")}
                      {district_where("sc.district_name")}
                    GROUP BY far.misconception_tag
                    ORDER BY occurrences DESC
                """
                tag_df = query(_tag_sql)
                if not tag_df.empty:
                    insight_card(
                        "Detected Misconceptions for This Standard",
                        "".join(
                            f"<b>{r['misconception_tag'].replace('_', ' ').title()}</b>: "
                            f"{int(r['occurrences'])} occurrences across "
                            f"{int(r['students'])} student(s)<br>"
                            for _, r in tag_df.iterrows()
                        ),
                        severity="warning",
                    )
            except Exception:
                pass  # Non-critical -- tag data is optional


# ---------------------------------------------------------------------------
# 5. Reteach Strategy Reference
# ---------------------------------------------------------------------------

section("Reteach Strategy Reference")
narrative(
    "Atlas-style Recommendations: each cataloged misconception paired with "
    "a targeted reteach strategy that teachers can implement immediately."
)

try:
    _reteach_sql = """
        SELECT
            pattern_label,
            standard_code,
            description,
            suggested_reteach,
            wrong_answer_pattern
        FROM gold.dim_misconception_pattern
        WHERE suggested_reteach IS NOT NULL
        ORDER BY standard_code, pattern_label
    """
    reteach_df = query(_reteach_sql)
except Exception as exc:
    st.error(f"Failed to load reteach strategies: {exc}")
    reteach_df = pd.DataFrame()

if reteach_df.empty:
    st.info("No reteach strategies available.")
else:
    display_df = reteach_df.rename(columns={
        "pattern_label": "Pattern",
        "standard_code": "Standard",
        "description": "Misconception Description",
        "suggested_reteach": "Reteach Strategy",
        "wrong_answer_pattern": "Wrong-Answer Pattern",
    })

    # Make pattern labels more readable
    display_df["Pattern"] = display_df["Pattern"].apply(
        lambda x: x.replace("_", " ").title() if isinstance(x, str) else x
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Pattern": st.column_config.TextColumn(width="medium"),
            "Standard": st.column_config.TextColumn(width="small"),
            "Misconception Description": st.column_config.TextColumn(width="large"),
            "Reteach Strategy": st.column_config.TextColumn(width="large"),
            "Wrong-Answer Pattern": st.column_config.TextColumn(width="medium"),
        },
    )

    insight_card(
        "Coverage",
        f"<b>{len(reteach_df)}</b> misconception patterns have documented reteach "
        f"strategies spanning <b>{reteach_df['standard_code'].nunique()}</b> standards.",
        severity="success",
    )
