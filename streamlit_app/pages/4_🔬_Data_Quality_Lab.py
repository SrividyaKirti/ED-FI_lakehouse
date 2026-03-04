"""Data Quality Lab — interactive pipeline validation and error quarantine simulation."""

import sys
import os

_app_dir = os.path.join(os.path.dirname(__file__), "..")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

import streamlit as st  # noqa: E402
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
    COLORS,
)

setup_page()


# ---------------------------------------------------------------------------
# Sample data templates
# ---------------------------------------------------------------------------

_EDFI_SAMPLE = pd.DataFrame([
    {
        "student_id": "STU-00401",
        "first_name": "Alice",
        "last_name": "Johnson",
        "birth_date": "2015-03-12",
        "school_id": "GB-ES-001",
        "grade_level": 4,
        "enrollment_date": "2025-08-15",
    },
    {
        "student_id": "STU-00402",
        "first_name": "Bob",
        "last_name": "Williams",
        "birth_date": "2016-07-22",
        "school_id": "GB-ES-003",
        "grade_level": 3,
        "enrollment_date": "2025-08-15",
    },
    {
        "student_id": "STU-00403",
        "first_name": "Carmen",
        "last_name": "Reyes",
        "birth_date": "2014-11-05",
        "school_id": "GB-MS-001",
        "grade_level": 5,
        "enrollment_date": "2025-08-15",
    },
])

_ONEROSTER_SAMPLE = pd.DataFrame([
    {
        "sourcedId": "abc-1234-def",
        "givenName": "David",
        "familyName": "Chen",
        "dateOfBirth": "2015-09-10",
        "orgSourcedId": "8fe8c372-72d0-5365-8ff7-dcf51285dda9",
        "grade": "4",
        "beginDate": "2025-08-18",
    },
    {
        "sourcedId": "abc-1234-ghi",
        "givenName": "Emma",
        "familyName": "Patel",
        "dateOfBirth": "2016-01-30",
        "orgSourcedId": "9f24cbba-d67f-5d4e-ace8-bf259651e1c4",
        "grade": "3",
        "beginDate": "2025-08-18",
    },
    {
        "sourcedId": "abc-1234-jkl",
        "givenName": "Fatima",
        "familyName": "Okafor",
        "dateOfBirth": "2014-06-17",
        "orgSourcedId": "d24d40eb-4782-50fe-a1db-7733856571d2",
        "grade": "5",
        "beginDate": "2025-08-18",
    },
])


# ---------------------------------------------------------------------------
# Error injection definitions
# ---------------------------------------------------------------------------

def _set_field(df: pd.DataFrame, row_idx: int, col: str, value) -> pd.DataFrame:
    """Return a copy of the dataframe with one cell changed."""
    out = df.copy()
    out.loc[row_idx, col] = value
    return out


_ERRORS = {
    "null_student_id": {
        "label": "Null Student ID",
        "description": "Sets the student ID to NULL (violates NOT NULL constraint).",
        "rule_name": "student_id_not_null",
        "rule_description": "Student ID is required",
        "field_name": "student_id",
        "expected_value": "Non-null student identifier",
        "apply_edfi": lambda df: _set_field(df, 0, "student_id", None),
        "apply_oneroster": lambda df: _set_field(df, 0, "sourcedId", None),
    },
    "invalid_school_id": {
        "label": "Invalid School ID",
        "description": "Changes the school ID to a value not in the registry.",
        "rule_name": "valid_school_id",
        "rule_description": "SchoolID not found in district registry",
        "field_name": "school_id",
        "expected_value": "Must match a school in seed_school_registry",
        "apply_edfi": lambda df: _set_field(df, 1, "school_id", "INVALID-SCH-999"),
        "apply_oneroster": lambda df: _set_field(df, 1, "orgSourcedId", "INVALID-ORG-999"),
    },
    "future_enrollment_date": {
        "label": "Future Enrollment Date",
        "description": "Sets the enrollment date to a date in the future.",
        "rule_name": "enrollment_date_not_future",
        "rule_description": "Enrollment start date cannot be in the future",
        "field_name": "enrollment_date",
        "expected_value": "Date must be <= today",
        "apply_edfi": lambda df: _set_field(df, 2, "enrollment_date", "2027-09-01"),
        "apply_oneroster": lambda df: _set_field(df, 2, "beginDate", "2027-09-01"),
    },
}


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _highlight_errors(
    original: pd.DataFrame, modified: pd.DataFrame
):
    """Return a styled dataframe highlighting cells that differ."""

    def _style(row):
        styles = [""] * len(row)
        idx = row.name
        for j, col in enumerate(row.index):
            orig_val = original.loc[idx, col] if idx < len(original) else None
            curr_val = row[col]
            if orig_val != curr_val:
                styles[j] = "background-color: #ffcdd2; font-weight: bold;"
        return styles

    return modified.style.apply(_style, axis=1)


# ---------------------------------------------------------------------------
# Processing logic
# ---------------------------------------------------------------------------

def _process_records(
    modified: pd.DataFrame,
    original: pd.DataFrame,
    active_errors: list[str],
    is_edfi: bool,
) -> None:
    """Simulate pipeline processing and show results."""

    total = len(modified)
    quarantined_records = []

    # Determine which rows are quarantined
    quarantined_rows = set()
    for err_key in active_errors:
        err = _ERRORS[err_key]
        # Each error affects a specific row index (0, 1, or 2)
        row_idx = {
            "null_student_id": 0,
            "invalid_school_id": 1,
            "future_enrollment_date": 2,
        }[err_key]
        quarantined_rows.add(row_idx)

        # Build quarantine entry
        id_col = "student_id" if is_edfi else "sourcedId"
        record_id = modified.loc[row_idx, id_col]
        if record_id is None:
            record_id = "(NULL)"

        field_col = err["field_name"]
        if not is_edfi and field_col == "school_id":
            field_col = "orgSourcedId"
        if not is_edfi and field_col == "enrollment_date":
            field_col = "beginDate"

        # Get the bad value
        val_col = field_col if field_col in modified.columns else err["field_name"]
        field_value = (
            str(modified.loc[row_idx, val_col])
            if val_col in modified.columns
            else "(NULL)"
        )

        quarantined_records.append({
            "source_system": "edfi" if is_edfi else "oneroster",
            "entity_type": "enrollment",
            "record_id": str(record_id),
            "rule_name": err["rule_name"],
            "rule_description": err["rule_description"],
            "field_name": err["field_name"],
            "field_value": field_value,
            "expected_value": err["expected_value"],
        })

    passed = total - len(quarantined_rows)
    quarantined = len(quarantined_rows)

    # --- Results summary ---
    section("Processing Results")

    stat_row([
        {"label": "Total Records", "value": str(total)},
        {"label": "Passed", "value": str(passed)},
        {"label": "Quarantined", "value": str(quarantined)},
    ])

    # Flow visualization
    source_label = "Ed-Fi XML" if is_edfi else "OneRoster CSV"
    dot = f"""
    digraph pipeline {{
        rankdir=LR;
        node [style=filled, fontsize=11];

        source [label="{source_label}\\n({total} records)", fillcolor="#e3f2fd", shape=box];
        validate [label="Validation\\nGate", fillcolor="#fff9c4", shape=diamond];
        passed [label="Passed\\n({passed} records)", fillcolor="#c8e6c9", shape=box];
        quarantine [label="Quarantined\\n({quarantined} records)", fillcolor="#ffcdd2", shape=box];

        source -> validate;
        validate -> passed [label="valid"];
        validate -> quarantine [label="invalid"];
    }}
    """
    st.graphviz_chart(dot, use_container_width=True)

    # Quarantine log
    if quarantined_records:
        # Build insight summary
        error_labels = [_ERRORS[k]["label"] for k in active_errors]
        insight_card(
            "Quarantine Summary",
            f"{quarantined} of {total} records quarantined: "
            f"{' and '.join(error_labels)} detected.",
            severity="warning",
        )

        st.markdown("#### Quarantine Log")
        st.dataframe(
            pd.DataFrame(quarantined_records),
            use_container_width=True,
            hide_index=True,
        )

    # --- Actual quarantine records from the production pipeline ---
    st.markdown("---")
    section(
        "Actual Pipeline Quarantine Records",
        "Real quarantine entries from the production pipeline run.",
    )

    try:
        actual = query("""
            SELECT source_system, entity_type, record_id, rule_name,
                   rule_description, field_name, field_value, expected_value
            FROM gold.fact_dq_quarantine_log
            ORDER BY rule_name, record_id
        """)
        st.dataframe(actual, use_container_width=True, hide_index=True)

        rule_counts = actual.groupby("rule_name").size().reset_index(name="count")
        total_quarantined = int(rule_counts["count"].sum())
        num_rules = len(rule_counts)

        # Horizontal bar chart of quarantine counts by rule
        fig = go.Figure(
            go.Bar(
                y=rule_counts["rule_name"],
                x=rule_counts["count"],
                orientation="h",
                marker=dict(color=COLORS["danger"]),
                text=rule_counts["count"],
                textposition="outside",
                hovertemplate="%{y}: %{x} records<extra></extra>",
            )
        )
        fig.update_layout(
            title_text="Quarantine by Rule",
            xaxis_title="Record Count",
            yaxis_title="",
            height=max(200, 60 * len(rule_counts)),
            yaxis=dict(autorange="reversed"),
        )
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

        insight_card(
            "Pipeline Data Quality",
            f"The pipeline caught <b>{total_quarantined}</b> records across "
            f"<b>{num_rules}</b> validation rules. All quarantined records "
            f"are preserved for audit.",
            severity="info",
        )
    except Exception as exc:
        st.error(f"Failed to load quarantine log: {exc}")


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

page_header(
    "Data Quality Lab",
    "Interactive pipeline validation and error quarantine simulation",
)

narrative(
    "This interactive lab demonstrates the pipeline's data quality validation. "
    "Inject errors into sample records and observe how the validation gate "
    "catches and quarantines them."
)

# --- Source format selector ---
source_format = st.radio(
    "Source Format",
    ["Ed-Fi XML", "OneRoster CSV"],
    horizontal=True,
)

is_edfi = source_format == "Ed-Fi XML"
base_data = _EDFI_SAMPLE.copy() if is_edfi else _ONEROSTER_SAMPLE.copy()

# --- Error toggles ---
section("Inject Errors")
active_errors = []
toggle_cols = st.columns(len(_ERRORS))
for col, (key, err) in zip(toggle_cols, _ERRORS.items()):
    with col:
        if st.toggle(err["label"], key=f"toggle_{key}"):
            active_errors.append(key)
        st.caption(err["description"])

# --- Apply errors ---
modified = base_data.copy()
for err_key in active_errors:
    err = _ERRORS[err_key]
    apply_fn = err["apply_edfi"] if is_edfi else err["apply_oneroster"]
    modified = apply_fn(modified)

# --- Data Preview ---
section("Data Preview")

if active_errors:
    st.markdown("*Red cells indicate injected errors.*")
    st.dataframe(
        _highlight_errors(base_data, modified),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.dataframe(modified, use_container_width=True, hide_index=True)

# --- Process Records button ---
st.markdown("---")
if st.button("Process Records", type="primary", use_container_width=True):
    _process_records(modified, base_data, active_errors, is_edfi)
