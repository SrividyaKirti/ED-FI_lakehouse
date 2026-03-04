"""Tab 3 -- Data Quality Simulator.

Interactive error-injection simulator that shows how the pipeline
validates and quarantines bad records from Ed-Fi XML and OneRoster CSV
source formats.
"""

import streamlit as st
import pandas as pd
from db import query


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


def _set_field(df: pd.DataFrame, row_idx: int, col: str, value) -> pd.DataFrame:
    """Return a copy of the dataframe with one cell changed."""
    out = df.copy()
    out.loc[row_idx, col] = value
    return out


# ---------------------------------------------------------------------------
# Rendering
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


def render() -> None:
    st.header("Data Quality Simulator")
    st.caption(
        "Inject common data errors and see how the pipeline's validation "
        "rules detect and quarantine bad records."
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
    st.markdown("### Inject Errors")
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

    # --- Preview ---
    st.markdown("### Data Preview")

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
        row_idx = {"null_student_id": 0, "invalid_school_id": 1, "future_enrollment_date": 2}[err_key]
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
        field_value = str(modified.loc[row_idx, val_col]) if val_col in modified.columns else "(NULL)"

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
    st.markdown("### Processing Results")

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Records", total)
    c2.metric("Passed", passed)
    c3.metric("Quarantined", quarantined)

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
        st.markdown("### Quarantine Log")
        st.dataframe(
            pd.DataFrame(quarantined_records),
            use_container_width=True,
            hide_index=True,
        )

    # Comparison with actual quarantine data
    st.markdown("---")
    st.markdown("### Actual Quarantine Records in Pipeline")
    st.caption("These are real quarantine entries from the production pipeline run.")

    try:
        actual = query("""
            SELECT source_system, entity_type, record_id, rule_name,
                   rule_description, field_name, field_value, expected_value
            FROM gold.fact_dq_quarantine_log
            ORDER BY rule_name, record_id
        """)
        st.dataframe(actual, use_container_width=True, hide_index=True)

        rule_counts = actual.groupby("rule_name").size().reset_index(name="count")
        st.markdown("**Quarantine by Rule**")
        for _, row in rule_counts.iterrows():
            st.markdown(f"- **{row['rule_name']}**: {row['count']} records")
    except Exception as exc:
        st.error(f"Failed to load quarantine log: {exc}")
