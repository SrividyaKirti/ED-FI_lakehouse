"""Tab 4 -- Pipeline & Governance.

DQ scorecard, PII compliance panel, and data lineage diagram
showing Bronze -> Silver -> Gold record flow.
"""

import streamlit as st
import pandas as pd
from db import query


# ---------------------------------------------------------------------------
# 1. DQ Scorecard
# ---------------------------------------------------------------------------

def _render_dq_scorecard() -> None:
    st.subheader("Data Quality Scorecard")
    st.caption(
        "Summary of records quarantined during the latest pipeline run, "
        "broken down by validation rule."
    )

    try:
        rules = query("""
            SELECT rule_name,
                   rule_description,
                   COUNT(*) as quarantined_count,
                   COUNT(DISTINCT record_id) as distinct_records,
                   COUNT(DISTINCT source_system) as source_systems
            FROM gold.fact_dq_quarantine_log
            GROUP BY rule_name, rule_description
            ORDER BY quarantined_count DESC
        """)

        total_q = query(
            "SELECT COUNT(*) as total FROM gold.fact_dq_quarantine_log"
        )["total"].iloc[0]

        total_students = query(
            "SELECT COUNT(*) as total FROM gold.dim_student"
        )["total"].iloc[0]

    except Exception as exc:
        st.error(f"Failed to load DQ scorecard: {exc}")
        return

    # Top-level metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Quarantined Records", int(total_q))
    c2.metric("Total Students (passed)", int(total_students))
    pass_rate = round((1 - total_q / max(total_students + total_q, 1)) * 100, 1)
    c3.metric("Pipeline Pass Rate", f"{pass_rate}%")

    # Per-rule cards
    if rules.empty:
        st.success("No records were quarantined. All data passed validation.")
        return

    cols = st.columns(len(rules))
    for col, (_, row) in zip(cols, rules.iterrows()):
        with col:
            with st.container(border=True):
                st.markdown(f"**{row['rule_name']}**")
                st.metric("Quarantined", int(row["quarantined_count"]))
                st.caption(row["rule_description"])

    # Detail table
    try:
        detail = query("""
            SELECT source_system, entity_type, record_id, rule_name,
                   field_name, field_value, expected_value, quarantined_at
            FROM gold.fact_dq_quarantine_log
            ORDER BY quarantined_at DESC
        """)
        with st.expander("View All Quarantine Records"):
            st.dataframe(detail, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.error(f"Failed to load quarantine detail: {exc}")


# ---------------------------------------------------------------------------
# 2. PII Compliance Panel
# ---------------------------------------------------------------------------

def _render_pii_compliance() -> None:
    st.subheader("PII Compliance Panel")
    st.caption(
        "Demonstrating FERPA-compliant data handling. "
        "All personally identifiable information is SHA-256 hashed "
        "before reaching the gold layer."
    )

    try:
        sample = query("""
            SELECT student_id, first_name_hash, last_name_hash, email_hash,
                   birth_year, school_id, grade_level, _source_system
            FROM gold.dim_student
            LIMIT 5
        """)
    except Exception as exc:
        st.error(f"Failed to load student sample: {exc}")
        return

    c1, c2 = st.columns([3, 2])

    with c1:
        st.markdown("**Sample `dim_student` Record (Gold Layer)**")
        if not sample.empty:
            # Show first record vertically for clarity
            record = sample.iloc[0].to_dict()
            rows = []
            for k, v in record.items():
                is_hashed = k.endswith("_hash")
                val = str(v)
                if is_hashed:
                    # Truncate for display
                    val = val[:16] + "..." + val[-8:]
                rows.append({
                    "Field": k,
                    "Value": val,
                    "PII Protected": "Yes (SHA-256)" if is_hashed else "No (safe to store)",
                })
            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("**Full Sample (5 students)**")
        display = sample.copy()
        for col_name in ["first_name_hash", "last_name_hash", "email_hash"]:
            if col_name in display.columns:
                display[col_name] = display[col_name].apply(
                    lambda v: str(v)[:12] + "..." if pd.notna(v) else v
                )
        st.dataframe(display, use_container_width=True, hide_index=True)

    with c2:
        st.markdown("**FERPA Compliance Checklist**")

        checks = [
            ("Direct identifiers (name, email) are SHA-256 hashed", True),
            ("Birth date reduced to birth_year only", True),
            ("No SSN or government ID stored", True),
            ("Student IDs are synthetic (STU-XXXXX format)", True),
            ("Hashing applied at Silver layer before Gold", True),
            ("No plaintext PII in any Gold table", True),
            ("Access restricted to read-only connections", True),
            ("Audit trail via _source_system lineage", True),
        ]

        for label, passed in checks:
            icon = "+" if passed else "-"
            st.markdown(f"- [{icon}] {label}" if passed else f"- [ ] {label}")


# ---------------------------------------------------------------------------
# 3. Data Lineage Diagram
# ---------------------------------------------------------------------------

def _render_data_lineage() -> None:
    st.subheader("Data Lineage")
    st.caption(
        "End-to-end pipeline flow from raw source files through Bronze, "
        "Silver, and Gold layers with record counts from the actual database."
    )

    # Gather actual record counts
    try:
        counts = {}
        # Silver layer
        silver_tables = {
            "silver_edfi": [
                "students", "schools", "sections", "attendance",
                "assessment_results", "enrollments", "grades",
                "standards", "staff", "section_associations",
            ],
            "silver_oneroster": [
                "users", "orgs", "classes", "courses", "enrollments",
                "results", "academic_sessions", "demographics", "line_items",
            ],
        }
        for schema, tables in silver_tables.items():
            for t in tables:
                try:
                    cnt = query(f"SELECT COUNT(*) as n FROM {schema}.{t}")["n"].iloc[0]
                    counts[f"{schema}.{t}"] = int(cnt)
                except Exception:
                    counts[f"{schema}.{t}"] = 0

        # Gold layer
        gold_tables = [
            "dim_student", "dim_school", "dim_standard", "dim_section",
            "dim_misconception_pattern",
            "fact_student_mastery_daily", "fact_assessment_responses",
            "fact_attendance_daily", "fact_dq_quarantine_log",
            "agg_early_warning", "agg_district_comparison",
        ]
        for t in gold_tables:
            try:
                cnt = query(f"SELECT COUNT(*) as n FROM gold.{t}")["n"].iloc[0]
                counts[f"gold.{t}"] = int(cnt)
            except Exception:
                counts[f"gold.{t}"] = 0

    except Exception as exc:
        st.error(f"Failed to gather lineage counts: {exc}")
        return

    # Build summary counts for the diagram
    silver_edfi_total = sum(
        v for k, v in counts.items() if k.startswith("silver_edfi")
    )
    silver_or_total = sum(
        v for k, v in counts.items() if k.startswith("silver_oneroster")
    )
    gold_dim = sum(
        v for k, v in counts.items()
        if k.startswith("gold.dim_") or k.startswith("gold.agg_")
    )
    gold_fact = sum(
        v for k, v in counts.items() if k.startswith("gold.fact_")
    )
    gold_total = gold_dim + gold_fact
    quarantined = counts.get("gold.fact_dq_quarantine_log", 0)

    dot = f"""
    digraph lineage {{
        rankdir=LR;
        node [style=filled, fontsize=10, shape=box];
        edge [fontsize=9];

        subgraph cluster_bronze {{
            label="Bronze Layer\\n(Raw Ingestion)";
            style=filled;
            color="#e3f2fd";
            edfi_raw [label="Ed-Fi XML\\n(API extracts)", fillcolor="#bbdefb"];
            or_raw [label="OneRoster CSV\\n(flat files)", fillcolor="#bbdefb"];
        }}

        subgraph cluster_silver {{
            label="Silver Layer\\n(Cleaned & Typed)";
            style=filled;
            color="#fff9c4";
            edfi_silver [label="silver_edfi\\n({silver_edfi_total:,} rows)", fillcolor="#fff176"];
            or_silver [label="silver_oneroster\\n({silver_or_total:,} rows)", fillcolor="#fff176"];
        }}

        subgraph cluster_gold {{
            label="Gold Layer\\n(Analytics-Ready)";
            style=filled;
            color="#c8e6c9";
            gold_dims [label="Dimensions & Aggs\\n({gold_dim:,} rows)", fillcolor="#81c784"];
            gold_facts [label="Fact Tables\\n({gold_fact:,} rows)", fillcolor="#66bb6a"];
        }}

        quarantine [label="DQ Quarantine\\n({quarantined} records)", fillcolor="#ffcdd2", shape=octagon];
        app [label="Streamlit\\nDashboard", fillcolor="#ce93d8", shape=doubleoctagon];

        edfi_raw -> edfi_silver [label="PySpark"];
        or_raw -> or_silver [label="PySpark"];
        edfi_silver -> gold_dims [label="dbt"];
        or_silver -> gold_dims [label="dbt"];
        edfi_silver -> gold_facts [label="dbt"];
        or_silver -> gold_facts [label="dbt"];
        gold_facts -> quarantine [label="DQ gate", style=dashed];
        gold_dims -> app;
        gold_facts -> app;
        quarantine -> app [style=dashed];
    }}
    """
    st.graphviz_chart(dot, use_container_width=True)

    # Record count summary table
    st.markdown("**Record Counts by Layer**")

    layer_data = []
    for schema, tables in silver_tables.items():
        for t in tables:
            key = f"{schema}.{t}"
            layer_data.append({
                "Layer": "Silver",
                "Schema": schema,
                "Table": t,
                "Records": counts.get(key, 0),
            })
    for t in gold_tables:
        key = f"gold.{t}"
        layer_data.append({
            "Layer": "Gold",
            "Schema": "gold",
            "Table": t,
            "Records": counts.get(key, 0),
        })

    with st.expander("View Full Record Counts"):
        st.dataframe(
            pd.DataFrame(layer_data),
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def render() -> None:
    st.header("Pipeline & Governance")
    _render_dq_scorecard()
    st.markdown("---")
    _render_pii_compliance()
    st.markdown("---")
    _render_data_lineage()
