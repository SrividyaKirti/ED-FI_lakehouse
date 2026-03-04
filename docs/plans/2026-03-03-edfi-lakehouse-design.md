# Ed-Fi Interoperability Lakehouse — Design Document

**Date:** 2026-03-03
**Author:** Vidya
**Purpose:** Portfolio project for Kiddom Senior Data Engineer application

---

## 1. Project Purpose & Positioning

### What This Is

A portfolio project submitted alongside a Kiddom Senior Data Engineer application. It demonstrates the ability to ingest messy K-12 data from multiple district formats (Ed-Fi XML, OneRoster CSV), transform it through a Medallion Architecture with defensive Data Quality gates, and surface unified analytics that mirror what Kiddom's product delivers to teachers and administrators.

### Target Audience

A hiring manager or engineering lead at Kiddom who will:
1. Click a published **Streamlit Cloud URL** (the primary deliverable)
2. Optionally browse the **GitHub repo** linked from within the app

Nobody is cloning the repo and running it locally. The app must tell the story on its own.

### What It Proves

| Skill | How It's Demonstrated |
|-------|----------------------|
| Product understanding | Mastery heatmap using Kiddom's "max value" method, Atlas-style misconception clustering, standards dependency chains |
| Lakehouse architecture | Bronze → Silver → Gold medallion with clear separation of concerns |
| Spark skills | PySpark for XML/CSV parsing, schema validation, PII hashing |
| dbt skills | Staging → intermediate → marts models, custom test suite, YAML documentation |
| Data quality engineering | 10 custom DQ gates encoding K-12 business rules, quarantine routing, interactive DQ simulator |
| PII/FERPA compliance | SHA-256 hashing at Silver layer, no raw PII in Gold, anonymization toggle in UI |
| Cloud readiness | Architecture designed for Snowflake (DuckDB used for dev with one-adapter swap), S3 landing zone documented |

### Application Question Alignment

**Q: Cloud Platform experience?**
> "I built an end-to-end pipeline designed for AWS S3 + Snowflake. For development, it runs locally via DuckDB (Snowflake-compatible SQL) — swapping to production Snowflake requires changing one dbt adapter. The architecture doc details the S3 landing zone, Snowflake external stages, and the production deployment path."

**Q: Data Transformation experience (Spark, dbt)?**
> "I use both for different purposes — PySpark handles upstream parsing (nested Ed-Fi XML → flat DataFrames, schema validation, PII hashing at scale), while dbt handles business-logic transformations (semantic standardization across district formats, standards-based mastery calculations, and a custom DQ test suite that quarantines records with invalid SchoolIDs or grade-level logic violations). This split is intentional: Spark excels at schema-on-read parsing of semi-structured data; dbt excels at testable, documented, version-controlled SQL transformations."

---

## 2. Technical Architecture

### Stack

| Component | Tool | Rationale |
|-----------|------|-----------|
| Data Generation | Python scripts | Generate Ed-Fi XML + OneRoster CSV simulating two districts |
| Parsing (Bronze → Silver) | PySpark (local via Docker) | Demonstrates Spark; handles XML parsing and PII hashing |
| Warehouse (dev) | DuckDB | Snowflake-compatible SQL, zero infrastructure, file-based |
| Transformations (Silver → Gold) | dbt with dbt-duckdb adapter | Business logic, semantic standardization, DQ gates |
| Orchestration | Airflow (Docker Compose) | DAGs that wire ingestion → Spark → dbt |
| Frontend | Streamlit on Streamlit Community Cloud | Published URL, queries bundled DuckDB/Parquet data |
| Infra docs | Architecture diagrams + Snowflake migration notes | Shows the production cloud path without requiring cloud spend |

### Approach: Local-First, Cloud-Ready

Everything runs locally with zero cloud spend. The code, SQL models, and architecture are identical to what would run on Snowflake/S3 — only the dbt adapter changes. The README and architecture documentation explicitly show the Snowflake deployment path.

**Why this approach:** DuckDB → Snowflake is a one-line swap. dbt abstracts the warehouse. A polished local project beats a half-finished cloud project. The hiring manager evaluates architecture, models, and tests — not whether you have an AWS account.

### Data Flow

```
Python generators
    ├── Ed-Fi XML (Grand Bend ISD)
    ├── OneRoster CSV (Riverside USD)
    └── Assessment response data (with planted misconception patterns)
            │
            ▼
    Local filesystem (Bronze) ── raw files, untouched audit trail
            │
            ▼
    PySpark jobs
    • Parse nested XML → flat DataFrames
    • Parse CSVs with schema validation
    • SHA-256 hash PII fields (names, emails, birth_date → birth_year)
    • Write Parquet → load into DuckDB Silver tables
            │
            ▼
    dbt (Silver → Gold in DuckDB)
    ├── staging/        ── per-source entity models
    ├── intermediate/   ── cross-source unification + semantic mapping
    ├── marts/          ── facts, dimensions, aggregates
    └── tests/          ── 10 custom DQ gates + standard tests
            │
            ▼
    DuckDB file (bundled in repo)
            │
            ▼
    Streamlit app (deployed on Streamlit Community Cloud)
    ├── Tab 1: Classroom Insights
    ├── Tab 2: District Intelligence
    ├── Tab 3: Data Quality Simulator (interactive)
    └── Tab 4: Pipeline & Governance
```

### What Runs Where

| Environment | What Runs | Purpose |
|-------------|-----------|---------|
| Docker Compose (local) | Airflow + PySpark + dbt | Builds the data (the pipeline) |
| Streamlit Community Cloud | Streamlit app + pre-built DuckDB file | Presents the data (the deliverable) |

The hiring manager sees the Streamlit app. If they clone the repo, they can run the full pipeline locally with `docker-compose up`.

---

## 3. Frontend Design

### Delivery Model

- **Primary deliverable:** A Streamlit Community Cloud URL included in the job application
- **Secondary deliverable:** GitHub repo linked from within the app
- **Data at runtime:** Bundled DuckDB file in the repo (no external database connections)

### Sidebar (always visible)

- Project title: "Ed-Fi Interoperability Lakehouse"
- One-liner: "A unified analytics layer for multi-district K-12 data"
- Author name + GitHub repo link
- Architecture diagram (static image)
- District selector: "Grand Bend ISD (Ed-Fi)" / "Riverside USD (OneRoster)" / "All Districts"

### Tab 1: "Classroom Insights"

**Audience:** Teacher / School Leader

**Components:**

1. **Mastery Heatmap** — Students (rows) × Standards (columns), color-coded by mastery level (Exceeding / Meeting / Developing / Needs Intervention). Uses Kiddom's "max value" calculation method (highest mark achieved wins). Filterable by grade level and class section.

2. **Misconception Clusters** (Atlas-inspired) — Detected patterns from assessment response data:
   - Cluster card example: *"8 students — CCSS.MATH.4.OA.A.1 — Common error: confusing 'how many fewer' (subtraction) with 'how many times' (division)"*
   - Each cluster shows: affected students (anonymized IDs), the standard, the wrong-answer pattern, a suggested reteach reference
   - Grouped by standard, sorted by cluster size (largest misconception first)

3. **Standards Dependency Chain** — A directed graph visualization showing prerequisite relationships between learning standards. Nodes are color-coded: green = class mastery above threshold, red = below. If a red node has a red parent, the root cause is upstream.
   - Example insight: *"87% of students failing CCSS.MATH.4.NF.B.3 also failed CCSS.MATH.3.NF.A.1. Recommendation: reteach the prerequisite before continuing Unit 5."*
   - Clicking a node shows which students are failing it and whether they also failed the prerequisite

4. **Early Warning Flags** — At-risk student identification:
   - Table: `Student ID (hashed)`, `Risk Level`, `Mastery Trend`, `Attendance Rate`, `Consecutive Assessments Below Developing`, `Recommended Action`
   - Scatter plot: attendance rate (x) vs. average mastery score (y), colored by risk tier
   - Rule: declining mastery trend (3+ consecutive assessments below Developing) + attendance < 90% = High Risk

### Tab 2: "District Intelligence"

**Audience:** District Administrator

**Components:**

1. **Cross-District Standards Comparison** — Grouped bar chart: for each standard, mastery % for Grand Bend ISD vs. Riverside USD. Standards with > 10% gap are highlighted. Accompanying detail table with actual numbers.
   - Example insight: *"Grand Bend ISD students are 15% behind Riverside USD on CCSS.MATH.4.NF.B.3 — despite similar demographics."*

2. **Curriculum Effectiveness** — Comparison panel:
   - Sections using Curriculum Version A vs. Version B
   - Mastery outcomes by standard for each version
   - Call-out: *"Version A shows +23% mastery on CCSS.MATH.4.OA.A.1. Key difference: Version A includes 3 additional practice activities in Lesson 7."*

3. **Enrollment & Demographic Summary** — Per-district breakdowns: student count, grade distribution, school count, section count. Data completeness indicator per district.

### Tab 3: "Data Quality Simulator"

**Audience:** Hiring manager / Engineering lead

**This is the interactive differentiator.**

**Layout:**

- **Left panel — "Source Data Preview":** A sample of 20 records from either Ed-Fi or OneRoster format (toggleable). Displayed as a table with raw field names.

- **Right panel — "Inject Errors":** Toggle switches:
  - ☐ Invalid SchoolID (3 records)
  - ☐ Grade-level violation — 2nd grader in AP Physics (2 records)
  - ☐ Future enrollment date (1 record)
  - ☐ Missing required field — null student_id (2 records)
  - ☐ Dangling reference — section doesn't exist (1 record)
  - Each toggle highlights affected rows in yellow in the left panel

- **Bottom panel — "Run Pipeline":**
  - Button: "Process Records"
  - Animated result: `20 records ingested → 15 passed ✅ → 5 quarantined 🔴`
  - Quarantine log table: record ID, rule violated, field value, expected value
  - Mini Sankey or flow diagram: Source → Passed/Quarantined → Gold Layer / Quarantine Table

### Tab 4: "Pipeline & Governance"

**Audience:** Data team / Compliance

**Components:**

1. **DQ Scorecard** — Per-district cards: records ingested / passed / quarantined / pass rate, last sync timestamp, green/yellow/red freshness indicator.

2. **dbt Test Results** — Table: test name, model tested, status (pass/fail), last run, description.

3. **PII Compliance Panel:**
   - Anonymization toggle: production vs. anonymized view of a sample record
   - PII field inventory: field name, classification, masking method, masking layer
   - FERPA compliance checklist:
     - ✅ Student names hashed before Silver layer
     - ✅ Email addresses hashed before Silver layer
     - ✅ Birth dates generalized to birth year only in Gold
     - ✅ No PII in analytics/Gold layer
     - ✅ Raw PII retained only in Bronze with restricted access

4. **Lineage Diagram** — Rendered flow: Ed-Fi XML / OneRoster CSV → Bronze → PySpark → Silver → dbt → Gold → Streamlit. Each node shows record counts.

---

## 4. Data Model

### 4.1 Generated Source Data (Bronze)

#### District A — Grand Bend ISD (Ed-Fi XML)

| File | Entity | Volume |
|------|--------|--------|
| `Students.xml` | Student demographics + identifiers | ~5,000 students, K-12 |
| `Schools.xml` | School buildings | 8 schools (5 elementary, 2 middle, 1 high) |
| `Staff.xml` | Teachers | ~200 |
| `Sections.xml` | Class sections | ~400 |
| `StudentSchoolAssociations.xml` | Enrollments | ~5,000 |
| `StudentSectionAssociations.xml` | Class rosters | ~15,000 |
| `Grades.xml` | Grading period grades | ~45,000 |
| `StudentAssessments.xml` | Assessment results with item responses | ~25,000 |
| `StudentSchoolAttendanceEvents.xml` | Daily attendance | ~500,000 |
| `LearningStandards.xml` | CCSS Math standards (K-5 focus) | ~200 standards |

#### District B — Riverside USD (OneRoster CSV)

| File | Entity | Volume |
|------|--------|--------|
| `orgs.csv` | Districts and schools | 6 schools (4 elementary, 1 middle, 1 high) |
| `users.csv` | Students + teachers | ~3,500 students + ~150 teachers |
| `classes.csv` | Sections | ~280 |
| `courses.csv` | Course catalog | ~60 |
| `enrollments.csv` | Student-to-class + teacher-to-class | ~10,200 |
| `academicSessions.csv` | Terms and grading periods | ~20 |
| `lineItems.csv` | Assignments/assessments | ~1,500 |
| `results.csv` | Scores with answer data | ~18,000 |
| `demographics.csv` | Birth dates, ethnicity | ~3,500 |

#### Planted Data Quality Issues

| Issue Type | Count | Purpose |
|------------|-------|---------|
| Invalid SchoolID (not in registry) | 15-20 | Powers `test_valid_school_id` |
| Grade-level logic violations (K-2 in AP courses) | 5-10 | Powers `test_grade_level_course_match` |
| Future enrollment dates | 5 | Powers `test_enrollment_date_not_future` |
| Null required fields (student_id) | 5 | Powers `test_student_id_not_null` |
| Dangling section references | 5 | Powers `test_section_exists_for_enrollment` |

#### Planted Misconception Patterns

| Standard | Pattern | Description |
|----------|---------|-------------|
| CCSS.MATH.4.OA.A.1 | `subtraction_instead_of_division` | Students confuse "how many fewer" with "how many times" |
| CCSS.MATH.3.NF.A.1 | `numerator_denominator_swap` | Students confuse which number goes on top vs. bottom |
| CCSS.MATH.4.NF.B.3 | `fraction_addition_whole_number` | Students add numerators AND denominators (1/3 + 1/4 = 2/7) — tied to 3.NF prerequisite failure |

### 4.2 Silver Layer (DuckDB — cleaned, PII-masked)

#### Staging Models

| Ed-Fi Source | OneRoster Source |
|-------------|-----------------|
| `stg_edfi__students` | `stg_oneroster__users` |
| `stg_edfi__schools` | `stg_oneroster__orgs` |
| `stg_edfi__sections` | `stg_oneroster__classes` |
| `stg_edfi__enrollments` | `stg_oneroster__enrollments` |
| `stg_edfi__staff` | `stg_oneroster__courses` |
| `stg_edfi__grades` | `stg_oneroster__results` |
| `stg_edfi__assessments` | `stg_oneroster__line_items` |
| `stg_edfi__attendance` | `stg_oneroster__academic_sessions` |
| `stg_edfi__standards` | `stg_oneroster__demographics` |

Each staging model:
- Renames source-specific columns to a common convention
- Casts types (strings to dates, integers, etc.)
- Hashes PII fields: SHA-256 on `student_name`, `email`, `birth_date` → outputs `student_id_hash`, `email_hash`, `birth_year`
- Adds `_source_system` column (`edfi` or `oneroster`)
- Adds `_loaded_at` timestamp

### 4.3 Intermediate Layer (cross-source unification)

| Model | Key Transformations |
|-------|-------------------|
| `int_students` | Union both sources, deduplicate by hashed ID, normalize grade levels |
| `int_schools` | Unified school registry with district attribution |
| `int_sections` | Unified sections with course + term linkage |
| `int_enrollments` | Unified student-section-school enrollments |
| `int_staff` | Unified teacher records |
| `int_grades` | Unified grading records |
| `int_assessments` | Unified assessment results at question level |
| `int_attendance` | Unified daily attendance events |
| `int_standards` | Learning standards reference with prerequisite relationships |

Key transformations:
- Grade level normalization: `"Ninth grade"` / `"09"` / `"Freshman"` → `9`
- School year alignment across different calendar conventions
- Deduplication of students appearing in both districts
- Standards code normalization to dotted CCSS format (e.g., `CCSS.MATH.4.OA.A.1`)

### 4.4 Gold Layer (marts)

#### Fact Tables

**`fact_student_mastery_daily`** — One row per student per standard per day

| Column | Type | Description |
|--------|------|-------------|
| `student_id_hash` | string | Anonymized student identifier |
| `standard_code` | string | e.g., CCSS.MATH.4.OA.A.1 |
| `school_id` | string | School identifier |
| `section_id` | string | Section identifier |
| `mastery_level` | string | Exceeding / Meeting / Developing / Needs Intervention |
| `max_score_to_date` | float | Kiddom's "max value" methodology |
| `assessment_count` | int | Number of assessments on this standard |
| `date_key` | date | Calendar date |

**`fact_assessment_responses`** — One row per student per question per assessment

| Column | Type | Description |
|--------|------|-------------|
| `student_id_hash` | string | Anonymized student identifier |
| `assessment_id` | string | Assessment identifier |
| `question_number` | int | Question sequence number |
| `standard_code` | string | Standard this question assesses |
| `correct_answer` | string | The correct answer |
| `student_answer` | string | What the student chose |
| `is_correct` | boolean | Whether the answer was correct |
| `misconception_tag` | string | Null if correct; pattern label if detected |

**`fact_dq_quarantine_log`** — One row per quarantined record

| Column | Type | Description |
|--------|------|-------------|
| `source_system` | string | edfi / oneroster |
| `entity_type` | string | student / enrollment / section / etc. |
| `record_id` | string | Original record identifier |
| `rule_name` | string | e.g., valid_school_id, grade_level_logic |
| `rule_description` | string | Human-readable explanation |
| `field_name` | string | The field that failed |
| `field_value` | string | The actual bad value |
| `expected_value` | string | What was expected |
| `quarantined_at` | timestamp | When the record was quarantined |

**`fact_attendance_daily`** — One row per student per day

| Column | Type | Description |
|--------|------|-------------|
| `student_id_hash` | string | Anonymized student identifier |
| `school_id` | string | School identifier |
| `attendance_date` | date | Calendar date |
| `status` | string | Present / Absent / Tardy / Excused |

#### Dimension Tables

**`dim_student`**

| Column | Type | Description |
|--------|------|-------------|
| `student_id_hash` | string | Anonymized student identifier |
| `grade_level` | int | Normalized grade level (K=0, 1-12) |
| `school_id` | string | Current school |
| `district_id` | string | District identifier |
| `enrollment_start_date` | date | When enrollment began |
| `birth_year` | int | Generalized from birth_date for FERPA |
| `is_active` | boolean | Currently enrolled |

**`dim_school`**

| Column | Type | Description |
|--------|------|-------------|
| `school_id` | string | School identifier |
| `school_name` | string | School name |
| `school_type` | string | Elementary / Middle / High |
| `district_id` | string | Parent district |
| `district_name` | string | District name |
| `source_system` | string | edfi / oneroster |

**`dim_standard`**

| Column | Type | Description |
|--------|------|-------------|
| `standard_code` | string | e.g., CCSS.MATH.4.OA.A.1 |
| `standard_description` | string | Full text description |
| `domain` | string | e.g., "Operations & Algebraic Thinking" |
| `grade_level` | int | Grade level for this standard |
| `prerequisite_standard_code` | string | FK to self — enables dependency chain |

**`dim_section`**

| Column | Type | Description |
|--------|------|-------------|
| `section_id` | string | Section identifier |
| `course_name` | string | Course name |
| `teacher_id_hash` | string | Anonymized teacher identifier |
| `school_id` | string | School identifier |
| `term_name` | string | Term/semester name |
| `curriculum_version` | string | "A" or "B" — for effectiveness comparison |

**`dim_misconception_pattern`**

| Column | Type | Description |
|--------|------|-------------|
| `misconception_id` | string | Unique pattern identifier |
| `standard_code` | string | Associated learning standard |
| `pattern_label` | string | e.g., "subtraction_instead_of_division" |
| `description` | string | Human-readable explanation of the misconception |
| `suggested_reteach` | string | e.g., "IM Unit 4, Lesson 7" |

#### Aggregated Views

**`agg_early_warning`** — Pre-computed risk scores

| Column | Type | Description |
|--------|------|-------------|
| `student_id_hash` | string | Anonymized student identifier |
| `risk_level` | string | High / Medium / Low |
| `avg_mastery_score` | float | Average across all standards |
| `attendance_rate` | float | 0.0 to 1.0 |
| `consecutive_below_developing` | int | Count of recent assessments below threshold |
| `declining_trend` | boolean | True if mastery trending downward |

**`agg_district_comparison`** — Pre-computed cross-district metrics

| Column | Type | Description |
|--------|------|-------------|
| `standard_code` | string | Learning standard |
| `district_id` | string | District identifier |
| `district_name` | string | District name |
| `mastery_pct` | float | % of students Meeting or Exceeding |
| `student_count` | int | Number of students assessed |

---

## 5. dbt Test Suite — DQ Gates

### Custom Schema Tests

#### 1. School Registry Validation (`test_valid_school_id`)
- **Description:** Every enrollment must reference a school that exists in the district registry
- **Logic:** JOIN dim_school on school_id — quarantine any record where school_id has no match
- **Applies to:** `int_enrollments`, `int_sections`, `fact_attendance_daily`

#### 2. Grade-Level Logic (`test_grade_level_course_match`)
- **Description:** Students cannot be enrolled in courses outside their grade band
- **Logic:** Elementary (K-5) students cannot be in courses coded as High School (9-12). A grade_level=2 student in a section where course_name contains "AP" → quarantine
- **Applies to:** `int_enrollments`

#### 3. Temporal Integrity (`test_enrollment_date_not_future`)
- **Description:** Enrollment start dates cannot be in the future
- **Logic:** `enrollment_start_date > current_date` → quarantine
- **Applies to:** `int_enrollments`

#### 4. Referential Integrity (`test_section_exists_for_enrollment`)
- **Description:** Every student-section enrollment must reference an existing section
- **Logic:** LEFT JOIN int_sections — quarantine where section_id is null
- **Applies to:** `int_enrollments`

#### 5. Required Field Completeness (`test_student_id_not_null`)
- **Description:** Student ID is required on all student-facing records
- **Logic:** `student_id IS NULL` → quarantine
- **Applies to:** `int_students`, `int_enrollments`, `int_grades`

#### 6. PII Masking Verification (`test_pii_masked_in_gold`)
- **Description:** No raw PII (unhashed names, emails, full birth dates) reaches the Gold layer
- **Logic:** Verify that `student_name`, `email`, `birth_date` columns do not exist in Gold models; verify `student_id_hash`, `email_hash`, `birth_year` are used instead
- **Applies to:** All mart models

#### 7. Attendance Rate Sanity (`test_attendance_rate_bounds`)
- **Description:** Attendance rate must be between 0% and 100%
- **Logic:** `attendance_rate < 0 OR attendance_rate > 1.0` → flag
- **Applies to:** `agg_early_warning`

#### 8. Mastery Score Consistency (`test_max_value_mastery`)
- **Description:** Mastery level must reflect the highest score achieved (Kiddom's max-value method)
- **Logic:** For each student-standard pair, verify `max_score_to_date >= all individual assessment scores`
- **Applies to:** `fact_student_mastery_daily`

#### 9. Cross-District Duplicate Detection (`test_no_duplicate_students_across_sources`)
- **Description:** Same student appearing in both districts should be merged, not duplicated
- **Logic:** `GROUP BY student_id_hash HAVING count(distinct source_system) > 1 AND count(*) > expected`
- **Applies to:** `int_students`

#### 10. Assessment Response Integrity (`test_answer_exists_for_scored_assessment`)
- **Description:** Every scored assessment must have at least one item-level response
- **Logic:** LEFT JOIN `fact_assessment_responses` — flag assessments with scores but no item data
- **Applies to:** `fact_assessment_responses`

### Quarantine Routing

Failed records route to `fact_dq_quarantine_log` with full context:

```
Record arrives → dbt test evaluates →
  PASS → continues to Gold layer
  FAIL → row written to fact_dq_quarantine_log with:
         rule_name, rule_description, the actual bad value,
         what was expected, timestamp
```

The DQ Simulator in Tab 3 reads from this quarantine log. When the user toggles error injection and clicks "Process," they see the exact quarantine entries these tests produce.

### Standard dbt Tests

Every staging model also includes:
- `unique` on primary keys
- `not_null` on required fields
- `accepted_values` on enum fields (grade levels, attendance status, mastery levels)
- `relationships` for foreign key integrity between models

---

## 6. Project Structure

```
ED-FI_lakehouse/
├── README.md                          # Architecture overview + Streamlit link + how to run locally
├── docs/
│   ├── plans/
│   │   └── 2026-03-03-edfi-lakehouse-design.md  # This document
│   └── architecture.png              # Architecture diagram
├── docker-compose.yml                 # Airflow + Spark services
├── dags/                              # Airflow DAGs
│   ├── ingest_edfi.py
│   └── ingest_oneroster.py
├── data_generation/                   # Mock data generators
│   ├── generate_edfi_xml.py
│   ├── generate_oneroster_csv.py
│   └── generate_assessment_responses.py
├── spark_jobs/                        # PySpark Bronze → Silver
│   ├── parse_edfi_xml.py
│   ├── parse_oneroster_csv.py
│   └── hash_pii.py
├── dbt_project/                       # dbt Silver → Gold
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                   # Per-source entity models
│   │   │   ├── edfi/
│   │   │   └── oneroster/
│   │   ├── intermediate/              # Cross-source unification
│   │   └── marts/                     # Gold layer facts + dimensions
│   ├── tests/                         # Custom DQ gate SQL tests
│   ├── macros/                        # Reusable transformations (PII hashing, grade normalization)
│   └── seeds/                         # Reference data (misconception patterns, standard prerequisites)
├── streamlit_app/                     # Frontend
│   ├── app.py                         # Main app with tab routing
│   ├── tabs/
│   │   ├── classroom_insights.py
│   │   ├── district_intelligence.py
│   │   ├── dq_simulator.py
│   │   └── pipeline_governance.py
│   ├── components/                    # Reusable chart/visualization components
│   ├── data/                          # Bundled DuckDB file for Streamlit Cloud
│   └── requirements.txt
├── data/                              # Local data directory (gitignored except samples)
│   ├── bronze/
│   │   ├── edfi/
│   │   └── oneroster/
│   ├── silver/
│   └── gold/
└── terraform/                         # Optional: IaC for production deployment path
    └── main.tf                        # S3 bucket + Snowflake external stage (documented, not deployed)
```

---

## 7. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| DuckDB over Snowflake for dev | DuckDB + dbt-duckdb adapter | Zero cost, zero setup, Snowflake-compatible SQL. One adapter swap for production. |
| PySpark over plain Python for parsing | PySpark | Demonstrates Spark skills for the application question. Handles XML parsing cleanly. Runs locally via Docker. |
| Separate Spark and dbt responsibilities | Spark = parsing/PII, dbt = business logic/tests | Shows architectural judgment: right tool for each job. |
| DQ quarantine over DQ rejection | Quarantine table with full context | Records aren't deleted — they're routed with explanations. Enables the DQ Simulator and auditability. |
| Kiddom's max-value mastery method | Highest mark achieved wins | Directly mirrors Kiddom's actual product behavior. Shows product research. |
| Planted misconception patterns | Rule-based, not ML | Achievable in 1-2 weeks. Demonstrates the Atlas concept without requiring model training. |
| Bundled DuckDB file for Streamlit Cloud | Pre-built data shipped with the app | No runtime database needed. Streamlit Cloud just serves the app with embedded data. |
| Four tabs with distinct audience personas | Teacher / Admin / Engineer / Compliance | Each tab proves a different skill, each speaks to a different evaluator. |

---

## 8. Kiddom Product Research Summary

### What Kiddom Does

Kiddom is a K-12 curriculum-first learning platform. It digitizes high-quality instructional materials (HQIM) from publishers (Illustrative Mathematics, EL Education, OpenSciEd), wraps them with planning, delivery, grading, and analytics tools, and layers AI on top.

### Core Product Cycle

**Plan → Deliver → Grade → Analyze → Adjust**

- Teachers access pre-loaded curriculum organized by unit and lesson
- Students complete assignments; results map to learning standards
- Mastery is tracked per student per standard using "max value" (highest mark wins)
- Mastery heatmaps show class-wide and individual progress
- Color-coded levels: Exceeding / Meeting / Developing / Needs Intervention

### Kiddom Atlas (Launched Feb 2026)

Kiddom's newest and most strategic product:
1. Students complete "cool-down" assessments (3 questions at end of each lesson)
2. Atlas analyzes responses overnight — scores and identifies misconception patterns
3. Atlas groups students by shared misconception into tiers
4. Atlas generates differentiated warm-up activities for next morning
5. Teachers review recommendations and deliver targeted small-group instruction
6. Cycle repeats daily

Early results: students using Atlas showed gains of up to 18% compared to peers.

### Data That Flows Through Kiddom

**Inbound:** Student rosters, teacher rosters, school structures (via Clever, ClassLink, OneRoster 1.1), SSO credentials, curriculum content from publishers, assessment content from CenterPoint/ANet.

**Generated internally:** Assignment submissions, grades (points + standards-based), standards mastery scores, cool-down responses, AI misconception analysis, curriculum usage data, teacher engagement data.

**Outbound:** Grades to external LMS (Canvas, Schoology, Google Classroom via LTI 1.3), anonymized insights to publishers, PDF report cards to parents.

### Integration Standards

OneRoster 1.1 (1EdTech certified), LTI 1.3, Clever API, ClassLink.

### Why This Project Matters to Kiddom

The core technical challenge is **data fragmentation**: one district sends Ed-Fi API JSON, another sends OneRoster CSV dumps, a third sends proprietary XML. Kiddom must unify all of this into a single analytics layer that powers mastery reports, Atlas AI, and district dashboards — while maintaining FERPA compliance. This project demonstrates exactly that capability.

---

## 9. Ed-Fi and OneRoster Data Standards Reference

### Ed-Fi Core Entities

The Ed-Fi Unifying Data Model covers 17 domains. Key entities for this project:

- **Organizations:** StateEducationAgency → LocalEducationAgency → School
- **People:** Student, Staff, Parent/Contact
- **Teaching & Learning:** Course → CourseOffering → Section (with Student/Staff Section Associations)
- **Assessment:** Assessment → AssessmentItem → LearningStandard → StudentAssessment
- **Attendance:** StudentSchoolAttendanceEvent, StudentSectionAttendanceEvent
- **Academic Record:** Grade, ReportCard, StudentAcademicRecord

Ed-Fi uses **natural composite keys** (not surrogate IDs) for most entities. Load order matters — entities must be loaded in dependency order.

### OneRoster CSV Files

Key files: `orgs.csv`, `users.csv`, `classes.csv`, `courses.csv`, `enrollments.csv`, `academicSessions.csv`, `lineItems.csv`, `results.csv`, `demographics.csv`.

All IDs use `sourcedId` (GUID format). Files are RFC 4180 CSV, UTF-8, case-sensitive headers.

### Cross-Standard Mapping

| Ed-Fi Entity | OneRoster CSV |
|-------------|---------------|
| LocalEducationAgency / School | `orgs.csv` |
| Student / Staff | `users.csv` |
| Section | `classes.csv` |
| Course | `courses.csv` |
| StudentSectionAssociation | `enrollments.csv` (role=student) |
| StaffSectionAssociation | `enrollments.csv` (role=teacher) |
| Session / GradingPeriod | `academicSessions.csv` |
| Grade / GradebookEntry | `lineItems.csv` + `results.csv` |

### Common Data Quality Issues

- **Dependency failures:** Loading entities out of order (enrollment before student exists)
- **Identifier mismatches:** Same student with different IDs across systems
- **Semantic gaps:** "Grade 9" vs. "09" vs. "Freshman" vs. "Ninth grade"
- **Referential integrity:** Enrollments pointing to non-existent sections
- **Format violations:** Wrong column order, non-UTF-8, case sensitivity in OneRoster
