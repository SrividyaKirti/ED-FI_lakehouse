# Kiddom-Aligned Dashboard Redesign

**Goal:** Restructure the Streamlit dashboard pages around Kiddom's core product themes — standards mastery, misconception detection, early warning/instructional grouping, and data governance — to demonstrate deep alignment with Kiddom's Data Architecture team priorities.

**Architecture:** Keep existing component library (theme.py, cards.py, charts.py, layout.py) and DuckDB backend. Replace pages 2-4 with new Kiddom-aligned pages. Keep pages 1 and 5 with minor adjustments.

**Tech Stack:** Streamlit 1.41, Plotly 6.0, DuckDB 1.2, existing component library.

---

## Page Structure

### Page 1: Executive Overview (minor tweaks)
- Add "Misconceptions Detected" KPI (count from `dim_misconception_pattern`)
- Reframe "What This Project Demonstrates" insight to mention misconception detection and formative assessment analytics alongside interoperability story
- Keep: mastery donut, pipeline health bar, architecture diagram

### Page 2: Standards Mastery (replaces Classroom Insights)
**KPIs:** Standards Tracked | Avg Mastery % | Students Assessed | Standards Meeting Target (≥70%)

**Section 1 — Mastery by Standard (heatmap)**
- Rows: 23 standards (standard_code)
- Columns: mastery levels (Exceeding, Meeting, Developing, Needs Intervention)
- Cell values: student counts
- Source: `fact_student_mastery_daily` deduplicated by student+standard

**Section 2 — Standard Deep Dive (selectbox drill-down)**
- Selectbox to pick a standard
- Score distribution histogram for selected standard
- District comparison side-by-side (from `agg_district_comparison`)
- Prerequisite chain context (from `dim_standard.prerequisite_standard_code`)

**Section 3 — Standards Dependency Chain**
- Graphviz DAG showing prerequisite relationships
- Node color = avg mastery score for that standard
- Moved from old Classroom Insights, reframed as "Curriculum Graph"

### Page 3: Misconception Analysis (replaces District Intelligence)
**KPIs:** Misconceptions Cataloged | Wrong-Answer Patterns | Students Affected | Reteach Strategies Available

**Section 1 — Misconception Clusters**
- One card per misconception pattern (from `dim_misconception_pattern`)
- Shows: pattern_label, description, wrong_answer_pattern, suggested_reteach
- Count of students who triggered each pattern (join to `fact_assessment_responses.misconception_tag`)

**Section 2 — Assessment Response Analysis**
- Accuracy rate by standard (% correct from `fact_assessment_responses.is_correct`)
- Horizontal bar chart: standards sorted by accuracy (lowest first = biggest gaps)
- Narrative: "Standards with lowest accuracy are candidates for reteaching"

**Section 3 — Common Wrong Answers**
- For selected standard (selectbox), show distribution of student_answer vs correct_answer
- Highlight wrong_answer_pattern matches from misconception patterns
- Table of students with misconception indicators for that standard

**Section 4 — Reteach Strategy Reference**
- Table from `dim_misconception_pattern`: standard, pattern, suggested_reteach
- Framed as "Atlas-style instructional recommendations"

### Page 4: Early Warning & Attendance (reworked)
**KPIs:** At-Risk Students | Avg Attendance Rate | Declining Trends | Schools Represented

**Section 1 — Risk Distribution**
- Grouped bar or pie chart by risk_level (High, Medium, Low)
- Source: `agg_early_warning`

**Section 2 — Risk Factor Scatter**
- X: attendance_rate, Y: avg_mastery_score, Color: risk_level, Size: count_below_developing
- Same as old early warning scatter but better framed as "instructional grouping"

**Section 3 — Attendance Patterns**
- Attendance status distribution (Present, Absent, Tardy) from `fact_attendance_daily`
- By school bar chart

**Section 4 — Instructional Grouping Table**
- Students grouped by risk_level
- Show: student_id, risk_level, attendance_rate, avg_mastery_score, primary weakness area
- Framed as "suggested student groups for targeted intervention"

### Page 5: Pipeline & Governance (keep, minor narrative tweaks)
- Update narratives to mention Kiddom-relevant context (FERPA for ed-tech, curriculum data standards)
- Keep: DQ scorecard, PII compliance panel, data lineage diagram
- No structural changes

---

## Key Framing Changes
- "Classroom Insights" → "Standards Mastery" (mirrors Kiddom's mastery tracking)
- "District Intelligence" → "Misconception Analysis" (mirrors Atlas's core feature)
- Early warning scatter → "Instructional Grouping" (mirrors Atlas's student grouping)
- Standards dependency chain → "Curriculum Graph" (mirrors Kiddom's curriculum graph architecture)
- Reteach strategies → explicitly called out as "Atlas-style recommendations"

## Data Sources
All data comes from existing gold layer tables — no schema changes needed.
