# Streamlit UI Redesign — Full Component Library Approach

**Date:** 2026-03-04
**Status:** Approved

## Goal

Transform the Streamlit app from a basic prototype into a polished, education-themed analytics product with narrative flow, custom components, and deeper insights. Add GitHub repo link to sidebar.

## Requirements

- **Visual style:** Education-themed professional (teals, purples, warm but polished)
- **Scope:** Narrative flow + visual polish + deeper analytics
- **Tech:** Streamlit + Plotly + custom CSS (no new dependencies)
- **Navigation:** Sidebar multipage navigation (replacing tabs)
- **Branding:** GitHub link + project info in sidebar

## Design System

### Color Palette

| Token | Color | Usage |
|-------|-------|-------|
| `primary` | `#0D7377` (deep teal) | Headers, primary actions, links |
| `primary-light` | `#14919B` (bright teal) | Hover states, highlights |
| `secondary` | `#6C5CE7` (soft purple) | Accents, secondary charts |
| `background` | `#FAFBFC` | Content area |
| `surface` | `#FFFFFF` | Cards, elevated content |
| `surface-alt` | `#F0F4F8` | Section backgrounds |
| `text-primary` | `#1A202C` | Body text |
| `text-secondary` | `#4A5568` | Captions, secondary info |
| `success` | `#38A169` | Exceeding, passing, positive |
| `warning` | `#ED8936` | Developing, medium risk |
| `danger` | `#E53E3E` | Needs intervention, high risk |
| `info` | `#3182CE` | Informational callouts |

### Typography

- Page titles: 28px, semibold, `text-primary`
- Section headers: 20px, semibold, colored left border accent
- Body: 16px, regular
- Captions/labels: 14px, `text-secondary`

### Spacing

8px base unit (8, 16, 24, 32, 48px increments)

## Component Library

### `components/theme.py` — Global CSS

Single `inject_css()` function called once in `app.py`. Styles cards, metric tiles, section headers, sidebar, page layout. All colors from design tokens.

### `components/cards.py` — Card Components

- `metric_card(label, value, delta=None, icon=None)` — KPI tile with optional trend
- `insight_card(title, body, severity="info")` — Colored callout (info/success/warning/danger)
- `stat_row(metrics: list[dict])` — Horizontal row of metric cards

### `components/charts.py` — Plotly Theme

- `apply_theme(fig)` — Consistent font, colors, margins, grid styling
- `COLORS` dict — Named color sequences (mastery levels, risk levels, districts)
- Removes Plotly clutter (logo, excess mode bar)

### `components/layout.py` — Page Structure

- `page_header(title, subtitle, icon=None)` — Page title with description
- `section(title, description=None)` — Section divider with colored left border
- `sidebar_branding()` — GitHub link, tech stack, author info
- `narrative(text)` — Styled paragraph for context between charts

### `components/__init__.py` — Re-exports everything

## App Structure

```
streamlit_app/
├── .streamlit/config.toml
├── app.py                          # CSS injection, sidebar branding
├── db.py                           # Unchanged
├── components/
│   ├── __init__.py
│   ├── theme.py
│   ├── cards.py
│   ├── charts.py
│   └── layout.py
├── pages/
│   ├── 1_📊_Executive_Overview.py
│   ├── 2_🎓_Classroom_Insights.py
│   ├── 3_🏫_District_Intelligence.py
│   ├── 4_🔬_Data_Quality_Lab.py
│   └── 5_🛡️_Pipeline_Governance.py
├── tabs/                           # DELETED
├── data/
│   └── lakehouse.duckdb
└── requirements.txt
```

## Page Designs

### Executive Overview (New Landing Page)

- **KPI row:** 350 Students | 14 Schools | 23 Standards | Pass Rate | At-Risk count
- **"What This Project Demonstrates" callout:** "A unified analytics layer built on two incompatible data standards — Ed-Fi XML and OneRoster CSV — harmonized through a Bronze-Silver-Gold lakehouse pipeline. The dashboards below present meaningful, actionable analytics on top of this unified layer."
- **Mastery distribution donut chart** — Exceeding/Meeting/Developing/Intervention breakdown
- **District comparison grouped bar** — Grand Bend vs Riverside by standard
- **Pipeline health horizontal bar** — Records per layer (Bronze/Silver/Gold/Quarantined)
- **Architecture diagram** — Simplified horizontal flow
- Every chart has narrative() or insight_card() explaining the "so what"

### Classroom Insights

- **KPI row:** Avg Mastery | Above Mastery % | Below Mastery % | Students Needing Intervention
- **Mastery Heatmap** — Same data, themed colors, narrative before, insight_card after with dynamic finding (e.g. most common gap standard)
- **Early Warning scatter** — Themed, narrative explaining quadrants, insight_card with count of dual-risk students
- **Misconception Clusters** — Styled cards instead of raw dataframe
- **Standards Dependency Chain** — Graphviz with themed node colors

### District Intelligence

- **Enrollment comparison** — Styled cards per district
- **Cross-district bar chart** — With narrative about which district leads
- **Curriculum effectiveness** — Themed bars with insight calling out the winner
- Dynamic insights generated from queries, not hardcoded

### Data Quality Lab

- Styled error toggle cards
- Processing results with proper metric cards
- Quarantine breakdown as themed bar chart
- Insight card summarizing what the simulation demonstrates

### Pipeline & Governance

- DQ Scorecard with metric cards and progress visual for pass rate
- PII Compliance with styled checklist (green checkmarks, proper cards)
- Lineage diagram with themed wrapper and narrative
- Insight: "All PII fields hashed at the Silver layer — zero raw PII in Gold"

## Page Pattern

Every page follows: **KPIs -> Narrative -> Chart -> Insight -> Repeat**

Each section tells you what you're looking at, shows the data, then tells you what it means. Insight cards are dynamically generated from queries.

## Sidebar Layout

```
┌─────────────────────────┐
│  Ed-Fi Lakehouse        │
│  Interoperability Demo  │
│                         │
│  ── Navigation ──       │  (auto from pages/)
│  📊 Executive Overview  │
│  🎓 Classroom Insights  │
│  🏫 District Intel      │
│  🔬 Data Quality Lab    │
│  🛡️ Pipeline Governance │
│                         │
│  ── Filters ──          │
│  District: [All ▼]      │
│                         │
│  ── About ──            │
│  GitHub repo link       │
│  Built by Vidya Kirti   │
│  DuckDB · dbt · PySpark │
│  Airflow · Streamlit    │
└─────────────────────────┘
```
