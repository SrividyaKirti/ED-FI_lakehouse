"""Reusable UI components for the Ed-Fi Lakehouse app."""

from components.theme import COLORS, inject_css
from components.cards import metric_card, insight_card, stat_row
from components.charts import (
    apply_theme,
    MASTERY_COLORS,
    RISK_COLORS,
    DISTRICT_COLORS,
    LAYER_COLORS,
    CURRICULUM_COLORS,
    SUBJECT_COLORS,
)
from components.layout import page_header, section, narrative, setup_page
from components.navigation import (
    init_nav_state,
    drill_into,
    go_back,
    go_to_level,
    breadcrumb,
    back_button,
    subject_where,
    school_where,
    district_where,
)
