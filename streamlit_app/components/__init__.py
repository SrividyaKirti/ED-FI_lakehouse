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
)
from components.layout import page_header, section, narrative, setup_page
