"""Plotly chart theming and color constants."""

import plotly.graph_objects as go
from components.theme import COLORS

# Named color sequences for consistent chart styling
MASTERY_COLORS = {
    "Exceeding": "#38A169",
    "Meeting": "#6C5CE7",
    "Developing": "#ED8936",
    "Needs Intervention": "#E53E3E",
}

RISK_COLORS = {
    "High": "#E53E3E",
    "Medium": "#ED8936",
    "Low": "#38A169",
}

DISTRICT_COLORS = {
    "Grand Bend ISD": "#0D7377",
    "Riverside USD": "#6C5CE7",
}

LAYER_COLORS = {
    "Bronze": "#ED8936",
    "Silver": "#A0AEC0",
    "Gold": "#D69E2E",
    "Quarantined": "#E53E3E",
}

CURRICULUM_COLORS = {
    "A": "#0D7377",
    "B": "#6C5CE7",
}

SUBJECT_COLORS = {
    "Math": "#0D7377",
    "ELA": "#6C5CE7",
    "Science": "#ED8936",
}


def apply_theme(fig: go.Figure) -> go.Figure:
    """Apply consistent styling to a Plotly figure."""
    fig.update_layout(
        font=dict(family="DM Sans, system-ui, sans-serif", color=COLORS["text_primary"]),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#E2E8F0",
            borderwidth=1,
            font=dict(size=12),
        ),
        xaxis=dict(
            gridcolor="#EDF2F7",
            linecolor="#E2E8F0",
            zerolinecolor="#E2E8F0",
        ),
        yaxis=dict(
            gridcolor="#EDF2F7",
            linecolor="#E2E8F0",
            zerolinecolor="#E2E8F0",
        ),
    )
    fig.update_layout(
        modebar=dict(
            bgcolor="rgba(0,0,0,0)",
            color="#A0AEC0",
            activecolor=COLORS["primary"],
        ),
    )
    return fig
