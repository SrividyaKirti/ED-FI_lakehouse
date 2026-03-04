"""Card components: metric tiles, insight callouts, stat rows."""

import streamlit as st


def metric_card(label: str, value: str, delta: str | None = None, delta_direction: str = "neutral") -> None:
    """Render a styled metric card with optional delta indicator."""
    delta_html = ""
    if delta:
        delta_class = delta_direction  # "positive", "negative", or "neutral"
        delta_html = f'<div class="metric-delta {delta_class}">{delta}</div>'

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def insight_card(title: str, body: str, severity: str = "info") -> None:
    """Render a colored insight callout. severity: info/success/warning/danger."""
    st.markdown(
        f"""
        <div class="insight-card {severity}">
            <div class="insight-title">{title}</div>
            <div class="insight-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_row(metrics: list[dict]) -> None:
    """Render a horizontal row of metric cards.

    Each dict: {"label": str, "value": str, "delta": str|None, "delta_direction": str}
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            metric_card(
                label=m["label"],
                value=m["value"],
                delta=m.get("delta"),
                delta_direction=m.get("delta_direction", "neutral"),
            )
