"""Drill-down navigation: breadcrumb, state management, SQL helpers."""

from __future__ import annotations

import streamlit as st

# ── Navigation levels in drill-down order ──────────────────────────────────
LEVELS = ("district", "school", "grade", "section", "student")

# Keys stored in session state for navigation
_NAV_KEYS = {
    "nav_level": "district",
    "nav_district": None,
    "nav_school_id": None,
    "nav_school_name": None,
    "nav_grade": None,
    "nav_section_id": None,
    "nav_section_name": None,
    "nav_student_id": None,
    "nav_path": "by-school",
    "subject_filter": "All",
}


def init_nav_state() -> None:
    """Ensure all navigation keys exist in session_state with defaults."""
    for key, default in _NAV_KEYS.items():
        if key not in st.session_state:
            st.session_state[key] = default


def drill_into(level: str, **kwargs) -> None:
    """Update session state to drill into *level* with the given context.

    Example::

        drill_into("school", nav_school_id="GB-ES-001",
                   nav_school_name="Grand Bend Elementary 1",
                   nav_district="Grand Bend ISD")
    """
    st.session_state["nav_level"] = level
    for k, v in kwargs.items():
        st.session_state[k] = v


def go_back() -> None:
    """Navigate up one level, clearing state below."""
    current = st.session_state.get("nav_level", "district")
    idx = LEVELS.index(current) if current in LEVELS else 0
    if idx <= 0:
        return
    new_level = LEVELS[idx - 1]
    st.session_state["nav_level"] = new_level

    # Clear state below the new level
    clear_from = LEVELS.index(new_level) + 1
    clear_map = {
        "school": ("nav_school_id", "nav_school_name"),
        "grade": ("nav_grade",),
        "section": ("nav_section_id", "nav_section_name"),
        "student": ("nav_student_id",),
    }
    for lvl in LEVELS[clear_from:]:
        for key in clear_map.get(lvl, ()):
            st.session_state[key] = None


def go_to_level(level: str) -> None:
    """Jump to a specific level, clearing everything below it."""
    st.session_state["nav_level"] = level
    clear_map = {
        "school": ("nav_school_id", "nav_school_name"),
        "grade": ("nav_grade",),
        "section": ("nav_section_id", "nav_section_name"),
        "student": ("nav_student_id",),
    }
    idx = LEVELS.index(level)
    for lvl in LEVELS[idx + 1 :]:
        for key in clear_map.get(lvl, ()):
            st.session_state[key] = None


def breadcrumb() -> None:
    """Render a clickable breadcrumb trail for the current drill-down path."""
    parts: list[str] = []
    level = st.session_state.get("nav_level", "district")

    district = st.session_state.get("nav_district")
    school_name = st.session_state.get("nav_school_name")
    grade = st.session_state.get("nav_grade")
    section_name = st.session_state.get("nav_section_name")
    student_id = st.session_state.get("nav_student_id")

    if district:
        parts.append(district)
    if school_name and level in ("school", "grade", "section", "student"):
        parts.append(school_name)
    if grade is not None and level in ("grade", "section", "student"):
        grade_label = "Kindergarten" if grade == 0 else f"Grade {grade}"
        parts.append(grade_label)
    if section_name and level in ("section", "student"):
        parts.append(section_name)
    if student_id and level == "student":
        parts.append(student_id)

    if not parts:
        return

    trail = " > ".join(parts)
    st.markdown(f"**{trail}**")


def back_button() -> bool:
    """Render a back button. Returns True if the user clicked it."""
    level = st.session_state.get("nav_level", "district")
    if level == "district":
        return False
    if st.button("← Back", key="nav_back"):
        go_back()
        st.rerun()
    return False


def subject_where(col: str = "subject") -> str:
    """Return a SQL WHERE fragment for the current subject filter.

    Returns empty string if filter is 'All'.
    """
    subj = st.session_state.get("subject_filter", "All")
    if subj == "All":
        return ""
    return f" AND {col} = '{subj}'"


def school_where(col: str = "school_id") -> str:
    """Return a SQL WHERE fragment for the current school selection."""
    sid = st.session_state.get("nav_school_id")
    if not sid:
        return ""
    return f" AND {col} = '{sid}'"


def district_where(col: str = "district_name") -> str:
    """Return a SQL WHERE fragment for the current district."""
    d = st.session_state.get("nav_district")
    if not d:
        return ""
    return f" AND {col} = '{d}'"
