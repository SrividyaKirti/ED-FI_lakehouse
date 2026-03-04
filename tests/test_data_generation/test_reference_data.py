"""Tests for reference data: school registry, learning standards, misconception patterns."""

from data_generation.reference_data import (
    get_learning_standards,
    get_misconception_patterns,
    get_school_registry,
)


class TestLearningStandards:
    def test_learning_standards_has_prerequisite_chains(self):
        """At least 3 standards must list a prerequisite standard code."""
        standards = get_learning_standards()
        with_prereqs = [
            s for s in standards if s["prerequisite_standard_code"] != ""
        ]
        assert len(with_prereqs) >= 3, (
            f"Expected at least 3 standards with prerequisites, got {len(with_prereqs)}"
        )

    def test_learning_standards_cover_grades_k_through_5(self):
        """Grade levels must span the full K-5 range (0 through 5)."""
        standards = get_learning_standards()
        grade_levels = {s["grade_level"] for s in standards}
        expected = {0, 1, 2, 3, 4, 5}
        assert expected.issubset(grade_levels), (
            f"Missing grade levels: {expected - grade_levels}"
        )


class TestSchoolRegistry:
    def test_school_registry_has_both_districts(self):
        """Registry must contain schools from both Grand Bend ISD and Riverside USD."""
        schools = get_school_registry()
        district_names = {s["district_name"] for s in schools}
        assert "Grand Bend ISD" in district_names
        assert "Riverside USD" in district_names

    def test_school_registry_has_school_types(self):
        """Registry must include Elementary, Middle, and High school types."""
        schools = get_school_registry()
        school_types = {s["school_type"] for s in schools}
        assert school_types == {"Elementary", "Middle", "High"}


class TestMisconceptionPatterns:
    def test_misconception_patterns_exist(self):
        """At least 3 misconception patterns must exist and reference the 3 key standards."""
        patterns = get_misconception_patterns()
        assert len(patterns) >= 3, (
            f"Expected at least 3 patterns, got {len(patterns)}"
        )
        standard_codes = {p["standard_code"] for p in patterns}
        for code in [
            "CCSS.MATH.4.OA.A.1",
            "CCSS.MATH.3.NF.A.1",
            "CCSS.MATH.4.NF.B.3",
        ]:
            assert code in standard_codes, (
                f"Missing required standard code {code} in misconception patterns"
            )
