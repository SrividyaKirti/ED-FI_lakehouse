"""Tests for OneRoster CSV data generator."""

from __future__ import annotations

import csv
import os
from typing import List

import pytest

from data_generation.generate_oneroster_csv import generate_oneroster_district

EXPECTED_FILES = [
    "orgs.csv",
    "users.csv",
    "classes.csv",
    "courses.csv",
    "enrollments.csv",
    "academicSessions.csv",
    "lineItems.csv",
    "results.csv",
    "demographics.csv",
]


@pytest.fixture(scope="module")
def oneroster_output(tmp_path_factory):
    """Generate a small OneRoster dataset once for all tests in this module."""
    output_dir = str(tmp_path_factory.mktemp("oneroster_csv"))
    generate_oneroster_district(output_dir, num_students=200)
    return output_dir


def _read_csv(output_dir: str, filename: str) -> List[dict]:
    """Read a CSV file and return a list of dicts."""
    filepath = os.path.join(output_dir, filename)
    with open(filepath, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class TestGeneratesAllRequiredFiles:
    def test_generates_all_required_files(self, oneroster_output):
        """All 9 CSV files must exist in the output directory."""
        generated = os.listdir(oneroster_output)
        for filename in EXPECTED_FILES:
            assert filename in generated, f"Missing expected file: {filename}"


class TestUsersCsvHasCorrectHeaders:
    def test_users_csv_has_correct_headers(self, oneroster_output):
        """users.csv must have OneRoster-compliant headers."""
        filepath = os.path.join(oneroster_output, "users.csv")
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
        expected_headers = [
            "sourcedId",
            "status",
            "dateLastModified",
            "enabledUser",
            "username",
            "givenName",
            "familyName",
            "middleName",
            "identifier",
            "email",
            "sms",
            "phone",
            "role",
            "grades",
            "password",
        ]
        assert headers == expected_headers, (
            f"Headers mismatch.\nExpected: {expected_headers}\nGot: {headers}"
        )

    def test_users_have_oneroster_grade_format(self, oneroster_output):
        """Student grades must use numeric format (00, 01, ..., 12), not Ed-Fi text."""
        rows = _read_csv(oneroster_output, "users.csv")
        students = [r for r in rows if r["role"] == "student"]
        assert len(students) > 0, "No students found"
        valid_grades = {f"{g:02d}" for g in range(13)}
        for student in students:
            grade = student["grades"]
            if grade:  # some may be empty for planted issues
                assert grade in valid_grades, (
                    f"Grade '{grade}' is not in OneRoster numeric format"
                )

    def test_users_have_uuid_sourced_ids(self, oneroster_output):
        """Most user sourcedIds should be UUID format (not STU-XXXXX)."""
        rows = _read_csv(oneroster_output, "users.csv")
        uuid_count = 0
        for row in rows:
            sid = row["sourcedId"]
            if sid and len(sid) == 36 and sid.count("-") == 4:
                uuid_count += 1
        # At least 95% should be UUIDs (allowing for ~2% planted nulls)
        assert uuid_count >= len(rows) * 0.95, (
            f"Expected at least 95% UUID sourcedIds, got {uuid_count}/{len(rows)}"
        )


class TestEnrollmentsReferenceValidUsersAndClasses:
    def test_enrollments_reference_valid_users_and_classes(self, oneroster_output):
        """Most enrollment references should be valid (except planted issues)."""
        enrollments = _read_csv(oneroster_output, "enrollments.csv")
        users = _read_csv(oneroster_output, "users.csv")
        classes = _read_csv(oneroster_output, "classes.csv")

        user_ids = {r["sourcedId"] for r in users if r["sourcedId"]}
        class_ids = {r["sourcedId"] for r in classes if r["sourcedId"]}

        valid_user_refs = 0
        valid_class_refs = 0
        total = len(enrollments)

        for enrollment in enrollments:
            if enrollment["userSourcedId"] in user_ids:
                valid_user_refs += 1
            if enrollment["classSourcedId"] in class_ids:
                valid_class_refs += 1

        # Most user references should be valid (all should be, actually)
        assert valid_user_refs >= total * 0.95, (
            f"Too many invalid user references: {total - valid_user_refs}"
        )
        # Most class references should be valid, but some dangling are planted
        assert valid_class_refs >= total * 0.90, (
            f"Too many invalid class references: {total - valid_class_refs}"
        )


class TestResultsHaveItemLevelData:
    def test_results_have_item_level_data(self, oneroster_output):
        """results.csv must include item-level columns and score data."""
        rows = _read_csv(oneroster_output, "results.csv")
        assert len(rows) > 0, "No results found"

        # Check that item-level columns exist
        first_row = rows[0]
        item_columns = [
            "question_number",
            "standard_code",
            "correct_answer",
            "student_answer",
            "misconception_indicator",
        ]
        for col in item_columns:
            assert col in first_row, f"Missing item-level column: {col}"

        # Check that scores are present
        rows_with_score = [r for r in rows if r.get("score")]
        assert len(rows_with_score) > 0, "No results with score data found"


class TestPlantedDqIssuesInOneroster:
    def test_dangling_class_references_exist(self, oneroster_output):
        """At least 5 enrollments should reference non-existent classes."""
        enrollments = _read_csv(oneroster_output, "enrollments.csv")
        classes = _read_csv(oneroster_output, "classes.csv")

        class_ids = {r["sourcedId"] for r in classes if r["sourcedId"]}

        dangling = [
            e for e in enrollments
            if e["classSourcedId"] and e["classSourcedId"] not in class_ids
        ]
        assert len(dangling) >= 5, (
            f"Expected at least 5 dangling classSourcedId, got {len(dangling)}"
        )

    def test_future_begin_dates_exist(self, oneroster_output):
        """At least 3 enrollments should have future beginDate values."""
        enrollments = _read_csv(oneroster_output, "enrollments.csv")
        future = [
            e for e in enrollments
            if e.get("beginDate", "") >= "2027-01-01"
        ]
        assert len(future) >= 3, (
            f"Expected at least 3 future beginDates, got {len(future)}"
        )

    def test_null_sourced_ids_in_users(self, oneroster_output):
        """Approximately 2% of users should have null/empty sourcedId."""
        rows = _read_csv(oneroster_output, "users.csv")
        null_ids = [r for r in rows if not r["sourcedId"].strip()]
        assert len(null_ids) >= 1, (
            "Expected at least 1 user with null sourcedId"
        )

    def test_grade_level_violations_exist(self, oneroster_output):
        """5-10 K-2 students should be enrolled in AP classes."""
        enrollments = _read_csv(oneroster_output, "enrollments.csv")
        users = _read_csv(oneroster_output, "users.csv")
        classes = _read_csv(oneroster_output, "classes.csv")

        # Build lookups
        student_grades = {}
        for u in users:
            if u["role"] == "student" and u["sourcedId"]:
                student_grades[u["sourcedId"]] = u.get("grades", "")

        ap_class_ids = {
            c["sourcedId"] for c in classes
            if "AP" in c.get("title", "")
        }

        violations = 0
        for e in enrollments:
            if e["role"] != "student":
                continue
            user_grade = student_grades.get(e["userSourcedId"], "")
            if user_grade in ("00", "01", "02") and e["classSourcedId"] in ap_class_ids:
                violations += 1

        assert violations >= 5, (
            f"Expected at least 5 grade-level violations (K-2 in AP), got {violations}"
        )

    def test_misconception_patterns_in_results(self, oneroster_output):
        """Results should include misconception indicators for the 3 target standards."""
        rows = _read_csv(oneroster_output, "results.csv")
        misconception_rows = [
            r for r in rows
            if r.get("misconception_indicator") and r["misconception_indicator"].strip()
        ]
        assert len(misconception_rows) >= 3, (
            f"Expected at least 3 rows with misconception_indicator, got {len(misconception_rows)}"
        )

        # Verify the target standards are represented
        target_standards = {
            "CCSS.MATH.4.OA.A.1",
            "CCSS.MATH.3.NF.A.1",
            "CCSS.MATH.4.NF.B.3",
        }
        found_standards = {
            r["standard_code"] for r in misconception_rows
            if r.get("standard_code")
        }
        for std in target_standards:
            assert std in found_standards, (
                f"Missing misconception pattern for standard: {std}"
            )
