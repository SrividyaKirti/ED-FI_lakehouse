"""Tests for the PySpark Ed-Fi XML parser."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

from data_generation.generate_edfi_xml import generate_edfi_district
from spark_jobs.parse_edfi_xml import (
    parse_assessment_results,
    parse_attendance,
    parse_enrollments,
    parse_grades,
    parse_schools,
    parse_section_associations,
    parse_sections,
    parse_staff,
    parse_standards,
    parse_students,
    run_all,
)


@pytest.fixture(scope="module")
def spark():
    """Create a module-scoped SparkSession for all tests."""
    # Ensure PySpark workers use the same Python as the test runner
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_parse_edfi")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def edfi_xml_dir(tmp_path_factory):
    """Generate a small Ed-Fi dataset once for all tests in this module."""
    output_dir = str(tmp_path_factory.mktemp("edfi_xml"))
    generate_edfi_district(output_dir, num_students=50)
    return output_dir


class TestParseStudents:
    def test_parse_students_returns_flat_dataframe(self, spark, edfi_xml_dir):
        """Row count matches XML student count; all required columns present."""
        df = parse_students(spark, edfi_xml_dir)
        assert df.count() == 50

        required_columns = {
            "student_unique_id",
            "first_name",
            "last_name",
            "birth_date",
            "email",
            "_source_system",
            "_loaded_at",
        }
        assert required_columns.issubset(set(df.columns))


class TestParseEnrollments:
    def test_parse_enrollments_returns_flat_dataframe(self, spark, edfi_xml_dir):
        """Enrollment count > 0 and all required columns present."""
        df = parse_enrollments(spark, edfi_xml_dir)
        assert df.count() > 0

        required_columns = {
            "student_unique_id",
            "school_id",
            "entry_date",
            "grade_level_descriptor",
            "_source_system",
            "_loaded_at",
        }
        assert required_columns.issubset(set(df.columns))


class TestParseAssessments:
    def test_parse_assessments_has_item_responses(self, spark, edfi_xml_dir):
        """Assessment item count > 0 and all required columns present."""
        df = parse_assessment_results(spark, edfi_xml_dir)
        assert df.count() > 0

        required_columns = {
            "student_unique_id",
            "assessment_id",
            "question_number",
            "standard_code",
            "correct_answer",
            "student_answer",
            "score",
            "assessment_date",
            "misconception_indicator",
            "_source_system",
            "_loaded_at",
        }
        assert required_columns.issubset(set(df.columns))


class TestParseAttendance:
    def test_parse_attendance_returns_daily_events(self, spark, edfi_xml_dir):
        """Attendance event count > 0 and all required columns present."""
        df = parse_attendance(spark, edfi_xml_dir)
        assert df.count() > 0

        required_columns = {
            "student_unique_id",
            "school_id",
            "event_date",
            "attendance_status",
            "_source_system",
            "_loaded_at",
        }
        assert required_columns.issubset(set(df.columns))


class TestSourceSystemColumn:
    def test_all_dataframes_have_source_system_column(self, spark, edfi_xml_dir):
        """Every parsed DataFrame must have _source_system = 'edfi'."""
        parse_functions = [
            parse_students,
            parse_schools,
            parse_staff,
            parse_sections,
            parse_enrollments,
            parse_section_associations,
            parse_grades,
            parse_assessment_results,
            parse_attendance,
            parse_standards,
        ]
        for fn in parse_functions:
            df = fn(spark, edfi_xml_dir)
            assert "_source_system" in df.columns, (
                f"{fn.__name__} is missing _source_system column"
            )
            # Every row should have _source_system == "edfi"
            non_edfi = df.filter(df["_source_system"] != "edfi").count()
            assert non_edfi == 0, (
                f"{fn.__name__} has {non_edfi} rows where _source_system != 'edfi'"
            )
