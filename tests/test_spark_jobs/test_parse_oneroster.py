"""Tests for the PySpark OneRoster CSV parser."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

from data_generation.generate_oneroster_csv import generate_oneroster_district
from spark_jobs.parse_oneroster_csv import (
    parse_academic_sessions,
    parse_classes,
    parse_courses,
    parse_demographics,
    parse_enrollments,
    parse_line_items,
    parse_orgs,
    parse_results,
    parse_users,
    run_all,
)

NUM_STUDENTS = 20


@pytest.fixture(scope="module")
def spark():
    """Create a module-scoped SparkSession for all tests."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_parse_oneroster")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def oneroster_csv_dir(tmp_path_factory):
    """Generate a small OneRoster dataset once for all tests in this module."""
    output_dir = str(tmp_path_factory.mktemp("oneroster_csv"))
    generate_oneroster_district(output_dir, num_students=NUM_STUDENTS)
    return output_dir


class TestParseUsers:
    def test_parse_users_returns_flat_dataframe(self, spark, oneroster_csv_dir):
        """Row count > num_students (includes teachers); required columns present."""
        df = parse_users(spark, oneroster_csv_dir)
        assert df.count() > NUM_STUDENTS

        required_columns = {
            "sourced_id",
            "given_name",
            "family_name",
            "email",
            "role",
            "grades",
            "_source_system",
            "_loaded_at",
        }
        assert required_columns.issubset(set(df.columns))


class TestParseEnrollments:
    def test_parse_enrollments_has_correct_roles(self, spark, oneroster_csv_dir):
        """Both 'student' and 'teacher' roles must be present."""
        df = parse_enrollments(spark, oneroster_csv_dir)
        roles = {row["role"] for row in df.select("role").distinct().collect()}
        assert "student" in roles
        assert "teacher" in roles


class TestParseResults:
    def test_parse_results_has_score_data(self, spark, oneroster_csv_dir):
        """Result count > 0; score and line_item columns present."""
        df = parse_results(spark, oneroster_csv_dir)
        assert df.count() > 0

        required_columns = {
            "score",
            "line_item_sourced_id",
            "_source_system",
            "_loaded_at",
        }
        assert required_columns.issubset(set(df.columns))


class TestSourceSystemColumn:
    def test_all_dataframes_have_source_system_column(self, spark, oneroster_csv_dir):
        """Every parsed DataFrame must have _source_system = 'oneroster'."""
        parse_functions = [
            parse_users,
            parse_orgs,
            parse_classes,
            parse_courses,
            parse_enrollments,
            parse_academic_sessions,
            parse_line_items,
            parse_results,
            parse_demographics,
        ]
        for fn in parse_functions:
            df = fn(spark, oneroster_csv_dir)
            assert "_source_system" in df.columns, (
                f"{fn.__name__} is missing _source_system column"
            )
            non_oneroster = df.filter(df["_source_system"] != "oneroster").count()
            assert non_oneroster == 0, (
                f"{fn.__name__} has {non_oneroster} rows where "
                f"_source_system != 'oneroster'"
            )
