"""Tests for PII hashing module."""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from spark_jobs.hash_pii import hash_pii_columns


@pytest.fixture(scope="module")
def spark():
    """Create a module-scoped SparkSession for all tests."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_hash_pii")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def sample_df(spark):
    """Create a small DataFrame with PII columns for testing."""
    data = [
        Row(
            id="1",
            first_name="Alice",
            last_name="Smith",
            email="alice@example.com",
            birth_date="2015-03-14",
            score=95,
        ),
        Row(
            id="2",
            first_name="Bob",
            last_name="Jones",
            email="bob@example.com",
            birth_date="2014-07-22",
            score=88,
        ),
    ]
    return spark.createDataFrame(data)


class TestHashPiiReplacesNameColumns:
    def test_hash_pii_replaces_name_columns(self, sample_df):
        """Original PII columns removed; hash columns added."""
        result = hash_pii_columns(
            sample_df,
            name_cols=["first_name", "last_name"],
            email_col="email",
            birth_date_col="birth_date",
        )

        result_cols = set(result.columns)

        # Original PII columns must be gone
        assert "first_name" not in result_cols
        assert "last_name" not in result_cols
        assert "email" not in result_cols
        assert "birth_date" not in result_cols

        # Hash columns must be present
        assert "first_name_hash" in result_cols
        assert "last_name_hash" in result_cols
        assert "email_hash" in result_cols
        assert "birth_year" in result_cols

        # Non-PII columns must be preserved
        assert "id" in result_cols
        assert "score" in result_cols


class TestHashIsDeterministic:
    def test_hash_is_deterministic(self, sample_df):
        """Same input produces same hash across two invocations."""
        result1 = hash_pii_columns(
            sample_df,
            name_cols=["first_name", "last_name"],
            email_col="email",
            birth_date_col="birth_date",
        )
        result2 = hash_pii_columns(
            sample_df,
            name_cols=["first_name", "last_name"],
            email_col="email",
            birth_date_col="birth_date",
        )

        rows1 = {
            row["id"]: row["first_name_hash"]
            for row in result1.collect()
        }
        rows2 = {
            row["id"]: row["first_name_hash"]
            for row in result2.collect()
        }
        assert rows1 == rows2


class TestBirthDateGeneralizedToYear:
    def test_birth_date_generalized_to_year(self, sample_df):
        """birth_date '2015-03-14' must produce birth_year 2015."""
        result = hash_pii_columns(
            sample_df,
            name_cols=["first_name", "last_name"],
            email_col="email",
            birth_date_col="birth_date",
        )

        alice = result.filter(result["id"] == "1").collect()[0]
        assert alice["birth_year"] == 2015

        bob = result.filter(result["id"] == "2").collect()[0]
        assert bob["birth_year"] == 2014
