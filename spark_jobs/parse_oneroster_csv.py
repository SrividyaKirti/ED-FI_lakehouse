"""Parse OneRoster 1.2 CSV files into flat PySpark DataFrames and write as Parquet.

This module implements the Bronze layer of the lakehouse pipeline for
OneRoster data: it reads raw CSV files (produced by the data generator),
renames columns from camelCase to snake_case, adds metadata columns, and
writes them as Parquet for downstream ingestion.

Usage::

    from pyspark.sql import SparkSession
    from spark_jobs.parse_oneroster_csv import run_all

    spark = SparkSession.builder.getOrCreate()
    run_all(spark, "data/bronze/oneroster", "data/bronze/parquet_oneroster")
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_meta(df: DataFrame) -> DataFrame:
    """Add _source_system and _loaded_at metadata columns."""
    return (
        df
        .withColumn("_source_system", lit("oneroster"))
        .withColumn("_loaded_at", lit(datetime.now().isoformat()))
    )


def _read_csv(spark: SparkSession, input_dir: str, filename: str) -> DataFrame:
    """Read a CSV file with headers into a DataFrame."""
    path = os.path.join(input_dir, filename)
    return spark.read.option("header", "true").csv(path)


# ---------------------------------------------------------------------------
# Individual parse functions
# ---------------------------------------------------------------------------


def parse_users(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse users.csv into a flat DataFrame.

    Output: sourced_id, given_name, family_name, email, role, grades,
            _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "users.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("givenName", "given_name")
        .withColumnRenamed("familyName", "family_name")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "given_name", "family_name", "email",
        "role", "grades", "_source_system", "_loaded_at",
    )


def parse_orgs(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse orgs.csv into a flat DataFrame.

    Output: sourced_id, name, type, identifier, parent_sourced_id,
            _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "orgs.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("parentSourcedId", "parent_sourced_id")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "name", "type", "identifier", "parent_sourced_id",
        "_source_system", "_loaded_at",
    )


def parse_classes(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse classes.csv into a flat DataFrame.

    Output: sourced_id, title, grades, course_sourced_id, class_code,
            class_type, school_sourced_id, term_sourced_ids,
            _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "classes.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("courseSourcedId", "course_sourced_id")
        .withColumnRenamed("classCode", "class_code")
        .withColumnRenamed("classType", "class_type")
        .withColumnRenamed("schoolSourcedId", "school_sourced_id")
        .withColumnRenamed("termSourcedIds", "term_sourced_ids")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "title", "grades", "course_sourced_id", "class_code",
        "class_type", "school_sourced_id", "term_sourced_ids",
        "_source_system", "_loaded_at",
    )


def parse_courses(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse courses.csv into a flat DataFrame.

    Output: sourced_id, title, course_code, grades, org_sourced_id,
            _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "courses.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("courseCode", "course_code")
        .withColumnRenamed("orgSourcedId", "org_sourced_id")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "title", "course_code", "grades", "org_sourced_id",
        "_source_system", "_loaded_at",
    )


def parse_enrollments(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse enrollments.csv into a flat DataFrame.

    Output: sourced_id, class_sourced_id, school_sourced_id, user_sourced_id,
            role, begin_date, end_date, _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "enrollments.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("classSourcedId", "class_sourced_id")
        .withColumnRenamed("schoolSourcedId", "school_sourced_id")
        .withColumnRenamed("userSourcedId", "user_sourced_id")
        .withColumnRenamed("beginDate", "begin_date")
        .withColumnRenamed("endDate", "end_date")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "class_sourced_id", "school_sourced_id",
        "user_sourced_id", "role", "begin_date", "end_date",
        "_source_system", "_loaded_at",
    )


def parse_academic_sessions(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse academicSessions.csv into a flat DataFrame.

    Output: sourced_id, title, type, start_date, end_date, parent_sourced_id,
            school_year, _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "academicSessions.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("startDate", "start_date")
        .withColumnRenamed("endDate", "end_date")
        .withColumnRenamed("parentSourcedId", "parent_sourced_id")
        .withColumnRenamed("schoolYear", "school_year")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "title", "type", "start_date", "end_date",
        "parent_sourced_id", "school_year", "_source_system", "_loaded_at",
    )


def parse_line_items(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse lineItems.csv into a flat DataFrame.

    Output: sourced_id, title, assign_date, due_date, class_sourced_id,
            result_value_min, result_value_max, _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "lineItems.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("assignDate", "assign_date")
        .withColumnRenamed("dueDate", "due_date")
        .withColumnRenamed("classSourcedId", "class_sourced_id")
        .withColumnRenamed("resultValueMin", "result_value_min")
        .withColumnRenamed("resultValueMax", "result_value_max")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "title", "assign_date", "due_date", "class_sourced_id",
        "result_value_min", "result_value_max", "_source_system", "_loaded_at",
    )


def parse_results(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse results.csv into a flat DataFrame.

    Output: sourced_id, line_item_sourced_id, student_sourced_id, score,
            score_date, question_number, standard_code, correct_answer,
            student_answer, misconception_indicator,
            _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "results.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("lineItemSourcedId", "line_item_sourced_id")
        .withColumnRenamed("studentSourcedId", "student_sourced_id")
        .withColumnRenamed("scoreDate", "score_date")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "line_item_sourced_id", "student_sourced_id", "score",
        "score_date", "question_number", "standard_code", "correct_answer",
        "student_answer", "misconception_indicator",
        "_source_system", "_loaded_at",
    )


def parse_demographics(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse demographics.csv into a flat DataFrame.

    Output: sourced_id, birth_date, sex, plus race/ethnicity columns,
            _source_system, _loaded_at
    """
    df = _read_csv(spark, input_dir, "demographics.csv")
    df = (
        df
        .withColumnRenamed("sourcedId", "sourced_id")
        .withColumnRenamed("birthDate", "birth_date")
        .withColumnRenamed("americanIndianOrAlaskaNative", "american_indian_or_alaska_native")
        .withColumnRenamed("blackOrAfricanAmerican", "black_or_african_american")
        .withColumnRenamed("nativeHawaiianOrOtherPacificIslander", "native_hawaiian_or_other_pacific_islander")
        .withColumnRenamed("demographicRaceTwoOrMoreRaces", "demographic_race_two_or_more_races")
        .withColumnRenamed("hispanicOrLatinoEthnicity", "hispanic_or_latino_ethnicity")
    )
    df = _add_meta(df)
    return df.select(
        "sourced_id", "birth_date", "sex",
        "american_indian_or_alaska_native", "asian",
        "black_or_african_american",
        "native_hawaiian_or_other_pacific_islander",
        "white", "demographic_race_two_or_more_races",
        "hispanic_or_latino_ethnicity",
        "_source_system", "_loaded_at",
    )


# ---------------------------------------------------------------------------
# run_all: orchestrate parsing and Parquet writes
# ---------------------------------------------------------------------------

_PARSE_REGISTRY = {
    "users": parse_users,
    "orgs": parse_orgs,
    "classes": parse_classes,
    "courses": parse_courses,
    "enrollments": parse_enrollments,
    "academic_sessions": parse_academic_sessions,
    "line_items": parse_line_items,
    "results": parse_results,
    "demographics": parse_demographics,
}


def run_all(
    spark: SparkSession,
    input_dir: str,
    output_dir: str,
) -> Dict[str, DataFrame]:
    """Parse all OneRoster CSV files and write each as Parquet.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    input_dir : str
        Directory containing the OneRoster CSV files.
    output_dir : str
        Directory where Parquet files will be written (one sub-directory
        per entity, e.g. ``output_dir/users/``).

    Returns
    -------
    dict[str, DataFrame]
        Mapping from entity name to its parsed DataFrame.
    """
    os.makedirs(output_dir, exist_ok=True)
    results: Dict[str, DataFrame] = {}

    for name, parse_fn in _PARSE_REGISTRY.items():
        df = parse_fn(spark, input_dir)
        parquet_path = os.path.join(output_dir, name)
        df.write.mode("overwrite").parquet(parquet_path)
        results[name] = df

    return results
