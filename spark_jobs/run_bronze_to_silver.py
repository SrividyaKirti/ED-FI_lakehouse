"""End-to-end Bronze-to-Silver pipeline runner.

Orchestrates the full pipeline:
1. Parse Ed-Fi XML into Parquet
2. Parse OneRoster CSV into Parquet
3. Apply PII hashing to student/user tables
4. Load Parquet into DuckDB Silver layer

Usage::

    python -m spark_jobs.run_bronze_to_silver

Or from Python::

    from spark_jobs.run_bronze_to_silver import run
    run()
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from spark_jobs.hash_pii import hash_pii_columns
from spark_jobs.load_to_duckdb import load_parquet_to_duckdb
from spark_jobs.parse_edfi_xml import run_all as edfi_run_all
from spark_jobs.parse_oneroster_csv import run_all as oneroster_run_all


def run(
    edfi_input: str = "data/bronze/edfi",
    oneroster_input: str = "data/bronze/oneroster",
    parquet_output: str = "data/silver",
    duckdb_path: str = "data/lakehouse.duckdb",
) -> None:
    """Run the full Bronze-to-Silver pipeline.

    Parameters
    ----------
    edfi_input : str or None
        Directory containing Ed-Fi XML interchange files.
        Pass ``None`` to skip Ed-Fi parsing.
    oneroster_input : str or None
        Directory containing OneRoster CSV files.
        Pass ``None`` to skip OneRoster parsing.
    parquet_output : str
        Root directory for Parquet output.  Ed-Fi tables go into
        ``parquet_output/edfi/`` and OneRoster tables into
        ``parquet_output/oneroster/``.
    duckdb_path : str
        Path to the DuckDB database file.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze_to_silver")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    try:
        # ------------------------------------------------------------------
        # Step 1 & 3a: Parse Ed-Fi XML and apply PII hashing to students
        # ------------------------------------------------------------------
        if edfi_input is not None:
            edfi_parquet = os.path.join(parquet_output, "edfi")
            print("Parsing Ed-Fi XML ...")
            edfi_dfs = edfi_run_all(spark, edfi_input, edfi_parquet)

            # Apply PII hashing to students table
            if "students" in edfi_dfs:
                print("Hashing PII in Ed-Fi students ...")
                hashed = hash_pii_columns(
                    edfi_dfs["students"],
                    name_cols=["first_name", "last_name"],
                    email_col="email",
                    birth_date_col="birth_date",
                )
                students_path = os.path.join(edfi_parquet, "students")
                hashed.write.mode("overwrite").parquet(students_path)

        # ------------------------------------------------------------------
        # Step 2 & 3b: Parse OneRoster CSV and apply PII hashing to users
        # ------------------------------------------------------------------
        if oneroster_input is not None:
            oneroster_parquet = os.path.join(parquet_output, "oneroster")
            print("Parsing OneRoster CSV ...")
            oneroster_dfs = oneroster_run_all(
                spark, oneroster_input, oneroster_parquet
            )

            # Apply PII hashing to users table.
            # OneRoster stores birth_date in the demographics table, not
            # users.  Join the two on sourced_id so hash_pii_columns can
            # generalise birth_date to birth_year.
            if "users" in oneroster_dfs:
                print("Hashing PII in OneRoster users ...")
                users_df = oneroster_dfs["users"]

                if "demographics" in oneroster_dfs:
                    demo_df = oneroster_dfs["demographics"].select(
                        "sourced_id", "birth_date"
                    )
                    users_df = users_df.join(
                        demo_df, on="sourced_id", how="left"
                    )

                hashed = hash_pii_columns(
                    users_df,
                    name_cols=["given_name", "family_name"],
                    email_col="email",
                    birth_date_col="birth_date",
                )
                users_path = os.path.join(oneroster_parquet, "users")
                hashed.write.mode("overwrite").parquet(users_path)

    finally:
        # ------------------------------------------------------------------
        # Step 4: Stop Spark
        # ------------------------------------------------------------------
        spark.stop()

    # ------------------------------------------------------------------
    # Step 5: Load Parquet into DuckDB
    # ------------------------------------------------------------------
    if edfi_input is not None:
        edfi_parquet = os.path.join(parquet_output, "edfi")
        print(f"Loading Ed-Fi Parquet into DuckDB ({duckdb_path}) ...")
        load_parquet_to_duckdb(edfi_parquet, duckdb_path, schema="silver_edfi")

    if oneroster_input is not None:
        oneroster_parquet = os.path.join(parquet_output, "oneroster")
        print(f"Loading OneRoster Parquet into DuckDB ({duckdb_path}) ...")
        load_parquet_to_duckdb(oneroster_parquet, duckdb_path, schema="silver_oneroster")

    print("Bronze-to-Silver pipeline complete.")


if __name__ == "__main__":
    run()
