"""Load PySpark-written Parquet directories into DuckDB Silver layer.

PySpark writes Parquet as *directories* containing one or more
``part-*.parquet`` files.  This module scans for those directories and
loads each one into a DuckDB table using ``read_parquet`` with a glob
pattern.

Usage::

    from spark_jobs.load_to_duckdb import load_parquet_to_duckdb

    load_parquet_to_duckdb("data/silver/edfi", "data/lakehouse.duckdb")
"""

from __future__ import annotations

import os

import duckdb


def load_parquet_to_duckdb(
    parquet_dir: str,
    duckdb_path: str,
    schema: str = "silver",
) -> None:
    """Load all Parquet directories under *parquet_dir* into DuckDB.

    Parameters
    ----------
    parquet_dir : str
        Root directory that contains one sub-directory per table.  Each
        sub-directory is expected to contain ``part-*.parquet`` files
        produced by PySpark.
    duckdb_path : str
        Path to the DuckDB database file (created if it does not exist).
    schema : str, optional
        DuckDB schema to create the tables in (default ``"silver"``).
    """
    con = duckdb.connect(duckdb_path)

    # Ensure the target schema exists.
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for entry in sorted(os.listdir(parquet_dir)):
        entry_path = os.path.join(parquet_dir, entry)
        if not os.path.isdir(entry_path):
            continue

        # Skip hidden directories (e.g. _SUCCESS, _metadata, .crc)
        if entry.startswith("_") or entry.startswith("."):
            continue

        table_name = entry
        glob_path = os.path.join(entry_path, "*.parquet")

        con.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table_name} "
            f"AS SELECT * FROM read_parquet('{glob_path}')"
        )

        row_count = con.execute(
            f"SELECT COUNT(*) FROM {schema}.{table_name}"
        ).fetchone()[0]
        print(f"  {schema}.{table_name}: {row_count} rows")

    con.close()
