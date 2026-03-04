"""Tests for DuckDB Parquet loader."""

import os
import tempfile

import duckdb
import pytest

from spark_jobs.load_to_duckdb import load_parquet_to_duckdb


@pytest.fixture()
def parquet_dir(tmp_path):
    """Create a directory mimicking PySpark Parquet output.

    PySpark writes Parquet as a *directory* containing one or more
    ``part-*.parquet`` files.  We simulate this by writing a single
    part file inside a named sub-directory.
    """
    table_dir = tmp_path / "test_table"
    table_dir.mkdir()
    part_file = str(table_dir / "part-00000.parquet")

    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT 1 AS id, 'alice' AS name) TO '{part_file}' (FORMAT PARQUET)"
    )
    con.close()
    return tmp_path


@pytest.fixture()
def duckdb_path(tmp_path):
    """Return a path for a temporary DuckDB file."""
    return str(tmp_path / "test.duckdb")


class TestLoadsParquetFilesAsTables:
    def test_loads_parquet_files_as_tables(self, parquet_dir, duckdb_path):
        """Parquet directory is loaded as a table with correct data."""
        load_parquet_to_duckdb(str(parquet_dir), duckdb_path)

        con = duckdb.connect(duckdb_path, read_only=True)
        rows = con.execute("SELECT * FROM silver.test_table").fetchall()
        con.close()

        assert len(rows) == 1
        assert rows[0] == (1, "alice")


class TestCreatesSchemaIfNotExists:
    def test_creates_schema_if_not_exists(self, parquet_dir, duckdb_path):
        """The specified schema is created even when it does not exist yet."""
        load_parquet_to_duckdb(str(parquet_dir), duckdb_path, schema="custom")

        con = duckdb.connect(duckdb_path, read_only=True)
        schemas = [
            row[0]
            for row in con.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        ]
        con.close()

        assert "custom" in schemas
