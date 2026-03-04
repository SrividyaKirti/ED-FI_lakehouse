"""Shared DuckDB connection for the Streamlit app."""
import os
import duckdb
import streamlit as st
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "lakehouse.duckdb")


@st.cache_resource
def get_connection():
    """Return a shared read-only DuckDB connection.

    NOTE: DuckDB connections are not thread-safe. This is acceptable for a
    single-user portfolio demo on Streamlit Cloud. For production multi-user
    apps, use per-request connections or a connection pool.
    """
    return duckdb.connect(DB_PATH, read_only=True)


def query(sql: str) -> pd.DataFrame:
    """Run a SQL query and return a pandas DataFrame."""
    con = get_connection()
    return con.execute(sql).fetchdf()
