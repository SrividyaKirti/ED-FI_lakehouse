"""Hash PII columns using SHA-256 for privacy-preserving analytics.

This module is called by the Bronze-to-Silver runner to hash personally
identifiable information before loading into the Silver layer.

Usage::

    from spark_jobs.hash_pii import hash_pii_columns

    hashed_df = hash_pii_columns(
        df,
        name_cols=["first_name", "last_name"],
        email_col="email",
        birth_date_col="birth_date",
    )
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, year


def hash_pii_columns(
    df: DataFrame,
    name_cols: List[str],
    email_col: str,
    birth_date_col: str,
) -> DataFrame:
    """Hash PII columns and drop the originals.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing PII columns.
    name_cols : list[str]
        Column names containing names to hash (e.g. ["first_name", "last_name"]).
        Each produces a ``{col}_hash`` column.
    email_col : str
        Column name containing email addresses.
        Produces an ``email_hash`` column.
    birth_date_col : str
        Column name containing birth dates (string format "YYYY-MM-DD").
        Produces a ``birth_year`` integer column.

    Returns
    -------
    DataFrame
        DataFrame with PII columns replaced by hashed/generalized versions.
        Original PII columns are dropped.
    """
    # Hash each name column
    for name_col in name_cols:
        df = df.withColumn(f"{name_col}_hash", sha2(col(name_col).cast("string"), 256))

    # Hash email
    df = df.withColumn("email_hash", sha2(col(email_col).cast("string"), 256))

    # Extract birth year
    df = df.withColumn("birth_year", year(col(birth_date_col).cast("date")))

    # Drop original PII columns
    columns_to_drop = list(name_cols) + [email_col, birth_date_col]
    df = df.drop(*columns_to_drop)

    return df
