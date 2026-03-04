#!/usr/bin/env python
"""Write reference data to dbt seed CSVs.

Usage (from repo root)::

    python -m data_generation.write_seeds

This reads from ``reference_data.py`` and writes three CSV files into
``dbt_project/seeds/``.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

from data_generation.reference_data import (
    get_learning_standards,
    get_misconception_patterns,
    get_school_registry,
)

SEED_DIR = Path(__file__).resolve().parent.parent / "dbt_project" / "seeds"


def _write_csv(rows: list[dict], filename: str) -> Path:
    """Write a list of dicts to *filename* inside the seed directory."""
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    filepath = SEED_DIR / filename
    if not rows:
        raise ValueError(f"No rows to write for {filename}")
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return filepath


def main() -> None:
    seeds = [
        (get_school_registry(), "seed_school_registry.csv"),
        (get_learning_standards(), "seed_learning_standards.csv"),
        (get_misconception_patterns(), "seed_misconception_patterns.csv"),
    ]
    for rows, filename in seeds:
        path = _write_csv(rows, filename)
        print(f"  wrote {len(rows):>3} rows -> {path}")


if __name__ == "__main__":
    main()
