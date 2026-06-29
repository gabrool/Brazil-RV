from __future__ import annotations

import polars as pl

from bralpha.quality.checks import (
    check_available_date_on_or_after_ref_date,
    check_no_duplicate_primary_keys,
    check_required_columns_present,
)


def validate_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
) -> None:
    check_required_columns_present(frame, required_columns)
    if frame.is_empty():
        return
    check_no_duplicate_primary_keys(frame, primary_keys)
    if "available_date" in frame.columns and "ref_date" in frame.columns:
        check_available_date_on_or_after_ref_date(frame)


def validate_asof_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
) -> None:
    validate_panel(frame, required_columns=required_columns, primary_keys=primary_keys)
    if frame.is_empty():
        return
    bad = frame.filter(pl.col("observation_available_date") > pl.col("ref_date")).height
    if bad:
        raise ValueError("Receita as-of panel uses observations after ref_date")
