from __future__ import annotations

import polars as pl

from bralpha.quality.checks import (
    check_no_duplicate_primary_keys,
    check_nonnegative_where_present,
    check_observation_panel_dates,
    check_required_columns_present,
    check_row_count_not_zero,
)


def validate_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
    nonnegative_columns: list[str] | None = None,
) -> None:
    check_row_count_not_zero(frame)
    check_required_columns_present(frame, required_columns)
    if not frame.is_empty():
        check_no_duplicate_primary_keys(frame, primary_keys)
        check_observation_panel_dates(frame)
    for column in nonnegative_columns or []:
        check_nonnegative_where_present(frame, column)


def validate_target_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
) -> None:
    check_row_count_not_zero(frame)
    check_required_columns_present(frame, required_columns)
    if frame.is_empty():
        return
    check_no_duplicate_primary_keys(frame, primary_keys)
    bad = frame.filter(pl.col("label_available_date") < pl.col("target_end_date")).height
    if bad:
        raise ValueError("label_available_date must be on or after target_end_date")
