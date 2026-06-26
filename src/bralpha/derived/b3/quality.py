from __future__ import annotations

import polars as pl

from bralpha.derived.b3.schemas import BANNED_FEATURE_NAMES
from bralpha.quality.checks import (
    check_available_date_on_or_after_ref_date,
    check_no_duplicate_primary_keys,
    check_nonnegative_where_present,
    check_required_columns_present,
    check_row_count_not_zero,
)


def validate_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
    allow_empty: bool = False,
    nonnegative_columns: list[str] | None = None,
) -> None:
    if not allow_empty:
        check_row_count_not_zero(frame)
    check_required_columns_present(frame, required_columns)
    if not frame.is_empty():
        check_no_duplicate_primary_keys(frame, primary_keys)
        if "available_date" in frame.columns:
            check_available_date_on_or_after_ref_date(frame)
    for column in nonnegative_columns or []:
        check_nonnegative_where_present(frame, column)


def validate_target_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
    allow_empty: bool = False,
) -> None:
    if not allow_empty:
        check_row_count_not_zero(frame)
    check_required_columns_present(frame, required_columns)
    if frame.is_empty():
        return
    check_no_duplicate_primary_keys(frame, primary_keys)
    bad = frame.filter(pl.col("label_available_date") < pl.col("target_end_date")).height
    if bad:
        raise ValueError("label_available_date must be on or after target_end_date")


def assert_no_banned_feature_columns(frame: pl.DataFrame) -> None:
    lowered = {column.lower() for column in frame.columns}
    banned = sorted(lowered & BANNED_FEATURE_NAMES)
    if banned:
        raise ValueError(f"banned transformer feature columns present: {banned}")
