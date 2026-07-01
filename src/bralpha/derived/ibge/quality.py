from __future__ import annotations

import polars as pl

from bralpha.quality.checks import (
    check_model_ready_panel_dates,
    check_no_duplicate_primary_keys,
    check_observation_panel_dates,
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
    check_observation_panel_dates(frame)


def validate_asof_panel(
    frame: pl.DataFrame,
    *,
    required_columns: list[str],
    primary_keys: list[str],
) -> None:
    check_required_columns_present(frame, required_columns)
    if frame.is_empty():
        return
    check_no_duplicate_primary_keys(frame, primary_keys)
    check_model_ready_panel_dates(frame)
