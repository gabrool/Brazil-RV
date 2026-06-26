from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.bcb.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.bcb.quality import validate_asof_panel, validate_panel
from bralpha.derived.bcb.schemas import (
    BCB_FOCUS_EXPECTATION_ASOF_DAILY_COLUMNS,
    BCB_FOCUS_EXPECTATION_OBSERVATION_DAILY_COLUMNS,
    BCB_FOCUS_REFERENCE_DATES_COLUMNS,
    PANEL_PRIMARY_KEYS,
)

EXPECTATION_KEY_COLUMNS = [
    "endpoint",
    "indicator",
    "indicator_detail",
    "reference_period",
    "meeting",
    "is_top5",
    "calculation_type",
    "base_calculation",
]


def build_focus_expectation_observation_daily(
    *,
    general: pl.DataFrame | None,
    top5: pl.DataFrame | None,
    availability_note: str,
    include_general: bool,
    include_top5: bool,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    inputs = []
    if include_general and general is not None:
        inputs.append(general)
    if include_top5 and top5 is not None:
        inputs.append(top5)
    if not inputs:
        return _empty_observation()

    frame = pl.concat(inputs, how="diagonal_relaxed")
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(
            expectation_key=_expectation_key_expr(),
            availability_note=pl.lit(availability_note),
        )
        .select(BCB_FOCUS_EXPECTATION_OBSERVATION_DAILY_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["focus_expectation_observation_daily"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=BCB_FOCUS_EXPECTATION_OBSERVATION_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["focus_expectation_observation_daily"],
    )
    return frame


def build_focus_expectation_asof_daily(
    observations: pl.DataFrame,
    *,
    selected_indicators: list[str],
    max_dense_keys: int,
    start: date,
    end: date,
) -> pl.DataFrame:
    if observations.is_empty() or not business_days_mon_fri(start, end):
        return _empty_asof()

    obs = observations
    if selected_indicators:
        obs = obs.filter(pl.col("indicator").is_in(selected_indicators))
    obs = (
        obs.filter(pl.col("available_date").is_not_null())
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .sort(["expectation_key", "observation_available_date"])
    )
    if obs.is_empty():
        return _empty_asof()

    key_count = obs.select("expectation_key").unique().height
    if key_count > max_dense_keys:
        raise ValueError(
            f"Selected Focus expectation keys exceed max_dense_keys: {key_count} > "
            f"{max_dense_keys}"
        )

    grid = business_day_frame(start, end).join(
        obs.select("expectation_key").unique().sort("expectation_key"),
        how="cross",
    )
    frame = (
        grid.sort(["expectation_key", "ref_date"])
        .join_asof(
            obs,
            left_on="ref_date",
            right_on="observation_available_date",
            by="expectation_key",
            strategy="backward",
            check_sortedness=False,
        )
        .filter(pl.col("observation_available_date").is_not_null())
        .with_columns(
            available_date=pl.col("ref_date"),
            is_available=pl.lit(True),
            is_observed_on_ref_date=pl.col("observation_ref_date") == pl.col("ref_date"),
            staleness_days=(pl.col("ref_date") - pl.col("observation_available_date"))
            .dt.total_days()
            .cast(pl.Int64),
        )
        .select(BCB_FOCUS_EXPECTATION_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=BCB_FOCUS_EXPECTATION_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["focus_expectation_asof_daily"],
    )
    return frame


def build_focus_reference_dates(reference_dates: pl.DataFrame) -> pl.DataFrame:
    frame = (
        reference_dates.select(BCB_FOCUS_REFERENCE_DATES_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["focus_reference_dates"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=BCB_FOCUS_REFERENCE_DATES_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["focus_reference_dates"],
    )
    return frame


def _expectation_key_expr() -> pl.Expr:
    return pl.concat_str(
        [pl.col(column).cast(pl.Utf8).fill_null("<null>") for column in EXPECTATION_KEY_COLUMNS],
        separator="|",
    )


def _empty_observation() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in BCB_FOCUS_EXPECTATION_OBSERVATION_DAILY_COLUMNS}
    )


def _empty_asof() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in BCB_FOCUS_EXPECTATION_ASOF_DAILY_COLUMNS}
    )
