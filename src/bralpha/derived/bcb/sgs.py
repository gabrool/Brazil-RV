from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.bcb.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.bcb.quality import validate_asof_panel, validate_panel
from bralpha.derived.bcb.schemas import (
    BCB_SGS_ASOF_DAILY_COLUMNS,
    BCB_SGS_OBSERVATION_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_sgs_observation_daily(
    silver: pl.DataFrame,
    *,
    include_model_usable_only: bool,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if include_model_usable_only:
        frame = frame.filter(pl.col("model_usable") == True)  # noqa: E712

    frame = frame.select(BCB_SGS_OBSERVATION_DAILY_COLUMNS)
    validate_panel(
        frame,
        required_columns=BCB_SGS_OBSERVATION_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["sgs_observation_daily"],
    )
    return frame


def build_sgs_asof_daily(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
) -> pl.DataFrame:
    if observations.is_empty() or not business_days_mon_fri(start, end):
        return _empty_asof()

    obs = (
        observations.filter(pl.col("available_date").is_not_null())
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .sort(["series_id", "observation_available_date"])
    )
    if obs.is_empty():
        return _empty_asof()

    grid = business_day_frame(start, end).join(
        obs.select("series_id").unique().sort("series_id"),
        how="cross",
    )
    frame = (
        grid.sort(["series_id", "ref_date"])
        .join_asof(
            obs,
            left_on="ref_date",
            right_on="observation_available_date",
            by="series_id",
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
        .select(BCB_SGS_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=BCB_SGS_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["sgs_asof_daily"],
    )
    return frame


def _empty_asof() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in BCB_SGS_ASOF_DAILY_COLUMNS})
