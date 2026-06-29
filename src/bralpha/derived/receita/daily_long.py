from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.receita.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.receita.quality import validate_asof_panel
from bralpha.derived.receita.schemas import (
    PANEL_PRIMARY_KEYS,
    RECEITA_DAILY_LONG_COLUMNS,
    RECEITA_STATE_ASOF_DAILY_COLUMNS,
)

STATE_KEY_COLUMNS = ["source_family", "feature_id", "value_name"]


def build_receita_state_asof_daily(
    *,
    feature_observations: pl.DataFrame | None = None,
    start: date,
    end: date,
    max_features: int,
) -> pl.DataFrame:
    observations = _state_rows(feature_observations)
    if observations is None or observations.is_empty() or not business_days_mon_fri(start, end):
        return _empty_state()

    obs = (
        observations.filter(pl.col("observation_available_date").is_not_null())
        .sort([*STATE_KEY_COLUMNS, "observation_available_date", "observation_ref_date"])
        .unique(
            subset=[*STATE_KEY_COLUMNS, "observation_available_date"],
            keep="last",
            maintain_order=True,
        )
        .sort([*STATE_KEY_COLUMNS, "observation_available_date"])
    )
    if obs.is_empty():
        return _empty_state()

    feature_count = obs.select(STATE_KEY_COLUMNS).unique().height
    if feature_count > max_features:
        raise ValueError(
            f"Receita tax-collection feature count {feature_count} exceeds "
            f"max_features={max_features}"
        )

    grid = business_day_frame(start, end).join(
        obs.select(STATE_KEY_COLUMNS).unique().sort(STATE_KEY_COLUMNS),
        how="cross",
    )
    frame = (
        grid.sort([*STATE_KEY_COLUMNS, "ref_date"])
        .join_asof(
            obs,
            left_on="ref_date",
            right_on="observation_available_date",
            by=STATE_KEY_COLUMNS,
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
        .select(RECEITA_STATE_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=RECEITA_STATE_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["state_asof_daily"],
    )
    return frame


def build_receita_daily_long(
    *,
    state_asof_daily: pl.DataFrame | None = None,
    include_tax_collection: bool,
) -> pl.DataFrame:
    if not include_tax_collection or state_asof_daily is None or state_asof_daily.is_empty():
        return _empty_daily_long()

    frame = (
        state_asof_daily.filter(pl.col("source_family") == "receita_tax_collection")
        .filter(pl.col("value").is_not_null())
        .select(RECEITA_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=RECEITA_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _state_rows(frame: pl.DataFrame | None) -> pl.DataFrame | None:
    if frame is None or frame.is_empty():
        return None
    return frame.select(
        [
            pl.lit("receita_tax_collection").alias("source_family"),
            pl.col("feature_id"),
            pl.lit("collection_amount_brl").alias("value_name"),
            pl.col("ref_date").alias("observation_ref_date"),
            pl.col("available_date").alias("observation_available_date"),
            pl.col("collection_amount_brl").cast(pl.Float64).alias("value"),
            pl.col("unit"),
            pl.col("source_version"),
        ]
    )


def _empty_state() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in RECEITA_STATE_ASOF_DAILY_COLUMNS})


def _empty_daily_long() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in RECEITA_DAILY_LONG_COLUMNS})
