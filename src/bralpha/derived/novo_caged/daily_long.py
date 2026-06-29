from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.novo_caged.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.novo_caged.pit import ensure_novo_caged_pit_columns
from bralpha.derived.novo_caged.quality import validate_asof_panel
from bralpha.derived.novo_caged.schemas import (
    NOVO_CAGED_DAILY_LONG_COLUMNS,
    NOVO_CAGED_STATE_ASOF_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.timing.vintages import AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE

STATE_KEY_COLUMNS = ["source_family", "feature_id", "value_name"]

_MOVEMENT_COUNT_VALUES = [
    ("movement_count", "records"),
    ("wage_count", "observations"),
    ("contract_hours_count", "observations"),
]

_WAGE_HOURS_VALUES = [
    ("wage_mean", "BRL"),
    ("contract_hours_mean", "hours"),
]


def build_novo_caged_state_asof_daily(
    *,
    movement_groups: pl.DataFrame | None = None,
    start: date,
    end: date,
    include_movement_counts: bool,
    include_wage_hours: bool,
    max_features: int,
) -> pl.DataFrame:
    metrics: list[tuple[str, str]] = []
    if include_movement_counts:
        metrics.extend(_MOVEMENT_COUNT_VALUES)
    if include_wage_hours:
        metrics.extend(_WAGE_HOURS_VALUES)

    observations = _state_rows(
        movement_groups,
        source_family="novo_caged_movements",
        metrics=metrics,
    )
    if observations is None or observations.is_empty() or not business_days_mon_fri(start, end):
        return _empty_state()

    obs = (
        observations.filter(pl.col("observation_available_date").is_not_null())
        .sort(
            [
                *STATE_KEY_COLUMNS,
                "observation_available_date",
                "first_seen_timestamp_utc",
                "revision_sequence",
                "vintage_id",
                "observation_ref_date",
            ]
        )
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
            f"Selected Novo CAGED feature count {feature_count} exceeds "
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
        .select(NOVO_CAGED_STATE_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=NOVO_CAGED_STATE_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["state_asof_daily"],
    )
    return frame


def build_novo_caged_daily_long(
    *,
    state_asof_daily: pl.DataFrame | None = None,
) -> pl.DataFrame:
    if state_asof_daily is None or state_asof_daily.is_empty():
        return _empty_daily_long()

    frame = (
        state_asof_daily.filter(pl.col("source_family") == "novo_caged_movements")
        .filter(pl.col("model_usable").fill_null(False))
        .filter(pl.col("availability_basis") != AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE)
        .filter(pl.col("value").is_not_null())
        .select(NOVO_CAGED_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=NOVO_CAGED_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _state_rows(
    frame: pl.DataFrame | None,
    *,
    source_family: str,
    metrics: list[tuple[str, str]],
) -> pl.DataFrame | None:
    if frame is None or frame.is_empty() or not metrics:
        return None
    frame = ensure_novo_caged_pit_columns(frame)
    rows: list[pl.DataFrame] = []
    for metric, unit in metrics:
        rows.append(
            frame.select(
                [
                    pl.lit(source_family).alias("source_family"),
                    pl.col("feature_id"),
                    pl.lit(metric).alias("value_name"),
                    pl.col("ref_date").alias("observation_ref_date"),
                    pl.col("available_date").alias("observation_available_date"),
                    pl.col("availability_policy"),
                    pl.col("availability_basis"),
                    pl.col("revision_policy"),
                    pl.col("release_date"),
                    pl.col("source_publication_datetime_utc"),
                    pl.col("source_last_modified_utc"),
                    pl.col("first_seen_timestamp_utc"),
                    pl.col("vintage_id"),
                    pl.col("revision_sequence"),
                    pl.col("model_usable"),
                    pl.col("model_usable_reason"),
                    pl.col(metric).cast(pl.Float64).alias("value"),
                    pl.lit(unit).alias("unit"),
                    pl.col("source_version"),
                ]
            )
        )
    return pl.concat(rows, how="diagonal_relaxed")


def _empty_state() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in NOVO_CAGED_STATE_ASOF_DAILY_COLUMNS})


def _empty_daily_long() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in NOVO_CAGED_DAILY_LONG_COLUMNS})
