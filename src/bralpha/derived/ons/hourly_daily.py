from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.ons.hydro import ons_feature_id
from bralpha.derived.ons.quality import validate_panel
from bralpha.derived.ons.schemas import (
    ONS_ENERGY_BALANCE_DAILY_OBSERVATION_COLUMNS,
    ONS_INTERCHANGE_DAILY_OBSERVATION_COLUMNS,
    ONS_PIT_SNAPSHOT_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.timing.vintages import (
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
)

ENERGY_BALANCE_METRICS = [
    ("load_mwmed", "load_count"),
    ("hydro_generation_mwmed", "hydro_generation_count"),
    ("thermal_generation_mwmed", "thermal_generation_count"),
    ("wind_generation_mwmed", "wind_generation_count"),
    ("solar_generation_mwmed", "solar_generation_count"),
    ("other_generation_mwmed", "other_generation_count"),
    ("interchange_mwmed", "interchange_count"),
]

INTERCHANGE_METRICS = [
    ("interchange_mwmed", "interchange_count"),
    ("programmed_interchange_mwmed", "programmed_interchange_count"),
]


def build_energy_balance_daily_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
    min_hour_count: int = 1,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_energy_balance()

    frame = _ensure_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty_energy_balance()

    metric_cols = [metric for metric, _ in ENERGY_BALANCE_METRICS]
    frame = frame.with_columns([pl.col(column).cast(pl.Float64) for column in metric_cols])
    aggregated = (
        frame.group_by(["ref_date", "subsystem_id", "subsystem", "vintage_id"])
        .agg(
            [
                pl.col("available_date").max().alias("available_date"),
                pl.col("availability_policy").max().alias("availability_policy"),
                pl.col("availability_basis").max().alias("availability_basis"),
                pl.col("revision_policy").max().alias("revision_policy"),
                pl.col("model_usable").fill_null(False).all().alias("model_usable"),
                *[
                    pl.col(column).max().alias(column)
                    for column in ONS_PIT_SNAPSHOT_COLUMNS
                    if column != "vintage_id"
                ],
                pl.col("availability_note").max().alias("availability_note"),
                pl.len().cast(pl.Int64).alias("hour_count"),
                pl.col("unit").max().alias("unit"),
                pl.col("source_version").max().alias("source_version"),
                *[pl.col(metric).mean().alias(metric) for metric in metric_cols],
                *[
                    pl.col(metric).is_not_null().sum().cast(pl.Int64).alias(count_col)
                    for metric, count_col in ENERGY_BALANCE_METRICS
                ],
            ]
        )
        .filter(pl.col("hour_count") >= min_hour_count)
        .with_columns(
            feature_id=pl.struct(["subsystem_id", "subsystem"]).map_elements(
                lambda row: ons_feature_id(
                    "ons_energy_balance_daily",
                    row["subsystem_id"],
                    row["subsystem"],
                ),
                return_dtype=pl.Utf8,
            )
        )
        .select(ONS_ENERGY_BALANCE_DAILY_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["energy_balance_daily_observation"],
            keep="last",
            maintain_order=True,
        )
        .sort(["subsystem_id", "ref_date"])
    )
    validate_panel(
        aggregated,
        required_columns=ONS_ENERGY_BALANCE_DAILY_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["energy_balance_daily_observation"],
    )
    return aggregated


def build_interchange_daily_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
    min_hour_count: int = 1,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_interchange()

    frame = _ensure_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty_interchange()

    metric_cols = [metric for metric, _ in INTERCHANGE_METRICS]
    frame = frame.with_columns([pl.col(column).cast(pl.Float64) for column in metric_cols])
    aggregated = (
        frame.group_by(
            [
                "ref_date",
                "source_subsystem_id",
                "source_subsystem",
                "target_subsystem_id",
                "target_subsystem",
                "vintage_id",
            ]
        )
        .agg(
            [
                pl.col("available_date").max().alias("available_date"),
                pl.col("availability_policy").max().alias("availability_policy"),
                pl.col("availability_basis").max().alias("availability_basis"),
                pl.col("revision_policy").max().alias("revision_policy"),
                pl.col("model_usable").fill_null(False).all().alias("model_usable"),
                *[
                    pl.col(column).max().alias(column)
                    for column in ONS_PIT_SNAPSHOT_COLUMNS
                    if column != "vintage_id"
                ],
                pl.col("availability_note").max().alias("availability_note"),
                pl.len().cast(pl.Int64).alias("hour_count"),
                pl.col("unit").max().alias("unit"),
                pl.col("source_version").max().alias("source_version"),
                *[pl.col(metric).mean().alias(metric) for metric in metric_cols],
                *[
                    pl.col(metric).is_not_null().sum().cast(pl.Int64).alias(count_col)
                    for metric, count_col in INTERCHANGE_METRICS
                ],
            ]
        )
        .filter(pl.col("hour_count") >= min_hour_count)
        .with_columns(
            feature_id=pl.struct(
                [
                    "source_subsystem_id",
                    "source_subsystem",
                    "target_subsystem_id",
                    "target_subsystem",
                ]
            ).map_elements(
                lambda row: ons_feature_id(
                    "ons_interchange_daily",
                    row["source_subsystem_id"],
                    row["source_subsystem"],
                    row["target_subsystem_id"],
                    row["target_subsystem"],
                ),
                return_dtype=pl.Utf8,
            )
        )
        .select(ONS_INTERCHANGE_DAILY_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["interchange_daily_observation"],
            keep="last",
            maintain_order=True,
        )
        .sort(["source_subsystem_id", "target_subsystem_id", "ref_date"])
    )
    validate_panel(
        aggregated,
        required_columns=ONS_INTERCHANGE_DAILY_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["interchange_daily_observation"],
    )
    return aggregated


def _empty_energy_balance() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in ONS_ENERGY_BALANCE_DAILY_OBSERVATION_COLUMNS}
    )


def _empty_interchange() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in ONS_INTERCHANGE_DAILY_OBSERVATION_COLUMNS}
    )


def _ensure_pit_columns(frame: pl.DataFrame) -> pl.DataFrame:
    additions = []
    if "availability_basis" not in frame.columns:
        additions.append(
            pl.lit(AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE).alias("availability_basis")
        )
    if "revision_policy" not in frame.columns:
        additions.append(pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY).alias("revision_policy"))
    if "model_usable" not in frame.columns:
        additions.append(pl.lit(False).alias("model_usable"))
    for column in ONS_PIT_SNAPSHOT_COLUMNS:
        if column not in frame.columns:
            additions.append(pl.lit(None).alias(column))
    if "availability_note" not in frame.columns:
        additions.append(pl.lit(None, dtype=pl.Utf8).alias("availability_note"))
    return frame.with_columns(additions) if additions else frame
