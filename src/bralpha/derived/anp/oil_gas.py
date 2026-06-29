from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.anp.pit import anp_pit_aggregations, ensure_anp_pit_columns
from bralpha.derived.anp.quality import validate_panel
from bralpha.derived.anp.schemas import (
    ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS,
    ANP_OIL_GAS_PRODUCTION_OBSERVATION_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.parsing.common import normalize_column_name

_GROUP_BY_COLUMNS = {"all", "region", "state"}


def build_oil_gas_production_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty(ANP_OIL_GAS_PRODUCTION_OBSERVATION_COLUMNS)

    frame = ensure_anp_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(ANP_OIL_GAS_PRODUCTION_OBSERVATION_COLUMNS)

    frame = (
        frame.with_columns(has_metric_value=pl.col("metric_value").is_not_null())
        .select(ANP_OIL_GAS_PRODUCTION_OBSERVATION_COLUMNS)
        .sort(["ref_date", "state", "location", "metric_type"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["oil_gas_production_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=ANP_OIL_GAS_PRODUCTION_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["oil_gas_production_observation"],
    )
    return frame


def build_oil_gas_group_observation(
    observations: pl.DataFrame,
    *,
    group_by: list[str],
    max_groups: int,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    _validate_groups(group_by)
    if observations.is_empty():
        return _empty(ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS)

    frame = observations
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS)

    parts = [_aggregate_group(frame, group_type) for group_type in group_by]
    panel = (
        pl.concat(parts, how="diagonal_relaxed")
        if parts
        else _empty(ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS)
    )
    if panel.is_empty():
        return panel

    feature_count = panel.select("feature_id").unique().height
    if feature_count > max_groups:
        raise ValueError(
            f"ANP oil/gas group count {feature_count} exceeds max_groups={max_groups}"
        )

    panel = panel.select(ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS).sort(
        ["ref_date", "group_type", "group_value", "location", "product", "metric_type"]
    )
    validate_panel(
        panel,
        required_columns=ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["oil_gas_group_observation"],
    )
    return panel


def anp_oil_gas_feature_id(
    group_type: object,
    group_value: object,
    location: object,
    product: object,
    metric_type: object,
) -> str:
    return (
        "anp_oil_gas|"
        f"{_token(group_type)}|{_token(group_value)}|{_token(location)}|"
        f"{_token(product)}|{_token(metric_type)}"
    )


def _aggregate_group(frame: pl.DataFrame, group_type: str) -> pl.DataFrame:
    working = frame.with_columns(
        group_type=pl.lit(group_type),
        group_value=(
            pl.lit("all")
            if group_type == "all"
            else pl.col(group_type).map_elements(_token, return_dtype=pl.Utf8)
        ),
    )
    return (
        working.group_by(
            [
                "ref_date",
                "group_type",
                "group_value",
                "location",
                "product",
                "metric_type",
                "vintage_id",
            ]
        )
        .agg(
            [
                pl.col("available_date").max(),
                pl.col("availability_policy").drop_nulls().first(),
                *anp_pit_aggregations(),
                _null_aware_sum("metric_value"),
                pl.col("metric_value")
                .is_not_null()
                .sum()
                .cast(pl.Int64)
                .alias("metric_value_count"),
                pl.col("state").n_unique().cast(pl.Int64).alias("state_count"),
                pl.col("unit").drop_nulls().first(),
                pl.col("source_version").drop_nulls().first(),
            ]
        )
        .with_columns(
            feature_id=pl.struct(
                ["group_type", "group_value", "location", "product", "metric_type"]
            ).map_elements(
                lambda row: anp_oil_gas_feature_id(
                    row["group_type"],
                    row["group_value"],
                    row["location"],
                    row["product"],
                    row["metric_type"],
                ),
                return_dtype=pl.Utf8,
            )
        )
    )


def _null_aware_sum(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).is_not_null().any())
        .then(pl.col(column).sum())
        .otherwise(None)
        .alias(column)
    )


def _validate_groups(group_by: list[str]) -> None:
    unknown = sorted(set(group_by) - _GROUP_BY_COLUMNS)
    if unknown:
        raise ValueError(f"Unsupported ANP oil/gas grouping(s): {unknown}")


def _token(value: object) -> str:
    if value is None:
        return "unknown"
    token = normalize_column_name(str(value).strip())
    return token or "unknown"


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in columns})
