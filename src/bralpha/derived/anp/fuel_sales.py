from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.anp.quality import validate_panel
from bralpha.derived.anp.schemas import (
    ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS,
    ANP_FUEL_SALES_OBSERVATION_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.parsing.common import normalize_column_name

_GROUP_BY_COLUMNS = {"all", "region", "state"}


def build_fuel_sales_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty(ANP_FUEL_SALES_OBSERVATION_COLUMNS)

    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(ANP_FUEL_SALES_OBSERVATION_COLUMNS)

    frame = (
        frame.with_columns(has_sales_volume_m3=pl.col("sales_volume_m3").is_not_null())
        .select(ANP_FUEL_SALES_OBSERVATION_COLUMNS)
        .sort(["ref_date", "state", "product"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["fuel_sales_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=ANP_FUEL_SALES_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fuel_sales_observation"],
    )
    return frame


def build_fuel_sales_group_observation(
    observations: pl.DataFrame,
    *,
    group_by: list[str],
    max_groups: int,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    _validate_groups(group_by)
    if observations.is_empty():
        return _empty(ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS)

    frame = observations
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS)

    parts = [_aggregate_group(frame, group_type) for group_type in group_by]
    panel = (
        pl.concat(parts, how="diagonal_relaxed")
        if parts
        else _empty(ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS)
    )
    if panel.is_empty():
        return panel

    feature_count = panel.select("feature_id").unique().height
    if feature_count > max_groups:
        raise ValueError(
            f"ANP fuel sales group count {feature_count} exceeds max_groups={max_groups}"
        )

    panel = panel.select(ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS).sort(
        ["ref_date", "group_type", "group_value", "product"]
    )
    validate_panel(
        panel,
        required_columns=ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fuel_sales_group_observation"],
    )
    return panel


def anp_fuel_sales_feature_id(
    group_type: object,
    group_value: object,
    product: object,
) -> str:
    return f"anp_fuel_sales|{_token(group_type)}|{_token(group_value)}|{_token(product)}"


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
        working.group_by(["ref_date", "group_type", "group_value", "product"])
        .agg(
            available_date=pl.col("available_date").max(),
            availability_policy=pl.col("availability_policy").drop_nulls().first(),
            sales_volume_m3=_null_aware_sum("sales_volume_m3"),
            sales_volume_count=pl.col("sales_volume_m3").is_not_null().sum().cast(pl.Int64),
            state_count=pl.col("state").n_unique().cast(pl.Int64),
            unit=pl.col("unit").drop_nulls().first(),
            source_version=pl.col("source_version").drop_nulls().first(),
        )
        .with_columns(
            feature_id=pl.struct(["group_type", "group_value", "product"]).map_elements(
                lambda row: anp_fuel_sales_feature_id(
                    row["group_type"],
                    row["group_value"],
                    row["product"],
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
        raise ValueError(f"Unsupported ANP fuel sales grouping(s): {unknown}")


def _token(value: object) -> str:
    if value is None:
        return "unknown"
    token = normalize_column_name(str(value).strip())
    return token or "unknown"


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in columns})
