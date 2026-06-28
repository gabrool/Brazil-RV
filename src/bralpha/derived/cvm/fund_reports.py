from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.cvm.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.cvm.quality import validate_asof_panel, validate_panel
from bralpha.derived.cvm.schemas import (
    CVM_FUND_DAILY_OBSERVATION_COLUMNS,
    CVM_FUND_FLOWS_DAILY_COLUMNS,
    CVM_FUND_GROUP_OBSERVATION_COLUMNS,
    CVM_FUND_STATE_ASOF_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.parsing.common import normalize_column_name

_GROUP_BY_COLUMNS = {"all", "fund_type"}


def build_fund_daily_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty(CVM_FUND_DAILY_OBSERVATION_COLUMNS)

    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(
            has_portfolio_value=pl.col("portfolio_value").is_not_null(),
            has_nav=pl.col("nav").is_not_null(),
            has_quota_value=pl.col("quota_value").is_not_null(),
            has_subscriptions=pl.col("subscriptions").is_not_null(),
            has_redemptions=pl.col("redemptions").is_not_null(),
            has_shareholder_count=pl.col("shareholder_count").is_not_null(),
        )
        .select(CVM_FUND_DAILY_OBSERVATION_COLUMNS)
        .sort(["ref_date", "fund_id"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["fund_daily_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=CVM_FUND_DAILY_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fund_daily_observation"],
    )
    return frame


def build_fund_group_observation(
    observations: pl.DataFrame,
    *,
    group_by: list[str],
    max_groups: int,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    unknown = sorted(set(group_by) - _GROUP_BY_COLUMNS)
    if unknown:
        raise ValueError(f"Unsupported CVM fund grouping(s): {unknown}")
    if observations.is_empty():
        return _empty(CVM_FUND_GROUP_OBSERVATION_COLUMNS)

    frame = observations
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(CVM_FUND_GROUP_OBSERVATION_COLUMNS)

    parts = [_aggregate_group(frame, group_type) for group_type in group_by]
    panel = (
        pl.concat(parts, how="diagonal_relaxed")
        if parts
        else _empty(CVM_FUND_GROUP_OBSERVATION_COLUMNS)
    )
    if panel.is_empty():
        return panel

    group_count = panel.select(["group_type", "group_value"]).unique().height
    if group_count > max_groups:
        raise ValueError(f"CVM group count {group_count} exceeds max_groups={max_groups}")

    panel = panel.select(CVM_FUND_GROUP_OBSERVATION_COLUMNS).sort(
        ["ref_date", "group_type", "group_value"]
    )
    validate_panel(
        panel,
        required_columns=CVM_FUND_GROUP_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fund_group_observation"],
    )
    return panel


def build_fund_flows_daily(
    group_observations: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if group_observations.is_empty():
        return _empty(CVM_FUND_FLOWS_DAILY_COLUMNS)

    frame = (
        group_observations.filter(pl.col("available_date").is_not_null())
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .with_columns(
            ref_date=pl.col("observation_available_date"),
            available_date=pl.col("observation_available_date"),
        )
    )
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.select(CVM_FUND_FLOWS_DAILY_COLUMNS)
        .sort(["ref_date", "feature_id", "observation_ref_date"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["fund_flows_daily"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=CVM_FUND_FLOWS_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fund_flows_daily"],
    )
    return frame


def build_fund_state_asof_daily(
    group_observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_groups: int,
) -> pl.DataFrame:
    if group_observations.is_empty() or not business_days_mon_fri(start, end):
        return _empty(CVM_FUND_STATE_ASOF_DAILY_COLUMNS)

    obs = (
        group_observations.filter(pl.col("available_date").is_not_null())
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .sort(["feature_id", "observation_available_date", "observation_ref_date"])
        .unique(
            subset=["feature_id", "observation_available_date"],
            keep="last",
            maintain_order=True,
        )
        .sort(["feature_id", "observation_available_date"])
    )
    if obs.is_empty():
        return _empty(CVM_FUND_STATE_ASOF_DAILY_COLUMNS)

    group_count = obs.select("feature_id").unique().height
    if group_count > max_groups:
        raise ValueError(f"CVM group count {group_count} exceeds max_groups={max_groups}")

    grid = business_day_frame(start, end).join(
        obs.select("feature_id").unique().sort("feature_id"),
        how="cross",
    )
    frame = (
        grid.sort(["feature_id", "ref_date"])
        .join_asof(
            obs,
            left_on="ref_date",
            right_on="observation_available_date",
            by="feature_id",
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
        .select(CVM_FUND_STATE_ASOF_DAILY_COLUMNS)
        .sort(["ref_date", "feature_id"])
    )
    validate_asof_panel(
        frame,
        required_columns=CVM_FUND_STATE_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fund_state_asof_daily"],
    )
    return frame


def cvm_group_feature_id(group_type: object, group_value: object) -> str:
    type_token = normalize_column_name(str(group_type).strip()) or "null"
    value_token = _token(group_value)
    return f"cvm_fund_group|{type_token}|{value_token}"


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
        working.group_by(["ref_date", "group_type", "group_value"])
        .agg(
            available_date=pl.col("available_date").max(),
            portfolio_value=_null_aware_sum("portfolio_value"),
            nav=_null_aware_sum("nav"),
            subscriptions=_null_aware_sum("subscriptions"),
            redemptions=_null_aware_sum("redemptions"),
            shareholder_count=_null_aware_sum("shareholder_count"),
            fund_count=pl.col("fund_id").n_unique(),
            portfolio_value_count=pl.col("portfolio_value").is_not_null().sum(),
            nav_count=pl.col("nav").is_not_null().sum(),
            subscriptions_count=pl.col("subscriptions").is_not_null().sum(),
            redemptions_count=pl.col("redemptions").is_not_null().sum(),
            shareholder_count_count=pl.col("shareholder_count").is_not_null().sum(),
            source_version=pl.col("source_version").drop_nulls().first(),
        )
        .with_columns(
            feature_id=pl.struct(["group_type", "group_value"]).map_elements(
                lambda row: cvm_group_feature_id(row["group_type"], row["group_value"]),
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


def _token(value: object) -> str:
    if value is None:
        return "unknown"
    token = normalize_column_name(str(value).strip())
    return token or "unknown"


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in columns})
