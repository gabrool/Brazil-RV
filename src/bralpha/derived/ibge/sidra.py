from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.ibge.calendar import business_day_frame, business_days_b3
from bralpha.derived.ibge.quality import validate_asof_panel, validate_panel
from bralpha.derived.ibge.schemas import (
    IBGE_SIDRA_ASOF_DAILY_COLUMNS,
    IBGE_SIDRA_OBSERVATION_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.ingestion.ibge.sidra import SidraSeriesConfig


def build_sidra_observation(
    silver: pl.DataFrame,
    *,
    series_config: list[SidraSeriesConfig],
    include_model_usable_only: bool,
    include_priorities: list[str],
    selected_dataset_slugs: list[str],
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_observation()

    metadata = _series_metadata(
        series_config,
        include_priorities=include_priorities,
        selected_dataset_slugs=selected_dataset_slugs,
    )
    if metadata.is_empty():
        return _empty_observation()

    frame = silver.join(metadata, on="dataset_slug", how="inner")
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if include_model_usable_only:
        frame = frame.filter(pl.col("model_usable") == True)  # noqa: E712

    frame = frame.with_columns(has_value=pl.col("value").is_not_null()).select(
        IBGE_SIDRA_OBSERVATION_COLUMNS
    )
    validate_panel(
        frame,
        required_columns=IBGE_SIDRA_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["sidra_observation"],
    )
    return frame


def build_sidra_asof_daily(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_dense_features: int,
) -> pl.DataFrame:
    if observations.is_empty() or not business_days_b3(start, end):
        return _empty_asof()

    obs = (
        observations.filter(
            pl.col("available_date").is_not_null() & (pl.col("model_usable") == True)  # noqa: E712
        )
        .with_columns(_feature_id_expr())
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .sort(["feature_id", "observation_available_date", "observation_ref_date", "period_code"])
        .unique(
            subset=["feature_id", "observation_available_date"],
            keep="last",
            maintain_order=True,
        )
        .sort(["feature_id", "observation_available_date"])
    )
    if obs.is_empty():
        return _empty_asof()

    feature_count = obs.get_column("feature_id").n_unique()
    if feature_count > max_dense_features:
        raise ValueError(
            f"IBGE SIDRA as-of feature count {feature_count} exceeds "
            f"max_dense_features={max_dense_features}"
        )

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
        .select(IBGE_SIDRA_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=IBGE_SIDRA_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["sidra_asof_daily"],
    )
    return frame


def _series_metadata(
    series_config: list[SidraSeriesConfig],
    *,
    include_priorities: list[str],
    selected_dataset_slugs: list[str],
) -> pl.DataFrame:
    priorities = {priority.upper() for priority in include_priorities}
    selected = set(selected_dataset_slugs)
    rows = [
        {
            "dataset_slug": item.dataset_slug,
            "frequency": item.frequency,
        }
        for item in series_config
        if item.priority.upper() in priorities and item.dataset_slug in selected
    ]
    if not rows:
        return pl.DataFrame(schema={"dataset_slug": pl.Utf8, "frequency": pl.Utf8})
    return pl.DataFrame(rows)


def _feature_id_expr() -> pl.Expr:
    return pl.concat_str(
        [
            pl.lit("ibge_sidra:"),
            pl.col("dataset_slug").cast(pl.Utf8),
            pl.lit(":"),
            pl.col("aggregate_id").cast(pl.Utf8),
            pl.lit(":"),
            pl.col("variable_id").cast(pl.Utf8),
            pl.lit(":"),
            pl.col("geography_id").cast(pl.Utf8).fill_null("__null__"),
            pl.lit(":"),
            pl.col("classification_key").cast(pl.Utf8).fill_null("__null__"),
        ]
    ).alias("feature_id")


def _empty_observation() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in IBGE_SIDRA_OBSERVATION_COLUMNS})


def _empty_asof() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in IBGE_SIDRA_ASOF_DAILY_COLUMNS})
