from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.fred.calendar import business_day_frame, business_days_b3
from bralpha.derived.fred.quality import validate_asof_panel, validate_panel
from bralpha.derived.fred.schemas import (
    FRED_ASOF_DAILY_COLUMNS,
    FRED_OBSERVATION_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.ingestion.fred.common import FredSeriesConfig
from bralpha.parsing.common import normalize_column_name


def build_fred_observation(
    silver: pl.DataFrame,
    *,
    series_config: list[FredSeriesConfig],
    include_model_usable_only: bool,
    include_priorities: list[str],
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_observation()

    metadata = _series_metadata(series_config, include_priorities=include_priorities)
    if metadata.is_empty():
        return _empty_observation()

    frame = _ensure_columns(
        silver.with_columns(pl.col("series_id").cast(pl.Utf8).str.to_uppercase()),
        FRED_OBSERVATION_COLUMNS,
    )
    frame = frame.join(metadata, on="series_id", how="inner")
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if include_model_usable_only:
        frame = frame.filter(pl.col("model_usable"))

    frame = (
        frame.with_columns(
            feature_id=_feature_id_expr(),
            has_value=pl.col("value").is_not_null(),
        )
        .select(FRED_OBSERVATION_COLUMNS)
        .sort(["series_id", "ref_date"])
    )
    validate_panel(
        frame,
        required_columns=FRED_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["observation"],
    )
    return frame


def build_fred_asof_daily(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_dense_series: int,
) -> pl.DataFrame:
    if observations.is_empty() or not business_days_b3(start, end):
        return _empty_asof()

    observations = _ensure_columns(observations, FRED_OBSERVATION_COLUMNS)
    obs = (
        observations.filter(pl.col("available_date").is_not_null() & pl.col("model_usable"))
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .sort(
            [
                "feature_id",
                "observation_available_date",
                "observation_ref_date",
                "vintage_date",
            ]
        )
        .unique(
            subset=["feature_id", "observation_available_date", "observation_ref_date"],
            keep="last",
            maintain_order=True,
        )
        .sort(["feature_id", "observation_available_date"])
    )
    if obs.is_empty():
        return _empty_asof()

    series_count = obs.select("feature_id").unique().height
    if series_count > max_dense_series:
        raise ValueError(
            f"Selected FRED series count {series_count} exceeds "
            f"max_dense_series={max_dense_series}"
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
        .select(FRED_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=FRED_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["asof_daily"],
    )
    return frame


def fred_feature_id(series_id: object) -> str:
    token = normalize_column_name(str(series_id).strip())
    return f"fred|{token or 'null'}"


def _series_metadata(
    series_config: list[FredSeriesConfig],
    *,
    include_priorities: list[str],
) -> pl.DataFrame:
    priorities = {priority.upper() for priority in include_priorities}
    rows = [
        {
            "series_id": item.series_id.strip().upper(),
        }
        for item in series_config
        if item.priority.upper() in priorities
    ]
    if not rows:
        return pl.DataFrame(schema={"series_id": pl.Utf8})
    return pl.DataFrame(rows)


def _feature_id_expr() -> pl.Expr:
    return pl.col("series_id").map_elements(fred_feature_id, return_dtype=pl.Utf8)


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns([pl.lit(None).alias(column) for column in missing])


def _empty_observation() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in FRED_OBSERVATION_COLUMNS})


def _empty_asof() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in FRED_ASOF_DAILY_COLUMNS})
