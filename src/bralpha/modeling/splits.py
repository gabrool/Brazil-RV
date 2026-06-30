from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.domain.b3_calendar import (
    add_business_days,
    previous_business_day,
)
from bralpha.modeling.config import ModelDatasetConfig

TRAIN_SPLIT = "train"
VALIDATION_SPLIT = "validation"
TEST_SPLIT = "test"


def assign_split(asof_date: date, config: ModelDatasetConfig) -> str | None:
    """Return train, validation, test, or None using only the sample as-of date."""
    if config.train.start <= asof_date <= config.train.end:
        return TRAIN_SPLIT
    if config.validation.start <= asof_date <= config.validation.end:
        return VALIDATION_SPLIT
    test_end = config.test.end
    if asof_date >= config.test.start and (test_end is None or asof_date <= test_end):
        return TEST_SPLIT
    return None


def add_split_column(
    frame: pl.DataFrame,
    config: ModelDatasetConfig,
    *,
    date_col: str = "ref_date",
) -> pl.DataFrame:
    """Add split column using asof/ref date only."""
    return frame.with_columns(
        pl.col(date_col)
        .map_elements(
            lambda value: assign_split(_date_value(value), config),
            return_dtype=pl.Utf8,
        )
        .alias("split")
    )


def effective_feature_start(config: ModelDatasetConfig) -> date:
    """Return earliest date needed to build features, including warmup."""
    candidate = config.model_start_date
    for _ in range(config.feature_warmup_business_days):
        candidate = previous_business_day(candidate)
    return candidate


def target_end_date(
    asof_date: date,
    horizon_business_days: int,
    holidays: set[date] | None = None,
) -> date:
    if horizon_business_days <= 0:
        raise ValueError("horizon_business_days must be positive")
    return add_business_days(asof_date, horizon_business_days, holidays)


def _date_value(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
