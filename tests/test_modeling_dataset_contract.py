from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from pydantic import ValidationError

from bralpha.domain.b3_calendar import business_days_between, next_business_day
from bralpha.modeling.config import ModelDatasetConfig, load_model_dataset_config
from bralpha.modeling.splits import (
    add_split_column,
    assign_split,
    effective_feature_start,
    target_end_date,
)


def test_model_dataset_config_loads_defaults(repo_root):
    config = load_model_dataset_config(repo_root)

    assert config.model_start_date == date(2013, 1, 2)
    assert config.lookback_business_days == 256
    assert config.feature_warmup_business_days == 504
    assert config.target_horizons_business_days == [1, 5, 21]
    assert config.split_assignment_date == "asof_date"
    assert config.embargo_business_days == 0
    assert config.train.start == date(2013, 1, 2)
    assert config.train.end == date(2021, 12, 31)
    assert config.validation.start == date(2022, 1, 3)
    assert config.validation.end == date(2023, 12, 29)
    assert config.test.start == date(2024, 1, 2)
    assert config.test.end is None


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda data: data.update({"embargo_business_days": 1}), "embargo"),
        (lambda data: data.update({"lookback_business_days": 0}), "lookback"),
        (
            lambda data: data.update({"feature_warmup_business_days": 255}),
            "feature_warmup",
        ),
        (
            lambda data: data.update({"target_horizons_business_days": [1, 1, 21]}),
            "unique",
        ),
        (
            lambda data: data.update({"target_horizons_business_days": [1, 5, 20]}),
            r"\[1, 5, 21\]",
        ),
        (
            lambda data: data["validation"].update({"start": "2021-12-31"}),
            "date order",
        ),
    ],
)
def test_model_dataset_config_validation(repo_root, mutation, match):
    data = load_model_dataset_config(repo_root).model_dump(mode="json")
    mutation(data)

    with pytest.raises(ValidationError, match=match):
        ModelDatasetConfig.model_validate(data)


@pytest.mark.parametrize(
    ("asof_date", "expected"),
    [
        (date(2013, 1, 1), None),
        (date(2013, 1, 2), "train"),
        (date(2021, 12, 31), "train"),
        (date(2022, 1, 3), "validation"),
        (date(2023, 12, 29), "validation"),
        (date(2024, 1, 2), "test"),
    ],
)
def test_assign_split_boundary_dates(repo_root, asof_date, expected):
    assert assign_split(asof_date, load_model_dataset_config(repo_root)) == expected


def test_no_embargo_between_configured_business_boundaries(repo_root):
    config = load_model_dataset_config(repo_root)

    assert next_business_day(config.train.end) == config.validation.start
    assert next_business_day(config.validation.end, holidays={date(2024, 1, 1)}) == (
        config.test.start
    )
    assert config.embargo_business_days == 0


def test_split_assignment_ignores_target_horizon_and_target_end_date(repo_root):
    config = load_model_dataset_config(repo_root)
    asof_date = date(2021, 12, 31)

    assert assign_split(asof_date, config) == "train"
    assert target_end_date(asof_date, 21) > config.validation.start
    assert assign_split(asof_date, config) == "train"

    frame = pl.DataFrame({"ref_date": [asof_date, date(2022, 1, 3), date(2024, 1, 2)]})
    assert add_split_column(frame, config)["split"].to_list() == [
        "train",
        "validation",
        "test",
    ]


def test_target_end_date_computes_business_day_horizons():
    asof_date = date(2024, 1, 2)

    assert target_end_date(asof_date, 1) == date(2024, 1, 3)
    assert target_end_date(asof_date, 5) == date(2024, 1, 9)
    assert target_end_date(asof_date, 21) == date(2024, 1, 31)
    assert target_end_date(date(2023, 12, 29), 1, holidays={date(2024, 1, 1)}) == (
        date(2024, 1, 2)
    )
    with pytest.raises(ValueError, match="positive"):
        target_end_date(asof_date, 0)


def test_effective_feature_start_uses_at_least_504_business_days(repo_root):
    config = load_model_dataset_config(repo_root)
    start = effective_feature_start(config)

    assert start < config.model_start_date
    assert business_days_between(start, config.model_start_date) >= 504
