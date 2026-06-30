from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class ModelSplitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: date
    end: date | None = None


class ModelDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_start_date: date
    lookback_business_days: int
    feature_warmup_business_days: int
    target_horizons_business_days: list[int]
    split_assignment_date: str
    embargo_business_days: int
    train: ModelSplitConfig
    validation: ModelSplitConfig
    test: ModelSplitConfig

    @field_validator("lookback_business_days")
    @classmethod
    def validate_positive_lookback(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("lookback_business_days must be positive")
        return value

    @field_validator("target_horizons_business_days")
    @classmethod
    def validate_horizons(cls, horizons: list[int]) -> list[int]:
        if any(not isinstance(horizon, int) or horizon <= 0 for horizon in horizons):
            raise ValueError("target_horizons_business_days must be positive integers")
        if len(set(horizons)) != len(horizons):
            raise ValueError("target_horizons_business_days must be unique")
        if horizons != [1, 5, 21]:
            raise ValueError("default target_horizons_business_days must be [1, 5, 21]")
        return horizons

    @model_validator(mode="after")
    def validate_contract(self) -> ModelDatasetConfig:
        if self.embargo_business_days != 0:
            raise ValueError("embargo_business_days must be 0 for this contract")
        if self.split_assignment_date != "asof_date":
            raise ValueError("split_assignment_date must be asof_date")
        if self.feature_warmup_business_days < self.lookback_business_days:
            raise ValueError("feature_warmup_business_days must be >= lookback_business_days")
        if self.train.end is None or self.validation.end is None:
            raise ValueError("train.end and validation.end are required")
        if not (
            self.model_start_date
            <= self.train.start
            <= self.train.end
            < self.validation.start
            <= self.validation.end
            < self.test.start
        ):
            raise ValueError(
                "date order must satisfy model_start_date <= train.start <= train.end "
                "< validation.start <= validation.end < test.start"
            )
        return self


def load_model_dataset_config(repo_root: Path) -> ModelDatasetConfig:
    data = _load_yaml(repo_root, "configs/modeling/dataset.yaml")
    return ModelDatasetConfig.model_validate(data)


def _load_yaml(repo_root: Path, relative_path: str) -> dict[str, Any]:
    path = repo_root / relative_path
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping: {path}")
    return data
