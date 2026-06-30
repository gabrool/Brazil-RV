from __future__ import annotations

import re
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class Stationarity(StrEnum):
    STATIONARY = "stationary"
    STATIONARYISH = "stationaryish"
    NONSTATIONARY_LEVEL = "nonstationary_level"
    BOUNDED = "bounded"
    BINARY = "binary"
    COUNT = "count"
    FLOW = "flow"
    DIAGNOSTIC = "diagnostic"


class PreprocessTransform(StrEnum):
    IDENTITY = "identity"
    PERCENT_TO_BP = "percent_to_bp"
    LOG_POSITIVE = "log_positive"
    LOG1P_POSITIVE = "log1p_positive"
    SIGNED_LOG1P = "signed_log1p"
    ALREADY_LOG = "already_log"
    ALREADY_RETURN = "already_return"
    BINARY = "binary"
    COUNT_LOG1P = "count_log1p"
    CLIP_ONLY = "clip_only"
    NONE = "none"


class Winsorization(StrEnum):
    NONE = "none"
    TRAIN_QUANTILE = "train_quantile"
    TRAIN_MAD = "train_mad"
    HARD_CLIP_THEN_TRAIN_QUANTILE = "hard_clip_then_train_quantile"


class Scaler(StrEnum):
    NONE = "none"
    TRAIN_ROBUST_ZSCORE = "train_robust_zscore"
    TRAIN_STANDARD_ZSCORE = "train_standard_zscore"


class FitScope(StrEnum):
    TRAIN_ONLY = "train_only"


class MissingPolicy(StrEnum):
    PRESERVE_NULL_ADD_MASK = "preserve_null_add_mask"
    PRESERVE_NULL = "preserve_null"
    ZERO_FILL_WITH_MASK = "zero_fill_with_mask"
    FALSE_FILL = "false_fill"
    FORWARD_FILL_WITH_STALENESS = "forward_fill_with_staleness"


class FeatureRuleSelector(BaseModel):
    model_config = ConfigDict(extra="forbid")

    panel: str | None = None
    source_family: str | None = None
    feature_id_regex: str | None = None
    value_name: str | None = None
    value_name_regex: str | None = None
    unit: str | None = None
    unit_regex: str | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> FeatureRuleSelector:
        if not any(
            [
                self.panel,
                self.source_family,
                self.feature_id_regex,
                self.value_name,
                self.value_name_regex,
                self.unit,
                self.unit_regex,
            ]
        ):
            raise ValueError("selector must define at least one field")
        if self.value_name and self.value_name_regex:
            raise ValueError("selector cannot define both value_name and value_name_regex")
        if self.unit and self.unit_regex:
            raise ValueError("selector cannot define both unit and unit_regex")
        return self

    @field_validator("feature_id_regex", "value_name_regex", "unit_regex")
    @classmethod
    def validate_regex(cls, value: str | None) -> str | None:
        if value is None:
            return value
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError(f"invalid regex: {value}") from exc
        return value


class FeaturePreprocessingSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transform: PreprocessTransform
    winsorization: Winsorization
    winsor_lower: float | None
    winsor_upper: float | None
    hard_min: float | None
    hard_max: float | None
    scaler: Scaler
    fit_scope: FitScope
    missing_policy: MissingPolicy
    positive_required: bool
    allow_negative: bool
    notes: str


class FeaturePreprocessingRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule_id: str
    priority: int
    selector: FeatureRuleSelector
    model_default: bool
    model_default_reason: str
    semantic_type: str
    raw_unit: str | None
    stationarity: Stationarity
    preprocessing: FeaturePreprocessingSpec

    @model_validator(mode="after")
    def validate_rule(self) -> FeaturePreprocessingRule:
        if not self.rule_id.strip():
            raise ValueError("rule_id must be non-empty")
        if self.model_default is False and not self.model_default_reason.strip():
            raise ValueError("model_default false rules require model_default_reason")
        if not self.semantic_type.strip():
            raise ValueError("semantic_type must be non-empty")
        if not self.preprocessing.notes.strip():
            raise ValueError("preprocessing notes must be non-empty")
        return self


class FeaturePreprocessingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: list[FeaturePreprocessingRule]

    @model_validator(mode="after")
    def validate_config(self) -> FeaturePreprocessingConfig:
        if not self.rules:
            raise ValueError("feature preprocessing config requires at least one rule")
        rule_ids = [rule.rule_id for rule in self.rules]
        if len(set(rule_ids)) != len(rule_ids):
            raise ValueError("rule_id values must be unique")
        priorities = [rule.priority for rule in self.rules]
        if len(set(priorities)) != len(priorities):
            raise ValueError("priority values must be unique")
        return self


METADATA_COLUMNS = [
    "preprocessing_rule_id",
    "model_default",
    "model_default_reason",
    "semantic_type",
    "raw_unit",
    "stationarity",
    "preprocess_transform",
    "preprocess_winsorization",
    "preprocess_winsor_lower",
    "preprocess_winsor_upper",
    "preprocess_hard_min",
    "preprocess_hard_max",
    "preprocess_scaler",
    "preprocess_fit_scope",
    "preprocess_missing_policy",
    "preprocess_positive_required",
    "preprocess_allow_negative",
    "preprocess_notes",
]


def load_feature_preprocessing_config(repo_root: Path) -> FeaturePreprocessingConfig:
    data = _load_yaml(repo_root, "configs/modeling/feature_preprocessing.yaml")
    return FeaturePreprocessingConfig.model_validate(data)


def match_feature_rule(
    row: Mapping[str, object],
    config: FeaturePreprocessingConfig,
) -> FeaturePreprocessingRule:
    matches = [rule for rule in config.rules if _selector_matches(rule.selector, row)]
    if len(matches) == 1:
        return matches[0]
    descriptor = _row_descriptor(row)
    if not matches:
        raise ValueError(f"No feature preprocessing rule matches {descriptor}")
    rule_ids = ", ".join(rule.rule_id for rule in matches)
    raise ValueError(f"Multiple feature preprocessing rules match {descriptor}: {rule_ids}")


def annotate_feature_frame(
    frame: pl.DataFrame,
    config: FeaturePreprocessingConfig,
) -> pl.DataFrame:
    rows = frame.to_dicts()
    if not rows:
        return frame.with_columns(_empty_metadata_expressions())

    metadata = [_metadata_row(match_feature_rule(row, config)) for row in rows]
    return pl.concat([frame, pl.DataFrame(metadata)], how="horizontal")


def _selector_matches(selector: FeatureRuleSelector, row: Mapping[str, object]) -> bool:
    exact_fields = {
        "panel": selector.panel,
        "source_family": selector.source_family,
        "value_name": selector.value_name,
        "unit": selector.unit,
    }
    for field, expected in exact_fields.items():
        if expected is not None and _row_text(row, field) != expected:
            return False

    regex_fields = {
        "feature_id": selector.feature_id_regex,
        "value_name": selector.value_name_regex,
        "unit": selector.unit_regex,
    }
    for field, pattern in regex_fields.items():
        if pattern is not None and re.search(pattern, _row_text(row, field)) is None:
            return False
    return True


def _row_text(row: Mapping[str, object], key: str) -> str:
    value = row.get(key)
    if value is None:
        return ""
    return str(value)


def _metadata_row(rule: FeaturePreprocessingRule) -> dict[str, object]:
    preprocessing = rule.preprocessing
    return {
        "preprocessing_rule_id": rule.rule_id,
        "model_default": rule.model_default,
        "model_default_reason": rule.model_default_reason,
        "semantic_type": rule.semantic_type,
        "raw_unit": rule.raw_unit,
        "stationarity": rule.stationarity.value,
        "preprocess_transform": preprocessing.transform.value,
        "preprocess_winsorization": preprocessing.winsorization.value,
        "preprocess_winsor_lower": preprocessing.winsor_lower,
        "preprocess_winsor_upper": preprocessing.winsor_upper,
        "preprocess_hard_min": preprocessing.hard_min,
        "preprocess_hard_max": preprocessing.hard_max,
        "preprocess_scaler": preprocessing.scaler.value,
        "preprocess_fit_scope": preprocessing.fit_scope.value,
        "preprocess_missing_policy": preprocessing.missing_policy.value,
        "preprocess_positive_required": preprocessing.positive_required,
        "preprocess_allow_negative": preprocessing.allow_negative,
        "preprocess_notes": preprocessing.notes,
    }


def _empty_metadata_expressions() -> list[pl.Expr]:
    return [
        pl.lit(None, dtype=pl.Utf8).alias("preprocessing_rule_id"),
        pl.lit(None, dtype=pl.Boolean).alias("model_default"),
        pl.lit(None, dtype=pl.Utf8).alias("model_default_reason"),
        pl.lit(None, dtype=pl.Utf8).alias("semantic_type"),
        pl.lit(None, dtype=pl.Utf8).alias("raw_unit"),
        pl.lit(None, dtype=pl.Utf8).alias("stationarity"),
        pl.lit(None, dtype=pl.Utf8).alias("preprocess_transform"),
        pl.lit(None, dtype=pl.Utf8).alias("preprocess_winsorization"),
        pl.lit(None, dtype=pl.Float64).alias("preprocess_winsor_lower"),
        pl.lit(None, dtype=pl.Float64).alias("preprocess_winsor_upper"),
        pl.lit(None, dtype=pl.Float64).alias("preprocess_hard_min"),
        pl.lit(None, dtype=pl.Float64).alias("preprocess_hard_max"),
        pl.lit(None, dtype=pl.Utf8).alias("preprocess_scaler"),
        pl.lit(None, dtype=pl.Utf8).alias("preprocess_fit_scope"),
        pl.lit(None, dtype=pl.Utf8).alias("preprocess_missing_policy"),
        pl.lit(None, dtype=pl.Boolean).alias("preprocess_positive_required"),
        pl.lit(None, dtype=pl.Boolean).alias("preprocess_allow_negative"),
        pl.lit(None, dtype=pl.Utf8).alias("preprocess_notes"),
    ]


def _row_descriptor(row: Mapping[str, object]) -> str:
    fields = ["panel", "source_family", "feature_id", "value_name", "unit"]
    values = ", ".join(f"{field}={row.get(field)!r}" for field in fields if field in row)
    return f"feature row ({values})"


def _load_yaml(repo_root: Path, relative_path: str) -> dict[str, Any]:
    path = repo_root / relative_path
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping: {path}")
    return data
