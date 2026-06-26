from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from bralpha.infra.paths import ResolvedPaths, resolve_project_paths
from bralpha.metadata.datasets import DatasetRegistry


class ProjectSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    package: str
    timezone: str
    frequency: str


class EngineeringConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hardware_constrained: bool
    lean_code: bool
    table_format: str


class PointInTimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required: bool
    availability_field: str


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: ProjectSection
    engineering: EngineeringConfig
    point_in_time: PointInTimeConfig


class PathsSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_root: Path
    raw: Path
    bronze: Path
    silver: Path
    gold: Path
    manifests: Path
    external: Path
    reports: Path


class PathsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    paths: PathsSection


class SleeveConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    description: str | None = None
    primary_roots: list[str] = Field(default_factory=list)
    secondary_roots: list[str] = Field(default_factory=list)

    @field_validator("primary_roots", "secondary_roots")
    @classmethod
    def normalize_roots(cls, roots: list[str]) -> list[str]:
        return [root.strip().upper() for root in roots]


class InstrumentsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    traded_sleeves: dict[str, SleeveConfig]
    research_context_roots: dict[str, Any] = Field(default_factory=dict)
    horizons: list[int]


class B3ResearchRoots(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary: list[str]
    secondary: list[str] = Field(default_factory=list)

    @field_validator("primary", "secondary")
    @classmethod
    def normalize_roots(cls, roots: list[str]) -> list[str]:
        return [root.strip().upper() for root in roots]


class B3ContinuousFuturesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    roll_policy: str
    max_front_rank: int
    min_days_to_maturity: int
    prefer_liquidity_when_available: bool


class B3DICurveConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_roots: list[str]
    tenor_days: list[int]
    interpolation: str

    @field_validator("source_roots")
    @classmethod
    def normalize_roots(cls, roots: list[str]) -> list[str]:
        return [root.strip().upper() for root in roots]


class B3TargetsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    horizons: list[int]
    target_types: list[str]


class B3ResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    roots: B3ResearchRoots
    continuous_futures: B3ContinuousFuturesConfig
    di_curve: B3DICurveConfig
    targets: B3TargetsConfig


class B3ResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    b3_research: B3ResearchSection


def _load_yaml(repo_root: Path, relative_path: str) -> dict[str, Any]:
    path = repo_root / relative_path
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {path}")
    return data


def load_project_config(repo_root: Path) -> ProjectConfig:
    return ProjectConfig.model_validate(_load_yaml(repo_root, "configs/project.yaml"))


def load_paths_config(repo_root: Path) -> PathsConfig:
    return PathsConfig.model_validate(_load_yaml(repo_root, "configs/paths.yaml"))


def load_instruments_config(repo_root: Path) -> InstrumentsConfig:
    return InstrumentsConfig.model_validate(_load_yaml(repo_root, "configs/instruments.yaml"))


def load_b3_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/b3.yaml"))


def load_bcb_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/bcb.yaml"))


def load_b3_research_config(repo_root: Path) -> B3ResearchConfig:
    return B3ResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/b3.yaml"))


__all__ = [
    "B3ContinuousFuturesConfig",
    "B3DICurveConfig",
    "B3ResearchConfig",
    "B3ResearchRoots",
    "B3ResearchSection",
    "B3TargetsConfig",
    "EngineeringConfig",
    "InstrumentsConfig",
    "PathsConfig",
    "PointInTimeConfig",
    "ProjectConfig",
    "ProjectSection",
    "ResolvedPaths",
    "SleeveConfig",
    "load_b3_dataset_registry",
    "load_bcb_dataset_registry",
    "load_instruments_config",
    "load_paths_config",
    "load_project_config",
    "load_b3_research_config",
    "resolve_project_paths",
]
