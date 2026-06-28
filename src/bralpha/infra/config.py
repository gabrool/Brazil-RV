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


class BCBCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class BCBSGSResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_model_usable_only: bool
    asof_panel: bool


class BCBPTAXResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    currencies: list[str]
    use_selected_bulletin_only: bool

    @field_validator("currencies")
    @classmethod
    def normalize_currencies(cls, currencies: list[str]) -> list[str]:
        return [currency.strip().upper() for currency in currencies]


class BCBFocusResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asof_panel: bool
    include_general_expectations: bool
    include_top5_expectations: bool
    max_dense_keys: int
    availability_note: str
    selected_indicators: list[str]


class BCBDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_sgs: bool
    include_ptax: bool
    include_focus: bool


class BCBResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: BCBCalendarConfig
    sgs: BCBSGSResearchConfig
    ptax: BCBPTAXResearchConfig
    focus: BCBFocusResearchConfig
    daily_long: BCBDailyLongConfig


class BCBResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bcb_research: BCBResearchSection


class IBGECalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class IBGESIDRAResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_model_usable_only: bool
    include_priorities: list[str]
    selected_dataset_slugs: list[str]
    max_dense_features: int


class IBGEReferencesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_release_calendar: bool
    include_products: bool
    include_news_metadata: bool


class IBGEDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_sidra: bool


class IBGEResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: IBGECalendarConfig
    sidra: IBGESIDRAResearchConfig
    references: IBGEReferencesConfig
    daily_long: IBGEDailyLongConfig


class IBGEResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ibge_research: IBGEResearchSection


class TesouroCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class TesouroPricesRatesResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_dense_securities: int


class TesouroFlowsResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_sales: bool
    include_redemptions: bool


class TesouroStockResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_tesouro_direto_stock: bool
    include_dpf_stock: bool
    max_dense_keys: int


class TesouroDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_prices_rates: bool
    include_flows: bool
    include_stock: bool


class TesouroResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: TesouroCalendarConfig
    prices_rates: TesouroPricesRatesResearchConfig
    flows: TesouroFlowsResearchConfig
    stock: TesouroStockResearchConfig
    daily_long: TesouroDailyLongConfig


class TesouroResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tesouro_research: TesouroResearchSection


class FredCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class FredObservationsResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_model_usable_only: bool
    include_priorities: list[str]
    max_dense_series: int


class FredReferencesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_series_reference: bool


class FredDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_observations: bool


class FredResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: FredCalendarConfig
    observations: FredObservationsResearchConfig
    references: FredReferencesConfig
    daily_long: FredDailyLongConfig


class FredResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fred_research: FredResearchSection


class CVMCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class CVMFundReportsResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_per_fund_observation: bool
    include_group_observation: bool
    include_flow_daily: bool
    include_state_asof_daily: bool
    group_by: list[str]
    max_groups: int


class CVMRegistryResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_current_reference: bool


class CVMDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_fund_flows: bool
    include_fund_state: bool


class CVMResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: CVMCalendarConfig
    fund_reports: CVMFundReportsResearchConfig
    registry: CVMRegistryResearchConfig
    daily_long: CVMDailyLongConfig


class CVMResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cvm_research: CVMResearchSection


class ONSCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class ONSHydroResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_ear_subsystem: bool
    include_ena_subsystem: bool


class ONSLoadCMOResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_load_daily: bool
    include_cmo_weekly: bool


class ONSHourlyDailyResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_energy_balance_daily: bool
    include_interchange_daily: bool
    aggregation: str
    min_hour_count: int


class ONSAsofResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_state_asof_daily: bool
    max_features: int


class ONSDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_hydro: bool
    include_load_cmo: bool
    include_energy_balance: bool
    include_interchange: bool


class ONSResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: ONSCalendarConfig
    hydro: ONSHydroResearchConfig
    load_cmo: ONSLoadCMOResearchConfig
    hourly_daily: ONSHourlyDailyResearchConfig
    asof: ONSAsofResearchConfig
    daily_long: ONSDailyLongConfig


class ONSResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ons_research: ONSResearchSection


class ANPCalendarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    timezone: str


class ANPFuelPricesResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_station_observation: bool
    include_group_observation: bool
    group_by: list[str]
    max_groups: int
    aggregation: str


class ANPFuelSalesResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_observation: bool
    include_group_observation: bool
    group_by: list[str]
    max_groups: int


class ANPOilGasResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_observation: bool
    include_group_observation: bool
    group_by: list[str]
    max_groups: int


class ANPAsofResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_state_asof_daily: bool
    max_features: int


class ANPDailyLongConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_fuel_prices: bool
    include_fuel_sales: bool
    include_oil_gas: bool


class ANPResearchSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar: ANPCalendarConfig
    fuel_prices: ANPFuelPricesResearchConfig
    fuel_sales: ANPFuelSalesResearchConfig
    oil_gas: ANPOilGasResearchConfig
    asof: ANPAsofResearchConfig
    daily_long: ANPDailyLongConfig


class ANPResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    anp_research: ANPResearchSection


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


def load_ibge_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/ibge.yaml"))


def load_anbima_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/anbima.yaml"))


def load_tesouro_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/tesouro.yaml"))


def load_fred_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/fred.yaml"))


def load_cvm_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/cvm.yaml"))


def load_ons_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/ons.yaml"))


def load_anp_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(_load_yaml(repo_root, "configs/datasets/anp.yaml"))


def load_novo_caged_dataset_registry(repo_root: Path) -> DatasetRegistry:
    return DatasetRegistry.model_validate(
        _load_yaml(repo_root, "configs/datasets/novo_caged.yaml")
    )


def load_b3_research_config(repo_root: Path) -> B3ResearchConfig:
    return B3ResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/b3.yaml"))


def load_bcb_research_config(repo_root: Path) -> BCBResearchConfig:
    return BCBResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/bcb.yaml"))


def load_ibge_research_config(repo_root: Path) -> IBGEResearchConfig:
    return IBGEResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/ibge.yaml"))


def load_tesouro_research_config(repo_root: Path) -> TesouroResearchConfig:
    return TesouroResearchConfig.model_validate(
        _load_yaml(repo_root, "configs/derived/tesouro.yaml")
    )


def load_fred_research_config(repo_root: Path) -> FredResearchConfig:
    return FredResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/fred.yaml"))


def load_cvm_research_config(repo_root: Path) -> CVMResearchConfig:
    return CVMResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/cvm.yaml"))


def load_ons_research_config(repo_root: Path) -> ONSResearchConfig:
    return ONSResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/ons.yaml"))


def load_anp_research_config(repo_root: Path) -> ANPResearchConfig:
    return ANPResearchConfig.model_validate(_load_yaml(repo_root, "configs/derived/anp.yaml"))


__all__ = [
    "ANPAsofResearchConfig",
    "ANPCalendarConfig",
    "ANPDailyLongConfig",
    "ANPFuelPricesResearchConfig",
    "ANPFuelSalesResearchConfig",
    "ANPOilGasResearchConfig",
    "ANPResearchConfig",
    "ANPResearchSection",
    "BCBCalendarConfig",
    "BCBDailyLongConfig",
    "BCBFocusResearchConfig",
    "BCBPTAXResearchConfig",
    "BCBResearchConfig",
    "BCBResearchSection",
    "BCBSGSResearchConfig",
    "B3ContinuousFuturesConfig",
    "B3DICurveConfig",
    "B3ResearchConfig",
    "B3ResearchRoots",
    "B3ResearchSection",
    "B3TargetsConfig",
    "CVMCalendarConfig",
    "CVMDailyLongConfig",
    "CVMFundReportsResearchConfig",
    "CVMRegistryResearchConfig",
    "CVMResearchConfig",
    "CVMResearchSection",
    "EngineeringConfig",
    "FredCalendarConfig",
    "FredDailyLongConfig",
    "FredObservationsResearchConfig",
    "FredReferencesConfig",
    "FredResearchConfig",
    "FredResearchSection",
    "IBGECalendarConfig",
    "IBGEDailyLongConfig",
    "IBGEReferencesConfig",
    "IBGEResearchConfig",
    "IBGEResearchSection",
    "IBGESIDRAResearchConfig",
    "InstrumentsConfig",
    "PathsConfig",
    "PointInTimeConfig",
    "ProjectConfig",
    "ProjectSection",
    "ONSAsofResearchConfig",
    "ONSCalendarConfig",
    "ONSDailyLongConfig",
    "ONSHourlyDailyResearchConfig",
    "ONSHydroResearchConfig",
    "ONSLoadCMOResearchConfig",
    "ONSResearchConfig",
    "ONSResearchSection",
    "ResolvedPaths",
    "SleeveConfig",
    "TesouroCalendarConfig",
    "TesouroDailyLongConfig",
    "TesouroFlowsResearchConfig",
    "TesouroPricesRatesResearchConfig",
    "TesouroResearchConfig",
    "TesouroResearchSection",
    "TesouroStockResearchConfig",
    "load_b3_dataset_registry",
    "load_anbima_dataset_registry",
    "load_anp_dataset_registry",
    "load_anp_research_config",
    "load_cvm_dataset_registry",
    "load_cvm_research_config",
    "load_novo_caged_dataset_registry",
    "load_ons_dataset_registry",
    "load_tesouro_dataset_registry",
    "load_fred_dataset_registry",
    "load_fred_research_config",
    "load_bcb_dataset_registry",
    "load_bcb_research_config",
    "load_ibge_dataset_registry",
    "load_ibge_research_config",
    "load_instruments_config",
    "load_ons_research_config",
    "load_paths_config",
    "load_project_config",
    "load_b3_research_config",
    "load_tesouro_research_config",
    "resolve_project_paths",
]
