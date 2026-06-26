from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    path_template: str
    format: str | None = None
    manifest_path: str | None = None


class SourceUrlConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    url_template: str | None = None
    endpoint: str | None = None
    params: dict[str, str] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    filename_template: str | None = None

    @model_validator(mode="after")
    def validate_locator(self) -> SourceUrlConfig:
        if self.url_template is None and self.endpoint is None:
            raise ValueError("source URL entry must define url_template or endpoint")
        return self

    def render(self, **values: Any) -> tuple[str, dict[str, str], dict[str, str], str | None]:
        if self.url_template is None:
            raise ValueError("source URL entry has no url_template")
        return (
            _render_template(self.url_template, values),
            {key: _render_template(value, values) for key, value in self.params.items()},
            {key: _render_template(value, values) for key, value in self.headers.items()},
            _render_template(self.filename_template, values) if self.filename_template else None,
        )


class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    dataset_id: str
    source: str | None = None
    priority: str
    frequency: str
    free_access: bool | None = None
    requires_auth: bool | None = None
    point_in_time_required: bool | None = None
    raw_format: str | None = None
    canonical_table: str
    partition_keys: list[str] = Field(default_factory=list)
    primary_keys: list[str] = Field(default_factory=list)
    quality_checks: list[str] = Field(default_factory=list)
    source_urls: list[SourceUrlConfig] = Field(default_factory=list)
    request_defaults: dict[str, Any] = Field(default_factory=dict)
    license_note: str = ""
    notes: str = ""

    @property
    def source_map_status(self) -> str | None:
        return (self.model_extra or {}).get("source_map_status")

    @field_validator("dataset_id", "priority", "frequency", "canonical_table")
    @classmethod
    def required_text(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("field must be a non-empty string")
        return value.strip()

    @field_validator("primary_keys", "quality_checks")
    @classmethod
    def clean_list(cls, value: list[str]) -> list[str]:
        if any(not item or not item.strip() for item in value):
            raise ValueError("field must not contain empty items")
        return value

    @model_validator(mode="after")
    def validate_live_dataset_keys(self) -> DatasetConfig:
        metadata_only_statuses = {"not_implemented_pending_url", "raw_only_or_pending_url"}
        if self.source_map_status not in metadata_only_statuses:
            if not self.primary_keys:
                raise ValueError("implemented datasets must define primary_keys")
            if not self.quality_checks:
                raise ValueError("implemented datasets must define quality_checks")
        return self

    def first_source_url(self) -> SourceUrlConfig:
        if not self.source_urls:
            raise ValueError(f"Dataset has no configured source URL: {self.dataset_id}")
        return self.source_urls[0]


class DatasetRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    free_access_default: bool
    requires_auth_default: bool
    point_in_time_required: bool
    raw_storage: StorageConfig
    bronze_storage: StorageConfig
    silver_storage: StorageConfig
    datasets: list[DatasetConfig]

    @model_validator(mode="after")
    def validate_and_apply_defaults(self) -> DatasetRegistry:
        seen: set[str] = set()
        for dataset in self.datasets:
            if dataset.dataset_id in seen:
                raise ValueError(f"Duplicate B3 dataset_id: {dataset.dataset_id}")
            seen.add(dataset.dataset_id)
            if dataset.source is None:
                dataset.source = self.source
            if dataset.free_access is None:
                dataset.free_access = self.free_access_default
            if dataset.requires_auth is None:
                dataset.requires_auth = self.requires_auth_default
            if dataset.point_in_time_required is None:
                dataset.point_in_time_required = self.point_in_time_required
        return self

    def get(self, dataset_id: str) -> DatasetConfig:
        for dataset in self.datasets:
            if dataset.dataset_id == dataset_id:
                return dataset
        raise KeyError(f"Unknown dataset_id: {dataset_id}")

    def p0_datasets(self) -> list[DatasetConfig]:
        return [dataset for dataset in self.datasets if dataset.priority.upper() == "P0"]


def render_dataset_request(
    dataset: DatasetConfig,
    *,
    ref_date: date | None = None,
    year: int | None = None,
    **values: Any,
) -> tuple[str, dict[str, str], dict[str, str], str]:
    render_values = {"dataset_id": dataset.dataset_id, **values}
    if ref_date is not None:
        render_values["ref_date"] = ref_date
        render_values.setdefault("year", ref_date.year)
    if year is not None:
        render_values["year"] = year
    url, params, headers, filename = dataset.first_source_url().render(**render_values)
    if filename is None:
        suffix = "bin"
        filename = f"{dataset.dataset_id}_{ref_date.isoformat() if ref_date else year}.{suffix}"
    return url, params, headers, filename


def dataset_endpoint_names(dataset: DatasetConfig) -> list[str]:
    return [
        source.endpoint
        for source in dataset.source_urls
        if source.endpoint is not None and source.endpoint.strip()
    ]


def _render_template(template: str, values: dict[str, Any]) -> str:
    try:
        return template.format(**values)
    except KeyError as exc:
        missing = exc.args[0]
        raise ValueError(f"Missing template value {missing!r} for {template!r}") from exc
