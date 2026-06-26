from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import yaml

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    IbgeDownloadResult,
    client_context,
    download_ibge_request,
    ibge_dataset,
    ibge_manifest_writer,
    ibge_paths,
    ibge_raw_store,
)


@dataclass(frozen=True)
class SidraSeriesConfig:
    dataset_slug: str
    priority: str
    aggregate_id: int
    table_name: str
    survey_code: str
    frequency: str
    period_selector: str | list[str]
    variables: str
    locations: str
    classifications: str
    view: str
    model_usable: bool
    release_calendar_product_id: int | None
    availability_policy: str


def load_sidra_series_config(repo_root: Path) -> list[SidraSeriesConfig]:
    path = repo_root / "configs" / "series" / "ibge_sidra.yaml"
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    rows = data.get("series", []) if isinstance(data, dict) else []
    return [SidraSeriesConfig(**row) for row in rows]


def download_sidra_series(
    repo_root: Path,
    *,
    start: date,
    end: date,
    priority: list[str] | None = None,
    dataset_slugs: list[str] | None = None,
    aggregate_ids: list[int] | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[IbgeDownloadResult]:
    dataset = ibge_dataset(repo_root, "ibge_sidra_series")
    paths = ibge_paths(repo_root)
    selected = select_sidra_series(
        load_sidra_series_config(repo_root),
        priority=priority,
        dataset_slugs=dataset_slugs,
        aggregate_ids=aggregate_ids,
    )
    results: list[IbgeDownloadResult] = []
    with client_context(client) as owned_client:
        for series in selected:
            periods = resolve_sidra_periods(series, start=start, end=end)
            url, params, filename = build_sidra_request(dataset, series=series, periods=periods)
            results.append(
                download_ibge_request(
                    dataset=dataset,
                    raw_store=ibge_raw_store(paths),
                    manifest_writer=ibge_manifest_writer(paths),
                    url=url,
                    params=params,
                    filename=filename,
                    client=owned_client,
                    downloaded_at=downloaded_at,
                    manifest_params={
                        "dataset_slug": series.dataset_slug,
                        "aggregate_id": series.aggregate_id,
                        "periods": periods,
                        **params,
                    },
                )
            )
    return results


def select_sidra_series(
    series: list[SidraSeriesConfig],
    *,
    priority: list[str] | None = None,
    dataset_slugs: list[str] | None = None,
    aggregate_ids: list[int] | None = None,
) -> list[SidraSeriesConfig]:
    requested_slugs = set(dataset_slugs or [])
    requested_aggregates = set(aggregate_ids or [])
    if requested_slugs or requested_aggregates:
        selected = [
            item
            for item in series
            if item.dataset_slug in requested_slugs or item.aggregate_id in requested_aggregates
        ]
    else:
        priorities = {item.upper() for item in (priority or ["P0"])}
        selected = [item for item in series if item.priority.upper() in priorities]

    missing_slugs = sorted(requested_slugs - {item.dataset_slug for item in selected})
    missing_aggregates = sorted(requested_aggregates - {item.aggregate_id for item in selected})
    if missing_slugs or missing_aggregates:
        raise ValueError(
            f"Unknown configured SIDRA filters: slugs={missing_slugs}, "
            f"aggregate_ids={missing_aggregates}"
        )
    return selected


def build_sidra_request(
    dataset: DatasetConfig,
    *,
    series: SidraSeriesConfig,
    periods: str,
) -> tuple[str, dict[str, str], str]:
    url, params, _, filename = dataset.first_source_url().render(
        dataset_slug=series.dataset_slug,
        aggregate_id=series.aggregate_id,
        periods=periods,
        variables=series.variables,
        locations=series.locations,
        classifications=series.classifications,
        view=series.view,
        periods_hash=_hash_text(periods),
    )
    if filename is None:
        periods_hash = _hash_text(periods)
        filename = f"ibge_sidra_{series.dataset_slug}_{series.aggregate_id}_{periods_hash}.json"
    return url, params, filename


def resolve_sidra_periods(series: SidraSeriesConfig, *, start: date, end: date) -> str:
    selector = series.period_selector
    if isinstance(selector, list):
        return "|".join(str(item) for item in selector)
    text = str(selector).strip()
    if text in {"all"} or text.startswith("-"):
        return text
    if text == "date_range":
        return "|".join(_periods_for_range(start, end, frequency=series.frequency))
    return text


def _periods_for_range(start: date, end: date, *, frequency: str) -> list[str]:
    if start > end:
        raise ValueError("start must be on or before end")
    if frequency == "quarterly":
        return _quarterly_periods(start, end)
    return _monthly_periods(start, end)


def _monthly_periods(start: date, end: date) -> list[str]:
    periods = []
    current = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while current <= last:
        periods.append(f"{current.year}{current.month:02d}")
        current = _add_months(current, 1)
    return periods


def _quarterly_periods(start: date, end: date) -> list[str]:
    periods = []
    current_year = start.year
    current_quarter = (start.month - 1) // 3 + 1
    end_key = (end.year, (end.month - 1) // 3 + 1)
    while (current_year, current_quarter) <= end_key:
        periods.append(f"{current_year}{current_quarter:02d}")
        current_quarter += 1
        if current_quarter == 5:
            current_year += 1
            current_quarter = 1
    return periods


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    return date(value.year + month_index // 12, month_index % 12 + 1, 1)


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
