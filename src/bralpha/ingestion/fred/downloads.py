from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    FredDownloadResult,
    client_context,
    download_fred_request,
    fred_dataset,
    fred_manifest_writer,
    fred_paths,
    fred_raw_store,
    load_fred_series_config,
    resolve_fred_api_key,
)


def download_fred_series_observations(
    repo_root: Path,
    *,
    start: date,
    end: date,
    series_ids: list[str] | None = None,
    api_key: str | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[FredDownloadResult]:
    if start > end:
        raise ValueError("start must be on or before end")
    dataset = fred_dataset(repo_root, "fred_series_observations")
    key = resolve_fred_api_key(api_key)
    configured = [row.series_id for row in load_fred_series_config(repo_root)]
    selected = _select_series(configured, series_ids)
    paths = fred_paths(repo_root)
    results: list[FredDownloadResult] = []

    with client_context(client) as owned_client:
        for series_id in selected:
            url, params, filename = build_fred_observations_request(
                dataset,
                series_id=series_id,
                start=start,
                end=end,
                api_key=key,
            )
            results.append(
                download_fred_request(
                    dataset=dataset,
                    raw_store=fred_raw_store(paths),
                    manifest_writer=fred_manifest_writer(paths),
                    url=url,
                    params=params,
                    filename=filename,
                    client=owned_client,
                    downloaded_at=downloaded_at,
                )
            )
    return results


def build_fred_observations_request(
    dataset: DatasetConfig,
    *,
    series_id: str,
    start: date,
    end: date,
    api_key: str,
) -> tuple[str, dict[str, str], str]:
    url, params, _, filename = dataset.first_source_url().render(
        series_id=series_id,
        start=start,
        end=end,
        api_key=api_key,
    )
    if filename is None:
        filename = f"fred_{series_id}_{start:%Y%m%d}_{end:%Y%m%d}.json"
    return url, params, filename


def _select_series(configured: list[str], series_ids: list[str] | None) -> list[str]:
    configured_upper = [series_id.strip().upper() for series_id in configured]
    if series_ids is None:
        return configured_upper
    requested = {series_id.strip().upper() for series_id in series_ids}
    selected = [series_id for series_id in configured_upper if series_id in requested]
    missing = sorted(requested - set(selected))
    if missing:
        raise ValueError(f"Unknown configured FRED series_id(s): {missing}")
    return selected
