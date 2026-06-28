from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.ingestion.ons.common import (
    ONSDownloadResult,
    client_context,
    download_ons_request,
    ons_dataset,
    ons_manifest_writer,
    ons_paths,
    ons_raw_store,
)
from bralpha.ingestion.ons.resources import ons_annual_resources


class ONSDatasetNotLiveError(RuntimeError):
    pass


def download_ons_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[ONSDownloadResult]:
    dataset = ons_dataset(repo_root, dataset_id)
    extra = dataset.model_extra or {}
    if dataset.source_map_status != "live_download" or not extra.get("direct_url_template"):
        raise ONSDatasetNotLiveError(
            f"ONS dataset {dataset_id} is source-map-only or not configured for live download"
        )
    if start is None or end is None:
        raise ValueError(f"ONS live annual dataset {dataset_id} requires start and end")

    paths = ons_paths(repo_root)
    raw_store = ons_raw_store(paths)
    manifest_writer = ons_manifest_writer(paths)
    resources = ons_annual_resources(dataset, start=start, end=end)
    results: list[ONSDownloadResult] = []
    with client_context(client) as active_client:
        for resource in resources:
            results.append(
                download_ons_request(
                    dataset=dataset,
                    raw_store=raw_store,
                    manifest_writer=manifest_writer,
                    url=resource.url,
                    filename=resource.filename,
                    client=active_client,
                    manifest_params={
                        "resource_name": resource.resource_name,
                        "year": resource.year,
                        "requested_start": start.isoformat(),
                        "requested_end": end.isoformat(),
                        "raw_format": dataset.raw_format or "csv_annual",
                    },
                    downloaded_at=downloaded_at,
                )
            )
    return results
