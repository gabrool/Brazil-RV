from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    CVMDownloadResult,
    client_context,
    cvm_dataset,
    cvm_manifest_writer,
    cvm_paths,
    cvm_raw_store,
    download_cvm_request,
)
from .periods import CVMResourceRequest, fund_daily_report_resources


class CVMDatasetNotLiveError(RuntimeError):
    pass


def download_cvm_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[CVMDownloadResult]:
    dataset = cvm_dataset(repo_root, dataset_id)
    _require_live_dataset(dataset)
    if dataset.dataset_id == "cvm_fund_daily_reports":
        if start is None or end is None:
            raise ValueError("cvm_fund_daily_reports requires start and end")
        requests = [
            _daily_resource_request(dataset, resource, start=start, end=end)
            for resource in fund_daily_report_resources(start, end)
        ]
    else:
        if start is not None or end is not None:
            raise ValueError(f"{dataset.dataset_id} does not accept start/end")
        requests = [_registry_resource_request(dataset)]

    paths = cvm_paths(repo_root)
    results: list[CVMDownloadResult] = []
    with client_context(client) as owned_client:
        for request in requests:
            results.append(
                download_cvm_request(
                    dataset=dataset,
                    raw_store=cvm_raw_store(paths),
                    manifest_writer=cvm_manifest_writer(paths),
                    url=request["url"],
                    filename=request["filename"],
                    client=owned_client,
                    params=request.get("params"),
                    headers=request.get("headers"),
                    manifest_params=request["manifest_params"],
                    downloaded_at=downloaded_at,
                )
            )
    return results


def _require_live_dataset(dataset: DatasetConfig) -> None:
    live_statuses = {"live_download", "raw_bronze_only_pending_normalizer"}
    if dataset.source_map_status not in live_statuses or not dataset.source_urls:
        raise CVMDatasetNotLiveError(
            f"CVM dataset {dataset.dataset_id!r} is not live in this PR "
            f"(status={dataset.source_map_status!r})"
        )


def _daily_resource_request(
    dataset: DatasetConfig,
    resource: CVMResourceRequest,
    *,
    start: date,
    end: date,
) -> dict[str, object]:
    return {
        "url": resource.url,
        "filename": resource.filename,
        "params": {},
        "headers": {},
        "manifest_params": {
            "resource_name": resource.resource_name,
            "period_year": resource.period_year,
            "period_month": resource.period_month,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "raw_format": dataset.raw_format,
        },
    }


def _registry_resource_request(dataset: DatasetConfig) -> dict[str, object]:
    source_url = dataset.first_source_url()
    url, params, headers, filename = source_url.render()
    if filename is None:
        suffix = "zip" if dataset.raw_format == "zip_csv" else "csv"
        filename = f"{dataset.dataset_id}.{suffix}"
    return {
        "url": url,
        "filename": filename,
        "params": params,
        "headers": headers,
        "manifest_params": {
            **params,
            "resource_name": source_url.name or dataset.dataset_id,
            "raw_format": dataset.raw_format,
        },
    }
