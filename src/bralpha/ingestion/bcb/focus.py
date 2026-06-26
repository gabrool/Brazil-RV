from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig, dataset_endpoint_names

from .common import (
    FOCUS_ODATA_BASE,
    BcbDownloadResult,
    bcb_dataset,
    bcb_manifest_writer,
    bcb_paths,
    bcb_raw_store,
    client_context,
    download_bcb_request,
    odata_value_count,
)

FOCUS_DATASETS = {
    "bcb_focus_expectations",
    "bcb_focus_top5_expectations",
    "bcb_focus_top5_reference_dates",
}


def download_focus_dataset(
    repo_root: Path,
    *,
    dataset_id: str,
    start: date,
    end: date,
    endpoints: list[str] | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[BcbDownloadResult]:
    if dataset_id not in FOCUS_DATASETS:
        raise ValueError(f"Unsupported Focus dataset: {dataset_id}")
    dataset = bcb_dataset(repo_root, dataset_id)
    paths = bcb_paths(repo_root)
    selected = endpoints or dataset_endpoint_names(dataset)
    page_size = int(dataset.request_defaults.get("page_size", 10000))
    results: list[BcbDownloadResult] = []
    with client_context(client) as owned_client:
        for endpoint in selected:
            results.extend(
                _download_odata_pages(
                    dataset=dataset,
                    endpoint=endpoint,
                    start=start,
                    end=end,
                    client=owned_client,
                    downloaded_at=downloaded_at,
                    page_size=page_size,
                    raw_store=bcb_raw_store(paths),
                    manifest_writer=bcb_manifest_writer(paths),
                )
            )
    return results


def build_focus_request(
    *,
    endpoint: str,
    start: date,
    end: date,
    skip: int = 0,
    top: int = 10000,
) -> tuple[str, dict[str, Any], str]:
    params: dict[str, Any] = {"$format": "json", "$top": str(top), "$skip": str(skip)}
    if endpoint != "DatasReferencia":
        params["$filter"] = f"Data ge '{start.isoformat()}' and Data le '{end.isoformat()}'"
        params["$orderby"] = "Data asc"
        window = f"{start:%Y%m%d}_{end:%Y%m%d}"
    else:
        window = "all"
    url = f"{FOCUS_ODATA_BASE}/{endpoint}"
    filename = f"bcb_focus_{endpoint}_{window}_skip{skip}.json"
    return url, params, filename


def _download_odata_pages(
    *,
    dataset: DatasetConfig,
    endpoint: str,
    start: date,
    end: date,
    client: HttpClient,
    downloaded_at: datetime | None,
    page_size: int,
    raw_store,
    manifest_writer,
) -> list[BcbDownloadResult]:
    results = []
    skip = 0
    while True:
        url, params, filename = build_focus_request(
            endpoint=endpoint,
            start=start,
            end=end,
            skip=skip,
            top=page_size,
        )
        result = download_bcb_request(
            dataset=dataset,
            raw_store=raw_store,
            manifest_writer=manifest_writer,
            url=url,
            params=params,
            filename=filename,
            client=client,
            downloaded_at=downloaded_at,
            manifest_params={
                "endpoint": endpoint,
                "start": start.isoformat(),
                "end": end.isoformat(),
                **params,
            },
        )
        results.append(result)
        if result.raw_path is None:
            break
        if odata_value_count(result.raw_path.read_bytes()) < page_size:
            break
        skip += page_size
    return results
