from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    ReceitaDownloadResult,
    client_context,
    download_receita_request,
    failure_manifest_record,
    receita_dataset,
    receita_manifest_writer,
    receita_paths,
    receita_raw_store,
)
from .resources import ReceitaResourceRequest, receita_collection_resources


class ReceitaDatasetNotLiveError(RuntimeError):
    pass


def download_receita_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[ReceitaDownloadResult]:
    dataset = receita_dataset(repo_root, dataset_id)
    _require_live_dataset(dataset)
    if dataset.dataset_id != "receita_tax_collection_monthly":
        raise ReceitaDatasetNotLiveError(
            f"Receita dataset {dataset.dataset_id!r} has no live downloader in this PR"
        )

    paths = receita_paths(repo_root)
    manifest_writer = receita_manifest_writer(paths)
    timestamp = downloaded_at
    results: list[ReceitaDownloadResult] = []
    with client_context(client) as active_client:
        page_response = _fetch_source_page(dataset, active_client)
        if not 200 <= page_response.status_code < 300:
            record = failure_manifest_record(
                dataset=dataset,
                url=page_response.url,
                request_params={
                    "resource_name": "source_page",
                    "requested_start": start.isoformat() if start else None,
                    "requested_end": end.isoformat() if end else None,
                },
                timestamp=timestamp or datetime.now(UTC),
                response=page_response,
                error_message=f"HTTP {page_response.status_code}",
            )
            manifest_writer.append(record)
            return [ReceitaDownloadResult(record=record, raw_path=None)]
        page_html = page_response.content.decode("utf-8", errors="replace")
        requests = receita_collection_resources(dataset, page_html)
        for request in requests:
            results.append(
                download_receita_request(
                    dataset=dataset,
                    raw_store=receita_raw_store(paths),
                    manifest_writer=manifest_writer,
                    url=request.url,
                    filename=request.filename,
                    client=active_client,
                    manifest_params=_manifest_params(request, start=start, end=end),
                    downloaded_at=downloaded_at,
                )
            )
    return results


def _require_live_dataset(dataset: DatasetConfig) -> None:
    if dataset.source_map_status != "live_download":
        raise ReceitaDatasetNotLiveError(
            f"Receita dataset {dataset.dataset_id!r} is not live in this PR "
            f"(status={dataset.source_map_status!r})"
        )


def _fetch_source_page(dataset: DatasetConfig, client: HttpClient):
    page_url = dataset.model_extra.get("source_page_url")
    if not page_url:
        raise ValueError(f"Dataset has no source_page_url: {dataset.dataset_id}")
    return client.get_bytes(str(page_url))


def _manifest_params(
    request: ReceitaResourceRequest,
    *,
    start: date | None,
    end: date | None,
) -> dict[str, object]:
    params: dict[str, object] = {
        "resource_name": request.resource_name,
        "resource_family": request.resource_family,
    }
    if request.ref_year is not None:
        params["ref_year"] = request.ref_year
    if request.ref_month is not None:
        params["ref_month"] = request.ref_month
    if start is not None:
        params["requested_start"] = start.isoformat()
    if end is not None:
        params["requested_end"] = end.isoformat()
    return params
