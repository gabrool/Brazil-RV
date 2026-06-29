from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

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
from .resources import (
    ReceitaResourceRequest,
    receita_collection_resources_from_html,
    receita_collection_resources_from_metadata,
)


class ReceitaDatasetNotLiveError(RuntimeError):
    pass


@dataclass(frozen=True)
class _DiscoveredReceitaResource:
    request: ReceitaResourceRequest
    discovery_mode: str
    discovery_url: str


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
    timestamp = downloaded_at or datetime.now(UTC)
    results: list[ReceitaDownloadResult] = []
    with client_context(client) as active_client:
        try:
            discovered = _discover_resources(
                dataset,
                client=active_client,
                start=start,
                end=end,
            )
        except Exception as exc:
            record = failure_manifest_record(
                dataset=dataset,
                url=_discovery_failure_url(dataset),
                request_params=_discovery_failure_params(dataset, start=start, end=end),
                timestamp=timestamp,
                response=None,
                error_message=str(exc),
            )
            manifest_writer.append(record)
            return [ReceitaDownloadResult(record=record, raw_path=None)]

        for discovered_resource in discovered:
            request = discovered_resource.request
            results.append(
                download_receita_request(
                    dataset=dataset,
                    raw_store=receita_raw_store(paths),
                    manifest_writer=manifest_writer,
                    url=request.url,
                    filename=request.filename,
                    client=active_client,
                    manifest_params=_manifest_params(
                        request,
                        start=start,
                        end=end,
                        discovery_mode=discovered_resource.discovery_mode,
                        discovery_url=discovered_resource.discovery_url,
                    ),
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


def _discover_resources(
    dataset: DatasetConfig,
    *,
    client: HttpClient,
    start: date | None,
    end: date | None,
) -> list[_DiscoveredReceitaResource]:
    errors: list[str] = []
    metadata_url = _metadata_api_url(dataset)
    if metadata_url:
        metadata_response = client.get_bytes(metadata_url)
        if 200 <= metadata_response.status_code < 300:
            try:
                requests = receita_collection_resources_from_metadata(
                    dataset,
                    metadata_response.content,
                )
                return [
                    _DiscoveredReceitaResource(
                        request=request,
                        discovery_mode="metadata_api",
                        discovery_url=metadata_response.url,
                    )
                    for request in requests
                ]
            except Exception as exc:
                errors.append(f"metadata_api {metadata_response.url}: {exc}")
        else:
            errors.append(
                f"metadata_api {metadata_response.url}: HTTP {metadata_response.status_code}"
            )

    page_response = _fetch_source_page(dataset, client)
    if 200 <= page_response.status_code < 300:
        try:
            page_html = page_response.content.decode("utf-8", errors="replace")
            requests = receita_collection_resources_from_html(dataset, page_html)
            return [
                _DiscoveredReceitaResource(
                    request=request,
                    discovery_mode="static_html",
                    discovery_url=page_response.url,
                )
                for request in requests
            ]
        except Exception as exc:
            errors.append(f"static_html {page_response.url}: {exc}")
    else:
        errors.append(f"static_html {page_response.url}: HTTP {page_response.status_code}")

    detail = "; ".join(errors) if errors else "no discovery path configured"
    requested = _requested_window_text(start=start, end=end)
    raise ValueError(f"Unable to discover Receita tax-collection resource{requested}: {detail}")


def _manifest_params(
    request: ReceitaResourceRequest,
    *,
    start: date | None,
    end: date | None,
    discovery_mode: str,
    discovery_url: str,
) -> dict[str, object]:
    params: dict[str, object] = {
        "resource_name": request.resource_name,
        "resource_family": request.resource_family,
        "discovery_mode": discovery_mode,
        "discovery_url": discovery_url,
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


def _discovery_failure_params(
    dataset: DatasetConfig,
    *,
    start: date | None,
    end: date | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "resource_name": "resource_discovery",
        "resource_family": "tax_collection_monthly",
        "discovery_mode": "metadata_api_then_static_html",
        "discovery_url": _discovery_failure_url(dataset),
    }
    metadata_url = _metadata_api_url(dataset)
    if metadata_url:
        params["metadata_api_url"] = metadata_url
    page_url = dataset.model_extra.get("source_page_url")
    if page_url:
        params["source_page_url"] = str(page_url)
    if start is not None:
        params["requested_start"] = start.isoformat()
    if end is not None:
        params["requested_end"] = end.isoformat()
    return params


def _metadata_api_url(dataset: DatasetConfig) -> str:
    discovery = dataset.model_extra.get("resource_discovery") or {}
    if not isinstance(discovery, dict):
        return ""
    return str(discovery.get("metadata_api_url") or "")


def _discovery_failure_url(dataset: DatasetConfig) -> str:
    return _metadata_api_url(dataset) or str(dataset.model_extra.get("source_page_url") or "")


def _requested_window_text(*, start: date | None, end: date | None) -> str:
    if start is None and end is None:
        return ""
    start_text = start.isoformat() if start else "open"
    end_text = end.isoformat() if end else "open"
    return f" for {start_text} to {end_text}"
