from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

from bralpha.infra.http import HttpClient
from bralpha.ingestion.tesouro.ckan import package_show, resource_download_url, select_resources
from bralpha.ingestion.tesouro.common import (
    TesouroDownloadResult,
    client_context,
    download_tesouro_request,
    tesouro_dataset,
    tesouro_manifest_writer,
    tesouro_paths,
    tesouro_raw_store,
)
from bralpha.metadata.datasets import DatasetConfig
from bralpha.parsing.common import normalize_column_name


class TesouroDatasetNotLiveError(ValueError):
    pass


def download_tesouro_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[TesouroDownloadResult]:
    dataset = tesouro_dataset(repo_root, dataset_id)
    _require_live_dataset(dataset)
    paths = tesouro_paths(repo_root)
    results: list[TesouroDownloadResult] = []

    with client_context(client) as owned_client:
        for resource in _selected_ckan_resources(dataset, owned_client):
            url = resource_download_url(resource)
            manifest_params = {
                "ckan_package": _extra(dataset, "ckan_package"),
                "resource_id": resource.get("id"),
                "resource_name": resource.get("name"),
            }
            if start is not None:
                manifest_params["start"] = start.isoformat()
            if end is not None:
                manifest_params["end"] = end.isoformat()
            results.append(
                download_tesouro_request(
                    dataset=dataset,
                    raw_store=tesouro_raw_store(paths),
                    manifest_writer=tesouro_manifest_writer(paths),
                    url=url,
                    params={},
                    filename=_filename_for_resource(dataset, resource),
                    client=owned_client,
                    downloaded_at=downloaded_at,
                    manifest_params=manifest_params,
                )
            )
    return results


def _require_live_dataset(dataset: DatasetConfig) -> None:
    if dataset.source_map_status != "live_download":
        raise TesouroDatasetNotLiveError(
            f"Tesouro dataset {dataset.dataset_id} is not configured for live download"
        )
    if not _extra(dataset, "ckan_package"):
        raise TesouroDatasetNotLiveError(
            f"Tesouro dataset {dataset.dataset_id} has no configured CKAN package"
        )


def _selected_ckan_resources(dataset: DatasetConfig, client: HttpClient) -> list[dict]:
    package = package_show(str(_extra(dataset, "ckan_package")), client)
    resources = select_resources(
        package,
        formats=list(_extra(dataset, "ckan_resource_formats") or []),
        name_contains=_optional_list(dataset, "ckan_resource_name_contains"),
        name_contains_any=_optional_list(dataset, "ckan_resource_name_contains_any"),
    )
    if len(resources) > 1 and "multi_resource" not in str(dataset.raw_format or ""):
        raise TesouroDatasetNotLiveError(
            f"Tesouro dataset {dataset.dataset_id} selected multiple CKAN resources "
            "but is not configured as multi-resource"
        )
    return resources


def _filename_for_resource(dataset: DatasetConfig, resource: dict) -> str:
    url_path = urlparse(resource_download_url(resource)).path
    suffix = Path(url_path).suffix or ".bin"
    resource_name = normalize_column_name(
        str(resource.get("name") or resource.get("id") or "resource")
    )
    if not resource_name:
        resource_name = dataset.dataset_id
    return f"{resource_name}{suffix}"


def _optional_list(dataset: DatasetConfig, key: str) -> list[str] | None:
    value = _extra(dataset, key)
    if value is None:
        return None
    return list(value)


def _extra(dataset: DatasetConfig, key: str):
    return (dataset.model_extra or {}).get(key)
