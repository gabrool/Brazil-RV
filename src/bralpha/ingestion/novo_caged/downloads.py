from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    NovoCagedDownloadResult,
    client_context,
    download_novo_caged_request,
    novo_caged_dataset,
    novo_caged_manifest_writer,
    novo_caged_paths,
    novo_caged_raw_store,
)
from .resources import (
    NovoCagedResourceRequest,
    novo_caged_monthly_resources,
    novo_caged_release_calendar_resource,
)


class NovoCagedDatasetNotLiveError(RuntimeError):
    pass


def download_novo_caged_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[NovoCagedDownloadResult]:
    dataset = novo_caged_dataset(repo_root, dataset_id)
    _require_live_dataset(dataset)
    if dataset.dataset_id == "novo_caged_movements_monthly":
        if start is None or end is None:
            raise ValueError("novo_caged_movements_monthly requires start and end")
        requests = novo_caged_monthly_resources(dataset, start, end)
    elif dataset.dataset_id == "novo_caged_release_calendar":
        requests = novo_caged_release_calendar_resource(dataset)
    else:
        raise NovoCagedDatasetNotLiveError(
            f"Novo CAGED dataset {dataset.dataset_id!r} has no live downloader in this PR"
        )

    paths = novo_caged_paths(repo_root)
    results: list[NovoCagedDownloadResult] = []
    with client_context(client) as active_client:
        for request in requests:
            results.append(
                download_novo_caged_request(
                    dataset=dataset,
                    raw_store=novo_caged_raw_store(paths),
                    manifest_writer=novo_caged_manifest_writer(paths),
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
        raise NovoCagedDatasetNotLiveError(
            f"Novo CAGED dataset {dataset.dataset_id!r} is not live in this PR "
            f"(status={dataset.source_map_status!r})"
        )


def _manifest_params(
    request: NovoCagedResourceRequest,
    *,
    start: date | None,
    end: date | None,
) -> dict[str, object]:
    params: dict[str, object] = {"resource_name": request.resource_name}
    if request.period is not None:
        params["period"] = request.period
    if request.year is not None:
        params["year"] = request.year
    if request.month is not None:
        params["month"] = request.month
    if request.record_kind is not None:
        params["record_kind"] = request.record_kind
    if start is not None:
        params["requested_start"] = start.isoformat()
    if end is not None:
        params["requested_end"] = end.isoformat()
    return params
