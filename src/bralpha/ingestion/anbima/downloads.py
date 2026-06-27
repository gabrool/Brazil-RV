from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.ingestion.anbima.common import (
    AnbimaDownloadResult,
    anbima_dataset,
    anbima_manifest_writer,
    anbima_paths,
    anbima_raw_store,
    client_context,
    download_anbima_request,
)


class ANBIMAEndpointNotVerifiedError(ValueError):
    pass


def download_anbima_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[AnbimaDownloadResult]:
    dataset = anbima_dataset(repo_root, dataset_id)
    _require_verified_live_dataset(dataset)
    paths = anbima_paths(repo_root)
    results: list[AnbimaDownloadResult] = []
    values = {
        "dataset_id": dataset.dataset_id,
        "start": start,
        "end": end,
    }
    with client_context(client) as owned_client:
        for source_url in dataset.source_urls:
            url, params, headers, filename = source_url.render(**values)
            if filename is None:
                filename = f"{dataset.dataset_id}.bin"
            results.append(
                download_anbima_request(
                    dataset=dataset,
                    raw_store=anbima_raw_store(paths),
                    manifest_writer=anbima_manifest_writer(paths),
                    url=url,
                    params=params,
                    headers=headers,
                    filename=filename,
                    client=owned_client,
                    downloaded_at=downloaded_at,
                    manifest_params={**params, "dataset_id": dataset.dataset_id},
                )
            )
    return results


def _require_verified_live_dataset(dataset) -> None:
    endpoint_verified = bool((dataset.model_extra or {}).get("endpoint_verified"))
    if (
        dataset.source_map_status != "live_download"
        or not endpoint_verified
        or not dataset.source_urls
    ):
        raise ANBIMAEndpointNotVerifiedError(
            f"ANBIMA dataset {dataset.dataset_id} has no verified live endpoint"
        )
