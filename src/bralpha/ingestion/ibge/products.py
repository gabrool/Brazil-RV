from __future__ import annotations

from datetime import datetime
from pathlib import Path

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


def download_products_metadata(
    repo_root: Path,
    *,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[IbgeDownloadResult]:
    dataset = ibge_dataset(repo_root, "ibge_products_metadata")
    paths = ibge_paths(repo_root)
    url, params, filename = build_products_request(dataset)
    with client_context(client) as owned_client:
        return [
            download_ibge_request(
                dataset=dataset,
                raw_store=ibge_raw_store(paths),
                manifest_writer=ibge_manifest_writer(paths),
                url=url,
                params=params,
                filename=filename,
                client=owned_client,
                downloaded_at=downloaded_at,
            )
        ]


def build_products_request(dataset: DatasetConfig) -> tuple[str, dict[str, str], str]:
    url, params, _, filename = dataset.first_source_url().render()
    return url, params, filename or "ibge_products_statistics.json"
