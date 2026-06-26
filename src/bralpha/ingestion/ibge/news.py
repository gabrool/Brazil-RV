from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    IbgeDownloadResult,
    client_context,
    download_ibge_request,
    ibge_dataset,
    ibge_items_count,
    ibge_manifest_writer,
    ibge_paths,
    ibge_raw_store,
)


def download_news_metadata(
    repo_root: Path,
    *,
    start: date,
    end: date,
    tipo: str = "release",
    product_id: int | None = None,
    page: int = 1,
    page_size: int = 100,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[IbgeDownloadResult]:
    dataset = ibge_dataset(repo_root, "ibge_news_releases_metadata")
    paths = ibge_paths(repo_root)
    results: list[IbgeDownloadResult] = []
    with client_context(client) as owned_client:
        current_page = page
        while True:
            url, params, filename = build_news_request(
                dataset,
                start=start,
                end=end,
                tipo=tipo,
                product_id=product_id,
                page=current_page,
                page_size=page_size,
            )
            result = download_ibge_request(
                dataset=dataset,
                raw_store=ibge_raw_store(paths),
                manifest_writer=ibge_manifest_writer(paths),
                url=url,
                params=params,
                filename=filename,
                client=owned_client,
                downloaded_at=downloaded_at,
                manifest_params={
                    "tipo": tipo,
                    "product_id": product_id,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "page": current_page,
                    **params,
                },
            )
            results.append(result)
            if result.raw_path is None or not result.record.success:
                break
            if ibge_items_count(result.raw_path.read_bytes()) < page_size:
                break
            current_page += 1
    return results


def build_news_request(
    dataset: DatasetConfig,
    *,
    start: date,
    end: date,
    tipo: str = "release",
    product_id: int | None = None,
    page: int = 1,
    page_size: int = 100,
) -> tuple[str, dict[str, str], str]:
    url, params, _, filename = dataset.first_source_url().render(
        start=start,
        end=end,
        tipo=tipo,
        product_id=product_id or "",
        page=page,
        page_size=page_size,
    )
    if filename is None:
        filename = f"ibge_news_{tipo}_{start:%Y%m%d}_{end:%Y%m%d}_page{page}.json"
    return url, params, filename
