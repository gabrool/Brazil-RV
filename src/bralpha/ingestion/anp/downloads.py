from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from bralpha.infra.http import HttpClient
from bralpha.metadata.datasets import DatasetConfig

from .common import (
    ANPDownloadResult,
    anp_dataset,
    anp_manifest_writer,
    anp_paths,
    anp_raw_store,
    client_context,
    download_anp_request,
)
from .resources import (
    ANPResourceRequest,
    anp_fuel_price_resources,
    anp_multi_page_resources,
    anp_single_page_resource,
)


class ANPDatasetNotLiveError(RuntimeError):
    pass


def download_anp_dataset(
    repo_root: Path,
    dataset_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> list[ANPDownloadResult]:
    dataset = anp_dataset(repo_root, dataset_id)
    _require_live_dataset(dataset)
    if dataset.dataset_id == "anp_fuel_prices_weekly":
        if start is None or end is None:
            raise ValueError("anp_fuel_prices_weekly requires start and end")
        requests = anp_fuel_price_resources(dataset, start, end)
        page_html = None
    elif dataset.dataset_id in {"anp_fuel_sales_monthly", "anp_oil_gas_production_monthly"}:
        requests, page_html = [], None
    else:
        raise ANPDatasetNotLiveError(
            f"ANP dataset {dataset.dataset_id!r} has no live downloader in this PR"
        )

    paths = anp_paths(repo_root)
    results: list[ANPDownloadResult] = []
    with client_context(client) as active_client:
        if dataset.dataset_id in {"anp_fuel_sales_monthly", "anp_oil_gas_production_monthly"}:
            page_html = _fetch_page_html(dataset, active_client)
            requests = (
                anp_single_page_resource(dataset, page_html)
                if dataset.dataset_id == "anp_fuel_sales_monthly"
                else anp_multi_page_resources(dataset, page_html)
            )
        for request in requests:
            results.append(
                download_anp_request(
                    dataset=dataset,
                    raw_store=anp_raw_store(paths),
                    manifest_writer=anp_manifest_writer(paths),
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
        raise ANPDatasetNotLiveError(
            f"ANP dataset {dataset.dataset_id!r} is not live in this PR "
            f"(status={dataset.source_map_status!r})"
        )


def _fetch_page_html(dataset: DatasetConfig, client: HttpClient) -> str:
    page_url = dataset.model_extra.get("source_page_url")
    if not page_url:
        raise ValueError(f"Dataset has no source_page_url: {dataset.dataset_id}")
    response = client.get_bytes(str(page_url))
    if not 200 <= response.status_code < 300:
        raise RuntimeError(
            f"Failed to fetch ANP source page for {dataset.dataset_id}: "
            f"HTTP {response.status_code}"
        )
    return response.content.decode("utf-8", errors="replace")


def _manifest_params(
    request: ANPResourceRequest,
    *,
    start: date | None,
    end: date | None,
) -> dict[str, object]:
    params: dict[str, object] = {
        "resource_name": request.resource_name,
        "resource_family": request.resource_family,
    }
    if request.year is not None:
        params["year"] = request.year
    if request.month is not None:
        params["month"] = request.month
    if request.semester is not None:
        params["semester"] = request.semester
    if start is not None:
        params["requested_start"] = start.isoformat()
    if end is not None:
        params["requested_end"] = end.isoformat()
    return params
