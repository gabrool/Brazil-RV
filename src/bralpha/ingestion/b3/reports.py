from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpClient, HttpResponse
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import client_context
from bralpha.metadata.datasets import DatasetConfig, render_dataset_request
from bralpha.metadata.manifest import ManifestRecord, ManifestWriter


def download_fee_schedule_page(
    repo_root: Path,
    *,
    fee_id: str,
    page_url: str,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> ManifestRecord:
    return _download_raw_page_dataset_item(
        repo_root,
        dataset_id="b3_fee_schedules",
        request_values={"fee_id": fee_id, "page_url": page_url},
        request_params={"fee_id": fee_id, "page_url": page_url},
        client=client,
        downloaded_at=downloaded_at,
    )


def download_product_specs_page(
    repo_root: Path,
    *,
    product_root: str,
    product_name: str,
    page_url: str,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> ManifestRecord:
    return _download_raw_page_dataset_item(
        repo_root,
        dataset_id="b3_product_specs_pages",
        request_values={
            "product_root": product_root,
            "product_name": product_name,
            "page_url": page_url,
        },
        request_params={
            "product_root": product_root,
            "product_name": product_name,
            "page_url": page_url,
        },
        client=client,
        downloaded_at=downloaded_at,
    )


def download_daily_bulletin_chapter_for_date(
    repo_root: Path,
    *,
    ref_date: date,
    report_section: str,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> ManifestRecord:
    _raise_pending_url(repo_root, "b3_daily_bulletin_chapters")


def download_market_data_public_report_for_date(
    repo_root: Path,
    *,
    ref_date: date,
    report_name: str,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> ManifestRecord:
    _raise_pending_url(repo_root, "b3_market_data_public_reports")


def _download_raw_page_dataset_item(
    repo_root: Path,
    *,
    dataset_id: str,
    request_values: dict[str, object],
    request_params: dict[str, object],
    client: HttpClient | None,
    downloaded_at: datetime | None,
) -> ManifestRecord:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    _require_source_urls(dataset)
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    manifest_writer = ManifestWriter(paths.manifests / "b3" / "downloads.jsonl")
    timestamp = downloaded_at or datetime.now(UTC)

    try:
        url, params, headers, filename = render_dataset_request(dataset, **request_values)
    except Exception as exc:
        record = ManifestRecord(
            dataset_id=dataset.dataset_id,
            source=dataset.source or "b3",
            source_url="",
            request_params=request_params,
            download_timestamp_utc=timestamp,
            file_size_bytes=0,
            license_note=dataset.license_note,
            success=False,
            error_message=str(exc),
        )
        manifest_writer.append(record)
        return record

    request_params = {**request_params, **params}
    response: HttpResponse | None = None
    try:
        with client_context(client) as owned_client:
            response = owned_client.get_bytes(url, params=params, headers=headers)
        success = 200 <= response.status_code < 300
        raw_path = None
        content_hash = sha256_bytes(response.content) if success else None
        if success:
            raw_path = RawStore(paths.raw).write_bytes(
                dataset.dataset_id,
                response.content,
                filename,
                timestamp,
            )
        record = _manifest_from_raw_page_response(
            dataset=dataset,
            response=response,
            request_params=request_params,
            timestamp=timestamp,
            raw_path=raw_path,
            content_hash=content_hash,
            success=success,
            error_message=None if success else f"HTTP {response.status_code}",
        )
    except Exception as exc:
        if response is None:
            record = ManifestRecord(
                dataset_id=dataset.dataset_id,
                source=dataset.source or "b3",
                source_url=url,
                request_params=request_params,
                download_timestamp_utc=timestamp,
                file_size_bytes=0,
                license_note=dataset.license_note,
                success=False,
                error_message=str(exc),
            )
        else:
            record = _manifest_from_raw_page_response(
                dataset=dataset,
                response=response,
                request_params=request_params,
                timestamp=timestamp,
                raw_path=None,
                content_hash=sha256_bytes(response.content),
                success=False,
                error_message=str(exc),
            )
    manifest_writer.append(record)
    return record


def _manifest_from_raw_page_response(
    *,
    dataset: DatasetConfig,
    response: HttpResponse,
    request_params: dict[str, object],
    timestamp: datetime,
    raw_path: Path | None,
    content_hash: str | None,
    success: bool,
    error_message: str | None,
) -> ManifestRecord:
    return ManifestRecord(
        dataset_id=dataset.dataset_id,
        source=dataset.source or "b3",
        source_url=response.url,
        request_params=request_params,
        download_timestamp_utc=timestamp,
        http_status=response.status_code,
        content_type=response.headers.get("content-type"),
        file_size_bytes=len(response.content),
        sha256=content_hash,
        raw_path=str(raw_path) if raw_path is not None else None,
        license_note=dataset.license_note,
        success=success,
        error_message=error_message,
    )


def _raise_pending_url(repo_root: Path, dataset_id: str) -> None:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    _require_source_urls(dataset)


def _require_source_urls(dataset: DatasetConfig) -> None:
    if not dataset.source_urls:
        raise NotImplementedError(
            f"{dataset.dataset_id} has no confirmed free source URL; add a config-owned "
            "source_urls entry before enabling live downloads"
        )
