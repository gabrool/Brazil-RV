from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from bralpha.domain.b3_calendar import is_business_day
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpClient, HttpResponse
from bralpha.infra.raw_store import RawStore
from bralpha.metadata.datasets import DatasetConfig, render_dataset_request
from bralpha.metadata.manifest import ManifestRecord, ManifestWriter


@dataclass(frozen=True)
class DownloadResult:
    record: ManifestRecord
    raw_path: Path | None


def download_daily_dataset_for_date(
    *,
    dataset: DatasetConfig,
    raw_store: RawStore,
    manifest_writer: ManifestWriter,
    ref_date: date,
    client: HttpClient,
    downloaded_at: datetime | None = None,
    holidays: set[date] | None = None,
    **request_values: object,
) -> DownloadResult:
    timestamp = downloaded_at or datetime.now(UTC)
    request_params = {"ref_date": ref_date.isoformat(), **request_values}
    if not is_business_day(ref_date, holidays):
        record = ManifestRecord(
            dataset_id=dataset.dataset_id,
            source=dataset.source or "b3",
            source_url="",
            request_params=request_params,
            download_timestamp_utc=timestamp,
            file_size_bytes=0,
            license_note=dataset.license_note,
            success=True,
            error_message="skipped_non_business_day",
        )
        manifest_writer.append(record)
        return DownloadResult(record=record, raw_path=None)

    try:
        url, params, headers, filename = render_dataset_request(
            dataset,
            ref_date=ref_date,
            **request_values,
        )
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
        return DownloadResult(record=record, raw_path=None)

    response: HttpResponse | None = None
    try:
        response = client.get_bytes(url, params=params, headers=headers)
        success = 200 <= response.status_code < 300
        raw_path = None
        content_hash = sha256_bytes(response.content) if success else None
        if success:
            raw_path = raw_store.write_bytes(
                dataset.dataset_id,
                response.content,
                filename,
                timestamp,
            )
        record = _manifest_from_response(
            dataset=dataset,
            response=response,
            params=params,
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
                request_params=params,
                download_timestamp_utc=timestamp,
                file_size_bytes=0,
                license_note=dataset.license_note,
                success=False,
                error_message=str(exc),
            )
        else:
            record = _manifest_from_response(
                dataset=dataset,
                response=response,
                params=params,
                timestamp=timestamp,
                raw_path=None,
                content_hash=sha256_bytes(response.content),
                success=False,
                error_message=str(exc),
            )
        raw_path = None
    manifest_writer.append(record)
    return DownloadResult(record=record, raw_path=raw_path)


def _manifest_from_response(
    *,
    dataset: DatasetConfig,
    response: HttpResponse,
    params: dict[str, str],
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
        request_params=params,
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
