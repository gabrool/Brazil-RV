from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpClient
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import client_context
from bralpha.metadata.datasets import render_dataset_request
from bralpha.metadata.manifest import ManifestRecord, ManifestWriter


def download_cotahist_year(
    repo_root: Path,
    *,
    year: int,
    client: HttpClient | None = None,
    downloaded_at: datetime | None = None,
) -> ManifestRecord:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get("b3_cotahist_yearly")
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    manifest_writer = ManifestWriter(paths.manifests / "b3" / "downloads.jsonl")
    timestamp = downloaded_at or datetime.now(UTC)

    try:
        url, params, headers, filename = render_dataset_request(dataset, year=year)
    except Exception as exc:
        record = ManifestRecord(
            dataset_id=dataset.dataset_id,
            source=dataset.source or "b3",
            source_url="",
            request_params={"year": year},
            download_timestamp_utc=timestamp,
            file_size_bytes=0,
            license_note=dataset.license_note,
            success=False,
            error_message=str(exc),
        )
        manifest_writer.append(record)
        return record

    request_params = {"year": year, **params}
    response = None
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
        record = ManifestRecord(
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
            record = ManifestRecord(
                dataset_id=dataset.dataset_id,
                source=dataset.source or "b3",
                source_url=response.url,
                request_params=request_params,
                download_timestamp_utc=timestamp,
                http_status=response.status_code,
                content_type=response.headers.get("content-type"),
                file_size_bytes=len(response.content),
                sha256=sha256_bytes(response.content),
                raw_path=None,
                license_note=dataset.license_note,
                success=False,
                error_message=str(exc),
            )
    manifest_writer.append(record)
    return record
