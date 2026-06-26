from __future__ import annotations

import json
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.infra.config import load_bcb_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpClient, HttpResponse
from bralpha.infra.paths import ResolvedPaths
from bralpha.infra.raw_store import RawStore
from bralpha.metadata.datasets import DatasetConfig
from bralpha.metadata.manifest import ManifestRecord, ManifestWriter

PTAX_ODATA_BASE = "https://olinda.bcb.gov.br/olinda/service/PTAX/version/v1/odata"
FOCUS_ODATA_BASE = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata"


@dataclass(frozen=True)
class BcbDownloadResult:
    record: ManifestRecord
    raw_path: Path | None


def client_context(client: HttpClient | None) -> AbstractContextManager[HttpClient]:
    return nullcontext(client) if client is not None else HttpClient()


def bcb_paths(repo_root: Path) -> ResolvedPaths:
    return resolve_project_paths(repo_root, load_paths_config(repo_root))


def bcb_dataset(repo_root: Path, dataset_id: str) -> DatasetConfig:
    return load_bcb_dataset_registry(repo_root).get(dataset_id)


def bcb_raw_store(paths: ResolvedPaths) -> RawStore:
    return RawStore(paths.raw, source="bcb")


def bcb_manifest_writer(paths: ResolvedPaths) -> ManifestWriter:
    return ManifestWriter(paths.manifests / "bcb" / "downloads.jsonl")


def bcb_bronze_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.bronze / "bcb" / dataset_id


def bcb_silver_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.silver / dataset_id


def download_bcb_request(
    *,
    dataset: DatasetConfig,
    raw_store: RawStore,
    manifest_writer: ManifestWriter,
    url: str,
    params: dict[str, Any],
    filename: str,
    client: HttpClient,
    downloaded_at: datetime | None = None,
    headers: dict[str, str] | None = None,
    manifest_params: dict[str, Any] | None = None,
) -> BcbDownloadResult:
    timestamp = downloaded_at or datetime.now(UTC)
    record_params = manifest_params or params
    response: HttpResponse | None = None
    try:
        response = client.get_bytes(url, params=params, headers=headers)
        success = 200 <= response.status_code < 300
        content_hash = sha256_bytes(response.content) if success else None
        raw_path = (
            raw_store.write_bytes(dataset.dataset_id, response.content, filename, timestamp)
            if success
            else None
        )
        record = _manifest_from_response(
            dataset=dataset,
            response=response,
            params=record_params,
            timestamp=timestamp,
            raw_path=raw_path,
            content_hash=content_hash,
            success=success,
            error_message=None if success else f"HTTP {response.status_code}",
        )
    except Exception as exc:
        raw_path = None
        record = _failure_record(
            dataset=dataset,
            url=url if response is None else response.url,
            params=record_params,
            timestamp=timestamp,
            response=response,
            error_message=str(exc),
        )
    manifest_writer.append(record)
    return BcbDownloadResult(record=record, raw_path=raw_path)


def odata_value_count(content: bytes) -> int:
    payload = json.loads(content.decode("utf-8-sig"))
    value = payload.get("value") if isinstance(payload, dict) else None
    return len(value) if isinstance(value, list) else 0


def write_bronze_frame(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    filename: str = "data.parquet",
) -> list[Path]:
    if frame.is_empty():
        return []
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / filename
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="diagonal_relaxed")
        available_keys = [key for key in primary_keys if key in frame.columns]
        if available_keys:
            frame = frame.unique(subset=available_keys, keep="last", maintain_order=True)
    frame.write_parquet(path)
    return [path]


def _manifest_from_response(
    *,
    dataset: DatasetConfig,
    response: HttpResponse,
    params: dict[str, Any],
    timestamp: datetime,
    raw_path: Path | None,
    content_hash: str | None,
    success: bool,
    error_message: str | None,
) -> ManifestRecord:
    return ManifestRecord(
        dataset_id=dataset.dataset_id,
        source=dataset.source or "bcb",
        source_url=response.url,
        request_params=dict(params),
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


def _failure_record(
    *,
    dataset: DatasetConfig,
    url: str,
    params: dict[str, Any],
    timestamp: datetime,
    response: HttpResponse | None,
    error_message: str,
) -> ManifestRecord:
    return ManifestRecord(
        dataset_id=dataset.dataset_id,
        source=dataset.source or "bcb",
        source_url=url,
        request_params=dict(params),
        download_timestamp_utc=timestamp,
        http_status=response.status_code if response is not None else None,
        content_type=response.headers.get("content-type") if response is not None else None,
        file_size_bytes=len(response.content) if response is not None else 0,
        sha256=sha256_bytes(response.content) if response is not None else None,
        raw_path=None,
        license_note=dataset.license_note,
        success=False,
        error_message=error_message,
    )
