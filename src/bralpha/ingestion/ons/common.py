from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.infra.config import (
    load_ons_dataset_registry,
    load_paths_config,
    resolve_project_paths,
)
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpClient, HttpResponse
from bralpha.infra.paths import ResolvedPaths
from bralpha.infra.raw_store import RawStore
from bralpha.metadata.datasets import DatasetConfig
from bralpha.metadata.manifest import ManifestRecord, ManifestWriter


@dataclass(frozen=True)
class ONSDownloadResult:
    record: ManifestRecord
    raw_path: Path | None


def client_context(client: HttpClient | None) -> AbstractContextManager[HttpClient]:
    return nullcontext(client) if client is not None else HttpClient()


def ons_paths(repo_root: Path) -> ResolvedPaths:
    return resolve_project_paths(repo_root, load_paths_config(repo_root))


def ons_dataset(repo_root: Path, dataset_id: str) -> DatasetConfig:
    return load_ons_dataset_registry(repo_root).get(dataset_id)


def ons_raw_store(paths: ResolvedPaths) -> RawStore:
    return RawStore(paths.raw, source="ons")


def ons_manifest_writer(paths: ResolvedPaths) -> ManifestWriter:
    return ManifestWriter(paths.manifests / "ons" / "downloads.jsonl")


def ons_bronze_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.bronze / "ons" / dataset_id


def ons_silver_root(paths: ResolvedPaths, dataset_id: str) -> Path:
    return paths.silver / dataset_id


def download_ons_request(
    *,
    dataset: DatasetConfig,
    raw_store: RawStore,
    manifest_writer: ManifestWriter,
    url: str,
    filename: str,
    client: HttpClient,
    manifest_params: dict[str, Any],
    downloaded_at: datetime | None = None,
) -> ONSDownloadResult:
    timestamp = downloaded_at or datetime.now(UTC)
    response: HttpResponse | None = None
    try:
        response = client.get_bytes(url)
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
            request_params=manifest_params,
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
            request_params=manifest_params,
            timestamp=timestamp,
            response=response,
            error_message=str(exc),
        )
    manifest_writer.append(record)
    return ONSDownloadResult(record=record, raw_path=raw_path)


def write_partitioned_frame(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    ref_date_col: str,
    partition_cols: list[str],
    filename: str = "data.parquet",
) -> list[Path]:
    if frame.is_empty():
        return []
    output_root.mkdir(parents=True, exist_ok=True)
    work = frame
    derived_year = False
    if "year" in partition_cols and "year" not in work.columns:
        if ref_date_col not in work.columns:
            raise ValueError("year partition requires ref_date_col")
        work = work.with_columns(
            pl.col(ref_date_col).map_elements(_year_from_value, return_dtype=pl.Int64).alias("year")
        )
        derived_year = True

    missing = [column for column in partition_cols if column not in work.columns]
    if missing:
        raise ValueError(f"Missing partition columns: {missing}")

    paths: list[Path] = []
    for values in work.select(partition_cols).unique(maintain_order=True).to_dicts():
        part = work.filter(_partition_filter(values))
        if derived_year:
            part = part.drop("year")
        part_dir = output_root
        for column in partition_cols:
            part_dir = part_dir / f"{column}={_partition_value(values[column])}"
        paths.extend(_write_part(part, part_dir, filename=filename, primary_keys=primary_keys))
    return paths


def _write_part(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    filename: str,
    primary_keys: list[str],
) -> list[Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / filename
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="diagonal_relaxed")
        available_keys = [key for key in primary_keys if key in frame.columns]
        if available_keys:
            frame = frame.unique(subset=available_keys, keep="last", maintain_order=True)
    frame.write_parquet(path)
    return [path]


def _partition_filter(values: dict[str, object]) -> pl.Expr:
    expr: pl.Expr | None = None
    for column, value in values.items():
        condition = pl.col(column).is_null() if value is None else pl.col(column) == value
        expr = condition if expr is None else expr & condition
    if expr is None:
        raise ValueError("partition values must not be empty")
    return expr


def _partition_value(value: object) -> str:
    if value is None:
        return "__null__"
    return str(value).replace("\\", "_").replace("/", "_")


def _year_from_value(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().year
    if isinstance(value, date):
        return value.year
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10]).year


def _manifest_from_response(
    *,
    dataset: DatasetConfig,
    response: HttpResponse,
    request_params: dict[str, Any],
    timestamp: datetime,
    raw_path: Path | None,
    content_hash: str | None,
    success: bool,
    error_message: str | None,
) -> ManifestRecord:
    return ManifestRecord(
        dataset_id=dataset.dataset_id,
        source=dataset.source or "ons",
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


def _failure_record(
    *,
    dataset: DatasetConfig,
    url: str,
    request_params: dict[str, Any],
    timestamp: datetime,
    response: HttpResponse | None,
    error_message: str,
) -> ManifestRecord:
    return ManifestRecord(
        dataset_id=dataset.dataset_id,
        source=dataset.source or "ons",
        source_url=url,
        request_params=request_params,
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
