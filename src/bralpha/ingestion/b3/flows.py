from __future__ import annotations

from datetime import date
from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.infra.http import HttpClient
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import (
    DownloadResult,
    client_context,
    download_daily_dataset_for_date,
)
from bralpha.metadata.datasets import DatasetConfig
from bralpha.metadata.manifest import ManifestWriter


def download_equities_investor_participation_for_date(
    repo_root: Path,
    *,
    ref_date: date,
    client: HttpClient | None = None,
    holidays: set[date] | None = None,
) -> DownloadResult:
    return _download_flow_dataset_for_date(
        repo_root,
        dataset_id="b3_equities_investor_participation",
        ref_date=ref_date,
        client=client,
        holidays=holidays,
    )


def download_foreign_investor_movement_for_date(
    repo_root: Path,
    *,
    ref_date: date,
    client: HttpClient | None = None,
    holidays: set[date] | None = None,
) -> DownloadResult:
    return _download_flow_dataset_for_date(
        repo_root,
        dataset_id="b3_foreign_investor_movement",
        ref_date=ref_date,
        client=client,
        holidays=holidays,
    )


def _download_flow_dataset_for_date(
    repo_root: Path,
    *,
    dataset_id: str,
    ref_date: date,
    client: HttpClient | None,
    holidays: set[date] | None,
) -> DownloadResult:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    _require_source_urls(dataset)
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    with client_context(client) as owned_client:
        return download_daily_dataset_for_date(
            dataset=dataset,
            raw_store=RawStore(paths.raw),
            manifest_writer=ManifestWriter(paths.manifests / "b3" / "downloads.jsonl"),
            ref_date=ref_date,
            client=owned_client,
            holidays=holidays,
        )


def _require_source_urls(dataset: DatasetConfig) -> None:
    if not dataset.source_urls:
        raise NotImplementedError(
            f"{dataset.dataset_id} has no confirmed free source URL; add a config-owned "
            "source_urls entry before enabling live downloads"
        )
