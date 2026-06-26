from __future__ import annotations

from datetime import date
from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.infra.http import HttpClient
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import DownloadResult, download_daily_dataset_for_date
from bralpha.metadata.manifest import ManifestWriter


def download_reference_rates_for_date(
    repo_root: Path,
    *,
    ref_date: date,
    client: HttpClient | None = None,
    holidays: set[date] | None = None,
) -> DownloadResult:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get("b3_reference_rates")
    if not dataset.source_urls:
        raise NotImplementedError(
            "b3_reference_rates has no confirmed free source URL; add a config-owned "
            "source_urls entry before enabling live downloads"
        )
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    return download_daily_dataset_for_date(
        dataset=dataset,
        raw_store=RawStore(paths.raw),
        manifest_writer=ManifestWriter(paths.manifests / "b3" / "downloads.jsonl"),
        ref_date=ref_date,
        client=client or HttpClient(),
        holidays=holidays,
    )
