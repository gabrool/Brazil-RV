from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.infra.http import HttpClient
from bralpha.infra.raw_store import RawStore
from bralpha.ingestion.b3.common import (
    DownloadResult,
    client_context,
    download_daily_dataset_for_date,
)
from bralpha.metadata.manifest import ManifestWriter


def download_trade_summary_for_date(
    repo_root: Path,
    *,
    ref_date: date,
    commodity: str,
    client: HttpClient | None = None,
    holidays: set[date] | None = None,
) -> DownloadResult:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get("b3_derivatives_trade_summary")
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    with client_context(client) as owned_client:
        return download_daily_dataset_for_date(
            dataset=dataset,
            raw_store=RawStore(paths.raw),
            manifest_writer=ManifestWriter(paths.manifests / "b3" / "downloads.jsonl"),
            ref_date=ref_date,
            client=owned_client,
            holidays=holidays,
            commodity=commodity.upper(),
        )


def download_trade_summary_range(
    repo_root: Path,
    *,
    start: date,
    end: date,
    commodities: list[str] | None = None,
    client: HttpClient | None = None,
    holidays: set[date] | None = None,
) -> list[DownloadResult]:
    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get("b3_derivatives_trade_summary")
    roots = commodities or dataset.request_defaults.get("commodity_roots", [])
    results: list[DownloadResult] = []
    current = start
    with client_context(client) as owned_client:
        while current <= end:
            for commodity in roots:
                results.append(
                    download_trade_summary_for_date(
                        repo_root,
                        ref_date=current,
                        commodity=str(commodity),
                        client=owned_client,
                        holidays=holidays,
                    )
                )
            current += timedelta(days=1)
    return results
