from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_anbima_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.anbima.common import (
    AnbimaDownloadResult,
    anbima_bronze_root,
    anbima_paths,
    anbima_silver_root,
)
from bralpha.ingestion.anbima.downloads import download_anbima_dataset
from bralpha.normalization.anbima_market import (
    SILVER_COLUMNS_BY_DATASET,
    normalize_anbima_to_silver,
    write_anbima_silver,
)
from bralpha.parsing.anbima_tabular import parse_anbima_file, write_anbima_bronze
from bralpha.quality.checks import run_quality_checks


def run_anbima_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
) -> dict[str, int]:
    registry = load_anbima_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    results = download_anbima_dataset(
        repo_root,
        dataset_id,
        start=start,
        end=end,
        client=client,
    )
    frames = [
        parse_anbima_file(
            Path(result.record.raw_path),
            raw_format=_implemented_raw_format(dataset.raw_format),
            source_dataset=result.record.dataset_id,
            download_timestamp_utc=result.record.download_timestamp_utc,
            sha256=result.record.sha256 or "",
        )
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = anbima_paths(repo_root)
    write_anbima_bronze(bronze, anbima_bronze_root(paths, dataset_id))

    silver = normalize_anbima_to_silver(dataset_id, bronze)
    required_columns = SILVER_COLUMNS_BY_DATASET.get(dataset_id)
    if required_columns is None:
        raise NotImplementedError(f"ANBIMA silver normalizer is not implemented for {dataset_id}")
    run_quality_checks(
        silver,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=required_columns,
    )
    write_anbima_silver(
        silver,
        anbima_silver_root(paths, dataset_id),
        primary_keys=dataset.primary_keys,
        partition_cols=dataset.partition_keys,
    )
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _successful_results(results: list[AnbimaDownloadResult]) -> list[AnbimaDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _implemented_raw_format(raw_format: str | None) -> str:
    if raw_format in {"json", "csv", "txt", "txt_semicolon"}:
        return raw_format
    raise ValueError(f"ANBIMA raw format is not implemented for live parsing: {raw_format}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)
    status = run_anbima_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
    )
    print(status)


if __name__ == "__main__":
    main()
