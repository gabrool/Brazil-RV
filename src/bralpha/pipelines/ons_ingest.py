from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_ons_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.ons.common import (
    ONSDownloadResult,
    ons_bronze_root,
    ons_paths,
    ons_silver_root,
)
from bralpha.ingestion.ons.downloads import download_ons_dataset
from bralpha.normalization.ons_power import (
    ONS_SILVER_COLUMNS_BY_DATASET,
    normalize_ons_to_silver,
    write_ons_silver,
)
from bralpha.parsing.ons_tabular import parse_ons_tabular_file, write_ons_bronze
from bralpha.quality.checks import run_quality_checks


def run_ons_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date,
    end: date,
    client: HttpClient | None = None,
) -> dict[str, int]:
    registry = load_ons_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    results = download_ons_dataset(
        repo_root,
        dataset_id,
        start=start,
        end=end,
        client=client,
    )
    paths = ons_paths(repo_root)
    bronze_rows = 0
    silver_rows = 0
    for result in _successful_results(results):
        bronze_chunk = _parse_successful_result(
            result, raw_format=dataset.raw_format or "csv_annual"
        )
        bronze_rows += bronze_chunk.height
        write_ons_bronze(bronze_chunk, ons_bronze_root(paths, dataset.dataset_id))

        silver_chunk = normalize_ons_to_silver(dataset.dataset_id, bronze_chunk)
        silver_chunk = _filter_window(silver_chunk, start=start, end=end)
        run_quality_checks(
            silver_chunk,
            check_names=dataset.quality_checks,
            primary_keys=dataset.primary_keys,
            required_columns=ONS_SILVER_COLUMNS_BY_DATASET[dataset.dataset_id],
        )
        silver_rows += silver_chunk.height
        write_ons_silver(
            silver_chunk,
            ons_silver_root(paths, dataset.dataset_id),
            primary_keys=dataset.primary_keys,
            partition_cols=dataset.partition_keys,
            ref_date_col="ref_date",
        )
    return {"downloads": len(results), "bronze_rows": bronze_rows, "silver_rows": silver_rows}


def _parse_successful_result(result: ONSDownloadResult, *, raw_format: str) -> pl.DataFrame:
    raw_path = Path(str(result.record.raw_path))
    params = result.record.request_params
    return parse_ons_tabular_file(
        raw_path,
        raw_format=raw_format,
        source_dataset=result.record.dataset_id,
        resource_name=str(params["resource_name"]),
        year=int(params["year"]),
        download_timestamp_utc=result.record.download_timestamp_utc,
        sha256=result.record.sha256 or "",
    )


def _successful_results(results: list[ONSDownloadResult]) -> list[ONSDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _filter_window(frame: pl.DataFrame, *, start: date, end: date) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.filter((pl.col("ref_date") >= start) & (pl.col("ref_date") <= end))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args(argv)
    status = run_ons_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
    )
    print(status)


if __name__ == "__main__":
    main()
