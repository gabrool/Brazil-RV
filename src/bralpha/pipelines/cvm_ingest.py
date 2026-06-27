from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_cvm_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.cvm.common import (
    CVMDownloadResult,
    cvm_bronze_root,
    cvm_paths,
    cvm_silver_root,
)
from bralpha.ingestion.cvm.downloads import download_cvm_dataset
from bralpha.normalization.cvm_funds import (
    CVM_SILVER_COLUMNS_BY_DATASET,
    normalize_cvm_to_silver,
    write_cvm_silver,
)
from bralpha.parsing.cvm_funds import (
    parse_cvm_fund_daily_report_file,
    parse_cvm_registry_file,
    write_cvm_fund_daily_bronze,
    write_cvm_registry_bronze,
)
from bralpha.quality.checks import run_quality_checks


def run_cvm_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
) -> dict[str, int]:
    registry = load_cvm_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    results = download_cvm_dataset(
        repo_root,
        dataset_id,
        start=start,
        end=end,
        client=client,
    )
    frames = [
        _parse_successful_result(result, raw_format=dataset.raw_format or "csv")
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = cvm_paths(repo_root)
    if dataset_id == "cvm_fund_daily_reports":
        write_cvm_fund_daily_bronze(bronze, cvm_bronze_root(paths, dataset.dataset_id))
    else:
        write_cvm_registry_bronze(bronze, cvm_bronze_root(paths, dataset.dataset_id))

    silver = normalize_cvm_to_silver(dataset_id, bronze)
    if dataset_id == "cvm_fund_daily_reports":
        if start is None or end is None:
            raise ValueError("cvm_fund_daily_reports requires start and end")
        silver = _filter_window(silver, start=start, end=end)
    run_quality_checks(
        silver,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=CVM_SILVER_COLUMNS_BY_DATASET[dataset_id],
    )
    write_cvm_silver(
        silver,
        cvm_silver_root(paths, dataset.dataset_id),
        primary_keys=dataset.primary_keys,
        partition_cols=dataset.partition_keys,
        ref_date_col=_silver_ref_date_col(dataset_id),
    )
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _parse_successful_result(result: CVMDownloadResult, *, raw_format: str) -> pl.DataFrame:
    raw_path = Path(str(result.record.raw_path))
    common = {
        "raw_format": raw_format,
        "source_dataset": result.record.dataset_id,
        "download_timestamp_utc": result.record.download_timestamp_utc,
        "sha256": result.record.sha256 or "",
    }
    if result.record.dataset_id == "cvm_fund_daily_reports":
        return parse_cvm_fund_daily_report_file(raw_path, **common)
    return parse_cvm_registry_file(raw_path, **common)


def _successful_results(results: list[CVMDownloadResult]) -> list[CVMDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _filter_window(frame: pl.DataFrame, *, start: date, end: date) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.filter((pl.col("ref_date") >= start) & (pl.col("ref_date") <= end))


def _silver_ref_date_col(dataset_id: str) -> str:
    if dataset_id == "cvm_fund_registry_current":
        return "snapshot_date"
    if dataset_id == "cvm_fund_registry_history":
        return "registry_event_date"
    if dataset_id == "cvm_fund_class_registry":
        return "snapshot_date"
    return "ref_date"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)
    status = run_cvm_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
    )
    print(status)


if __name__ == "__main__":
    main()
