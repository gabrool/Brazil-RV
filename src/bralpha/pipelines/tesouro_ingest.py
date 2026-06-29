from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.infra.config import load_tesouro_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.tesouro.common import (
    TesouroDownloadResult,
    tesouro_bronze_root,
    tesouro_paths,
    tesouro_silver_root,
)
from bralpha.ingestion.tesouro.downloads import download_tesouro_dataset
from bralpha.normalization.tesouro_market import (
    SILVER_COLUMNS_BY_DATASET,
    normalize_tesouro_to_silver,
    write_tesouro_silver,
)
from bralpha.parsing.tesouro_tabular import parse_tesouro_file, write_tesouro_bronze
from bralpha.quality.checks import run_quality_checks


def run_tesouro_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
) -> dict[str, int]:
    registry = load_tesouro_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    results = download_tesouro_dataset(
        repo_root,
        dataset_id,
        start=start,
        end=end,
        client=client,
    )
    frames = [
        parse_tesouro_file(
            Path(result.record.raw_path),
            raw_format=_implemented_raw_format(dataset.raw_format),
            source_dataset=result.record.dataset_id,
            download_timestamp_utc=result.record.download_timestamp_utc,
            sha256=result.record.sha256 or "",
            resource_name=str(result.record.request_params.get("resource_name") or ""),
        )
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = tesouro_paths(repo_root)
    write_tesouro_bronze(bronze, tesouro_bronze_root(paths, dataset_id))

    silver = normalize_tesouro_to_silver(
        dataset_id,
        bronze,
        holidays=_reference_holidays(paths),
    )
    required_columns = SILVER_COLUMNS_BY_DATASET.get(dataset_id)
    if required_columns is None:
        raise NotImplementedError(f"Tesouro silver normalizer is not implemented for {dataset_id}")
    run_quality_checks(
        silver,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=required_columns,
    )
    write_tesouro_silver(
        silver,
        tesouro_silver_root(paths, dataset_id),
        primary_keys=dataset.primary_keys,
        partition_cols=dataset.partition_keys,
    )
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _successful_results(results: list[TesouroDownloadResult]) -> list[TesouroDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _implemented_raw_format(raw_format: str | None) -> str:
    if raw_format in {"csv", "csv_multi_resource"}:
        return raw_format
    raise ValueError(f"Tesouro raw format is not implemented for live parsing: {raw_format}")


def _reference_holidays(paths) -> set[date] | None:
    for dataset_id in ["b3_holiday_calendar", "reference_calendar"]:
        holidays = _holiday_dates(paths.silver / dataset_id)
        if holidays is not None:
            return holidays
    return None


def _holiday_dates(root: Path) -> set[date] | None:
    if not root.exists():
        return None
    files = sorted(root.glob("**/*.parquet"))
    if not files:
        return None
    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    if frame.is_empty() or "ref_date" not in frame.columns:
        return None
    if "is_business_day" in frame.columns:
        frame = frame.filter(pl.col("is_business_day") == False)  # noqa: E712
    return {_as_date(row["ref_date"]) for row in frame.select("ref_date").to_dicts()}


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)
    status = run_tesouro_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
    )
    print(status)


if __name__ == "__main__":
    main()
