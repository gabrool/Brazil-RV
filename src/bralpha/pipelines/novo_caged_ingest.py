from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_novo_caged_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.novo_caged.common import (
    NovoCagedDownloadResult,
    novo_caged_bronze_root,
    novo_caged_paths,
    novo_caged_silver_root,
)
from bralpha.ingestion.novo_caged.downloads import download_novo_caged_dataset
from bralpha.normalization.novo_caged_labor import (
    NOVO_CAGED_SILVER_COLUMNS_BY_DATASET,
    normalize_novo_caged_to_silver,
    write_novo_caged_silver,
)
from bralpha.parsing.novo_caged_tabular import (
    parse_novo_caged_tabular_file,
    write_novo_caged_bronze,
)
from bralpha.quality.checks import run_quality_checks


def run_novo_caged_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
) -> dict[str, int]:
    registry = load_novo_caged_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    results = download_novo_caged_dataset(
        repo_root,
        dataset_id,
        start=start,
        end=end,
        client=client,
    )
    paths = novo_caged_paths(repo_root)
    bronze_rows = 0
    silver_rows = 0
    for result in _successful_results(results):
        bronze_chunk = _parse_successful_result(result, raw_format=dataset.raw_format or "txt")
        bronze_rows += bronze_chunk.height
        write_novo_caged_bronze(
            bronze_chunk,
            novo_caged_bronze_root(paths, dataset.dataset_id),
        )

        silver_chunk = normalize_novo_caged_to_silver(dataset.dataset_id, bronze_chunk)
        silver_chunk = _filter_window(silver_chunk, start=start, end=end)
        run_quality_checks(
            silver_chunk,
            check_names=dataset.quality_checks,
            primary_keys=dataset.primary_keys,
            required_columns=NOVO_CAGED_SILVER_COLUMNS_BY_DATASET[dataset.dataset_id],
        )
        silver_rows += silver_chunk.height
        write_novo_caged_silver(
            silver_chunk,
            novo_caged_silver_root(paths, dataset.dataset_id),
            primary_keys=dataset.primary_keys,
            partition_cols=dataset.partition_keys,
            ref_date_col="ref_date",
        )
    return {"downloads": len(results), "bronze_rows": bronze_rows, "silver_rows": silver_rows}


def _parse_successful_result(
    result: NovoCagedDownloadResult,
    *,
    raw_format: str,
) -> pl.DataFrame:
    raw_path = Path(str(result.record.raw_path))
    params = result.record.request_params
    return parse_novo_caged_tabular_file(
        raw_path,
        raw_format=raw_format,
        source_dataset=result.record.dataset_id,
        resource_name=str(params["resource_name"]),
        period=_optional_text(params.get("period")),
        year=_optional_int(params.get("year")),
        month=_optional_int(params.get("month")),
        record_kind=_optional_text(params.get("record_kind")),
        download_timestamp_utc=result.record.download_timestamp_utc,
        sha256=result.record.sha256 or "",
    )


def _successful_results(
    results: list[NovoCagedDownloadResult],
) -> list[NovoCagedDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _filter_window(
    frame: pl.DataFrame,
    *,
    start: date | None,
    end: date | None,
) -> pl.DataFrame:
    if frame.is_empty() or (start is None and end is None) or "ref_date" not in frame.columns:
        return frame
    expr = pl.lit(True)
    if start is not None:
        expr = expr & (pl.col("ref_date") >= start)
    if end is not None:
        expr = expr & (pl.col("ref_date") <= end)
    return frame.filter(expr)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)
    status = run_novo_caged_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
    )
    print(status)


if __name__ == "__main__":
    main()
