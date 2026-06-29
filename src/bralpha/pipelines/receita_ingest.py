from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_receita_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.receita.common import (
    ReceitaDownloadResult,
    receita_bronze_root,
    receita_paths,
    receita_silver_root,
)
from bralpha.ingestion.receita.downloads import download_receita_dataset
from bralpha.metadata.manifest import manifest_bronze_metadata
from bralpha.normalization.receita_revenue import (
    RECEITA_SILVER_COLUMNS_BY_DATASET,
    normalize_receita_to_silver,
    write_receita_silver,
)
from bralpha.parsing.receita_tabular import parse_receita_tabular_file, write_receita_bronze
from bralpha.quality.checks import run_quality_checks


def run_receita_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
) -> dict[str, int]:
    if start is None or end is None:
        raise ValueError(
            "Receita ingest requires start and end to avoid accidental full-history writes"
        )
    if start > end:
        raise ValueError("Receita ingest requires start <= end")

    registry = load_receita_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    results = download_receita_dataset(
        repo_root,
        dataset_id,
        start=start,
        end=end,
        client=client,
    )
    paths = receita_paths(repo_root)
    bronze_rows = 0
    silver_rows = 0
    for result in _successful_results(results):
        bronze_chunk = _parse_successful_result(result, raw_format=dataset.raw_format or "csv")
        bronze_rows += bronze_chunk.height
        write_receita_bronze(bronze_chunk, receita_bronze_root(paths, dataset.dataset_id))

        silver_chunk = normalize_receita_to_silver(dataset.dataset_id, bronze_chunk)
        silver_chunk = _filter_window(silver_chunk, start=start, end=end)
        run_quality_checks(
            silver_chunk,
            check_names=dataset.quality_checks,
            primary_keys=dataset.primary_keys,
            required_columns=RECEITA_SILVER_COLUMNS_BY_DATASET[dataset.dataset_id],
        )
        silver_rows += silver_chunk.height
        write_receita_silver(
            silver_chunk,
            receita_silver_root(paths, dataset.dataset_id),
            primary_keys=dataset.primary_keys,
            partition_cols=dataset.partition_keys,
            ref_date_col="ref_date",
        )
    return {"downloads": len(results), "bronze_rows": bronze_rows, "silver_rows": silver_rows}


def _parse_successful_result(
    result: ReceitaDownloadResult,
    *,
    raw_format: str,
) -> pl.DataFrame:
    raw_path = Path(str(result.record.raw_path))
    params = result.record.request_params
    parsed = parse_receita_tabular_file(
        raw_path,
        raw_format=_raw_format_for_path(raw_path, raw_format),
        source_dataset=result.record.dataset_id,
        resource_name=str(params["resource_name"]),
        resource_family=str(params.get("resource_family") or result.record.dataset_id),
        download_timestamp_utc=result.record.download_timestamp_utc,
        sha256=result.record.sha256 or "",
    )
    metadata = manifest_bronze_metadata(result.record)
    return parsed.with_columns([pl.lit(value).alias(column) for column, value in metadata.items()])


def _successful_results(results: list[ReceitaDownloadResult]) -> list[ReceitaDownloadResult]:
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


def _raw_format_for_path(path: Path, configured: str) -> str:
    suffix = path.suffix.lower().lstrip(".")
    if suffix in {"csv", "txt", "zip", "xlsx", "xls", "ods", "pdf"}:
        return suffix
    return configured


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args(argv)
    status = run_receita_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
    )
    print(status)


if __name__ == "__main__":
    main()
