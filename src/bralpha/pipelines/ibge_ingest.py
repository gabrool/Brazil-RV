from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_ibge_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.ibge.calendar import download_release_calendar
from bralpha.ingestion.ibge.common import (
    IbgeDownloadResult,
    ibge_bronze_root,
    ibge_paths,
    ibge_silver_root,
)
from bralpha.ingestion.ibge.news import download_news_metadata
from bralpha.ingestion.ibge.products import download_products_metadata
from bralpha.ingestion.ibge.sidra import download_sidra_series, load_sidra_series_config
from bralpha.normalization.ibge_calendar import (
    IBGE_CALENDAR_SILVER_COLUMNS,
    normalize_calendar_to_silver,
    write_calendar_silver,
)
from bralpha.normalization.ibge_news import (
    IBGE_NEWS_SILVER_COLUMNS,
    normalize_news_to_silver,
    write_news_silver,
)
from bralpha.normalization.ibge_products import (
    IBGE_PRODUCTS_SILVER_COLUMNS,
    normalize_products_to_silver,
    write_products_silver,
)
from bralpha.normalization.ibge_sidra import (
    IBGE_SIDRA_SILVER_COLUMNS,
    normalize_sidra_to_silver,
    write_sidra_silver,
)
from bralpha.parsing.ibge_calendar import parse_calendar_file, write_calendar_bronze
from bralpha.parsing.ibge_news import parse_news_file, write_news_bronze
from bralpha.parsing.ibge_products import parse_products_file, write_products_bronze
from bralpha.parsing.ibge_sidra import parse_sidra_file, write_sidra_bronze
from bralpha.quality.checks import run_quality_checks

SUPPORTED_DATASETS = [
    "ibge_sidra_series",
    "ibge_release_calendar",
    "ibge_products_metadata",
    "ibge_news_releases_metadata",
]


def run_ibge_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date | None = None,
    end: date | None = None,
    client: HttpClient | None = None,
    priority: list[str] | None = None,
    dataset_slugs: list[str] | None = None,
    aggregate_ids: list[int] | None = None,
    product_id: int | None = None,
    tipo: str = "release",
) -> dict[str, int]:
    if dataset_id not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported IBGE ingest dataset: {dataset_id}")
    if dataset_id == "ibge_products_metadata":
        return _run_products(repo_root, client=client)
    if start is None or end is None:
        raise ValueError(f"{dataset_id} requires start and end dates")
    if dataset_id == "ibge_sidra_series":
        return _run_sidra(
            repo_root,
            start,
            end,
            client=client,
            priority=priority,
            dataset_slugs=dataset_slugs,
            aggregate_ids=aggregate_ids,
        )
    if dataset_id == "ibge_release_calendar":
        return _run_calendar(repo_root, start, end, client=client, product_id=product_id)
    return _run_news(repo_root, start, end, client=client, product_id=product_id, tipo=tipo)


def _run_sidra(
    repo_root: Path,
    start: date,
    end: date,
    *,
    client: HttpClient | None,
    priority: list[str] | None,
    dataset_slugs: list[str] | None,
    aggregate_ids: list[int] | None,
) -> dict[str, int]:
    results = download_sidra_series(
        repo_root,
        start=start,
        end=end,
        priority=priority,
        dataset_slugs=dataset_slugs,
        aggregate_ids=aggregate_ids,
        client=client,
    )
    frames = []
    for result in _successful_results(results):
        params = result.record.request_params
        frames.append(
            parse_sidra_file(
                Path(result.record.raw_path),
                dataset_slug=str(params["dataset_slug"]),
                aggregate_id=int(params["aggregate_id"]),
                source_dataset=result.record.dataset_id,
                download_timestamp_utc=result.record.download_timestamp_utc,
                sha256=result.record.sha256 or "",
            )
        )
    bronze = _concat(frames)
    paths = ibge_paths(repo_root)
    write_sidra_bronze(bronze, ibge_bronze_root(paths, "ibge_sidra_series"))
    silver = normalize_sidra_to_silver(
        bronze,
        series_config=load_sidra_series_config(repo_root),
        release_calendar=_read_parquet_root(ibge_silver_root(paths, "ibge_release_calendar")),
    )
    _quality(repo_root, "ibge_sidra_series", silver, IBGE_SIDRA_SILVER_COLUMNS)
    write_sidra_silver(silver, ibge_silver_root(paths, "ibge_sidra_series"))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _run_calendar(
    repo_root: Path,
    start: date,
    end: date,
    *,
    client: HttpClient | None,
    product_id: int | None,
) -> dict[str, int]:
    results = download_release_calendar(
        repo_root,
        start=start,
        end=end,
        product_id=product_id,
        client=client,
    )
    frames = [
        parse_calendar_file(
            Path(result.record.raw_path),
            source_dataset=result.record.dataset_id,
            download_timestamp_utc=result.record.download_timestamp_utc,
            sha256=result.record.sha256 or "",
        )
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = ibge_paths(repo_root)
    write_calendar_bronze(bronze, ibge_bronze_root(paths, "ibge_release_calendar"))
    silver = normalize_calendar_to_silver(bronze)
    _quality(repo_root, "ibge_release_calendar", silver, IBGE_CALENDAR_SILVER_COLUMNS)
    write_calendar_silver(silver, ibge_silver_root(paths, "ibge_release_calendar"))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _run_products(repo_root: Path, *, client: HttpClient | None) -> dict[str, int]:
    results = download_products_metadata(repo_root, client=client)
    frames = [
        parse_products_file(
            Path(result.record.raw_path),
            source_dataset=result.record.dataset_id,
            download_timestamp_utc=result.record.download_timestamp_utc,
            sha256=result.record.sha256 or "",
        )
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = ibge_paths(repo_root)
    write_products_bronze(bronze, ibge_bronze_root(paths, "ibge_products_metadata"))
    silver = normalize_products_to_silver(bronze)
    _quality(repo_root, "ibge_products_metadata", silver, IBGE_PRODUCTS_SILVER_COLUMNS)
    write_products_silver(silver, ibge_silver_root(paths, "ibge_products_metadata"))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _run_news(
    repo_root: Path,
    start: date,
    end: date,
    *,
    client: HttpClient | None,
    product_id: int | None,
    tipo: str,
) -> dict[str, int]:
    results = download_news_metadata(
        repo_root,
        start=start,
        end=end,
        product_id=product_id,
        tipo=tipo,
        client=client,
    )
    frames = [
        parse_news_file(
            Path(result.record.raw_path),
            source_dataset=result.record.dataset_id,
            download_timestamp_utc=result.record.download_timestamp_utc,
            sha256=result.record.sha256 or "",
        )
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = ibge_paths(repo_root)
    write_news_bronze(bronze, ibge_bronze_root(paths, "ibge_news_releases_metadata"))
    silver = normalize_news_to_silver(bronze)
    _quality(repo_root, "ibge_news_releases_metadata", silver, IBGE_NEWS_SILVER_COLUMNS)
    write_news_silver(silver, ibge_silver_root(paths, "ibge_news_releases_metadata"))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _successful_results(results: list[IbgeDownloadResult]) -> list[IbgeDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _read_parquet_root(root: Path) -> pl.DataFrame | None:
    if not root.exists():
        return None
    paths = sorted(root.glob("**/*.parquet"))
    if not paths:
        return None
    return pl.read_parquet(paths)


def _quality(
    repo_root: Path,
    dataset_id: str,
    frame: pl.DataFrame,
    required_columns: list[str],
) -> None:
    dataset = load_ibge_dataset_registry(repo_root).get(dataset_id)
    run_quality_checks(
        frame,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=required_columns,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", choices=SUPPORTED_DATASETS, required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--priority", action="append")
    parser.add_argument("--series-slug", action="append", dest="dataset_slugs")
    parser.add_argument("--aggregate-id", type=int, action="append", dest="aggregate_ids")
    parser.add_argument("--product-id", type=int)
    parser.add_argument("--tipo", default="release")
    args = parser.parse_args(argv)
    status = run_ibge_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
        priority=args.priority,
        dataset_slugs=args.dataset_slugs,
        aggregate_ids=args.aggregate_ids,
        product_id=args.product_id,
        tipo=args.tipo,
    )
    print(status)


if __name__ == "__main__":
    main()
