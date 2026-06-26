from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_bcb_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.bcb.common import (
    BcbDownloadResult,
    bcb_bronze_root,
    bcb_paths,
    bcb_silver_root,
)
from bralpha.ingestion.bcb.focus import download_focus_dataset
from bralpha.ingestion.bcb.ptax import download_ptax_exchange_rates
from bralpha.ingestion.bcb.sgs import download_sgs_series, load_sgs_series_config
from bralpha.normalization.bcb_focus import (
    BCB_FOCUS_EXPECTATION_COLUMNS,
    BCB_FOCUS_REFERENCE_DATE_COLUMNS,
    normalize_focus_expectations_to_silver,
    normalize_focus_reference_dates_to_silver,
    write_focus_expectations_silver,
    write_focus_reference_dates_silver,
)
from bralpha.normalization.bcb_ptax import (
    BCB_PTAX_SILVER_COLUMNS,
    normalize_ptax_to_silver,
    write_ptax_silver,
)
from bralpha.normalization.bcb_sgs import (
    BCB_SGS_SILVER_COLUMNS,
    normalize_sgs_to_silver,
    write_sgs_silver,
)
from bralpha.parsing.bcb_focus import parse_focus_file, write_focus_bronze
from bralpha.parsing.bcb_ptax import parse_ptax_file, write_ptax_bronze
from bralpha.parsing.bcb_sgs import parse_sgs_file, write_sgs_bronze
from bralpha.quality.checks import run_quality_checks

P0_DATASETS = [
    "bcb_sgs_series",
    "bcb_ptax_exchange_rates",
    "bcb_focus_expectations",
    "bcb_focus_top5_expectations",
    "bcb_focus_top5_reference_dates",
]


def run_bcb_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date,
    end: date,
    client: HttpClient | None = None,
    series_ids: list[int] | None = None,
    currencies: list[str] | None = None,
    endpoints: list[str] | None = None,
) -> dict[str, int]:
    if dataset_id not in P0_DATASETS:
        raise ValueError(f"Unsupported BCB ingest dataset: {dataset_id}")
    if dataset_id == "bcb_sgs_series":
        return _run_sgs(repo_root, start, end, client=client, series_ids=series_ids)
    if dataset_id == "bcb_ptax_exchange_rates":
        return _run_ptax(repo_root, start, end, client=client, currencies=currencies)
    return _run_focus(repo_root, dataset_id, start, end, client=client, endpoints=endpoints)


def _run_sgs(
    repo_root: Path,
    start: date,
    end: date,
    *,
    client: HttpClient | None,
    series_ids: list[int] | None,
) -> dict[str, int]:
    results = download_sgs_series(
        repo_root,
        start=start,
        end=end,
        series_ids=series_ids,
        client=client,
    )
    frames = []
    for result in _successful_results(results):
        series_id = int(result.record.request_params["series_id"])
        frames.append(
            parse_sgs_file(
                Path(result.record.raw_path),
                series_id=series_id,
                source_dataset=result.record.dataset_id,
                download_timestamp_utc=result.record.download_timestamp_utc,
                sha256=result.record.sha256 or "",
            )
        )
    bronze = _concat(frames)
    paths = bcb_paths(repo_root)
    write_sgs_bronze(bronze, bcb_bronze_root(paths, "bcb_sgs_series"))
    silver = normalize_sgs_to_silver(bronze, series_config=load_sgs_series_config(repo_root))
    _quality(repo_root, "bcb_sgs_series", silver, BCB_SGS_SILVER_COLUMNS)
    write_sgs_silver(silver, bcb_silver_root(paths, "bcb_sgs_series"))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _run_ptax(
    repo_root: Path,
    start: date,
    end: date,
    *,
    client: HttpClient | None,
    currencies: list[str] | None,
) -> dict[str, int]:
    results = download_ptax_exchange_rates(
        repo_root,
        start=start,
        end=end,
        currencies=currencies,
        client=client,
    )
    frames = []
    for result in _successful_results(results):
        params = result.record.request_params
        frames.append(
            parse_ptax_file(
                Path(result.record.raw_path),
                endpoint=str(params["endpoint"]),
                source_dataset=result.record.dataset_id,
                download_timestamp_utc=result.record.download_timestamp_utc,
                sha256=result.record.sha256 or "",
                currency_code=params.get("currency"),
            )
        )
    bronze = _concat(frames)
    paths = bcb_paths(repo_root)
    write_ptax_bronze(bronze, bcb_bronze_root(paths, "bcb_ptax_exchange_rates"))
    currencies_frame = bronze.filter(pl.col("endpoint") == "Currencies") if bronze.height else None
    silver = normalize_ptax_to_silver(bronze, currencies=currencies_frame)
    _quality(repo_root, "bcb_ptax_exchange_rates", silver, BCB_PTAX_SILVER_COLUMNS)
    write_ptax_silver(silver, bcb_silver_root(paths, "bcb_ptax_exchange_rates"))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _run_focus(
    repo_root: Path,
    dataset_id: str,
    start: date,
    end: date,
    *,
    client: HttpClient | None,
    endpoints: list[str] | None,
) -> dict[str, int]:
    results = download_focus_dataset(
        repo_root,
        dataset_id=dataset_id,
        start=start,
        end=end,
        endpoints=endpoints,
        client=client,
    )
    frames = []
    for result in _successful_results(results):
        params = result.record.request_params
        frames.append(
            parse_focus_file(
                Path(result.record.raw_path),
                endpoint=str(params["endpoint"]),
                source_dataset=result.record.dataset_id,
                download_timestamp_utc=result.record.download_timestamp_utc,
                sha256=result.record.sha256 or "",
            )
        )
    bronze = _concat(frames)
    paths = bcb_paths(repo_root)
    write_focus_bronze(bronze, bcb_bronze_root(paths, dataset_id))
    if dataset_id == "bcb_focus_top5_reference_dates":
        silver = normalize_focus_reference_dates_to_silver(bronze)
        _quality(repo_root, dataset_id, silver, BCB_FOCUS_REFERENCE_DATE_COLUMNS)
        write_focus_reference_dates_silver(silver, bcb_silver_root(paths, dataset_id))
    else:
        silver = normalize_focus_expectations_to_silver(bronze)
        _quality(repo_root, dataset_id, silver, BCB_FOCUS_EXPECTATION_COLUMNS)
        write_focus_expectations_silver(silver, bcb_silver_root(paths, dataset_id))
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _successful_results(results: list[BcbDownloadResult]) -> list[BcbDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _quality(
    repo_root: Path,
    dataset_id: str,
    frame: pl.DataFrame,
    required_columns: list[str],
) -> None:
    dataset = load_bcb_dataset_registry(repo_root).get(dataset_id)
    run_quality_checks(
        frame,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=required_columns,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dataset", choices=P0_DATASETS, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--series-id", type=int, action="append", dest="series_ids")
    parser.add_argument("--currency", action="append", dest="currencies")
    parser.add_argument("--endpoint", action="append", dest="endpoints")
    args = parser.parse_args(argv)
    status = run_bcb_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        series_ids=args.series_ids,
        currencies=args.currencies,
        endpoints=args.endpoints,
    )
    print(status)


if __name__ == "__main__":
    main()
