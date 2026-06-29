from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from bralpha.infra.config import load_fred_dataset_registry
from bralpha.infra.http import HttpClient
from bralpha.ingestion.fred.common import (
    FRED_LATEST_SNAPSHOT_REQUEST,
    FRED_VINTAGE_REQUEST,
    FredDownloadResult,
    fred_bronze_root,
    fred_paths,
    fred_silver_root,
    load_fred_series_config,
)
from bralpha.ingestion.fred.downloads import download_fred_series_observations
from bralpha.normalization.fred import (
    FRED_SILVER_COLUMNS,
    normalize_fred_observations_to_silver,
    write_fred_silver,
)
from bralpha.parsing.fred_observations import (
    parse_fred_observations_file,
    write_fred_observations_bronze,
)
from bralpha.quality.checks import run_quality_checks


def run_fred_ingest(
    *,
    repo_root: Path,
    start: date,
    end: date,
    realtime_start: date | None = None,
    realtime_end: date | None = None,
    series_ids: list[str] | None = None,
    api_key: str | None = None,
    client: HttpClient | None = None,
) -> dict[str, int]:
    registry = load_fred_dataset_registry(repo_root)
    dataset = registry.get("fred_series_observations")
    results = download_fred_series_observations(
        repo_root,
        start=start,
        end=end,
        realtime_start=realtime_start,
        realtime_end=realtime_end,
        series_ids=series_ids,
        api_key=api_key,
        client=client,
    )
    frames = [
        parse_fred_observations_file(
            Path(result.record.raw_path),
            series_id=str(result.record.request_params.get("series_id")),
            source_dataset=result.record.dataset_id,
            download_timestamp_utc=result.record.download_timestamp_utc,
            sha256=result.record.sha256 or "",
            vintage_request_mode=_vintage_request_mode(result.record.request_params),
            request_observation_start=result.record.request_params.get("observation_start"),
            request_observation_end=result.record.request_params.get("observation_end"),
            request_realtime_start=result.record.request_params.get("realtime_start"),
            request_realtime_end=result.record.request_params.get("realtime_end"),
            request_vintage_dates=_request_vintage_dates(result.record.request_params),
        )
        for result in _successful_results(results)
    ]
    bronze = _concat(frames)
    paths = fred_paths(repo_root)
    write_fred_observations_bronze(bronze, fred_bronze_root(paths, dataset.dataset_id))

    silver = normalize_fred_observations_to_silver(
        bronze,
        series_config=load_fred_series_config(repo_root),
    )
    silver = _filter_window(silver, start=start, end=end)
    run_quality_checks(
        silver,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=FRED_SILVER_COLUMNS,
    )
    write_fred_silver(
        silver,
        fred_silver_root(paths, dataset.dataset_id),
        primary_keys=dataset.primary_keys,
        partition_cols=dataset.partition_keys,
    )
    return {"downloads": len(results), "bronze_rows": bronze.height, "silver_rows": silver.height}


def _successful_results(results: list[FredDownloadResult]) -> list[FredDownloadResult]:
    return [result for result in results if result.raw_path is not None and result.record.success]


def _concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _filter_window(frame: pl.DataFrame, *, start: date, end: date) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.filter((pl.col("ref_date") >= start) & (pl.col("ref_date") <= end))


def _vintage_request_mode(params: dict[str, object]) -> str:
    if str(params.get("output_type", "")).strip() == "2":
        return FRED_VINTAGE_REQUEST
    return FRED_LATEST_SNAPSHOT_REQUEST


def _request_vintage_dates(params: dict[str, object]) -> list[str] | None:
    value = params.get("vintage_dates")
    if value is None:
        return None
    dates = [item.strip() for item in str(value).split(",") if item.strip()]
    return dates or None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--realtime-start")
    parser.add_argument("--realtime-end")
    parser.add_argument("--series", action="append", dest="series_ids")
    args = parser.parse_args(argv)
    status = run_fred_ingest(
        repo_root=Path(args.repo_root).resolve(),
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        realtime_start=date.fromisoformat(args.realtime_start) if args.realtime_start else None,
        realtime_end=date.fromisoformat(args.realtime_end) if args.realtime_end else None,
        series_ids=args.series_ids,
    )
    print(status)


if __name__ == "__main__":
    main()
