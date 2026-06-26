from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from bralpha.infra.config import load_b3_dataset_registry, load_paths_config, resolve_project_paths
from bralpha.ingestion.b3.settlements import download_settlements_range
from bralpha.normalization.b3_market_daily import (
    MARKET_DAILY_COLUMNS,
    normalize_settlements_to_market_daily,
    write_market_daily,
)
from bralpha.parsing.b3_settlements import parse_settlements_file, write_settlements_bronze
from bralpha.quality.checks import run_quality_checks


def run_b3_ingest(
    *,
    repo_root: Path,
    dataset_id: str,
    start: date,
    end: date,
    commodities: list[str] | None = None,
) -> None:
    if dataset_id != "b3_futures_settlements":
        raise ValueError(
            "CLI ingestion is currently implemented for "
            f"b3_futures_settlements: {dataset_id}"
        )

    registry = load_b3_dataset_registry(repo_root)
    dataset = registry.get(dataset_id)
    paths = resolve_project_paths(repo_root, load_paths_config(repo_root))
    results = download_settlements_range(repo_root, start=start, end=end, commodities=commodities)
    bronze_frames = []
    for result in results:
        if result.raw_path is None or not result.record.success:
            continue
        params = result.record.request_params
        data_param = params.get("Data")
        ref = _parse_b3_param_date(data_param) if data_param else start
        commodity = params.get("Mercadoria")
        bronze_frames.append(
            parse_settlements_file(
                result.raw_path,
                ref_date=ref,
                commodity=commodity,
                source_dataset=dataset_id,
                download_timestamp_utc=result.record.download_timestamp_utc,
                sha256=result.record.sha256 or "",
            )
        )
    if not bronze_frames:
        return

    import polars as pl

    bronze = pl.concat(bronze_frames, how="diagonal_relaxed")
    write_settlements_bronze(bronze, paths.bronze / "b3" / dataset_id)
    silver = normalize_settlements_to_market_daily(bronze)
    run_quality_checks(
        silver,
        check_names=dataset.quality_checks,
        primary_keys=dataset.primary_keys,
        required_columns=MARKET_DAILY_COLUMNS,
    )
    write_market_daily(silver, paths.silver / dataset.canonical_table, dataset.primary_keys)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--commodity", action="append", dest="commodities")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args(argv)
    run_b3_ingest(
        repo_root=Path(args.repo_root).resolve(),
        dataset_id=args.dataset,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        commodities=args.commodities,
    )


def _parse_b3_param_date(value: str) -> date:
    day, month, year = value.split("/")
    return date(int(year), int(month), int(day))


if __name__ == "__main__":
    main()
