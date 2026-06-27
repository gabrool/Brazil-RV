from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.ingestion.fred.common import FredSeriesConfig
from bralpha.normalization.fred import (
    FRED_SILVER_COLUMNS,
    normalize_fred_observations_to_silver,
    write_fred_silver,
)
from bralpha.parsing.fred_observations import parse_fred_observations_bytes


def test_fred_normalizer_joins_metadata_and_maps_values(repo_root):
    bronze = _bronze(repo_root)

    silver = normalize_fred_observations_to_silver(
        bronze,
        series_config=[_series_config()],
    )

    assert silver.columns == FRED_SILVER_COLUMNS
    first = silver.row(0, named=True)
    second = silver.row(1, named=True)
    assert first["series_name"] == "10-Year Treasury"
    assert first["category"] == "treasury_nominal"
    assert first["value"] == 4.25
    assert first["value_status"] == "ok"
    assert first["available_date"] == date(2024, 1, 3)
    assert first["available_date"] != date(2024, 1, 10)
    assert second["value"] is None
    assert second["raw_value"] == "."
    assert second["value_status"] == "missing"
    assert second["realtime_start"] == date(2024, 1, 5)
    assert second["model_usable"] is True


def test_fred_silver_writer_partitions_and_upserts_primary_key(repo_root, tmp_path):
    silver = normalize_fred_observations_to_silver(
        _bronze(repo_root),
        series_config=[_series_config()],
    )

    paths = write_fred_silver(
        silver,
        tmp_path,
        primary_keys=["series_id", "ref_date"],
        partition_cols=["series_id", "year"],
    )
    write_fred_silver(
        silver,
        tmp_path,
        primary_keys=["series_id", "ref_date"],
        partition_cols=["series_id", "year"],
    )
    written = pl.read_parquet(paths[0])

    assert paths[0].parent == tmp_path / "series_id=DGS10" / "year=2024"
    assert written.group_by(["series_id", "ref_date"]).len().height == 2
    assert written.height == 2


def _bronze(repo_root) -> pl.DataFrame:
    return parse_fred_observations_bytes(
        b"""
        {
          "realtime_start": "2024-01-04",
          "realtime_end": "2024-01-04",
          "observation_start": "2024-01-02",
          "observation_end": "2024-01-03",
          "units": "lin",
          "output_type": 1,
          "file_type": "json",
          "order_by": "observation_date",
          "sort_order": "asc",
          "count": 2,
          "offset": 0,
          "limit": 100000,
          "observations": [
            {
              "realtime_start": "2024-01-04",
              "realtime_end": "2024-01-04",
              "date": "2024-01-02",
              "value": "4.25"
            },
            {
              "realtime_start": "2024-01-05",
              "realtime_end": "2024-01-05",
              "date": "2024-01-03",
              "value": "."
            }
          ]
        }
        """,
        series_id="DGS10",
        source_dataset="fred_series_observations",
        download_timestamp_utc=datetime(2024, 1, 10, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )


def _series_config() -> FredSeriesConfig:
    return FredSeriesConfig(
        series_id="DGS10",
        name="10-Year Treasury",
        category="treasury_nominal",
        frequency="daily",
        unit="percent",
        source="FRED / Board of Governors",
        priority="P0",
        model_usable=True,
        availability_policy="date_only_next_business_day",
        notes="test",
    )
