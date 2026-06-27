from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.parsing.fred_observations import (
    FRED_OBSERVATIONS_BRONZE_COLUMNS,
    parse_fred_observations_bytes,
    write_fred_observations_bronze,
)

FRED_JSON = b"""
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
"""


def test_fred_parser_preserves_observations_and_metadata(repo_root):
    bronze = _bronze(repo_root)

    assert bronze.columns == FRED_OBSERVATIONS_BRONZE_COLUMNS
    assert bronze["series_id"].to_list() == ["DGS10", "DGS10"]
    assert bronze["ref_date"].to_list() == [date(2024, 1, 2), date(2024, 1, 3)]
    assert bronze["raw_value"].to_list() == ["4.25", "."]
    assert bronze["realtime_start"].to_list() == ["2024-01-04", "2024-01-05"]
    assert bronze["count"].to_list() == [2, 2]
    assert bronze["limit"].to_list() == [100000, 100000]


def test_fred_bronze_writer_partitions_by_series_and_year(repo_root, tmp_path):
    bronze = _bronze(repo_root)

    paths = write_fred_observations_bronze(bronze, tmp_path)
    write_fred_observations_bronze(bronze, tmp_path)
    written = pl.read_parquet(paths[0])

    assert paths[0].parent == tmp_path / "series_id=DGS10" / "year=2024"
    assert written.height == 2


def _bronze(repo_root) -> pl.DataFrame:
    return parse_fred_observations_bytes(
        FRED_JSON,
        series_id="DGS10",
        source_dataset="fred_series_observations",
        download_timestamp_utc=datetime(2024, 1, 10, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )
