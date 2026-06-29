from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from bralpha.ingestion.fred.common import FRED_VINTAGE_REQUEST, FredSeriesConfig
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
    assert first["vintage_date"] == date(2024, 1, 4)
    assert first["vintage_id"].startswith("fred:fred_series_observations:")
    assert first["available_date"] == date(2024, 1, 3)
    assert first["availability_basis"] == "source_date_only"
    assert first["vintage_policy"] == "latest_snapshot_allowed"
    assert first["vintage_request_mode"] == "latest_snapshot"
    assert first["revision_policy"] == "unrevised"
    assert first["first_seen_timestamp_utc"] == datetime(2024, 1, 10, 12)
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
        primary_keys=["series_id", "ref_date", "vintage_date"],
        partition_cols=["series_id", "year"],
    )
    write_fred_silver(
        silver,
        tmp_path,
        primary_keys=["series_id", "ref_date", "vintage_date"],
        partition_cols=["series_id", "year"],
    )
    written = pl.read_parquet(paths[0])

    assert paths[0].parent == tmp_path / "series_id=DGS10" / "year=2024"
    assert written.group_by(["series_id", "ref_date", "vintage_date"]).len().height == 2
    assert written.height == 2


def test_revised_fred_current_snapshot_is_not_model_usable(repo_root):
    silver = normalize_fred_observations_to_silver(
        _revised_bronze(repo_root, vintages=[("2024-02-15", "8400")]),
        series_config=[_revised_series_config()],
    )
    row = silver.row(0, named=True)

    assert row["available_date"] == date(2024, 2, 16)
    assert row["availability_basis"] == "current_snapshot_no_vintage"
    assert row["revision_policy"] == "revised_use_vintages"
    assert row["model_usable"] is False


def test_revised_fred_multiple_vintages_are_model_usable(repo_root):
    silver = normalize_fred_observations_to_silver(
        _revised_bronze(
            repo_root,
            vintages=[("2024-02-15", "8400"), ("2024-03-15", "8450")],
            vintage_request_mode=FRED_VINTAGE_REQUEST,
        ),
        series_config=[_revised_series_config()],
    ).sort("available_date")

    assert silver["value"].to_list() == [8400.0, 8450.0]
    assert silver["available_date"].to_list() == [date(2024, 2, 16), date(2024, 3, 18)]
    assert silver["availability_basis"].to_list() == [FRED_VINTAGE_REQUEST, FRED_VINTAGE_REQUEST]
    assert silver["model_usable"].to_list() == [True, True]


def test_revised_fred_single_true_vintage_is_model_usable(repo_root):
    silver = normalize_fred_observations_to_silver(
        _revised_bronze(
            repo_root,
            vintages=[("2024-02-15", "8400")],
            vintage_request_mode=FRED_VINTAGE_REQUEST,
        ),
        series_config=[_revised_series_config()],
    )
    row = silver.row(0, named=True)

    assert row["availability_basis"] == FRED_VINTAGE_REQUEST
    assert row["model_usable"] is True


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


def _revised_series_config() -> FredSeriesConfig:
    return FredSeriesConfig(
        series_id="PCOPPUSDM",
        name="Copper",
        category="commodity_proxy",
        frequency="monthly",
        unit="usd_per_metric_ton",
        source="FRED / IMF",
        priority="P1",
        model_usable=True,
        availability_policy="date_only_next_business_day",
        series_kind="revised_macro",
        vintage_policy="fred_realtime_vintages_required",
        vintage_request_mode=FRED_VINTAGE_REQUEST,
        model_usable_without_vintage=False,
        notes="test",
    )


def _revised_bronze(
    repo_root,
    *,
    vintages: list[tuple[str, str]],
    vintage_request_mode: str = "latest_snapshot",
) -> pl.DataFrame:
    observations = ",\n".join(
        f"""
            {{
              "realtime_start": "{vintage_date}",
              "realtime_end": "{vintage_date}",
              "date": "2024-01-31",
              "value": "{value}"
            }}"""
        for vintage_date, value in vintages
    )
    return parse_fred_observations_bytes(
        f"""
        {{
          "realtime_start": "2024-03-15",
          "realtime_end": "2024-03-15",
          "observation_start": "2024-01-31",
          "observation_end": "2024-01-31",
          "units": "lin",
          "output_type": 1,
          "file_type": "json",
          "order_by": "observation_date",
          "sort_order": "asc",
          "count": {len(vintages)},
          "offset": 0,
          "limit": 100000,
          "observations": [{observations}
          ]
        }}
        """.encode(),
        series_id="PCOPPUSDM",
        source_dataset="fred_series_observations",
        download_timestamp_utc=datetime(2024, 3, 20, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="revised",
        vintage_request_mode=vintage_request_mode,
    )
