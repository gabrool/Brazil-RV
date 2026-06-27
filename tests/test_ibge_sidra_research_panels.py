from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from bralpha.derived.ibge.sidra import build_sidra_asof_daily, build_sidra_observation
from bralpha.infra.config import load_ibge_research_config
from bralpha.ingestion.ibge.sidra import SidraSeriesConfig, load_sidra_series_config

BANNED_FEATURE_COLUMNS = {
    "rolling_mean",
    "zscore",
    "rolling_vol",
    "rolling_corr",
    "pca",
    "surprise",
    "revision",
    "breadth",
    "stationarity",
    "real_rate",
}


def test_sidra_observation_filters_scope_and_preserves_source_context(repo_root):
    config = load_ibge_research_config(repo_root).ibge_research
    panel = build_sidra_observation(
        pl.DataFrame(
            [
                _silver_row(dataset_slug="ipca", model_usable=True, value=0.42),
                _silver_row(dataset_slug="ipca", period_code="202402", model_usable=False),
                _silver_row(dataset_slug="ipp_producer_prices", model_usable=True),
            ]
        ),
        series_config=load_sidra_series_config(repo_root),
        include_model_usable_only=config.sidra.include_model_usable_only,
        include_priorities=config.sidra.include_priorities,
        selected_dataset_slugs=config.sidra.selected_dataset_slugs,
    )

    row = panel.row(0, named=True)
    assert panel.height == 1
    assert row["dataset_slug"] == "ipca"
    assert row["frequency"] == "monthly"
    assert row["value"] == 0.42
    assert row["raw_value"] == "0.42"
    assert row["value_status"] == "ok"
    assert row["unit"] == "%"
    assert row["geography_id"] == "1"
    assert row["classification_key"] == "315=7169"
    assert row["has_value"] is True
    assert BANNED_FEATURE_COLUMNS.isdisjoint(panel.columns)


def test_sidra_asof_uses_latest_available_observation_and_keeps_withheld_latest():
    observations = build_sidra_observation(
        pl.DataFrame(
            [
                _silver_row(
                    period_code="202312",
                    ref_period_start=date(2023, 12, 1),
                    ref_period_end=date(2024, 1, 1),
                    ref_date=date(2024, 1, 1),
                    available_date=date(2024, 1, 2),
                    value=1.0,
                    raw_value="1.0",
                    value_status="ok",
                ),
                _silver_row(
                    period_code="202401",
                    ref_period_start=date(2024, 1, 1),
                    ref_period_end=date(2024, 1, 5),
                    ref_date=date(2024, 1, 5),
                    available_date=date(2024, 1, 5),
                    value=None,
                    raw_value="X",
                    value_status="withheld",
                ),
            ]
        ),
        series_config=[_series()],
        include_model_usable_only=True,
        include_priorities=["P0"],
        selected_dataset_slugs=["ipca"],
    )

    asof = build_sidra_asof_daily(
        observations,
        start=date(2024, 1, 1),
        end=date(2024, 1, 8),
        max_dense_features=10,
    ).sort("ref_date")

    assert asof["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ]
    assert asof["value"].to_list()[:3] == [1.0, 1.0, 1.0]
    assert asof["value"].to_list()[3:] == [None, None]
    assert asof["value_status"].to_list()[3:] == ["withheld", "withheld"]
    assert asof["has_value"].to_list()[3:] == [False, False]
    assert asof["staleness_days"].to_list() == [0, 1, 2, 0, 3]
    assert asof["available_date"].to_list() == asof["ref_date"].to_list()
    assert (
        asof.filter(pl.col("observation_available_date") > pl.col("ref_date")).height == 0
    )
    assert BANNED_FEATURE_COLUMNS.isdisjoint(asof.columns)


def test_sidra_asof_uses_pre_window_history_at_output_start():
    observations = build_sidra_observation(
        pl.DataFrame(
            [
                _silver_row(
                    ref_date=date(2023, 12, 31),
                    available_date=date(2024, 1, 2),
                    value=1.0,
                )
            ]
        ),
        series_config=[_series()],
        include_model_usable_only=True,
        include_priorities=["P0"],
        selected_dataset_slugs=["ipca"],
    )

    asof = build_sidra_asof_daily(
        observations,
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
        max_dense_features=10,
    )

    row = asof.row(0, named=True)
    assert row["ref_date"] == date(2024, 1, 3)
    assert row["observation_ref_date"] == date(2023, 12, 31)
    assert row["observation_available_date"] == date(2024, 1, 2)
    assert row["value"] == 1.0


def test_sidra_asof_raises_when_feature_count_exceeds_limit():
    observations = build_sidra_observation(
        pl.DataFrame(
            [
                _silver_row(variable_id="63", value=1.0),
                _silver_row(variable_id="64", value=2.0),
            ]
        ),
        series_config=[_series()],
        include_model_usable_only=True,
        include_priorities=["P0"],
        selected_dataset_slugs=["ipca"],
    )

    with pytest.raises(ValueError, match="max_dense_features"):
        build_sidra_asof_daily(
            observations,
            start=date(2024, 2, 9),
            end=date(2024, 2, 9),
            max_dense_features=1,
        )


def _series() -> SidraSeriesConfig:
    return SidraSeriesConfig(
        dataset_slug="ipca",
        priority="P0",
        aggregate_id=7060,
        table_name="IPCA",
        survey_code="snipc",
        frequency="monthly",
        period_selector="date_range",
        variables="all",
        locations="N1[all]",
        classifications="315[all]",
        view="",
        model_usable=True,
        release_calendar_product_id=9256,
        release_calendar_product_id_status="verified",
        availability_policy="calendar_or_date_only_next_business_day",
    )


def _silver_row(
    *,
    dataset_slug: str = "ipca",
    aggregate_id: int = 7060,
    variable_id: str = "63",
    period_code: str = "202401",
    ref_period_start: date = date(2024, 1, 1),
    ref_period_end: date = date(2024, 1, 31),
    ref_date: date = date(2024, 1, 31),
    available_date: date = date(2024, 2, 9),
    model_usable: bool = True,
    value: float | None = 0.42,
    raw_value: str = "0.42",
    value_status: str = "ok",
) -> dict[str, object]:
    return {
        "dataset_slug": dataset_slug,
        "aggregate_id": aggregate_id,
        "variable_id": variable_id,
        "variable_name": "IPCA monthly variation",
        "unit": "%",
        "period_code": period_code,
        "period_label": period_code,
        "ref_period_start": ref_period_start,
        "ref_period_end": ref_period_end,
        "ref_date": ref_date,
        "release_date": available_date,
        "available_datetime_local": datetime(2024, 2, 9, 9),
        "available_datetime_utc": datetime(2024, 2, 9, 12),
        "available_date": available_date,
        "availability_policy": "exact_timestamp_cutoff",
        "availability_note": None,
        "model_usable": model_usable,
        "geography_level": "N1",
        "geography_id": "1",
        "geography_name": "Brasil",
        "classification_key": "315=7169",
        "classifications_json": "[]",
        "value": value,
        "raw_value": raw_value,
        "value_status": value_status,
        "source": "ibge",
        "source_dataset": "ibge_sidra_series",
        "download_timestamp_utc": datetime(2024, 2, 9, 12),
        "raw_path": "raw.json",
        "sha256": "abc",
        "source_version": "v0",
    }
