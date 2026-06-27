from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.fred.observations import (
    build_fred_asof_daily,
    build_fred_observation,
)
from bralpha.ingestion.fred.common import FredSeriesConfig


def test_fred_observation_preserves_fields_and_filters_configured_rows():
    silver = pl.DataFrame(
        [
            _silver_row("DGS10", date(2024, 1, 2), date(2024, 1, 3), 4.0, "4.0", "ok"),
            _silver_row(
                "SP500",
                date(2024, 1, 2),
                date(2024, 1, 3),
                5000.0,
                "5000",
                "ok",
                model_usable=False,
            ),
            _silver_row("DGS30", date(2024, 1, 2), date(2024, 1, 3), 4.5, "4.5", "ok"),
            _silver_row(
                "DGS10",
                date(2024, 1, 3),
                date(2024, 1, 4),
                None,
                ".",
                "missing",
            ),
        ]
    )

    panel = build_fred_observation(
        silver,
        series_config=_series_config(),
        include_model_usable_only=True,
        include_priorities=["P0"],
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    ).sort(["series_id", "ref_date"])

    assert panel["series_id"].to_list() == ["DGS10", "DGS10"]
    assert panel["feature_id"].to_list() == ["fred|dgs10", "fred|dgs10"]
    assert panel["raw_value"].to_list() == ["4.0", "."]
    assert panel["value_status"].to_list() == ["ok", "missing"]
    assert panel["has_value"].to_list() == [True, False]
    assert panel["realtime_start"].to_list() == [date(2024, 1, 2), date(2024, 1, 3)]
    assert "return" not in panel.columns
    assert "spread" not in panel.columns


def test_fred_asof_uses_latest_available_observation_and_pre_window_history():
    observations = build_fred_observation(
        pl.DataFrame(
            [
                _silver_row("DGS10", date(2023, 12, 29), date(2024, 1, 2), 4.0, "4", "ok"),
                _silver_row("DGS10", date(2024, 1, 3), date(2024, 1, 4), 4.1, "4.1", "ok"),
            ]
        ),
        series_config=_series_config(),
        include_model_usable_only=True,
        include_priorities=["P0"],
        start=None,
        end=date(2024, 1, 5),
    )

    panel = build_fred_asof_daily(
        observations,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
        max_dense_series=5000,
    ).sort("ref_date")

    assert panel["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert panel["available_date"].to_list() == panel["ref_date"].to_list()
    assert panel["value"].to_list() == [4.0, 4.0, 4.1, 4.1]
    assert panel["observation_ref_date"].to_list() == [
        date(2023, 12, 29),
        date(2023, 12, 29),
        date(2024, 1, 3),
        date(2024, 1, 3),
    ]
    assert panel["staleness_days"].to_list() == [0, 1, 0, 1]
    assert panel.filter(pl.col("ref_date") == date(2024, 1, 1)).is_empty()
    assert panel.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()


def test_fred_asof_preserves_latest_missing_instead_of_older_numeric_value():
    observations = build_fred_observation(
        pl.DataFrame(
            [
                _silver_row("DGS10", date(2024, 1, 1), date(2024, 1, 2), 4.0, "4", "ok"),
                _silver_row(
                    "DGS10",
                    date(2024, 1, 3),
                    date(2024, 1, 4),
                    None,
                    ".",
                    "missing",
                ),
            ]
        ),
        series_config=_series_config(),
        include_model_usable_only=True,
        include_priorities=["P0"],
    )

    panel = build_fred_asof_daily(
        observations,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        max_dense_series=5000,
    ).sort("ref_date")

    latest = panel.filter(pl.col("ref_date") == date(2024, 1, 4)).row(0, named=True)
    assert latest["value"] is None
    assert latest["raw_value"] == "."
    assert latest["value_status"] == "missing"
    assert latest["has_value"] is False
    assert panel.filter(pl.col("ref_date") == date(2024, 1, 5))["value"].item() is None


def test_fred_asof_carries_monthly_series_with_staleness():
    observations = build_fred_observation(
        pl.DataFrame(
            [
                _silver_row(
                    "PCOPPUSDM",
                    date(2023, 12, 31),
                    date(2024, 1, 2),
                    8400.0,
                    "8400",
                    "ok",
                    frequency="monthly",
                    unit="usd_per_metric_ton",
                )
            ]
        ),
        series_config=_series_config(),
        include_model_usable_only=True,
        include_priorities=["P1"],
    )

    panel = build_fred_asof_daily(
        observations,
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        max_dense_series=5000,
    ).sort("ref_date")

    assert panel["value"].to_list() == [8400.0, 8400.0, 8400.0, 8400.0]
    assert panel["staleness_days"].to_list() == [0, 1, 2, 3]


def test_fred_asof_max_dense_series_raises():
    observations = build_fred_observation(
        pl.DataFrame(
            [
                _silver_row("DGS10", date(2024, 1, 1), date(2024, 1, 2), 4.0, "4", "ok"),
                _silver_row("SP500", date(2024, 1, 1), date(2024, 1, 2), 5000.0, "5000", "ok"),
            ]
        ),
        series_config=_series_config(),
        include_model_usable_only=True,
        include_priorities=["P0"],
    )

    with pytest.raises(ValueError, match="max_dense_series=1"):
        build_fred_asof_daily(
            observations,
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
            max_dense_series=1,
        )


def _silver_row(
    series_id: str,
    ref_date: date,
    available_date: date,
    value: float | None,
    raw_value: str,
    value_status: str,
    *,
    frequency: str = "daily",
    unit: str = "percent",
    model_usable: bool = True,
) -> dict[str, object]:
    return {
        "series_id": series_id,
        "series_name": series_id,
        "category": "commodity_proxy" if series_id == "PCOPPUSDM" else "treasury_nominal",
        "frequency": frequency,
        "unit": unit,
        "ref_date": ref_date,
        "available_date": available_date,
        "availability_policy": "date_only_next_business_day",
        "value": value,
        "raw_value": raw_value,
        "value_status": value_status,
        "realtime_start": ref_date,
        "realtime_end": ref_date,
        "model_usable": model_usable,
        "source_version": "v0",
    }


def _series_config() -> list[FredSeriesConfig]:
    return [
        FredSeriesConfig(
            series_id="DGS10",
            name="10Y",
            category="treasury_nominal",
            frequency="daily",
            unit="percent",
            source="FRED",
            priority="P0",
            model_usable=True,
            availability_policy="date_only_next_business_day",
            notes="",
        ),
        FredSeriesConfig(
            series_id="SP500",
            name="S&P 500",
            category="equity_volatility_proxy",
            frequency="daily",
            unit="index",
            source="FRED",
            priority="P0",
            model_usable=True,
            availability_policy="date_only_next_business_day",
            notes="limited history",
        ),
        FredSeriesConfig(
            series_id="PCOPPUSDM",
            name="Copper",
            category="commodity_proxy",
            frequency="monthly",
            unit="usd_per_metric_ton",
            source="FRED",
            priority="P1",
            model_usable=True,
            availability_policy="date_only_next_business_day",
            notes="monthly IMF series",
        ),
        FredSeriesConfig(
            series_id="DGS30",
            name="30Y",
            category="treasury_nominal",
            frequency="daily",
            unit="percent",
            source="FRED",
            priority="P2",
            model_usable=True,
            availability_policy="date_only_next_business_day",
            notes="deferred priority for test",
        ),
    ]
