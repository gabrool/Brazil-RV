from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.anp.daily_long import build_anp_daily_long, build_anp_state_asof_daily
from bralpha.derived.anp.schemas import PANEL_PRIMARY_KEYS


def test_anp_state_asof_uses_pre_window_history_and_latest_available_missing_values():
    prices = pl.DataFrame(
        {
            "ref_date": [date(2023, 12, 29), date(2024, 1, 3)],
            "available_date": [date(2024, 1, 2), date(2024, 1, 4)],
            "availability_policy": [
                "anp_weekly_price_survey_conservative_7d_next_business_day",
                "anp_weekly_price_survey_conservative_7d_next_business_day",
            ],
            "group_type": ["all", "all"],
            "group_value": ["all", "all"],
            "product": ["GASOLINA C", "GASOLINA C"],
            "feature_id": ["anp_fuel_price|all|all|gasolina_c"] * 2,
            "sale_price": [5.0, None],
            "purchase_price": [4.0, None],
            "station_count": [2, 2],
            "sale_price_count": [2, 0],
            "purchase_price_count": [2, 0],
            "unit": ["BRL/l", "BRL/l"],
            "source_version": ["v0", "v0"],
        }
    )

    state = build_anp_state_asof_daily(
        fuel_prices=prices,
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
        max_features=100,
    )
    sale = state.filter(pl.col("value_name") == "sale_price").sort("ref_date")

    assert sale["ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert sale["value"].to_list() == [5.0, 5.0, None, None]
    assert sale["observation_ref_date"].to_list() == [
        date(2023, 12, 29),
        date(2023, 12, 29),
        date(2024, 1, 3),
        date(2024, 1, 3),
    ]
    assert sale["staleness_days"].to_list() == [0, 1, 0, 1]
    assert state.filter(pl.col("ref_date") == date(2024, 1, 1)).is_empty()
    assert state.filter(pl.col("observation_available_date") > pl.col("ref_date")).is_empty()


def test_anp_state_asof_carries_monthly_sales_and_oil_gas_with_staleness():
    sales = _sales_group()
    oil = _oil_group()

    state = build_anp_state_asof_daily(
        fuel_sales=sales,
        oil_gas=oil,
        start=date(2024, 2, 1),
        end=date(2024, 2, 5),
        max_features=100,
    ).sort(["source_family", "value_name", "ref_date"])

    sales_value = state.filter(
        (pl.col("source_family") == "anp_fuel_sales")
        & (pl.col("value_name") == "sales_volume_m3")
    ).sort("ref_date")
    assert sales_value["value"].to_list() == [100.0, 100.0, 100.0]
    assert sales_value["staleness_days"].to_list() == [0, 1, 4]

    oil_value = state.filter(
        (pl.col("source_family") == "anp_oil_gas") & (pl.col("value_name") == "metric_value")
    ).sort("ref_date")
    assert oil_value["value"].to_list() == [10.0, 10.0, 10.0]
    assert oil_value["staleness_days"].to_list() == [0, 1, 4]


def test_anp_state_asof_max_feature_guard():
    with pytest.raises(ValueError, match="max_features=1"):
        build_anp_state_asof_daily(
            fuel_prices=_price_group(),
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
            max_features=1,
        )


def test_anp_daily_long_includes_group_families_drops_null_values_and_keeps_long_pk():
    state = build_anp_state_asof_daily(
        fuel_prices=_price_group(),
        fuel_sales=_sales_group(),
        oil_gas=_oil_group(),
        start=date(2024, 2, 1),
        end=date(2024, 2, 2),
        max_features=100,
    )

    daily_long = build_anp_daily_long(
        state_asof_daily=state,
        include_fuel_prices=True,
        include_fuel_sales=True,
        include_oil_gas=True,
    )

    assert daily_long.filter(pl.col("value").is_null()).is_empty()
    assert daily_long.group_by(PANEL_PRIMARY_KEYS["daily_long"]).len().height == daily_long.height
    assert set(daily_long["source_family"].unique().to_list()) == {
        "anp_fuel_price",
        "anp_fuel_sales",
        "anp_oil_gas",
    }
    assert "station_cnpj" not in daily_long.columns
    assert "municipality" not in daily_long.columns


def _price_group() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 30)],
            "available_date": [date(2024, 2, 1)],
            "availability_policy": ["anp_weekly_price_survey_conservative_7d_next_business_day"],
            "group_type": ["all"],
            "group_value": ["all"],
            "product": ["GASOLINA C"],
            "feature_id": ["anp_fuel_price|all|all|gasolina_c"],
            "sale_price": [5.0],
            "purchase_price": [None],
            "station_count": [2],
            "sale_price_count": [2],
            "purchase_price_count": [0],
            "unit": ["BRL/l"],
            "source_version": ["v0"],
        }
    )


def _sales_group() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 31)],
            "available_date": [date(2024, 2, 1)],
            "availability_policy": ["anp_monthly_next_month_end_next_business_day"],
            "group_type": ["all"],
            "group_value": ["all"],
            "product": ["GASOLINA C"],
            "feature_id": ["anp_fuel_sales|all|all|gasolina_c"],
            "sales_volume_m3": [100.0],
            "sales_volume_count": [1],
            "state_count": [1],
            "unit": ["m3"],
            "source_version": ["v0"],
        }
    )


def _oil_group() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 31)],
            "available_date": [date(2024, 2, 1)],
            "availability_policy": ["anp_monthly_next_month_end_next_business_day"],
            "group_type": ["all"],
            "group_value": ["all"],
            "location": ["Mar"],
            "product": ["Petroleo"],
            "metric_type": ["petroleum_production"],
            "feature_id": ["anp_oil_gas|all|all|mar|petroleo|petroleum_production"],
            "metric_value": [10.0],
            "metric_value_count": [1],
            "state_count": [1],
            "unit": ["m3"],
            "source_version": ["v0"],
        }
    )
