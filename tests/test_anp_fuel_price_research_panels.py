from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.anp.fuel_prices import (
    build_fuel_price_group_observation,
    build_fuel_price_station_observation,
)


def test_fuel_price_station_observation_preserves_values_and_missingness():
    panel = build_fuel_price_station_observation(pl.DataFrame(_price_rows()))

    row = panel.filter(pl.col("observation_id") == "obs-b").row(0, named=True)
    assert row["sale_price"] == 7.0
    assert row["purchase_price"] is None
    assert row["station_cnpj"] == "00.000.000/0002-00"
    assert row["brand"] == "BRANCA"
    assert row["has_sale_price"] is True
    assert row["has_purchase_price"] is False
    assert panel.group_by("observation_id").len().height == panel.height


def test_fuel_price_group_observation_aggregates_allowed_geographies():
    stations = build_fuel_price_station_observation(pl.DataFrame(_price_rows()))

    panel = build_fuel_price_group_observation(
        stations,
        group_by=["all", "region", "state"],
        max_groups=100,
    )

    all_gas = panel.filter(
        (pl.col("group_type") == "all") & (pl.col("product") == "GASOLINA C")
    ).row(0, named=True)
    assert all_gas["group_value"] == "all"
    assert all_gas["feature_id"] == "anp_fuel_price|all|all|gasolina_c"
    assert all_gas["available_date"] == date(2024, 1, 10)
    assert all_gas["sale_price"] == pytest.approx((5.0 + 7.0 + 8.0) / 3)
    assert all_gas["purchase_price"] == 4.0
    assert all_gas["station_count"] == 3
    assert all_gas["sale_price_count"] == 3
    assert all_gas["purchase_price_count"] == 1

    sp_gas = panel.filter(
        (pl.col("group_type") == "state")
        & (pl.col("group_value") == "sp")
        & (pl.col("product") == "GASOLINA C")
    ).row(0, named=True)
    assert sp_gas["sale_price"] == 6.0
    assert sp_gas["station_count"] == 2


def test_fuel_price_group_observation_null_mean_remains_null_when_all_missing():
    stations = build_fuel_price_station_observation(
        pl.DataFrame([_price_row("obs-a", "SP", sale_price=None, purchase_price=None)])
    )

    panel = build_fuel_price_group_observation(stations, group_by=["all"], max_groups=100)
    row = panel.row(0, named=True)

    assert row["sale_price"] is None
    assert row["purchase_price"] is None
    assert row["sale_price_count"] == 0
    assert row["purchase_price_count"] == 0


def test_fuel_price_group_observation_max_groups_guard():
    stations = build_fuel_price_station_observation(
        pl.DataFrame(
            [
                _price_row(f"obs-{index}", f"S{index}", product=f"PROD {index}")
                for index in range(3)
            ]
        )
    )

    with pytest.raises(ValueError, match="max_groups=2"):
        build_fuel_price_group_observation(stations, group_by=["state"], max_groups=2)


def _price_rows() -> list[dict[str, object]]:
    return [
        _price_row("obs-a", "SP", sale_price=5.0, purchase_price=4.0),
        _price_row(
            "obs-b",
            "SP",
            sale_price=7.0,
            purchase_price=None,
            station_cnpj="00.000.000/0002-00",
            available_date=date(2024, 1, 10),
        ),
        _price_row("obs-c", "RJ", sale_price=8.0, purchase_price=None),
    ]


def _price_row(
    observation_id: str,
    state: str,
    *,
    sale_price: float | None = 5.0,
    purchase_price: float | None = 4.0,
    product: str = "GASOLINA C",
    station_cnpj: str = "00.000.000/0001-00",
    available_date: date = date(2024, 1, 9),
) -> dict[str, object]:
    return {
        "observation_id": observation_id,
        "ref_date": date(2024, 1, 2),
        "available_date": available_date,
        "availability_policy": "anp_weekly_price_survey_conservative_7d_next_business_day",
        "region": "Sudeste",
        "state": state,
        "municipality": "Sao Paulo",
        "station_name": "Posto A",
        "station_cnpj": station_cnpj,
        "product": product,
        "sale_price": sale_price,
        "purchase_price": purchase_price,
        "unit": "BRL/l",
        "brand": "BRANCA",
        "resource_family": "ethanol_gasoline_monthly_2023_2025",
        "source_version": "v0",
    }
