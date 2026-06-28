from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.anp.fuel_sales import (
    build_fuel_sales_group_observation,
    build_fuel_sales_observation,
)


def test_fuel_sales_observation_preserves_monthly_values_and_missingness():
    panel = build_fuel_sales_observation(pl.DataFrame(_sales_rows()))

    row = panel.filter(pl.col("state") == "RJ").row(0, named=True)
    assert row["sales_volume_m3"] is None
    assert row["has_sales_volume_m3"] is False
    assert row["unit"] == "m3"
    assert panel.group_by(["ref_date", "state", "product"]).len().height == panel.height


def test_fuel_sales_group_observation_sums_official_volumes_and_counts():
    observations = build_fuel_sales_observation(pl.DataFrame(_sales_rows()))

    panel = build_fuel_sales_group_observation(
        observations,
        group_by=["all", "region", "state"],
        max_groups=100,
    )

    all_row = panel.filter(
        (pl.col("group_type") == "all") & (pl.col("product") == "GASOLINA C")
    ).row(0, named=True)
    assert all_row["feature_id"] == "anp_fuel_sales|all|all|gasolina_c"
    assert all_row["available_date"] == date(2024, 2, 2)
    assert all_row["sales_volume_m3"] == 100.0
    assert all_row["sales_volume_count"] == 1
    assert all_row["state_count"] == 2

    sp_row = panel.filter(
        (pl.col("group_type") == "state") & (pl.col("group_value") == "sp")
    ).row(0, named=True)
    assert sp_row["sales_volume_m3"] == 100.0
    assert sp_row["state_count"] == 1


def test_fuel_sales_group_observation_null_aware_sum_remains_null_when_all_missing():
    observations = build_fuel_sales_observation(
        pl.DataFrame([_sales_row("SP", sales_volume_m3=None)])
    )

    panel = build_fuel_sales_group_observation(observations, group_by=["all"], max_groups=100)
    row = panel.row(0, named=True)

    assert row["sales_volume_m3"] is None
    assert row["sales_volume_count"] == 0


def test_fuel_sales_group_observation_max_groups_guard():
    observations = build_fuel_sales_observation(
        pl.DataFrame([_sales_row(f"S{index}", product=f"PROD {index}") for index in range(3)])
    )

    with pytest.raises(ValueError, match="max_groups=2"):
        build_fuel_sales_group_observation(observations, group_by=["state"], max_groups=2)


def _sales_rows() -> list[dict[str, object]]:
    return [
        _sales_row("SP", sales_volume_m3=100.0),
        _sales_row("RJ", sales_volume_m3=None, available_date=date(2024, 2, 2)),
    ]


def _sales_row(
    state: str,
    *,
    sales_volume_m3: float | None = 100.0,
    product: str = "GASOLINA C",
    available_date: date = date(2024, 2, 1),
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 31),
        "available_date": available_date,
        "availability_policy": "anp_monthly_next_month_end_next_business_day",
        "year": 2024,
        "month": 1,
        "region": "Sudeste",
        "state": state,
        "product": product,
        "sales_volume_m3": sales_volume_m3,
        "unit": "m3",
        "source_version": "v0",
    }
