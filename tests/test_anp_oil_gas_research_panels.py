from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.anp.oil_gas import (
    build_oil_gas_group_observation,
    build_oil_gas_production_observation,
)


def test_oil_gas_observation_preserves_metric_unit_and_missingness():
    panel = build_oil_gas_production_observation(pl.DataFrame(_production_rows()))

    row = panel.filter(pl.col("state") == "ES").row(0, named=True)
    assert row["metric_type"] == "petroleum_production"
    assert row["metric_value"] is None
    assert row["unit"] == "m3"
    assert row["resource_family"] == "petroleum_production"
    assert row["has_metric_value"] is False
    assert (
        panel.group_by(["ref_date", "state", "location", "metric_type"]).len().height
        == panel.height
    )


def test_oil_gas_group_observation_sums_official_metric_values_and_counts():
    observations = build_oil_gas_production_observation(pl.DataFrame(_production_rows()))

    panel = build_oil_gas_group_observation(
        observations,
        group_by=["all", "region", "state"],
        max_groups=100,
    )

    all_row = panel.filter(
        (pl.col("group_type") == "all")
        & (pl.col("location") == "Mar")
        & (pl.col("metric_type") == "petroleum_production")
    ).row(0, named=True)
    assert all_row["feature_id"] == "anp_oil_gas|all|all|mar|petroleo|petroleum_production"
    assert all_row["available_date"] == date(2024, 2, 2)
    assert all_row["metric_value"] == 10.0
    assert all_row["metric_value_count"] == 1
    assert all_row["state_count"] == 2
    assert all_row["unit"] == "m3"


def test_oil_gas_group_observation_null_aware_sum_remains_null_when_all_missing():
    observations = build_oil_gas_production_observation(
        pl.DataFrame([_production_row("RJ", metric_value=None)])
    )

    panel = build_oil_gas_group_observation(observations, group_by=["all"], max_groups=100)
    row = panel.row(0, named=True)

    assert row["metric_value"] is None
    assert row["metric_value_count"] == 0


def test_oil_gas_group_observation_max_groups_guard():
    observations = build_oil_gas_production_observation(
        pl.DataFrame(
            [
                _production_row(f"S{index}", product=f"PROD {index}")
                for index in range(3)
            ]
        )
    )

    with pytest.raises(ValueError, match="max_groups=2"):
        build_oil_gas_group_observation(observations, group_by=["state"], max_groups=2)


def _production_rows() -> list[dict[str, object]]:
    return [
        _production_row("RJ", metric_value=10.0),
        _production_row("ES", metric_value=None, available_date=date(2024, 2, 2)),
    ]


def _production_row(
    state: str,
    *,
    metric_value: float | None = 10.0,
    product: str = "Petroleo",
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
        "location": "Mar",
        "product": product,
        "metric_type": "petroleum_production",
        "metric_value": metric_value,
        "unit": "m3",
        "resource_family": "petroleum_production",
        "source_version": "v0",
    }
