from __future__ import annotations

from datetime import date
from math import log

import polars as pl
import pytest

from bralpha.derived.anp.features import build_anp_fuel_feature_daily


def test_anp_price_parity_and_monthly_yoy_use_warmup_history():
    rows = []
    rows.extend(
        _asof_metric_rows(
            date(2024, 1, 5),
            "anp_fuel_price",
            "anp_fuel_price|state|sp|gasolina_comum",
            {"sale_price": 3.0, "purchase_price": 2.5, "station_count": 10},
        )
    )
    rows.extend(
        _asof_metric_rows(
            date(2024, 1, 12),
            "anp_fuel_price",
            "anp_fuel_price|state|sp|gasolina_comum",
            {"sale_price": 6.0, "purchase_price": 5.0, "station_count": 12},
            metadata={
                "availability_policy": "anp_official",
                "availability_basis": "official_release",
                "revision_policy": "revised_with_snapshots",
                "vintage_id": "anp-v1",
                "model_usable": True,
                "model_usable_reason": "fixture",
            },
        )
    )
    rows.extend(
        _asof_metric_rows(
            date(2024, 1, 12),
            "anp_fuel_price",
            "anp_fuel_price|state|sp|etanol_hidratado",
            {"sale_price": 4.5, "purchase_price": 4.0, "station_count": 8},
        )
    )
    rows.extend(
        _asof_metric_rows(
            date(2024, 1, 12),
            "anp_fuel_price",
            "anp_fuel_price|state|sp|oleo_diesel",
            {"sale_price": 5.4, "purchase_price": 5.0, "station_count": 7},
        )
    )
    for index in range(13):
        ref_date = date(2024 + index // 12, index % 12 + 1, 20)
        rows.extend(
            _asof_metric_rows(
                ref_date,
                "anp_fuel_sales",
                "anp_fuel_sales|all|all|gasolina_comum",
                {
                    "sales_volume_m3": 100.0 + 10.0 * index,
                    "sales_volume_count": 1,
                    "state_count": 1,
                },
            )
        )

    features = build_anp_fuel_feature_daily(
        pl.DataFrame(rows),
        start=date(2024, 1, 12),
        end=date(2025, 1, 20),
    )

    assert _value(
        features,
        date(2024, 1, 12),
        "anp_fuel:anp_fuel_price|state|sp|gasolina_comum",
        "sale_price_log_change_1obs",
    ) == pytest.approx(log(6.0 / 3.0))
    row = _feature_row(
        features,
        date(2024, 1, 12),
        "anp_fuel:anp_fuel_price|state|sp|gasolina_comum",
        "sale_price_log",
    )
    assert row["availability_policy"] == "anp_official"
    assert row["availability_basis"] == "official_release"
    assert row["revision_policy"] == "revised_with_snapshots"
    assert row["vintage_id"] == "anp-v1"
    assert row["model_usable"] is True
    assert row["model_usable_reason"] == "fixture"
    assert _value(
        features,
        date(2024, 1, 12),
        "anp_fuel:state|sp:cross_product",
        "ethanol_gasoline_parity",
    ) == pytest.approx(0.75)
    assert _value(
        features,
        date(2024, 1, 12),
        "anp_fuel:state|sp:cross_product",
        "diesel_gasoline_spread_pct",
    ) == pytest.approx(-10.0)
    assert _value(
        features,
        date(2025, 1, 20),
        "anp_fuel:anp_fuel_sales|all|all|gasolina_comum",
        "sales_volume_yoy_log_change",
    ) == pytest.approx(log(220.0 / 100.0))
    assert features["ref_date"].min() >= date(2024, 1, 12)


def _asof_metric_rows(
    ref_date: date,
    source_family: str,
    feature_id: str,
    values: dict[str, float],
    *,
    metadata: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    return [
        {
            "ref_date": ref_date,
            "available_date": ref_date,
            "source_family": source_family,
            "feature_id": feature_id,
            "observation_ref_date": ref_date,
            "observation_available_date": ref_date,
            "value_name": value_name,
            "value": value,
            "unit": "value",
            "is_available": True,
            "is_observed_on_ref_date": True,
            "staleness_days": 0,
            "source_version": "fixture",
            **(metadata or {}),
        }
        for value_name, value in values.items()
    ]


def _value(frame: pl.DataFrame, ref_date: date, feature_id: str, value_name: str) -> float:
    return _feature_row(frame, ref_date, feature_id, value_name)["value"]


def _feature_row(
    frame: pl.DataFrame,
    ref_date: date,
    feature_id: str,
    value_name: str,
) -> dict[str, object]:
    return frame.filter(
        (pl.col("ref_date") == ref_date)
        & (pl.col("feature_id") == feature_id)
        & (pl.col("value_name") == value_name)
    ).row(0, named=True)
