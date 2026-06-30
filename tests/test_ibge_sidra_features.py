from __future__ import annotations

from datetime import date
from math import log

import polars as pl
import pytest

from bralpha.derived.ibge.sidra_features import build_sidra_feature_daily


def test_sidra_features_compute_inflation_trailing_sums_from_observation_history():
    rows = [
        _sidra_row(
            date(2024, month, 15),
            "ipca",
            "ibge_sidra:ipca:fixture",
            float(month),
            frequency="monthly",
        )
        for month in range(1, 13)
    ]
    final_date = date(2024, 12, 15)

    features = build_sidra_feature_daily(pl.DataFrame(rows), start=final_date, end=final_date)

    feature_id = "ibge_sidra_feature:ibge_sidra:ipca:fixture"
    assert _value(features, feature_id, "monthly_pct") == 12.0
    assert _value(features, feature_id, "change_1obs_pp") == 1.0
    assert _value(features, feature_id, "trailing_3obs_sum_pct") == 33.0
    assert _value(features, feature_id, "trailing_12obs_sum_pct") == 78.0


def test_sidra_features_use_quarterly_yoy_lag_for_gdp_volume():
    rows = [
        _sidra_row(
            date(2024, quarter, 15),
            "gdp_volume_change",
            "ibge_sidra:gdp_volume_change:fixture",
            value,
            frequency="quarterly",
        )
        for quarter, value in zip([1, 4, 7, 10, 12], [1.0, 2.0, 3.0, 4.0, 6.0], strict=True)
    ]
    final_date = date(2024, 12, 15)

    features = build_sidra_feature_daily(pl.DataFrame(rows), start=final_date, end=final_date)

    feature_id = "ibge_sidra_feature:ibge_sidra:gdp_volume_change:fixture"
    assert _value(features, feature_id, "level_pct") == 6.0
    assert _value(features, feature_id, "change_1obs_pp") == 2.0
    assert _value(features, feature_id, "yoy_change_pp") == 5.0


def test_sidra_features_compute_monthly_activity_yoy_log_change():
    rows = [
        _sidra_row(
            date(2024, month, 15),
            "pim_industrial_production",
            "ibge_sidra:pim_industrial_production:fixture",
            100.0 + month,
            frequency="monthly",
        )
        for month in range(1, 13)
    ] + [
        _sidra_row(
            date(2025, 1, 15),
            "pim_industrial_production",
            "ibge_sidra:pim_industrial_production:fixture",
            115.0,
            frequency="monthly",
        )
    ]

    features = build_sidra_feature_daily(
        pl.DataFrame(rows),
        start=date(2025, 1, 15),
        end=date(2025, 1, 15),
    )

    feature_id = "ibge_sidra_feature:ibge_sidra:pim_industrial_production:fixture"
    assert _value(features, feature_id, "log_level") == pytest.approx(log(115.0))
    assert _value(features, feature_id, "yoy_log_change") == pytest.approx(log(115.0 / 101.0))


def _sidra_row(
    ref_date: date,
    dataset_slug: str,
    feature_id: str,
    value: float,
    *,
    frequency: str,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "feature_id": feature_id,
        "dataset_slug": dataset_slug,
        "aggregate_id": "fixture",
        "variable_id": "fixture",
        "variable_name": "fixture",
        "unit": "percent" if "ipca" in dataset_slug or "gdp" in dataset_slug else "index",
        "frequency": frequency,
        "observation_ref_date": ref_date,
        "observation_available_date": ref_date,
        "ref_period_start": ref_date,
        "ref_period_end": ref_date,
        "period_code": ref_date.isoformat(),
        "period_label": ref_date.isoformat(),
        "release_date": ref_date,
        "available_datetime_local": None,
        "available_datetime_utc": None,
        "availability_policy": "official_calendar",
        "availability_basis": "official_release_timestamp",
        "revision_policy": "revised_use_vintages",
        "vintage_id": f"vintage:{feature_id}:{ref_date}",
        "first_seen_timestamp_utc": None,
        "source_publication_datetime_utc": None,
        "geography_level": "br",
        "geography_id": "1",
        "geography_name": "Brasil",
        "classification_key": None,
        "classifications_json": None,
        "value": value,
        "raw_value": str(value),
        "value_status": "ok",
        "has_value": True,
        "is_available": True,
        "is_observed_on_ref_date": True,
        "staleness_days": 0,
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float | None:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
