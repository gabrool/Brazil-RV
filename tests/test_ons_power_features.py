from __future__ import annotations

from datetime import date
from math import log, log1p
from statistics import mean, stdev

import polars as pl
import pytest

from bralpha.derived.ons.features import build_ons_power_feature_daily


def test_ons_generation_shares_and_backward_only_seasonal_z():
    rows = []
    prior_values = [50.0 + index for index in range(24)]
    for index, value in enumerate(prior_values):
        ref_date = date(2000 + index, 1, 15)
        rows.extend(
            _asof_metric_rows(
                ref_date,
                "ons_ear_subsystem",
                "ons_ear_subsystem|se",
                {"stored_energy_percent": value, "stored_energy_mwmes": value * 100.0},
            )
        )
    current_date = date(2024, 1, 15)
    rows.extend(
        _asof_metric_rows(
            current_date,
            "ons_ear_subsystem",
            "ons_ear_subsystem|se",
            {"stored_energy_percent": 90.0, "stored_energy_mwmes": 9000.0},
        )
    )
    rows.extend(
        _asof_metric_rows(
            current_date,
            "ons_energy_balance_daily",
            "ons_energy_balance_daily|se",
            {
                "load_mwmed": 100.0,
                "hydro_generation_mwmed": 50.0,
                "thermal_generation_mwmed": 25.0,
                "wind_generation_mwmed": 15.0,
                "solar_generation_mwmed": 10.0,
                "other_generation_mwmed": 0.0,
                "interchange_mwmed": 5.0,
                "hour_count": 24.0,
            },
        )
    )

    features = build_ons_power_feature_daily(
        pl.DataFrame(rows),
        start=current_date,
        end=current_date,
    )

    expected_z = (90.0 - mean(prior_values)) / stdev(prior_values)
    assert _value(
        features,
        "ons_power:ons_ear_subsystem|se",
        "stored_energy_percent_seasonal_z",
    ) == pytest.approx(expected_z)
    assert _value(
        features,
        "ons_power:ons_energy_balance_daily|se",
        "hydro_generation_share_pct",
    ) == pytest.approx(50.0)
    assert _value(
        features,
        "ons_power:ons_energy_balance_daily|se",
        "interchange_to_load_pct",
    ) == pytest.approx(5.0)
    assert features["ref_date"].to_list() and set(features["ref_date"].to_list()) == {
        current_date
    }


def test_ons_interchange_features_and_ena_unit_branching_preserve_pit_metadata():
    current_date = date(2024, 1, 15)
    rows = []
    rows.extend(
        _asof_metric_rows(
            current_date,
            "ons_ena_subsystem",
            "ons_ena_subsystem|se|percent_mlt",
            {"ena_value": 95.0},
            unit="percent_mlt",
        )
    )
    rows.extend(
        _asof_metric_rows(
            current_date,
            "ons_ena_subsystem",
            "ons_ena_subsystem|se|physical",
            {"ena_value": 2_500.0},
            unit="MWmed",
        )
    )
    rows.extend(
        _asof_metric_rows(
            current_date,
            "ons_interchange_daily",
            "ons_interchange_daily|se|s",
            {
                "interchange_mwmed": -10.0,
                "programmed_interchange_mwmed": 5.0,
                "hour_count": 24.0,
            },
            metadata={
                "availability_policy": "ons_first_seen",
                "availability_basis": "first_seen_timestamp",
                "revision_policy": "revised_with_snapshots",
                "vintage_id": "ons-v1",
                "model_usable": True,
                "model_usable_reason": "fixture",
            },
        )
    )

    features = build_ons_power_feature_daily(
        pl.DataFrame(rows),
        start=current_date,
        end=current_date,
    )

    assert _value(
        features,
        "ons_power:ons_ena_subsystem|se|percent_mlt",
        "ena_percent_mlt_level",
    ) == pytest.approx(95.0)
    assert _value(
        features,
        "ons_power:ons_ena_subsystem|se|physical",
        "ena_mwmed_log",
    ) == pytest.approx(log(2_500.0))
    assert (
        features.filter(
            (pl.col("feature_id") == "ons_power:ons_ena_subsystem|se|physical")
            & (pl.col("value_name").str.starts_with("ena_percent_mlt"))
        ).height
        == 0
    )
    assert _value(
        features,
        "ons_power:ons_interchange_daily|se|s",
        "interchange_mwmed_signed_log",
    ) == pytest.approx(-log1p(10.0))
    row = _feature_row(
        features,
        "ons_power:ons_interchange_daily|se|s",
        "programmed_interchange_mwmed_signed_log",
    )
    assert row["value"] == pytest.approx(log1p(5.0))
    assert row["availability_policy"] == "ons_first_seen"
    assert row["availability_basis"] == "first_seen_timestamp"
    assert row["revision_policy"] == "revised_with_snapshots"
    assert row["vintage_id"] == "ons-v1"
    assert row["model_usable"] is True
    assert row["model_usable_reason"] == "fixture"


def _asof_metric_rows(
    ref_date: date,
    source_family: str,
    feature_id: str,
    values: dict[str, float],
    *,
    unit: str = "value",
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
            "unit": unit,
            "is_available": True,
            "is_observed_on_ref_date": True,
            "staleness_days": 0,
            "source_version": "fixture",
            **(metadata or {}),
        }
        for value_name, value in values.items()
    ]


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float:
    return _feature_row(frame, feature_id, value_name)["value"]


def _feature_row(frame: pl.DataFrame, feature_id: str, value_name: str) -> dict[str, object]:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    ).row(0, named=True)
