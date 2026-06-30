from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.novo_caged.features import build_novo_caged_feature_daily


def test_novo_caged_combines_signs_and_computes_state_diffusion():
    ref_date = date(2024, 1, 31)
    rows = []
    rows.extend(
        _movement_rows(
            ref_date,
            "novo_caged_movement|state|sp|plus_1",
            {
                "movement_count": 100,
                "wage_mean": 2500.0,
                "wage_count": 100,
                "contract_hours_mean": 40.0,
                "contract_hours_count": 100,
            },
            metadata={
                "availability_policy": "novo_caged_calendar_first_seen",
                "availability_basis": "official_calendar_and_first_seen",
                "revision_policy": "revised_with_snapshots",
                "vintage_id": "caged-v1",
                "model_usable": True,
                "model_usable_reason": "fixture",
            },
        )
    )
    rows.extend(
        _movement_rows(
            ref_date,
            "novo_caged_movement|state|sp|minus_1",
            {
                "movement_count": 40,
                "wage_mean": 2300.0,
                "wage_count": 40,
                "contract_hours_mean": 38.0,
                "contract_hours_count": 40,
            },
            metadata={
                "availability_policy": "novo_caged_calendar_first_seen",
                "availability_basis": "official_calendar_and_first_seen",
                "revision_policy": "revised_with_snapshots",
                "vintage_id": "caged-v1",
                "model_usable": True,
                "model_usable_reason": "fixture",
            },
        )
    )
    rows.extend(
        _movement_rows(
            ref_date,
            "novo_caged_movement|state|rj|plus_1",
            {"movement_count": 20, "wage_mean": 2400.0, "wage_count": 20},
        )
    )
    rows.extend(
        _movement_rows(
            ref_date,
            "novo_caged_movement|state|rj|minus_1",
            {"movement_count": 30, "wage_mean": 2350.0, "wage_count": 30},
        )
    )

    features = build_novo_caged_feature_daily(
        pl.DataFrame(rows),
        start=ref_date,
        end=ref_date,
    )

    sp_feature = "novo_caged:novo_caged_movement|state|sp"
    assert _value(features, sp_feature, "admissions_count") == pytest.approx(100.0)
    assert _value(features, sp_feature, "dismissals_count") == pytest.approx(40.0)
    assert _value(features, sp_feature, "net_jobs") == pytest.approx(60.0)
    assert _value(features, sp_feature, "admission_dismissal_ratio") == pytest.approx(2.5)
    row = features.filter(
        (pl.col("feature_id") == sp_feature) & (pl.col("value_name") == "net_jobs")
    ).row(0, named=True)
    assert row["availability_policy"] == "novo_caged_calendar_first_seen"
    assert row["availability_basis"] == "official_calendar_and_first_seen"
    assert row["revision_policy"] == "revised_with_snapshots"
    assert row["vintage_id"] == "caged-v1"
    assert row["model_usable"] is True
    assert row["model_usable_reason"] == "fixture"
    assert _value(features, sp_feature, "state_positive_diffusion_share_pct") == pytest.approx(
        50.0
    )


def _movement_rows(
    ref_date: date,
    feature_id: str,
    values: dict[str, float],
    *,
    metadata: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    return [
        {
            "ref_date": ref_date,
            "available_date": ref_date,
            "source_family": "novo_caged_movements",
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


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
