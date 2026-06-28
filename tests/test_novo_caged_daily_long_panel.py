from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.novo_caged.daily_long import (
    build_novo_caged_daily_long,
    build_novo_caged_state_asof_daily,
)


def test_state_asof_uses_pre_window_history_and_carries_staleness():
    state = build_novo_caged_state_asof_daily(
        movement_groups=_movement_groups(),
        start=date(2024, 4, 4),
        end=date(2024, 4, 5),
        include_movement_counts=True,
        include_wage_hours=True,
        max_features=100,
    )

    rows = state.filter(
        (pl.col("ref_date") == date(2024, 4, 5))
        & (pl.col("feature_id") == "novo_caged_movement|all|all|1")
        & (pl.col("value_name") == "movement_count")
    ).to_dicts()
    assert rows[0]["observation_ref_date"] == date(2024, 2, 29)
    assert rows[0]["observation_available_date"] == date(2024, 4, 3)
    assert rows[0]["staleness_days"] == 2
    assert rows[0]["value"] == 1.0


def test_state_asof_emits_no_rows_before_first_availability():
    state = build_novo_caged_state_asof_daily(
        movement_groups=_movement_groups(),
        start=date(2024, 3, 1),
        end=date(2024, 3, 1),
        include_movement_counts=True,
        include_wage_hours=True,
        max_features=100,
    )

    assert state.is_empty()


def test_state_asof_preserves_latest_missing_observation():
    state = build_novo_caged_state_asof_daily(
        movement_groups=_movement_groups(),
        start=date(2024, 4, 5),
        end=date(2024, 4, 5),
        include_movement_counts=True,
        include_wage_hours=True,
        max_features=100,
    )

    wage = state.filter(
        (pl.col("feature_id") == "novo_caged_movement|all|all|1")
        & (pl.col("value_name") == "wage_mean")
    ).to_dicts()[0]
    assert wage["observation_ref_date"] == date(2024, 2, 29)
    assert wage["value"] is None


def test_state_asof_max_features_guard():
    with pytest.raises(ValueError, match="exceeds max_features"):
        build_novo_caged_state_asof_daily(
            movement_groups=_movement_groups(),
            start=date(2024, 4, 5),
            end=date(2024, 4, 5),
            include_movement_counts=True,
            include_wage_hours=True,
            max_features=1,
        )


def test_daily_long_drops_null_values_and_keeps_long_primary_key():
    state = build_novo_caged_state_asof_daily(
        movement_groups=_movement_groups(),
        start=date(2024, 4, 5),
        end=date(2024, 4, 5),
        include_movement_counts=True,
        include_wage_hours=True,
        max_features=100,
    )

    daily_long = build_novo_caged_daily_long(state_asof_daily=state)

    assert daily_long.filter(pl.col("value").is_null()).is_empty()
    duplicate_keys = daily_long.group_by(
        ["ref_date", "source_family", "feature_id", "value_name"]
    ).len()
    assert duplicate_keys.filter(pl.col("len") > 1).is_empty()
    assert set(daily_long["source_family"].to_list()) == {"novo_caged_movements"}
    assert "is_observed_on_ref_date" not in daily_long.columns
    assert "movement_record_id" not in daily_long.columns


def _movement_groups() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 31), date(2024, 2, 29)],
            "available_date": [date(2024, 3, 4), date(2024, 4, 3)],
            "silver_available_date": [date(2024, 3, 12), date(2024, 4, 3)],
            "calendar_available_date": [date(2024, 3, 4), None],
            "availability_source": ["official_calendar", "conservative_fallback"],
            "availability_policy": [
                "novo_caged_official_release_calendar",
                "novo_caged_conservative_next_month_end_plus_2bd",
            ],
            "group_type": ["all", "all"],
            "group_value": ["all", "all"],
            "movement_sign": ["1", "1"],
            "feature_id": [
                "novo_caged_movement|all|all|1",
                "novo_caged_movement|all|all|1",
            ],
            "movement_count": [2, 1],
            "wage_mean": [1000.0, None],
            "contract_hours_mean": [42.0, 40.0],
            "wage_count": [1, 0],
            "contract_hours_count": [2, 1],
            "source_version": ["v0", "v0"],
        }
    )
