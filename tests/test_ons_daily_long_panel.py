from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.ons.daily_long import build_ons_daily_long, build_ons_state_asof_daily
from bralpha.derived.ons.schemas import PANEL_PRIMARY_KEYS
from bralpha.timing.vintages import AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE


def test_ons_state_asof_uses_pre_window_history_and_latest_available_missing_values():
    ear = _ear_observation()

    state = build_ons_state_asof_daily(
        ear=ear,
        start=date(2024, 1, 1),
        end=date(2024, 1, 8),
        max_features=10,
    )
    stored = state.filter(pl.col("value_name") == "stored_energy_mwmes")

    assert stored["ref_date"].to_list() == [
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ]
    assert stored["value"].to_list() == [10.0, 10.0, None, None]
    assert stored["observation_ref_date"].to_list() == [
        date(2024, 1, 2),
        date(2024, 1, 2),
        date(2024, 1, 4),
        date(2024, 1, 4),
    ]
    assert stored["staleness_days"].to_list() == [0, 1, 0, 3]


def test_ons_state_asof_carries_weekly_cmo_forward_with_staleness():
    cmo = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 6)],
            "available_date": [date(2024, 1, 8)],
            "availability_policy": ["ons_conservative_next_business_day"],
            "subsystem_id": ["SE"],
            "subsystem": ["Sudeste"],
            "load_block": ["leve"],
            "feature_id": ["ons_cmo_weekly|se|sudeste|leve"],
            "cmo_brl_mwh": [100.0],
            "unit": ["BRL/MWh"],
            "has_cmo_brl_mwh": [True],
            "source_version": ["v0"],
        }
    )

    state = build_ons_state_asof_daily(
        cmo=cmo,
        start=date(2024, 1, 8),
        end=date(2024, 1, 10),
        max_features=10,
    )

    assert state["ref_date"].to_list() == [date(2024, 1, 8), date(2024, 1, 9), date(2024, 1, 10)]
    assert state["staleness_days"].to_list() == [0, 1, 2]
    assert state["value"].to_list() == [100.0, 100.0, 100.0]


def test_ons_state_asof_max_feature_guard():
    ear = pl.concat(
        [
            _ear_observation(feature_id="ons_ear_subsystem|se|sudeste"),
            _ear_observation(feature_id="ons_ear_subsystem|ne|nordeste"),
        ],
        how="diagonal_relaxed",
    )

    with pytest.raises(ValueError, match="max_features"):
        build_ons_state_asof_daily(
            ear=ear,
            start=date(2024, 1, 3),
            end=date(2024, 1, 3),
            max_features=1,
        )


def test_ons_daily_long_drops_null_values_and_keeps_long_primary_key():
    state = build_ons_state_asof_daily(
        ear=_ear_observation(),
        start=date(2024, 1, 3),
        end=date(2024, 1, 8),
        max_features=10,
    )

    daily_long = build_ons_daily_long(
        state_asof_daily=state,
        include_hydro=True,
        include_load_cmo=False,
        include_energy_balance=False,
        include_interchange=False,
    )

    assert daily_long.filter(pl.col("value").is_null()).is_empty()
    assert "ref_datetime" not in daily_long.columns
    assert daily_long.group_by(PANEL_PRIMARY_KEYS["daily_long"]).len().height == daily_long.height
    assert set(daily_long["source_family"].unique().to_list()) == {"ons_ear_subsystem"}


def test_ons_daily_long_excludes_current_snapshot_rows():
    state = build_ons_state_asof_daily(
        ear=_ear_observation().with_columns(
            availability_basis=pl.lit(AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE),
            model_usable=pl.lit(False),
        ),
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
        max_features=10,
    )

    daily_long = build_ons_daily_long(
        state_asof_daily=state,
        include_hydro=True,
        include_load_cmo=False,
        include_energy_balance=False,
        include_interchange=False,
    )

    assert daily_long.is_empty()


def test_ons_state_asof_uses_later_snapshot_only_after_available_date():
    ear = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "available_date": [date(2024, 1, 2), date(2024, 1, 4)],
            "availability_policy": ["ons_first_seen_snapshot", "ons_first_seen_snapshot"],
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "feature_id": ["ons_ear_subsystem|se|sudeste"] * 2,
            "stored_energy_mwmes": [50.0, 55.0],
            "stored_energy_percent": [50.0, 55.0],
            "stored_energy_max_mwmes": [100.0, 100.0],
            "unit": ["MWmes", "MWmes"],
            "has_stored_energy_mwmes": [True, True],
            "has_stored_energy_percent": [True, True],
            "has_stored_energy_max_mwmes": [True, True],
            "vintage_id": ["ons-v1", "ons-v2"],
            "revision_sequence": [0, 1],
            "source_version": ["v0", "v0"],
        }
    )

    state = build_ons_state_asof_daily(
        ear=ear,
        start=date(2024, 1, 2),
        end=date(2024, 1, 4),
        max_features=10,
    )
    stored = state.filter(pl.col("value_name") == "stored_energy_mwmes").sort("ref_date")

    assert stored["value"].to_list() == [50.0, 50.0, 55.0]
    assert stored["vintage_id"].to_list() == ["ons-v1", "ons-v1", "ons-v2"]


def _ear_observation(feature_id: str = "ons_ear_subsystem|se|sudeste") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 2), date(2024, 1, 4)],
            "available_date": [date(2024, 1, 3), date(2024, 1, 5)],
            "availability_policy": ["ons_conservative_next_business_day"] * 2,
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "feature_id": [feature_id, feature_id],
            "stored_energy_mwmes": [10.0, None],
            "stored_energy_percent": [20.0, 21.0],
            "stored_energy_max_mwmes": [100.0, 100.0],
            "unit": ["MWmes", "MWmes"],
            "has_stored_energy_mwmes": [True, False],
            "has_stored_energy_percent": [True, True],
            "has_stored_energy_max_mwmes": [True, True],
            "source_version": ["v0", "v0"],
        }
    )
