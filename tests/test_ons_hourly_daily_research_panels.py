from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.derived.ons.hourly_daily import (
    build_energy_balance_daily_observation,
    build_interchange_daily_observation,
)


def test_ons_energy_balance_daily_uses_means_counts_and_max_availability():
    silver = pl.DataFrame(
        {
            "ref_datetime": [datetime(2024, 1, 2, 0), datetime(2024, 1, 2, 1)],
            "ref_date": [date(2024, 1, 2), date(2024, 1, 2)],
            "available_date": [date(2024, 1, 3), date(2024, 1, 4)],
            "availability_policy": ["ons_conservative_next_business_day"] * 2,
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "load_mwmed": [100.0, 120.0],
            "hydro_generation_mwmed": [50.0, None],
            "thermal_generation_mwmed": [20.0, 30.0],
            "wind_generation_mwmed": [10.0, 20.0],
            "solar_generation_mwmed": [5.0, 15.0],
            "other_generation_mwmed": [None, None],
            "interchange_mwmed": [-5.0, -7.0],
            "unit": ["MWmed", "MWmed"],
            "source_version": ["v0", "v0"],
        }
    )

    panel = build_energy_balance_daily_observation(silver)

    assert panel.height == 1
    row = panel.to_dicts()[0]
    assert row["feature_id"] == "ons_energy_balance_daily|se|sudeste"
    assert row["available_date"] == date(2024, 1, 4)
    assert row["load_mwmed"] == 110.0
    assert row["hydro_generation_mwmed"] == 50.0
    assert row["other_generation_mwmed"] is None
    assert row["hour_count"] == 2
    assert row["hydro_generation_count"] == 1
    assert row["other_generation_count"] == 0
    assert "hydro_share" not in panel.columns
    assert "thermal_gap" not in panel.columns


def test_ons_interchange_daily_preserves_direction_without_netting():
    silver = pl.DataFrame(
        {
            "ref_datetime": [
                datetime(2024, 1, 2, 0),
                datetime(2024, 1, 2, 1),
                datetime(2024, 1, 2, 0),
            ],
            "ref_date": [date(2024, 1, 2)] * 3,
            "available_date": [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 3)],
            "availability_policy": ["ons_conservative_next_business_day"] * 3,
            "source_subsystem_id": ["SE", "SE", "NE"],
            "source_subsystem": ["Sudeste", "Sudeste", "Nordeste"],
            "target_subsystem_id": ["NE", "NE", "SE"],
            "target_subsystem": ["Nordeste", "Nordeste", "Sudeste"],
            "interchange_mwmed": [100.0, 120.0, -30.0],
            "programmed_interchange_mwmed": [90.0, None, -25.0],
            "unit": ["MWmed", "MWmed", "MWmed"],
            "source_version": ["v0", "v0", "v0"],
        }
    )

    panel = build_interchange_daily_observation(silver)

    assert panel.height == 2
    se_to_ne = panel.filter(pl.col("source_subsystem_id") == "SE").to_dicts()[0]
    assert se_to_ne["feature_id"] == "ons_interchange_daily|se|sudeste|ne|nordeste"
    assert se_to_ne["interchange_mwmed"] == 110.0
    assert se_to_ne["programmed_interchange_mwmed"] == 90.0
    assert se_to_ne["hour_count"] == 2
    assert se_to_ne["programmed_interchange_count"] == 1
    assert "net_interchange_mwmed" not in panel.columns
