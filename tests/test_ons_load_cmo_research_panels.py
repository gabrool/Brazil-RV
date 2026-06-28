from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.ons.load_cmo import (
    build_cmo_weekly_observation,
    build_load_daily_observation,
)


def test_ons_load_observation_preserves_methodology_note_and_missingness():
    silver = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 2), date(2024, 1, 3)],
            "available_date": [date(2024, 1, 3), date(2024, 1, 4)],
            "availability_policy": ["ons_conservative_next_business_day"] * 2,
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "load_mwmed": [80.0, None],
            "unit": ["MWmed", "MWmed"],
            "methodology_note": ["official_bucket", "official_bucket"],
            "source_version": ["v0", "v0"],
        }
    )

    panel = build_load_daily_observation(silver)

    assert panel["feature_id"].to_list() == [
        "ons_load_daily|se|sudeste",
        "ons_load_daily|se|sudeste",
    ]
    assert panel["methodology_note"].to_list() == ["official_bucket", "official_bucket"]
    assert panel["has_load_mwmed"].to_list() == [True, False]


def test_ons_cmo_observation_preserves_weekly_rows_without_daily_fill():
    silver = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 6), date(2024, 1, 6)],
            "available_date": [date(2024, 1, 8), date(2024, 1, 8)],
            "availability_policy": ["ons_conservative_next_business_day"] * 2,
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "load_block": ["leve", "pesada"],
            "cmo_brl_mwh": [90.0, None],
            "unit": ["BRL/MWh", "BRL/MWh"],
            "source_version": ["v0", "v0"],
        }
    )

    panel = build_cmo_weekly_observation(silver)

    assert panel.height == 2
    assert panel["ref_date"].unique().to_list() == [date(2024, 1, 6)]
    assert panel["feature_id"].to_list() == [
        "ons_cmo_weekly|se|sudeste|leve",
        "ons_cmo_weekly|se|sudeste|pesada",
    ]
    assert panel["has_cmo_brl_mwh"].to_list() == [True, False]
    assert panel.group_by(["ref_date", "subsystem_id", "load_block"]).len().height == panel.height
