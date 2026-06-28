from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.ons.hydro import (
    build_ear_subsystem_observation,
    build_ena_subsystem_observation,
)
from bralpha.derived.ons.schemas import (
    ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS,
    ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS,
)


def test_ons_ear_observation_preserves_values_and_missingness_flags():
    silver = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 2), date(2024, 1, 3)],
            "available_date": [date(2024, 1, 3), date(2024, 1, 4)],
            "availability_policy": ["ons_conservative_next_business_day"] * 2,
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "stored_energy_mwmes": [50.0, None],
            "stored_energy_percent": [55.0, 56.0],
            "stored_energy_max_mwmes": [100.0, 100.0],
            "unit": ["MWmes", "MWmes"],
            "source_version": ["v0", "v0"],
        }
    )

    panel = build_ear_subsystem_observation(silver)

    assert panel.columns == ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS
    assert panel["feature_id"].to_list() == [
        "ons_ear_subsystem|se|sudeste",
        "ons_ear_subsystem|se|sudeste",
    ]
    assert panel["stored_energy_mwmes"].to_list() == [50.0, None]
    assert panel["has_stored_energy_mwmes"].to_list() == [True, False]
    assert panel.group_by(["ref_date", "subsystem_id"]).len().height == panel.height


def test_ons_ena_observation_preserves_type_rows_without_derived_features():
    silver = pl.DataFrame(
        {
            "ref_date": [date(2024, 1, 2), date(2024, 1, 2)],
            "available_date": [date(2024, 1, 3), date(2024, 1, 3)],
            "availability_policy": ["ons_conservative_next_business_day"] * 2,
            "subsystem_id": ["SE", "SE"],
            "subsystem": ["Sudeste", "Sudeste"],
            "ena_type": ["bruta_mwmed", "armazenavel_mwmed"],
            "ena_value": [10.0, None],
            "unit": ["MWmed", "MWmed"],
            "source_version": ["v0", "v0"],
        }
    )

    panel = build_ena_subsystem_observation(silver)

    assert panel.columns == ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS
    assert panel["feature_id"].to_list() == [
        "ons_ena_subsystem|se|sudeste|armazenavel_mwmed",
        "ons_ena_subsystem|se|sudeste|bruta_mwmed",
    ]
    assert panel["has_ena_value"].to_list() == [False, True]
    assert panel.group_by(["ref_date", "subsystem_id", "ena_type"]).len().height == panel.height
