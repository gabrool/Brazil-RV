from __future__ import annotations

from bralpha.derived.pit_metadata import PIT_METADATA_COLUMNS

ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "subsystem_id",
    "subsystem",
    "feature_id",
    "stored_energy_mwmes",
    "stored_energy_percent",
    "stored_energy_max_mwmes",
    "unit",
    "has_stored_energy_mwmes",
    "has_stored_energy_percent",
    "has_stored_energy_max_mwmes",
    "source_version",
]

ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "subsystem_id",
    "subsystem",
    "ena_type",
    "feature_id",
    "ena_value",
    "unit",
    "has_ena_value",
    "source_version",
]

ONS_LOAD_DAILY_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "subsystem_id",
    "subsystem",
    "feature_id",
    "load_mwmed",
    "unit",
    "methodology_note",
    "has_load_mwmed",
    "source_version",
]

ONS_CMO_WEEKLY_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "subsystem_id",
    "subsystem",
    "load_block",
    "feature_id",
    "cmo_brl_mwh",
    "unit",
    "has_cmo_brl_mwh",
    "source_version",
]

ONS_ENERGY_BALANCE_DAILY_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "subsystem_id",
    "subsystem",
    "feature_id",
    "load_mwmed",
    "hydro_generation_mwmed",
    "thermal_generation_mwmed",
    "wind_generation_mwmed",
    "solar_generation_mwmed",
    "other_generation_mwmed",
    "interchange_mwmed",
    "hour_count",
    "load_count",
    "hydro_generation_count",
    "thermal_generation_count",
    "wind_generation_count",
    "solar_generation_count",
    "other_generation_count",
    "interchange_count",
    "unit",
    "source_version",
]

ONS_INTERCHANGE_DAILY_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "source_subsystem_id",
    "source_subsystem",
    "target_subsystem_id",
    "target_subsystem",
    "feature_id",
    "interchange_mwmed",
    "programmed_interchange_mwmed",
    "hour_count",
    "interchange_count",
    "programmed_interchange_count",
    "unit",
    "source_version",
]

ONS_STATE_ASOF_DAILY_COLUMNS = [
    "ref_date",
    "available_date",
    "source_family",
    "feature_id",
    "observation_ref_date",
    "observation_available_date",
    "value_name",
    "value",
    "unit",
    "is_available",
    "is_observed_on_ref_date",
    "staleness_days",
    "source_version",
]

ONS_DAILY_LONG_COLUMNS = [
    "ref_date",
    "available_date",
    "source_family",
    "feature_id",
    "value_name",
    "value",
    "unit",
    "observation_ref_date",
    "observation_available_date",
    "is_available",
    "staleness_days",
    "source_version",
]

ONS_POWER_FEATURE_DAILY_COLUMNS = [*ONS_DAILY_LONG_COLUMNS, *PIT_METADATA_COLUMNS]

PANEL_PRIMARY_KEYS = {
    "ear_subsystem_observation": ["ref_date", "subsystem_id"],
    "ena_subsystem_observation": ["ref_date", "subsystem_id", "ena_type"],
    "load_daily_observation": ["ref_date", "subsystem_id"],
    "cmo_weekly_observation": ["ref_date", "subsystem_id", "load_block"],
    "energy_balance_daily_observation": ["ref_date", "subsystem_id"],
    "interchange_daily_observation": [
        "ref_date",
        "source_subsystem_id",
        "target_subsystem_id",
    ],
    "state_asof_daily": ["ref_date", "source_family", "feature_id", "value_name"],
    "power_feature_daily": ["ref_date", "source_family", "feature_id", "value_name"],
    "daily_long": ["ref_date", "source_family", "feature_id", "value_name"],
}
