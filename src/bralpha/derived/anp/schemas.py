from __future__ import annotations

from bralpha.derived.pit_metadata import PIT_METADATA_COLUMNS

ANP_FUEL_PRICE_STATION_OBSERVATION_COLUMNS = [
    "observation_id",
    "ref_date",
    "available_date",
    "availability_policy",
    "region",
    "state",
    "municipality",
    "station_name",
    "station_cnpj",
    "product",
    "sale_price",
    "purchase_price",
    "unit",
    "brand",
    "resource_family",
    "has_sale_price",
    "has_purchase_price",
    "source_version",
]

ANP_FUEL_PRICE_GROUP_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "group_type",
    "group_value",
    "product",
    "feature_id",
    "sale_price",
    "purchase_price",
    "station_count",
    "sale_price_count",
    "purchase_price_count",
    "unit",
    "source_version",
]

ANP_FUEL_SALES_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "year",
    "month",
    "region",
    "state",
    "product",
    "sales_volume_m3",
    "unit",
    "has_sales_volume_m3",
    "source_version",
]

ANP_FUEL_SALES_GROUP_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "group_type",
    "group_value",
    "product",
    "feature_id",
    "sales_volume_m3",
    "sales_volume_count",
    "state_count",
    "unit",
    "source_version",
]

ANP_OIL_GAS_PRODUCTION_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "year",
    "month",
    "region",
    "state",
    "location",
    "product",
    "metric_type",
    "metric_value",
    "unit",
    "resource_family",
    "has_metric_value",
    "source_version",
]

ANP_OIL_GAS_GROUP_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "group_type",
    "group_value",
    "location",
    "product",
    "metric_type",
    "feature_id",
    "metric_value",
    "metric_value_count",
    "state_count",
    "unit",
    "source_version",
]

ANP_STATE_ASOF_DAILY_COLUMNS = [
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

ANP_DAILY_LONG_COLUMNS = [
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

ANP_FUEL_FEATURE_DAILY_COLUMNS = [*ANP_DAILY_LONG_COLUMNS, *PIT_METADATA_COLUMNS]

PANEL_PRIMARY_KEYS = {
    "fuel_price_station_observation": ["observation_id"],
    "fuel_price_group_observation": ["ref_date", "group_type", "group_value", "product"],
    "fuel_sales_observation": ["ref_date", "state", "product"],
    "fuel_sales_group_observation": ["ref_date", "group_type", "group_value", "product"],
    "oil_gas_production_observation": ["ref_date", "state", "location", "metric_type"],
    "oil_gas_group_observation": [
        "ref_date",
        "group_type",
        "group_value",
        "location",
        "product",
        "metric_type",
    ],
    "state_asof_daily": ["ref_date", "source_family", "feature_id", "value_name"],
    "fuel_feature_daily": ["ref_date", "source_family", "feature_id", "value_name"],
    "daily_long": ["ref_date", "source_family", "feature_id", "value_name"],
}
