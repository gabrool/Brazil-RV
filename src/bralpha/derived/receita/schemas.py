from __future__ import annotations

RECEITA_TAX_COLLECTION_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "year",
    "month",
    "collection_scope",
    "revenue_category",
    "revenue_subcategory",
    "revenue_code",
    "revenue_key",
    "revenue_name",
    "table_kind",
    "collection_amount_brl",
    "unit",
    "source_table",
    "has_collection_amount_brl",
    "source_version",
]

RECEITA_TAX_COLLECTION_FEATURE_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "availability_policy",
    "collection_scope",
    "revenue_category",
    "revenue_subcategory",
    "revenue_code",
    "revenue_key",
    "revenue_name",
    "table_kind",
    "feature_id",
    "collection_amount_brl",
    "unit",
    "source_table",
    "has_collection_amount_brl",
    "source_version",
]

RECEITA_STATE_ASOF_DAILY_COLUMNS = [
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

RECEITA_DAILY_LONG_COLUMNS = [
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

PANEL_PRIMARY_KEYS = {
    "tax_collection_observation": [
        "ref_date",
        "collection_scope",
        "revenue_category",
        "revenue_code",
        "revenue_key",
        "table_kind",
    ],
    "tax_collection_feature_observation": ["ref_date", "feature_id"],
    "state_asof_daily": ["ref_date", "source_family", "feature_id", "value_name"],
    "daily_long": ["ref_date", "source_family", "feature_id", "value_name"],
}
