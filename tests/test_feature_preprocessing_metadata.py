from __future__ import annotations

from copy import deepcopy

import polars as pl
import pytest
from pydantic import ValidationError

from bralpha.modeling.feature_metadata import (
    METADATA_COLUMNS,
    FeaturePreprocessingConfig,
    FeaturePreprocessingRule,
    FeatureRuleSelector,
    annotate_feature_frame,
    load_feature_preprocessing_config,
    match_feature_rule,
)


def test_feature_preprocessing_config_loads_and_fixture_rows_cover_every_rule(repo_root):
    config = load_feature_preprocessing_config(repo_root)
    rule_ids = {rule.rule_id for rule in config.rules}

    assert len(config.rules) > 50
    assert rule_ids == set(_FEATURE_ROWS_BY_RULE_ID)

    for rule_id, row in _FEATURE_ROWS_BY_RULE_ID.items():
        assert match_feature_rule(row, config).rule_id == rule_id


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda data: data["rules"].append({**data["rules"][0], "priority": 99999}),
            "rule_id",
        ),
        (
            lambda data: data["rules"].append(
                {**data["rules"][0], "rule_id": "duplicate_priority"}
            ),
            "priority",
        ),
        (
            lambda data: data["rules"][0]["preprocessing"].update(
                {"transform": "full_sample_leaky_scale"}
            ),
            "transform",
        ),
        (
            lambda data: data["rules"][0]["preprocessing"].update(
                {"fit_scope": "full_sample"}
            ),
            "fit_scope",
        ),
        (
            lambda data: data["rules"][0].update(
                {"model_default": False, "model_default_reason": ""}
            ),
            "model_default",
        ),
        (
            lambda data: data["rules"][0].update({"selector": {}}),
            "selector",
        ),
        (
            lambda data: data["rules"][0].update(
                {
                    "selector": {
                        "panel": "continuous_futures_daily",
                        "value_name": "settlement",
                        "value_name_regex": "settlement",
                    }
                }
            ),
            "value_name",
        ),
        (
            lambda data: data["rules"][0].update(
                {"selector": {"source_family": "fred", "feature_id_regex": "["}}
            ),
            "regex",
        ),
    ],
)
def test_feature_preprocessing_config_validation(repo_root, mutation, match):
    data = load_feature_preprocessing_config(repo_root).model_dump(mode="json")
    mutation(data)

    with pytest.raises(ValidationError, match=match):
        FeaturePreprocessingConfig.model_validate(data)


def test_selector_rejects_empty_and_ambiguous_forms():
    with pytest.raises(ValidationError, match="selector"):
        FeatureRuleSelector.model_validate({})

    with pytest.raises(ValidationError, match="value_name"):
        FeatureRuleSelector.model_validate(
            {"source_family": "fred", "value_name": "value", "value_name_regex": "value"}
        )


def test_unknown_feature_row_fails_closed(repo_root):
    config = load_feature_preprocessing_config(repo_root)

    with pytest.raises(ValueError, match="No feature preprocessing rule"):
        match_feature_rule(
            {
                "source_family": "brand_new_source",
                "feature_id": "brand_new_source|x",
                "value_name": "value",
                "unit": "index",
            },
            config,
        )


def test_overlapping_rules_are_errors_not_priority_resolved(repo_root):
    config = load_feature_preprocessing_config(repo_root)
    data = config.model_dump(mode="json")
    duplicate = deepcopy(data["rules"][0])
    duplicate["rule_id"] = "intentional_overlap"
    duplicate["priority"] = 99999
    data["rules"].append(duplicate)
    overlap_config = FeaturePreprocessingConfig.model_validate(data)

    with pytest.raises(ValueError, match="Multiple feature preprocessing rules"):
        match_feature_rule(_FEATURE_ROWS_BY_RULE_ID[data["rules"][0]["rule_id"]], overlap_config)


def test_annotate_feature_frame_adds_metadata_columns_without_transforming_values(repo_root):
    config = load_feature_preprocessing_config(repo_root)
    frame = pl.DataFrame(
        [
            {
                "source_family": "fred",
                "feature_id": "fred|dgs10",
                "value_name": "value",
                "unit": "percent",
                "value": 4.5,
            },
            {
                "source_family": "focus",
                "feature_id": "focus:ipca",
                "value_name": "min_value",
                "unit": None,
                "value": 2.0,
            },
        ]
    )

    annotated = annotate_feature_frame(frame, config)

    assert set(METADATA_COLUMNS).issubset(annotated.columns)
    assert annotated["value"].to_list() == [4.5, 2.0]
    assert annotated["preprocessing_rule_id"].to_list() == [
        "fred_rate_percent_levels",
        "bcb_focus_min_max",
    ]
    assert annotated["preprocess_transform"].to_list() == ["percent_to_bp", "identity"]
    assert annotated["model_default"].to_list() == [False, False]


def test_key_model_default_decisions(repo_root):
    config = load_feature_preprocessing_config(repo_root)

    assert (
        _rule(config, panel="di_curve_grid_daily", value_name="curve_value").model_default
        is False
    )
    assert (
        _rule(config, panel="di_curve_grid_daily", value_name="implied_annual_rate").model_default
        is False
    )
    assert (
        _rule(config, panel="di_curve_grid_daily", value_name="discount_factor").model_default
        is False
    )
    assert (
        _rule(config, panel="di_curve_grid_daily", value_name="implied_annual_rate_bp")
        .model_default
        is False
    )
    assert (
        _rule(config, panel="di_curve_grid_daily", value_name="log_discount_factor")
        .model_default
        is False
    )
    assert (
        _rule(
            config,
            source_family="b3_di_curve_feature",
            feature_id="b3_di_curve:DI1:252bd",
            value_name="rate_level_bp",
        ).model_default
        is True
    )
    assert (
        _rule(
            config,
            source_family="b3_di_curve_feature",
            feature_id="b3_di_curve:DI1:252bd",
            value_name="log_df_change_1bd",
        ).preprocessing.transform
        == "already_return"
    )
    assert (
        _rule(
            config,
            source_family="b3_futures_feature",
            feature_id="b3_futures:DI1_R1",
            value_name="log_return_1bd",
        ).model_default
        is True
    )
    assert (
        _rule(config, panel="continuous_futures_daily", value_name="settlement").model_default
        is False
    )
    assert _rule(config, panel="index_daily", value_name="close").model_default is False
    assert (
        _rule(config, panel="index_composition_daily", value_name="weight").model_default
        is False
    )
    assert _rule(config, panel="listed_market_daily", value_name="close").model_default is False
    assert (
        _rule(
            config,
            source_family="sgs",
            feature_id="sgs:selic_over",
            value_name="value",
        ).model_default
        is False
    )
    assert _rule(config, source_family="focus", value_name="min_value").model_default is False
    assert _rule(config, source_family="focus", value_name="max_value").model_default is False
    assert (
        _rule(
            config,
            source_family="fred_market_feature",
            feature_id="fred_market:dcoilwtico",
            value_name="signed_log_level",
            unit="signed_log_level",
        ).preprocessing.transform
        == "already_log"
    )
    assert (
        _rule(
            config,
            source_family="anp_fuel_price",
            feature_id="anp_fuel_price|all|all|gasolina_c",
            value_name="sale_price",
        ).model_default
        is False
    )
    assert (
        _rule(
            config,
            source_family="fred_market_feature",
            feature_id="fred_market:dcoilwtico",
            value_name="signed_log_change_1bd",
            unit="signed_log_change",
        ).preprocessing.transform
        == "already_return"
    )
    assert (
        _rule(
            config,
            source_family="ons_ear_subsystem",
            feature_id="ons_ear_subsystem|se",
            value_name="stored_energy_percent",
        ).model_default
        is False
    )
    assert (
        _rule(
            config,
            source_family="cvm_fund_state",
            feature_id="cvm_fund_group|all|all",
            value_name="nav",
        ).model_default
        is False
    )
    assert (
        _rule(
            config,
            source_family="novo_caged_movements",
            feature_id="novo_caged_movements|all|admitidos",
            value_name="movement_count",
        ).model_default
        is False
    )
    assert (
        _rule(
            config,
            source_family="receita_tax_collection",
            feature_id="receita_tax_collection|all|principal|001_irpj",
            value_name="collection_amount_brl",
        ).model_default
        is False
    )


def test_staleness_and_ons_ena_unit_specific_rules(repo_root):
    config = load_feature_preprocessing_config(repo_root)

    staleness = _rule(
        config,
        source_family="bcb_sgs_feature",
        feature_id="bcb_sgs_feature:inflation:ipca_staleness_days",
        value_name="ipca_staleness_days",
        unit="days",
    )
    assert staleness.preprocessing.transform == "clip_only"
    assert staleness.preprocessing.hard_min == 0
    assert staleness.preprocessing.hard_max == 756

    percent_mlt = _rule(
        config,
        source_family="ons_ena_subsystem",
        feature_id="ons_ena_subsystem|se|percent_mlt",
        value_name="ena_value",
        unit="percent_mlt",
    )
    physical = _rule(
        config,
        source_family="ons_ena_subsystem",
        feature_id="ons_ena_subsystem|se|mwmed",
        value_name="ena_value",
        unit="MWmed",
    )

    assert percent_mlt.preprocessing.transform == "identity"
    assert percent_mlt.preprocessing.hard_max == 300
    assert physical.preprocessing.transform == "log1p_positive"


def test_rule_model_has_all_required_fields(repo_root):
    config = load_feature_preprocessing_config(repo_root)

    for rule in config.rules:
        assert FeaturePreprocessingRule.model_validate(rule.model_dump(mode="json"))
        assert rule.preprocessing.fit_scope == "train_only"
        if rule.model_default is False:
            assert rule.model_default_reason.strip()


def test_feature_upgrade_docs_cover_upgraded_families(repo_root):
    upgrade_doc = (repo_root / "docs" / "FEATURE_UPGRADES_CONTRACT.md").read_text()
    preprocessing_doc = (
        repo_root / "docs" / "FEATURE_PREPROCESSING_METADATA_CONTRACT.md"
    ).read_text()

    for source_family in _UPGRADED_SOURCE_FAMILIES:
        assert source_family in upgrade_doc
        assert source_family in preprocessing_doc

    assert "values remain unpreprocessed" in upgrade_doc
    assert "does not apply transforms in the feature builders" in upgrade_doc


def _rule(
    config: FeaturePreprocessingConfig,
    *,
    panel: str | None = None,
    source_family: str | None = None,
    feature_id: str | None = None,
    value_name: str,
    unit: str | None = None,
):
    row = {"value_name": value_name}
    if panel is not None:
        row["panel"] = panel
    if source_family is not None:
        row["source_family"] = source_family
    if feature_id is not None:
        row["feature_id"] = feature_id
    if unit is not None:
        row["unit"] = unit
    return match_feature_rule(row, config)


_FEATURE_ROWS_BY_RULE_ID = {
    "b3_continuous_futures_settlement": {
        "panel": "continuous_futures_daily",
        "value_name": "settlement",
    },
    "b3_continuous_futures_quote_diff": {
        "panel": "continuous_futures_daily",
        "value_name": "quote_diff_1d",
    },
    "b3_continuous_futures_quote_return": {
        "panel": "continuous_futures_daily",
        "value_name": "quote_pct_change_1d",
    },
    "b3_continuous_futures_liquidity": {
        "panel": "continuous_futures_daily",
        "value_name": "volume",
    },
    "b3_continuous_futures_maturity_distance": {
        "panel": "continuous_futures_daily",
        "value_name": "business_days_to_maturity",
    },
    "b3_continuous_futures_flags": {
        "panel": "continuous_futures_daily",
        "value_name": "is_roll_date",
    },
    "b3_di_grid_implied_annual_rate_bp": {
        "panel": "di_curve_grid_daily",
        "value_name": "implied_annual_rate_bp",
    },
    "b3_di_grid_log_discount_factor": {
        "panel": "di_curve_grid_daily",
        "value_name": "log_discount_factor",
    },
    "b3_di_grid_tenor_business_days": {
        "panel": "di_curve_grid_daily",
        "value_name": "tenor_business_days",
    },
    "b3_di_grid_flags": {"panel": "di_curve_grid_daily", "value_name": "is_interpolated"},
    "b3_di_grid_redundant_levels": {
        "panel": "di_curve_grid_daily",
        "value_name": "curve_value",
    },
    "b3_di_contract_raw_settlement_pu": {
        "panel": "di_curve_contract_daily",
        "value_name": "raw_settlement_pu",
    },
    "b3_di_contract_implied_annual_rate_bp": {
        "panel": "di_curve_contract_daily",
        "value_name": "implied_annual_rate_bp",
    },
    "b3_di_contract_log_discount_factor": {
        "panel": "di_curve_contract_daily",
        "value_name": "log_discount_factor",
    },
    "b3_di_contract_maturity_distance": {
        "panel": "di_curve_contract_daily",
        "value_name": "business_days_to_maturity",
    },
    "b3_di_contract_liquidity": {
        "panel": "di_curve_contract_daily",
        "value_name": "open_interest",
    },
    "b3_di_contract_flags": {
        "panel": "di_curve_contract_daily",
        "value_name": "is_observed",
    },
    "b3_di_contract_rate_changes": {
        "panel": "di_curve_contract_daily",
        "value_name": "implied_annual_rate_bp_change_1d",
    },
    "b3_di_contract_log_discount_factor_change": {
        "panel": "di_curve_contract_daily",
        "value_name": "log_discount_factor_change_1d",
    },
    "b3_di_contract_redundant_levels": {
        "panel": "di_curve_contract_daily",
        "value_name": "implied_annual_rate",
    },
    "b3_di_contract_redundant_curve_pct_change": {
        "panel": "di_curve_contract_daily",
        "value_name": "curve_value_pct_change_1d",
    },
    "b3_index_daily_prices": {"panel": "index_daily", "value_name": "close"},
    "b3_index_daily_liquidity": {"panel": "index_daily", "value_name": "volume"},
    "b3_listed_market_price_levels": {
        "panel": "listed_market_daily",
        "value_name": "close",
    },
    "b3_listed_market_liquidity": {
        "panel": "listed_market_daily",
        "value_name": "number_of_trades",
    },
    "b3_index_composition_weight": {
        "panel": "index_composition_daily",
        "value_name": "weight",
    },
    "b3_index_composition_theoretical_quantity": {
        "panel": "index_composition_daily",
        "value_name": "theoretical_quantity",
    },
    "b3_di_curve_feature_rate_levels_shapes_forwards": {
        "source_family": "b3_di_curve_feature",
        "feature_id": "b3_di_curve:DI1:shape",
        "value_name": "forward_21_63_bp",
        "unit": "bp",
    },
    "b3_di_curve_feature_rate_changes": {
        "source_family": "b3_di_curve_feature",
        "feature_id": "b3_di_curve:DI1:252bd",
        "value_name": "rate_change_5bd_bp",
        "unit": "bp",
    },
    "b3_di_curve_feature_log_discount_factor": {
        "source_family": "b3_di_curve_feature",
        "feature_id": "b3_di_curve:DI1:252bd",
        "value_name": "log_discount_factor",
        "unit": "log_discount_factor",
    },
    "b3_di_curve_feature_log_df_changes": {
        "source_family": "b3_di_curve_feature",
        "feature_id": "b3_di_curve:DI1:252bd",
        "value_name": "log_df_change_5bd",
        "unit": "log_change",
    },
    "b3_di_curve_feature_flags": {
        "source_family": "b3_di_curve_feature",
        "feature_id": "b3_di_curve:DI1:252bd",
        "value_name": "is_interpolated",
        "unit": "flag",
    },
    "b3_futures_feature_log_settlement": {
        "source_family": "b3_futures_feature",
        "feature_id": "b3_futures:DI1_R1",
        "value_name": "log_settlement",
        "unit": "log_quote",
    },
    "b3_futures_feature_log_returns": {
        "source_family": "b3_futures_feature",
        "feature_id": "b3_futures:DI1_R1",
        "value_name": "log_return_5bd",
        "unit": "log_return",
    },
    "b3_futures_feature_realized_vol": {
        "source_family": "b3_futures_feature",
        "feature_id": "b3_futures:DI1_R1",
        "value_name": "realized_vol_21bd_ann",
        "unit": "annualized_log_vol",
    },
    "b3_futures_feature_log_liquidity": {
        "source_family": "b3_futures_feature",
        "feature_id": "b3_futures:DI1_R1",
        "value_name": "volume_log1p",
        "unit": "log_count",
    },
    "b3_futures_feature_ratios_maturity": {
        "source_family": "b3_futures_feature",
        "feature_id": "b3_futures:DI1_R1",
        "value_name": "volume_open_interest_ratio",
        "unit": "ratio",
    },
    "b3_futures_feature_flags": {
        "source_family": "b3_futures_feature",
        "feature_id": "b3_futures:DI1_R1",
        "value_name": "is_roll_date",
        "unit": "flag",
    },
    "b3_index_feature_logs": {
        "source_family": "b3_index_feature",
        "feature_id": "b3_index:IBOV",
        "value_name": "log_close",
        "unit": "log_points",
    },
    "b3_index_feature_returns": {
        "source_family": "b3_index_feature",
        "feature_id": "b3_index:IBOV",
        "value_name": "log_return_21bd",
        "unit": "log_return",
    },
    "b3_index_feature_vol_drawdown": {
        "source_family": "b3_index_feature",
        "feature_id": "b3_index:IBOV",
        "value_name": "close_drawdown_252bd_pct",
        "unit": "percent",
    },
    "b3_index_composition_feature_counts": {
        "source_family": "b3_index_composition_feature",
        "feature_id": "b3_index_composition:IBOV",
        "value_name": "constituent_count",
        "unit": "count",
    },
    "b3_index_composition_feature_concentration": {
        "source_family": "b3_index_composition_feature",
        "feature_id": "b3_index_composition:IBOV",
        "value_name": "hhi_weight",
        "unit": "hhi",
    },
    "bcb_raw_sgs_engineered_replacements": {
        "source_family": "sgs",
        "feature_id": "sgs:selic_over",
        "value_name": "value",
        "unit": "percent_annualized",
    },
    "bcb_sgs_feature_selic_rate_levels": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:rates:selic_over_level_pa",
        "value_name": "selic_over_level_pa",
        "unit": "percent_pa",
    },
    "bcb_sgs_feature_selic_bp_changes": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:rates:selic_target_change_1bd_bp",
        "value_name": "selic_target_change_1bd_bp",
        "unit": "bp",
    },
    "bcb_sgs_feature_policy_step_flag": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:rates:selic_policy_step_flag",
        "value_name": "selic_policy_step_flag",
        "unit": "flag",
    },
    "bcb_sgs_feature_inflation_percent": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:inflation:ipca_monthly_pct",
        "value_name": "ipca_monthly_pct",
        "unit": "percent",
    },
    "bcb_sgs_feature_staleness_days": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:inflation:ipca_staleness_days",
        "value_name": "ipca_staleness_days",
        "unit": "days",
    },
    "bcb_sgs_feature_reserves_raw_level": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:external_reserves:reserves_usd_mn_level",
        "value_name": "reserves_usd_mn_level",
        "unit": "usd_mn",
    },
    "bcb_sgs_feature_reserves_log_level": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:external_reserves:reserves_log_level",
        "value_name": "reserves_log_level",
        "unit": "log_usd_mn",
    },
    "bcb_sgs_feature_reserves_log_changes": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:external_reserves:reserves_log_change_1bd",
        "value_name": "reserves_log_change_1bd",
        "unit": "log_change",
    },
    "bcb_sgs_feature_reserves_percent_changes": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:external_reserves:reserves_pct_change_20bd",
        "value_name": "reserves_pct_change_20bd",
        "unit": "percent",
    },
    "bcb_sgs_feature_real_policy_rates": {
        "source_family": "bcb_sgs_feature",
        "feature_id": "bcb_sgs_feature:rates:real_policy_rate_12m_ipca_bp",
        "value_name": "real_policy_rate_12m_ipca_bp",
        "unit": "basis_points",
    },
    "bcb_ptax_fx_rates": {
        "source_family": "ptax",
        "feature_id": "ptax:USD",
        "value_name": "bid_rate",
        "unit": None,
    },
    "bcb_ptax_feature_logs": {
        "source_family": "bcb_ptax_feature",
        "feature_id": "bcb_ptax:USD",
        "value_name": "log_mid_rate",
        "unit": "log_fx_rate",
    },
    "bcb_ptax_feature_returns": {
        "source_family": "bcb_ptax_feature",
        "feature_id": "bcb_ptax:USD",
        "value_name": "log_return_5bd",
        "unit": "log_return",
    },
    "bcb_ptax_feature_vol_spreads": {
        "source_family": "bcb_ptax_feature",
        "feature_id": "bcb_ptax:USD",
        "value_name": "bid_ask_spread_bp",
        "unit": "bp",
    },
    "bcb_ptax_feature_raw_mid_levels": {
        "source_family": "bcb_ptax_feature",
        "feature_id": "bcb_ptax:USD",
        "value_name": "mid_rate",
        "unit": "brl_per_currency",
    },
    "bcb_focus_mean_median": {
        "source_family": "focus",
        "feature_id": "focus:ipca",
        "value_name": "mean",
        "unit": None,
    },
    "bcb_focus_std_dev": {
        "source_family": "focus",
        "feature_id": "focus:ipca",
        "value_name": "std_dev",
        "unit": None,
    },
    "bcb_focus_respondents": {
        "source_family": "focus",
        "feature_id": "focus:ipca",
        "value_name": "respondents",
        "unit": None,
    },
    "bcb_focus_min_max": {
        "source_family": "focus",
        "feature_id": "focus:ipca",
        "value_name": "min_value",
        "unit": None,
    },
    "bcb_focus_feature_levels_spreads": {
        "source_family": "bcb_focus_feature",
        "feature_id": "bcb_focus:ipca",
        "value_name": "median_level",
        "unit": None,
    },
    "bcb_focus_feature_revisions": {
        "source_family": "bcb_focus_feature",
        "feature_id": "bcb_focus:ipca",
        "value_name": "median_revision_5bd",
        "unit": None,
    },
    "bcb_focus_feature_respondents_log": {
        "source_family": "bcb_focus_feature",
        "feature_id": "bcb_focus:ipca",
        "value_name": "respondents_log1p",
        "unit": "log_count",
    },
    "bcb_focus_feature_std_dev_log": {
        "source_family": "bcb_focus_feature",
        "feature_id": "bcb_focus:ipca",
        "value_name": "std_dev_log1p",
        "unit": "log_value",
    },
    "bcb_focus_feature_dispersion_ratio": {
        "source_family": "bcb_focus_feature",
        "feature_id": "bcb_focus:ipca",
        "value_name": "dispersion_to_abs_median",
        "unit": "ratio",
    },
    "fred_rate_percent_levels": {
        "source_family": "fred",
        "feature_id": "fred|dgs10",
        "value_name": "value",
        "unit": "percent",
    },
    "fred_positive_index_fx_commodity_levels": {
        "source_family": "fred",
        "feature_id": "fred|sp500",
        "value_name": "value",
        "unit": "index",
    },
    "fred_vix_positive_volatility": {
        "source_family": "fred",
        "feature_id": "fred|vixcls",
        "value_name": "value",
        "unit": "index",
    },
    "fred_oil_signed_price_levels": {
        "source_family": "fred",
        "feature_id": "fred|dcoilwtico",
        "value_name": "value",
        "unit": "usd_per_barrel",
    },
    "fred_rate_feature_levels_spreads": {
        "source_family": "fred_rate_feature",
        "feature_id": "fred_rate:dgs10",
        "value_name": "level_bp",
        "unit": "bp",
    },
    "fred_rate_feature_changes": {
        "source_family": "fred_rate_feature",
        "feature_id": "fred_rate:dgs10",
        "value_name": "change_5bd_bp",
        "unit": "bp",
    },
    "fred_market_feature_logs": {
        "source_family": "fred_market_feature",
        "feature_id": "fred_market:sp500",
        "value_name": "log_level",
        "unit": "log_level",
    },
    "fred_market_feature_returns_changes": {
        "source_family": "fred_market_feature",
        "feature_id": "fred_market:sp500",
        "value_name": "log_return_5bd",
        "unit": "log_return",
    },
    "fred_market_feature_realized_vol": {
        "source_family": "fred_market_feature",
        "feature_id": "fred_market:sp500",
        "value_name": "realized_vol_21bd_ann",
        "unit": "annualized_log_vol",
    },
    "br_rv_cross_feature_bp": {
        "source_family": "br_rv_cross_feature",
        "feature_id": "br_rv_cross:rates",
        "value_name": "brl_di_2y_minus_ust_2y_bp",
        "unit": "bp",
    },
    "br_rv_cross_feature_returns": {
        "source_family": "br_rv_cross_feature",
        "feature_id": "br_rv_cross:fx",
        "value_name": "brl_fx_idiosyncratic_return_5bd",
        "unit": "log_return",
    },
    "ibge_sidra_inflation_percent": {
        "source_family": "ibge_sidra",
        "feature_id": "ibge_sidra:ipca:7060:63:1:315=7169",
        "value_name": "value",
        "unit": "%",
    },
    "ibge_sidra_gdp_volume_change": {
        "source_family": "ibge_sidra",
        "feature_id": "ibge_sidra:gdp_volume_change:5932:6561:1",
        "value_name": "value",
        "unit": "%",
    },
    "ibge_sidra_positive_index_levels": {
        "source_family": "ibge_sidra",
        "feature_id": "ibge_sidra:pim_industrial_production:8888:12606:1",
        "value_name": "value",
        "unit": "index",
    },
    "ibge_sidra_gdp_current_values": {
        "source_family": "ibge_sidra",
        "feature_id": "ibge_sidra:gdp_current_values:5932:37:1",
        "value_name": "value",
        "unit": "BRL",
    },
    "ibge_sidra_labor_rate_percent": {
        "source_family": "ibge_sidra",
        "feature_id": "ibge_sidra:pnad_unemployment_rate:6381:4099:1",
        "value_name": "value",
        "unit": "%",
    },
    "ibge_sidra_labor_positive_levels": {
        "source_family": "ibge_sidra",
        "feature_id": "ibge_sidra:pnad_real_income:6390:5932:1",
        "value_name": "value",
        "unit": "BRL",
    },
    "ibge_sidra_feature_percent_levels_sums": {
        "source_family": "ibge_sidra_feature",
        "feature_id": "ibge_sidra_feature:ibge_sidra:ipca:7060:63:1:315=7169",
        "value_name": "trailing_12obs_sum_pct",
        "unit": "percent",
    },
    "ibge_sidra_feature_percentage_point_changes": {
        "source_family": "ibge_sidra_feature",
        "feature_id": "ibge_sidra_feature:ibge_sidra:gdp_volume_change:5932:6561:1",
        "value_name": "yoy_change_pp",
        "unit": "percentage_points",
    },
    "ibge_sidra_feature_log_levels": {
        "source_family": "ibge_sidra_feature",
        "feature_id": "ibge_sidra_feature:ibge_sidra:pim_industrial_production:8888:12606:1",
        "value_name": "log_level",
        "unit": "log_level",
    },
    "ibge_sidra_feature_log_changes": {
        "source_family": "ibge_sidra_feature",
        "feature_id": "ibge_sidra_feature:ibge_sidra:pim_industrial_production:8888:12606:1",
        "value_name": "yoy_log_change",
        "unit": "log_change",
    },
    "tesouro_direto_prices_rates_rates": {
        "source_family": "tesouro_direto_prices_rates",
        "feature_id": "tesouro_direto_prices_rates|tesouro_selic",
        "value_name": "buy_rate",
        "unit": "percent",
    },
    "tesouro_direto_prices_rates_prices": {
        "source_family": "tesouro_direto_prices_rates",
        "feature_id": "tesouro_direto_prices_rates|tesouro_selic",
        "value_name": "buy_price",
        "unit": "BRL",
    },
    "tesouro_direto_flows_counts": {
        "source_family": "tesouro_direto_flows",
        "feature_id": "tesouro_direto_flows|sale|null|tesouro_selic",
        "value_name": "quantity",
        "unit": "count",
    },
    "tesouro_direto_flows_value": {
        "source_family": "tesouro_direto_flows",
        "feature_id": "tesouro_direto_flows|sale|null|tesouro_selic",
        "value_name": "value",
        "unit": "BRL",
    },
    "tesouro_direto_stock_counts": {
        "source_family": "tesouro_direto_stock",
        "feature_id": "tesouro_direto_stock|tesouro_selic",
        "value_name": "investor_count",
        "unit": "count",
    },
    "tesouro_direto_stock_value": {
        "source_family": "tesouro_direto_stock",
        "feature_id": "tesouro_direto_stock|tesouro_selic",
        "value_name": "stock_value",
        "unit": "BRL",
    },
    "tesouro_dpf_stock_value": {
        "source_family": "tesouro_dpf_stock",
        "feature_id": "tesouro_dpf_stock|dpmfi|lft|selic|0_a_1_ano",
        "value_name": "stock_value",
        "unit": "BRL",
    },
    "tesouro_feature_rates": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_price:tesouro_direto_prices_rates|tesouro_selic",
        "value_name": "mid_rate_bp",
        "unit": "bp",
    },
    "tesouro_feature_price_stock_logs": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_price:tesouro_direto_prices_rates|tesouro_selic",
        "value_name": "log_mid_price",
        "unit": "log_brl",
    },
    "tesouro_feature_price_returns": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_price:tesouro_direto_prices_rates|tesouro_selic",
        "value_name": "price_log_return_5bd",
        "unit": "log_return",
    },
    "tesouro_feature_raw_mid_price": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_price:tesouro_direto_prices_rates|tesouro_selic",
        "value_name": "mid_price",
        "unit": "brl",
    },
    "tesouro_feature_flow_amounts": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_flow:tesouro_direto_flows|tesouro_selic",
        "value_name": "net_flow_value",
        "unit": "brl",
    },
    "tesouro_feature_counts": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_flow:tesouro_direto_flows|tesouro_selic",
        "value_name": "sales_quantity",
        "unit": "count",
    },
    "tesouro_feature_shares_ratios": {
        "source_family": "tesouro_feature",
        "feature_id": "tesouro_flow:tesouro_direto_flows|tesouro_selic",
        "value_name": "redemption_share_pct",
        "unit": "percent",
    },
    "anp_fuel_price_levels": {
        "source_family": "anp_fuel_price",
        "feature_id": "anp_fuel_price|all|all|gasolina_c",
        "value_name": "sale_price",
        "unit": "BRL/l",
    },
    "anp_fuel_price_counts": {
        "source_family": "anp_fuel_price",
        "feature_id": "anp_fuel_price|all|all|gasolina_c",
        "value_name": "station_count",
        "unit": "stations",
    },
    "anp_fuel_sales_volume": {
        "source_family": "anp_fuel_sales",
        "feature_id": "anp_fuel_sales|all|all|gasolina_c",
        "value_name": "sales_volume_m3",
        "unit": "m3",
    },
    "anp_fuel_sales_counts": {
        "source_family": "anp_fuel_sales",
        "feature_id": "anp_fuel_sales|all|all|gasolina_c",
        "value_name": "sales_volume_count",
        "unit": "observations",
    },
    "anp_oil_gas_metric_value": {
        "source_family": "anp_oil_gas",
        "feature_id": "anp_oil_gas|all|all|mar|petroleo|petroleum_production",
        "value_name": "metric_value",
        "unit": "m3",
    },
    "anp_oil_gas_counts": {
        "source_family": "anp_oil_gas",
        "feature_id": "anp_oil_gas|all|all|mar|petroleo|petroleum_production",
        "value_name": "metric_value_count",
        "unit": "observations",
    },
    "anp_fuel_feature_logs": {
        "source_family": "anp_fuel_feature",
        "feature_id": "anp_fuel:anp_fuel_price|state|sp|gasolina_comum",
        "value_name": "sale_price_log",
        "unit": "log_price",
    },
    "anp_fuel_feature_log_changes": {
        "source_family": "anp_fuel_feature",
        "feature_id": "anp_fuel:anp_fuel_price|state|sp|gasolina_comum",
        "value_name": "sale_price_log_change_1obs",
        "unit": "log_return",
    },
    "anp_fuel_feature_ratios_spreads": {
        "source_family": "anp_fuel_feature",
        "feature_id": "anp_fuel:state|sp:cross_product",
        "value_name": "ethanol_gasoline_parity",
        "unit": "ratio",
    },
    "ons_ear_stored_energy_levels": {
        "source_family": "ons_ear_subsystem",
        "feature_id": "ons_ear_subsystem|se",
        "value_name": "stored_energy_mwmes",
        "unit": "MWmes",
    },
    "ons_ear_stored_energy_percent": {
        "source_family": "ons_ear_subsystem",
        "feature_id": "ons_ear_subsystem|se",
        "value_name": "stored_energy_percent",
        "unit": "percent",
    },
    "ons_ena_percent_mlt": {
        "source_family": "ons_ena_subsystem",
        "feature_id": "ons_ena_subsystem|se|percent_mlt",
        "value_name": "ena_value",
        "unit": "percent_mlt",
    },
    "ons_ena_positive_level": {
        "source_family": "ons_ena_subsystem",
        "feature_id": "ons_ena_subsystem|se|mwmed",
        "value_name": "ena_value",
        "unit": "MWmed",
    },
    "ons_load_daily_load": {
        "source_family": "ons_load_daily",
        "feature_id": "ons_load_daily|se",
        "value_name": "load_mwmed",
        "unit": "MWmed",
    },
    "ons_cmo_weekly_cost": {
        "source_family": "ons_cmo_weekly",
        "feature_id": "ons_cmo_weekly|se",
        "value_name": "cmo_brl_mwh",
        "unit": "BRL/MWh",
    },
    "ons_energy_balance_positive_generation": {
        "source_family": "ons_energy_balance_daily",
        "feature_id": "ons_energy_balance_daily|se",
        "value_name": "hydro_generation_mwmed",
        "unit": "MWmed",
    },
    "ons_energy_balance_interchange": {
        "source_family": "ons_energy_balance_daily",
        "feature_id": "ons_energy_balance_daily|se",
        "value_name": "interchange_mwmed",
        "unit": "MWmed",
    },
    "ons_energy_balance_hour_count": {
        "source_family": "ons_energy_balance_daily",
        "feature_id": "ons_energy_balance_daily|se",
        "value_name": "hour_count",
        "unit": "count",
    },
    "ons_interchange_signed_flows": {
        "source_family": "ons_interchange_daily",
        "feature_id": "ons_interchange_daily|se|s",
        "value_name": "programmed_interchange_mwmed",
        "unit": "MWmed",
    },
    "ons_interchange_hour_count": {
        "source_family": "ons_interchange_daily",
        "feature_id": "ons_interchange_daily|se|s",
        "value_name": "hour_count",
        "unit": "count",
    },
    "ons_power_feature_percent_levels_shares": {
        "source_family": "ons_power_feature",
        "feature_id": "ons_power:ons_energy_balance_daily|se",
        "value_name": "hydro_generation_share_pct",
        "unit": "percent",
    },
    "ons_power_feature_percent_changes_z": {
        "source_family": "ons_power_feature",
        "feature_id": "ons_power:ons_ear_subsystem|se",
        "value_name": "stored_energy_percent_seasonal_z",
        "unit": "z_score",
    },
    "ons_power_feature_log_levels": {
        "source_family": "ons_power_feature",
        "feature_id": "ons_power:ons_ear_subsystem|se",
        "value_name": "stored_energy_mwmes_log",
        "unit": "log_mwmed",
    },
    "ons_power_feature_log_changes": {
        "source_family": "ons_power_feature",
        "feature_id": "ons_power:ons_cmo_weekly|se",
        "value_name": "cmo_log_change_1obs",
        "unit": "log_return",
    },
    "cvm_fund_flows_amounts": {
        "source_family": "cvm_fund_flows",
        "feature_id": "cvm_fund_group|all|all",
        "value_name": "subscriptions",
        "unit": "BRL",
    },
    "cvm_fund_flows_counts": {
        "source_family": "cvm_fund_flows",
        "feature_id": "cvm_fund_group|all|all",
        "value_name": "fund_count",
        "unit": "funds",
    },
    "cvm_fund_state_amounts": {
        "source_family": "cvm_fund_state",
        "feature_id": "cvm_fund_group|all|all",
        "value_name": "nav",
        "unit": "BRL",
    },
    "cvm_fund_state_counts": {
        "source_family": "cvm_fund_state",
        "feature_id": "cvm_fund_group|all|all",
        "value_name": "shareholder_count",
        "unit": "shareholders",
    },
    "cvm_fund_feature_logs": {
        "source_family": "cvm_fund_feature",
        "feature_id": "cvm_fund:cvm_fund_group|all|all",
        "value_name": "nav_log",
        "unit": "log_brl",
    },
    "cvm_fund_feature_flows": {
        "source_family": "cvm_fund_feature",
        "feature_id": "cvm_fund:cvm_fund_group|all|all",
        "value_name": "net_flow_brl",
        "unit": "BRL",
    },
    "cvm_fund_feature_ratios": {
        "source_family": "cvm_fund_feature",
        "feature_id": "cvm_fund:cvm_fund_group|all|all",
        "value_name": "net_flow_to_nav_pct",
        "unit": "percent",
    },
    "novo_caged_movement_counts": {
        "source_family": "novo_caged_movements",
        "feature_id": "novo_caged_movements|all|admitidos",
        "value_name": "movement_count",
        "unit": "records",
    },
    "novo_caged_wage_mean": {
        "source_family": "novo_caged_movements",
        "feature_id": "novo_caged_movements|all|admitidos",
        "value_name": "wage_mean",
        "unit": "BRL",
    },
    "novo_caged_contract_hours_mean": {
        "source_family": "novo_caged_movements",
        "feature_id": "novo_caged_movements|all|admitidos",
        "value_name": "contract_hours_mean",
        "unit": "hours",
    },
    "novo_caged_feature_counts": {
        "source_family": "novo_caged_feature",
        "feature_id": "novo_caged:novo_caged_movement|state|sp",
        "value_name": "admissions_count",
        "unit": "jobs",
    },
    "novo_caged_feature_net_jobs": {
        "source_family": "novo_caged_feature",
        "feature_id": "novo_caged:novo_caged_movement|state|sp",
        "value_name": "net_jobs",
        "unit": "jobs",
    },
    "novo_caged_feature_ratios_shares": {
        "source_family": "novo_caged_feature",
        "feature_id": "novo_caged:novo_caged_movement|state|sp",
        "value_name": "state_positive_diffusion_share_pct",
        "unit": "percent",
    },
    "novo_caged_feature_wage": {
        "source_family": "novo_caged_feature",
        "feature_id": "novo_caged:novo_caged_movement|state|sp",
        "value_name": "wage_mean_log",
        "unit": "log_brl",
    },
    "novo_caged_feature_wage_change": {
        "source_family": "novo_caged_feature",
        "feature_id": "novo_caged:novo_caged_movement|state|sp",
        "value_name": "wage_mean_yoy_log_change",
        "unit": "log_return",
    },
    "novo_caged_feature_contract_hours": {
        "source_family": "novo_caged_feature",
        "feature_id": "novo_caged:novo_caged_movement|state|sp",
        "value_name": "contract_hours_mean",
        "unit": "hours",
    },
    "receita_tax_collection_amount": {
        "source_family": "receita_tax_collection",
        "feature_id": "receita_tax_collection|all|principal|001_irpj",
        "value_name": "collection_amount_brl",
        "unit": "BRL",
    },
    "receita_feature_signed_logs": {
        "source_family": "receita_feature",
        "feature_id": "receita:receita_tax_collection|all|principal|001_irpj",
        "value_name": "collection_signed_log",
        "unit": "signed_log_brl",
    },
    "receita_feature_yoy": {
        "source_family": "receita_feature",
        "feature_id": "receita:receita_tax_collection|all|principal|001_irpj",
        "value_name": "real_collection_yoy_pct",
        "unit": "percent",
    },
    "receita_feature_category_share": {
        "source_family": "receita_feature",
        "feature_id": "receita:receita_tax_collection|all|principal|001_irpj",
        "value_name": "category_share_pct",
        "unit": "percent",
    },
}


_UPGRADED_SOURCE_FAMILIES = [
    "b3_di_curve_feature",
    "b3_futures_feature",
    "b3_index_feature",
    "b3_index_composition_feature",
    "bcb_sgs_feature",
    "bcb_ptax_feature",
    "bcb_focus_feature",
    "fred_rate_feature",
    "fred_market_feature",
    "tesouro_feature",
    "br_rv_cross_feature",
    "ibge_sidra_feature",
    "anp_fuel_feature",
    "ons_power_feature",
    "cvm_fund_feature",
    "novo_caged_feature",
    "receita_feature",
]
