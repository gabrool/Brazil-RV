from __future__ import annotations

from datetime import date
from math import log1p

import polars as pl
import pytest

from bralpha.derived.cvm.features import build_cvm_fund_feature_daily


def test_cvm_nav_flow_ratios_are_computed_from_pit_inputs():
    ref_date = date(2024, 1, 31)
    flows = pl.DataFrame(
        [
            {
                "ref_date": ref_date,
                "available_date": ref_date,
                "observation_ref_date": ref_date,
                "observation_available_date": ref_date,
                "group_type": "all",
                "group_value": "all",
                "feature_id": "cvm_fund_group|all|all",
                "subscriptions": 100.0,
                "redemptions": 40.0,
                "subscriptions_count": 1,
                "redemptions_count": 1,
                "fund_count": 2,
                "source_version": "fixture",
            }
        ]
    )
    state = pl.DataFrame(
        [
            {
                "ref_date": ref_date,
                "available_date": ref_date,
                "group_type": "all",
                "group_value": "all",
                "feature_id": "cvm_fund_group|all|all",
                "observation_ref_date": ref_date,
                "observation_available_date": ref_date,
                "portfolio_value": 1200.0,
                "nav": 1000.0,
                "shareholder_count": 200.0,
                "fund_count": 2.0,
                "portfolio_value_count": 1,
                "nav_count": 1,
                "shareholder_count_count": 1,
                "is_available": True,
                "is_observed_on_ref_date": True,
                "staleness_days": 0,
                "source_version": "fixture",
            }
        ]
    )

    features = build_cvm_fund_feature_daily(
        fund_flows_daily=flows,
        fund_state_asof_daily=state,
        start=ref_date,
        end=ref_date,
    )

    assert _value(features, "net_flow_brl") == pytest.approx(60.0)
    assert _value(features, "net_flow_to_nav_pct") == pytest.approx(6.0)
    assert _value(features, "gross_flow_to_nav_pct") == pytest.approx(14.0)
    assert _value(features, "redemption_share_pct") == pytest.approx(40.0 / 140.0 * 100.0)


def test_cvm_zero_flows_and_balances_use_log1p_safe_features_and_preserve_metadata():
    ref_date = date(2024, 1, 31)
    metadata = {
        "availability_policy": "cvm_first_seen",
        "availability_basis": "first_seen_timestamp",
        "revision_policy": "revised_with_snapshots",
        "vintage_id": "cvm-v1",
        "model_usable": True,
        "model_usable_reason": "fixture",
    }
    flows = pl.DataFrame(
        [
            {
                "ref_date": ref_date,
                "available_date": ref_date,
                "observation_ref_date": ref_date,
                "observation_available_date": ref_date,
                "group_type": "all",
                "group_value": "all",
                "feature_id": "cvm_fund_group|all|all",
                "subscriptions": 0.0,
                "redemptions": 0.0,
                "subscriptions_count": 1,
                "redemptions_count": 1,
                "fund_count": 2,
                "source_version": "fixture",
                **metadata,
            }
        ]
    )
    state = pl.DataFrame(
        [
            {
                "ref_date": ref_date,
                "available_date": ref_date,
                "group_type": "all",
                "group_value": "all",
                "feature_id": "cvm_fund_group|all|all",
                "observation_ref_date": ref_date,
                "observation_available_date": ref_date,
                "portfolio_value": 0.0,
                "nav": 0.0,
                "shareholder_count": 0.0,
                "fund_count": 2.0,
                "portfolio_value_count": 1,
                "nav_count": 1,
                "shareholder_count_count": 1,
                "is_available": True,
                "is_observed_on_ref_date": True,
                "staleness_days": 0,
                "source_version": "fixture",
                **metadata,
            }
        ]
    )

    features = build_cvm_fund_feature_daily(
        fund_flows_daily=flows,
        fund_state_asof_daily=state,
        start=ref_date,
        end=ref_date,
    )

    assert _value(features, "subscriptions_log") == pytest.approx(log1p(0.0))
    assert _value(features, "redemptions_log") == pytest.approx(log1p(0.0))
    assert _value(features, "nav_log") == pytest.approx(log1p(0.0))
    assert _value(features, "portfolio_value_log") == pytest.approx(log1p(0.0))
    row = features.filter(pl.col("value_name") == "nav_log").row(0, named=True)
    assert row["availability_policy"] == "cvm_first_seen"
    assert row["availability_basis"] == "first_seen_timestamp"
    assert row["revision_policy"] == "revised_with_snapshots"
    assert row["vintage_id"] == "cvm-v1"
    assert row["model_usable"] is True
    assert row["model_usable_reason"] == "fixture"


def _value(frame: pl.DataFrame, value_name: str) -> float:
    return frame.filter(pl.col("value_name") == value_name)["value"].item()
