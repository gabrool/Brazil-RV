from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.cross_features import build_br_rv_cross_feature_daily


def test_cross_features_compute_same_date_rv_spreads():
    ref_date = date(2024, 1, 2)
    features = build_br_rv_cross_feature_daily(
        b3_di_feature_daily=pl.DataFrame(
            [
                _row(
                    ref_date,
                    "b3_di_curve_feature",
                    "b3_di_curve:DI1:504bd",
                    "rate_level_bp",
                    1200.0,
                ),
                _row(
                    ref_date,
                    "b3_di_curve_feature",
                    "b3_di_curve:DI1:1260bd",
                    "rate_level_bp",
                    1300.0,
                ),
            ]
        ),
        bcb_sgs_feature_daily=pl.DataFrame(
            [
                _row(
                    ref_date,
                    "bcb_sgs_feature",
                    "bcb_sgs_feature:rates:selic_target_level_pa",
                    "selic_target_level_pa",
                    10.0,
                ),
                _row(
                    ref_date,
                    "bcb_sgs_feature",
                    "bcb_sgs_feature:inflation:ipca_12m_sum_pct",
                    "ipca_12m_sum_pct",
                    4.0,
                ),
            ]
        ),
        fred_rate_feature_daily=pl.DataFrame(
            [
                _row(ref_date, "fred_rate_feature", "fred_rate:dgs2", "level_bp", 450.0),
                _row(ref_date, "fred_rate_feature", "fred_rate:dgs5", "level_bp", 475.0),
                _row(ref_date, "fred_rate_feature", "fred_rate:curve", "fed_target_mid_bp", 525.0),
                _row(ref_date, "fred_rate_feature", "fred_rate:dff", "level_bp", 533.0),
            ]
        ),
        bcb_ptax_feature_daily=pl.DataFrame(
            [
                _row(ref_date, "bcb_ptax_feature", "bcb_ptax:USD", "log_return_1bd", 0.01),
                _row(ref_date, "bcb_ptax_feature", "bcb_ptax:USD", "log_return_5bd", 0.04),
                _row(ref_date, "bcb_ptax_feature", "bcb_ptax:USD", "log_return_21bd", 0.08),
            ]
        ),
        fred_market_feature_daily=pl.DataFrame(
            [
                _row(
                    ref_date,
                    "fred_market_feature",
                    "fred_market:dtwexemegs",
                    "log_return_1bd",
                    0.003,
                ),
                _row(
                    ref_date,
                    "fred_market_feature",
                    "fred_market:dtwexemegs",
                    "log_return_5bd",
                    0.010,
                ),
                _row(
                    ref_date,
                    "fred_market_feature",
                    "fred_market:dtwexemegs",
                    "log_return_21bd",
                    0.030,
                ),
                _row(
                    ref_date,
                    "fred_market_feature",
                    "fred_market:sp500",
                    "log_return_1bd",
                    0.004,
                ),
                _row(
                    ref_date,
                    "fred_market_feature",
                    "fred_market:sp500",
                    "log_return_21bd",
                    0.050,
                ),
            ]
        ),
        b3_index_feature_daily=pl.DataFrame(
            [
                _row(ref_date, "b3_index_feature", "b3_index:IBOV", "log_return_1bd", 0.006),
                _row(ref_date, "b3_index_feature", "b3_index:IBOV", "log_return_21bd", 0.090),
            ]
        ),
    )

    assert _value(features, "br_rv_cross:policy", "brl_policy_carry_vs_fed_mid_bp") == 475.0
    assert _value(features, "br_rv_cross:rates", "brl_di_2y_minus_ust_2y_bp") == 750.0
    assert _value(
        features,
        "br_rv_cross:rates",
        "brl_di_2y_real_minus_us_2y_proxy_bp",
    ) == 350.0
    assert _value(
        features,
        "br_rv_cross:fx",
        "brl_usd_minus_em_dollar_log_return_1bd",
    ) == pytest.approx(0.007)
    assert _value(
        features,
        "br_rv_cross:fx",
        "brl_fx_idiosyncratic_return_5bd",
    ) == pytest.approx(0.03)
    assert _value(
        features,
        "br_rv_cross:fx",
        "brl_fx_idiosyncratic_return_21bd",
    ) == pytest.approx(0.05)
    assert _value(
        features,
        "br_rv_cross:equity",
        "ibov_minus_sp500_log_return_1bd",
    ) == pytest.approx(0.002)
    assert _value(
        features,
        "br_rv_cross:equity",
        "ibov_sp500_relative_return_21bd",
    ) == pytest.approx(0.04)


def test_cross_features_emit_null_when_required_input_is_missing():
    ref_date = date(2024, 1, 2)
    features = build_br_rv_cross_feature_daily(
        b3_di_feature_daily=pl.DataFrame(
            [
                _row(
                    ref_date,
                    "b3_di_curve_feature",
                    "b3_di_curve:DI1:504bd",
                    "rate_level_bp",
                    1200.0,
                )
            ]
        )
    )

    row = features.filter(
        (pl.col("feature_id") == "br_rv_cross:rates")
        & (pl.col("value_name") == "brl_di_2y_minus_ust_2y_bp")
    ).row(0, named=True)
    assert row["value"] is None
    assert row["is_available"] is False


def test_cross_features_use_latest_contributor_availability():
    ref_date = date(2024, 1, 2)
    delayed_available = date(2024, 1, 3)
    features = build_br_rv_cross_feature_daily(
        b3_di_feature_daily=pl.DataFrame(
            [
                _row(
                    ref_date,
                    "b3_di_curve_feature",
                    "b3_di_curve:DI1:504bd",
                    "rate_level_bp",
                    1200.0,
                    available_date=delayed_available,
                    observation_available_date=delayed_available,
                )
            ]
        ),
        fred_rate_feature_daily=pl.DataFrame(
            [_row(ref_date, "fred_rate_feature", "fred_rate:dgs2", "level_bp", 450.0)]
        ),
    )

    row = features.filter(
        (pl.col("feature_id") == "br_rv_cross:rates")
        & (pl.col("value_name") == "brl_di_2y_minus_ust_2y_bp")
    ).row(0, named=True)

    assert row["value"] == pytest.approx(750.0)
    assert row["available_date"] == delayed_available


def _row(
    ref_date: date,
    source_family: str,
    feature_id: str,
    value_name: str,
    value: float,
    *,
    available_date: date | None = None,
    observation_available_date: date | None = None,
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": available_date or ref_date,
        "source_family": source_family,
        "feature_id": feature_id,
        "value_name": value_name,
        "value": value,
        "unit": "unit",
        "observation_ref_date": ref_date,
        "observation_available_date": observation_available_date or ref_date,
        "is_available": True,
        "staleness_days": 0,
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float | None:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
