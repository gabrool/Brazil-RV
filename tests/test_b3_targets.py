from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.b3.targets import build_targets_daily


def test_targets_use_future_values_only_in_separate_target_panel():
    targets = build_targets_daily(
        continuous_futures_daily=pl.DataFrame(
            [
                _continuous_row(date(2024, 1, 2), date(2024, 1, 3), 10.0),
                _continuous_row(date(2024, 1, 3), date(2024, 1, 4), 11.0),
                _continuous_row(date(2024, 1, 4), date(2024, 1, 5), 12.0),
            ]
        ),
        horizons=[1, 2],
        target_types=["quote_diff", "quote_pct_change"],
    )
    one_day = targets.filter(
        (pl.col("ref_date") == date(2024, 1, 2))
        & (pl.col("horizon") == 1)
        & (pl.col("target_type") == "quote_diff")
    ).row(0, named=True)

    assert one_day["target_value"] == 1.0
    assert one_day["target_end_date"] == date(2024, 1, 3)
    assert one_day["label_available_date"] == date(2024, 1, 4)
    assert "target_value" in targets.columns


def test_di_targets_use_bp_and_log_discount_factor_changes():
    targets = build_targets_daily(
        di_curve_grid_daily=pl.DataFrame(
            [
                _di_curve_row(date(2024, 1, 2), date(2024, 1, 3), 0.13, -0.12),
                _di_curve_row(date(2024, 1, 3), date(2024, 1, 4), 0.131, -0.125),
            ]
        ),
        horizons=[1],
        target_types=["rate_bp_change", "discount_factor_log_change"],
    )
    bp = targets.filter(pl.col("target_type") == "rate_bp_change").row(0, named=True)
    log_df = targets.filter(pl.col("target_type") == "discount_factor_log_change").row(
        0,
        named=True,
    )

    assert bp["target_id"] == "DI1_252BD"
    assert bp["target_value"] == pytest.approx(10.0)
    assert log_df["target_value"] == pytest.approx(-0.005)


def test_target_types_are_scoped_by_asset_family():
    targets = build_targets_daily(
        continuous_futures_daily=pl.DataFrame(
            [
                _continuous_row(date(2024, 1, 2), date(2024, 1, 3), 10.0),
                _continuous_row(date(2024, 1, 3), date(2024, 1, 4), 11.0),
            ]
        ),
        di_curve_grid_daily=pl.DataFrame(
            [
                _di_curve_row(date(2024, 1, 2), date(2024, 1, 3), 0.13, -0.12),
                _di_curve_row(date(2024, 1, 3), date(2024, 1, 4), 0.131, -0.125),
            ]
        ),
        index_daily=pl.DataFrame(
            [
                _index_row(date(2024, 1, 2), date(2024, 1, 3), 100_000.0),
                _index_row(date(2024, 1, 3), date(2024, 1, 4), 101_000.0),
            ]
        ),
        horizons=[1],
        target_types=[
            "quote_diff",
            "quote_pct_change",
            "rate_bp_change",
            "discount_factor_log_change",
        ],
    )
    observed = {
        asset_family: set(
            targets.filter(pl.col("asset_family") == asset_family)["target_type"].to_list()
        )
        for asset_family in ["futures", "index", "di_curve"]
    }

    assert observed["futures"] == {"quote_diff", "quote_pct_change"}
    assert observed["index"] == {"quote_diff", "quote_pct_change"}
    assert observed["di_curve"] == {"rate_bp_change", "discount_factor_log_change"}


def _continuous_row(ref_date: date, available_date: date, settlement: float):
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "continuous_id": "DI1_R1",
        "settlement": settlement,
        "source_version": "v0",
    }


def _di_curve_row(ref_date: date, available_date: date, rate: float, log_df: float):
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "curve_id": "DI1",
        "tenor_days": 252,
        "tenor_business_days": 252,
        "curve_value": rate,
        "log_discount_factor": log_df,
        "source_version": "v0",
    }


def _index_row(ref_date: date, available_date: date, close: float):
    return {
        "ref_date": ref_date,
        "available_date": available_date,
        "index_id": "IBOV",
        "close": close,
        "source_version": "v0",
    }
