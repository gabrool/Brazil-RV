from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.b3.di_curve import build_di_curve_contract_daily, build_di_curve_grid_daily
from bralpha.domain.di_futures import annual_rate_from_discount_factor, pu_from_annual_rate


def test_di_curve_contract_filters_di1_and_derives_di_economics():
    pu_13 = pu_from_annual_rate(0.13, 252)
    contract_panel = pl.DataFrame(
        [
            _futures_row("DI1", "DI1_F26", "F26", 252, pu_13),
            _futures_row("DOL", "DOL_F26", "F26", 20, 5000.0),
        ]
    )

    curve = build_di_curve_contract_daily(contract_panel, source_roots=["DI1"])

    assert curve.height == 1
    assert curve["contract_id"].item() == "DI1_F26"
    assert curve["raw_settlement_pu"].item() == pytest.approx(pu_13)
    assert curve["discount_factor"].item() == pytest.approx(pu_13 / 100_000)
    assert curve["implied_annual_rate"].item() == pytest.approx(0.13)
    assert curve["implied_annual_rate_bp"].item() == pytest.approx(1300.0)
    assert curve["curve_value"].item() == pytest.approx(0.13)
    assert curve["curve_value_kind"].item() == "implied_annual_rate"
    assert curve["is_observed"].item() is True


def test_di_curve_grid_interpolates_log_df_by_business_days():
    left_pu = pu_from_annual_rate(0.10, 20)
    right_pu = pu_from_annual_rate(0.20, 40)
    curve = build_di_curve_contract_daily(
        pl.DataFrame(
            [
                _futures_row("DI1", "DI1_F26", "F26", 20, left_pu),
                _futures_row("DI1", "DI1_G26", "G26", 40, right_pu),
            ]
        ),
        source_roots=["DI1"],
    )

    grid = build_di_curve_grid_daily(
        curve,
        tenor_days=[10, 30, 50],
        interpolation_method="linear_by_days_to_maturity",
    )
    tenor_30 = grid.filter(pl.col("tenor_days") == 30).row(0, named=True)
    tenor_10 = grid.filter(pl.col("tenor_days") == 10).row(0, named=True)
    tenor_50 = grid.filter(pl.col("tenor_days") == 50).row(0, named=True)
    expected_log_df = (
        curve.filter(pl.col("contract_id") == "DI1_F26")["log_discount_factor"].item()
        + curve.filter(pl.col("contract_id") == "DI1_G26")["log_discount_factor"].item()
    ) / 2
    expected_df = pytest.approx(2.718281828459045**expected_log_df)

    assert tenor_30["tenor_business_days"] == 30
    assert tenor_30["log_discount_factor"] == pytest.approx(expected_log_df)
    assert tenor_30["discount_factor"] == expected_df
    assert tenor_30["curve_value"] == pytest.approx(
        annual_rate_from_discount_factor(tenor_30["discount_factor"], 30)
    )
    assert tenor_30["is_interpolated"] is True
    assert tenor_30["is_extrapolated"] is False
    assert tenor_10["curve_value"] is None
    assert tenor_10["is_extrapolated"] is False
    assert tenor_50["curve_value"] is None
    assert tenor_50["is_extrapolated"] is False


def test_di_contract_changes_are_bp_and_log_df_based():
    curve = build_di_curve_contract_daily(
        pl.DataFrame(
            [
                _futures_row(
                    "DI1",
                    "DI1_F26",
                    "F26",
                    252,
                    pu_from_annual_rate(0.13, 252),
                    ref_date=date(2024, 1, 2),
                ),
                _futures_row(
                    "DI1",
                    "DI1_F26",
                    "F26",
                    251,
                    pu_from_annual_rate(0.131, 251),
                    ref_date=date(2024, 1, 3),
                ),
            ]
        ),
        source_roots=["DI1"],
    )
    second = curve.sort("ref_date").row(1, named=True)

    assert second["implied_annual_rate_bp_change_1d"] == pytest.approx(10.0)
    assert second["log_discount_factor_change_1d"] is not None


def _futures_row(
    root: str,
    contract_id: str,
    maturity_code: str,
    days: int,
    settlement: float | None,
    *,
    ref_date: date = date(2024, 1, 2),
):
    return {
        "ref_date": ref_date,
        "available_date": date(2024, 1, 3),
        "root": root,
        "commodity": root,
        "contract_id": contract_id,
        "maturity_code": maturity_code,
        "maturity_date": date(2026, 1, 2),
        "days_to_maturity_calendar": days + 100,
        "business_days_to_maturity": days,
        "contract_rank_by_maturity": 1,
        "settlement": settlement,
        "volume": 100,
        "open_interest": 1000,
        "source_version": "v0",
    }
