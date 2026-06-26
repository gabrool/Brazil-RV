from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.b3.di_curve import build_di_curve_contract_daily, build_di_curve_grid_daily


def test_di_curve_contract_filters_di1_and_preserves_raw_curve_values():
    contract_panel = pl.DataFrame(
        [
            _futures_row("DI1", "DI1_F26", "F26", 20, 10.0),
            _futures_row("DOL", "DOL_F26", "F26", 20, 5000.0),
        ]
    )

    curve = build_di_curve_contract_daily(contract_panel, source_roots=["DI1"])

    assert curve.height == 1
    assert curve["contract_id"].item() == "DI1_F26"
    assert curve["curve_value"].item() == 10.0
    assert curve["is_observed"].item() is True


def test_di_curve_grid_interpolates_and_does_not_unmark_extrapolation():
    curve = build_di_curve_contract_daily(
        pl.DataFrame(
            [
                _futures_row("DI1", "DI1_F26", "F26", 20, 10.0),
                _futures_row("DI1", "DI1_G26", "G26", 40, 20.0),
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

    assert tenor_30["curve_value"] == 15.0
    assert tenor_30["is_interpolated"] is True
    assert tenor_30["is_extrapolated"] is False
    assert tenor_10["curve_value"] is None
    assert tenor_10["is_extrapolated"] is False
    assert tenor_50["curve_value"] is None
    assert tenor_50["is_extrapolated"] is False


def _futures_row(root: str, contract_id: str, maturity_code: str, days: int, settlement: float):
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 3),
        "root": root,
        "commodity": root,
        "contract_id": contract_id,
        "maturity_code": maturity_code,
        "maturity_date": date(2026, 1, 2),
        "days_to_maturity_calendar": days,
        "business_days_to_maturity": days,
        "contract_rank_by_maturity": 1,
        "settlement": settlement,
        "volume": 100,
        "open_interest": 1000,
        "source_version": "v0",
    }
