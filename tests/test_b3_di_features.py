from __future__ import annotations

from datetime import date
from math import exp, log

import polars as pl
import pytest

from bralpha.derived.b3.di_features import build_di_curve_feature_daily
from bralpha.domain.di_futures import annual_rate_from_discount_factor


def test_di_curve_features_compute_changes_forwards_and_rolldown_with_warmup():
    first = date(2024, 1, 2)
    second = date(2024, 1, 3)
    frame = pl.DataFrame(
        [
            _grid_row(first, tenor, rate)
            for tenor, rate in _rates(0.10).items()
        ]
        + [
            _grid_row(second, tenor, rate)
            for tenor, rate in _rates(0.101).items()
        ]
    )

    features = build_di_curve_feature_daily(frame, start=second, end=second)

    assert features["ref_date"].unique().to_list() == [second]
    tenor_252 = _value(features, "b3_di_curve:DI1:252bd", "rate_change_1bd_bp")
    assert tenor_252 == pytest.approx(10.0)
    forward = _value(features, "b3_di_curve:DI1:shape", "forward_21_63_bp")
    expected_forward = (
        exp((_log_df(0.101, 21) - _log_df(0.102, 63)) * 252.0 / (63 - 21)) - 1.0
    ) * 10_000.0
    assert forward == pytest.approx(expected_forward)

    rolldown = _value(features, "b3_di_curve:DI1:126bd", "rolldown_5bd_bp")
    target_log_df = _log_df(0.102, 63) + (_log_df(0.103, 126) - _log_df(0.102, 63)) * (
        (121 - 63) / (126 - 63)
    )
    expected_rolldown = annual_rate_from_discount_factor(exp(target_log_df), 121) * 10_000.0
    expected_rolldown -= 0.103 * 10_000.0
    assert rolldown == pytest.approx(expected_rolldown)

    slope = _value(features, "b3_di_curve:DI1:shape", "slope_21_252_bp")
    assert slope == pytest.approx(30.0)
    assert _value(features, "b3_di_curve:DI1:21bd", "rolldown_21bd_bp") is None


def test_di_curve_feature_flags_are_binary_values():
    features = build_di_curve_feature_daily(
        pl.DataFrame(
            [
                {
                    **_grid_row(date(2024, 1, 2), 63, 0.10),
                    "is_interpolated": True,
                    "is_extrapolated": False,
                }
            ]
        )
    )

    assert _value(features, "b3_di_curve:DI1:63bd", "is_interpolated") == 1.0
    assert _value(features, "b3_di_curve:DI1:63bd", "is_extrapolated") == 0.0


def _rates(base: float) -> dict[int, float]:
    return {
        21: base,
        63: base + 0.001,
        126: base + 0.002,
        252: base + 0.003,
        504: base + 0.004,
        756: base + 0.005,
        1260: base + 0.006,
    }


def _grid_row(ref_date: date, tenor: int, rate: float) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "curve_id": "DI1",
        "tenor_days": tenor,
        "tenor_business_days": tenor,
        "curve_value": rate,
        "curve_value_kind": "implied_annual_rate",
        "discount_factor": exp(_log_df(rate, tenor)),
        "log_discount_factor": _log_df(rate, tenor),
        "implied_annual_rate": rate,
        "implied_annual_rate_bp": rate * 10_000.0,
        "interpolation_method": "linear_by_days_to_maturity",
        "left_contract_id": "L",
        "right_contract_id": "R",
        "left_days_to_maturity": tenor,
        "right_days_to_maturity": tenor,
        "is_interpolated": False,
        "is_extrapolated": False,
        "has_curve_value": True,
        "source_version": "v0",
    }


def _log_df(rate: float, tenor: int) -> float:
    return -log(1.0 + rate) * tenor / 252.0


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float | None:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
