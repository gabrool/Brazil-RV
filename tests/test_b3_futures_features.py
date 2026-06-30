from __future__ import annotations

from datetime import date, timedelta
from math import log, sqrt
from statistics import stdev

import polars as pl
import pytest

from bralpha.derived.b3.futures_features import build_futures_feature_daily


def test_futures_features_use_sequence_history_and_filter_output_window():
    start = date(2024, 1, 2)
    settlements = [100.0, 101.0, 102.0, 101.0, 103.0, 104.0]
    frame = pl.DataFrame(
        [
            _continuous_row(start + timedelta(days=offset), settlement, "C1")
            for offset, settlement in enumerate(settlements)
        ]
    )
    final_date = start + timedelta(days=5)

    features = build_futures_feature_daily(frame, start=final_date, end=final_date)

    assert features["ref_date"].unique().to_list() == [final_date]
    assert _value(features, "log_settlement") == pytest.approx(log(104.0))
    assert _value(features, "log_return_5bd") == pytest.approx(log(104.0 / 100.0))
    expected_returns = [
        log(101.0 / 100.0),
        log(102.0 / 101.0),
        log(101.0 / 102.0),
        log(103.0 / 101.0),
        log(104.0 / 103.0),
    ]
    assert _value(features, "realized_vol_5bd_ann") == pytest.approx(
        stdev(expected_returns) * sqrt(252.0)
    )
    assert _value(features, "volume_log1p") == pytest.approx(log(101.0))
    assert _value(features, "volume_open_interest_ratio") == pytest.approx(0.1)
    assert _value(features, "is_tradeable") == 1.0


def test_futures_features_null_same_contract_return_on_roll_and_compute_roll_gap():
    frame = pl.DataFrame(
        [
            _continuous_row(date(2024, 1, 2), 100.0, "C1"),
            {
                **_continuous_row(date(2024, 1, 3), 103.0, "C2"),
                "previous_contract_id": "C1",
                "is_roll_date": True,
                "quote_pct_change_1d": 0.05,
                "same_contract_quote_pct_change_1d": 0.02,
            },
        ]
    )

    features = build_futures_feature_daily(frame, start=date(2024, 1, 3), end=date(2024, 1, 3))

    assert _value(features, "same_contract_log_return_1bd") is None
    assert _value(features, "roll_gap_pct") == pytest.approx(3.0)
    assert _value(features, "is_roll_date") == 1.0


def _continuous_row(ref_date: date, settlement: float, contract_id: str) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "continuous_id": "DI1_R1",
        "root": "DI1",
        "rank": 1,
        "selected_contract_id": contract_id,
        "selected_maturity_code": "F26",
        "selected_maturity_date": date(2026, 1, 2),
        "days_to_maturity_calendar": 500,
        "business_days_to_maturity": 350,
        "roll_policy": "maturity_rank",
        "is_roll_date": False,
        "previous_contract_id": contract_id,
        "settlement": settlement,
        "quote_diff_1d": None,
        "quote_pct_change_1d": None,
        "same_contract_quote_diff_1d": None,
        "same_contract_quote_pct_change_1d": None,
        "volume": 100,
        "open_interest": 1000,
        "is_tradeable": True,
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, value_name: str) -> float | None:
    return frame.filter(pl.col("value_name") == value_name)["value"].item()
