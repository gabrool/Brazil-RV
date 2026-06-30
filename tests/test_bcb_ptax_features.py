from __future__ import annotations

from datetime import date, timedelta
from math import log, sqrt
from statistics import stdev

import polars as pl
import pytest

from bralpha.derived.bcb.ptax_features import build_ptax_feature_daily


def test_ptax_features_compute_mid_spread_returns_and_vol_with_warmup():
    start = date(2024, 1, 2)
    mids = [5.00 + index * 0.01 for index in range(22)]
    frame = pl.DataFrame(
        [_ptax_row(start + timedelta(days=index), mid) for index, mid in enumerate(mids)]
    )
    final_date = start + timedelta(days=21)

    features = build_ptax_feature_daily(frame, start=final_date, end=final_date)

    assert features["ref_date"].unique().to_list() == [final_date]
    assert _value(features, "mid_rate") == pytest.approx(mids[-1])
    assert _value(features, "log_mid_rate") == pytest.approx(log(mids[-1]))
    assert _value(features, "bid_ask_spread_bp") == pytest.approx(
        10_000.0 * (0.02 / mids[-1])
    )
    assert _value(features, "parity_bid_ask_spread_bp") == pytest.approx(
        10_000.0 * (0.02 / 1.0)
    )
    assert _value(features, "log_return_21bd") == pytest.approx(log(mids[-1] / mids[0]))
    expected_returns = [log(mids[index] / mids[index - 1]) for index in range(1, 22)]
    assert _value(features, "realized_vol_21bd_ann") == pytest.approx(
        stdev(expected_returns) * sqrt(252.0)
    )


def _ptax_row(ref_date: date, mid: float) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "currency_code": "USD",
        "currency_name": "USD",
        "selected_bulletin_type": "Fechamento",
        "quote_datetime": None,
        "bid_rate": mid - 0.01,
        "ask_rate": mid + 0.01,
        "bid_parity": 0.99,
        "ask_parity": 1.01,
        "has_quote": True,
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, value_name: str) -> float | None:
    return frame.filter(pl.col("value_name") == value_name)["value"].item()
