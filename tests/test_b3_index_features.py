from __future__ import annotations

from datetime import date, timedelta
from math import log, sqrt
from statistics import stdev

import polars as pl
import pytest

from bralpha.derived.b3.index_features import (
    build_index_composition_feature_daily,
    build_index_feature_daily,
)


def test_index_features_compute_returns_vols_range_and_drawdown_with_warmup():
    start = date(2024, 1, 2)
    closes = [100.0 + index for index in range(21)] + [115.0]
    frame = pl.DataFrame(
        [_index_row(start + timedelta(days=offset), close) for offset, close in enumerate(closes)]
    )
    final_date = start + timedelta(days=21)

    features = build_index_feature_daily(frame, start=final_date, end=final_date)

    assert features["ref_date"].unique().to_list() == [final_date]
    assert _value(features, "log_close") == pytest.approx(log(115.0))
    assert _value(features, "log_return_21bd") == pytest.approx(log(115.0 / 100.0))
    expected_returns = [log(closes[index] / closes[index - 1]) for index in range(1, 22)]
    assert _value(features, "realized_vol_21bd_ann") == pytest.approx(
        stdev(expected_returns) * sqrt(252.0)
    )
    assert _value(features, "intraday_range_log") == pytest.approx(log(120.0 / 110.0))
    assert _value(features, "close_drawdown_252bd_pct") == pytest.approx(
        100.0 * (115.0 / 120.0 - 1.0)
    )
    assert _value(features, "financial_volume_log1p") == pytest.approx(log(10_001.0))


def test_index_composition_features_normalize_fraction_weights():
    frame = pl.DataFrame(
        [
            _composition_row("AAA", 0.50),
            _composition_row("BBB", 0.30),
            _composition_row("CCC", 0.20),
        ]
    )

    features = build_index_composition_feature_daily(frame)

    assert _value(features, "constituent_count") == 3.0
    assert _value(features, "weight_top1_pct") == pytest.approx(50.0)
    assert _value(features, "top5_weight_pct") == pytest.approx(100.0)
    assert _value(features, "hhi_weight") == pytest.approx(0.5**2 + 0.3**2 + 0.2**2)
    assert _value(features, "effective_constituents") == pytest.approx(
        1.0 / (0.5**2 + 0.3**2 + 0.2**2)
    )


def _index_row(ref_date: date, close: float) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "index_id": "IBOV",
        "close": close,
        "open": close - 1.0,
        "high": close + 5.0,
        "low": close - 5.0,
        "volume": 1000,
        "financial_volume": 10_000,
        "number_of_trades": 500,
        "currency": "BRL",
        "unit": "points",
        "source_version": "v0",
    }


def _composition_row(symbol: str, weight: float) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 2),
        "index_id": "IBOV",
        "symbol": symbol,
        "isin": f"BR{symbol}",
        "security_id": symbol,
        "name": symbol,
        "weight": weight,
        "theoretical_quantity": 1000,
        "source_dataset": "fixture",
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, value_name: str) -> float | None:
    return frame.filter(pl.col("value_name") == value_name)["value"].item()
