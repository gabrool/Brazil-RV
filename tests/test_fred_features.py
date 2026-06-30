from __future__ import annotations

from datetime import date, timedelta
from math import log, log1p, sqrt
from statistics import stdev

import polars as pl
import pytest

from bralpha.derived.fred.features import (
    build_fred_market_feature_daily,
    build_fred_rate_feature_daily,
)


def test_fred_rate_features_compute_levels_changes_and_curve_spreads():
    start = date(2024, 1, 2)
    rows = []
    for offset in range(6):
        ref_date = start + timedelta(days=offset)
        rows.extend(
            [
                _asof_row(ref_date, "DGS2", 4.0 + offset * 0.01),
                _asof_row(ref_date, "DGS10", 4.5 + offset * 0.02),
                _asof_row(ref_date, "DFEDTARU", 5.50 + offset * 0.01),
                _asof_row(ref_date, "DFEDTARL", 5.25 + offset * 0.01),
            ]
        )
    final_date = start + timedelta(days=5)

    features = build_fred_rate_feature_daily(pl.DataFrame(rows), start=final_date, end=final_date)

    assert _value(features, "fred_rate:dgs10", "level_bp") == pytest.approx(460.0)
    assert _value(features, "fred_rate:dgs10", "change_5bd_bp") == pytest.approx(10.0)
    assert _value(features, "fred_rate:curve", "ust_slope_2y_10y_bp") == pytest.approx(
        55.0
    )
    assert _value(features, "fred_rate:curve", "fed_target_mid_bp") == pytest.approx(542.5)
    assert _value(features, "fred_rate:curve", "fed_target_mid_change_5bd_bp") == pytest.approx(
        5.0
    )


def test_fred_market_features_compute_returns_vol_and_vix_changes():
    start = date(2024, 1, 2)
    levels = [100.0 + index for index in range(22)]
    rows = [
        _asof_row(start + timedelta(days=index), "SP500", level, unit="index")
        for index, level in enumerate(levels)
    ]
    rows.extend(
        [
            _asof_row(start, "VIXCLS", 12.0, unit="index"),
            _asof_row(start + timedelta(days=1), "VIXCLS", 13.5, unit="index"),
        ]
    )
    final_date = start + timedelta(days=21)

    features = build_fred_market_feature_daily(pl.DataFrame(rows), start=final_date, end=final_date)
    full_features = build_fred_market_feature_daily(
        pl.DataFrame(rows),
        start=start + timedelta(days=1),
        end=start + timedelta(days=1),
    )

    assert _value(features, "fred_market:sp500", "log_level") == pytest.approx(log(levels[-1]))
    assert _value(features, "fred_market:sp500", "log_return_21bd") == pytest.approx(
        log(levels[-1] / levels[0])
    )
    expected_returns = [log(levels[index] / levels[index - 1]) for index in range(1, 22)]
    assert _value(features, "fred_market:sp500", "realized_vol_21bd_ann") == pytest.approx(
        stdev(expected_returns) * sqrt(252.0)
    )
    assert _value(full_features, "fred_market:vixcls", "change_1bd") == pytest.approx(1.5)


def test_fred_oil_features_are_signed_safe_for_negative_prices():
    start = date(2020, 4, 20)
    rows = [
        _asof_row(start, "DCOILWTICO", 18.27, unit="usd_per_barrel"),
        _asof_row(start + timedelta(days=1), "DCOILWTICO", -37.63, unit="usd_per_barrel"),
    ]

    features = build_fred_market_feature_daily(
        pl.DataFrame(rows),
        start=start + timedelta(days=1),
        end=start + timedelta(days=1),
    )

    current_signed_log = -log1p(37.63)
    previous_signed_log = log1p(18.27)
    assert _value(
        features,
        "fred_market:dcoilwtico",
        "signed_log_level",
    ) == pytest.approx(current_signed_log)
    assert _value(
        features,
        "fred_market:dcoilwtico",
        "signed_log_change_1bd",
    ) == pytest.approx(current_signed_log - previous_signed_log)


def _asof_row(
    ref_date: date,
    series_id: str,
    value: float,
    *,
    unit: str = "percent",
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "feature_id": f"fred|{series_id.lower()}",
        "series_id": series_id,
        "series_name": series_id,
        "category": "test",
        "frequency": "daily",
        "unit": unit,
        "observation_ref_date": ref_date,
        "vintage_date": ref_date,
        "vintage_id": f"vintage:{series_id}:{ref_date}",
        "observation_available_date": ref_date,
        "availability_policy": "date_only_next_business_day",
        "availability_basis": "source_date_only",
        "series_kind": "market_daily",
        "vintage_policy": "latest_snapshot_allowed",
        "vintage_request_mode": "latest_snapshot",
        "revision_policy": "unrevised",
        "first_seen_timestamp_utc": None,
        "value": value,
        "raw_value": str(value),
        "value_status": "ok",
        "has_value": True,
        "realtime_start": ref_date,
        "realtime_end": ref_date,
        "is_available": True,
        "is_observed_on_ref_date": True,
        "staleness_days": 0,
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float | None:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
