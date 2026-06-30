from __future__ import annotations

from datetime import date, timedelta
from math import log

import polars as pl
import pytest

from bralpha.derived.tesouro.features import build_tesouro_feature_daily


def test_tesouro_features_compute_mid_prices_rate_changes_and_returns_with_warmup():
    start = date(2024, 1, 2)
    prices = [950.0, 951.0, 953.0, 954.0, 955.0, 960.0]
    frame = pl.DataFrame(
        [
            _price_row(start + timedelta(days=index), 11.0 + index * 0.01, price)
            for index, price in enumerate(prices)
        ]
    )
    final_date = start + timedelta(days=5)

    features = build_tesouro_feature_daily(
        direto_prices_rates_asof_daily=frame,
        start=final_date,
        end=final_date,
    )

    assert _value(features, "tesouro_price:tesouro_selic", "mid_rate_bp") == pytest.approx(1110.0)
    assert _value(features, "tesouro_price:tesouro_selic", "rate_spread_bp") == pytest.approx(10.0)
    assert _value(
        features,
        "tesouro_price:tesouro_selic",
        "price_bid_ask_spread_pct",
    ) == pytest.approx(100.0 * 2.0 / 960.0)
    assert _value(features, "tesouro_price:tesouro_selic", "price_log_return_5bd") == pytest.approx(
        log(960.0 / 950.0)
    )


def test_tesouro_features_compute_net_flow_and_flow_to_stock():
    features = build_tesouro_feature_daily(
        direto_flows_daily=pl.DataFrame(
            [
                _flow_row("sale", 1000.0, 10.0),
                _flow_row("redemption", 250.0, 2.0),
            ]
        ),
        direto_stock_asof_daily=pl.DataFrame([_stock_row(10_000.0)]),
    )

    assert _value(features, "tesouro_flow:tesouro_flow_fixture", "net_flow_value") == 750.0
    assert _value(features, "tesouro_flow:tesouro_flow_fixture", "gross_flow_value") == 1250.0
    assert _value(features, "tesouro_flow:tesouro_flow_fixture", "redemption_share_pct") == 20.0
    assert _value(
        features,
        "tesouro_flow:tesouro_flow_fixture",
        "net_flow_to_stock_value_pct",
    ) == 7.5
    assert _value(
        features,
        "tesouro_stock:tesouro_stock_fixture",
        "stock_value_log",
    ) == pytest.approx(log(10_000.0))


def test_tesouro_stock_features_compute_21bd_changes_from_warmup_history():
    start = date(2024, 1, 2)
    direto_stock = pl.DataFrame(
        [
            _stock_row(
                10_000.0 + index,
                ref_date=start + timedelta(days=index),
                feature_id="tesouro_stock_fixture",
            )
            for index in range(22)
        ]
    )
    dpf_stock = pl.DataFrame(
        [
            _stock_row(
                20_000.0 + index,
                ref_date=start + timedelta(days=index),
                feature_id="dpf_stock_fixture",
            )
            for index in range(22)
        ]
    )
    final_date = start + timedelta(days=21)

    features = build_tesouro_feature_daily(
        direto_stock_asof_daily=direto_stock,
        dpf_stock_asof_daily=dpf_stock,
        start=final_date,
        end=final_date,
    )

    assert _value(
        features,
        "tesouro_stock:tesouro_stock_fixture",
        "stock_value_change_21bd_pct",
    ) == pytest.approx(100.0 * (10_021.0 / 10_000.0 - 1.0))
    assert _value(
        features,
        "tesouro_dpf_stock:dpf_stock_fixture",
        "stock_value_change_21bd_pct",
    ) == pytest.approx(100.0 * (20_021.0 / 20_000.0 - 1.0))


def _price_row(ref_date: date, rate: float, price: float) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "feature_id": "tesouro_selic",
        "security_name": "Tesouro Selic",
        "security_type": "Tesouro Selic",
        "maturity_date": date(2027, 3, 1),
        "observation_ref_date": ref_date,
        "observation_available_date": ref_date,
        "buy_rate": rate,
        "sell_rate": rate + 0.1,
        "buy_price": price - 1.0,
        "sell_price": price + 1.0,
        "unit": "BRL",
        "has_rate": True,
        "has_price": True,
        "is_available": True,
        "is_observed_on_ref_date": True,
        "staleness_days": 0,
        "source_version": "v0",
    }


def _flow_row(flow_type: str, value: float, quantity: float) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 3),
        "available_date": date(2024, 1, 3),
        "observation_ref_date": date(2024, 1, 2),
        "observation_available_date": date(2024, 1, 3),
        "availability_policy": "tesouro_direto_sales_official_2bd",
        "availability_basis": "weekday_fallback",
        "flow_type": flow_type,
        "redemption_type": None,
        "security_name": "Tesouro Selic",
        "security_type": "Tesouro Selic",
        "maturity_date": date(2027, 3, 1),
        "feature_id": "tesouro_flow_fixture",
        "quantity": quantity,
        "value": value,
        "investor_count": 3,
        "unit": "BRL",
        "source_dataset": "fixture",
        "source_version": "v0",
    }


def _stock_row(
    stock_value: float,
    *,
    ref_date: date = date(2024, 1, 3),
    feature_id: str = "tesouro_stock_fixture",
) -> dict[str, object]:
    return {
        "ref_date": ref_date,
        "available_date": ref_date,
        "feature_id": feature_id,
        "security_name": "Tesouro Selic",
        "security_type": "Tesouro Selic",
        "maturity_date": date(2027, 3, 1),
        "observation_ref_date": ref_date,
        "observation_available_date": ref_date,
        "quantity": 100.0,
        "stock_value": stock_value,
        "investor_count": 5,
        "unit": "BRL",
        "is_available": True,
        "is_observed_on_ref_date": True,
        "staleness_days": 0,
        "source_version": "v0",
    }


def _value(frame: pl.DataFrame, feature_id: str, value_name: str) -> float | None:
    return frame.filter(
        (pl.col("feature_id") == feature_id) & (pl.col("value_name") == value_name)
    )["value"].item()
