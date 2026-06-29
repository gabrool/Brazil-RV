from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.tesouro.daily_long import build_daily_long


def test_daily_long_includes_all_tesouro_families_and_drops_null_values():
    panel = build_daily_long(
        direto_prices_rates_asof_daily=_prices_asof(),
        direto_flows_daily=_flows_daily(),
        direto_stock_asof_daily=_direto_stock_asof(),
        dpf_stock_asof_daily=_dpf_stock_asof(),
        include_prices_rates=True,
        include_flows=True,
        include_stock=True,
    )

    keys = {
        (row["source_family"], row["feature_id"], row["value_name"])
        for row in panel.select(["source_family", "feature_id", "value_name"]).to_dicts()
    }
    assert (
        "tesouro_direto_prices_rates",
        "price-feature",
        "buy_rate",
    ) in keys
    assert (
        "tesouro_direto_prices_rates",
        "price-feature",
        "sell_rate",
    ) not in keys
    assert ("tesouro_direto_flows", "flow-feature", "quantity") in keys
    assert ("tesouro_direto_stock", "td-stock-feature", "stock_value") in keys
    assert ("tesouro_dpf_stock", "dpf-stock-feature", "stock_value") in keys
    flow = panel.filter(
        (pl.col("source_family") == "tesouro_direto_flows")
        & (pl.col("value_name") == "quantity")
    ).row(0, named=True)
    assert flow["availability_policy"] == "tesouro_direto_sales_official_2bd"
    assert flow["availability_basis"] == "weekday_fallback"
    assert not panel.filter(pl.col("value").is_null()).height


def test_daily_long_uses_long_primary_key():
    panel = build_daily_long(
        direto_prices_rates_asof_daily=_prices_asof(),
        direto_flows_daily=_flows_daily(),
        direto_stock_asof_daily=_direto_stock_asof(),
        dpf_stock_asof_daily=_dpf_stock_asof(),
        include_prices_rates=True,
        include_flows=True,
        include_stock=True,
    )

    keys = ["ref_date", "source_family", "feature_id", "value_name"]
    assert panel.group_by(keys).len().height == panel.height


def _prices_asof() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 3),
                "available_date": date(2024, 1, 3),
                "feature_id": "price-feature",
                "security_name": "Tesouro Prefixado",
                "security_type": "Tesouro Prefixado",
                "maturity_date": date(2027, 1, 1),
                "observation_ref_date": date(2024, 1, 2),
                "observation_available_date": date(2024, 1, 3),
                "availability_policy": "tesouro_direto_sales_official_2bd",
                "buy_rate": 11.0,
                "sell_rate": None,
                "buy_price": 950.0,
                "sell_price": 949.5,
                "unit": "BRL",
                "has_rate": True,
                "has_price": True,
                "is_available": True,
                "is_observed_on_ref_date": False,
                "staleness_days": 0,
                "source_version": "v0",
            }
        ]
    )


def _flows_daily() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 3),
                "available_date": date(2024, 1, 3),
                "observation_ref_date": date(2024, 1, 2),
                "observation_available_date": date(2024, 1, 3),
                "availability_policy": "tesouro_direto_sales_official_2bd",
                "availability_basis": "weekday_fallback",
                "flow_type": "sale",
                "redemption_type": None,
                "security_name": "Tesouro Selic",
                "security_type": "Tesouro Selic",
                "maturity_date": date(2027, 3, 1),
                "feature_id": "flow-feature",
                "quantity": 10.0,
                "value": 1000.0,
                "investor_count": None,
                "unit": "BRL",
                "source_dataset": "tesouro_direto_sales",
                "source_version": "v0",
            }
        ]
    )


def _direto_stock_asof() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 3, 1),
                "available_date": date(2024, 3, 1),
                "feature_id": "td-stock-feature",
                "security_name": "Tesouro Prefixado",
                "security_type": "Tesouro Prefixado",
                "maturity_date": date(2027, 1, 1),
                "observation_ref_date": date(2024, 1, 31),
                "observation_available_date": date(2024, 3, 1),
                "quantity": 100.0,
                "stock_value": 98765.43,
                "investor_count": 9,
                "unit": "BRL",
                "is_available": True,
                "is_observed_on_ref_date": False,
                "staleness_days": 0,
                "source_version": "v0",
            }
        ]
    )


def _dpf_stock_asof() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "ref_date": date(2024, 3, 18),
                "available_date": date(2024, 3, 18),
                "feature_id": "dpf-stock-feature",
                "debt_category": "DPMFi",
                "instrument_type": "LFT",
                "indexer": "Selic",
                "holder_or_maturity_bucket": "0 a 1 ano",
                "observation_ref_date": date(2024, 1, 31),
                "observation_available_date": date(2024, 3, 18),
                "stock_value": 123456.78,
                "unit": "BRL",
                "is_available": True,
                "is_observed_on_ref_date": False,
                "staleness_days": 0,
                "source_version": "v0",
            }
        ]
    )
