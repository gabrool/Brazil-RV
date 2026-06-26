from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.b3.listed_market import (
    build_index_composition_daily,
    build_index_daily,
    build_listed_market_daily,
)


def test_listed_market_preserves_isin_and_daily_supersedes_yearly_duplicate():
    yearly = pl.DataFrame(
        [_listed_row("b3_cotahist_yearly", close=10.0, isin="BRPETRACNPR6")]
    )
    daily = pl.DataFrame(
        [_listed_row("b3_cotahist_daily", close=10.5, isin="BRPETRACNPR6")]
    )

    panel = build_listed_market_daily(cotahist_yearly=yearly, cotahist_daily=daily)

    assert panel.height == 1
    assert panel["source_dataset"].item() == "b3_cotahist_daily"
    assert panel["close"].item() == 10.5
    assert panel["isin"].item() == "BRPETRACNPR6"


def test_listed_market_reference_enrichment_maxes_available_date():
    cotahist = pl.DataFrame(
        [
            _listed_row(
                "b3_cotahist_yearly",
                close=10.0,
                isin=None,
                asset_class=None,
                name=None,
            )
        ]
    )
    reference = pl.DataFrame(
        [
            {
                "symbol": "PETR4",
                "market_type": "010",
                "isin": "BRPETRACNPR6",
                "name": "PETROBRAS PN",
                "asset_class": "equity",
                "available_date": date(2024, 1, 5),
                "source_version": "reference-v0",
            }
        ]
    )

    panel = build_listed_market_daily(cotahist_yearly=cotahist, traded_securities=reference)
    row = panel.row(0, named=True)

    assert row["isin"] == "BRPETRACNPR6"
    assert row["name"] == "PETROBRAS PN"
    assert row["available_date"] == date(2024, 1, 5)
    assert row["source_version"] == "reference-v0|v0"


def test_listed_market_keeps_cotahist_availability_when_reference_unused():
    cotahist = pl.DataFrame(
        [
            _listed_row(
                "b3_cotahist_yearly",
                close=10.0,
                isin="BRPETRACNPR6",
                asset_class="equity",
                name="PETROBRAS PN",
            )
        ]
    )
    reference = pl.DataFrame(
        [
            {
                "symbol": "PETR4",
                "market_type": "010",
                "isin": "BRPETRACNPR6",
                "name": "PETROBRAS PN",
                "asset_class": "equity",
                "available_date": date(2024, 1, 5),
                "source_version": "reference-v0",
            }
        ]
    )

    panel = build_listed_market_daily(cotahist_yearly=cotahist, traded_securities=reference)

    assert panel["available_date"].item() == date(2024, 1, 3)
    assert panel["source_version"].item() == "v0"


def test_index_daily_preserves_raw_levels():
    panel = build_index_daily(
        pl.DataFrame(
            [
                {
                    "ref_date": date(2024, 1, 2),
                    "available_date": date(2024, 1, 3),
                    "index_id": "IBOV",
                    "close": 130000.0,
                    "open": 129000.0,
                    "high": 131000.0,
                    "low": 128000.0,
                    "volume": None,
                    "financial_volume": None,
                    "number_of_trades": None,
                    "currency": "BRL",
                    "unit": "points",
                    "source_version": "v0",
                }
            ]
        )
    )

    assert panel["close"].item() == 130000.0
    assert "quote_pct_change_1d" not in panel.columns


def test_index_composition_keeps_source_datasets_separate_without_forward_fill():
    panel = build_index_composition_daily(
        indexes_current_portfolio=pl.DataFrame(
            [_composition_row("b3_indexes_current_portfolio", weight=10.0)]
        ),
        indexes_theoretical_portfolio=pl.DataFrame(
            [_composition_row("b3_indexes_theoretical_portfolio", weight=11.0)]
        ),
    )

    assert panel.height == 2
    assert set(panel["source_dataset"]) == {
        "b3_indexes_current_portfolio",
        "b3_indexes_theoretical_portfolio",
    }
    assert panel["ref_date"].to_list() == [date(2024, 1, 2), date(2024, 1, 2)]


def _listed_row(
    source_dataset: str,
    *,
    close: float,
    isin: str | None,
    asset_class: str | None = "equity",
    name: str | None = None,
):
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 3),
        "symbol": "PETR4",
        "isin": isin,
        "market_type": "010",
        "asset_class": asset_class,
        "name": name,
        "open": 9.0,
        "high": 11.0,
        "low": 8.0,
        "close": close,
        "volume": 100,
        "financial_volume": 1000.0,
        "number_of_trades": 10,
        "source_dataset": source_dataset,
        "source_version": "v0",
    }


def _composition_row(source_dataset: str, *, weight: float):
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 3),
        "index_id": "IBOV",
        "symbol": "PETR4",
        "isin": "BRPETRACNPR6",
        "security_id": "PETR4_010",
        "name": "PETROBRAS PN",
        "weight": weight,
        "theoretical_quantity": 1000.0,
        "source_dataset": source_dataset,
        "source_version": "v0",
    }
