from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.derived.b3.futures_contract_panel import build_futures_contract_daily


def test_futures_contract_panel_merges_sources_and_maxes_available_date():
    settlements = pl.DataFrame(
        [
            _source_row("b3_futures_settlements", "DI1", "F26", settlement=10.0),
            _source_row("b3_futures_settlements", "DI1", "G26", settlement=11.0),
        ]
    )
    open_interest = pl.DataFrame(
        [_source_row("b3_derivatives_open_interest", "DI1", "F26", open_interest=1000)]
    )
    trade_summary = pl.DataFrame(
        [
            _source_row(
                "b3_derivatives_trade_summary",
                "DI1",
                "F26",
                available_date=date(2024, 1, 5),
                volume=200,
                financial_volume=12345.0,
                number_of_trades=40,
            )
        ]
    )
    contract_master = pl.DataFrame(
        [
            {
                "contract_id": "DI1_F26",
                "maturity_date": date(2026, 1, 2),
                "quote_convention": "price",
                "source_version": "master-v0",
            }
        ]
    )

    panel = build_futures_contract_daily(
        settlements=settlements,
        open_interest=open_interest,
        trade_summary=trade_summary,
        contract_master=contract_master,
    )
    f26 = panel.filter(pl.col("contract_id") == "DI1_F26").row(0, named=True)

    assert f26["available_date"] == date(2024, 1, 5)
    assert f26["source_datasets"] == (
        "b3_derivatives_open_interest|b3_derivatives_trade_summary|b3_futures_settlements"
    )
    assert f26["settlement"] == 10.0
    assert f26["volume"] == 200
    assert f26["open_interest"] == 1000
    assert f26["quote_convention"] == "price"
    assert f26["contract_rank_by_maturity"] == 1
    assert f26["is_tradeable"] is True


def test_futures_contract_panel_rejects_duplicate_source_merge_rows():
    settlements = pl.DataFrame(
        [
            _source_row("b3_futures_settlements", "DI1", "F26", settlement=10.0),
            _source_row("b3_futures_settlements", "DI1", "F26", settlement=10.1),
        ]
    )

    with pytest.raises(ValueError, match="duplicate source merge rows"):
        build_futures_contract_daily(settlements=settlements)


def test_futures_contract_panel_does_not_overwrite_with_trade_nulls():
    settlements = pl.DataFrame(
        [_source_row("b3_futures_settlements", "DI1", "F26", settlement=10.0, volume=123)]
    )
    trade_summary = pl.DataFrame(
        [_source_row("b3_derivatives_trade_summary", "DI1", "F26", volume=None)]
    )

    panel = build_futures_contract_daily(settlements=settlements, trade_summary=trade_summary)

    assert panel["volume"].item() == 123


def _source_row(
    source_dataset: str,
    commodity: str,
    maturity_code: str,
    *,
    available_date: date = date(2024, 1, 3),
    settlement: float | None = None,
    volume: int | None = None,
    financial_volume: float | None = None,
    number_of_trades: int | None = None,
    open_interest: int | None = None,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": available_date,
        "source_dataset": source_dataset,
        "commodity": commodity,
        "maturity_code": maturity_code,
        "contract_id": f"{commodity}_{maturity_code}",
        "settlement": settlement,
        "volume": volume,
        "financial_volume": financial_volume,
        "number_of_trades": number_of_trades,
        "open_interest": open_interest,
        "source_version": "v0",
    }
