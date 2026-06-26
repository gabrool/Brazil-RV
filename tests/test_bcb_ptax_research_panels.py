from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.derived.bcb.ptax import build_ptax_selected_daily


def test_ptax_selected_panel_keeps_only_selected_bulletin_and_raw_quotes():
    silver = pl.DataFrame(
        [
            _ptax_row("USD", "Abertura", False, 4.90, 4.91),
            _ptax_row("USD", "Fechamento", True, 5.00, 5.01),
        ]
    )

    panel = build_ptax_selected_daily(
        silver,
        currencies=["USD"],
        use_selected_bulletin_only=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    row = panel.row(0, named=True)
    assert panel.height == 1
    assert row["selected_bulletin_type"] == "Fechamento"
    assert row["bid_rate"] == 5.00
    assert row["ask_rate"] == 5.01
    assert row["bid_parity"] == 1.0
    assert row["ask_parity"] == 1.0
    assert row["has_quote"] is True


def test_ptax_selected_panel_has_one_row_per_date_currency():
    silver = pl.DataFrame(
        [
            _ptax_row("USD", "Fechamento", True, 5.00, 5.01),
            _ptax_row("EUR", "Fechamento", True, 6.00, 6.01),
        ]
    )

    panel = build_ptax_selected_daily(
        silver,
        currencies=["USD", "EUR"],
        use_selected_bulletin_only=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    assert panel.group_by(["ref_date", "currency_code"]).len().height == 2


def test_ptax_panel_does_not_compute_mid_spread_or_returns():
    panel = build_ptax_selected_daily(
        pl.DataFrame([_ptax_row("USD", "Fechamento", True, 5.00, 5.01)]),
        currencies=["USD"],
        use_selected_bulletin_only=True,
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    assert {"mid", "spread", "return", "basis"}.isdisjoint(panel.columns)


def _ptax_row(
    currency_code: str,
    bulletin_type: str,
    is_selected_bulletin: bool,
    bid_rate: float,
    ask_rate: float,
) -> dict[str, object]:
    return {
        "ref_date": date(2024, 1, 2),
        "available_date": date(2024, 1, 2),
        "currency_code": currency_code,
        "currency_name": currency_code,
        "endpoint": "ExchangeRatePeriod",
        "bulletin_type": bulletin_type,
        "quote_datetime": datetime(2024, 1, 2, 13),
        "is_selected_bulletin": is_selected_bulletin,
        "bid_rate": bid_rate,
        "ask_rate": ask_rate,
        "bid_parity": 1.0,
        "ask_parity": 1.0,
        "source_version": "v0",
    }
