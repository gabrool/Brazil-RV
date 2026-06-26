from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.normalization.b3_curves import (
    CURVE_DAILY_COLUMNS,
    normalize_reference_rates_to_curve_daily,
)
from bralpha.normalization.b3_market_daily import (
    normalize_open_interest_to_market_daily,
    normalize_trade_summary_to_market_daily,
)
from bralpha.normalization.b3_reference import (
    REFERENCE_CALENDAR_COLUMNS,
    REFERENCE_CONTRACT_COLUMNS,
    normalize_contract_master,
    normalize_holiday_calendar,
)
from bralpha.parsing.b3_settlements import parse_settlements_bytes
from bralpha.quality.checks import QualityCheckError, run_quality_checks


def test_open_interest_normalizes_to_market_daily():
    bronze = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "commodity": "DI1",
                "maturity_code": "F26",
                "open_interest": 123,
                "source": "b3",
                "source_dataset": "b3_derivatives_open_interest",
            }
        ]
    )
    silver = normalize_open_interest_to_market_daily(bronze)

    assert silver["contract_id"].item() == "DI1_F26"
    assert silver["open_interest"].item() == 123
    assert silver["available_date"].item() == date(2024, 1, 3)


def test_trade_summary_normalizes_to_market_daily():
    bronze = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "commodity": "DOL",
                "maturity_code": "M25",
                "volume": "10",
                "financial_volume": "1.234,50",
                "number_of_trades": "3",
                "source": "b3",
                "source_dataset": "b3_derivatives_trade_summary",
            }
        ]
    )
    silver = normalize_trade_summary_to_market_daily(bronze)

    assert silver["contract_id"].item() == "DOL_M25"
    assert silver["volume"].item() == 10
    assert silver["financial_volume"].item() == 1234.5


def test_derivatives_html_parser_supports_open_interest_fields():
    html = b"""
    <table>
      <tr><th>VENCTO</th><th>CONTR. ABERT.(1)</th><th>CONTR. NEGOC.</th></tr>
      <tr><td>F26</td><td>1.000</td><td>200</td></tr>
    </table>
    """
    bronze = parse_settlements_bytes(
        html,
        ref_date=date(2024, 1, 2),
        commodity="DI1",
        source_dataset="b3_derivatives_open_interest",
        download_timestamp_utc=date(2024, 1, 2),
        raw_path=__file__,
        sha256="abc",
    )

    assert bronze["open_interest"].item() == 1000
    assert bronze["volume"].item() == 200


def test_reference_rates_curve_daily_quality():
    bronze = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "curve_id": "PRE",
                "tenor_days": "252",
                "forward_date": "2025-01-02",
                "rate": "12,50",
            }
        ]
    )
    curve = normalize_reference_rates_to_curve_daily(bronze)

    assert curve.columns == CURVE_DAILY_COLUMNS
    assert curve["rate"].item() == 0.125
    run_quality_checks(
        curve,
        check_names=[
            "row_count_not_zero",
            "no_duplicate_primary_keys",
            "rate_within_plausible_bounds",
            "available_date_on_or_after_ref_date",
        ],
        primary_keys=["ref_date", "curve_id", "tenor_days"],
        required_columns=CURVE_DAILY_COLUMNS,
    )


def test_reference_rates_plausibility_check_fails():
    curve = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "curve_id": "PRE", "tenor_days": 1, "rate": 3.0}]
    )
    with pytest.raises(QualityCheckError):
        run_quality_checks(
            curve,
            check_names=["rate_within_plausible_bounds"],
            primary_keys=["ref_date", "curve_id", "tenor_days"],
            required_columns=["ref_date", "curve_id", "tenor_days"],
        )


def test_contract_master_and_holiday_calendar_reference_outputs():
    contracts = normalize_contract_master(
        [
            {
                "contract_id": "DI1_F26",
                "symbol_root": "DI1",
                "commodity": "DI1",
                "asset_class": "rates",
                "maturity_code": "F26",
                "maturity_date": "2026-01-02",
                "contract_multiplier": "1",
                "tick_size": "0.001",
            }
        ]
    )
    holidays = normalize_holiday_calendar(
        [{"calendar_id": "B3", "ref_date": "2024-01-01", "holiday_name": "Confraternizacao"}]
    )

    assert contracts.columns == REFERENCE_CONTRACT_COLUMNS
    assert holidays.columns == REFERENCE_CALENDAR_COLUMNS
    run_quality_checks(
        contracts,
        check_names=["no_duplicate_primary_keys", "required_columns_present"],
        primary_keys=["contract_id"],
        required_columns=REFERENCE_CONTRACT_COLUMNS,
    )
