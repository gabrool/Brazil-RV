from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from bralpha.normalization.b3_market_daily import (
    normalize_cotahist_to_market_daily,
    normalize_indexes_historical_to_market_daily,
)
from bralpha.normalization.b3_reference import (
    INDEX_COMPOSITION_COLUMNS,
    REFERENCE_SECURITY_COLUMNS,
    normalize_index_composition,
    normalize_traded_securities,
)
from bralpha.parsing.b3_cotahist import (
    iter_cotahist_chunks,
    parse_cotahist_file,
    parse_cotahist_line,
)
from bralpha.quality.checks import QualityCheckError, run_quality_checks


def test_cotahist_fixed_width_line_parses():
    line = _cotahist_line()
    parsed = parse_cotahist_line(line)

    assert parsed is not None
    assert parsed["ref_date"] == date(2024, 1, 2)
    assert parsed["symbol"] == "PETR4"
    assert parsed["market_type"] == "010"
    assert parsed["open"] == 10.0
    assert parsed["close"] == 10.5
    assert parsed["volume"] == 1234


def test_cotahist_chunked_parser(tmp_path):
    path = tmp_path / "COTAHIST_A2024.TXT"
    path.write_text("00HEADER\n" + _cotahist_line() + "\n99TRAILER\n", encoding="latin1")

    chunks = list(iter_cotahist_chunks(path, chunk_size=1))
    parsed = parse_cotahist_file(path, chunk_size=1)

    assert len(chunks) == 1
    assert parsed.height == 1
    assert parsed["financial_volume"].item() == 20000.0


def test_cotahist_silver_market_daily_quality():
    bronze = pl.DataFrame([parse_cotahist_line(_cotahist_line())])
    silver = normalize_cotahist_to_market_daily(bronze)

    assert silver["asset_class"].item() == "equity"
    assert silver["close"].item() == 10.5
    run_quality_checks(
        silver,
        check_names=["row_count_not_zero", "no_duplicate_primary_keys", "nonnegative_volume"],
        primary_keys=["ref_date", "symbol"],
        required_columns=["ref_date", "symbol", "volume"],
    )


def test_index_history_normalizes_to_market_daily_and_checks_positive_value():
    bronze = pl.DataFrame(
        [
            {
                "ref_date": date(2024, 1, 2),
                "index_id": "IBOV",
                "index_value": 130000.0,
                "source": "b3",
                "source_dataset": "b3_indexes_historical_data",
            }
        ]
    )
    silver = normalize_indexes_historical_to_market_daily(bronze)

    assert silver["asset_class"].item() == "index"
    assert silver["close"].item() == 130000.0
    run_quality_checks(
        silver,
        check_names=["row_count_not_zero", "no_duplicate_primary_keys", "positive_index_value"],
        primary_keys=["ref_date", "symbol"],
        required_columns=["ref_date", "symbol", "close"],
    )


def test_index_composition_and_security_reference_outputs():
    composition = normalize_index_composition(
        pl.DataFrame(
            [
                {
                    "ref_date": date(2024, 1, 2),
                    "index_id": "IBOV",
                    "symbol": "PETR4",
                    "isin": "BRPETRACNPR6",
                    "name": "PETROBRAS PN",
                    "weight": "10,5",
                    "theoretical_quantity": "1000",
                }
            ]
        )
    )
    securities = normalize_traded_securities(
        pl.DataFrame(
            [
                {
                    "symbol": "PETR4",
                    "isin": "BRPETRACNPR6",
                    "name": "PETROBRAS PN",
                    "market_type": "010",
                    "asset_class": "equity",
                }
            ]
        )
    )

    assert composition.columns == INDEX_COMPOSITION_COLUMNS
    assert composition["weight"].item() == 10.5
    assert securities.columns == REFERENCE_SECURITY_COLUMNS
    assert securities["security_id"].item() == "PETR4_010"


def test_index_composition_weight_check_fails_on_negative():
    composition = pl.DataFrame(
        [{"ref_date": date(2024, 1, 2), "index_id": "IBOV", "symbol": "PETR4", "weight": -1.0}]
    )
    with pytest.raises(QualityCheckError):
        run_quality_checks(
            composition,
            check_names=["nonnegative_weight_where_present"],
            primary_keys=["ref_date", "index_id", "symbol"],
            required_columns=["ref_date", "index_id", "symbol", "weight"],
        )


def _cotahist_line() -> str:
    chars = [" "] * 245

    def put(start: int, end: int, value: str) -> None:
        chars[start - 1 : end] = list(value.ljust(end - start + 1)[: end - start + 1])

    def put_num(start: int, end: int, value: int) -> None:
        put(start, end, str(value).zfill(end - start + 1))

    put(1, 2, "01")
    put(3, 10, "20240102")
    put(11, 12, "02")
    put(13, 24, "PETR4")
    put(25, 27, "010")
    put(28, 39, "PETROBRAS")
    put(40, 49, "PN")
    put_num(57, 69, 1000)
    put_num(70, 82, 1100)
    put_num(83, 95, 900)
    put_num(96, 108, 1005)
    put_num(109, 121, 1050)
    put_num(122, 134, 1040)
    put_num(135, 147, 1060)
    put_num(148, 152, 12)
    put_num(153, 170, 1234)
    put_num(171, 188, 2000000)
    return "".join(chars)
