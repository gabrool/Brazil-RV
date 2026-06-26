from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.domain.b3_contracts import build_b3_contract_id
from bralpha.domain.instruments import asset_class_for_root
from bralpha.parsing.common import parse_decimal, parse_int, write_partitioned_by_year

MARKET_DAILY_COLUMNS = [
    "ref_date",
    "available_date",
    "source",
    "source_dataset",
    "asset_id",
    "contract_id",
    "symbol",
    "commodity",
    "maturity_code",
    "asset_class",
    "open",
    "high",
    "low",
    "close",
    "settlement",
    "previous_settlement",
    "price_change",
    "settlement_value",
    "volume",
    "financial_volume",
    "number_of_trades",
    "open_interest",
    "currency",
    "unit",
    "source_version",
]


def normalize_settlements_to_market_daily(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _as_date(row["ref_date"])
        commodity = _text(row.get("commodity"))
        maturity_code = _text(row.get("maturity_code"))
        contract_id = None
        symbol = None
        if commodity and maturity_code:
            contract_id = build_b3_contract_id(commodity, maturity_code)
            symbol = f"{commodity}{maturity_code}"
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": next_business_day(ref_date, holidays),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_futures_settlements"),
                "asset_id": contract_id,
                "contract_id": contract_id,
                "symbol": symbol,
                "commodity": commodity,
                "maturity_code": maturity_code,
                "asset_class": asset_class_for_root(commodity) if commodity else None,
                "open": parse_decimal(row.get("open")),
                "high": parse_decimal(row.get("high")),
                "low": parse_decimal(row.get("low")),
                "close": parse_decimal(row.get("close")),
                "settlement": parse_decimal(row.get("settlement")),
                "previous_settlement": parse_decimal(row.get("previous_settlement")),
                "price_change": parse_decimal(row.get("price_change")),
                "settlement_value": None,
                "volume": parse_int(row.get("volume")),
                "financial_volume": parse_decimal(row.get("financial_volume")),
                "number_of_trades": parse_int(row.get("number_of_trades")),
                "open_interest": parse_int(row.get("open_interest")),
                "currency": "BRL",
                "unit": None,
                "source_version": source_version,
            }
        )
    return _market_daily_frame(rows)


def normalize_open_interest_to_market_daily(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _as_date(row["ref_date"])
        commodity = _text(row.get("commodity"))
        maturity_code = _text(row.get("maturity_code"))
        contract_id = (
            build_b3_contract_id(commodity, maturity_code)
            if commodity and maturity_code
            else None
        )
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": next_business_day(ref_date, holidays),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_derivatives_open_interest"),
                "asset_id": contract_id,
                "contract_id": contract_id,
                "symbol": f"{commodity}{maturity_code}" if commodity and maturity_code else None,
                "commodity": commodity,
                "maturity_code": maturity_code,
                "asset_class": asset_class_for_root(commodity) if commodity else None,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "settlement": None,
                "previous_settlement": None,
                "price_change": None,
                "settlement_value": None,
                "volume": None,
                "financial_volume": None,
                "number_of_trades": None,
                "open_interest": parse_int(row.get("open_interest")),
                "currency": "BRL",
                "unit": None,
                "source_version": source_version,
            }
        )
    return _market_daily_frame(rows)


def normalize_trade_summary_to_market_daily(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _as_date(row["ref_date"])
        commodity = _text(row.get("commodity"))
        maturity_code = _text(row.get("maturity_code"))
        contract_id = (
            build_b3_contract_id(commodity, maturity_code)
            if commodity and maturity_code
            else None
        )
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": next_business_day(ref_date, holidays),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_derivatives_trade_summary"),
                "asset_id": contract_id,
                "contract_id": contract_id,
                "symbol": f"{commodity}{maturity_code}" if commodity and maturity_code else None,
                "commodity": commodity,
                "maturity_code": maturity_code,
                "asset_class": asset_class_for_root(commodity) if commodity else None,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "settlement": None,
                "previous_settlement": None,
                "price_change": None,
                "settlement_value": None,
                "volume": parse_int(row.get("volume")),
                "financial_volume": parse_decimal(row.get("financial_volume")),
                "number_of_trades": parse_int(row.get("number_of_trades")),
                "open_interest": None,
                "currency": "BRL",
                "unit": None,
                "source_version": source_version,
            }
        )
    return _market_daily_frame(rows)


def normalize_cotahist_to_market_daily(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _as_date(row["ref_date"])
        symbol = _text(row.get("symbol"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": next_business_day(ref_date, holidays),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_cotahist_yearly"),
                "asset_id": symbol,
                "contract_id": None,
                "symbol": symbol,
                "commodity": None,
                "maturity_code": None,
                "asset_class": _cotahist_asset_class(row.get("market_type")),
                "open": parse_decimal(row.get("open")),
                "high": parse_decimal(row.get("high")),
                "low": parse_decimal(row.get("low")),
                "close": parse_decimal(row.get("close")),
                "settlement": None,
                "previous_settlement": None,
                "price_change": None,
                "settlement_value": None,
                "volume": parse_int(row.get("volume")),
                "financial_volume": parse_decimal(row.get("financial_volume")),
                "number_of_trades": parse_int(row.get("number_of_trades")),
                "open_interest": None,
                "currency": "BRL",
                "unit": None,
                "source_version": source_version,
            }
        )
    return _market_daily_frame(rows)


def normalize_indexes_historical_to_market_daily(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _as_date(row["ref_date"])
        index_id = _text(row.get("index_id"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": next_business_day(ref_date, holidays),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_indexes_historical_data"),
                "asset_id": index_id,
                "contract_id": None,
                "symbol": index_id,
                "commodity": None,
                "maturity_code": None,
                "asset_class": "index",
                "open": parse_decimal(row.get("open")),
                "high": parse_decimal(row.get("high")),
                "low": parse_decimal(row.get("low")),
                "close": parse_decimal(row.get("close", row.get("index_value"))),
                "settlement": None,
                "previous_settlement": None,
                "price_change": None,
                "settlement_value": None,
                "volume": None,
                "financial_volume": None,
                "number_of_trades": None,
                "open_interest": None,
                "currency": "BRL",
                "unit": "index_points",
                "source_version": source_version,
            }
        )
    return _market_daily_frame(rows)


def write_market_daily(
    frame: pl.DataFrame,
    output_root: Path,
    primary_keys: list[str],
) -> list[Path]:
    return write_partitioned_by_year(frame, output_root, primary_keys=primary_keys)


def _market_daily_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in MARKET_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(MARKET_DAILY_COLUMNS)


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _cotahist_asset_class(market_type: object) -> str:
    text = str(market_type or "").strip()
    if text in {"010", "020"}:
        return "equity"
    if text in {"070", "080"}:
        return "option"
    return "listed_security"
