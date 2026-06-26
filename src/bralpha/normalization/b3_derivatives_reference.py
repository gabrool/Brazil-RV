from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.domain.b3_contracts import build_b3_contract_id
from bralpha.domain.instruments import asset_class_for_root
from bralpha.parsing.common import parse_decimal, write_source_partitioned

DERIVATIVES_REFERENCE_PRICE_COLUMNS = [
    "ref_date",
    "available_date",
    "asset_id",
    "contract_id",
    "symbol",
    "commodity",
    "maturity_code",
    "asset_class",
    "price_type",
    "reference_price",
    "currency",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_derivatives_reference_prices(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        commodity = _text(row.get("commodity"))
        maturity_code = _text(row.get("maturity_code"))
        symbol = _text(row.get("symbol"))
        contract_id = _text(row.get("contract_id"))
        if contract_id is None and commodity and maturity_code:
            contract_id = build_b3_contract_id(commodity, maturity_code)
        if symbol is None and commodity and maturity_code:
            symbol = f"{commodity}{maturity_code}"
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "asset_id": contract_id or symbol,
                "contract_id": contract_id,
                "symbol": symbol,
                "commodity": commodity,
                "maturity_code": maturity_code,
                "asset_class": _text(row.get("asset_class"))
                or (asset_class_for_root(commodity) if commodity else None),
                "price_type": _text(row.get("price_type")) or "REFERENCE_PRICE",
                "reference_price": parse_decimal(row.get("reference_price")),
                "currency": _text(row.get("currency")) or "BRL",
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_derivatives_reference_prices"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, DERIVATIVES_REFERENCE_PRICE_COLUMNS)


def write_derivatives_reference_prices(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
) -> list[Path]:
    return write_source_partitioned(frame, output_root, primary_keys=primary_keys)


def _frame(rows: list[dict[str, object]], columns: list[str]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in columns})
    return pl.DataFrame(rows).select(columns)


def _required_date(value: object) -> date:
    parsed = _optional_date(value)
    if parsed is None:
        raise ValueError("date value is required")
    return parsed


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
