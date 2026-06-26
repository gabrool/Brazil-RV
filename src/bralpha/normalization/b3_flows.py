from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.parsing.common import (
    normalize_column_name,
    parse_decimal,
    parse_int,
    write_source_partitioned,
)

EQUITIES_INVESTOR_PARTICIPATION_COLUMNS = [
    "ref_date",
    "available_date",
    "market_segment",
    "investor_type",
    "buy_value",
    "sell_value",
    "net_value",
    "buy_volume",
    "sell_volume",
    "net_volume",
    "participation_pct",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]

FOREIGN_INVESTOR_MOVEMENT_COLUMNS = [
    "ref_date",
    "available_date",
    "market_segment",
    "foreign_buy_value",
    "foreign_sell_value",
    "foreign_net_value",
    "foreign_balance_or_position",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_equities_investor_participation(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "market_segment": _text(_value(row, "market_segment", "segmento", "mercado"))
                or "EQUITIES",
                "investor_type": _text(_value(row, "investor_type", "tipo_investidor")),
                "buy_value": parse_decimal(_value(row, "buy_value", "valor_compra", "compra")),
                "sell_value": parse_decimal(_value(row, "sell_value", "valor_venda", "venda")),
                "net_value": parse_decimal(_value(row, "net_value", "saldo", "valor_liquido")),
                "buy_volume": parse_int(_value(row, "buy_volume", "volume_compra")),
                "sell_volume": parse_int(_value(row, "sell_volume", "volume_venda")),
                "net_volume": parse_int(_value(row, "net_volume", "volume_liquido")),
                "participation_pct": parse_decimal(
                    _value(row, "participation_pct", "participacao", "part")
                ),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_equities_investor_participation"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, EQUITIES_INVESTOR_PARTICIPATION_COLUMNS)


def normalize_foreign_investor_movement(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "market_segment": _text(_value(row, "market_segment", "segmento", "mercado"))
                or "EQUITIES",
                "foreign_buy_value": parse_decimal(
                    _value(row, "foreign_buy_value", "buy_value", "valor_compra", "compra")
                ),
                "foreign_sell_value": parse_decimal(
                    _value(row, "foreign_sell_value", "sell_value", "valor_venda", "venda")
                ),
                "foreign_net_value": parse_decimal(
                    _value(row, "foreign_net_value", "net_value", "valor_liquido")
                ),
                "foreign_balance_or_position": parse_decimal(
                    _value(
                        row,
                        "foreign_balance_or_position",
                        "balance",
                        "position",
                        "saldo",
                    )
                ),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_foreign_investor_movement"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, FOREIGN_INVESTOR_MOVEMENT_COLUMNS)


def write_flow_observations(
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


def _value(row: dict[str, object], *aliases: str) -> object:
    for alias in aliases:
        normalized = normalize_column_name(alias)
        if normalized in row:
            return row[normalized]
    return None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
