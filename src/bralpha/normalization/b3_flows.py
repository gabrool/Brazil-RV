from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.parsing.common import parse_decimal, parse_int, write_source_partitioned

FLOW_OBSERVATION_COLUMNS = [
    "ref_date",
    "available_date",
    "flow_type",
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


def normalize_equities_investor_participation(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    return _normalize_flow_rows(
        bronze,
        flow_type="equities_investor_participation",
        source_dataset="b3_equities_investor_participation",
        holidays=holidays,
        source_version=source_version,
    )


def normalize_foreign_investor_movement(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    return _normalize_flow_rows(
        bronze,
        flow_type="foreign_investor_movement",
        source_dataset="b3_foreign_investor_movement",
        holidays=holidays,
        source_version=source_version,
        default_investor_type="FOREIGN",
    )


def write_flow_observations(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
) -> list[Path]:
    return write_source_partitioned(frame, output_root, primary_keys=primary_keys)


def _normalize_flow_rows(
    bronze: pl.DataFrame,
    *,
    flow_type: str,
    source_dataset: str,
    holidays: set[date] | None,
    source_version: str,
    default_investor_type: str | None = None,
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _required_date(row.get("ref_date"))
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "flow_type": flow_type,
                "market_segment": _text(row.get("market_segment")) or "EQUITIES",
                "investor_type": _text(row.get("investor_type")) or default_investor_type,
                "buy_value": parse_decimal(row.get("buy_value")),
                "sell_value": parse_decimal(row.get("sell_value")),
                "net_value": parse_decimal(row.get("net_value")),
                "buy_volume": parse_int(row.get("buy_volume")),
                "sell_volume": parse_int(row.get("sell_volume")),
                "net_volume": parse_int(row.get("net_volume")),
                "participation_pct": parse_decimal(row.get("participation_pct")),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", source_dataset),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows, FLOW_OBSERVATION_COLUMNS)


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
