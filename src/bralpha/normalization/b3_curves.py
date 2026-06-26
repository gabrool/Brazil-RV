from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.parsing.common import parse_decimal, parse_int, write_source_partitioned

CURVE_DAILY_COLUMNS = [
    "ref_date",
    "available_date",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "curve_id",
    "tenor_days",
    "forward_date",
    "rate",
    "rate_type",
    "compounding",
    "business_day_basis",
    "currency",
    "source_version",
]


def normalize_reference_rates_to_curve_daily(
    bronze: pl.DataFrame,
    *,
    holidays: set[date] | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        ref_date = _as_date(row["ref_date"])
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": _optional_date(row.get("available_date"))
                or next_business_day(ref_date, holidays),
                "source": row.get("source", "b3"),
                "source_dataset": row.get("source_dataset", "b3_reference_rates"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "curve_id": _text(row.get("curve_id")) or _text(row.get("curve")) or "B3_REFERENCE",
                "tenor_days": parse_int(row.get("tenor_days")),
                "forward_date": _optional_date(row.get("forward_date")),
                "rate": _rate(row.get("rate")),
                "rate_type": _text(row.get("rate_type")) or "nominal",
                "compounding": _text(row.get("compounding")) or "annual",
                "business_day_basis": _text(row.get("business_day_basis")) or "business_252",
                "currency": _text(row.get("currency")) or "BRL",
                "source_version": source_version,
            }
        )
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in CURVE_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(CURVE_DAILY_COLUMNS)


def write_curve_daily(
    frame: pl.DataFrame,
    output_root: Path,
    primary_keys: list[str],
) -> list[Path]:
    return write_source_partitioned(frame, output_root, primary_keys=primary_keys)


def _rate(value: object) -> float | None:
    parsed = parse_decimal(value)
    if parsed is None:
        return None
    return parsed / 100 if parsed > 2 else parsed


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return _as_date(text)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
