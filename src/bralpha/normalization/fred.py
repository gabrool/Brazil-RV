from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.fred.common import FredSeriesConfig, write_partitioned_frame
from bralpha.parsing.common import parse_decimal
from bralpha.timing.availability import usable_date_from_date_only

FRED_SILVER_COLUMNS = [
    "series_id",
    "series_name",
    "category",
    "frequency",
    "unit",
    "ref_date",
    "available_date",
    "availability_policy",
    "value",
    "raw_value",
    "value_status",
    "realtime_start",
    "realtime_end",
    "model_usable",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_fred_observations_to_silver(
    bronze: pl.DataFrame,
    *,
    series_config: list[FredSeriesConfig],
    source_version: str = "v0",
) -> pl.DataFrame:
    config_by_id = {config.series_id.upper(): config for config in series_config}
    rows = []
    for row in bronze.to_dicts():
        series_id = str(row["series_id"]).upper()
        config = config_by_id.get(series_id)
        ref_date = _parse_date(row.get("ref_date"))
        raw_value = _raw_text(row.get("raw_value"))
        value, value_status = _value(raw_value)
        available_date = _available_date(ref_date, config)
        rows.append(
            {
                "series_id": series_id,
                "series_name": config.name if config else None,
                "category": config.category if config else None,
                "frequency": config.frequency if config else None,
                "unit": config.unit if config else None,
                "ref_date": ref_date,
                "available_date": available_date,
                "availability_policy": (
                    config.availability_policy if config else "unknown"
                ),
                "value": value,
                "raw_value": raw_value,
                "value_status": value_status,
                "realtime_start": _parse_date(row.get("realtime_start")),
                "realtime_end": _parse_date(row.get("realtime_end")),
                "model_usable": bool(config and config.model_usable and available_date is not None),
                "source": row.get("source", "fred"),
                "source_dataset": row.get("source_dataset", "fred_series_observations"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows)


def write_fred_silver(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    primary_keys: list[str],
    partition_cols: list[str],
) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=primary_keys,
        ref_date_col="ref_date",
        partition_cols=partition_cols,
    )


def _value(raw_value: str | None) -> tuple[float | None, str]:
    if raw_value is None or raw_value.strip() in {"", "."}:
        return None, "missing"
    return parse_decimal(raw_value), "ok"


def _raw_text(value: object) -> str | None:
    if value is None:
        return None
    return str(value).strip()


def _available_date(
    ref_date: date | None,
    config: FredSeriesConfig | None,
) -> date | None:
    if ref_date is None or config is None:
        return None
    if config.availability_policy == "date_only_next_business_day":
        return usable_date_from_date_only(ref_date)
    return None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in FRED_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(FRED_SILVER_COLUMNS)
