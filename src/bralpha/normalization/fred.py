from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.fred.common import (
    FRED_LATEST_SNAPSHOT_REQUEST,
    FRED_VINTAGE_REQUEST,
    FredSeriesConfig,
    write_partitioned_frame,
)
from bralpha.parsing.common import parse_decimal
from bralpha.timing.availability import usable_date_from_date_only
from bralpha.timing.vintages import (
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    AVAILABILITY_SOURCE_DATE_ONLY,
    REVISION_REVISED_USE_VINTAGES,
    REVISION_UNREVISED,
    available_date_from_vintage_date,
    make_vintage_id,
    model_usable_from_revision_policy,
)

FRED_SILVER_COLUMNS = [
    "series_id",
    "series_name",
    "category",
    "frequency",
    "unit",
    "ref_date",
    "vintage_date",
    "vintage_id",
    "available_date",
    "availability_policy",
    "availability_basis",
    "series_kind",
    "vintage_policy",
    "vintage_request_mode",
    "revision_policy",
    "value",
    "raw_value",
    "value_status",
    "realtime_start",
    "realtime_end",
    "model_usable",
    "source",
    "source_dataset",
    "first_seen_timestamp_utc",
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
        vintage_date = _parse_date(row.get("vintage_date") or row.get("realtime_start"))
        vintage_request_mode = _vintage_request_mode(row)
        raw_value = _raw_text(row.get("raw_value"))
        value, value_status = _value(raw_value)
        vintage_required = bool(
            config and config.vintage_policy == "fred_realtime_vintages_required"
        )
        available_date = _available_date(ref_date, vintage_date, config)
        vintage_id = _vintage_id(row, series_id, vintage_date, config)
        revision_policy = (
            REVISION_REVISED_USE_VINTAGES if vintage_required else REVISION_UNREVISED
        )
        availability_basis = _availability_basis(
            vintage_required=vintage_required,
            vintage_request_mode=vintage_request_mode,
        )
        rows.append(
            {
                "series_id": series_id,
                "series_name": config.name if config else None,
                "category": config.category if config else None,
                "frequency": config.frequency if config else None,
                "unit": config.unit if config else None,
                "ref_date": ref_date,
                "vintage_date": vintage_date,
                "vintage_id": vintage_id,
                "available_date": available_date,
                "availability_policy": (
                    config.availability_policy if config else "unknown"
                ),
                "availability_basis": availability_basis,
                "series_kind": config.series_kind if config else None,
                "vintage_policy": config.vintage_policy if config else "unknown",
                "vintage_request_mode": vintage_request_mode,
                "revision_policy": revision_policy,
                "value": value,
                "raw_value": raw_value,
                "value_status": value_status,
                "realtime_start": _parse_date(row.get("realtime_start")),
                "realtime_end": _parse_date(row.get("realtime_end")),
                "model_usable": bool(
                    config
                    and available_date is not None
                    and model_usable_from_revision_policy(
                        configured_model_usable=config.model_usable,
                        revision_policy=revision_policy,
                        vintage_id=vintage_id,
                        availability_basis=availability_basis,
                        model_usable_without_vintage=config.model_usable_without_vintage,
                    )
                ),
                "source": row.get("source", "fred"),
                "source_dataset": row.get("source_dataset", "fred_series_observations"),
                "first_seen_timestamp_utc": row.get("download_timestamp_utc"),
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
    vintage_date: date | None,
    config: FredSeriesConfig | None,
) -> date | None:
    if config is None:
        return None
    if config.vintage_policy == "fred_realtime_vintages_required":
        return available_date_from_vintage_date(vintage_date)
    if ref_date is None:
        return None
    if config.availability_policy == "date_only_next_business_day":
        return usable_date_from_date_only(ref_date)
    return None


def _availability_basis(*, vintage_required: bool, vintage_request_mode: str) -> str:
    if not vintage_required:
        return AVAILABILITY_SOURCE_DATE_ONLY
    if vintage_request_mode == FRED_VINTAGE_REQUEST:
        return FRED_VINTAGE_REQUEST
    return AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE


def _vintage_request_mode(row: dict[str, Any]) -> str:
    value = row.get("vintage_request_mode")
    if value is None:
        return FRED_LATEST_SNAPSHOT_REQUEST
    text = str(value).strip()
    return text or FRED_LATEST_SNAPSHOT_REQUEST


def _vintage_id(
    row: dict[str, Any],
    series_id: str,
    vintage_date: date | None,
    config: FredSeriesConfig | None,
) -> str | None:
    if config is None:
        return None
    if vintage_date is None and config.vintage_policy == "fred_realtime_vintages_required":
        return None
    return make_vintage_id(
        source="fred",
        dataset_id=str(row.get("source_dataset", "fred_series_observations")),
        resource_id=series_id,
        publication_timestamp=vintage_date,
        first_seen_timestamp_utc=row.get("download_timestamp_utc"),
        content_hash=str(row.get("sha256") or ""),
    )


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
