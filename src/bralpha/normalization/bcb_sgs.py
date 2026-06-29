from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl

from bralpha.domain.b3_calendar import next_business_day
from bralpha.ingestion.bcb.sgs import SgsSeriesConfig
from bralpha.parsing.common import parse_decimal, write_source_partitioned
from bralpha.timing.vintages import model_usable_from_revision_policy

BCB_SGS_SILVER_COLUMNS = [
    "ref_date",
    "available_date",
    "series_id",
    "series_slug",
    "series_name",
    "category",
    "frequency",
    "value",
    "unit",
    "availability_policy",
    "availability_basis",
    "revision_policy",
    "model_usable",
    "source_reference_url",
    "notes",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_sgs_to_silver(
    bronze: pl.DataFrame,
    *,
    series_config: list[SgsSeriesConfig],
    source_version: str = "v0",
) -> pl.DataFrame:
    config_by_id = {config.series_id: config for config in series_config}
    rows = []
    for row in bronze.to_dicts():
        series_id = int(row["series_id"])
        config = config_by_id.get(series_id)
        ref_date = _parse_bcb_date(row["data"])
        available_date = _available_date(ref_date, config)
        model_usable = _model_usable(config, available_date)
        rows.append(
            {
                "ref_date": ref_date,
                "available_date": available_date,
                "series_id": series_id,
                "series_slug": config.slug if config else None,
                "series_name": config.name if config else None,
                "category": config.category if config else None,
                "frequency": config.frequency if config else None,
                "value": parse_decimal(row.get("valor")),
                "unit": config.unit if config else None,
                "availability_policy": config.availability_policy if config else "unknown",
                "availability_basis": config.availability_basis if config else "unknown",
                "revision_policy": config.revision_policy if config else "unknown",
                "model_usable": model_usable,
                "source_reference_url": config.source_reference_url if config else None,
                "notes": config.notes if config else None,
                "source": row.get("source", "bcb"),
                "source_dataset": row.get("source_dataset", "bcb_sgs_series"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows)


def write_sgs_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        primary_keys=["series_id", "ref_date"],
    )


def _available_date(
    ref_date: date,
    config: SgsSeriesConfig | None,
) -> date | None:
    if config is None:
        return None
    if config.availability_policy == "next_business_day":
        return next_business_day(ref_date)
    if config.availability_policy == "configured_lag_days":
        lag_days = config.availability_lag_days
        if lag_days is None:
            return None
        return ref_date + timedelta(days=lag_days)
    if config.availability_policy == "same_day":
        return ref_date
    return None


def _model_usable(config: SgsSeriesConfig | None, available_date: date | None) -> bool:
    if config is None or available_date is None:
        return False
    return model_usable_from_revision_policy(
        configured_model_usable=config.model_usable,
        revision_policy=config.revision_policy,
    )


def _parse_bcb_date(value: object) -> date:
    if isinstance(value, date):
        return value
    text = str(value)
    if "/" in text:
        day, month, year = text.split("/")
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_SGS_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(BCB_SGS_SILVER_COLUMNS)
