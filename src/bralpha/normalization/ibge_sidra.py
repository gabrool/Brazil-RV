from __future__ import annotations

from calendar import monthrange
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.ibge.sidra import SidraSeriesConfig
from bralpha.parsing.common import parse_decimal, write_source_partitioned

IBGE_SIDRA_SILVER_COLUMNS = [
    "dataset_slug",
    "aggregate_id",
    "variable_id",
    "variable_name",
    "unit",
    "period_code",
    "period_label",
    "ref_period_start",
    "ref_period_end",
    "ref_date",
    "release_date",
    "available_datetime_local",
    "available_datetime_utc",
    "available_date",
    "availability_policy",
    "availability_note",
    "model_usable",
    "geography_level",
    "geography_id",
    "geography_name",
    "classification_key",
    "classifications_json",
    "value",
    "raw_value",
    "value_status",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_sidra_to_silver(
    bronze: pl.DataFrame,
    *,
    series_config: list[SidraSeriesConfig],
    release_calendar: pl.DataFrame | None = None,
    source_version: str = "v0",
) -> pl.DataFrame:
    config_by_slug = {config.dataset_slug: config for config in series_config}
    calendar = _calendar_lookup(release_calendar)
    rows = []
    for row in bronze.to_dicts():
        config = config_by_slug.get(str(row.get("dataset_slug")))
        period_start, period_end = _period_bounds(row.get("period_code"), config=config)
        ref_date = period_end
        value, value_status = _value_and_status(row.get("raw_value"))
        release = _release_match(calendar, config, period_start, period_end)
        availability_policy = "unmatched_release_calendar"
        availability_note = None
        model_usable = False
        if period_start is None or period_end is None:
            availability_policy = "unparsed_period"
            availability_note = "period_code could not be mapped to dates"
        elif release is not None and release.get("available_date") is not None:
            availability_policy = str(release.get("availability_policy"))
            if config and config.model_usable and _release_calendar_product_is_verified(config):
                model_usable = True
            elif config and config.model_usable:
                availability_note = "release calendar product id is not verified"
        rows.append(
            {
                "dataset_slug": row.get("dataset_slug"),
                "aggregate_id": _int_or_none(row.get("aggregate_id")),
                "variable_id": str(row.get("variable_id")),
                "variable_name": row.get("variable_name"),
                "unit": row.get("unit"),
                "period_code": row.get("period_code"),
                "period_label": row.get("period_label") or row.get("period_code"),
                "ref_period_start": period_start,
                "ref_period_end": period_end,
                "ref_date": ref_date,
                "release_date": release.get("release_date") if release else None,
                "available_datetime_local": (
                    release.get("available_datetime_local") if release else None
                ),
                "available_datetime_utc": (
                    release.get("available_datetime_utc") if release else None
                ),
                "available_date": release.get("available_date") if release else None,
                "availability_policy": availability_policy,
                "availability_note": availability_note,
                "model_usable": model_usable,
                "geography_level": row.get("geography_level"),
                "geography_id": row.get("geography_id"),
                "geography_name": row.get("geography_name"),
                "classification_key": row.get("classification_key"),
                "classifications_json": row.get("classifications_json"),
                "value": value,
                "raw_value": row.get("raw_value"),
                "value_status": value_status,
                "source": row.get("source", "ibge"),
                "source_dataset": row.get("source_dataset", "ibge_sidra_series"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows)


def write_sidra_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        ref_date_col="ref_date",
        primary_keys=[
            "dataset_slug",
            "aggregate_id",
            "variable_id",
            "period_code",
            "geography_id",
            "classification_key",
        ],
    )


def _calendar_lookup(
    release_calendar: pl.DataFrame | None,
) -> dict[tuple[int, date, date], dict[str, Any]]:
    if release_calendar is None or release_calendar.is_empty():
        return {}
    required = {"product_id", "reference_period_start", "reference_period_end"}
    if not required.issubset(set(release_calendar.columns)):
        return {}
    lookup = {}
    sort_columns = [
        column
        for column in ["product_id", "reference_period_start", "available_date", "event_id"]
        if column in release_calendar.columns
    ]
    frame = release_calendar.sort(sort_columns) if sort_columns else release_calendar
    for row in frame.to_dicts():
        product_id = _int_or_none(row.get("product_id"))
        start = row.get("reference_period_start")
        end = row.get("reference_period_end")
        if product_id is None or not isinstance(start, date) or not isinstance(end, date):
            continue
        lookup.setdefault((product_id, start, end), row)
    return lookup


def _release_match(
    calendar: dict[tuple[int, date, date], dict[str, Any]],
    config: SidraSeriesConfig | None,
    period_start: date | None,
    period_end: date | None,
) -> dict[str, Any] | None:
    if (
        config is None
        or config.release_calendar_product_id is None
        or period_start is None
        or period_end is None
    ):
        return None
    return calendar.get((config.release_calendar_product_id, period_start, period_end))


def _release_calendar_product_is_verified(config: SidraSeriesConfig) -> bool:
    return (
        config.release_calendar_product_id is not None
        and config.release_calendar_product_id_status == "verified"
    )


def _period_bounds(
    period_code: object,
    *,
    config: SidraSeriesConfig | None,
) -> tuple[date | None, date | None]:
    text = "" if period_code is None else str(period_code).strip()
    if len(text) == 4 and text.isdigit():
        year = int(text)
        return date(year, 1, 1), date(year, 12, 31)
    if len(text) != 6 or not text.isdigit():
        return None, None

    year = int(text[:4])
    suffix = int(text[4:])
    frequency = config.frequency if config else ""
    if frequency == "quarterly":
        if suffix not in {1, 2, 3, 4}:
            return None, None
        start_month = (suffix - 1) * 3 + 1
        end_month = suffix * 3
        return (
            date(year, start_month, 1),
            date(year, end_month, monthrange(year, end_month)[1]),
        )
    if suffix < 1 or suffix > 12:
        return None, None
    end = date(year, suffix, monthrange(year, suffix)[1])
    if frequency == "moving_quarter_monthly":
        start = _add_months(date(year, suffix, 1), -2)
        return start, end
    return date(year, suffix, 1), end


def _value_and_status(raw_value: object) -> tuple[float | None, str]:
    if raw_value is None:
        return None, "missing"
    text = str(raw_value).strip()
    if text in {"", "..", "..."}:
        return None, "missing"
    if text in {"-", "--"}:
        return None, "not_applicable"
    if text.upper() == "X":
        return None, "withheld"
    try:
        value = parse_decimal(text)
    except ValueError:
        return None, "unparsed"
    if value is None:
        return None, "unparsed"
    return value, "zero_absolute" if value == 0 else "ok"


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    return date(value.year + month_index // 12, month_index % 12 + 1, 1)


def _int_or_none(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_SIDRA_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_SIDRA_SILVER_COLUMNS)
