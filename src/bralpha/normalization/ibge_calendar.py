from __future__ import annotations

from calendar import monthrange
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.parsing.common import write_source_partitioned
from bralpha.timing.availability import (
    DEFAULT_DECISION_CUTOFF_TIME,
    DEFAULT_TIMING_TIMEZONE,
    decision_cutoff_datetime,
    usable_date_from_available_datetime,
    usable_date_from_date_only,
)

IBGE_CALENDAR_SILVER_COLUMNS = [
    "event_id",
    "product_id",
    "product_name",
    "survey_code",
    "survey_name",
    "release_title",
    "release_date",
    "release_time_local",
    "available_datetime_local",
    "available_datetime_utc",
    "available_date",
    "availability_policy",
    "reference_period",
    "reference_period_start",
    "reference_period_end",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_calendar_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        release_datetime, exact_time = _parse_ibge_datetime(row.get("data_divulgacao"))
        if release_datetime is None:
            release_date = None
            release_time = None
            available_date = None
            available_datetime_utc = None
            policy = "missing_release_datetime"
        else:
            release_date = release_datetime.date()
            release_time = release_datetime.time() if exact_time else None
            available_date, policy = _available_date(release_datetime, exact_time=exact_time)
            available_datetime_utc = _to_utc_naive(release_datetime) if exact_time else None

        period_start, period_end = _reference_period_bounds(row)
        rows.append(
            {
                "event_id": _int_or_none(row.get("id")),
                "product_id": _int_or_none(row.get("produto_id")),
                "product_name": _clean_text(row.get("nome_produto")),
                "survey_code": _clean_text(row.get("alias_produto")),
                "survey_name": _clean_text(row.get("nome_produto")),
                "release_title": _clean_text(row.get("titulo")),
                "release_date": release_date,
                "release_time_local": release_time,
                "available_datetime_local": release_datetime if exact_time else None,
                "available_datetime_utc": available_datetime_utc,
                "available_date": available_date,
                "availability_policy": policy,
                "reference_period": _reference_period_label(row),
                "reference_period_start": period_start,
                "reference_period_end": period_end,
                "source": row.get("source", "ibge"),
                "source_dataset": row.get("source_dataset", "ibge_release_calendar"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows)


def write_calendar_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        ref_date_col="release_date",
        primary_keys=["event_id"],
    )


def _available_date(release_datetime: datetime, *, exact_time: bool) -> tuple[date, str]:
    if exact_time:
        return (
            usable_date_from_available_datetime(
                release_datetime,
                cutoff_time=DEFAULT_DECISION_CUTOFF_TIME,
            ),
            "exact_timestamp_cutoff",
        )
    return usable_date_from_date_only(release_datetime.date()), "date_only_next_business_day"


def _reference_period_bounds(row: dict[str, Any]) -> tuple[date | None, date | None]:
    start_year = _int_or_none(row.get("ano_referencia_inicio"))
    end_year = _int_or_none(row.get("ano_referencia_fim")) or start_year
    if start_year is None or end_year is None:
        return None, None
    start_month = _int_or_none(row.get("mes_referencia_inicio")) or 1
    end_month = _int_or_none(row.get("mes_referencia_fim")) or 12
    start = date(start_year, start_month, 1)
    end = date(end_year, end_month, monthrange(end_year, end_month)[1])
    return start, end


def _reference_period_label(row: dict[str, Any]) -> str | None:
    start_year = _int_or_none(row.get("ano_referencia_inicio"))
    end_year = _int_or_none(row.get("ano_referencia_fim")) or start_year
    if start_year is None or end_year is None:
        return None
    start_month = _int_or_none(row.get("mes_referencia_inicio"))
    end_month = _int_or_none(row.get("mes_referencia_fim"))
    if start_month is None and end_month is None:
        return str(start_year) if start_year == end_year else f"{start_year}-{end_year}"
    end_month = end_month or start_month
    return f"{start_year}-{start_month:02d}/{end_year}-{end_month:02d}"


def _parse_ibge_datetime(value: object) -> tuple[datetime | None, bool]:
    text = _clean_text(value)
    if text is None:
        return None, False
    if " " in text:
        parsed = datetime.strptime(text, "%d/%m/%Y %H:%M:%S")
        return parsed, True
    parsed_date = datetime.strptime(text[:10], "%d/%m/%Y")
    return parsed_date, False


def _to_utc_naive(value: datetime) -> datetime:
    tzinfo = decision_cutoff_datetime(
        value.date(),
        cutoff_time=time(0),
        tz_name=DEFAULT_TIMING_TIMEZONE,
    ).tzinfo
    return value.replace(tzinfo=tzinfo).astimezone(UTC).replace(tzinfo=None)


def _int_or_none(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_CALENDAR_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_CALENDAR_SILVER_COLUMNS)
