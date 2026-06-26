from __future__ import annotations

from datetime import UTC, datetime, time
from pathlib import Path

import polars as pl

from bralpha.parsing.common import write_source_partitioned
from bralpha.timing.availability import (
    DEFAULT_DECISION_CUTOFF_TIME,
    DEFAULT_TIMING_TIMEZONE,
    decision_cutoff_datetime,
    usable_date_from_available_datetime,
    usable_date_from_date_only,
)

IBGE_NEWS_SILVER_COLUMNS = [
    "news_id",
    "product_id",
    "product_name",
    "title",
    "type",
    "published_datetime_local",
    "published_datetime_utc",
    "published_date",
    "available_date",
    "url",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "source_version",
]


def normalize_news_to_silver(
    bronze: pl.DataFrame,
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = []
    for row in bronze.to_dicts():
        published_datetime, exact_time = _parse_ibge_datetime(row.get("data_publicacao"))
        if published_datetime is None:
            published_date = None
            available_date = None
            published_datetime_utc = None
        else:
            published_date = published_datetime.date()
            available_date = (
                usable_date_from_available_datetime(
                    published_datetime,
                    cutoff_time=DEFAULT_DECISION_CUTOFF_TIME,
                )
                if exact_time
                else usable_date_from_date_only(published_datetime.date())
            )
            published_datetime_utc = _to_utc_naive(published_datetime) if exact_time else None
        rows.append(
            {
                "news_id": _int_or_none(row.get("id")),
                "product_id": _int_or_none(row.get("produto_id")),
                "product_name": _product_name(row.get("produtos")),
                "title": _clean_text(row.get("titulo")),
                "type": _clean_text(row.get("tipo")),
                "published_datetime_local": published_datetime if exact_time else None,
                "published_datetime_utc": published_datetime_utc,
                "published_date": published_date,
                "available_date": available_date,
                "url": _clean_text(row.get("link")),
                "source": row.get("source", "ibge"),
                "source_dataset": row.get("source_dataset", "ibge_news_releases_metadata"),
                "download_timestamp_utc": row.get("download_timestamp_utc"),
                "raw_path": row.get("raw_path"),
                "sha256": row.get("sha256"),
                "source_version": source_version,
            }
        )
    return _frame(rows)


def write_news_silver(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_source_partitioned(
        frame,
        output_root,
        ref_date_col="published_date",
        primary_keys=["news_id"],
    )


def _parse_ibge_datetime(value: object) -> tuple[datetime | None, bool]:
    text = _clean_text(value)
    if text is None:
        return None, False
    if " " in text:
        return datetime.strptime(text, "%d/%m/%Y %H:%M:%S"), True
    return datetime.strptime(text[:10], "%d/%m/%Y"), False


def _to_utc_naive(value: datetime) -> datetime:
    tzinfo = decision_cutoff_datetime(
        value.date(),
        cutoff_time=time(0),
        tz_name=DEFAULT_TIMING_TIMEZONE,
    ).tzinfo
    return value.replace(tzinfo=tzinfo).astimezone(UTC).replace(tzinfo=None)


def _product_name(value: object) -> str | None:
    text = _clean_text(value)
    if text is None or "|" not in text:
        return None
    parts = text.split("|")
    if len(parts) < 2:
        return None
    return parts[1].split("#", maxsplit=1)[0].strip() or None


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
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_NEWS_SILVER_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_NEWS_SILVER_COLUMNS)
