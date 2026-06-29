from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.fred.common import (
    FRED_LATEST_SNAPSHOT_REQUEST,
    write_partitioned_frame,
)

FRED_OBSERVATIONS_BRONZE_COLUMNS = [
    "series_id",
    "vintage_request_mode",
    "request_observation_start",
    "request_observation_end",
    "request_realtime_start",
    "request_realtime_end",
    "request_vintage_dates",
    "vintage_date",
    "realtime_start",
    "realtime_end",
    "observation_start",
    "observation_end",
    "units",
    "output_type",
    "file_type",
    "order_by",
    "sort_order",
    "count",
    "offset",
    "limit",
    "ref_date",
    "raw_value",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_fred_observations_bytes(
    content: bytes,
    *,
    series_id: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    vintage_request_mode: str = FRED_LATEST_SNAPSHOT_REQUEST,
    request_observation_start: date | str | None = None,
    request_observation_end: date | str | None = None,
    request_realtime_start: date | str | None = None,
    request_realtime_end: date | str | None = None,
    request_vintage_dates: Sequence[date | str] | None = None,
) -> pl.DataFrame:
    payload = json.loads(content.decode("utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("FRED observations JSON payload must be an object")
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise ValueError("FRED observations JSON payload must contain observations")
    timestamp = _naive_utc(download_timestamp_utc)
    rows = [
        {
            "series_id": series_id,
            "vintage_request_mode": vintage_request_mode,
            "request_observation_start": _date_text(request_observation_start),
            "request_observation_end": _date_text(request_observation_end),
            "request_realtime_start": _date_text(request_realtime_start),
            "request_realtime_end": _date_text(request_realtime_end),
            "request_vintage_dates": _vintage_dates_text(request_vintage_dates),
            "vintage_date": _parse_date(
                observation.get("realtime_start") or payload.get("realtime_start")
            ),
            "realtime_start": observation.get("realtime_start") or payload.get("realtime_start"),
            "realtime_end": observation.get("realtime_end") or payload.get("realtime_end"),
            "observation_start": payload.get("observation_start"),
            "observation_end": payload.get("observation_end"),
            "units": payload.get("units"),
            "output_type": payload.get("output_type"),
            "file_type": payload.get("file_type"),
            "order_by": payload.get("order_by"),
            "sort_order": payload.get("sort_order"),
            "count": payload.get("count"),
            "offset": payload.get("offset"),
            "limit": payload.get("limit"),
            "ref_date": _parse_date(observation.get("date")),
            "raw_value": observation.get("value"),
            "source": "fred",
            "source_dataset": source_dataset,
            "download_timestamp_utc": timestamp,
            "raw_path": str(raw_path),
            "sha256": sha256,
        }
        for observation in observations
        if isinstance(observation, dict)
    ]
    return _frame(rows)


def parse_fred_observations_file(
    raw_path: Path,
    *,
    series_id: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
    vintage_request_mode: str = FRED_LATEST_SNAPSHOT_REQUEST,
    request_observation_start: date | str | None = None,
    request_observation_end: date | str | None = None,
    request_realtime_start: date | str | None = None,
    request_realtime_end: date | str | None = None,
    request_vintage_dates: Sequence[date | str] | None = None,
) -> pl.DataFrame:
    return parse_fred_observations_bytes(
        raw_path.read_bytes(),
        series_id=series_id,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        vintage_request_mode=vintage_request_mode,
        request_observation_start=request_observation_start,
        request_observation_end=request_observation_end,
        request_realtime_start=request_realtime_start,
        request_realtime_end=request_realtime_end,
        request_vintage_dates=request_vintage_dates,
    )


def write_fred_observations_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=[
            "series_id",
            "ref_date",
            "vintage_date",
            "vintage_request_mode",
            "source_dataset",
        ],
        ref_date_col="ref_date",
        partition_cols=["series_id", "year"],
    )


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in FRED_OBSERVATIONS_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(FRED_OBSERVATIONS_BRONZE_COLUMNS)


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


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _date_text(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _vintage_dates_text(values: Sequence[date | str] | None) -> str | None:
    if not values:
        return None
    return ",".join(_date_text(value) or "" for value in values)
