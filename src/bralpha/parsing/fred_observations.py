from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.fred.common import write_partitioned_frame

FRED_OBSERVATIONS_BRONZE_COLUMNS = [
    "series_id",
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
) -> pl.DataFrame:
    return parse_fred_observations_bytes(
        raw_path.read_bytes(),
        series_id=series_id,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_fred_observations_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["series_id", "ref_date", "source_dataset"],
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
