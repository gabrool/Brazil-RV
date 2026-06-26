from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from bralpha.ingestion.bcb.common import write_bronze_frame

BCB_SGS_BRONZE_COLUMNS = [
    "series_id",
    "ref_date",
    "data",
    "valor",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_sgs_bytes(
    content: bytes,
    *,
    series_id: int,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    payload = json.loads(content.decode("utf-8-sig"))
    if not isinstance(payload, list):
        raise ValueError("SGS JSON payload must be a list of rows")
    timestamp = _naive_utc(download_timestamp_utc)
    rows = [
        {
            "series_id": series_id,
            "ref_date": _parse_bcb_date(row.get("data")),
            "data": row.get("data"),
            "valor": row.get("valor"),
            "source": "bcb",
            "source_dataset": source_dataset,
            "download_timestamp_utc": timestamp,
            "raw_path": str(raw_path),
            "sha256": sha256,
        }
        for row in payload
        if isinstance(row, dict)
    ]
    return _frame(rows)


def parse_sgs_file(
    raw_path: Path,
    *,
    series_id: int,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_sgs_bytes(
        raw_path.read_bytes(),
        series_id=series_id,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_sgs_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_bronze_frame(
        frame,
        output_root,
        primary_keys=["series_id", "ref_date", "source_dataset"],
        ref_date_col="ref_date",
        partition_cols=["series_id", "year"],
    )


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_SGS_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(BCB_SGS_BRONZE_COLUMNS)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _parse_bcb_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        day, month, year = text.split("/")
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])
