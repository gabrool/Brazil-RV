from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.tesouro.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

TESOURO_BRONZE_BASE_COLUMNS = [
    "row_index",
    "resource_name",
    "raw_fields_json",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_tesouro_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    resource_name: str | None = None,
) -> pl.DataFrame:
    rows = _rows_for_format(content, raw_format=raw_format)
    timestamp = _naive_utc(download_timestamp_utc)
    parsed = []
    for index, fields in enumerate(rows):
        normalized = _normalized_raw_columns(fields)
        parsed.append(
            {
                "row_index": index,
                "resource_name": resource_name or raw_path.stem,
                "raw_fields_json": json.dumps(fields, sort_keys=True, separators=(",", ":")),
                "source": "tesouro",
                "source_dataset": source_dataset,
                "download_timestamp_utc": timestamp,
                "raw_path": str(raw_path),
                "sha256": sha256,
                **normalized,
            }
        )
    return _frame(parsed)


def parse_tesouro_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
    resource_name: str | None = None,
) -> pl.DataFrame:
    return parse_tesouro_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        resource_name=resource_name,
    )


def write_tesouro_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["row_index", "resource_name", "source_dataset", "raw_path"],
    )


def _rows_for_format(content: bytes, *, raw_format: str) -> list[dict[str, Any]]:
    normalized = raw_format.lower()
    if normalized in {"csv", "csv_multi_resource"}:
        return _delimited_rows(content)
    raise ValueError(f"Unsupported Tesouro raw format: {raw_format}")


def _delimited_rows(content: bytes) -> list[dict[str, Any]]:
    text = _decode_text(content)
    delimiter = _detect_delimiter(text[:4096])
    reader = csv.DictReader(StringIO(text), delimiter=delimiter)
    return [dict(row) for row in reader]


def _normalized_raw_columns(fields: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in fields.items():
        column = f"raw_{normalize_column_name(key)}"
        if column in TESOURO_BRONZE_BASE_COLUMNS:
            column = f"raw_field_{column}"
        output[column] = value
    return output


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in TESOURO_BRONZE_BASE_COLUMNS})
    columns = TESOURO_BRONZE_BASE_COLUMNS + sorted(
        {column for row in rows for column in row if column not in TESOURO_BRONZE_BASE_COLUMNS}
    )
    return pl.DataFrame(rows).select(columns)


def _detect_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ";"


def _decode_text(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
