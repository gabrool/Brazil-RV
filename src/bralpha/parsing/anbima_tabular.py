from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.anbima.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

ANBIMA_BRONZE_BASE_COLUMNS = [
    "row_index",
    "raw_fields_json",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_anbima_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    rows = _rows_for_format(content, raw_format=raw_format)
    timestamp = _naive_utc(download_timestamp_utc)
    parsed = []
    for index, fields in enumerate(rows):
        normalized = _normalized_raw_columns(fields)
        parsed.append(
            {
                "row_index": index,
                "raw_fields_json": json.dumps(fields, sort_keys=True, separators=(",", ":")),
                "source": "anbima",
                "source_dataset": source_dataset,
                "download_timestamp_utc": timestamp,
                "raw_path": str(raw_path),
                "sha256": sha256,
                **normalized,
            }
        )
    return _frame(parsed)


def parse_anbima_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_anbima_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_anbima_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["row_index", "source_dataset", "raw_path"],
    )


def _rows_for_format(content: bytes, *, raw_format: str) -> list[dict[str, Any]]:
    normalized = raw_format.lower()
    if normalized == "json":
        return _json_rows(content)
    if normalized == "csv":
        return _delimited_rows(content, delimiter=",")
    if normalized in {"txt", "txt_semicolon"}:
        return _delimited_rows(content, delimiter=";")
    raise ValueError(f"Unsupported ANBIMA raw format: {raw_format}")


def _json_rows(content: bytes) -> list[dict[str, Any]]:
    payload = json.loads(content.decode("utf-8-sig"))
    if isinstance(payload, list):
        values = payload
    elif isinstance(payload, dict):
        values = _first_list(payload)
        if values is None:
            values = [payload]
    else:
        raise ValueError("ANBIMA JSON payload must be a list or object")
    return [_dict_row(value) for value in values]


def _first_list(payload: dict[str, Any]) -> list[Any] | None:
    for key in ("items", "value", "data", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return None


def _delimited_rows(content: bytes, *, delimiter: str) -> list[dict[str, Any]]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(StringIO(text), delimiter=delimiter)
    return [dict(row) for row in reader]


def _dict_row(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {"value": value}


def _normalized_raw_columns(fields: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in fields.items():
        column = f"raw_{normalize_column_name(key)}"
        if column in ANBIMA_BRONZE_BASE_COLUMNS:
            column = f"raw_field_{column}"
        output[column] = value
    return output


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in ANBIMA_BRONZE_BASE_COLUMNS})
    columns = ANBIMA_BRONZE_BASE_COLUMNS + sorted(
        {column for row in rows for column in row if column not in ANBIMA_BRONZE_BASE_COLUMNS}
    )
    return pl.DataFrame(rows).select(columns)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
