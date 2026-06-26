from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.ibge.common import write_bronze_frame

IBGE_PRODUCTS_BRONZE_COLUMNS = [
    "id",
    "tipo",
    "titulo",
    "alias",
    "sigla",
    "catId",
    "catTitle",
    "parentCatId",
    "parentCatTitle",
    "path",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_products_bytes(
    content: bytes,
    *,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    rows = [
        {
            **{column: item.get(column) for column in IBGE_PRODUCTS_BRONZE_COLUMNS[:10]},
            "source": "ibge",
            "source_dataset": source_dataset,
            "download_timestamp_utc": timestamp,
            "raw_path": str(raw_path),
            "sha256": sha256,
        }
        for item in _items(content)
    ]
    return _frame(rows)


def parse_products_file(
    raw_path: Path,
    *,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_products_bytes(
        raw_path.read_bytes(),
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_products_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_bronze_frame(frame, output_root, primary_keys=["id"])


def _items(content: bytes) -> list[dict[str, Any]]:
    payload = json.loads(content.decode("utf-8-sig"))
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("value"), list):
        return [row for row in payload["value"] if isinstance(row, dict)]
    raise ValueError("IBGE products JSON payload must be a list")


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_PRODUCTS_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_PRODUCTS_BRONZE_COLUMNS)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
