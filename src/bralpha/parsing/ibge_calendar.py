from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.ibge.common import write_bronze_frame

IBGE_CALENDAR_BRONZE_COLUMNS = [
    "id",
    "titulo",
    "descricao",
    "data_divulgacao",
    "tipo_id",
    "tipo",
    "produto_id",
    "nome_produto",
    "alias_produto",
    "descricao_produto",
    "ano_referencia_inicio",
    "mes_referencia_inicio",
    "ano_referencia_fim",
    "mes_referencia_fim",
    "link",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_calendar_bytes(
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
            **{column: item.get(column) for column in IBGE_CALENDAR_BRONZE_COLUMNS[:15]},
            "source": "ibge",
            "source_dataset": source_dataset,
            "download_timestamp_utc": timestamp,
            "raw_path": str(raw_path),
            "sha256": sha256,
        }
        for item in _items(content)
    ]
    return _frame(rows)


def parse_calendar_file(
    raw_path: Path,
    *,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_calendar_bytes(
        raw_path.read_bytes(),
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_calendar_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_bronze_frame(
        frame,
        output_root,
        primary_keys=["id"],
        ref_date_col="data_divulgacao",
        partition_cols=["year"],
    )


def _items(content: bytes) -> list[dict[str, Any]]:
    payload = json.loads(content.decode("utf-8-sig"))
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return [row for row in payload["items"] if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    raise ValueError("IBGE calendar JSON payload must contain an items list")


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in IBGE_CALENDAR_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(IBGE_CALENDAR_BRONZE_COLUMNS)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
