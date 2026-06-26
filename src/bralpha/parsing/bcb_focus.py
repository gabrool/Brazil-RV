from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bralpha.ingestion.bcb.common import write_bronze_frame

BCB_FOCUS_BRONZE_COLUMNS = [
    "endpoint",
    "row_index",
    "Indicador",
    "indicador",
    "IndicadorDetalhe",
    "Data",
    "DataReferencia",
    "Reuniao",
    "reuniao",
    "Suavizada",
    "Media",
    "media",
    "Mediana",
    "mediana",
    "DesvioPadrao",
    "desvioPadrao",
    "coeficienteVariacao",
    "Minimo",
    "minimo",
    "Maximo",
    "maximo",
    "numeroRespondentes",
    "baseCalculo",
    "tipoCalculo",
    "periodo",
    "DataReferencia1",
    "DataReferencia2",
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
]


def parse_focus_bytes(
    content: bytes,
    *,
    endpoint: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    rows = _odata_rows(content)
    timestamp = _naive_utc(download_timestamp_utc)
    parsed = []
    for row_index, row in enumerate(rows):
        parsed.append(
            {
                **{
                    column: row.get(column)
                    for column in BCB_FOCUS_BRONZE_COLUMNS
                    if column
                    not in {
                        "endpoint",
                        "row_index",
                        "source",
                        "source_dataset",
                        "download_timestamp_utc",
                        "raw_path",
                        "sha256",
                    }
                },
                "endpoint": endpoint,
                "row_index": row_index,
                "source": "bcb",
                "source_dataset": source_dataset,
                "download_timestamp_utc": timestamp,
                "raw_path": str(raw_path),
                "sha256": sha256,
            }
        )
    return _frame(parsed)


def parse_focus_file(
    raw_path: Path,
    *,
    endpoint: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_focus_bytes(
        raw_path.read_bytes(),
        endpoint=endpoint,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_focus_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    if frame.is_empty():
        return []
    primary_keys = ["endpoint", "raw_path", "sha256", "row_index"]
    if "Data" not in frame.columns:
        return write_bronze_frame(
            frame,
            output_root,
            primary_keys=primary_keys,
            partition_cols=["endpoint"],
        )

    paths: list[Path] = []
    dated = frame.filter(pl.col("Data").is_not_null())
    if not dated.is_empty():
        paths.extend(
            write_bronze_frame(
                dated,
                output_root,
                primary_keys=primary_keys,
                ref_date_col="Data",
                partition_cols=["endpoint", "year"],
            )
        )
    undated = frame.filter(pl.col("Data").is_null())
    if not undated.is_empty():
        paths.extend(
            write_bronze_frame(
                undated,
                output_root,
                primary_keys=primary_keys,
                partition_cols=["endpoint"],
            )
        )
    return paths


def _odata_rows(content: bytes) -> list[dict[str, object]]:
    payload = json.loads(content.decode("utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise ValueError("Focus OData JSON payload must contain a value list")
    return [row for row in payload["value"] if isinstance(row, dict)]


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_FOCUS_BRONZE_COLUMNS})
    return pl.DataFrame(rows).select(BCB_FOCUS_BRONZE_COLUMNS)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
