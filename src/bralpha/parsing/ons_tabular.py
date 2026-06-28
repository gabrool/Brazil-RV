from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bralpha.ingestion.ons.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

ONS_BRONZE_LINEAGE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "resource_name",
    "year",
    "row_index",
]


def parse_ons_tabular_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    year: int,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    if raw_format != "csv_annual":
        raise ValueError(f"Unsupported ONS raw format: {raw_format}")
    return _augment_source_frame(
        _read_csv_strings(content),
        source_dataset=source_dataset,
        resource_name=resource_name,
        year=year,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def parse_ons_tabular_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    year: int,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_ons_tabular_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        resource_name=resource_name,
        year=year,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_ons_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["source_dataset", "raw_path", "resource_name", "year", "row_index"],
        ref_date_col="year",
        partition_cols=["year"],
    )


def _read_csv_strings(content: bytes) -> pl.DataFrame:
    text = _decode_text(content)
    delimiter = _detect_delimiter(text[:4096])
    frame = pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=delimiter,
        infer_schema_length=0,
        ignore_errors=False,
        null_values=[],
    )
    if frame.is_empty() and not frame.columns:
        return frame
    names = _unique_raw_column_names(frame.columns)
    return frame.rename(dict(zip(frame.columns, names, strict=False))).with_columns(
        [pl.col(column).cast(pl.Utf8, strict=False) for column in names]
    )


def _augment_source_frame(
    frame: pl.DataFrame,
    *,
    source_dataset: str,
    resource_name: str,
    year: int,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    frame = frame.with_columns(pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_index"))
    frame = frame.with_columns(
        [
            pl.lit("ons").alias("source"),
            pl.lit(source_dataset).alias("source_dataset"),
            pl.lit(timestamp).alias("download_timestamp_utc"),
            pl.lit(str(raw_path)).alias("raw_path"),
            pl.lit(sha256).alias("sha256"),
            pl.lit(resource_name).alias("resource_name"),
            pl.lit(year).alias("year"),
        ]
    )
    raw_columns = [
        column
        for column in frame.columns
        if column.startswith("raw_") and column not in ONS_BRONZE_LINEAGE_COLUMNS
    ]
    return frame.select([*ONS_BRONZE_LINEAGE_COLUMNS, *raw_columns])


def _unique_raw_column_names(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    names: list[str] = []
    for column in columns:
        base = f"raw_{normalize_column_name(column)}"
        count = seen.get(base, 0) + 1
        seen[base] = count
        names.append(base if count == 1 else f"{base}_{count}")
    return names


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
