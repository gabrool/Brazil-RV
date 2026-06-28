from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.anp.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

ANP_BRONZE_LINEAGE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "resource_name",
    "resource_family",
    "year",
    "month",
    "semester",
    "inner_filename",
    "row_index",
]


def parse_anp_tabular_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    resource_family: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    year: int | None = None,
    month: int | None = None,
    semester: int | None = None,
) -> pl.DataFrame:
    members = _csv_members(content, raw_format=raw_format)
    frames = [
        _augment_source_frame(
            _read_csv_strings(member_content),
            source_dataset=source_dataset,
            resource_name=resource_name,
            resource_family=resource_family,
            download_timestamp_utc=download_timestamp_utc,
            raw_path=raw_path,
            sha256=sha256,
            inner_filename=inner_filename,
            year=year,
            month=month,
            semester=semester,
        )
        for inner_filename, member_content in members
    ]
    if not frames:
        return _empty_bronze_frame()
    return pl.concat(frames, how="diagonal_relaxed")


def parse_anp_tabular_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    resource_family: str,
    download_timestamp_utc: datetime,
    sha256: str,
    year: int | None = None,
    month: int | None = None,
    semester: int | None = None,
) -> pl.DataFrame:
    return parse_anp_tabular_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        resource_name=resource_name,
        resource_family=resource_family,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        year=year,
        month=month,
        semester=semester,
    )


def write_anp_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["source_dataset", "raw_path", "inner_filename", "row_index"],
        ref_date_col="year",
        partition_cols=["year"] if "year" in frame.columns else [],
    )


def _csv_members(content: bytes, *, raw_format: str) -> list[tuple[str | None, bytes]]:
    if raw_format in {"csv", "csv_multi_resource"}:
        return [(None, content)]
    if raw_format == "mixed_csv_zip":
        if content[:4] == b"PK\x03\x04":
            return _zip_members(content)
        return [(None, content)]
    if raw_format == "zip_csv":
        return _zip_members(content)
    raise ValueError(f"Unsupported ANP raw format: {raw_format}")


def _zip_members(content: bytes) -> list[tuple[str | None, bytes]]:
    members: list[tuple[str | None, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for info in sorted(archive.infolist(), key=lambda item: item.filename):
            if info.is_dir():
                continue
            lower_name = info.filename.lower()
            if lower_name.endswith((".csv", ".txt")):
                members.append((info.filename, archive.read(info)))
    if not members:
        raise ValueError("ANP ZIP payload did not contain CSV members")
    return members


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
    resource_family: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    inner_filename: str | None,
    year: int | None,
    month: int | None,
    semester: int | None,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    frame = frame.with_columns(pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_index"))
    frame = frame.with_columns(
        [
            pl.lit("anp").alias("source"),
            pl.lit(source_dataset).alias("source_dataset"),
            pl.lit(timestamp).alias("download_timestamp_utc"),
            pl.lit(str(raw_path)).alias("raw_path"),
            pl.lit(sha256).alias("sha256"),
            pl.lit(resource_name).alias("resource_name"),
            pl.lit(resource_family).alias("resource_family"),
            _period_literal_or_raw(frame, year, "ano", return_dtype=pl.Int64).alias("year"),
            _period_literal_or_raw(frame, month, "mes", return_dtype=pl.Int64).alias("month"),
            pl.lit(semester, dtype=pl.Int64).alias("semester"),
            pl.lit(inner_filename).alias("inner_filename"),
        ]
    )
    ordered = [column for column in ANP_BRONZE_LINEAGE_COLUMNS if column in frame.columns]
    raw_columns = [
        column
        for column in frame.columns
        if column.startswith("raw_") and column not in ANP_BRONZE_LINEAGE_COLUMNS
    ]
    return frame.select([*ordered, *raw_columns])


def _period_literal_or_raw(
    frame: pl.DataFrame,
    literal: int | None,
    raw_alias: str,
    *,
    return_dtype: pl.DataType,
) -> pl.Expr:
    if literal is not None:
        return pl.lit(literal, dtype=return_dtype)
    raw_column = f"raw_{normalize_column_name(raw_alias)}"
    if raw_column in frame.columns:
        if raw_alias == "mes":
            return pl.col(raw_column).map_elements(_month_number, return_dtype=return_dtype)
        return pl.col(raw_column).map_elements(_int_or_none, return_dtype=return_dtype)
    return pl.lit(None, dtype=return_dtype)


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


def _month_number(value: Any) -> int | None:
    if value is None:
        return None
    text = normalize_column_name(str(value))
    if not text:
        return None
    if text.isdigit():
        return int(text)
    months = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }
    return months.get(text)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    return int(text) if text else None


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _empty_bronze_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ANP_BRONZE_LINEAGE_COLUMNS})
