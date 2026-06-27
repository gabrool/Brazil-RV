from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bralpha.ingestion.cvm.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

CVM_BRONZE_LINEAGE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "inner_filename",
    "row_index",
]

CVM_FUND_DAILY_BRONZE_COLUMNS = [
    *CVM_BRONZE_LINEAGE_COLUMNS,
    "ref_date",
    "fund_id",
]


def parse_cvm_fund_daily_report_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    return _parse_cvm_tabular_bytes(
        content,
        raw_format=raw_format,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        daily_report=True,
    )


def parse_cvm_fund_daily_report_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_cvm_fund_daily_report_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def parse_cvm_registry_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    return _parse_cvm_tabular_bytes(
        content,
        raw_format=raw_format,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        daily_report=False,
    )


def parse_cvm_registry_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_cvm_registry_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_cvm_fund_daily_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    if "ref_date" not in frame.columns:
        return write_partitioned_frame(
            frame,
            output_root,
            primary_keys=["source_dataset", "raw_path", "inner_filename", "row_index"],
            ref_date_col="row_index",
            partition_cols=[],
        )
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["source_dataset", "raw_path", "inner_filename", "row_index"],
        ref_date_col="ref_date",
        partition_cols=["year", "month"],
    )


def write_cvm_registry_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["source_dataset", "raw_path", "inner_filename", "row_index"],
        ref_date_col="row_index",
        partition_cols=[],
    )


def _parse_cvm_tabular_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    daily_report: bool,
) -> pl.DataFrame:
    members = _csv_members(content, raw_format=raw_format)
    frames = [
        _augment_source_frame(
            _read_csv_strings(member_content),
            source_dataset=source_dataset,
            download_timestamp_utc=download_timestamp_utc,
            raw_path=raw_path,
            sha256=sha256,
            inner_filename=inner_filename,
            daily_report=daily_report,
        )
        for inner_filename, member_content in members
    ]
    if not frames:
        return _empty_bronze_frame(daily_report=daily_report)
    return pl.concat(frames, how="diagonal_relaxed")


def _csv_members(content: bytes, *, raw_format: str) -> list[tuple[str | None, bytes]]:
    if raw_format == "csv":
        return [(None, content)]
    if raw_format != "zip_csv":
        raise ValueError(f"Unsupported CVM raw format: {raw_format}")
    members: list[tuple[str | None, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for info in sorted(archive.infolist(), key=lambda item: item.filename):
            if info.is_dir():
                continue
            lower_name = info.filename.lower()
            if not (lower_name.endswith(".csv") or lower_name.endswith(".txt")):
                continue
            members.append((info.filename, archive.read(info)))
    if not members:
        raise ValueError("CVM ZIP payload did not contain CSV members")
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
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    inner_filename: str | None,
    daily_report: bool,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    frame = frame.with_columns(pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_index"))
    frame = frame.with_columns(
        [
            pl.lit("cvm").alias("source"),
            pl.lit(source_dataset).alias("source_dataset"),
            pl.lit(timestamp).alias("download_timestamp_utc"),
            pl.lit(str(raw_path)).alias("raw_path"),
            pl.lit(sha256).alias("sha256"),
            pl.lit(inner_filename).alias("inner_filename"),
        ]
    )
    if daily_report:
        frame = _with_optional_from_raw(frame, "ref_date", "raw_dt_comptc", _parse_date)
        frame = _with_optional_from_raw(frame, "fund_id", "raw_cnpj_fundo", _clean_text)
        ordered = [
            column
            for column in CVM_FUND_DAILY_BRONZE_COLUMNS
            if column in frame.columns
        ]
    else:
        ordered = [column for column in CVM_BRONZE_LINEAGE_COLUMNS if column in frame.columns]
    raw_columns = [
        column
        for column in frame.columns
        if column.startswith("raw_")
        and column not in CVM_BRONZE_LINEAGE_COLUMNS
        and column not in ordered
    ]
    return frame.select([*ordered, *raw_columns])


def _with_optional_from_raw(
    frame: pl.DataFrame,
    target: str,
    source: str,
    parser,
) -> pl.DataFrame:
    if source not in frame.columns:
        return frame.with_columns(pl.lit(None).alias(target))
    return frame.with_columns(
        pl.col(source).map_elements(parser, return_dtype=_return_dtype(target)).alias(target)
    )


def _return_dtype(target: str) -> pl.DataType:
    if target == "ref_date":
        return pl.Date
    return pl.Utf8


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
    if "/" in text:
        day, month, year = text[:10].split("/")
        return date(int(year), int(month), int(day))
    return date.fromisoformat(text[:10])


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _empty_bronze_frame(*, daily_report: bool) -> pl.DataFrame:
    columns = CVM_FUND_DAILY_BRONZE_COLUMNS if daily_report else CVM_BRONZE_LINEAGE_COLUMNS
    return pl.DataFrame(schema={column: pl.Null for column in columns})
