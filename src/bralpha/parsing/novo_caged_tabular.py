from __future__ import annotations

import csv
import io
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import py7zr

from bralpha.ingestion.novo_caged.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

NOVO_CAGED_BRONZE_LINEAGE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "resource_name",
    "record_kind",
    "period",
    "year",
    "month",
    "inner_filename",
    "row_index",
]

NOVO_CAGED_CALENDAR_BRONZE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "row_index",
    "raw_text",
]


def parse_novo_caged_tabular_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    period: str | None = None,
    year: int | None = None,
    month: int | None = None,
    record_kind: str | None = None,
) -> pl.DataFrame:
    if raw_format == "html":
        return _parse_calendar_html(
            content,
            source_dataset=source_dataset,
            download_timestamp_utc=download_timestamp_utc,
            raw_path=raw_path,
            sha256=sha256,
        )
    members = _text_members(content, raw_format=raw_format)
    frames = [
        _augment_movement_frame(
            _read_delimited_strings(member_content),
            source_dataset=source_dataset,
            resource_name=resource_name,
            download_timestamp_utc=download_timestamp_utc,
            raw_path=raw_path,
            sha256=sha256,
            inner_filename=inner_filename,
            period=period,
            year=year,
            month=month,
            record_kind=record_kind,
        )
        for inner_filename, member_content in members
    ]
    if not frames:
        return _empty_movement_bronze_frame()
    return pl.concat(frames, how="diagonal_relaxed")


def parse_novo_caged_tabular_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    download_timestamp_utc: datetime,
    sha256: str,
    period: str | None = None,
    year: int | None = None,
    month: int | None = None,
    record_kind: str | None = None,
) -> pl.DataFrame:
    return parse_novo_caged_tabular_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        resource_name=resource_name,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
        period=period,
        year=year,
        month=month,
        record_kind=record_kind,
    )


def write_novo_caged_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    has_year_partition = "year" in frame.columns and frame["year"].null_count() < frame.height
    partition_cols = ["year"] if has_year_partition else []
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["source_dataset", "raw_path", "inner_filename", "row_index"],
        ref_date_col="year",
        partition_cols=partition_cols,
    )


def _parse_calendar_html(
    content: bytes,
    *,
    source_dataset: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    text = _decode_text(content)
    return pl.DataFrame(
        [
            {
                "source": "novo_caged",
                "source_dataset": source_dataset,
                "download_timestamp_utc": timestamp,
                "raw_path": str(raw_path),
                "sha256": sha256,
                "row_index": 0,
                "raw_text": text,
            }
        ]
    ).select(NOVO_CAGED_CALENDAR_BRONZE_COLUMNS)


def _text_members(content: bytes, *, raw_format: str) -> list[tuple[str | None, bytes]]:
    if raw_format == "txt":
        return [(None, content)]
    if raw_format == "7z_txt":
        if content.startswith(b"7z\xbc\xaf\x27\x1c"):
            return _seven_zip_members(content)
        return [(None, content)]
    raise ValueError(f"Unsupported Novo CAGED raw format: {raw_format}")


def _seven_zip_members(content: bytes) -> list[tuple[str | None, bytes]]:
    members: list[tuple[str | None, bytes]] = []
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        with py7zr.SevenZipFile(io.BytesIO(content), mode="r") as archive:
            names = sorted(archive.getnames())
            archive.extractall(path=temp_path)
        for name in names:
            candidate = temp_path / name
            if candidate.is_dir():
                continue
            if candidate.suffix.lower() not in {".txt", ".csv"}:
                continue
            members.append((name.replace("\\", "/"), candidate.read_bytes()))
    if not members:
        raise ValueError("Novo CAGED 7z payload did not contain TXT/CSV members")
    return members


def _read_delimited_strings(content: bytes) -> pl.DataFrame:
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


def _augment_movement_frame(
    frame: pl.DataFrame,
    *,
    source_dataset: str,
    resource_name: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    inner_filename: str | None,
    period: str | None,
    year: int | None,
    month: int | None,
    record_kind: str | None,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    frame = frame.with_columns(pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_index"))
    frame = frame.with_columns(
        [
            pl.lit("novo_caged").alias("source"),
            pl.lit(source_dataset).alias("source_dataset"),
            pl.lit(timestamp).alias("download_timestamp_utc"),
            pl.lit(str(raw_path)).alias("raw_path"),
            pl.lit(sha256).alias("sha256"),
            pl.lit(resource_name).alias("resource_name"),
            pl.lit(record_kind).alias("record_kind"),
            _period_literal_or_raw(frame, period).alias("period"),
            _period_year_or_literal(frame, year).alias("year"),
            _period_month_or_literal(frame, month).alias("month"),
            pl.lit(inner_filename).alias("inner_filename"),
        ]
    )
    ordered = [column for column in NOVO_CAGED_BRONZE_LINEAGE_COLUMNS if column in frame.columns]
    raw_columns = [
        column
        for column in frame.columns
        if column.startswith("raw_") and column not in NOVO_CAGED_BRONZE_LINEAGE_COLUMNS
    ]
    return frame.select([*ordered, *raw_columns])


def _period_literal_or_raw(frame: pl.DataFrame, period: str | None) -> pl.Expr:
    if period is not None:
        return pl.lit(period)
    for raw_column in ("raw_competenciamov", "raw_competencia"):
        if raw_column in frame.columns:
            return pl.col(raw_column).map_elements(_period_text, return_dtype=pl.Utf8)
    return pl.lit(None, dtype=pl.Utf8)


def _period_year_or_literal(frame: pl.DataFrame, year: int | None) -> pl.Expr:
    if year is not None:
        return pl.lit(year, dtype=pl.Int64)
    return _period_literal_or_raw(frame, None).map_elements(_period_year, return_dtype=pl.Int64)


def _period_month_or_literal(frame: pl.DataFrame, month: int | None) -> pl.Expr:
    if month is not None:
        return pl.lit(month, dtype=pl.Int64)
    return _period_literal_or_raw(frame, None).map_elements(_period_month, return_dtype=pl.Int64)


def _period_text(value: object) -> str | None:
    if value is None:
        return None
    text = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(text) >= 6:
        return text[:6]
    return None


def _period_year(value: object) -> int | None:
    period = _period_text(value)
    return int(period[:4]) if period else None


def _period_month(value: object) -> int | None:
    period = _period_text(value)
    return int(period[4:6]) if period else None


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
        return csv.Sniffer().sniff(sample, delimiters=";\t,|").delimiter
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


def _empty_movement_bronze_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in NOVO_CAGED_BRONZE_LINEAGE_COLUMNS})
