from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bralpha.ingestion.receita.common import write_partitioned_frame
from bralpha.parsing.common import normalize_column_name

RECEITA_BRONZE_LINEAGE_COLUMNS = [
    "source",
    "source_dataset",
    "download_timestamp_utc",
    "raw_path",
    "sha256",
    "resource_name",
    "resource_family",
    "sheet_name",
    "inner_filename",
    "row_index",
    "year",
]


class ReceitaUnsupportedFormatError(ValueError):
    pass


def parse_receita_tabular_bytes(
    content: bytes,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    resource_family: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
) -> pl.DataFrame:
    members = _tabular_members(content, raw_format=raw_format)
    frames = [
        _augment_frame(
            _read_delimited_strings(member_content),
            source_dataset=source_dataset,
            resource_name=resource_name,
            resource_family=resource_family,
            download_timestamp_utc=download_timestamp_utc,
            raw_path=raw_path,
            sha256=sha256,
            sheet_name=None,
            inner_filename=inner_filename,
        )
        for inner_filename, member_content in members
    ]
    if not frames:
        return _empty_bronze_frame()
    return pl.concat(frames, how="diagonal_relaxed")


def parse_receita_tabular_file(
    raw_path: Path,
    *,
    raw_format: str,
    source_dataset: str,
    resource_name: str,
    resource_family: str,
    download_timestamp_utc: datetime,
    sha256: str,
) -> pl.DataFrame:
    return parse_receita_tabular_bytes(
        raw_path.read_bytes(),
        raw_format=raw_format,
        source_dataset=source_dataset,
        resource_name=resource_name,
        resource_family=resource_family,
        download_timestamp_utc=download_timestamp_utc,
        raw_path=raw_path,
        sha256=sha256,
    )


def write_receita_bronze(frame: pl.DataFrame, output_root: Path) -> list[Path]:
    has_year_partition = "year" in frame.columns and frame["year"].null_count() < frame.height
    return write_partitioned_frame(
        frame,
        output_root,
        primary_keys=["source_dataset", "raw_path", "inner_filename", "sheet_name", "row_index"],
        ref_date_col="year",
        partition_cols=["year"] if has_year_partition else [],
    )


def _tabular_members(content: bytes, *, raw_format: str) -> list[tuple[str | None, bytes]]:
    extension = _raw_format_extension(raw_format)
    if extension in {"csv", "txt", "official_structured_tabular"}:
        return [(None, content)]
    if extension in {"zip", "zip_csv"}:
        return _zip_members(content)
    if extension in {"xlsx", "xls", "ods"}:
        raise ReceitaUnsupportedFormatError(
            "Receita XLSX/ODS parsing requires a fixture-verified optional dependency"
        )
    if extension == "pdf":
        raise ReceitaUnsupportedFormatError("Receita PDF parsing is out of scope")
    raise ReceitaUnsupportedFormatError(f"Unsupported Receita raw format: {raw_format}")


def _raw_format_extension(raw_format: str) -> str:
    text = raw_format.lower().strip().lstrip(".")
    supported = {
        "official_structured_tabular",
        "csv",
        "txt",
        "zip",
        "zip_csv",
        "xlsx",
        "xls",
        "ods",
        "pdf",
    }
    if text in supported:
        return text
    return text.rsplit(".", 1)[-1]


def _zip_members(content: bytes) -> list[tuple[str | None, bytes]]:
    members: list[tuple[str | None, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for name in sorted(archive.namelist()):
            if name.endswith("/"):
                continue
            suffix = Path(name).suffix.lower()
            if suffix not in {".csv", ".txt"}:
                continue
            members.append((name.replace("\\", "/"), archive.read(name)))
    if not members:
        raise ValueError("Receita ZIP payload did not contain CSV/TXT members")
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


def _augment_frame(
    frame: pl.DataFrame,
    *,
    source_dataset: str,
    resource_name: str,
    resource_family: str,
    download_timestamp_utc: datetime,
    raw_path: Path,
    sha256: str,
    sheet_name: str | None,
    inner_filename: str | None,
) -> pl.DataFrame:
    timestamp = _naive_utc(download_timestamp_utc)
    frame = frame.with_columns(pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_index"))
    frame = frame.with_columns(
        [
            pl.lit("receita").alias("source"),
            pl.lit(source_dataset).alias("source_dataset"),
            pl.lit(timestamp).alias("download_timestamp_utc"),
            pl.lit(str(raw_path)).alias("raw_path"),
            pl.lit(sha256).alias("sha256"),
            pl.lit(resource_name).alias("resource_name"),
            pl.lit(resource_family).alias("resource_family"),
            pl.lit(sheet_name).alias("sheet_name"),
            pl.lit(inner_filename).alias("inner_filename"),
            _year_expr(frame).alias("year"),
        ]
    )
    ordered = [column for column in RECEITA_BRONZE_LINEAGE_COLUMNS if column in frame.columns]
    raw_columns = [
        column
        for column in frame.columns
        if column.startswith("raw_") and column not in RECEITA_BRONZE_LINEAGE_COLUMNS
    ]
    return frame.select([*ordered, *raw_columns])


def _year_expr(frame: pl.DataFrame) -> pl.Expr:
    for column in ("raw_ano", "raw_year"):
        if column in frame.columns:
            return pl.col(column).map_elements(_year_from_text, return_dtype=pl.Int64)
    for column in ("raw_competencia", "raw_periodo", "raw_mes_de_arrecadacao"):
        if column in frame.columns:
            return pl.col(column).map_elements(_year_from_text, return_dtype=pl.Int64)
    return pl.lit(None, dtype=pl.Int64)


def _year_from_text(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 4:
        return int(digits[:4])
    return None


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


def _empty_bronze_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in RECEITA_BRONZE_LINEAGE_COLUMNS})
