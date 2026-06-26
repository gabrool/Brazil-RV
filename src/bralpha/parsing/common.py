from __future__ import annotations

import csv
import html.parser
import io
import re
import unicodedata
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Literal

import polars as pl


def normalize_column_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(name))
    ascii_name = "".join(char for char in normalized if not unicodedata.combining(char))
    ascii_name = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_name).strip("_").lower()
    return ascii_name


def normalize_columns(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.rename({column: normalize_column_name(column) for column in frame.columns})


HeaderRequirement = str | Sequence[str]


def read_delimited_or_html(
    content: bytes,
    *,
    required_any: Sequence[str] | None = None,
    required_all: Sequence[HeaderRequirement] | None = None,
) -> pl.DataFrame:
    text = _decode_text(content)
    if "<table" in text.lower():
        tables = _extract_html_tables(text)
        if not tables:
            raise ValueError("No HTML table found in content")
        rows = select_table_by_headers(
            tables,
            required_any=required_any,
            required_all=required_all,
        )
        if len(rows) < 2:
            raise ValueError("HTML table must contain a header and at least one row")
        return normalize_columns(pl.DataFrame(rows[1:], schema=rows[0], orient="row"))

    sample = text[:4096]
    delimiter = _detect_delimiter(sample)
    return normalize_columns(
        pl.read_csv(
            io.BytesIO(content),
            separator=delimiter,
            infer_schema_length=0,
            ignore_errors=False,
            null_values=["", "NA", "N/A", "null", "None"],
        )
    )


def parse_decimal(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "--"}:
        return None
    text = text.replace("\xa0", "").replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    text = re.sub(r"[^0-9.+-]", "", text)
    if not text or text in {".", "+", "-"}:
        return None
    return float(text)


def parse_int(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "--"}:
        return None
    text = re.sub(r"[^0-9+-]", "", text)
    if not text or text in {"+", "-"}:
        return None
    return int(text)


def select_first_existing(frame: pl.DataFrame, aliases: Iterable[str]) -> str | None:
    columns = set(frame.columns)
    for alias in aliases:
        normalized = normalize_column_name(alias)
        if normalized in columns:
            return normalized
    return None


def select_table_by_headers(
    tables: Sequence[Sequence[Sequence[str]]],
    *,
    required_any: Sequence[str] | None = None,
    required_all: Sequence[HeaderRequirement] | None = None,
) -> list[list[str]]:
    for table in tables:
        if not table:
            continue
        headers = [normalize_column_name(header) for header in table[0]]
        if required_any and not any(_has_header(headers, item) for item in required_any):
            continue
        if required_all and not all(_requirement_matches(headers, item) for item in required_all):
            continue
        return [list(row) for row in table]
    raise ValueError("No HTML table matched required headers")


def write_partitioned_by_year(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    ref_date_col: str = "ref_date",
    primary_keys: list[str] | None = None,
    filename: str = "data.parquet",
) -> list[Path]:
    if frame.is_empty():
        return []
    if ref_date_col not in frame.columns:
        raise ValueError(f"Missing partition date column: {ref_date_col}")

    output_root.mkdir(parents=True, exist_ok=True)
    frame = frame.with_columns(pl.col(ref_date_col).dt.year().alias("__year"))
    paths: list[Path] = []
    for year in frame.select("__year").unique().to_series().to_list():
        part_dir = output_root / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / filename
        part = frame.filter(pl.col("__year") == year).drop("__year")
        if path.exists():
            existing = pl.read_parquet(path)
            part = pl.concat([existing, part], how="diagonal_relaxed")
            if primary_keys:
                part = part.unique(subset=primary_keys, keep="last", maintain_order=True)
        part.write_parquet(path)
        paths.append(path)
    return paths


def write_source_partitioned(
    frame: pl.DataFrame,
    output_root: Path,
    *,
    ref_date_col: str = "ref_date",
    primary_keys: list[str] | None = None,
    mode: Literal["upsert", "append_chunks"] = "upsert",
    filename: str = "data.parquet",
) -> list[Path]:
    if frame.is_empty():
        return []
    if ref_date_col not in frame.columns:
        raise ValueError(f"Missing partition date column: {ref_date_col}")

    output_root.mkdir(parents=True, exist_ok=True)
    frame = frame.with_columns(pl.col(ref_date_col).dt.year().alias("__year"))
    paths: list[Path] = []
    for year in frame.select("__year").unique().to_series().to_list():
        part_dir = output_root / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part = frame.filter(pl.col("__year") == year).drop("__year")
        if mode == "append_chunks":
            path = part_dir / _next_chunk_filename(part_dir)
        elif mode == "upsert":
            path = part_dir / filename
            if path.exists():
                part = pl.concat([pl.read_parquet(path), part], how="diagonal_relaxed")
                if primary_keys:
                    keys = list(primary_keys)
                    if "source_dataset" in part.columns and "source_dataset" not in keys:
                        keys.append("source_dataset")
                    part = part.unique(subset=keys, keep="last", maintain_order=True)
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
        part.write_parquet(path)
        paths.append(path)
    return paths


class _TableParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            text = " ".join("".join(self._current_cell).split())
            self._current_row.append(text)
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None and self._current_table is not None:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            if self._current_table:
                self.tables.append(self._current_table)
            self._current_table = None


def _extract_html_tables(text: str) -> list[list[list[str]]]:
    parser = _TableParser()
    parser.feed(text)
    return parser.tables


def _requirement_matches(headers: Sequence[str], requirement: HeaderRequirement) -> bool:
    if isinstance(requirement, str):
        return _has_header(headers, requirement)
    return any(_has_header(headers, item) for item in requirement)


def _has_header(headers: Sequence[str], expected: str) -> bool:
    normalized = normalize_column_name(expected)
    return any(
        header == normalized
        or header.startswith(f"{normalized}_")
        or normalized.startswith(f"{header}_")
        for header in headers
    )


def _next_chunk_filename(part_dir: Path) -> str:
    existing = sorted(part_dir.glob("chunk-*.parquet"))
    return f"chunk-{len(existing):06d}.parquet"


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
