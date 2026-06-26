from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bralpha.parsing.common import HeaderRequirement, read_delimited_or_html


def parse_tabular_reference_file(
    content: bytes,
    *,
    source_dataset: str,
    ref_date: date | None = None,
    download_timestamp_utc: datetime | None = None,
    raw_path: Path | str | None = None,
    sha256: str | None = None,
    required_any: Sequence[str] | None = None,
    required_all: Sequence[HeaderRequirement] | None = None,
) -> pl.DataFrame:
    frame = read_delimited_or_html(
        content,
        required_any=required_any,
        required_all=required_all,
    )
    expressions = [
        pl.lit("b3").alias("source"),
        pl.lit(source_dataset).alias("source_dataset"),
        pl.lit(download_timestamp_utc).alias("download_timestamp_utc"),
        pl.lit(str(raw_path) if raw_path is not None else None).alias("raw_path"),
        pl.lit(sha256).alias("sha256"),
    ]
    if ref_date is not None and "ref_date" not in frame.columns:
        expressions.append(pl.lit(ref_date).alias("ref_date"))
    return frame.with_columns(expressions)
