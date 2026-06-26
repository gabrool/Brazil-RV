from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bralpha.parsing.common import HeaderRequirement, read_delimited_or_html


def parse_fee_schedule_table(
    content: bytes,
    *,
    fee_id: str,
    download_date: date,
    source_dataset: str = "b3_fee_schedules",
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
    return frame.with_columns(
        [
            pl.lit(fee_id).alias("fee_id"),
            pl.lit(download_date).alias("download_date"),
            pl.lit("b3").alias("source"),
            pl.lit(source_dataset).alias("source_dataset"),
            pl.lit(download_timestamp_utc).alias("download_timestamp_utc"),
            pl.lit(str(raw_path) if raw_path is not None else None).alias("raw_path"),
            pl.lit(sha256).alias("sha256"),
        ]
    )
