from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.novo_caged.quality import validate_panel
from bralpha.derived.novo_caged.schemas import (
    NOVO_CAGED_RELEASE_CALENDAR_REFERENCE_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_release_calendar_reference(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_reference()

    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty_reference()

    panel = (
        frame.select(NOVO_CAGED_RELEASE_CALENDAR_REFERENCE_COLUMNS)
        .sort(["ref_date", "release_date"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["release_calendar_reference"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        panel,
        required_columns=NOVO_CAGED_RELEASE_CALENDAR_REFERENCE_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["release_calendar_reference"],
    )
    return panel


def _empty_reference() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in NOVO_CAGED_RELEASE_CALENDAR_REFERENCE_COLUMNS}
    )
