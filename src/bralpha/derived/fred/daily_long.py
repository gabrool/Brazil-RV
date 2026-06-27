from __future__ import annotations

import polars as pl

from bralpha.derived.fred.quality import validate_asof_panel
from bralpha.derived.fred.schemas import (
    FRED_DAILY_LONG_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_fred_daily_long(
    *,
    asof_daily: pl.DataFrame | None = None,
    include_observations: bool,
) -> pl.DataFrame:
    if not include_observations or asof_daily is None or asof_daily.is_empty():
        return _empty()

    frame = (
        asof_daily.with_columns(
            source_family=pl.lit("fred"),
            value_name=pl.lit("value"),
            value=pl.col("value").cast(pl.Float64),
        )
        .filter(pl.col("value").is_not_null())
        .select(FRED_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=FRED_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in FRED_DAILY_LONG_COLUMNS})
