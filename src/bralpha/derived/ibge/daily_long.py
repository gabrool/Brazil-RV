from __future__ import annotations

import polars as pl

from bralpha.derived.ibge.quality import validate_asof_panel
from bralpha.derived.ibge.schemas import IBGE_DAILY_LONG_COLUMNS, PANEL_PRIMARY_KEYS


def build_daily_long(
    *,
    sidra_asof_daily: pl.DataFrame | None = None,
    include_sidra: bool,
) -> pl.DataFrame:
    if not include_sidra or sidra_asof_daily is None or sidra_asof_daily.is_empty():
        return _empty()

    frame = (
        sidra_asof_daily.with_columns(
            source_family=pl.lit("ibge_sidra"),
            value_name=pl.lit("value"),
            value=pl.col("value").cast(pl.Float64),
        )
        .filter(pl.col("value").is_not_null())
        .select(IBGE_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=IBGE_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in IBGE_DAILY_LONG_COLUMNS})
