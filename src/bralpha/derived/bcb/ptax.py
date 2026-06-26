from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.bcb.quality import validate_panel
from bralpha.derived.bcb.schemas import (
    BCB_PTAX_SELECTED_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_ptax_selected_daily(
    silver: pl.DataFrame,
    *,
    currencies: list[str],
    use_selected_bulletin_only: bool,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if currencies:
        frame = frame.filter(pl.col("currency_code").is_in(currencies))
    if use_selected_bulletin_only:
        frame = frame.filter(pl.col("is_selected_bulletin"))

    frame = (
        frame.with_columns(
            selected_bulletin_type=pl.col("bulletin_type"),
            has_quote=pl.col("bid_rate").is_not_null() | pl.col("ask_rate").is_not_null(),
        )
        .select(BCB_PTAX_SELECTED_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["ptax_selected_daily"], keep="last", maintain_order=True)
    )
    validate_panel(
        frame,
        required_columns=BCB_PTAX_SELECTED_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["ptax_selected_daily"],
        nonnegative_columns=["bid_rate", "ask_rate", "bid_parity", "ask_parity"],
    )
    return frame
