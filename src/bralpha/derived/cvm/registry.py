from __future__ import annotations

import polars as pl

from bralpha.derived.cvm.pit import ensure_cvm_pit_columns
from bralpha.derived.cvm.quality import validate_panel
from bralpha.derived.cvm.schemas import (
    CVM_FUND_REGISTRY_CURRENT_REFERENCE_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_fund_registry_current_reference(silver: pl.DataFrame) -> pl.DataFrame:
    if silver.is_empty():
        return _empty()

    frame = (
        ensure_cvm_pit_columns(silver)
        .select(CVM_FUND_REGISTRY_CURRENT_REFERENCE_COLUMNS)
        .sort(["fund_id", "snapshot_date"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["fund_registry_current_reference"],
            keep="last",
            maintain_order=True,
        )
        .sort("fund_id")
    )
    validate_panel(
        frame,
        required_columns=CVM_FUND_REGISTRY_CURRENT_REFERENCE_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["fund_registry_current_reference"],
    )
    return frame


def _empty() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in CVM_FUND_REGISTRY_CURRENT_REFERENCE_COLUMNS}
    )
