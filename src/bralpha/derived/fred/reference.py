from __future__ import annotations

import polars as pl

from bralpha.derived.fred.observations import fred_feature_id
from bralpha.derived.fred.quality import validate_panel
from bralpha.derived.fred.schemas import (
    FRED_SERIES_REFERENCE_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.ingestion.fred.common import FredSeriesConfig


def build_fred_series_reference(
    series_config: list[FredSeriesConfig],
    *,
    source_version: str = "v0",
) -> pl.DataFrame:
    rows = [
        {
            "series_id": item.series_id.strip().upper(),
            "series_name": item.name,
            "category": item.category,
            "frequency": item.frequency,
            "unit": item.unit,
            "source": item.source,
            "priority": item.priority,
            "model_usable": item.model_usable,
            "availability_policy": item.availability_policy,
            "series_kind": item.series_kind,
            "vintage_policy": item.vintage_policy,
            "vintage_request_mode": item.vintage_request_mode,
            "model_usable_without_vintage": item.model_usable_without_vintage,
            "notes": item.notes,
            "feature_id": fred_feature_id(item.series_id),
            "source_version": source_version,
        }
        for item in series_config
    ]
    frame = (
        pl.DataFrame(rows).select(FRED_SERIES_REFERENCE_COLUMNS)
        if rows
        else _empty_reference()
    )
    validate_panel(
        frame,
        required_columns=FRED_SERIES_REFERENCE_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["series_reference"],
    )
    return frame


def _empty_reference() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in FRED_SERIES_REFERENCE_COLUMNS})
