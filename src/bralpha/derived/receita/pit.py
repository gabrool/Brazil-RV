from __future__ import annotations

import polars as pl

from bralpha.timing.vintages import (
    AVAILABILITY_CONSERVATIVE_HEURISTIC,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
)


def ensure_receita_pit_columns(frame: pl.DataFrame) -> pl.DataFrame:
    defaults = {
        "availability_basis": AVAILABILITY_CONSERVATIVE_HEURISTIC,
        "revision_policy": REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
        "release_date": None,
        "source_publication_datetime_utc": None,
        "source_last_modified_utc": None,
        "first_seen_timestamp_utc": None,
        "vintage_id": "legacy",
        "revision_sequence": 0,
        "model_usable": True,
        "model_usable_reason": "legacy_fixture_default",
    }
    missing = [
        pl.lit(value).alias(column)
        for column, value in defaults.items()
        if column not in frame.columns
    ]
    if not missing:
        return frame
    return frame.with_columns(missing)
