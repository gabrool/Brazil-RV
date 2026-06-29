from __future__ import annotations

import polars as pl

from bralpha.timing.vintages import (
    AVAILABILITY_OFFICIAL_LAG_POLICY,
    REVISION_OFFICIAL_LAG_NO_REVISIONS,
)


def ensure_anp_pit_columns(frame: pl.DataFrame) -> pl.DataFrame:
    defaults = {
        "availability_basis": AVAILABILITY_OFFICIAL_LAG_POLICY,
        "revision_policy": REVISION_OFFICIAL_LAG_NO_REVISIONS,
        "release_date": None,
        "source_publication_datetime_utc": None,
        "source_last_modified_utc": None,
        "first_seen_timestamp_utc": None,
        "vintage_id": "legacy",
        "revision_sequence": 0,
        "model_usable": True,
        "model_usable_reason": REVISION_OFFICIAL_LAG_NO_REVISIONS,
    }
    missing = [
        pl.lit(value).alias(column)
        for column, value in defaults.items()
        if column not in frame.columns
    ]
    if not missing:
        return frame
    return frame.with_columns(missing)


def anp_pit_aggregations() -> list[pl.Expr]:
    return [
        pl.col("availability_basis").drop_nulls().first(),
        pl.col("revision_policy").drop_nulls().first(),
        pl.col("release_date").max(),
        pl.col("source_publication_datetime_utc").drop_nulls().max(),
        pl.col("source_last_modified_utc").drop_nulls().max(),
        pl.col("first_seen_timestamp_utc").drop_nulls().max(),
        pl.col("revision_sequence").max(),
        pl.col("model_usable").fill_null(False).all(),
        pl.col("model_usable_reason").drop_nulls().first(),
    ]
