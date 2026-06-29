from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.receita.pit import ensure_receita_pit_columns
from bralpha.derived.receita.quality import validate_panel
from bralpha.derived.receita.schemas import (
    PANEL_PRIMARY_KEYS,
    RECEITA_TAX_COLLECTION_FEATURE_OBSERVATION_COLUMNS,
    RECEITA_TAX_COLLECTION_OBSERVATION_COLUMNS,
)
from bralpha.parsing.common import normalize_column_name


def build_tax_collection_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty(RECEITA_TAX_COLLECTION_OBSERVATION_COLUMNS)

    frame = ensure_receita_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(RECEITA_TAX_COLLECTION_OBSERVATION_COLUMNS)

    panel = (
        frame.with_columns(has_collection_amount_brl=pl.col("collection_amount_brl").is_not_null())
        .select(RECEITA_TAX_COLLECTION_OBSERVATION_COLUMNS)
        .sort(PANEL_PRIMARY_KEYS["tax_collection_observation"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["tax_collection_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        panel,
        required_columns=RECEITA_TAX_COLLECTION_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["tax_collection_observation"],
    )
    return panel


def build_tax_collection_feature_observation(
    observations: pl.DataFrame,
    *,
    max_features: int,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if observations.is_empty():
        return _empty(RECEITA_TAX_COLLECTION_FEATURE_OBSERVATION_COLUMNS)

    frame = ensure_receita_pit_columns(observations)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)
    if frame.is_empty():
        return _empty(RECEITA_TAX_COLLECTION_FEATURE_OBSERVATION_COLUMNS)

    panel = frame.with_columns(
        feature_id=pl.struct(["collection_scope", "table_kind", "revenue_key"]).map_elements(
            lambda row: receita_tax_collection_feature_id(
                row["collection_scope"],
                row["table_kind"],
                row["revenue_key"],
            ),
            return_dtype=pl.Utf8,
        )
    )
    _raise_on_feature_collisions(panel)
    feature_count = panel.select("feature_id").unique().height
    if feature_count > max_features:
        raise ValueError(
            f"Receita tax-collection feature count {feature_count} exceeds "
            f"max_features={max_features}"
        )

    panel = (
        panel.select(RECEITA_TAX_COLLECTION_FEATURE_OBSERVATION_COLUMNS)
        .sort(["ref_date", "feature_id"])
        .unique(
            subset=PANEL_PRIMARY_KEYS["tax_collection_feature_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        panel,
        required_columns=RECEITA_TAX_COLLECTION_FEATURE_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["tax_collection_feature_observation"],
    )
    return panel


def receita_tax_collection_feature_id(
    collection_scope: object,
    table_kind: object,
    revenue_key: object,
) -> str:
    return (
        "receita_tax_collection|"
        f"{_token(collection_scope)}|{_token(table_kind)}|{_token(revenue_key)}"
    )


def _raise_on_feature_collisions(frame: pl.DataFrame) -> None:
    duplicate_keys = frame.group_by(PANEL_PRIMARY_KEYS["tax_collection_feature_observation"]).len()
    duplicate_keys = duplicate_keys.filter(pl.col("len") > 1)
    if duplicate_keys.is_empty():
        return
    sample = duplicate_keys.select(["ref_date", "feature_id"]).head(3).to_dicts()
    raise ValueError(f"Receita feature_id collision for same ref_date: {sample}")


def _token(value: object) -> str:
    if value is None:
        return "unknown"
    token = normalize_column_name(str(value).strip())
    return token or "unknown"


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in columns})
