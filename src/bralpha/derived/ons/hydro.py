from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.ons.quality import validate_panel
from bralpha.derived.ons.schemas import (
    ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS,
    ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS,
    ONS_PIT_SNAPSHOT_COLUMNS,
    PANEL_PRIMARY_KEYS,
)
from bralpha.parsing.common import normalize_column_name
from bralpha.timing.vintages import (
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
)


def build_ear_subsystem_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_ear()

    frame = _ensure_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(
            feature_id=_feature_id_expr("ons_ear_subsystem", ["subsystem_id", "subsystem"]),
            has_stored_energy_mwmes=pl.col("stored_energy_mwmes").is_not_null(),
            has_stored_energy_percent=pl.col("stored_energy_percent").is_not_null(),
            has_stored_energy_max_mwmes=pl.col("stored_energy_max_mwmes").is_not_null(),
        )
        .select(ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["ear_subsystem_observation"],
            keep="last",
            maintain_order=True,
        )
        .sort(["subsystem_id", "ref_date"])
    )
    validate_panel(
        frame,
        required_columns=ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["ear_subsystem_observation"],
    )
    return frame


def build_ena_subsystem_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_ena()

    frame = _ensure_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(
            feature_id=_feature_id_expr(
                "ons_ena_subsystem",
                ["subsystem_id", "subsystem", "ena_type"],
            ),
            has_ena_value=pl.col("ena_value").is_not_null(),
        )
        .select(ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["ena_subsystem_observation"],
            keep="last",
            maintain_order=True,
        )
        .sort(["subsystem_id", "ena_type", "ref_date"])
    )
    validate_panel(
        frame,
        required_columns=ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["ena_subsystem_observation"],
    )
    return frame


def ons_feature_id(prefix: str, *parts: Any) -> str:
    tokens = [_token(part) for part in parts]
    return "|".join([prefix, *tokens])


def _feature_id_expr(prefix: str, columns: list[str]) -> pl.Expr:
    return pl.struct(columns).map_elements(
        lambda row: ons_feature_id(prefix, *[row[column] for column in columns]),
        return_dtype=pl.Utf8,
    )


def _token(value: Any) -> str:
    if value is None:
        return "null"
    token = normalize_column_name(str(value).strip())
    return token or "null"


def _ensure_pit_columns(frame: pl.DataFrame) -> pl.DataFrame:
    additions = []
    if "availability_basis" not in frame.columns:
        additions.append(
            pl.lit(AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE).alias("availability_basis")
        )
    if "revision_policy" not in frame.columns:
        additions.append(pl.lit(REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY).alias("revision_policy"))
    if "model_usable" not in frame.columns:
        additions.append(pl.lit(False).alias("model_usable"))
    for column in ONS_PIT_SNAPSHOT_COLUMNS:
        if column not in frame.columns:
            additions.append(pl.lit(None).alias(column))
    if "availability_note" not in frame.columns:
        additions.append(pl.lit(None, dtype=pl.Utf8).alias("availability_note"))
    return frame.with_columns(additions) if additions else frame


def _empty_ear() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in ONS_EAR_SUBSYSTEM_OBSERVATION_COLUMNS}
    )


def _empty_ena() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in ONS_ENA_SUBSYSTEM_OBSERVATION_COLUMNS}
    )
