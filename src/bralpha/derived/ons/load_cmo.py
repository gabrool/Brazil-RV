from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.ons.hydro import ons_feature_id
from bralpha.derived.ons.quality import validate_panel
from bralpha.derived.ons.schemas import (
    ONS_CMO_WEEKLY_OBSERVATION_COLUMNS,
    ONS_LOAD_DAILY_OBSERVATION_COLUMNS,
    PANEL_PRIMARY_KEYS,
)


def build_load_daily_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_load()

    frame = _ensure_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(
            feature_id=pl.struct(["subsystem_id", "subsystem"]).map_elements(
                lambda row: ons_feature_id(
                    "ons_load_daily",
                    row["subsystem_id"],
                    row["subsystem"],
                ),
                return_dtype=pl.Utf8,
            ),
            has_load_mwmed=pl.col("load_mwmed").is_not_null(),
        )
        .select(ONS_LOAD_DAILY_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["load_daily_observation"],
            keep="last",
            maintain_order=True,
        )
        .sort(["subsystem_id", "ref_date"])
    )
    validate_panel(
        frame,
        required_columns=ONS_LOAD_DAILY_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["load_daily_observation"],
    )
    return frame


def build_cmo_weekly_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    if silver.is_empty():
        return _empty_cmo()

    frame = _ensure_pit_columns(silver)
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(
            feature_id=pl.struct(["subsystem_id", "subsystem", "load_block"]).map_elements(
                lambda row: ons_feature_id(
                    "ons_cmo_weekly",
                    row["subsystem_id"],
                    row["subsystem"],
                    row["load_block"],
                ),
                return_dtype=pl.Utf8,
            ),
            has_cmo_brl_mwh=pl.col("cmo_brl_mwh").is_not_null(),
        )
        .select(ONS_CMO_WEEKLY_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["cmo_weekly_observation"],
            keep="last",
            maintain_order=True,
        )
        .sort(["subsystem_id", "load_block", "ref_date"])
    )
    validate_panel(
        frame,
        required_columns=ONS_CMO_WEEKLY_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["cmo_weekly_observation"],
    )
    return frame


def _empty_load() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ONS_LOAD_DAILY_OBSERVATION_COLUMNS})


def _empty_cmo() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ONS_CMO_WEEKLY_OBSERVATION_COLUMNS})


def _ensure_pit_columns(frame: pl.DataFrame) -> pl.DataFrame:
    additions = []
    if "availability_basis" not in frame.columns:
        additions.append(pl.lit("fixture_or_legacy_model_usable").alias("availability_basis"))
    if "revision_policy" not in frame.columns:
        additions.append(pl.lit("fixture_or_legacy").alias("revision_policy"))
    if "model_usable" not in frame.columns:
        additions.append(pl.lit(True).alias("model_usable"))
    if "availability_note" not in frame.columns:
        additions.append(pl.lit(None, dtype=pl.Utf8).alias("availability_note"))
    return frame.with_columns(additions) if additions else frame
