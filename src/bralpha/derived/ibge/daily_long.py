from __future__ import annotations

import polars as pl

from bralpha.derived.ibge.quality import validate_asof_panel
from bralpha.derived.ibge.schemas import IBGE_DAILY_LONG_COLUMNS, PANEL_PRIMARY_KEYS


def build_daily_long(
    *,
    sidra_asof_daily: pl.DataFrame | None = None,
    sidra_feature_daily: pl.DataFrame | None = None,
    include_sidra: bool,
) -> pl.DataFrame:
    frames = []
    if include_sidra and sidra_asof_daily is not None and not sidra_asof_daily.is_empty():
        frames.append(
            sidra_asof_daily.with_columns(
                source_family=pl.lit("ibge_sidra"),
                value_name=pl.lit("value"),
                value=pl.col("value").cast(pl.Float64),
            )
            .filter(pl.col("value").is_not_null())
            .select(IBGE_DAILY_LONG_COLUMNS)
        )
    if include_sidra and sidra_feature_daily is not None and not sidra_feature_daily.is_empty():
        frames.append(
            _ensure_columns(sidra_feature_daily, IBGE_DAILY_LONG_COLUMNS)
            .filter(pl.col("value").is_not_null())
            .select(IBGE_DAILY_LONG_COLUMNS)
        )
    if not frames:
        return _empty()

    frame = (
        pl.concat(frames, how="diagonal_relaxed")
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


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns([pl.lit(None).alias(column) for column in missing])
