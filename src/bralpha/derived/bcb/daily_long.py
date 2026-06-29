from __future__ import annotations

import polars as pl

from bralpha.derived.bcb.quality import validate_asof_panel
from bralpha.derived.bcb.schemas import BCB_DAILY_LONG_COLUMNS, PANEL_PRIMARY_KEYS

PTAX_VALUE_COLUMNS = ["bid_rate", "ask_rate", "bid_parity", "ask_parity"]
FOCUS_VALUE_COLUMNS = ["mean", "median", "std_dev", "min_value", "max_value", "respondents"]


def build_daily_long(
    *,
    sgs_asof_daily: pl.DataFrame | None = None,
    sgs_feature_daily: pl.DataFrame | None = None,
    ptax_selected_daily: pl.DataFrame | None = None,
    focus_expectation_asof_daily: pl.DataFrame | None = None,
    include_sgs: bool,
    include_ptax: bool,
    include_focus: bool,
) -> pl.DataFrame:
    frames = []
    if include_sgs and sgs_asof_daily is not None and not sgs_asof_daily.is_empty():
        frames.append(_sgs_rows(sgs_asof_daily))
    if include_sgs and sgs_feature_daily is not None and not sgs_feature_daily.is_empty():
        frames.append(_sgs_feature_rows(sgs_feature_daily))
    if include_ptax and ptax_selected_daily is not None and not ptax_selected_daily.is_empty():
        frames.extend(_ptax_rows(ptax_selected_daily))
    if (
        include_focus
        and focus_expectation_asof_daily is not None
        and not focus_expectation_asof_daily.is_empty()
    ):
        frames.extend(_focus_rows(focus_expectation_asof_daily))

    if not frames:
        return _empty()

    frame = (
        pl.concat(frames, how="diagonal_relaxed")
        .filter(pl.col("value").is_not_null())
        .select(BCB_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=BCB_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _sgs_rows(frame: pl.DataFrame) -> pl.DataFrame:
    frame = _ensure_columns(frame, BCB_DAILY_LONG_COLUMNS)
    return (
        frame.filter(pl.col("model_usable").fill_null(False))
        .with_columns(
            source_family=pl.lit("sgs"),
            feature_id=pl.concat_str(
                [
                    pl.lit("sgs:"),
                    pl.coalesce([pl.col("series_slug"), pl.col("series_id").cast(pl.Utf8)]),
                ]
            ),
            value_name=pl.lit("value"),
            value=pl.col("value").cast(pl.Float64),
        )
        .select(BCB_DAILY_LONG_COLUMNS)
    )


def _sgs_feature_rows(frame: pl.DataFrame) -> pl.DataFrame:
    frame = _ensure_columns(frame, BCB_DAILY_LONG_COLUMNS)
    return (
        frame.filter(pl.col("model_usable").fill_null(False))
        .with_columns(value=pl.col("value").cast(pl.Float64))
        .select(BCB_DAILY_LONG_COLUMNS)
    )


def _ptax_rows(frame: pl.DataFrame) -> list[pl.DataFrame]:
    return [
        _ensure_columns(
            frame.with_columns(
                source_family=pl.lit("ptax"),
                feature_id=pl.concat_str([pl.lit("ptax:"), pl.col("currency_code")]),
                value_name=pl.lit(column),
                value=pl.col(column).cast(pl.Float64),
                unit=pl.lit(None, dtype=pl.Utf8),
                observation_ref_date=pl.col("ref_date"),
                observation_available_date=pl.col("available_date"),
                ref_date=pl.col("available_date"),
                is_available=pl.col("has_quote"),
                staleness_days=pl.lit(0, dtype=pl.Int64),
            ),
            BCB_DAILY_LONG_COLUMNS,
        ).select(BCB_DAILY_LONG_COLUMNS)
        for column in PTAX_VALUE_COLUMNS
    ]


def _focus_rows(frame: pl.DataFrame) -> list[pl.DataFrame]:
    return [
        _ensure_columns(
            frame.with_columns(
                source_family=pl.lit("focus"),
                feature_id=pl.concat_str([pl.lit("focus:"), pl.col("expectation_key")]),
                value_name=pl.lit(column),
                value=pl.col(column).cast(pl.Float64),
                unit=pl.lit(None, dtype=pl.Utf8),
            ),
            BCB_DAILY_LONG_COLUMNS,
        ).select(BCB_DAILY_LONG_COLUMNS)
        for column in FOCUS_VALUE_COLUMNS
    ]


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in BCB_DAILY_LONG_COLUMNS})


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns([pl.lit(None).alias(column) for column in missing])
