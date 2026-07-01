from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.tesouro.calendar import business_day_frame, business_days_b3
from bralpha.derived.tesouro.prices_rates import direto_feature_id
from bralpha.derived.tesouro.quality import validate_asof_panel, validate_panel
from bralpha.derived.tesouro.schemas import (
    PANEL_PRIMARY_KEYS,
    TESOURO_DIRETO_STOCK_ASOF_DAILY_COLUMNS,
    TESOURO_DIRETO_STOCK_OBSERVATION_COLUMNS,
    TESOURO_DPF_STOCK_ASOF_DAILY_COLUMNS,
    TESOURO_DPF_STOCK_OBSERVATION_COLUMNS,
)
from bralpha.parsing.common import normalize_column_name


def build_direto_stock_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(feature_id=_direto_stock_feature_id_expr())
        .select(TESOURO_DIRETO_STOCK_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["direto_stock_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=TESOURO_DIRETO_STOCK_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["direto_stock_observation"],
    )
    return frame


def build_direto_stock_asof_daily(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_dense_keys: int,
) -> pl.DataFrame:
    return _state_asof(
        observations,
        start=start,
        end=end,
        max_dense_keys=max_dense_keys,
        columns=TESOURO_DIRETO_STOCK_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["direto_stock_asof_daily"],
    )


def build_dpf_stock_observation(
    silver: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    frame = silver
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.with_columns(feature_id=_dpf_feature_id_expr())
        .select(TESOURO_DPF_STOCK_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["dpf_stock_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=TESOURO_DPF_STOCK_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["dpf_stock_observation"],
    )
    return frame


def build_dpf_stock_asof_daily(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_dense_keys: int,
) -> pl.DataFrame:
    return _state_asof(
        observations,
        start=start,
        end=end,
        max_dense_keys=max_dense_keys,
        columns=TESOURO_DPF_STOCK_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["dpf_stock_asof_daily"],
    )


def _state_asof(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_dense_keys: int,
    columns: list[str],
    primary_keys: list[str],
) -> pl.DataFrame:
    days = business_days_b3(start, end)
    if observations.is_empty() or not days:
        return _empty(columns)

    obs = (
        observations.filter(pl.col("available_date").is_not_null())
        .rename(
            {
                "ref_date": "observation_ref_date",
                "available_date": "observation_available_date",
            }
        )
        .sort(["feature_id", "observation_available_date"])
    )
    if obs.is_empty():
        return _empty(columns)

    key_count = obs.select("feature_id").unique().height
    if key_count > max_dense_keys:
        raise ValueError(
            f"Selected Tesouro stock keys exceed max_dense_keys: {key_count} > {max_dense_keys}"
        )

    grid = business_day_frame(start, end).join(
        obs.select("feature_id").unique().sort("feature_id"),
        how="cross",
    )
    frame = (
        grid.sort(["feature_id", "ref_date"])
        .join_asof(
            obs,
            left_on="ref_date",
            right_on="observation_available_date",
            by="feature_id",
            strategy="backward",
            check_sortedness=False,
        )
        .filter(pl.col("observation_available_date").is_not_null())
        .with_columns(
            available_date=pl.col("ref_date"),
            is_available=pl.lit(True),
            is_observed_on_ref_date=pl.col("observation_ref_date") == pl.col("ref_date"),
            staleness_days=(pl.col("ref_date") - pl.col("observation_available_date"))
            .dt.total_days()
            .cast(pl.Int64),
        )
        .select(columns)
    )
    validate_asof_panel(frame, required_columns=columns, primary_keys=primary_keys)
    return frame


def _direto_stock_feature_id_expr() -> pl.Expr:
    return pl.struct(["security_type", "security_name", "maturity_date"]).map_elements(
        lambda row: direto_feature_id(
            "tesouro_direto_stock",
            row["security_type"],
            row["security_name"],
            row["maturity_date"],
        ),
        return_dtype=pl.Utf8,
    )


def _dpf_feature_id_expr() -> pl.Expr:
    return pl.struct(
        ["debt_category", "instrument_type", "indexer", "holder_or_maturity_bucket"]
    ).map_elements(
        lambda row: "|".join(
            [
                "tesouro_dpf_stock",
                _text_token(row["debt_category"]),
                _text_token(row["instrument_type"]),
                _text_token(row["indexer"]),
                _text_token(row["holder_or_maturity_bucket"]),
            ]
        ),
        return_dtype=pl.Utf8,
    )


def _text_token(value: Any) -> str:
    if value is None:
        return "null"
    token = normalize_column_name(str(value).strip())
    return token or "null"


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in columns})
