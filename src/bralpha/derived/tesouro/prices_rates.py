from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.tesouro.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.tesouro.quality import validate_asof_panel, validate_panel
from bralpha.derived.tesouro.schemas import (
    PANEL_PRIMARY_KEYS,
    TESOURO_DIRETO_PRICES_RATES_ASOF_DAILY_COLUMNS,
    TESOURO_DIRETO_PRICES_RATES_OBSERVATION_COLUMNS,
)
from bralpha.parsing.common import normalize_column_name


def build_direto_prices_rates_observation(
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
        frame.with_columns(
            feature_id=_direto_feature_id_expr("tesouro_direto_prices_rates"),
            has_rate=pl.col("buy_rate").is_not_null() | pl.col("sell_rate").is_not_null(),
            has_price=pl.col("buy_price").is_not_null() | pl.col("sell_price").is_not_null(),
        )
        .select(TESOURO_DIRETO_PRICES_RATES_OBSERVATION_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["direto_prices_rates_observation"],
            keep="last",
            maintain_order=True,
        )
    )
    validate_panel(
        frame,
        required_columns=TESOURO_DIRETO_PRICES_RATES_OBSERVATION_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["direto_prices_rates_observation"],
    )
    return frame


def build_direto_prices_rates_asof_daily(
    observations: pl.DataFrame,
    *,
    start: date,
    end: date,
    max_dense_securities: int,
) -> pl.DataFrame:
    days = business_days_mon_fri(start, end)
    if observations.is_empty() or not days:
        return _empty_asof()

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
        return _empty_asof()

    security_count = obs.select("feature_id").unique().height
    if security_count > max_dense_securities:
        raise ValueError(
            f"Selected Tesouro Direto securities exceed max_dense_securities: "
            f"{security_count} > {max_dense_securities}"
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
        .select(TESOURO_DIRETO_PRICES_RATES_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=TESOURO_DIRETO_PRICES_RATES_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["direto_prices_rates_asof_daily"],
    )
    return frame


def direto_feature_id(
    prefix: str,
    security_type: Any,
    security_name: Any,
    maturity_date: Any,
) -> str:
    return "|".join(
        [
            prefix,
            _text_token(security_type),
            _text_token(security_name),
            _date_token(maturity_date),
        ]
    )


def _direto_feature_id_expr(prefix: str) -> pl.Expr:
    return pl.struct(["security_type", "security_name", "maturity_date"]).map_elements(
        lambda row: direto_feature_id(
            prefix,
            row["security_type"],
            row["security_name"],
            row["maturity_date"],
        ),
        return_dtype=pl.Utf8,
    )


def _text_token(value: Any) -> str:
    if value is None:
        return "null"
    token = normalize_column_name(str(value).strip())
    return token or "null"


def _date_token(value: Any) -> str:
    if value is None:
        return "null"
    text = str(value).strip()
    return text[:10] if text else "null"


def _empty_asof() -> pl.DataFrame:
    return pl.DataFrame(
        schema={column: pl.Null for column in TESOURO_DIRETO_PRICES_RATES_ASOF_DAILY_COLUMNS}
    )
