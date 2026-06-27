from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.tesouro.quality import validate_panel
from bralpha.derived.tesouro.schemas import (
    PANEL_PRIMARY_KEYS,
    TESOURO_DIRETO_FLOWS_DAILY_COLUMNS,
)
from bralpha.parsing.common import normalize_column_name


def build_direto_flows_daily(
    *,
    sales: pl.DataFrame | None,
    redemptions: pl.DataFrame | None,
    include_sales: bool,
    include_redemptions: bool,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    frames = []
    if include_sales and sales is not None and not sales.is_empty():
        frames.append(_sales_rows(sales))
    if include_redemptions and redemptions is not None and not redemptions.is_empty():
        frames.append(_redemption_rows(redemptions))
    if not frames:
        return _empty()

    frame = pl.concat(frames, how="diagonal_relaxed").filter(pl.col("ref_date").is_not_null())
    if start is not None:
        frame = frame.filter(pl.col("ref_date") >= start)
    if end is not None:
        frame = frame.filter(pl.col("ref_date") <= end)

    frame = (
        frame.select(TESOURO_DIRETO_FLOWS_DAILY_COLUMNS)
        .unique(
            subset=PANEL_PRIMARY_KEYS["direto_flows_daily"],
            keep="last",
            maintain_order=True,
        )
        .sort(["ref_date", "flow_type", "feature_id"])
    )
    validate_panel(
        frame,
        required_columns=TESOURO_DIRETO_FLOWS_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["direto_flows_daily"],
    )
    return frame


def _sales_rows(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        _ensure_columns(frame, ["investor_count", "source_dataset"])
        .filter(pl.col("available_date").is_not_null())
        .with_columns(
            observation_ref_date=pl.col("ref_date"),
            observation_available_date=pl.col("available_date"),
            ref_date=pl.col("available_date"),
            available_date=pl.col("available_date"),
            flow_type=pl.lit("sale"),
            redemption_type=pl.lit(None, dtype=pl.Utf8),
        )
        .with_columns(feature_id=_flow_feature_id_expr())
    )


def _redemption_rows(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        _ensure_columns(frame, ["investor_count", "source_dataset"])
        .filter(pl.col("available_date").is_not_null())
        .with_columns(
            observation_ref_date=pl.col("ref_date"),
            observation_available_date=pl.col("available_date"),
            ref_date=pl.col("available_date"),
            available_date=pl.col("available_date"),
            flow_type=pl.lit("redemption"),
        )
        .with_columns(feature_id=_flow_feature_id_expr())
    )


def _flow_feature_id_expr() -> pl.Expr:
    return pl.struct(
        ["flow_type", "redemption_type", "security_type", "security_name", "maturity_date"]
    ).map_elements(
        lambda row: "|".join(
            [
                "tesouro_direto_flows",
                _text_token(row["flow_type"]),
                _text_token(row["redemption_type"]),
                _text_token(row["security_type"]),
                _text_token(row["security_name"]),
                _date_token(row["maturity_date"]),
            ]
        ),
        return_dtype=pl.Utf8,
    )


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns([pl.lit(None).alias(column) for column in missing])


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


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in TESOURO_DIRETO_FLOWS_DAILY_COLUMNS})
