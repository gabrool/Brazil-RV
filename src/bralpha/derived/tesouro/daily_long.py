from __future__ import annotations

import polars as pl

from bralpha.derived.tesouro.quality import validate_asof_panel
from bralpha.derived.tesouro.schemas import PANEL_PRIMARY_KEYS, TESOURO_DAILY_LONG_COLUMNS

PRICES_RATES_VALUE_COLUMNS = ["buy_rate", "sell_rate", "buy_price", "sell_price"]
FLOW_VALUE_COLUMNS = ["quantity", "value", "investor_count"]
DIRETO_STOCK_VALUE_COLUMNS = ["quantity", "stock_value", "investor_count"]
DPF_STOCK_VALUE_COLUMNS = ["stock_value"]


def build_daily_long(
    *,
    direto_prices_rates_asof_daily: pl.DataFrame | None = None,
    direto_flows_daily: pl.DataFrame | None = None,
    direto_stock_asof_daily: pl.DataFrame | None = None,
    dpf_stock_asof_daily: pl.DataFrame | None = None,
    feature_daily: pl.DataFrame | None = None,
    include_prices_rates: bool,
    include_flows: bool,
    include_stock: bool,
) -> pl.DataFrame:
    frames = []
    if feature_daily is not None and not feature_daily.is_empty():
        frames.append(
            _ensure_columns(feature_daily, TESOURO_DAILY_LONG_COLUMNS)
            .filter(pl.col("value").is_not_null())
            .select(TESOURO_DAILY_LONG_COLUMNS)
        )
    if (
        include_prices_rates
        and direto_prices_rates_asof_daily is not None
        and not direto_prices_rates_asof_daily.is_empty()
    ):
        frames.extend(
            _value_rows(
                direto_prices_rates_asof_daily,
                source_family="tesouro_direto_prices_rates",
                value_columns=PRICES_RATES_VALUE_COLUMNS,
            )
        )
    if include_flows and direto_flows_daily is not None and not direto_flows_daily.is_empty():
        frames.extend(_flow_rows(direto_flows_daily))
    if include_stock:
        if direto_stock_asof_daily is not None and not direto_stock_asof_daily.is_empty():
            frames.extend(
                _value_rows(
                    direto_stock_asof_daily,
                    source_family="tesouro_direto_stock",
                    value_columns=DIRETO_STOCK_VALUE_COLUMNS,
                )
            )
        if dpf_stock_asof_daily is not None and not dpf_stock_asof_daily.is_empty():
            frames.extend(
                _value_rows(
                    dpf_stock_asof_daily,
                    source_family="tesouro_dpf_stock",
                    value_columns=DPF_STOCK_VALUE_COLUMNS,
                )
            )

    if not frames:
        return _empty()

    frame = (
        pl.concat(frames, how="diagonal_relaxed")
        .filter(pl.col("value").is_not_null())
        .select(TESOURO_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=TESOURO_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _value_rows(
    frame: pl.DataFrame,
    *,
    source_family: str,
    value_columns: list[str],
) -> list[pl.DataFrame]:
    return [
        _ensure_columns(
            frame.with_columns(
                source_family=pl.lit(source_family),
                value_name=pl.lit(column),
                value=pl.col(column).cast(pl.Float64),
            ),
            TESOURO_DAILY_LONG_COLUMNS,
        ).select(TESOURO_DAILY_LONG_COLUMNS)
        for column in value_columns
    ]


def _flow_rows(frame: pl.DataFrame) -> list[pl.DataFrame]:
    return [
        _ensure_columns(
            frame.with_columns(
                source_family=pl.lit("tesouro_direto_flows"),
                value_name=pl.lit(column),
                value=pl.col(column).cast(pl.Float64),
                is_available=pl.lit(True),
                staleness_days=pl.lit(0, dtype=pl.Int64),
            ),
            TESOURO_DAILY_LONG_COLUMNS,
        ).select(TESOURO_DAILY_LONG_COLUMNS)
        for column in FLOW_VALUE_COLUMNS
    ]


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in TESOURO_DAILY_LONG_COLUMNS})


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns([pl.lit(None).alias(column) for column in missing])
