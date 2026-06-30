from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.anp.calendar import business_day_frame, business_days_mon_fri
from bralpha.derived.anp.quality import validate_asof_panel
from bralpha.derived.anp.schemas import (
    ANP_DAILY_LONG_COLUMNS,
    ANP_STATE_ASOF_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)

STATE_KEY_COLUMNS = ["source_family", "feature_id", "value_name"]

_FUEL_PRICE_VALUES = [
    ("sale_price", None),
    ("purchase_price", None),
    ("station_count", "stations"),
    ("sale_price_count", "observations"),
    ("purchase_price_count", "observations"),
]

_FUEL_SALES_VALUES = [
    ("sales_volume_m3", None),
    ("sales_volume_count", "observations"),
    ("state_count", "states"),
]

_OIL_GAS_VALUES = [
    ("metric_value", None),
    ("metric_value_count", "observations"),
    ("state_count", "states"),
]


def build_anp_state_asof_daily(
    *,
    fuel_prices: pl.DataFrame | None = None,
    fuel_sales: pl.DataFrame | None = None,
    oil_gas: pl.DataFrame | None = None,
    start: date,
    end: date,
    max_features: int,
) -> pl.DataFrame:
    observations = _concat(
        [
            _state_rows(
                fuel_prices,
                source_family="anp_fuel_price",
                metrics=_FUEL_PRICE_VALUES,
            ),
            _state_rows(
                fuel_sales,
                source_family="anp_fuel_sales",
                metrics=_FUEL_SALES_VALUES,
            ),
            _state_rows(
                oil_gas,
                source_family="anp_oil_gas",
                metrics=_OIL_GAS_VALUES,
            ),
        ]
    )
    if observations.is_empty() or not business_days_mon_fri(start, end):
        return _empty_state()

    obs = (
        observations.filter(pl.col("observation_available_date").is_not_null())
        .sort([*STATE_KEY_COLUMNS, "observation_available_date", "observation_ref_date"])
        .unique(
            subset=[*STATE_KEY_COLUMNS, "observation_available_date"],
            keep="last",
            maintain_order=True,
        )
        .sort([*STATE_KEY_COLUMNS, "observation_available_date"])
    )
    if obs.is_empty():
        return _empty_state()

    feature_count = obs.select(STATE_KEY_COLUMNS).unique().height
    if feature_count > max_features:
        raise ValueError(
            f"Selected ANP feature count {feature_count} exceeds max_features={max_features}"
        )

    grid = business_day_frame(start, end).join(
        obs.select(STATE_KEY_COLUMNS).unique().sort(STATE_KEY_COLUMNS),
        how="cross",
    )
    frame = (
        grid.sort([*STATE_KEY_COLUMNS, "ref_date"])
        .join_asof(
            obs,
            left_on="ref_date",
            right_on="observation_available_date",
            by=STATE_KEY_COLUMNS,
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
        .select(ANP_STATE_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=ANP_STATE_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["state_asof_daily"],
    )
    return frame


def build_anp_daily_long(
    *,
    state_asof_daily: pl.DataFrame | None = None,
    fuel_feature_daily: pl.DataFrame | None = None,
    include_fuel_prices: bool,
    include_fuel_sales: bool,
    include_oil_gas: bool,
) -> pl.DataFrame:
    parts: list[pl.DataFrame] = []
    if fuel_feature_daily is not None and not fuel_feature_daily.is_empty():
        parts.append(fuel_feature_daily.select(ANP_DAILY_LONG_COLUMNS))

    if state_asof_daily is None or state_asof_daily.is_empty():
        if not parts:
            return _empty_daily_long()
        frame = _concat(parts)
        return _validated_daily_long(frame)

    families = _included_families(
        include_fuel_prices=include_fuel_prices,
        include_fuel_sales=include_fuel_sales,
        include_oil_gas=include_oil_gas,
    )
    if families:
        parts.append(
            state_asof_daily.filter(pl.col("source_family").is_in(families)).select(
                ANP_DAILY_LONG_COLUMNS
            )
        )

    if not parts:
        return _empty_daily_long()

    return _validated_daily_long(_concat(parts))


def _validated_daily_long(frame: pl.DataFrame) -> pl.DataFrame:
    frame = (
        frame.filter(pl.col("value").is_not_null())
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=ANP_DAILY_LONG_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["daily_long"],
    )
    return frame


def _state_rows(
    frame: pl.DataFrame | None,
    *,
    source_family: str,
    metrics: list[tuple[str, str | None]],
) -> pl.DataFrame | None:
    if frame is None or frame.is_empty():
        return None
    rows: list[pl.DataFrame] = []
    for metric, fixed_unit in metrics:
        unit_expr = pl.lit(fixed_unit) if fixed_unit is not None else pl.col("unit")
        rows.append(
            frame.select(
                [
                    pl.lit(source_family).alias("source_family"),
                    pl.col("feature_id"),
                    pl.lit(metric).alias("value_name"),
                    pl.col("ref_date").alias("observation_ref_date"),
                    pl.col("available_date").alias("observation_available_date"),
                    pl.col(metric).cast(pl.Float64).alias("value"),
                    unit_expr.alias("unit"),
                    pl.col("source_version"),
                ]
            )
        )
    return _concat(rows)


def _included_families(
    *,
    include_fuel_prices: bool,
    include_fuel_sales: bool,
    include_oil_gas: bool,
) -> list[str]:
    families: list[str] = []
    if include_fuel_prices:
        families.append("anp_fuel_price")
    if include_fuel_sales:
        families.append("anp_fuel_sales")
    if include_oil_gas:
        families.append("anp_oil_gas")
    return families


def _concat(frames: list[pl.DataFrame | None]) -> pl.DataFrame:
    available = [frame for frame in frames if frame is not None and not frame.is_empty()]
    if not available:
        return pl.DataFrame()
    return pl.concat(available, how="diagonal_relaxed")


def _empty_state() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ANP_STATE_ASOF_DAILY_COLUMNS})


def _empty_daily_long() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ANP_DAILY_LONG_COLUMNS})
