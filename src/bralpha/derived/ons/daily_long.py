from __future__ import annotations

from datetime import date

import polars as pl

from bralpha.derived.ons.calendar import business_day_frame, business_days_b3
from bralpha.derived.ons.quality import validate_asof_panel
from bralpha.derived.ons.schemas import (
    ONS_DAILY_LONG_COLUMNS,
    ONS_STATE_ASOF_DAILY_COLUMNS,
    PANEL_PRIMARY_KEYS,
)

STATE_KEY_COLUMNS = ["source_family", "feature_id", "value_name"]


def build_ons_state_asof_daily(
    *,
    ear: pl.DataFrame | None = None,
    ena: pl.DataFrame | None = None,
    load: pl.DataFrame | None = None,
    cmo: pl.DataFrame | None = None,
    energy_balance: pl.DataFrame | None = None,
    interchange: pl.DataFrame | None = None,
    start: date,
    end: date,
    max_features: int,
) -> pl.DataFrame:
    observations = _concat(
        [
            _state_rows(
                ear,
                source_family="ons_ear_subsystem",
                metrics=[
                    ("stored_energy_mwmes", None),
                    ("stored_energy_percent", None),
                    ("stored_energy_max_mwmes", None),
                ],
            ),
            _state_rows(
                ena,
                source_family="ons_ena_subsystem",
                metrics=[("ena_value", None)],
            ),
            _state_rows(
                load,
                source_family="ons_load_daily",
                metrics=[("load_mwmed", None)],
            ),
            _state_rows(
                cmo,
                source_family="ons_cmo_weekly",
                metrics=[("cmo_brl_mwh", None)],
            ),
            _state_rows(
                energy_balance,
                source_family="ons_energy_balance_daily",
                metrics=[
                    ("load_mwmed", None),
                    ("hydro_generation_mwmed", None),
                    ("thermal_generation_mwmed", None),
                    ("wind_generation_mwmed", None),
                    ("solar_generation_mwmed", None),
                    ("other_generation_mwmed", None),
                    ("interchange_mwmed", None),
                    ("hour_count", "count"),
                ],
            ),
            _state_rows(
                interchange,
                source_family="ons_interchange_daily",
                metrics=[
                    ("interchange_mwmed", None),
                    ("programmed_interchange_mwmed", None),
                    ("hour_count", "count"),
                ],
            ),
        ]
    )
    if observations.is_empty() or not business_days_b3(start, end):
        return _empty_state()

    obs = (
        observations.filter(pl.col("observation_available_date").is_not_null())
        .filter(pl.col("model_usable").fill_null(False))
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
            f"Selected ONS feature count {feature_count} exceeds max_features={max_features}"
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
        .select(ONS_STATE_ASOF_DAILY_COLUMNS)
    )
    validate_asof_panel(
        frame,
        required_columns=ONS_STATE_ASOF_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["state_asof_daily"],
    )
    return frame


def build_ons_daily_long(
    *,
    state_asof_daily: pl.DataFrame | None = None,
    include_hydro: bool,
    include_load_cmo: bool,
    include_energy_balance: bool,
    include_interchange: bool,
) -> pl.DataFrame:
    if state_asof_daily is None or state_asof_daily.is_empty():
        return _empty_daily_long()

    families = _included_families(
        include_hydro=include_hydro,
        include_load_cmo=include_load_cmo,
        include_energy_balance=include_energy_balance,
        include_interchange=include_interchange,
    )
    if not families:
        return _empty_daily_long()

    frame = (
        state_asof_daily.filter(pl.col("source_family").is_in(families))
        .filter(pl.col("value").is_not_null())
        .select(ONS_DAILY_LONG_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["daily_long"], keep="last", maintain_order=True)
    )
    validate_asof_panel(
        frame,
        required_columns=ONS_DAILY_LONG_COLUMNS,
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
    if "model_usable" not in frame.columns:
        frame = frame.with_columns(pl.lit(True).alias("model_usable"))
    rows = []
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
                    pl.col("model_usable"),
                ]
            )
        )
    return _concat(rows)


def _included_families(
    *,
    include_hydro: bool,
    include_load_cmo: bool,
    include_energy_balance: bool,
    include_interchange: bool,
) -> list[str]:
    families: list[str] = []
    if include_hydro:
        families.extend(["ons_ear_subsystem", "ons_ena_subsystem"])
    if include_load_cmo:
        families.extend(["ons_load_daily", "ons_cmo_weekly"])
    if include_energy_balance:
        families.append("ons_energy_balance_daily")
    if include_interchange:
        families.append("ons_interchange_daily")
    return families


def _concat(frames: list[pl.DataFrame | None]) -> pl.DataFrame:
    available = [frame for frame in frames if frame is not None and not frame.is_empty()]
    if not available:
        return pl.DataFrame()
    return pl.concat(available, how="diagonal_relaxed")


def _empty_state() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ONS_STATE_ASOF_DAILY_COLUMNS})


def _empty_daily_long() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in ONS_DAILY_LONG_COLUMNS})
