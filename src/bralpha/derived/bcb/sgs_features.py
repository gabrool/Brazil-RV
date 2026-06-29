from __future__ import annotations

from datetime import date
from math import log
from typing import Any

import polars as pl

from bralpha.derived.bcb.quality import validate_asof_panel
from bralpha.derived.bcb.schemas import BCB_SGS_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS

SOURCE_FAMILY = "bcb_sgs_feature"


def build_sgs_feature_daily(sgs_asof_daily: pl.DataFrame) -> pl.DataFrame:
    if sgs_asof_daily.is_empty():
        return _empty()
    frame = _ensure_columns(
        sgs_asof_daily,
        [
            "ref_date",
            "available_date",
            "series_slug",
            "category",
            "observation_ref_date",
            "observation_available_date",
            "value",
            "availability_policy",
            "availability_basis",
            "revision_policy",
            "model_usable",
            "is_available",
            "staleness_days",
            "source_version",
        ],
    ).filter(
        pl.col("model_usable").fill_null(False) & pl.col("is_available").fill_null(False)
    )
    if frame.is_empty():
        return _empty()

    rows: list[dict[str, object]] = []
    rows.extend(_rate_features(frame))
    rows.extend(_ipca_features(frame))
    rows.extend(_reserves_features(frame))
    if not rows:
        return _empty()

    output = (
        pl.DataFrame(rows)
        .select(BCB_SGS_FEATURE_DAILY_COLUMNS)
        .unique(subset=PANEL_PRIMARY_KEYS["sgs_feature_daily"], keep="last")
        .sort(["ref_date", "feature_id", "value_name"])
    )
    validate_asof_panel(
        output,
        required_columns=BCB_SGS_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["sgs_feature_daily"],
    )
    return output


def _rate_features(frame: pl.DataFrame) -> list[dict[str, object]]:
    over = _rows_by_ref_date(_series_rows(frame, "selic_over"))
    target_rows = _series_rows(frame, "selic_target")
    target = _rows_by_ref_date(target_rows)
    rows: list[dict[str, object]] = []

    for ref_date, row in over.items():
        rows.append(_feature_row(row, "rates", "selic_over_level_pa", row["value"], "percent_pa"))
        if ref_date in target:
            rows.append(
                _feature_row(
                    _combined_base([row, target[ref_date]]),
                    "rates",
                    "selic_over_minus_target_bp",
                    (_float(row["value"]) - _float(target[ref_date]["value"])) * 100,
                    "basis_points",
                )
            )

    for row in target_rows:
        rows.append(
            _feature_row(row, "rates", "selic_target_level_pa", row["value"], "percent_pa")
        )

    for offset, feature_name in [
        (1, "selic_target_change_1bd_bp"),
        (5, "selic_target_change_5bd_bp"),
    ]:
        for index, row in enumerate(target_rows):
            if index < offset:
                continue
            previous = target_rows[index - offset]
            change = (_float(row["value"]) - _float(previous["value"])) * 100
            rows.append(_feature_row(row, "rates", feature_name, change, "basis_points"))
            if offset == 1:
                rows.append(
                    _feature_row(
                        row,
                        "rates",
                        "selic_policy_step_flag",
                        1.0 if change != 0 else 0.0,
                        "flag",
                    )
                )
    return rows


def _ipca_features(frame: pl.DataFrame) -> list[dict[str, object]]:
    ipca_rows = _series_rows(frame, "ipca")
    if not ipca_rows:
        return []

    observations: dict[date, float] = {}
    for row in ipca_rows:
        observation_ref_date = row.get("observation_ref_date")
        if isinstance(observation_ref_date, date) and row.get("value") is not None:
            observations[observation_ref_date] = _float(row["value"])
    observation_months = sorted(observations)

    rows: list[dict[str, object]] = []
    for row in ipca_rows:
        rows.append(
            _feature_row(row, "inflation", "ipca_monthly_pct", row["value"], "percent")
        )
        rows.append(
            _feature_row(
                row,
                "inflation",
                "ipca_staleness_days",
                row.get("staleness_days"),
                "days",
            )
        )
        observation_ref_date = row.get("observation_ref_date")
        if not isinstance(observation_ref_date, date):
            continue
        available_months = [
            month for month in observation_months if month <= observation_ref_date
        ]
        last_3 = [_float(observations[month]) for month in available_months[-3:]]
        last_12 = [_float(observations[month]) for month in available_months[-12:]]
        if len(last_3) == 3:
            value_3m = sum(last_3)
            rows.append(
                _feature_row(row, "inflation", "ipca_3m_sum_pct", value_3m, "percent")
            )
            rows.append(
                _feature_row(
                    row,
                    "inflation",
                    "ipca_3m_ann_pct",
                    ((1 + value_3m / 100) ** 4 - 1) * 100,
                    "percent_annualized",
                )
            )
        if len(last_12) == 12:
            rows.append(
                _feature_row(
                    row,
                    "inflation",
                    "ipca_12m_sum_pct",
                    sum(last_12),
                    "percent",
                )
            )
    return rows


def _reserves_features(frame: pl.DataFrame) -> list[dict[str, object]]:
    reserves = _series_rows(frame, "international_reserves_liquidity")
    rows: list[dict[str, object]] = []
    levels = [_float(row["value"]) for row in reserves]
    logs = [log(value) if value > 0 else None for value in levels]

    for index, row in enumerate(reserves):
        value = levels[index]
        log_value = logs[index]
        rows.append(
            _feature_row(
                row,
                "external_reserves",
                "reserves_usd_mn_level",
                value,
                "usd_millions",
            )
        )
        rows.append(
            _feature_row(
                row,
                "external_reserves",
                "reserves_log_level",
                log_value,
                "log",
            )
        )
        for offset, feature_name in [
            (1, "reserves_log_change_1bd"),
            (5, "reserves_log_change_5bd"),
        ]:
            if index >= offset and log_value is not None and logs[index - offset] is not None:
                rows.append(
                    _feature_row(
                        row,
                        "external_reserves",
                        feature_name,
                        log_value - logs[index - offset],
                        "log_change",
                    )
                )
        if index >= 20 and levels[index - 20] != 0:
            rows.append(
                _feature_row(
                    row,
                    "external_reserves",
                    "reserves_pct_change_20bd",
                    (value / levels[index - 20] - 1) * 100,
                    "percent",
                )
            )
        trailing_high = max(levels[max(0, index - 251) : index + 1])
        if trailing_high > 0:
            rows.append(
                _feature_row(
                    row,
                    "external_reserves",
                    "reserves_drawdown_from_252bd_high_pct",
                    (value / trailing_high - 1) * 100,
                    "percent",
                )
            )
    return rows


def _feature_row(
    base: dict[str, Any],
    family: str,
    value_name: str,
    value: object,
    unit: str,
) -> dict[str, object]:
    return {
        "ref_date": base["ref_date"],
        "available_date": base.get("available_date") or base["ref_date"],
        "source_family": SOURCE_FAMILY,
        "feature_id": f"{SOURCE_FAMILY}:{family}:{value_name}",
        "value_name": value_name,
        "value": None if value is None else _float(value),
        "unit": unit,
        "observation_ref_date": base.get("observation_ref_date"),
        "observation_available_date": base.get("observation_available_date"),
        "availability_policy": base.get("availability_policy"),
        "availability_basis": base.get("availability_basis"),
        "revision_policy": base.get("revision_policy"),
        "model_usable": True,
        "is_available": base.get("is_available", True),
        "staleness_days": base.get("staleness_days"),
        "source_version": base.get("source_version"),
    }


def _combined_base(rows: list[dict[str, Any]]) -> dict[str, object]:
    base = dict(rows[0])
    observation_ref_dates = [
        row.get("observation_ref_date")
        for row in rows
        if isinstance(row.get("observation_ref_date"), date)
    ]
    observation_available_dates = [
        row.get("observation_available_date")
        for row in rows
        if isinstance(row.get("observation_available_date"), date)
    ]
    staleness_values = [
        row.get("staleness_days") for row in rows if row.get("staleness_days") is not None
    ]
    if observation_ref_dates:
        base["observation_ref_date"] = max(observation_ref_dates)
    if observation_available_dates:
        base["observation_available_date"] = max(observation_available_dates)
    if staleness_values:
        base["staleness_days"] = max(staleness_values)
    return base


def _series_rows(frame: pl.DataFrame, slug: str) -> list[dict[str, Any]]:
    return (
        frame.filter(pl.col("series_slug") == slug)
        .sort("ref_date")
        .to_dicts()
    )


def _rows_by_ref_date(rows: list[dict[str, Any]]) -> dict[date, dict[str, Any]]:
    return {row["ref_date"]: row for row in rows if isinstance(row.get("ref_date"), date)}


def _float(value: object) -> float:
    return float(value)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.Null for column in BCB_SGS_FEATURE_DAILY_COLUMNS})


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns([pl.lit(None).alias(column) for column in missing])
