from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import exp
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import validate_panel
from bralpha.derived.b3.schemas import B3_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.feature_utils import (
    as_date,
    diff,
    feature_row,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
)
from bralpha.domain.di_futures import annual_rate_from_discount_factor

SOURCE_FAMILY = "b3_di_curve_feature"
CHANGE_LAGS = [1, 5, 21]
ROLLDOWN_HORIZONS = [1, 5, 21]
SLOPES = [
    ("slope_21_252_bp", 21, 252),
    ("slope_63_252_bp", 63, 252),
    ("slope_252_504_bp", 252, 504),
    ("slope_504_1260_bp", 504, 1260),
]
BUTTERFLIES = [
    ("butterfly_63_252_504_bp", 63, 252, 504),
    ("butterfly_252_504_1260_bp", 252, 504, 1260),
]
FORWARDS = [
    ("forward_21_63_bp", 21, 63),
    ("forward_63_126_bp", 63, 126),
    ("forward_126_252_bp", 126, 252),
    ("forward_252_504_bp", 252, 504),
    ("forward_504_756_bp", 504, 756),
    ("forward_756_1260_bp", 756, 1260),
]


def build_di_curve_feature_daily(
    di_curve_grid_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    grid_rows = [_normalize_row(row) for row in di_curve_grid_daily.to_dicts()]
    by_series: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    by_date_curve: dict[tuple[date, str], list[dict[str, Any]]] = defaultdict(list)
    for row in grid_rows:
        if row["tenor_business_days"] is None:
            continue
        by_series[(row["curve_id"], row["tenor_business_days"])].append(row)
        by_date_curve[(row["ref_date"], row["curve_id"])].append(row)

    rows: list[dict[str, Any]] = []
    for series_rows in by_series.values():
        series_rows.sort(key=lambda item: item["ref_date"])
        rows.extend(_tenor_feature_rows(series_rows, by_date_curve, start=start, end=end))

    for key in sorted(by_date_curve):
        curve_rows = sorted(by_date_curve[key], key=lambda item: item["tenor_business_days"] or 0)
        rows.extend(_shape_feature_rows(curve_rows, start=start, end=end))

    frame = _frame(rows)
    validate_panel(
        frame,
        required_columns=B3_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["di_feature_daily"],
    )
    return frame


def _tenor_feature_rows(
    series_rows: list[dict[str, Any]],
    by_date_curve: dict[tuple[date, str], list[dict[str, Any]]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    rows = []
    for position, row in enumerate(series_rows):
        ref_date = row["ref_date"]
        if not in_output_window(ref_date, start, end):
            continue
        feature_id = f"b3_di_curve:{row['curve_id']}:{row['tenor_business_days']}bd"
        rows.extend(
            [
                _row(row, feature_id, "rate_level_bp", row["implied_annual_rate_bp"], "bp"),
                _row(
                    row,
                    feature_id,
                    "log_discount_factor",
                    row["log_discount_factor"],
                    "log_discount_factor",
                ),
                _row(row, feature_id, "is_interpolated", row["is_interpolated"], "flag"),
                _row(row, feature_id, "is_extrapolated", row["is_extrapolated"], "flag"),
            ]
        )
        for lag in CHANGE_LAGS:
            lagged = _lag(series_rows, position, lag)
            rows.append(
                _row(
                    row,
                    feature_id,
                    f"rate_change_{lag}bd_bp",
                    _lagged_diff(row, lagged, "implied_annual_rate_bp"),
                    "bp",
                    extra_rows=[lagged],
                )
            )
            rows.append(
                _row(
                    row,
                    feature_id,
                    f"log_df_change_{lag}bd",
                    _lagged_diff(row, lagged, "log_discount_factor"),
                    "log_change",
                    extra_rows=[lagged],
                )
            )
        for horizon in ROLLDOWN_HORIZONS:
            curve_rows = by_date_curve[(row["ref_date"], row["curve_id"])]
            value = _rolldown(row, curve_rows, horizon)
            rows.append(_row(row, feature_id, f"rolldown_{horizon}bd_bp", value, "bp"))
    return rows


def _shape_feature_rows(
    curve_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    if not curve_rows:
        return []
    ref_date = curve_rows[0]["ref_date"]
    if not in_output_window(ref_date, start, end):
        return []
    by_tenor = {row["tenor_business_days"]: row for row in curve_rows}
    curve_id = curve_rows[0]["curve_id"]
    feature_id = f"b3_di_curve:{curve_id}:shape"
    rows = []
    for value_name, left, right in SLOPES:
        rows.append(
            _shape_row(
                ref_date,
                feature_id,
                value_name,
                _rate_diff(by_tenor, right, left),
                "bp",
                [by_tenor.get(left), by_tenor.get(right)],
            )
        )
    for value_name, left, middle, right in BUTTERFLIES:
        left_row = by_tenor.get(left)
        middle_row = by_tenor.get(middle)
        right_row = by_tenor.get(right)
        value = None
        if left_row is not None and middle_row is not None and right_row is not None:
            middle_rate = optional_float(middle_row.get("implied_annual_rate_bp"))
            left_rate = optional_float(left_row.get("implied_annual_rate_bp"))
            right_rate = optional_float(right_row.get("implied_annual_rate_bp"))
            if middle_rate is not None and left_rate is not None and right_rate is not None:
                value = 2.0 * middle_rate - left_rate - right_rate
        rows.append(
            _shape_row(
                ref_date,
                feature_id,
                value_name,
                value,
                "bp",
                [left_row, middle_row, right_row],
            )
        )
    for value_name, left, right in FORWARDS:
        rows.append(
            _shape_row(
                ref_date,
                feature_id,
                value_name,
                _forward_bp(by_tenor, left, right),
                "bp",
                [by_tenor.get(left), by_tenor.get(right)],
            )
        )
    return rows


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "ref_date": as_date(row["ref_date"]),
        "available_date": as_date(row["available_date"]),
        "curve_id": str(row.get("curve_id") or "DI1"),
        "tenor_business_days": _optional_int(row.get("tenor_business_days")),
        "implied_annual_rate_bp": optional_float(row.get("implied_annual_rate_bp")),
        "log_discount_factor": optional_float(row.get("log_discount_factor")),
        "is_interpolated": bool(row.get("is_interpolated")),
        "is_extrapolated": bool(row.get("is_extrapolated")),
        "source_version": row.get("source_version") or "v0",
    }


def _row(
    row: dict[str, Any],
    feature_id: str,
    value_name: str,
    value: float | bool | None,
    unit: str,
    *,
    extra_rows: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    extra_rows = extra_rows or []
    all_rows = [row, *[extra for extra in extra_rows if extra is not None]]
    return feature_row(
        ref_date=row["ref_date"],
        source_family=SOURCE_FAMILY,
        feature_id=feature_id,
        value_name=value_name,
        value=value,
        unit=unit,
        observation_ref_date=row["ref_date"],
        observation_available_date=max_date(*(item.get("available_date") for item in all_rows)),
        source_version=join_versions(*(item.get("source_version") for item in all_rows)),
    )


def _shape_row(
    ref_date: date,
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
    input_rows: list[dict[str, Any] | None],
) -> dict[str, Any]:
    rows = [row for row in input_rows if row is not None]
    return feature_row(
        ref_date=ref_date,
        source_family=SOURCE_FAMILY,
        feature_id=feature_id,
        value_name=value_name,
        value=value,
        unit=unit,
        observation_ref_date=ref_date,
        observation_available_date=max_date(*(row.get("available_date") for row in rows)),
        source_version=join_versions(*(row.get("source_version") for row in rows)),
    )


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _lagged_diff(
    row: dict[str, Any],
    lagged: dict[str, Any] | None,
    column: str,
) -> float | None:
    if lagged is None:
        return None
    return diff(row.get(column), lagged.get(column))


def _rolldown(row: dict[str, Any], series_rows: list[dict[str, Any]], horizon: int) -> float | None:
    tenor = row["tenor_business_days"]
    current_rate = optional_float(row.get("implied_annual_rate_bp"))
    if tenor is None or current_rate is None:
        return None
    target_tenor = tenor - horizon
    if target_tenor <= 0:
        return None
    target_log_df = _interpolated_log_df(series_rows, target_tenor)
    if target_log_df is None:
        return None
    target_df = exp(target_log_df)
    target_rate = annual_rate_from_discount_factor(target_df, target_tenor)
    if target_rate is None:
        return None
    return target_rate * 10_000.0 - current_rate


def _interpolated_log_df(rows: list[dict[str, Any]], tenor: int) -> float | None:
    valid = sorted(
        [
            row
            for row in rows
            if row.get("tenor_business_days") is not None
            and row.get("log_discount_factor") is not None
        ],
        key=lambda item: item["tenor_business_days"],
    )
    left = None
    right = None
    for row in valid:
        row_tenor = row["tenor_business_days"]
        if row_tenor <= tenor:
            left = row
        if row_tenor >= tenor and right is None:
            right = row
    if left is None or right is None:
        return None
    if left["tenor_business_days"] == right["tenor_business_days"]:
        return optional_float(left.get("log_discount_factor"))
    width = right["tenor_business_days"] - left["tenor_business_days"]
    if width <= 0:
        return None
    left_log_df = optional_float(left.get("log_discount_factor"))
    right_log_df = optional_float(right.get("log_discount_factor"))
    if left_log_df is None or right_log_df is None:
        return None
    weight = (tenor - left["tenor_business_days"]) / width
    return left_log_df + (right_log_df - left_log_df) * weight


def _rate_diff(
    by_tenor: dict[int | None, dict[str, Any]],
    right: int,
    left: int,
) -> float | None:
    right_row = by_tenor.get(right)
    left_row = by_tenor.get(left)
    if right_row is None or left_row is None:
        return None
    return diff(right_row.get("implied_annual_rate_bp"), left_row.get("implied_annual_rate_bp"))


def _forward_bp(by_tenor: dict[int | None, dict[str, Any]], left: int, right: int) -> float | None:
    if right <= left:
        return None
    left_row = by_tenor.get(left)
    right_row = by_tenor.get(right)
    if left_row is None or right_row is None:
        return None
    left_log_df = optional_float(left_row.get("log_discount_factor"))
    right_log_df = optional_float(right_row.get("log_discount_factor"))
    if left_log_df is None or right_log_df is None:
        return None
    return (exp((left_log_df - right_log_df) * 252.0 / (right - left)) - 1.0) * 10_000.0


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in B3_FEATURE_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(B3_FEATURE_DAILY_COLUMNS)
