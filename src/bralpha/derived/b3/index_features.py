from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.b3.quality import validate_panel
from bralpha.derived.b3.schemas import B3_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.feature_utils import (
    as_date,
    feature_row,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    realized_vol_ann,
    safe_log,
    safe_log1p,
    safe_log_return,
)

INDEX_SOURCE_FAMILY = "b3_index_feature"
COMPOSITION_SOURCE_FAMILY = "b3_index_composition_feature"
RETURN_LAGS = [1, 5, 21]
VOL_WINDOWS = [21, 63]


def build_index_feature_daily(
    index_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in index_daily.to_dicts():
        normalized = _normalize_index_row(row)
        grouped[normalized["index_id"]].append(normalized)

    rows: list[dict[str, Any]] = []
    for series_rows in grouped.values():
        series_rows.sort(key=lambda item: item["ref_date"])
        rows.extend(_index_series_feature_rows(series_rows, start=start, end=end))

    frame = _frame(rows)
    validate_panel(
        frame,
        required_columns=B3_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["index_feature_daily"],
    )
    return frame


def build_index_composition_feature_daily(
    index_composition_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    grouped: dict[tuple[date, str], list[dict[str, Any]]] = defaultdict(list)
    for row in index_composition_daily.to_dicts():
        normalized = _normalize_composition_row(row)
        grouped[(normalized["ref_date"], normalized["index_id"])].append(normalized)

    rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        ref_date, _index_id = key
        if not in_output_window(ref_date, start, end):
            continue
        rows.extend(_composition_feature_rows(grouped[key]))

    frame = _frame(rows)
    validate_panel(
        frame,
        required_columns=B3_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["index_composition_feature_daily"],
    )
    return frame


def _index_series_feature_rows(
    series_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    rows = []
    one_day_returns: list[float | None] = []
    trailing_closes: list[float] = []
    for position, row in enumerate(series_rows):
        previous = _lag(series_rows, position, 1)
        one_day_return = safe_log_return(
            row.get("close"),
            previous.get("close") if previous is not None else None,
        )
        one_day_returns.append(one_day_return)
        close = optional_float(row.get("close"))
        if close is not None and close > 0:
            trailing_closes.append(close)

        if not in_output_window(row["ref_date"], start, end):
            continue
        feature_id = f"b3_index:{row['index_id']}"
        rows.append(
            _index_row(row, feature_id, "log_close", safe_log(row.get("close")), "log_points")
        )
        for lag in RETURN_LAGS:
            lagged = _lag(series_rows, position, lag)
            rows.append(
                _index_row(
                    row,
                    feature_id,
                    f"log_return_{lag}bd",
                    safe_log_return(
                        row.get("close"),
                        lagged.get("close") if lagged is not None else None,
                    ),
                    "log_return",
                    extra_rows=[lagged],
                )
            )
        for window in VOL_WINDOWS:
            rows.append(
                _index_row(
                    row,
                    feature_id,
                    f"realized_vol_{window}bd_ann",
                    realized_vol_ann(one_day_returns, window),
                    "annualized_log_vol",
                )
            )
        rows.extend(
            [
                _index_row(
                    row,
                    feature_id,
                    "intraday_range_log",
                    _intraday_range_log(row),
                    "log_range",
                ),
                _index_row(
                    row,
                    feature_id,
                    "close_drawdown_252bd_pct",
                    _drawdown_pct(close, trailing_closes[-252:]),
                    "percent",
                ),
                _index_row(
                    row,
                    feature_id,
                    "volume_log1p",
                    safe_log1p(row.get("volume")),
                    "log_count",
                ),
                _index_row(
                    row,
                    feature_id,
                    "financial_volume_log1p",
                    safe_log1p(row.get("financial_volume")),
                    "log_notional",
                ),
                _index_row(
                    row,
                    feature_id,
                    "number_of_trades_log1p",
                    safe_log1p(row.get("number_of_trades")),
                    "log_count",
                ),
            ]
        )
    return rows


def _composition_feature_rows(component_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not component_rows:
        return []
    ref_date = component_rows[0]["ref_date"]
    index_id = component_rows[0]["index_id"]
    feature_id = f"b3_index_composition:{index_id}"
    weights = [optional_float(row.get("weight")) for row in component_rows]
    weights = [value for value in weights if value is not None and value >= 0]
    max_weight = max(weights) if weights else None
    fractions = [
        value if max_weight is not None and max_weight <= 1.0 else value / 100.0
        for value in weights
    ]
    fractions = sorted(fractions, reverse=True)
    hhi = sum(weight * weight for weight in fractions)
    effective = 1.0 / hhi if hhi > 0 else None
    metrics = {
        "constituent_count": (len(weights), "count"),
        "weight_top1_pct": (_sum_top_pct(fractions, 1), "percent"),
        "top5_weight_pct": (_sum_top_pct(fractions, 5), "percent"),
        "top10_weight_pct": (_sum_top_pct(fractions, 10), "percent"),
        "hhi_weight": (hhi if fractions else None, "hhi"),
        "effective_constituents": (effective, "count"),
    }
    return [
        feature_row(
            ref_date=ref_date,
            source_family=COMPOSITION_SOURCE_FAMILY,
            feature_id=feature_id,
            value_name=value_name,
            value=value,
            unit=unit,
            observation_ref_date=ref_date,
            observation_available_date=max_date(
                *(row.get("available_date") for row in component_rows)
            ),
            source_version=join_versions(*(row.get("source_version") for row in component_rows)),
        )
        for value_name, (value, unit) in metrics.items()
    ]


def _normalize_index_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "ref_date": as_date(row["ref_date"]),
        "available_date": as_date(row["available_date"]),
        "index_id": str(row.get("index_id")),
        "source_version": row.get("source_version") or "v0",
    }


def _normalize_composition_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "ref_date": as_date(row["ref_date"]),
        "available_date": as_date(row["available_date"]),
        "index_id": str(row.get("index_id")),
        "source_version": row.get("source_version") or "v0",
    }


def _index_row(
    row: dict[str, Any],
    feature_id: str,
    value_name: str,
    value: float | None,
    unit: str,
    *,
    extra_rows: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    extra_rows = extra_rows or []
    all_rows = [row, *[extra for extra in extra_rows if extra is not None]]
    return feature_row(
        ref_date=row["ref_date"],
        source_family=INDEX_SOURCE_FAMILY,
        feature_id=feature_id,
        value_name=value_name,
        value=value,
        unit=unit,
        observation_ref_date=row["ref_date"],
        observation_available_date=max_date(*(item.get("available_date") for item in all_rows)),
        source_version=join_versions(*(item.get("source_version") for item in all_rows)),
    )


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _intraday_range_log(row: dict[str, Any]) -> float | None:
    high = optional_float(row.get("high"))
    low = optional_float(row.get("low"))
    if high is None or low is None or high <= 0 or low <= 0:
        return None
    return safe_log(high / low)


def _drawdown_pct(close: float | None, trailing_closes: list[float]) -> float | None:
    if close is None or close <= 0 or not trailing_closes:
        return None
    high = max(trailing_closes)
    if high <= 0:
        return None
    return 100.0 * (close / high - 1.0)


def _sum_top_pct(fractions: list[float], count: int) -> float | None:
    if not fractions:
        return None
    return 100.0 * sum(fractions[:count])


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in B3_FEATURE_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(B3_FEATURE_DAILY_COLUMNS)
