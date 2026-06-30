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
    safe_ratio,
)

SOURCE_FAMILY = "b3_futures_feature"
RETURN_LAGS = [1, 5, 21]
VOL_WINDOWS = [5, 21, 63]


def build_futures_feature_daily(
    continuous_futures_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in continuous_futures_daily.to_dicts():
        normalized = _normalize_row(row)
        grouped[normalized["continuous_id"]].append(normalized)

    rows: list[dict[str, Any]] = []
    for series_rows in grouped.values():
        series_rows.sort(key=lambda item: item["ref_date"])
        rows.extend(_series_feature_rows(series_rows, start=start, end=end))

    frame = _frame(rows)
    validate_panel(
        frame,
        required_columns=B3_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["futures_feature_daily"],
    )
    return frame


def _series_feature_rows(
    series_rows: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    rows = []
    one_day_returns: list[float | None] = []
    for position, row in enumerate(series_rows):
        previous = _lag(series_rows, position, 1)
        one_day_return = safe_log_return(
            row.get("settlement"),
            previous.get("settlement") if previous is not None else None,
        )
        one_day_returns.append(one_day_return)
        if not in_output_window(row["ref_date"], start, end):
            continue

        feature_id = f"b3_futures:{row['continuous_id']}"
        rows.append(
            _row(
                row,
                feature_id,
                "log_settlement",
                safe_log(row.get("settlement")),
                "log_quote",
            )
        )
        for lag in RETURN_LAGS:
            lagged = _lag(series_rows, position, lag)
            rows.append(
                _row(
                    row,
                    feature_id,
                    f"log_return_{lag}bd",
                    safe_log_return(
                        row.get("settlement"),
                        lagged.get("settlement") if lagged is not None else None,
                    ),
                    "log_return",
                    extra_rows=[lagged],
                )
            )
        rows.append(
            _row(
                row,
                feature_id,
                "same_contract_log_return_1bd",
                _same_contract_log_return(row, previous),
                "log_return",
                extra_rows=[previous],
            )
        )
        for window in VOL_WINDOWS:
            rows.append(
                _row(
                    row,
                    feature_id,
                    f"realized_vol_{window}bd_ann",
                    realized_vol_ann(one_day_returns, window),
                    "annualized_log_vol",
                )
            )
        rows.extend(
            [
                _row(row, feature_id, "volume_log1p", safe_log1p(row.get("volume")), "log_count"),
                _row(
                    row,
                    feature_id,
                    "open_interest_log1p",
                    safe_log1p(row.get("open_interest")),
                    "log_count",
                ),
                _row(
                    row,
                    feature_id,
                    "volume_open_interest_ratio",
                    safe_ratio(row.get("volume"), row.get("open_interest")),
                    "ratio",
                ),
                _row(row, feature_id, "roll_gap_pct", _roll_gap_pct(row), "percent"),
                _row(row, feature_id, "is_roll_date", row.get("is_roll_date"), "flag"),
                _row(row, feature_id, "is_tradeable", row.get("is_tradeable"), "flag"),
                _row(
                    row,
                    feature_id,
                    "business_days_to_maturity",
                    optional_float(row.get("business_days_to_maturity")),
                    "business_days",
                ),
            ]
        )
    return rows


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "ref_date": as_date(row["ref_date"]),
        "available_date": as_date(row["available_date"]),
        "continuous_id": str(row.get("continuous_id")),
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


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _same_contract_log_return(
    row: dict[str, Any],
    previous: dict[str, Any] | None,
) -> float | None:
    if previous is None:
        return None
    if row.get("selected_contract_id") != previous.get("selected_contract_id"):
        return None
    return safe_log_return(row.get("settlement"), previous.get("settlement"))


def _roll_gap_pct(row: dict[str, Any]) -> float | None:
    quote_return = optional_float(row.get("quote_pct_change_1d"))
    same_contract_return = optional_float(row.get("same_contract_quote_pct_change_1d"))
    if quote_return is None or same_contract_return is None:
        return None
    return 100.0 * (quote_return - same_contract_return)


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in B3_FEATURE_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(B3_FEATURE_DAILY_COLUMNS)
