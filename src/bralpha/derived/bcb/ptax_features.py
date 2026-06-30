from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import polars as pl

from bralpha.derived.bcb.quality import validate_asof_panel
from bralpha.derived.bcb.schemas import BCB_PTAX_FEATURE_DAILY_COLUMNS, PANEL_PRIMARY_KEYS
from bralpha.derived.feature_utils import (
    as_date,
    feature_row,
    in_output_window,
    join_versions,
    max_date,
    optional_float,
    realized_vol_ann,
    safe_log,
    safe_log_return,
    safe_ratio,
)

SOURCE_FAMILY = "bcb_ptax_feature"
RETURN_LAGS = [1, 5, 21]
VOL_WINDOWS = [21, 63]


def build_ptax_feature_daily(
    ptax_selected_daily: pl.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ptax_selected_daily.to_dicts():
        normalized = _normalize_row(row)
        grouped[normalized["currency_code"]].append(normalized)

    rows: list[dict[str, Any]] = []
    for series_rows in grouped.values():
        series_rows.sort(key=lambda item: item["ref_date"])
        rows.extend(_series_feature_rows(series_rows, start=start, end=end))

    frame = _frame(rows)
    if frame.is_empty():
        return frame
    validate_asof_panel(
        frame,
        required_columns=BCB_PTAX_FEATURE_DAILY_COLUMNS,
        primary_keys=PANEL_PRIMARY_KEYS["ptax_feature_daily"],
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
        one_day_return = safe_log_return(_mid_rate(row), _mid_rate(previous))
        one_day_returns.append(one_day_return)
        if not in_output_window(row["ref_date"], start, end):
            continue

        feature_id = f"bcb_ptax:{row['currency_code']}"
        mid_rate = _mid_rate(row)
        mid_parity = _mid_parity(row)
        rows.extend(
            [
                _row(row, feature_id, "mid_rate", mid_rate, "brl_per_currency"),
                _row(row, feature_id, "log_mid_rate", safe_log(mid_rate), "log_fx_rate"),
                _row(
                    row,
                    feature_id,
                    "bid_ask_spread_bp",
                    _spread_bp(row.get("bid_rate"), row.get("ask_rate")),
                    "bp",
                ),
                _row(row, feature_id, "mid_parity", mid_parity, "parity"),
                _row(row, feature_id, "log_mid_parity", safe_log(mid_parity), "log_parity"),
                _row(
                    row,
                    feature_id,
                    "parity_bid_ask_spread_bp",
                    _spread_bp(row.get("bid_parity"), row.get("ask_parity")),
                    "bp",
                ),
            ]
        )
        for lag in RETURN_LAGS:
            lagged = _lag(series_rows, position, lag)
            rows.append(
                _row(
                    row,
                    feature_id,
                    f"log_return_{lag}bd",
                    safe_log_return(mid_rate, _mid_rate(lagged)),
                    "log_return",
                    extra_rows=[lagged],
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
    return rows


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "ref_date": as_date(row["ref_date"]),
        "available_date": as_date(row["available_date"]),
        "currency_code": str(row.get("currency_code") or "").upper(),
        "source_version": row.get("source_version") or "v0",
    }


def _row(
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
    base = feature_row(
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
    base["availability_policy"] = None
    base["availability_basis"] = None
    base["revision_policy"] = "unrevised"
    base["model_usable"] = True
    return base


def _mid_rate(row: dict[str, Any] | None) -> float | None:
    if row is None:
        return None
    bid = optional_float(row.get("bid_rate"))
    ask = optional_float(row.get("ask_rate"))
    if bid is None and ask is None:
        return None
    if bid is None:
        return ask
    if ask is None:
        return bid
    return (bid + ask) / 2.0


def _mid_parity(row: dict[str, Any]) -> float | None:
    bid = optional_float(row.get("bid_parity"))
    ask = optional_float(row.get("ask_parity"))
    if bid is None and ask is None:
        return None
    if bid is None:
        return ask
    if ask is None:
        return bid
    return (bid + ask) / 2.0


def _spread_bp(bid: Any, ask: Any) -> float | None:
    bid_float = optional_float(bid)
    ask_float = optional_float(ask)
    mid = _mid_from_values(bid, ask)
    if bid_float is None or ask_float is None or mid in {None, 0.0}:
        return None
    ratio = safe_ratio(ask_float - bid_float, mid)
    if ratio is None:
        return None
    return 10_000.0 * ratio


def _mid_from_values(bid: Any, ask: Any) -> float | None:
    bid_float = optional_float(bid)
    ask_float = optional_float(ask)
    if bid_float is None and ask_float is None:
        return None
    if bid_float is None:
        return ask_float
    if ask_float is None:
        return bid_float
    return (bid_float + ask_float) / 2.0


def _lag(rows: list[dict[str, Any]], position: int, lag: int) -> dict[str, Any] | None:
    lag_position = position - lag
    if lag_position < 0:
        return None
    return rows[lag_position]


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.Null for column in BCB_PTAX_FEATURE_DAILY_COLUMNS})
    return pl.DataFrame(rows).select(BCB_PTAX_FEATURE_DAILY_COLUMNS)
